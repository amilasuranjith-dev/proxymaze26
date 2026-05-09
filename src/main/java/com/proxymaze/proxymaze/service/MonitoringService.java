package com.proxymaze.proxymaze.service;

import com.proxymaze.proxymaze.model.MonitoringConfigData;
import com.proxymaze.proxymaze.model.ProxyEntry;
import com.proxymaze.proxymaze.store.DataStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.*;

@Service
public class MonitoringService {

    private final DataStore store;
    private final AlertService alertService;

    // Single-threaded scheduler controls the monitoring loop
    private final ScheduledExecutorService scheduler =
        Executors.newSingleThreadScheduledExecutor();

    // Thread pool for parallel proxy probes
    private final ExecutorService probePool =
        Executors.newCachedThreadPool();

    // Keeps reference to the current scheduled task so we can cancel it on reschedule
    private volatile ScheduledFuture<?> currentTask;

    // HttpClient - created once, used for all probes
    private final HttpClient httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(30))
        .followRedirects(HttpClient.Redirect.NEVER)
        .build();

    @Autowired
    public MonitoringService(DataStore store, AlertService alertService) {
        this.store = store;
        this.alertService = alertService;
    }

    @PostConstruct
    public void start() {
        // Start the monitoring loop with default config on startup
        reschedule(store.getConfig().getCheckIntervalSeconds());
    }

    @PreDestroy
    public void stop() {
        if (currentTask != null) currentTask.cancel(false);
        scheduler.shutdown();
        probePool.shutdown();
    }

    //Cancels the current loop and starts a new one with the new interval.
    public synchronized void reschedule(int intervalSeconds) {
        if (currentTask != null && !currentTask.isDone()) {
            currentTask.cancel(false); // don't interrupt if running
        }
        currentTask = scheduler.scheduleAtFixedRate(
            this::runCheckCycle,
            0,                   // start immediately
            intervalSeconds,
            TimeUnit.SECONDS
        );
    }

    //Trigger an immediate check cycle (used when new proxies are added).
    public void triggerImmediateCheck() {
        probePool.submit(this::runCheckCycle);
    }

    //One complete monitoring pass: probe all proxies, then evaluate alerts.
    private void runCheckCycle() {
        try {
            MonitoringConfigData config = store.getConfig();
            List<ProxyEntry> proxies = store.getAllProxies();

            if (proxies.isEmpty()) {
                alertService.evaluate();
                return;
            }

            // Probe all proxies in parallel
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (ProxyEntry proxy : proxies) {
                futures.add(CompletableFuture.runAsync(() -> {
                    String result = probeProxy(proxy.getUrl(), config.getRequestTimeoutMs());
                    proxy.recordCheck(result, Instant.now());
                    store.incrementTotalChecks();
                }, probePool));
            }

            // Wait for ALL probes to finish before evaluating alerts
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

            // Evaluate alerts after all probes are done
            alertService.evaluate();

        } catch (Exception e) {
            // Never let a crash kill the monitoring loop
            System.err.println("MonitoringService cycle error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Makes a real HTTP GET request to the proxy URL.
     * Returns "up" if 2xx received within timeout, "down" otherwise.
     *
     * - 2xx within timeout_ms → "up"
     * - Timeout, connection refused, any error → "down"
     * - 5xx response → "down"
     * - Any other non-2xx → "down"
     */
    private String probeProxy(String url, int timeoutMs) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofMillis(timeoutMs))
                .GET()
                .build();

            HttpResponse<Void> response = httpClient.send(
                request,
                HttpResponse.BodyHandlers.discarding() // don't read body, save memory
            );

            int code = response.statusCode();
            if (code >= 200 && code < 300) {
                return "up";
            } else {
                return "down"; // 5xx, 4xx, 3xx — only 2xx = up
            }

        } catch (Exception e) {
            // HttpTimeoutException, ConnectException, IOException, etc.
            return "down";
        }
    }
}

