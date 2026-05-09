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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class MonitoringService {

    private final DataStore store;
    private final AlertService alertService;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService probePool = Executors.newCachedThreadPool();
    private volatile ScheduledFuture<?> currentTask;
    private final AtomicBoolean cycleRunning = new AtomicBoolean(false);
    private final AtomicBoolean immediateCheckQueued = new AtomicBoolean(false);
    private final AtomicLong cycleCounter = new AtomicLong(0);

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofMillis(500))
            .followRedirects(HttpClient.Redirect.NEVER)
            .build();

    @Autowired
    public MonitoringService(DataStore store, AlertService alertService) {
        this.store = store;
        this.alertService = alertService;
    }

    @PostConstruct
    public void start() {
        reschedule(store.getConfig().getCheckIntervalSeconds());
    }

    @PreDestroy
    public void stop() {
        if (currentTask != null) currentTask.cancel(false);
        scheduler.shutdown();
        probePool.shutdown();
    }

    public synchronized void reschedule(int intervalSeconds) {
        if (currentTask != null && !currentTask.isDone()) {
            currentTask.cancel(false);
        }
        currentTask = scheduler.scheduleWithFixedDelay(
                this::runCheckCycle,
                0,
                intervalSeconds,
                TimeUnit.SECONDS);
    }

    public void triggerImmediateCheck() {
        requestImmediateCheck();
    }

    public boolean triggerImmediateCheckAndWait() {
        int timeoutMs = store.getConfig().getRequestTimeoutMs();
        int proxyCount = Math.max(1, store.getPoolSize());
        long maxWaitMs = Math.max(1000L, (long) timeoutMs * proxyCount + 500L);
        return triggerImmediateCheckAndWait(Duration.ofMillis(maxWaitMs));
    }

    private void runCheckCycle() {
        if (!cycleRunning.compareAndSet(false, true)) {
            return;
        }
        try {
            MonitoringConfigData config = store.getConfig();
            List<ProxyEntry> proxies = store.getAllProxies();

            if (proxies.isEmpty()) {
                alertService.evaluate();
                return;
            }

            int timeoutMs = config.getRequestTimeoutMs();
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (ProxyEntry proxy : proxies) {
                futures.add(CompletableFuture.runAsync(() -> {
                    String result = probeProxy(proxy.getUrl(), timeoutMs);
                    proxy.recordCheck(result, Instant.now());
                    store.incrementTotalChecks();
                }, probePool));
            }

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            alertService.evaluate();

        } catch (Exception e) {
            System.err.println("MonitoringService cycle error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            cycleCounter.incrementAndGet();
            cycleRunning.set(false);
            if (immediateCheckQueued.getAndSet(false)) {
                probePool.submit(this::runCheckCycle);
            }
        }
    }

    private void requestImmediateCheck() {
        if (cycleRunning.get()) {
            immediateCheckQueued.set(true);
            return;
        }
        probePool.submit(this::runCheckCycle);
    }

    private boolean triggerImmediateCheckAndWait(Duration timeout) {
        long target = cycleCounter.get() + 1;
        requestImmediateCheck();
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (cycleCounter.get() >= target) {
                return true;
            }
            try {
                Thread.sleep(25);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return cycleCounter.get() >= target;
    }

    private String probeProxy(String url, int timeoutMs) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofMillis(timeoutMs))
                    .GET()
                    .build();

            HttpResponse<Void> response = httpClient.send(
                    request,
                    HttpResponse.BodyHandlers.discarding()
            );

            int code = response.statusCode();
            return (code >= 200 && code < 300) ? "up" : "down";

        } catch (Exception e) {
            return "down";
        }
    }
}
