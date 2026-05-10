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
import java.util.concurrent.atomic.AtomicLong;

/**
 * Autonomous background monitoring engine.
 *
 * Design principles:
 * 1. Uses scheduleWithFixedDelay — never overlaps cycles
 * 2. Config changes reschedule the delay for subsequent cycles
 * 3. Each cycle: snapshot config → snapshot proxies → probe concurrently →
 *    wait for all probes → evaluate alerts → dispatch webhooks
 * 4. No hardcoded connect timeout — uses configurable request_timeout_ms
 * 5. Immediate check support with blocking wait via cycleCounter
 * 6. Single-threaded scheduler guarantees no overlapping cycles
 */
@Service
public class MonitoringService {

    private final DataStore store;
    private final AlertService alertService;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "monitoring-scheduler");
        t.setDaemon(true);
        return t;
    });
    private final ExecutorService probePool = Executors.newFixedThreadPool(
            Math.max(4, Runtime.getRuntime().availableProcessors() * 2),
            r -> {
                Thread t = new Thread(r, "probe-worker");
                t.setDaemon(true);
                return t;
            });

    private volatile ScheduledFuture<?> currentTask;
    private final AtomicLong cycleCounter = new AtomicLong(0);

    // No hardcoded connect timeout — per-request timeout controls everything
    private final HttpClient httpClient = HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NEVER)
            .build();

    @Autowired
    public MonitoringService(DataStore store, AlertService alertService) {
        this.store = store;
        this.alertService = alertService;
    }

    @PostConstruct
    public void start() {
        int interval = store.getConfig().getCheckIntervalSeconds();
        currentTask = scheduler.scheduleWithFixedDelay(
                this::runCheckCycle,
                interval,  // first cycle after one interval
                Math.max(1, interval),
                TimeUnit.SECONDS);
    }

    @PreDestroy
    public void stop() {
        if (currentTask != null) currentTask.cancel(false);
        scheduler.shutdownNow();
        probePool.shutdownNow();
    }

    /**
     * Called by ConfigController when config changes.
     * Cancels the current schedule and reschedules with the new interval.
     * Does NOT fire an immediate cycle — next cycle fires after the new interval.
     */
    public synchronized void reschedule(int intervalSeconds) {
        if (currentTask != null && !currentTask.isDone()) {
            currentTask.cancel(false);
        }
        int interval = Math.max(1, intervalSeconds);
        currentTask = scheduler.scheduleWithFixedDelay(
                this::runCheckCycle,
                interval,
                interval,
                TimeUnit.SECONDS);
    }

    /**
     * Fire-and-forget immediate check trigger.
     */
    public void triggerImmediateCheck() {
        scheduler.execute(this::runCheckCycle);
    }

    /**
     * Trigger an immediate check and block until at least one COMPLETE cycle
     * finishes AFTER this call. Uses the cycleCounter to detect completion.
     *
     * This is called by POST /proxies to ensure the evaluator sees updated state.
     */
    public boolean triggerImmediateCheckAndWait() {
        int timeoutMs = store.getConfig().getRequestTimeoutMs();
        int proxyCount = Math.max(1, store.getPoolSize());
        long maxWaitMs = Math.max(5000L, (long) timeoutMs * proxyCount + 3000L);

        long targetCycle = cycleCounter.get() + 1;

        // Submit the cycle to the scheduler thread — queues behind any in-flight cycle
        scheduler.execute(this::runCheckCycle);

        // Spin-wait for the cycle to complete (with backoff sleep)
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(maxWaitMs);
        while (System.nanoTime() < deadline) {
            if (cycleCounter.get() >= targetCycle) {
                return true;
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return cycleCounter.get() >= targetCycle;
    }

    /**
     * Runs a single monitoring cycle. The single-threaded scheduler guarantees
     * this is never called concurrently — cycles are inherently serialized.
     */
    private void runCheckCycle() {
        try {
            // Step 1: Snapshot config
            MonitoringConfigData config = store.getConfig();
            int timeoutMs = config.getRequestTimeoutMs();

            // Step 2: Snapshot proxies
            List<ProxyEntry> proxies = store.getAllProxies();

            if (proxies.isEmpty()) {
                alertService.evaluate();
                return;
            }

            // Step 3: Probe all proxies concurrently
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (ProxyEntry proxy : proxies) {
                futures.add(CompletableFuture.runAsync(() -> {
                    String result = probeProxy(proxy.getUrl(), timeoutMs);
                    proxy.recordCheck(result, Instant.now());
                    store.incrementTotalChecks();
                }, probePool));
            }

            // Step 4: Wait for ALL probes to complete before evaluating alerts
            try {
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                        .get(timeoutMs + 5000L, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                // Some probes timed out via the future — cancel and proceed
                futures.forEach(f -> f.cancel(true));
            } catch (ExecutionException e) {
                System.err.println("MonitoringService probe error: " + e.getMessage());
            }

            // Step 5: Atomically evaluate alerts based on current proxy state
            alertService.evaluate();

        } catch (Exception e) {
            System.err.println("MonitoringService cycle error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            cycleCounter.incrementAndGet();
        }
    }

    /**
     * Probes a single proxy URL.
     * Returns "up" for 2xx responses, "down" for everything else
     * (5xx, 4xx, timeout, connection refused, any exception).
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
                    HttpResponse.BodyHandlers.discarding()
            );

            int code = response.statusCode();
            // Only 2xx is considered UP — everything else is DOWN
            return (code >= 200 && code < 300) ? "up" : "down";

        } catch (Exception e) {
            // Timeout, connection refused, IO error — all DOWN
            return "down";
        }
    }
}
