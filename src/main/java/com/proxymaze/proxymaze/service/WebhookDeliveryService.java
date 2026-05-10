package com.proxymaze.proxymaze.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.proxymaze.proxymaze.model.Alert;
import com.proxymaze.proxymaze.model.Integration;
import com.proxymaze.proxymaze.model.WebhookRegistration;
import com.proxymaze.proxymaze.store.DataStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;

/**
 * Asynchronous webhook delivery engine with retry.
 *
 * Design:
 * - Payloads are built as IMMUTABLE snapshots at event time (no live references)
 * - Per-receiver single-thread executor preserves event ordering
 * - Retries use exponential backoff: 1s, 2s, 4s, 8s, 16s (capped at 30s)
 * - Retry only on 5xx and network/timeout errors
 * - No retry on 4xx (permanent failure)
 * - Idempotency via deliveredKeys set prevents duplicate successful deliveries
 * - 60-second delivery deadline per event
 */
@Service
public class WebhookDeliveryService {

    private static final DateTimeFormatter ISO_INSTANT = DateTimeFormatter.ISO_INSTANT;
    private static final long DELIVERY_DEADLINE_SECONDS = 60;

    private final DataStore store;
    private final ObjectMapper objectMapper;
    private final ScheduledExecutorService retryScheduler = Executors.newScheduledThreadPool(2, r -> {
        Thread t = new Thread(r, "webhook-retry");
        t.setDaemon(true);
        return t;
    });
    private final Map<String, ExecutorService> receiverExecutors = new ConcurrentHashMap<>();

    private final HttpClient httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .build();

    private final Set<String> deliveredKeys = Collections.newSetFromMap(new ConcurrentHashMap<>());

    @Autowired
    public WebhookDeliveryService(DataStore store, ObjectMapper objectMapper) {
        this.store = store;
        this.objectMapper = objectMapper;
    }

    @PreDestroy
    public void shutdown() {
        for (ExecutorService executor : receiverExecutors.values()) {
            executor.shutdown();
        }
        retryScheduler.shutdown();
    }

    public void deliverAlertFired(Alert alert) {
        Map<String, Object> payload = buildFiredPayload(alert);
        for (WebhookRegistration wh : store.getAllWebhooks()) {
            String receiverKey = "wh:" + wh.getWebhookId();
            String key = alert.getAlertId() + ":fired:" + wh.getWebhookId();
            enqueueDelivery(receiverKey, wh.getUrl(), payload, key);
        }
        for (Integration integration : store.getAllIntegrations()) {
            if (integration.getEvents().contains("alert.fired")) {
                String receiverKey = "int:" + integration.getIntegrationId();
                String key = alert.getAlertId() + ":fired:integration:" + integration.getIntegrationId();
                if ("slack".equals(integration.getType())) {
                    enqueueDelivery(receiverKey, integration.getWebhookUrl(), buildSlackFiredPayload(alert, integration), key);
                } else if ("discord".equals(integration.getType())) {
                    enqueueDelivery(receiverKey, integration.getWebhookUrl(), buildDiscordFiredPayload(alert), key);
                }
            }
        }
    }

    public void deliverAlertResolved(Alert alert) {
        Map<String, Object> payload = buildResolvedPayload(alert);
        for (WebhookRegistration wh : store.getAllWebhooks()) {
            String receiverKey = "wh:" + wh.getWebhookId();
            String key = alert.getAlertId() + ":resolved:" + wh.getWebhookId();
            enqueueDelivery(receiverKey, wh.getUrl(), payload, key);
        }
        for (Integration integration : store.getAllIntegrations()) {
            if (integration.getEvents().contains("alert.resolved")) {
                String receiverKey = "int:" + integration.getIntegrationId();
                String key = alert.getAlertId() + ":resolved:integration:" + integration.getIntegrationId();
                if ("slack".equals(integration.getType())) {
                    enqueueDelivery(receiverKey, integration.getWebhookUrl(), buildSlackResolvedPayload(alert, integration), key);
                } else if ("discord".equals(integration.getType())) {
                    enqueueDelivery(receiverKey, integration.getWebhookUrl(), buildDiscordResolvedPayload(alert), key);
                }
            }
        }
    }

    private void enqueueDelivery(String receiverKey, String url, Map<String, Object> payload, String deliveryKey) {
        if (deliveredKeys.contains(deliveryKey)) return;
        Instant start = Instant.now();
        getReceiverExecutor(receiverKey).submit(() -> deliverWithRetry(url, payload, deliveryKey, start, 0));
    }

    private void deliverWithRetry(String url, Map<String, Object> payload, String deliveryKey, Instant start, int attempt) {
        if (deliveredKeys.contains(deliveryKey)) return;
        if (isDeadlineExceeded(start)) return;

        try {
            String json = objectMapper.writeValueAsString(payload);
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            int code = response.statusCode();

            if (code >= 200 && code < 300) {
                // Success — mark as delivered, no more retries
                deliveredKeys.add(deliveryKey);
                store.incrementWebhookDeliveries();
            } else if (code >= 500) {
                // Server error — retryable
                scheduleRetry(url, payload, deliveryKey, start, attempt);
            }
            // 4xx = permanent failure, do not retry (silently drop)

        } catch (Exception e) {
            // Network error, timeout, connection refused — retryable
            scheduleRetry(url, payload, deliveryKey, start, attempt);
        }
    }

    private void scheduleRetry(String url, Map<String, Object> payload, String deliveryKey, Instant start, int attempt) {
        if (deliveredKeys.contains(deliveryKey)) return;

        long elapsedSeconds = Duration.between(start, Instant.now()).getSeconds();
        long remainingSeconds = DELIVERY_DEADLINE_SECONDS - elapsedSeconds;
        if (remainingSeconds <= 0) return;

        // Exponential backoff: 1s, 2s, 4s, 8s, 16s, capped at 30s
        long delayMs = Math.min((long) Math.pow(2, attempt) * 1000L, 30_000L);
        long delaySeconds = Math.max(1, delayMs / 1000);
        delaySeconds = Math.min(delaySeconds, remainingSeconds);
        if (delaySeconds <= 0) return;

        retryScheduler.schedule(
            () -> deliverWithRetry(url, payload, deliveryKey, start, attempt + 1),
            delaySeconds,
            TimeUnit.SECONDS
        );
    }

    private boolean isDeadlineExceeded(Instant start) {
        return Duration.between(start, Instant.now()).getSeconds() >= DELIVERY_DEADLINE_SECONDS;
    }

    private ExecutorService getReceiverExecutor(String receiverKey) {
        return receiverExecutors.computeIfAbsent(receiverKey, key -> Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "webhook-" + key);
            t.setDaemon(true);
            return t;
        }));
    }

    // ============================================================
    // Payload builders — snapshot ALL values eagerly at build time
    // ============================================================

    private Map<String, Object> buildFiredPayload(Alert alert) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("event", "alert.fired");
        payload.put("alert_id", alert.getAlertId());
        payload.put("status", "active");
        payload.put("fired_at", formatInstant(alert.getFiredAt()));
        payload.put("failure_rate", alert.getFailureRate());
        payload.put("total_proxies", alert.getTotalProxies());
        payload.put("failed_proxies", alert.getFailedProxies());
        payload.put("failed_proxy_ids", new ArrayList<>(alert.getFailedProxyIds()));
        payload.put("threshold", alert.getThreshold());
        payload.put("message", alert.getMessage());
        return payload;
    }

    private Map<String, Object> buildResolvedPayload(Alert alert) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("event", "alert.resolved");
        payload.put("alert_id", alert.getAlertId());
        payload.put("status", "resolved");
        Instant resolvedAt = alert.getResolvedAt() != null ? alert.getResolvedAt() : Instant.now();
        payload.put("resolved_at", formatInstant(resolvedAt));
        payload.put("fired_at", formatInstant(alert.getFiredAt()));
        payload.put("failure_rate", alert.getFailureRate());
        payload.put("total_proxies", alert.getTotalProxies());
        payload.put("failed_proxies", alert.getFailedProxies());
        payload.put("failed_proxy_ids", new ArrayList<>(alert.getFailedProxyIds()));
        payload.put("threshold", alert.getThreshold());
        payload.put("message", alert.getMessage());
        return payload;
    }

    private Map<String, Object> buildSlackFiredPayload(Alert alert, Integration integration) {
        List<Map<String, Object>> fields = new ArrayList<>();
        fields.add(field("Alert ID", alert.getAlertId()));
        fields.add(field("Failure Rate", String.format("%.2f (%.0f%%)", alert.getFailureRate(), alert.getFailureRate() * 100)));
        fields.add(field("Failed Proxies", String.valueOf(alert.getFailedProxies())));
        fields.add(field("Threshold", String.valueOf(alert.getThreshold())));
        fields.add(field("Failed IDs", String.join(", ", alert.getFailedProxyIds())));
        fields.add(field("Fired At", formatInstant(alert.getFiredAt())));

        Map<String, Object> attachment = new LinkedHashMap<>();
        attachment.put("color", "#FF0000");
        attachment.put("fields", fields);
        attachment.put("footer", "ProxyMaze'26 — Torch Labs");
        attachment.put("ts", alert.getFiredAt().getEpochSecond());

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("username", integration.getUsername() != null ? integration.getUsername() : "ProxyWatch");
        payload.put("text", "\uD83D\uDEA8 *ALERT FIRED* — Proxy pool failure rate exceeded threshold!");
        payload.put("attachments", List.of(attachment));
        return payload;
    }

    private Map<String, Object> buildSlackResolvedPayload(Alert alert, Integration integration) {
        List<Map<String, Object>> fields = new ArrayList<>();
        fields.add(field("Alert ID", alert.getAlertId()));
        fields.add(field("Failure Rate", String.format("%.2f", alert.getFailureRate())));
        fields.add(field("Failed Proxies", String.valueOf(alert.getFailedProxies())));
        fields.add(field("Threshold", String.valueOf(alert.getThreshold())));
        fields.add(field("Failed IDs", alert.getFailedProxyIds().isEmpty() ? "none" : String.join(", ", alert.getFailedProxyIds())));
        fields.add(field("Fired At", formatInstant(alert.getFiredAt())));

        Instant ts = alert.getResolvedAt() != null ? alert.getResolvedAt() : Instant.now();
        Map<String, Object> attachment = new LinkedHashMap<>();
        attachment.put("color", "#00FF00");
        attachment.put("fields", fields);
        attachment.put("footer", "ProxyMaze'26 — Torch Labs");
        attachment.put("ts", ts.getEpochSecond());

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("username", integration.getUsername() != null ? integration.getUsername() : "ProxyWatch");
        payload.put("text", "\u2705 *ALERT RESOLVED* — Proxy pool is healthy again.");
        payload.put("attachments", List.of(attachment));
        return payload;
    }

    private Map<String, Object> buildDiscordFiredPayload(Alert alert) {
        List<Map<String, Object>> fields = new ArrayList<>();
        fields.add(discordField("Alert ID", alert.getAlertId()));
        fields.add(discordField("Failure Rate", String.format("%.2f", alert.getFailureRate())));
        fields.add(discordField("Failed Proxies", String.valueOf(alert.getFailedProxies())));
        fields.add(discordField("Threshold", String.valueOf(alert.getThreshold())));
        fields.add(discordField("Failed IDs", String.join(", ", alert.getFailedProxyIds())));

        Map<String, Object> embed = new LinkedHashMap<>();
        embed.put("title", "\uD83D\uDEA8 Alert Fired — Proxy Pool Degraded");
        embed.put("description", "Proxy pool failure rate has exceeded the threshold of " + alert.getThreshold() + ". Immediate attention required.");
        embed.put("color", 16711680);
        embed.put("fields", fields);

        Map<String, Object> footer = new HashMap<>();
        footer.put("text", "ProxyMaze'26 — Torch Labs Monitoring");
        embed.put("footer", footer);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("embeds", List.of(embed));
        return payload;
    }

    private Map<String, Object> buildDiscordResolvedPayload(Alert alert) {
        List<Map<String, Object>> fields = new ArrayList<>();
        fields.add(discordField("Alert ID", alert.getAlertId()));
        fields.add(discordField("Failure Rate", String.format("%.2f", alert.getFailureRate())));
        fields.add(discordField("Failed Proxies", String.valueOf(alert.getFailedProxies())));
        fields.add(discordField("Threshold", String.valueOf(alert.getThreshold())));
        fields.add(discordField("Failed IDs", alert.getFailedProxyIds().isEmpty() ? "none" : String.join(", ", alert.getFailedProxyIds())));

        Map<String, Object> embed = new LinkedHashMap<>();
        embed.put("title", "\u2705 Alert Resolved — Proxy Pool Healthy");
        embed.put("description", "Proxy pool failure rate has dropped below threshold. System is healthy.");
        embed.put("color", 65280);
        embed.put("fields", fields);

        Map<String, Object> footer = new HashMap<>();
        footer.put("text", "ProxyMaze'26 — Torch Labs Monitoring");
        embed.put("footer", footer);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("embeds", List.of(embed));
        return payload;
    }

    private Map<String, Object> field(String title, String value) {
        Map<String, Object> f = new LinkedHashMap<>();
        f.put("title", title);
        f.put("value", value);
        return f;
    }

    private Map<String, Object> discordField(String name, String value) {
        Map<String, Object> f = new LinkedHashMap<>();
        f.put("name", name);
        f.put("value", value);
        f.put("inline", true);
        return f;
    }

    private String formatInstant(Instant instant) {
        return ISO_INSTANT.format(instant);
    }
}
