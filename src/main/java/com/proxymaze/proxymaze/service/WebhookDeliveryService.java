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
import java.util.*;
import java.util.concurrent.*;

// Webhook delivery service handles async delivery of alerts to webhooks.
@Service
public class WebhookDeliveryService {

    private final DataStore store;
    private final ObjectMapper objectMapper;
    private final ExecutorService deliveryPool = Executors.newCachedThreadPool();
    private final ScheduledExecutorService retryScheduler =
        Executors.newSingleThreadScheduledExecutor();

    private final HttpClient httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .build();

    //successfully delivered. Prevents duplicate deliveries.
    private final Set<String> deliveredKeys =
        Collections.newSetFromMap(new ConcurrentHashMap<>());

    @Autowired
    public WebhookDeliveryService(DataStore store, ObjectMapper objectMapper) {
        this.store = store;
        this.objectMapper = objectMapper;
    }

    @PreDestroy
    public void shutdown() {
        deliveryPool.shutdown();
        retryScheduler.shutdown();
    }

    // ---------------------------------------------------------------
    // alert.fired
    // ---------------------------------------------------------------

    public void deliverAlertFired(Alert alert) {
        Map<String, Object> payload = buildFiredPayload(alert);

        // Generic webhooks
        for (WebhookRegistration wh : store.getAllWebhooks()) {
            String key = alert.getAlertId() + ":fired:" + wh.getWebhookId();
            deliveryPool.submit(() ->
                deliverWithRetry(wh.getUrl(), payload, key, 0)
            );
        }

        // Slack/Discord integrations
        for (Integration integration : store.getAllIntegrations()) {
            if (integration.getEvents().contains("alert.fired")) {
                String key = alert.getAlertId() + ":fired:integration:" + integration.getIntegrationId();
                if ("slack".equals(integration.getType())) {
                    Map<String, Object> slackPayload = buildSlackFiredPayload(alert, integration);
                    deliveryPool.submit(() ->
                        deliverWithRetry(integration.getWebhookUrl(), slackPayload, key, 0)
                    );
                } else if ("discord".equals(integration.getType())) {
                    Map<String, Object> discordPayload = buildDiscordFiredPayload(alert);
                    deliveryPool.submit(() ->
                        deliverWithRetry(integration.getWebhookUrl(), discordPayload, key, 0)
                    );
                }
            }
        }
    }

    // ---------------------------------------------------------------
    // alert.resolved
    // ---------------------------------------------------------------

    public void deliverAlertResolved(Alert alert) {
        Map<String, Object> payload = buildResolvedPayload(alert);

        // Generic webhooks
        for (WebhookRegistration wh : store.getAllWebhooks()) {
            String key = alert.getAlertId() + ":resolved:" + wh.getWebhookId();
            deliveryPool.submit(() ->
                deliverWithRetry(wh.getUrl(), payload, key, 0)
            );
        }

        // Slack/Discord integrations
        for (Integration integration : store.getAllIntegrations()) {
            if (integration.getEvents().contains("alert.resolved")) {
                String key = alert.getAlertId() + ":resolved:integration:" + integration.getIntegrationId();
                if ("slack".equals(integration.getType())) {
                    Map<String, Object> slackPayload = buildSlackResolvedPayload(alert, integration);
                    deliveryPool.submit(() ->
                        deliverWithRetry(integration.getWebhookUrl(), slackPayload, key, 0)
                    );
                } else if ("discord".equals(integration.getType())) {
                    Map<String, Object> discordPayload = buildDiscordResolvedPayload(alert);
                    deliveryPool.submit(() ->
                        deliverWithRetry(integration.getWebhookUrl(), discordPayload, key, 0)
                    );
                }
            }
        }
    }

    // ---------------------------------------------------------------
    // Core retry-aware delivery
    // ---------------------------------------------------------------

    private void deliverWithRetry(String url, Map<String, Object> payload,
                                   String deliveryKey, int attempt) {
        // Already successfully delivered — stop
        if (deliveredKeys.contains(deliveryKey)) return;

        try {
            String json = objectMapper.writeValueAsString(payload);
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build();

            HttpResponse<String> response = httpClient.send(
                request, HttpResponse.BodyHandlers.ofString()
            );

            int code = response.statusCode();

            if (code >= 200 && code < 300) {
                // SUCCESS — mark as delivered, never send again
                deliveredKeys.add(deliveryKey);
                store.incrementWebhookDeliveries();

            } else if (code == 500 || code == 502 || code == 503 || code == 504) {
                // Transient failure — schedule retry with backoff
                scheduleRetry(url, payload, deliveryKey, attempt);

            }
            // Other errors (400, 401, etc.) — don't retry per spec

        } catch (Exception e) {
            // Connection error — also retry
            scheduleRetry(url, payload, deliveryKey, attempt);
        }
    }

    private void scheduleRetry(String url, Map<String, Object> payload,
                                String deliveryKey, int attempt) {
        // Exponential backoff: 2s, 4s, 8s... capped at 30s
        long delaySeconds = Math.min(2L * (long) Math.pow(2, attempt), 30L);
        retryScheduler.schedule(
            () -> deliverWithRetry(url, payload, deliveryKey, attempt + 1),
            delaySeconds,
            TimeUnit.SECONDS
        );
    }

    // ---------------------------------------------------------------
    // Payload Builders
    // ---------------------------------------------------------------

    private Map<String, Object> buildFiredPayload(Alert alert) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("event", "alert.fired");
        payload.put("alert_id", alert.getAlertId());
        payload.put("fired_at", alert.getFiredAt().toString());
        payload.put("failure_rate", alert.getFailureRate());
        payload.put("total_proxies", alert.getTotalProxies());
        payload.put("failed_proxies", alert.getFailedProxies());
        payload.put("failed_proxy_ids", alert.getFailedProxyIds());
        payload.put("threshold", alert.getThreshold());
        payload.put("message", alert.getMessage());
        return payload;
    }

    private Map<String, Object> buildResolvedPayload(Alert alert) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("event", "alert.resolved");
        payload.put("alert_id", alert.getAlertId());
        payload.put("resolved_at", alert.getResolvedAt().toString());
        return payload;
    }

    // ---------------------------------------------------------------
    // Slack Payload Builder
    // ---------------------------------------------------------------

    private Map<String, Object> buildSlackFiredPayload(Alert alert, Integration integration) {
        List<Map<String, Object>> fields = new ArrayList<>();
        fields.add(field("Alert ID", alert.getAlertId()));
        fields.add(field("Failure Rate", String.format("%.2f (%.0f%%)",
            alert.getFailureRate(), alert.getFailureRate() * 100)));
        fields.add(field("Failed Proxies", String.valueOf(alert.getFailedProxies())));
        fields.add(field("Threshold", String.valueOf(alert.getThreshold())));
        fields.add(field("Failed IDs", String.join(", ", alert.getFailedProxyIds())));
        fields.add(field("Fired At", alert.getFiredAt().toString()));

        Map<String, Object> attachment = new LinkedHashMap<>();
        attachment.put("color", "#FF0000");  // Red for alert fired
        attachment.put("fields", fields);
        attachment.put("footer", "ProxyMaze'26 — Torch Labs");
        attachment.put("ts", alert.getFiredAt().getEpochSecond()); // integer, not float/string

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("username", integration.getUsername() != null ? integration.getUsername() : "ProxyWatch");
        payload.put("text", "🚨 *ALERT FIRED* — Proxy pool failure rate exceeded threshold!");
        payload.put("attachments", List.of(attachment));
        return payload;
    }

    private Map<String, Object> buildSlackResolvedPayload(Alert alert, Integration integration) {
        List<Map<String, Object>> fields = new ArrayList<>();
        fields.add(field("Alert ID", alert.getAlertId()));
        fields.add(field("Failure Rate", String.format("%.2f", alert.getFailureRate())));
        fields.add(field("Failed Proxies", String.valueOf(alert.getFailedProxies())));
        fields.add(field("Threshold", String.valueOf(alert.getThreshold())));
        fields.add(field("Failed IDs", alert.getFailedProxyIds().isEmpty()
            ? "none" : String.join(", ", alert.getFailedProxyIds())));
        fields.add(field("Fired At", alert.getFiredAt().toString()));

        Instant ts = alert.getResolvedAt() != null ? alert.getResolvedAt() : Instant.now();

        Map<String, Object> attachment = new LinkedHashMap<>();
        attachment.put("color", "#00FF00");  // Green for resolved
        attachment.put("fields", fields);
        attachment.put("footer", "ProxyMaze'26 — Torch Labs");
        attachment.put("ts", ts.getEpochSecond()); // integer

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("username", integration.getUsername() != null ? integration.getUsername() : "ProxyWatch");
        payload.put("text", "✅ *ALERT RESOLVED* — Proxy pool is healthy again.");
        payload.put("attachments", List.of(attachment));
        return payload;
    }

    // ---------------------------------------------------------------
    // Discord Payload Builder
    // ---------------------------------------------------------------

    private Map<String, Object> buildDiscordFiredPayload(Alert alert) {
        List<Map<String, Object>> fields = new ArrayList<>();
        fields.add(discordField("Alert ID", alert.getAlertId()));
        fields.add(discordField("Failure Rate", String.format("%.2f", alert.getFailureRate())));
        fields.add(discordField("Failed Proxies", String.valueOf(alert.getFailedProxies())));
        fields.add(discordField("Threshold", String.valueOf(alert.getThreshold())));
        fields.add(discordField("Failed IDs", String.join(", ", alert.getFailedProxyIds())));

        Map<String, Object> embed = new LinkedHashMap<>();
        embed.put("title", "🚨 Alert Fired — Proxy Pool Degraded");
        embed.put("description", "Proxy pool failure rate has exceeded the threshold of "
            + alert.getThreshold() + ". Immediate attention required.");
        embed.put("color", 16711680); // Red: 0xFF0000 = 16711680 (integer, 0-16777215)
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
        fields.add(discordField("Failed IDs", alert.getFailedProxyIds().isEmpty()
            ? "none" : String.join(", ", alert.getFailedProxyIds())));

        Map<String, Object> embed = new LinkedHashMap<>();
        embed.put("title", "✅ Alert Resolved — Proxy Pool Healthy");
        embed.put("description", "Proxy pool failure rate has dropped below threshold. System is healthy.");
        embed.put("color", 65280); // Green: 0x00FF00 = 65280
        embed.put("fields", fields);

        Map<String, Object> footer = new HashMap<>();
        footer.put("text", "ProxyMaze'26 — Torch Labs Monitoring");
        embed.put("footer", footer);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("embeds", List.of(embed));
        return payload;
    }

    // ---------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------

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
}

