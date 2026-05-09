package com.proxymaze.proxymaze.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

@Getter
public class Alert {
    @JsonProperty("alert_id")
    private final String alertId;

    @JsonProperty("status")
    private volatile String status;

    // Live failure_rate — updated on each cycle while the alert is active
    @JsonProperty("failure_rate")
    private volatile double failureRate;

    @JsonProperty("total_proxies")
    private final int totalProxies;

    // Live count of currently-down proxies — updated on each cycle while active
    @JsonProperty("failed_proxies")
    private volatile int failedProxies;

    // IMMUTABLE fire-time snapshot — never mutated after construction.
    // Per spec: "the exact set of failed_proxy_ids that were down at the time of firing."
    @JsonProperty("failed_proxy_ids")
    private final List<String> failedProxyIds;

    @JsonProperty("threshold")
    private final double threshold = 0.2;

    @JsonProperty("fired_at")
    private final Instant firedAt;

    @JsonProperty("resolved_at")
    private volatile Instant resolvedAt;

    @JsonProperty("message")
    private volatile String message;

    public Alert(String alertId, double failureRate, int totalProxies,
                 int failedProxies, List<String> failedProxyIds, Instant firedAt) {
        this.alertId = alertId;
        this.status = "active";
        this.failureRate = failureRate;
        this.totalProxies = totalProxies;
        this.failedProxies = failedProxies;
        // Defensive immutable copy — this list is NEVER changed after this point
        this.failedProxyIds = Collections.unmodifiableList(List.copyOf(failedProxyIds));
        this.firedAt = firedAt;
        this.resolvedAt = null;
        this.message = buildMessage(failedProxies, totalProxies, failureRate);
    }

    /**
     * Updates live metrics (failure_rate, failed_proxies) for the duration of an
     * ongoing breach, so GET /proxies and GET /alerts show current health.
     * <p>
     * failed_proxy_ids is intentionally NOT updated here — the spec mandates it
     * must represent the exact set of proxies that were down at the time of firing.
     */
    public synchronized void updateActiveState(double newRate, int failed) {
        this.failureRate = newRate;
        this.failedProxies = failed;
        this.message = buildMessage(failed, this.totalProxies, newRate);
    }

    /** Resolve the alert when failure rate drops below threshold. */
    public synchronized void resolve(Instant resolvedAt) {
        this.status = "resolved";
        this.resolvedAt = resolvedAt;
    }

    private String buildMessage(int failed, int total, double rate) {
        return String.format("Proxy pool failure rate exceeded threshold: %d/%d down (%.1f%%)",
            failed, total, rate * 100.0);
    }
}
