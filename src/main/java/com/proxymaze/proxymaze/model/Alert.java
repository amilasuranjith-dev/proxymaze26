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

    @JsonProperty("failure_rate")
    private volatile double failureRate;

    @JsonProperty("total_proxies")
    private final int totalProxies;

    @JsonProperty("failed_proxies")
    private volatile int failedProxies;

    // Immutable snapshot of the proxy IDs that were down at the moment of firing.
    // Per spec this list must never change for the lifetime of the alert.
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
        this.failedProxyIds = Collections.unmodifiableList(List.copyOf(failedProxyIds));
        this.firedAt = firedAt;
        this.resolvedAt = null;
        this.message = buildMessage(failedProxies, totalProxies, failureRate);
    }

    public synchronized void updateActiveState(double newRate, int failed) {
        this.failureRate = newRate;
        this.failedProxies = failed;
        this.message = buildMessage(failed, this.totalProxies, newRate);
    }

    public synchronized void resolve(Instant resolvedAt) {
        this.status = "resolved";
        this.resolvedAt = resolvedAt;
    }

    private String buildMessage(int failed, int total, double rate) {
        return String.format("Proxy pool failure rate exceeded threshold: %d/%d down (%.1f%%)",
            failed, total, rate * 100.0);
    }
}
