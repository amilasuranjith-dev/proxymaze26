package com.proxymaze.proxymaze.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Getter
public class Alert {
    @JsonProperty("alert_id")
    private final String alertId;

    @JsonProperty("status")
    private volatile String status;   // "active" or "resolved"

    @JsonProperty("failure_rate")
    private volatile double failureRate;

    @JsonProperty("total_proxies")
    private final int totalProxies;

    @JsonProperty("failed_proxies")
    private volatile int failedProxies;

    @JsonProperty("failed_proxy_ids")
    private volatile List<String> failedProxyIds;

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
        this.failedProxyIds = new ArrayList<>(failedProxyIds);
        this.firedAt = firedAt;
        this.resolvedAt = null;
        this.message = buildMessage(failedProxies, totalProxies, failureRate);
    }

    //Update the active alert to reflect current down state during ongoing breach.
    //Keep total_proxies as the fire-time snapshot per spec.
    public synchronized void updateActiveState(double newRate,
                                               int failed, List<String> failedIds) {
        this.failureRate = newRate;
        this.failedProxies = failed;
        this.failedProxyIds = new ArrayList<>(failedIds);
        this.message = buildMessage(failed, this.totalProxies, newRate);
    }

    //Resolve the alert when failure rate drops below threshold.
    public synchronized void resolve(Instant resolvedAt) {
        this.status = "resolved";
        this.resolvedAt = resolvedAt;
    }

    private String buildMessage(int failed, int total, double rate) {
        double pct = rate * 100.0;
        return String.format("Proxy pool failure rate exceeded threshold: %d/%d down (%.1f%%)",
            failed, total, pct);
    }
}
