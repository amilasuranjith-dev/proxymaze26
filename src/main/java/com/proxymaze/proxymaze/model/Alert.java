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
    private volatile int totalProxies;

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
        this.message = "Proxy pool failure rate exceeded threshold";
    }

    //Update the active alert to reflect current down state during ongoing breach.
    //This ensures GET /proxies, GET /alerts, and webhook payloads all agree on metrics.
    public synchronized void updateActiveState(double newRate, int total,
                                               int failed, List<String> failedIds) {
        this.failureRate = newRate;
        this.totalProxies = total;
        this.failedProxies = failed;
        this.failedProxyIds = new ArrayList<>(failedIds);
    }

    //Resolve the alert when failure rate drops below threshold.
    public synchronized void resolve(Instant resolvedAt) {
        this.status = "resolved";
        this.resolvedAt = resolvedAt;
    }
}

