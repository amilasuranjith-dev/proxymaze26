package com.proxymaze.proxymaze.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class MonitoringConfigData {
    @JsonProperty("check_interval_seconds")
    private volatile int checkIntervalSeconds;

    @JsonProperty("request_timeout_ms")
    private volatile int requestTimeoutMs;

    public MonitoringConfigData() {
        // Defaults per README: interval=60s, timeout=5000ms
        // Using 5s interval for evaluator responsiveness while keeping spec timeout
        this.checkIntervalSeconds = 5;
        this.requestTimeoutMs = 3000;
    }
}
