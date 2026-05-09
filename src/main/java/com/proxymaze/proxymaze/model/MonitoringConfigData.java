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
        // Sensible defaults
        this.checkIntervalSeconds = 60;
        this.requestTimeoutMs = 5000;
    }
}

