package com.proxymaze.proxymaze.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConfigRequest {
    @JsonProperty("check_interval_seconds")
    private int checkIntervalSeconds;

    @JsonProperty("request_timeout_ms")
    private int requestTimeoutMs;
}

