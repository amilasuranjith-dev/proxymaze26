package com.proxymaze.proxymaze.model;

import java.util.List;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Integration {
    @JsonProperty("integration_id")
    private String integrationId;

    @JsonProperty("type")
    private String type;

    @JsonProperty("webhook_url")
    private String webhookUrl;

    @JsonProperty("username")
    private String username;

    @JsonProperty("events")
    private List<String> events;
}

