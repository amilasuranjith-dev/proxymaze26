package com.proxymaze.proxymaze.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class WebhookRegistration {
    @JsonProperty("webhook_id")
    private String webhookId;

    @JsonProperty("url")
    private String url;
}

