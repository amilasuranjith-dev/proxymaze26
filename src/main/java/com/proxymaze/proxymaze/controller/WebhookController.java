package com.proxymaze.proxymaze.controller;

import com.proxymaze.proxymaze.dto.WebhookRequest;
import com.proxymaze.proxymaze.model.WebhookRegistration;
import com.proxymaze.proxymaze.store.DataStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

@RestController
public class WebhookController {

    private final DataStore store;

    @Autowired
    public WebhookController(DataStore store) {
        this.store = store;
    }

    //POST /webhooks — Register a webhook receiver URL.
    //Additional fields in request body must be silently ignored.
    @PostMapping("/webhooks")
    public ResponseEntity<Map<String, Object>> registerWebhook(@RequestBody WebhookRequest request) {
        String webhookId = "wh-" + UUID.randomUUID().toString().substring(0, 8);
        WebhookRegistration registration = new WebhookRegistration(webhookId, request.getUrl());
        store.addWebhook(registration);

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("webhook_id", webhookId);
        body.put("url", request.getUrl());

        return ResponseEntity.status(HttpStatus.CREATED).body(body);
    }
}

