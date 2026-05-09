package com.proxymaze.proxymaze.controller;

import com.proxymaze.proxymaze.dto.IntegrationRequest;
import com.proxymaze.proxymaze.model.Integration;
import com.proxymaze.proxymaze.store.DataStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

@RestController
public class IntegrationController {

    private final DataStore store;

    @Autowired
    public IntegrationController(DataStore store) {
        this.store = store;
    }

    //POST /integrations — Register a Slack or Discord integration.
    // Response: 200 OK or 201 Created (use 201)
    @PostMapping("/integrations")
    public ResponseEntity<Map<String, Object>> registerIntegration(
            @RequestBody IntegrationRequest request) {

        String integrationId = "int-" + UUID.randomUUID().toString().substring(0, 8);
        Integration integration = new Integration(
            integrationId,
            request.getType(),
            request.getWebhookUrl(),
            request.getUsername(),
            request.getEvents()
        );
        store.addIntegration(integration);

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("integration_id", integrationId);
        body.put("type", request.getType());
        body.put("webhook_url", request.getWebhookUrl());

        return ResponseEntity.status(HttpStatus.CREATED).body(body);
    }
}

