package com.proxymaze.proxymaze.controller;

import com.proxymaze.proxymaze.store.DataStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;

@RestController
public class MetricsController {

    private final DataStore store;

    @Autowired
    public MetricsController(DataStore store) {
        this.store = store;
    }

    //GET /metrics — Operational stats.
    @GetMapping("/metrics")
    public ResponseEntity<Map<String, Object>> getMetrics() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("total_checks", store.getTotalChecks());
        body.put("current_pool_size", store.getPoolSize());
        body.put("active_alerts", store.getActiveAlertsCount());
        body.put("total_alerts", store.getTotalAlerts());
        body.put("webhook_deliveries", store.getWebhookDeliveries());
        return ResponseEntity.ok(body);
    }
}

