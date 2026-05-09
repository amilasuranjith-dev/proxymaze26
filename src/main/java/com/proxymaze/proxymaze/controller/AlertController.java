package com.proxymaze.proxymaze.controller;

import com.proxymaze.proxymaze.model.Alert;
import com.proxymaze.proxymaze.store.DataStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
public class AlertController {

    private final DataStore store;

    @Autowired
    public AlertController(DataStore store) {
        this.store = store;
    }

    @GetMapping("/alerts")
    public ResponseEntity<List<Map<String, Object>>> getAllAlerts() {
        List<Map<String, Object>> result = new ArrayList<>();
        for (Alert alert : store.getAllAlerts()) {
            result.add(buildAlertResponse(alert));
        }
        return ResponseEntity.ok(result);
    }

    private Map<String, Object> buildAlertResponse(Alert alert) {
        Map<String, Object> msg = new LinkedHashMap<>();
        msg.put("alert_id", alert.getAlertId());
        msg.put("status", alert.getStatus());
        msg.put("failure_rate", alert.getFailureRate());
        msg.put("total_proxies", alert.getTotalProxies());
        msg.put("failed_proxies", alert.getFailedProxies());
        msg.put("failed_proxy_ids", alert.getFailedProxyIds());
        msg.put("threshold", alert.getThreshold());
        msg.put("fired_at", alert.getFiredAt().toString());
        msg.put("resolved_at", alert.getResolvedAt() != null
            ? alert.getResolvedAt().toString() : null);
        msg.put("message", alert.getMessage());
        return msg;
    }
}

