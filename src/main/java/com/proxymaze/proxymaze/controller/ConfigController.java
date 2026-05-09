package com.proxymaze.proxymaze.controller;

import com.proxymaze.proxymaze.dto.ConfigRequest;
import com.proxymaze.proxymaze.model.MonitoringConfigData;
import com.proxymaze.proxymaze.service.MonitoringService;
import com.proxymaze.proxymaze.store.DataStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.LinkedHashMap;
import java.util.Map;

@RestController
public class ConfigController {

    private final DataStore store;
    private final MonitoringService monitoringService;

    @Autowired
    public ConfigController(DataStore store, MonitoringService monitoringService) {
        this.store = store;
        this.monitoringService = monitoringService;
    }

    //monitoring interval and timeout
    @PostMapping("/config")
    public ResponseEntity<Map<String, Object>> setConfig(@RequestBody ConfigRequest request) {
        store.updateConfig(request.getCheckIntervalSeconds(), request.getRequestTimeoutMs());

        // Reschedule the monitoring loop with the new interval
        monitoringService.reschedule(request.getCheckIntervalSeconds());

        return ResponseEntity.ok(buildConfigResponse());
    }

    //Return current configuration.
    @GetMapping("/config")
    public ResponseEntity<Map<String, Object>> getConfig() {
        return ResponseEntity.ok(buildConfigResponse());
    }

    private Map<String, Object> buildConfigResponse() {
        MonitoringConfigData config = store.getConfig();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("check_interval_seconds", config.getCheckIntervalSeconds());
        body.put("request_timeout_ms", config.getRequestTimeoutMs());
        return body;
    }
}

