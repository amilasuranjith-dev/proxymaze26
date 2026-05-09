package com.proxymaze.proxymaze.controller;

import com.proxymaze.proxymaze.dto.ProxyPoolRequest;
import com.proxymaze.proxymaze.model.CheckRecord;
import com.proxymaze.proxymaze.model.ProxyEntry;
import com.proxymaze.proxymaze.service.MonitoringService;
import com.proxymaze.proxymaze.store.DataStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.*;

@RestController
public class ProxyController {

    private static final DateTimeFormatter ISO_INSTANT = DateTimeFormatter.ISO_INSTANT;

    private final DataStore store;
    private final MonitoringService monitoringService;

    @Autowired
    public ProxyController(DataStore store, MonitoringService monitoringService) {
        this.store = store;
        this.monitoringService = monitoringService;
    }

    // Add proxies to the pool.
    //replace: true  > clear current pool first, then add
    //replace: false > append to existing pool
    // Proxies start as "pending" and transition via background probes
    @PostMapping("/proxies")
    public ResponseEntity<Map<String, Object>> addProxies(@RequestBody ProxyPoolRequest request) {
        List<ProxyEntry> added = store.addProxies(request.getProxies(), request.isReplace());

        // Trigger an immediate check so proxies don't stay "pending" for long
        monitoringService.triggerImmediateCheck();

        List<Map<String, Object>> proxyList = new ArrayList<>();
        for (ProxyEntry entry : added) {
            proxyList.add(buildProxySummary(entry));
        }

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("accepted", added.size());
        body.put("proxies", proxyList);

        return ResponseEntity.status(HttpStatus.CREATED).body(body);
    }


    //GET /proxies > Full pool summary with per proxy state
    //Values MUST reflect background checks, not trigger new checks
    @GetMapping("/proxies")
    public ResponseEntity<Map<String, Object>> getAllProxies() {
        List<ProxyEntry> proxies = store.getAllProxies();
        int total = proxies.size();
        long upCount = proxies.stream().filter(p -> "up".equals(p.getStatus())).count();
        long downCount = proxies.stream().filter(p -> "down".equals(p.getStatus())).count();
        double failureRate = total > 0 ? (double) downCount / total : 0.0;

        List<Map<String, Object>> proxyList = new ArrayList<>();
        for (ProxyEntry p : proxies) {
            proxyList.add(buildProxySummary(p));
        }

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("total", total);
        body.put("up", (int) upCount);
        body.put("down", (int) downCount);
        body.put("failure_rate", failureRate);
        body.put("proxies", proxyList);

        return ResponseEntity.ok(body);
    }

    //GET /proxies/{id} — Full details for one proxy
    //Returns 404 for unknown IDs
    @GetMapping("/proxies/{id}")
    public ResponseEntity<?> getProxy(@PathVariable String id) {
        return store.getProxy(id)
            .map(p -> ResponseEntity.ok(buildProxyDetail(p)))
            .orElse(ResponseEntity.notFound().build());
    }

    //GET /proxies/{id}/history — Check history as JSON array.
    //Returns 404 for unknown IDs
    @GetMapping("/proxies/{id}/history")
    public ResponseEntity<?> getProxyHistory(@PathVariable String id) {
        return store.getProxy(id)
            .map(p -> {
                List<Map<String, Object>> history = new ArrayList<>();
                for (CheckRecord record : p.getHistoryCopy()) {
                    Map<String, Object> entry = new LinkedHashMap<>();
                    entry.put("checked_at", formatInstant(record.getCheckedAt()));
                    entry.put("status", record.getStatus());
                    history.add(entry);
                }
                return ResponseEntity.ok((Object) history);
            })
            .orElse(ResponseEntity.notFound().build());
    }

    //DELETE /proxies — Clear the proxy pool
    //CRITICAL: Alerts must NOT be deleted. Returns 204
    @DeleteMapping("/proxies")
    public ResponseEntity<Void> deleteProxies() {
        store.clearPool();
        // NOTE: alerts are NOT cleared — they persist in the DataStore
        return ResponseEntity.noContent().build();
    }

    // ---------------------------------------------------------------
    // Response Builders
    // ---------------------------------------------------------------

    private Map<String, Object> buildProxySummary(ProxyEntry p) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", p.getId());
        m.put("url", p.getUrl());
        m.put("status", p.getStatus());
        m.put("last_checked_at", p.getLastCheckedAt() != null
            ? formatInstant(p.getLastCheckedAt()) : null);
        m.put("consecutive_failures", p.getConsecutiveFailures());
        return m;
    }

    private Map<String, Object> buildProxyDetail(ProxyEntry p) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", p.getId());
        m.put("url", p.getUrl());
        m.put("status", p.getStatus());
        m.put("last_checked_at", p.getLastCheckedAt() != null
            ? formatInstant(p.getLastCheckedAt()) : null);
        m.put("consecutive_failures", p.getConsecutiveFailures());
        m.put("total_checks", p.getTotalChecks());
        m.put("uptime_percentage", p.getUptimePercentage());

        List<Map<String, Object>> history = new ArrayList<>();
        for (CheckRecord record : p.getHistoryCopy()) {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("checked_at", formatInstant(record.getCheckedAt()));
            entry.put("status", record.getStatus());
            history.add(entry);
        }
        m.put("history", history);
        return m;
    }

    private String formatInstant(Instant instant) {
        return ISO_INSTANT.format(instant);
    }
}

