package com.proxymaze.proxymaze.store;

import com.proxymaze.proxymaze.model.*;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Component
public class DataStore {

    private final MonitoringConfigData config = new MonitoringConfigData();

    private final Map<String, ProxyEntry> proxyPool =
        Collections.synchronizedMap(new LinkedHashMap<>());

    private final List<Alert> alerts = new CopyOnWriteArrayList<>();
    private volatile Alert activeAlert = null;

    private final List<WebhookRegistration> webhooks = new CopyOnWriteArrayList<>();
    private final List<Integration> integrations = new CopyOnWriteArrayList<>();

    private final AtomicLong totalChecks       = new AtomicLong(0);
    private final AtomicLong totalAlerts       = new AtomicLong(0);
    private final AtomicLong webhookDeliveries = new AtomicLong(0);

    public MonitoringConfigData getConfig() { return config; }

    public void updateConfig(int intervalSeconds, int timeoutMs) {
        config.setCheckIntervalSeconds(intervalSeconds);
        config.setRequestTimeoutMs(timeoutMs);
    }

    // Derives a stable proxy ID from the last path segment of the URL.
    public static String extractId(String url) {
        if (url == null || url.isBlank()) return "";
        String trimmed = url.trim();
        while (trimmed.endsWith("/")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        int lastSlash = trimmed.lastIndexOf('/');
        return lastSlash >= 0 ? trimmed.substring(lastSlash + 1) : trimmed;
    }

    public List<ProxyEntry> addProxies(List<String> urls, boolean replace) {
        synchronized (proxyPool) {
            if (replace) proxyPool.clear();
            if (urls == null) return List.of();
            List<ProxyEntry> added = new ArrayList<>();
            for (String url : urls) {
                String id = extractId(url);
                ProxyEntry entry = new ProxyEntry(id, url);
                proxyPool.put(id, entry);
                added.add(entry);
            }
            return added;
        }
    }

    public void clearPool() {
        synchronized (proxyPool) { proxyPool.clear(); }
    }

    public List<ProxyEntry> getAllProxies() {
        synchronized (proxyPool) { return new ArrayList<>(proxyPool.values()); }
    }

    public Optional<ProxyEntry> getProxy(String id) {
        return Optional.ofNullable(proxyPool.get(id));
    }

    public int getPoolSize() { return proxyPool.size(); }

    public double calculateFailureRate() {
        synchronized (proxyPool) {
            int total = proxyPool.size();
            if (total == 0) return 0.0;
            long downCount = proxyPool.values().stream()
                .filter(p -> "down".equals(p.getStatus()))
                .count();
            return (double) downCount / total;
        }
    }

    public List<String> getDownProxyIds() {
        synchronized (proxyPool) {
            return proxyPool.values().stream()
                .filter(p -> "down".equals(p.getStatus()))
                .map(ProxyEntry::getId)
                .collect(Collectors.toList());
        }
    }

    public int getDownCount() {
        synchronized (proxyPool) {
            return (int) proxyPool.values().stream()
                .filter(p -> "down".equals(p.getStatus()))
                .count();
        }
    }

    public Alert getActiveAlert() { return activeAlert; }
    public void setActiveAlert(Alert alert) { this.activeAlert = alert; }

    public void addAlert(Alert alert) {
        alerts.add(alert);
        totalAlerts.incrementAndGet();
    }

    public List<Alert> getAllAlerts() { return new ArrayList<>(alerts); }

    public void addWebhook(WebhookRegistration wh) { webhooks.add(wh); }
    public List<WebhookRegistration> getAllWebhooks() { return new ArrayList<>(webhooks); }

    public void addIntegration(Integration integration) { integrations.add(integration); }
    public List<Integration> getAllIntegrations() { return new ArrayList<>(integrations); }

    public void incrementTotalChecks()       { totalChecks.incrementAndGet(); }
    public void incrementWebhookDeliveries() { webhookDeliveries.incrementAndGet(); }

    public long getTotalChecks()       { return totalChecks.get(); }
    public long getTotalAlerts()       { return totalAlerts.get(); }
    public long getWebhookDeliveries() { return webhookDeliveries.get(); }
    public int getActiveAlertsCount()  { return activeAlert != null ? 1 : 0; }
}
