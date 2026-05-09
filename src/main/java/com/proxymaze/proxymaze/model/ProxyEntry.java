package com.proxymaze.proxymaze.model;

import lombok.Getter;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Getter
public class ProxyEntry {
    private final String id;
    private final String url;
    private volatile String status;
    private volatile Instant lastCheckedAt;
    private volatile int consecutiveFailures;
    private volatile int totalChecks;
    private final List<CheckRecord> history = new ArrayList<>();

    public ProxyEntry(String id, String url) {
        this.id = id;
        this.url = url;
        this.status = "pending";
    }

    public synchronized void recordCheck(String checkStatus, Instant checkedAt) {
        this.status = checkStatus;
        this.lastCheckedAt = checkedAt;
        this.totalChecks++;
        if ("down".equals(checkStatus)) {
            this.consecutiveFailures++;
        } else {
            this.consecutiveFailures = 0;
        }
        this.history.add(new CheckRecord(checkedAt, checkStatus));
    }

    public synchronized double getUptimePercentage() {
        if (totalChecks == 0) return 0.0;
        long upCount = history.stream().filter(r -> "up".equals(r.getStatus())).count();
        return Math.round((upCount * 100.0 / totalChecks) * 10.0) / 10.0;
    }

    public synchronized List<CheckRecord> getHistoryCopy() {
        return new ArrayList<>(history);
    }
}
