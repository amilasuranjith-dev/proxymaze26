package com.proxymaze.proxymaze.service;

import com.proxymaze.proxymaze.model.Alert;
import com.proxymaze.proxymaze.store.DataStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Alert lifecycle state machine.
 *
 * States:
 *   NORMAL (no active alert)
 *     → rate >= 0.20 → fire new alert (ACTIVE)
 *   ACTIVE
 *     → rate < 0.20 → resolve alert (RESOLVED), clear activeAlert
 *     → rate >= 0.20 → update live metrics (no new alert, no webhook)
 *   RESOLVED
 *     → handled by NORMAL path since activeAlert is null after resolution
 *     → re-breach creates a FRESH alert with NEW ID
 *
 * Thread safety: synchronized on `this` — called exclusively from the monitoring cycle.
 * All metric reads use an atomic snapshot from DataStore to ensure consistency.
 */
@Service
public class AlertService {

    private static final double THRESHOLD = 0.20;

    private final DataStore store;
    private final WebhookDeliveryService webhookDeliveryService;

    @Autowired
    public AlertService(DataStore store, WebhookDeliveryService webhookDeliveryService) {
        this.store = store;
        this.webhookDeliveryService = webhookDeliveryService;
    }

    /**
     * Evaluate alert state based on an ATOMIC snapshot of proxy failure metrics.
     * This ensures /proxies, /alerts, and webhook payloads all agree.
     */
    public synchronized void evaluate() {
        // Take a single atomic snapshot — all decisions and payloads use this
        DataStore.FailureSnapshot snapshot = store.getFailureSnapshot();

        double failureRate = snapshot.failureRate();
        int downCount = snapshot.downCount();
        List<String> downIds = snapshot.downIds();
        int totalProxies = snapshot.totalProxies();

        Alert activeAlert = store.getActiveAlert();

        if (activeAlert == null) {
            // NORMAL state — check for breach
            if (totalProxies > 0 && failureRate >= THRESHOLD) {
                String alertId = store.nextAlertId();
                Instant now = Instant.now();
                Alert newAlert = new Alert(alertId, failureRate, totalProxies, downCount, downIds, now);
                store.addAlert(newAlert);
                store.setActiveAlert(newAlert);
                webhookDeliveryService.deliverAlertFired(newAlert);
            }
        } else {
            // ACTIVE state — check for resolution or update
            if (failureRate < THRESHOLD) {
                // Resolve: update metrics to current state, then resolve
                activeAlert.updateActiveState(failureRate, totalProxies, downCount, downIds);
                activeAlert.resolve(Instant.now());
                store.setActiveAlert(null);
                webhookDeliveryService.deliverAlertResolved(activeAlert);
            } else {
                // Still in breach — update live metrics, no new alert, no webhook
                activeAlert.updateActiveState(failureRate, totalProxies, downCount, downIds);
            }
        }
    }
}
