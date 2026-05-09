package com.proxymaze.proxymaze.service;

import com.proxymaze.proxymaze.model.Alert;
import com.proxymaze.proxymaze.store.DataStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

// State machine:
//   No active alert  + rate >= 0.20  →  fire new alert
//   Active alert     + rate <  0.20  →  resolve alert
//   Active alert     + rate >= 0.20  →  update live metrics (no new alert, no duplicate webhook)
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

    public synchronized void evaluate() {
        double failureRate = store.calculateFailureRate();
        List<String> downIds = store.getDownProxyIds();
        int downCount = downIds.size();
        int totalProxies = store.getPoolSize();

        Alert activeAlert = store.getActiveAlert();

        if (activeAlert == null) {
            if (failureRate >= THRESHOLD) {
                String alertId = "alert-" + UUID.randomUUID().toString().substring(0, 8);
                Alert newAlert = new Alert(alertId, failureRate, totalProxies, downCount, downIds, Instant.now());
                store.addAlert(newAlert);
                store.setActiveAlert(newAlert);
                webhookDeliveryService.deliverAlertFired(newAlert);
            }
        } else {
            if (failureRate < THRESHOLD) {
                activeAlert.updateActiveState(failureRate, downCount);
                activeAlert.resolve(Instant.now());
                store.setActiveAlert(null);
                webhookDeliveryService.deliverAlertResolved(activeAlert);
            } else {
                // Still breaching — update live metrics (rate, count) on the active alert.
                // failed_proxy_ids is NOT updated: it remains frozen as the fire-time snapshot.
                activeAlert.updateActiveState(failureRate, downCount);
            }
        }
    }
}
