package com.proxymaze.proxymaze.service;

import com.proxymaze.proxymaze.model.Alert;
import com.proxymaze.proxymaze.store.DataStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

//Alert service manages the complete alert lifecycle.
//State machine:
//     Normal (no active alert):
//    failure_rate >= 0.20 -> fire alert -> Active
//ctive (alert exists):
//      failure_rate < 0.20  -> resolve alert -> Resolved
//      failure_rate >= 0.20 -> update alert with current state (no new alert)
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

    //Evaluates current failure rate and updates alert state accordingly.
    public synchronized void evaluate() {
        double failureRate = store.calculateFailureRate();
        List<String> downIds = store.getDownProxyIds();
        int downCount = downIds.size();
        int totalProxies = store.getPoolSize();

        Alert activeAlert = store.getActiveAlert();

        if (activeAlert == null) {
            // No active alert — check if we need to fire one
            if (failureRate >= THRESHOLD) {
                String alertId = "alert-" + UUID.randomUUID().toString().substring(0, 8);
                Alert newAlert = new Alert(
                    alertId,
                    failureRate,
                    totalProxies,
                    downCount,
                    downIds,
                    Instant.now()
                );
                store.addAlert(newAlert);
                store.setActiveAlert(newAlert);

                // Deliver webhook events asynchronously
                webhookDeliveryService.deliverAlertFired(newAlert);
            }

        } else {
            // There is an active alert
            if (failureRate < THRESHOLD) {
                // Resolve the alert
                Instant resolvedAt = Instant.now();
                activeAlert.resolve(resolvedAt);
                store.setActiveAlert(null);

                // Deliver resolved event
                webhookDeliveryService.deliverAlertResolved(activeAlert);

            } else {
                // Still breaching — update live metrics (rate, count) on the active alert.
                // failed_proxy_ids is NOT updated: it remains frozen as the fire-time snapshot.
                activeAlert.updateActiveState(failureRate, downCount);
            }
        }
    }
}

