package com.proxymaze.proxymaze;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.proxymaze.proxymaze.model.Alert;
import com.proxymaze.proxymaze.store.DataStore;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
class ProxymazeApplicationTests {

	@Autowired
	MockMvc mvc;

	@Autowired
	ObjectMapper om;

	@Autowired
	DataStore store;

	@Test
	void health_ok() throws Exception {
		mvc.perform(get("/health"))
			.andExpect(status().isOk())
			.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
			.andExpect(jsonPath("$.status").value("ok"));
	}

	@Test
	void config_roundTrip_and_ignores_unknown_fields() throws Exception {
		String body = "{\"check_interval_seconds\":15,\"request_timeout_ms\":3000,\"unknown\":123}";
		mvc.perform(post("/config")
				.contentType(MediaType.APPLICATION_JSON)
				.content(body))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.check_interval_seconds").value(15))
			.andExpect(jsonPath("$.request_timeout_ms").value(3000));

		mvc.perform(get("/config"))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.check_interval_seconds").value(15))
			.andExpect(jsonPath("$.request_timeout_ms").value(3000));
	}

	@Test
	void proxies_lifecycle_and_history_contract() throws Exception {
		// Start with empty pool
		mvc.perform(delete("/proxies")).andExpect(status().isNoContent());

		// Add proxies, replace=true, ignore extra fields
		String addBody = "{\"proxies\":[\"https://example.com/proxy/px-101\",\"https://example.com/proxy/px-102\"],\"replace\":true,\"extra\":\"x\"}";
		mvc.perform(post("/proxies").contentType(MediaType.APPLICATION_JSON).content(addBody))
			.andExpect(status().isCreated())
			.andExpect(jsonPath("$.accepted").value(2))
			.andExpect(jsonPath("$.proxies[0].id").exists())
			.andExpect(jsonPath("$.proxies[0].status").value("pending"));

		// GET /proxies structure
		mvc.perform(get("/proxies"))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.total").value(2))
			.andExpect(jsonPath("$.failure_rate").exists())
			.andExpect(jsonPath("$.proxies").isArray())
			// last_checked_at may be null until the first background probe completes
			.andExpect(jsonPath("$.proxies[0]").isMap())
			.andExpect(jsonPath("$.proxies[0].consecutive_failures").exists());

		// GET single proxy (should exist) and include required fields
		mvc.perform(get("/proxies/px-101"))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.id").value("px-101"))
			.andExpect(jsonPath("$.total_checks").exists())
			.andExpect(jsonPath("$.uptime_percentage").exists())
			.andExpect(jsonPath("$.history").isArray());

		// GET history endpoint returns array (may be empty until first check)
		mvc.perform(get("/proxies/px-101/history"))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$").isArray());

		// Unknown ID => 404
		mvc.perform(get("/proxies/does-not-exist")).andExpect(status().isNotFound());
		mvc.perform(get("/proxies/does-not-exist/history")).andExpect(status().isNotFound());
	}

	@Test
	void alerts_webhooks_integrations_and_metrics_contract() throws Exception {
		// webhooks accepts unknown fields
		mvc.perform(post("/webhooks")
				.contentType(MediaType.APPLICATION_JSON)
				.content("{\"url\":\"https://receiver.example/webhook\",\"x\":1}"))
			.andExpect(status().isCreated())
			.andExpect(jsonPath("$.webhook_id").exists())
			.andExpect(jsonPath("$.url").value("https://receiver.example/webhook"));

		// integrations accepts unknown fields
		mvc.perform(post("/integrations")
				.contentType(MediaType.APPLICATION_JSON)
				.content("{\"type\":\"slack\",\"webhook_url\":\"https://receiver.example/slack\",\"username\":\"ProxyWatch\",\"events\":[\"alert.fired\",\"alert.resolved\"],\"y\":2}"))
			.andExpect(status().isCreated())
			.andExpect(jsonPath("$.integration_id").exists())
			.andExpect(jsonPath("$.type").value("slack"));

		// alerts returns array (may be empty)
		mvc.perform(get("/alerts"))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$").isArray());

		// metrics must be non-empty json
		mvc.perform(get("/metrics"))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.total_checks").exists())
			.andExpect(jsonPath("$.current_pool_size").exists())
			.andExpect(jsonPath("$.active_alerts").exists())
			.andExpect(jsonPath("$.total_alerts").exists())
			.andExpect(jsonPath("$.webhook_deliveries").exists());
	}

	@Test
	void deleteProxies_doesNotClearAlerts() throws Exception {
		Alert alert = new Alert("alert-test", 0.5, 2, 1, List.of("px-1"), Instant.now());
		store.addAlert(alert);

		mvc.perform(delete("/proxies"))
			.andExpect(status().isNoContent());

		mvc.perform(get("/alerts"))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$[?(@.alert_id=='alert-test')]").exists());
	}
}
