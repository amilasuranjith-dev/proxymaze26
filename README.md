# ProxyMaze26

ProxyMaze26 is a Spring Boot service that monitors a pool of proxy URLs, tracks uptime, and triggers alerts when the pool degrades. It runs entirely in-memory and exposes a REST API for managing proxies, configuration, alerts, metrics, and webhook integrations.

## Requirements

- Java 17+
- Maven (or use the included `mvnw` wrapper)

## Running locally

```bash
./mvnw spring-boot:run
```

The service listens on `8080` by default. Override the port with `PORT`:

```bash
PORT=9090 ./mvnw spring-boot:run
```

## Behavior overview

- Proxies are checked on a configurable interval (default: 60 seconds).
- A proxy is **up** only when it returns a 2xx response within the timeout (default: 5000 ms).
- An alert fires when the pool failure rate is **>= 0.20** and resolves when it drops below 0.20.
- Alert notifications can be delivered to generic webhooks or Slack/Discord integrations.

## JSON conventions

- All JSON responses use **snake_case**.
- Request bodies ignore unknown fields.
- Timestamps are ISO-8601 UTC strings (e.g., `2026-04-24T10:15:30Z`).

## API

### Health

- `GET /health` → `{ "status": "ok" }`

### Configuration

- `GET /config` → current configuration
- `POST /config` → update configuration
  - Body fields: `check_interval_seconds`, `request_timeout_ms`
  - Defaults: `check_interval_seconds = 60`, `request_timeout_ms = 5000`

### Proxies

- `POST /proxies` → add proxies to the pool
  - Body fields: `proxies` (array of URLs), `replace` (boolean)
  - When `replace` is true, the pool is cleared before adding
  - IDs are derived from the URL path segment (e.g., `.../px-101` → `px-101`)
- `GET /proxies` → pool summary with per-proxy status
- `GET /proxies/{id}` → detailed proxy status, checks, and uptime percentage
- `GET /proxies/{id}/history` → check history only
- `DELETE /proxies` → clears the pool (alerts remain)

### Alerts

- `GET /alerts` → list of alert records
  - Alerts fire at failure rate >= 0.20 and resolve below 0.20
  - Alert records include `failure_rate`, `failed_proxy_ids`, `threshold`, `fired_at`, and `resolved_at`

### Webhooks

- `POST /webhooks` → register a generic webhook
  - Body fields: `url`
  - Response: `webhook_id`, `url`

### Integrations

- `POST /integrations` → register a Slack or Discord integration
  - Body fields: `type` (`slack` or `discord`), `webhook_url`, `username` (optional), `events`
  - Supported events: `alert.fired`, `alert.resolved`

### Metrics

- `GET /metrics` → operational stats
  - `total_checks`, `current_pool_size`, `active_alerts`, `total_alerts`, `webhook_deliveries`

## Webhook delivery rules

- Deliveries are serialized per receiver to preserve event ordering.
- Retries occur for 500/502/503/504 responses or connection failures.
- Exponential backoff is used and stops after 60 seconds.

## Development

Run tests:

```bash
./mvnw test
```

Build a jar:

```bash
./mvnw package
```
