# Validator Health Collector

`validator-health-collector` exports Prometheus metrics for Cosmos Hub validator-set health. It collects validator concentration, signing and jailing risk, governance participation, upgrades, rolling redelegation outflow, and collector health data from public Cosmos REST and CometBFT RPC endpoints.

The collector discovers candidate endpoints from the Cosmos Chain Registry, validates that they serve the requested chain, and rotates away from unhealthy endpoints. REST and RPC endpoints can also be pinned explicitly. Ordinary REST health and transaction-search capability are probed separately, so a node can remain available for staking and governance while being excluded from redelegation searches.

Most metrics read current chain state. The redelegation scanner additionally depends on a Cosmos REST endpoint with a responsive transaction index for `GET /cosmos/tx/v1beta1/txs`; it does not scrape Mintscan or require an archive RPC node. The scanner runs every five minutes independently of the hourly snapshot, backfills a 168-hour rolling window on startup, and rebuilds that window from the REST index after restart. It uses bounded pagination and publishes no partial scan, so no PVC is required.

## Run locally

Requirements:

- Go 1.25 or newer
- Network access to the Cosmos Chain Registry and public Cosmos Hub endpoints

```sh
go run . -chain=cosmoshub -listen=:9090 -interval=1h \
  -redelegation-interval=5m -redelegation-window=168h
```

Metrics are available at `http://localhost:9090/metrics` and the health endpoint is available at `http://localhost:9090/health`.

The redelegation alert threshold defaults to 2% for local development when `-config` is omitted. `validator_health_redelegation_threshold_crossed_timestamp_seconds` records the event time of the most recent below-to-at/above transition and does not refresh while a source remains over threshold. It is reconstructed from the rolling event set on restart. `validator_health_redelegation_latest_event_timestamp_seconds` remains a separate dashboard freshness signal.

Production passes a Git-backed configuration file:

```yaml
redelegation:
  alert_threshold_percent: 2
```

Use it with `-config=/etc/validator-health/config.yaml`. The value may be decimal, must be greater than 0 and at most 100, and an explicitly supplied missing or invalid file prevents startup.

To bypass endpoint discovery:

```sh
go run . \
  -rest=https://example-rest-endpoint \
  -rpc=https://example-rpc-endpoint
```

## Governance history

Live proposals are always exported. Closed proposals are exported for 14 days by default, matching the shortest retention in the target monitoring stack. Override the bound with `-proposal-history`, for example `-proposal-history=168h`.

Turnout history is built prospectively from the collector's hourly Prometheus samples. The collector does not reconstruct old vote timelines from transactions. Entity-level vote attribution is queried only for live proposals through the current governance votes REST API; final tally, quorum, and veto state remain available even if that optional attribution query fails.

## Entity mapping

By default, every validator is treated as an independent entity. `entity.yaml` groups validators operated by the same entity so concentration metrics reflect the operator rather than individual validator records.

Pass a different map with `-entity-map=/path/to/entity.yaml`. If the specified file exists but cannot be parsed, the collector exits instead of publishing misleading ungrouped metrics.

## Container image

The `main` branch publishes an amd64 image to:

```text
ghcr.io/danbryan/validator-health-collector:v0.1.3
```

Build it locally with:

```sh
docker build -t validator-health-collector .
```

## Validate changes

```sh
go test ./...
go test -race ./collector ./endpoints
go vet ./...
```
