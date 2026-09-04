# Validator Health Collector

`validator-health-collector` exports Prometheus metrics for Cosmos Hub validator-set health. It collects validator concentration, signing and jailing risk, governance participation, upgrades, rolling redelegation outflow, managed-delegation exposure, and collector health data from public Cosmos REST and CometBFT RPC endpoints.

The collector discovers candidate endpoints from the Cosmos Chain Registry, validates that they serve the requested chain, and rotates away from unhealthy endpoints. REST and RPC endpoints can also be pinned explicitly. Ordinary REST health and transaction-search capability are probed separately, so a node can remain available for staking and governance while being excluded from redelegation searches.

Most metrics read current chain state. The redelegation scanner additionally depends on a Cosmos REST endpoint with a responsive transaction index for `GET /cosmos/tx/v1beta1/txs`; it does not scrape Mintscan or require an archive RPC node. The scanner runs every five minutes independently of the hourly snapshot, backfills a 168-hour rolling window on startup, and rebuilds that window from the REST index after restart. It uses bounded pagination and publishes no partial scan, so no PVC is required.

## Run locally

Requirements:

- Go 1.25 or newer
- Network access to the Cosmos Chain Registry and public Cosmos Hub endpoints

```sh
go run . -chain=cosmoshub -listen=:9090 -interval=1h \
  -redelegation-interval=5m -redelegation-window=168h \
  -managed-delegation-interval=5m
```

Metrics are available at `http://localhost:9090/metrics` and the health endpoint is available at `http://localhost:9090/health`.

The redelegation alert threshold defaults to 2% for local development when `-config` is omitted. `validator_health_redelegation_threshold_crossed_timestamp_seconds` records the event time of the most recent below-to-at/above transition and does not refresh while a source remains over threshold. It is reconstructed from the rolling event set on restart. `validator_health_redelegation_latest_event_timestamp_seconds` remains a separate dashboard freshness signal.

Production passes a Git-backed configuration file:

```yaml
redelegation:
  alert_threshold_percent: 2
managed_delegations:
  warning_jail_progress_percent: 10
  critical_jail_progress_percent: 80
  accounts:
    - name: Amina
      address: cosmos1f3vdsge09avpxsym5233xgskwv2q5s3cg57dcs
```

Use it with `-config=/etc/validator-health/config.yaml`. Thresholds may be decimal. The managed thresholds are percentages of the live maximum missed-block budget before jail, and must satisfy `0 < warning < critical < 100`. Account names and valid `cosmos` account addresses are trimmed, account addresses are canonicalized to lowercase, and both must be unique after normalization. An explicitly supplied missing or invalid file prevents startup.

## Managed delegation risk

The managed-delegation scanner discovers every current positive `uatom` delegation from the configured accounts. It aggregates overlapping accounts into one validator risk series while retaining per-account balances. Zero balances do not count.

Every five minutes it refreshes validator metadata, the active consensus set, slashing state, live slashing parameters, the latest 10 completed CometBFT commits, each sampled height's exact consensus set, and receiving redelegation entries returned by current chain state. Metrics expose jail progress, the miss rate over sampled blocks where each validator was eligible, estimated time to jail, current jailed and tombstoned state, one-time downtime and double-sign slash exposure, estimated hourly reward loss, and the amount estimated to be available or locked for another redelegation. Any receiving redelegation entry still returned by the chain locks the full current delegation for that delegator-account and destination-validator pair, regardless of its calculated balance or whether its completion timestamp has passed locally. Completion timestamps remain available as informational unlock estimates. Reward-loss metrics are omitted when their optional issuance or distribution inputs are unavailable; signing-risk metrics continue to update.

A scan publishes no partial roster. On a core query failure, the last complete metrics remain available, `validator_health_managed_delegation_scan_success` becomes zero, and its last-success timestamp does not advance.

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
ghcr.io/danbryan/validator-health-collector:v0.1.4
```

Build it locally with:

```sh
docker build -t validator-health-collector .
```

## Validate changes

```sh
go test ./...
go test -race ./collector ./config ./endpoints
go vet ./...
```
