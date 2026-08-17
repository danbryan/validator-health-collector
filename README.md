# Validator Health Collector

`validator-health-collector` exports Prometheus metrics for Cosmos Hub validator-set health. It collects validator concentration, signing and jailing risk, governance participation, upgrade, and collector health data from public Cosmos REST and CometBFT RPC endpoints.

The collector discovers candidate endpoints from the Cosmos Chain Registry, validates that they serve the requested chain, and rotates away from unhealthy endpoints. REST and RPC endpoints can also be pinned explicitly.

## Run locally

Requirements:

- Go 1.25 or newer
- Network access to the Cosmos Chain Registry and public Cosmos Hub endpoints

```sh
go run . -chain=cosmoshub -listen=:9090 -interval=1h
```

Metrics are available at `http://localhost:9090/metrics` and the health endpoint is available at `http://localhost:9090/health`.

To bypass endpoint discovery:

```sh
go run . \
  -rest=https://example-rest-endpoint \
  -rpc=https://example-rpc-endpoint
```

## Entity mapping

By default, every validator is treated as an independent entity. `entity.yaml` groups validators operated by the same entity so concentration metrics reflect the operator rather than individual validator records.

Pass a different map with `-entity-map=/path/to/entity.yaml`. If the specified file exists but cannot be parsed, the collector exits instead of publishing misleading ungrouped metrics.

## Container image

The `main` branch publishes an amd64 image to:

```text
ghcr.io/danbryan/validator-health-collector:v0.1.0
```

Build it locally with:

```sh
docker build -t validator-health-collector .
```

## Validate changes

```sh
go test ./...
go test -race ./collector
go vet ./...
```
