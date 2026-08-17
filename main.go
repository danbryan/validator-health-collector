package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/cosmos/platform/apps/validator-health-collector/collector"
	"github.com/cosmos/platform/apps/validator-health-collector/endpoints"
	"github.com/cosmos/platform/apps/validator-health-collector/entity"
)

func main() {
	restURL := flag.String("rest", "", "Pin the Cosmos REST API URL, bypassing chain-registry discovery")
	rpcURL := flag.String("rpc", "", "Pin the CometBFT RPC URL, bypassing chain-registry discovery")
	chain := flag.String("chain", "cosmoshub", "Chain registry directory name")
	registryURL := flag.String("registry-url", endpoints.DefaultRegistryURL,
		"Chain registry chain.json template, with %s for the chain name")
	entityMapPath := flag.String("entity-map", "entity.yaml", "Path to entity map YAML file")
	listenAddr := flag.String("listen", ":9090", "HTTP listen address for /metrics")
	pollInterval := flag.Duration("interval", 1*time.Hour, "Polling interval")
	flag.Parse()

	// The entity map is what turns per-validator power into per-operator power, so
	// whether it loaded decides whether concentration is measured correctly. An
	// absent file is a legitimate configuration meaning every validator is its own
	// entity. A file that exists but cannot be read or parsed is a misconfiguration,
	// and continuing past it would publish ungrouped numbers that look plausible:
	// with Coinbase's two validators left separate, the largest-entity share reads
	// 0.2033 instead of 0.2061. Fail loudly instead.
	em := make(map[string]string)
	switch _, statErr := os.Stat(*entityMapPath); {
	case statErr == nil:
		loaded, err := entity.Load(*entityMapPath)
		if err != nil {
			log.Fatalf("Entity map %s exists but could not be loaded, refusing to start "+
				"and report ungrouped concentration: %v", *entityMapPath, err)
		}
		em = loaded
		log.Printf("Loaded entity map with %d entries from %s", len(em), *entityMapPath)
	case os.IsNotExist(statErr):
		log.Printf("No entity map at %s, treating every validator as its own entity", *entityMapPath)
	default:
		log.Fatalf("Could not stat entity map %s: %v", *entityMapPath, statErr)
	}

	resolver := endpoints.NewResolver(endpoints.Options{
		RegistryURL:  *registryURL,
		Chain:        *chain,
		RESTOverride: *restURL,
		RPCOverride:  *rpcURL,
	})

	c := collector.New(resolver, em)

	// Register metrics with Prometheus
	prometheus.MustRegister(c.Metrics())

	// Start the collection loop in the background
	go c.Run(*pollInterval)

	// HTTP server for /metrics
	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	log.Printf("Starting validator health collector on %s", *listenAddr)
	log.Printf("Chain: %s, registry: %s", *chain, *registryURL)
	logEndpointSource("REST", *restURL)
	logEndpointSource("RPC", *rpcURL)
	log.Printf("Poll interval: %v", *pollInterval)

	server := &http.Server{
		Addr:              *listenAddr,
		ReadHeaderTimeout: 10 * time.Second,
	}
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

// logEndpointSource records whether a protocol is pinned or discovered, so the
// startup log makes the dependency obvious.
func logEndpointSource(protocol, override string) {
	if override == "" {
		log.Printf("%s endpoint: discovered from the chain registry", protocol)
		return
	}
	log.Printf("%s endpoint: pinned to %s by flag, discovery bypassed", protocol, override)
}
