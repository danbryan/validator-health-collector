package collector

import (
	"fmt"
	"log"
	"sort"
	"sync"
	"time"
)

const monikerRefreshInterval = time.Hour

// RedelegationEvent is one successful uatom MsgBeginRedelegate message.
type RedelegationEvent struct {
	TxHash       string
	MessageIndex int
	Timestamp    time.Time
	Source       string
	Destination  string
	AmountUAtom  float64
}

func (e RedelegationEvent) key() string {
	return fmt.Sprintf("%s:%d", e.TxHash, e.MessageIndex)
}

// RedelegationScanner owns the rolling in-memory event window. It rebuilds the
// window from the transaction index whenever the process starts.
type RedelegationScanner struct {
	scanMu sync.Mutex
	mu     sync.Mutex

	txClient       *RESTClient
	stateClient    *RESTClient
	entityMap      map[string]string
	metrics        *Metrics
	window         time.Duration
	overlap        time.Duration
	thresholdRatio float64

	events            map[string]RedelegationEvent
	lastSuccess       time.Time
	monikers          map[string]string
	monikersRefreshed time.Time
	thresholdStates   map[string]redelegationThresholdState
}

func NewRedelegationScanner(
	metrics *Metrics,
	entityMap map[string]string,
	window time.Duration,
	overlap time.Duration,
	thresholdRatio float64,
) *RedelegationScanner {
	metrics.RedelegationAlertThreshold.Set(thresholdRatio)
	return &RedelegationScanner{
		txClient:        NewRESTClient(),
		stateClient:     NewRESTClient(),
		entityMap:       entityMap,
		metrics:         metrics,
		window:          window,
		overlap:         overlap,
		thresholdRatio:  thresholdRatio,
		events:          make(map[string]RedelegationEvent),
		monikers:        make(map[string]string),
		thresholdStates: make(map[string]redelegationThresholdState),
	}
}

// SetEndpoints keeps ordinary REST state queries separate from transaction
// search, because the latter requires an enabled and responsive tx index.
func (s *RedelegationScanner) SetEndpoints(rest, txSearch []string) {
	s.stateClient.SetEndpoints(rest)
	s.txClient.SetEndpoints(txSearch)
}

// Scan rebuilds or incrementally refreshes the rolling event window.
func (s *RedelegationScanner) Scan(now time.Time) error {
	s.scanMu.Lock()
	defer s.scanMu.Unlock()

	if err := s.scan(now); err != nil {
		s.metrics.RedelegationScanSuccess.Set(0)
		return err
	}
	s.metrics.RedelegationScanSuccess.Set(1)
	s.metrics.RedelegationScanLastSuccess.Set(float64(now.Unix()))
	return nil
}

func (s *RedelegationScanner) scan(now time.Time) error {
	windowCutoff := now.Add(-s.window)

	s.mu.Lock()
	since := windowCutoff
	if !s.lastSuccess.IsZero() {
		incrementalCutoff := s.lastSuccess.Add(-s.overlap)
		if incrementalCutoff.After(since) {
			since = incrementalCutoff
		}
	}
	s.mu.Unlock()

	found, err := s.txClient.QueryRedelegations(since)
	if err != nil {
		return fmt.Errorf("querying redelegations: %w", err)
	}
	bondedUAtom, err := s.stateClient.QueryStakingPool()
	if err != nil {
		return fmt.Errorf("querying current bonded pool for redelegation ratios: %w", err)
	}
	if bondedUAtom <= 0 {
		return fmt.Errorf("current bonded pool is not positive: %v", bondedUAtom)
	}

	s.refreshMonikers(now)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Work on a replacement map and publish only after every required query has
	// succeeded, so a bounded-pagination or endpoint failure cannot leak partial
	// results into Prometheus.
	next := make(map[string]RedelegationEvent, len(s.events)+len(found))
	for key, event := range s.events {
		if !event.Timestamp.Before(windowCutoff) {
			next[key] = event
		}
	}
	for _, event := range found {
		if !event.Timestamp.Before(windowCutoff) {
			next[event.key()] = event
		}
	}

	s.events = next
	s.lastSuccess = now
	s.publishLocked(bondedUAtom)
	return nil
}

func (s *RedelegationScanner) refreshMonikers(now time.Time) {
	s.mu.Lock()
	fresh := !s.monikersRefreshed.IsZero() && now.Sub(s.monikersRefreshed) < monikerRefreshInterval
	s.mu.Unlock()
	if fresh {
		return
	}

	validators, err := s.stateClient.QueryBondedValidators()
	if err != nil {
		log.Printf("WARN: redelegation moniker refresh failed, retaining cache: %v", err)
		return
	}
	monikers := make(map[string]string, len(validators))
	for _, validator := range validators {
		monikers[validator.OperatorAddress] = validator.Description.Moniker
	}

	s.mu.Lock()
	s.monikers = monikers
	s.monikersRefreshed = now
	s.mu.Unlock()
}

type redelegationSourceAggregate struct {
	amount float64
	latest time.Time
}

type redelegationPair struct {
	source      string
	destination string
}

type redelegationPairAggregate struct {
	amount float64
	count  int
}

type redelegationThresholdState struct {
	above     bool
	crossedAt time.Time
}

func (s *RedelegationScanner) publishLocked(bondedUAtom float64) {
	s.metrics.RedelegationOutflowRatio.Reset()
	s.metrics.RedelegationOutflowATOM.Reset()
	s.metrics.RedelegationFlowRatio.Reset()
	s.metrics.RedelegationFlowATOM.Reset()
	s.metrics.RedelegationEventCount.Reset()
	s.metrics.RedelegationLatestEvent.Reset()
	s.metrics.RedelegationThresholdCrossed.Reset()

	bySource := make(map[string]redelegationSourceAggregate)
	byPair := make(map[redelegationPair]redelegationPairAggregate)
	for _, event := range s.events {
		source := bySource[event.Source]
		source.amount += event.AmountUAtom
		if event.Timestamp.After(source.latest) {
			source.latest = event.Timestamp
		}
		bySource[event.Source] = source

		key := redelegationPair{source: event.Source, destination: event.Destination}
		pair := byPair[key]
		pair.amount += event.AmountUAtom
		pair.count++
		byPair[key] = pair
	}

	s.updateThresholdStatesLocked(bySource, bondedUAtom)

	sources := make([]string, 0, len(bySource))
	for source := range bySource {
		sources = append(sources, source)
	}
	sort.Strings(sources)
	for _, source := range sources {
		aggregate := bySource[source]
		moniker := s.monikerName(source)
		entity := s.entityName(source)
		s.metrics.RedelegationOutflowRatio.WithLabelValues(source, moniker, entity).Set(aggregate.amount / bondedUAtom)
		s.metrics.RedelegationOutflowATOM.WithLabelValues(source, moniker, entity).Set(aggregate.amount / 1_000_000)
		s.metrics.RedelegationLatestEvent.WithLabelValues(source, moniker, entity).Set(float64(aggregate.latest.Unix()))
		if crossedAt := s.thresholdStates[source].crossedAt; !crossedAt.IsZero() {
			s.metrics.RedelegationThresholdCrossed.WithLabelValues(source, moniker, entity).Set(float64(crossedAt.Unix()))
		}
	}

	pairs := make([]redelegationPair, 0, len(byPair))
	for pair := range byPair {
		pairs = append(pairs, pair)
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].source == pairs[j].source {
			return pairs[i].destination < pairs[j].destination
		}
		return pairs[i].source < pairs[j].source
	})
	for _, pair := range pairs {
		aggregate := byPair[pair]
		labels := []string{
			pair.source,
			s.monikerName(pair.source),
			s.entityName(pair.source),
			pair.destination,
			s.monikerName(pair.destination),
			s.entityName(pair.destination),
		}
		s.metrics.RedelegationFlowRatio.WithLabelValues(labels...).Set(aggregate.amount / bondedUAtom)
		s.metrics.RedelegationFlowATOM.WithLabelValues(labels...).Set(aggregate.amount / 1_000_000)
		s.metrics.RedelegationEventCount.WithLabelValues(labels...).Set(float64(aggregate.count))
	}
}

func (s *RedelegationScanner) updateThresholdStatesLocked(
	bySource map[string]redelegationSourceAggregate,
	bondedUAtom float64,
) {
	thresholdUAtom := bondedUAtom * s.thresholdRatio
	for source, state := range s.thresholdStates {
		if _, exists := bySource[source]; !exists {
			state.above = false
			s.thresholdStates[source] = state
		}
	}

	for source, aggregate := range bySource {
		above := aggregate.amount >= thresholdUAtom
		state, exists := s.thresholdStates[source]
		switch {
		case !above:
			state.above = false
		case !exists || !state.above:
			state.above = true
			state.crossedAt = s.reconstructCrossingLocked(source, thresholdUAtom)
		default:
			// Preserve the original crossing while the source remains at or above
			// threshold. Later events update latest_event, not the alert lifecycle.
			state.above = true
		}
		s.thresholdStates[source] = state
	}
}

func (s *RedelegationScanner) reconstructCrossingLocked(source string, thresholdUAtom float64) time.Time {
	events := make([]RedelegationEvent, 0)
	for _, event := range s.events {
		if event.Source == source {
			events = append(events, event)
		}
	}
	sortRedelegationEvents(events)

	cumulative := 0.0
	for _, event := range events {
		cumulative += event.AmountUAtom
		if cumulative >= thresholdUAtom {
			return event.Timestamp
		}
	}
	return time.Time{}
}

func (s *RedelegationScanner) monikerName(validator string) string {
	if name := s.monikers[validator]; name != "" {
		return name
	}
	return validator
}

func (s *RedelegationScanner) entityName(validator string) string {
	if name := s.entityMap[validator]; name != "" {
		return name
	}
	return "unmapped"
}

func (s *RedelegationScanner) eventCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.events)
}
