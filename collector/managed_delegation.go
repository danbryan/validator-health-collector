package collector

import (
	"errors"
	"fmt"
	"log"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	collectorconfig "github.com/danbryan/validator-health-collector/config"
)

const (
	managedBlockTimeSample   = int64(2000)
	managedRecentCommitCount = int64(10)
	hoursPerYear             = 365 * 24
	uatomPerATOM             = 1_000_000
)

type managedAccountValidator struct {
	delegatorAddress string
	operatorAddress  string
}

type managedAccountPosition struct {
	accountName      string
	delegatorAddress string
	operatorAddress  string
	amountUAtom      float64
}

type sampledCommit struct {
	signers  CommitSigners
	eligible map[string]bool
}

type managedAggregate struct {
	operatorAddress  string
	moniker          string
	entity           string
	consensusAddress string
	accounts         []string
	amountUAtom      float64
	missedBlocks     float64
	active           bool
	jailed           bool
	tombstoned       bool
	commission       float64
	lockedUAtom      float64
	nextUnlock       time.Time
	finalUnlock      time.Time
	recentMissRate   float64
}

type managedScanResult struct {
	accounts         []managedAccountPosition
	aggregates       []managedAggregate
	totalUAtom       float64
	slashing         *SlashingParams
	blockInterval    float64
	annualProvisions float64
	communityTax     float64
	bondedTokens     float64
	rewardsAvailable bool
}

// ManagedDelegationScanner independently monitors risk attached to current
// positive delegations held by configured accounts.
type ManagedDelegationScanner struct {
	scanMu sync.Mutex

	restClient *RESTClient
	rpcClient  *RPCClient
	accounts   []collectorconfig.ManagedDelegator
	entityMap  map[string]string
	metrics    *Metrics
}

// NewManagedDelegationScanner constructs a scanner and publishes configuration
// gauges before the first endpoint resolution or chain query.
func NewManagedDelegationScanner(
	metrics *Metrics,
	entityMap map[string]string,
	cfg collectorconfig.ManagedDelegations,
) *ManagedDelegationScanner {
	metrics.managedMu.Lock()
	metrics.ManagedDelegationConfiguredAccounts.Set(float64(len(cfg.Accounts)))
	metrics.ManagedDelegationWarningJailProgress.Set(cfg.WarningJailProgressPercent / 100)
	metrics.ManagedDelegationCriticalJailProgress.Set(cfg.CriticalJailProgressPercent / 100)
	metrics.managedMu.Unlock()
	return &ManagedDelegationScanner{
		restClient: NewRESTClient(),
		rpcClient:  NewRPCClient(),
		accounts:   append([]collectorconfig.ManagedDelegator(nil), cfg.Accounts...),
		entityMap:  entityMap,
		metrics:    metrics,
	}
}

// ConfigureManagedDelegations configures the independent managed-delegation scanner.
func (c *Collector) ConfigureManagedDelegations(cfg collectorconfig.ManagedDelegations, interval time.Duration) {
	c.managedDelegationInterval = interval
	c.managedDelegations = NewManagedDelegationScanner(c.metrics, c.entityMap, cfg)
}

// SetEndpoints replaces the independently rotating REST and RPC candidate lists.
func (s *ManagedDelegationScanner) SetEndpoints(rest, rpc []string) {
	s.restClient.SetEndpoints(rest)
	s.rpcClient.SetEndpoints(rpc)
}

// Scan performs a complete managed-delegation refresh.
func (s *ManagedDelegationScanner) Scan(now time.Time) error {
	s.scanMu.Lock()
	defer s.scanMu.Unlock()

	result, err := s.collect(now)
	if err != nil {
		s.metrics.managedMu.Lock()
		s.metrics.ManagedDelegationScanSuccess.Set(0)
		s.metrics.managedMu.Unlock()
		return err
	}
	s.publish(result, now)
	log.Printf("Managed delegation scan complete: %d accounts, %d validators, %.2f ATOM", len(s.accounts), len(result.aggregates), result.totalUAtom/uatomPerATOM)
	return nil
}

func (s *ManagedDelegationScanner) collect(now time.Time) (managedScanResult, error) {
	result := managedScanResult{}
	pairFinalUnlocks := make(map[managedAccountValidator]time.Time)
	var accountErrors []error

	for _, account := range s.accounts {
		delegations, delegationErr := s.restClient.QueryDelegations(account.Address)
		if delegationErr != nil {
			accountErrors = append(accountErrors, fmt.Errorf("managed account %q delegations: %w", account.Name, delegationErr))
		} else {
			byOperator := make(map[string]float64)
			for _, delegation := range delegations {
				byOperator[delegation.OperatorAddress] += delegation.AmountUAtom
			}
			for operator, amount := range byOperator {
				result.accounts = append(result.accounts, managedAccountPosition{
					accountName:      account.Name,
					delegatorAddress: account.Address,
					operatorAddress:  operator,
					amountUAtom:      amount,
				})
				result.totalUAtom += amount
			}
		}

		redelegations, redelegationErr := s.restClient.QueryReceivingRedelegations(account.Address)
		if redelegationErr != nil {
			accountErrors = append(accountErrors, fmt.Errorf("managed account %q redelegations: %w", account.Name, redelegationErr))
		} else {
			for _, entry := range redelegations {
				pair := managedAccountValidator{
					delegatorAddress: entry.DelegatorAddress,
					operatorAddress:  entry.DestinationOperator,
				}
				latest, exists := pairFinalUnlocks[pair]
				if !exists || entry.CompletionTime.After(latest) {
					pairFinalUnlocks[pair] = entry.CompletionTime
				}
			}
		}
	}
	if err := errors.Join(accountErrors...); err != nil {
		return managedScanResult{}, err
	}

	validators, err := s.restClient.QueryAllValidators()
	if err != nil {
		return managedScanResult{}, fmt.Errorf("querying all validator metadata: %w", err)
	}
	signingInfos, err := s.restClient.QuerySigningInfos()
	if err != nil {
		return managedScanResult{}, fmt.Errorf("querying managed validator signing infos: %w", err)
	}
	result.slashing, err = s.restClient.QuerySlashingParams()
	if err != nil {
		return managedScanResult{}, fmt.Errorf("querying managed validator slashing params: %w", err)
	}
	consensusSet, err := s.rpcClient.QueryConsensusSet()
	if err != nil {
		return managedScanResult{}, fmt.Errorf("querying managed validator consensus set: %w", err)
	}
	if len(consensusSet) == 0 {
		return managedScanResult{}, fmt.Errorf("managed validator consensus set is empty")
	}
	status, err := s.rpcClient.QueryCometStatus()
	if err != nil {
		return managedScanResult{}, fmt.Errorf("querying managed scan block height: %w", err)
	}
	recentCommits, err := s.queryRecentCommits(status.Height)
	if err != nil {
		return managedScanResult{}, err
	}
	result.blockInterval, err = s.rpcClient.MeasureBlockTime(managedBlockTimeSample)
	if err != nil {
		return managedScanResult{}, fmt.Errorf("measuring managed scan block interval: %w", err)
	}
	if result.blockInterval <= 0 {
		return managedScanResult{}, fmt.Errorf("measured managed scan block interval is not positive: %v", result.blockInterval)
	}

	validatorByOperator := make(map[string]Validator, len(validators))
	for _, validator := range validators {
		if validator.OperatorAddress == "" {
			return managedScanResult{}, fmt.Errorf("validator metadata contains an empty operator address")
		}
		validatorByOperator[validator.OperatorAddress] = validator
	}
	signingByConsensus := make(map[string]SigningInfo, len(signingInfos))
	for _, info := range signingInfos {
		signingByConsensus[info.Address] = info
	}

	type aggregateBuilder struct {
		amount   float64
		accounts map[string]bool
	}
	builders := make(map[string]*aggregateBuilder)
	for _, position := range result.accounts {
		builder := builders[position.operatorAddress]
		if builder == nil {
			builder = &aggregateBuilder{accounts: make(map[string]bool)}
			builders[position.operatorAddress] = builder
		}
		builder.amount += position.amountUAtom
		builder.accounts[position.accountName] = true
	}

	operators := make([]string, 0, len(builders))
	for operator := range builders {
		operators = append(operators, operator)
	}
	sort.Strings(operators)
	for _, operator := range operators {
		builder := builders[operator]
		validator, ok := validatorByOperator[operator]
		if !ok {
			return managedScanResult{}, fmt.Errorf("managed delegation validator %s is absent from all-validator metadata", operator)
		}
		consensusAddress, err := consensusAddressFromPubKey(pubKeyOf(validator))
		if err != nil {
			return managedScanResult{}, fmt.Errorf("deriving consensus address for %s: %w", operator, err)
		}
		consensusHex, err := consensusHexFromPubKey(pubKeyOf(validator))
		if err != nil {
			return managedScanResult{}, fmt.Errorf("deriving consensus membership address for %s: %w", operator, err)
		}

		accountNames := make([]string, 0, len(builder.accounts))
		for name := range builder.accounts {
			accountNames = append(accountNames, name)
		}
		sort.Strings(accountNames)
		aggregate := managedAggregate{
			operatorAddress:  operator,
			moniker:          validator.Description.Moniker,
			entity:           s.entityName(operator, validator.Description.Moniker),
			consensusAddress: consensusAddress,
			accounts:         accountNames,
			amountUAtom:      builder.amount,
			active:           consensusSet[consensusHex],
		}

		if info, exists := signingByConsensus[consensusAddress]; exists {
			aggregate.missedBlocks, err = parseUAtom(info.MissedBlocksCounter)
			if err != nil {
				return managedScanResult{}, fmt.Errorf("managed validator %s has invalid missed block counter %q: %w", operator, info.MissedBlocksCounter, err)
			}
			aggregate.tombstoned = info.Tombstoned
			if info.JailedUntil != "" {
				jailedUntil, parseErr := time.Parse(time.RFC3339Nano, info.JailedUntil)
				if parseErr != nil {
					return managedScanResult{}, fmt.Errorf("managed validator %s has invalid jailed_until %q: %w", operator, info.JailedUntil, parseErr)
				}
				aggregate.jailed = jailedUntil.After(now)
			}
		}
		aggregate.jailed = aggregate.jailed || validator.Jailed
		aggregate.commission, err = parseRatio(validator.Commission.CommissionRates.Rate)
		if err != nil {
			aggregate.commission = math.NaN()
		}

		for _, position := range result.accounts {
			if position.operatorAddress != operator {
				continue
			}
			pair := managedAccountValidator{
				delegatorAddress: position.delegatorAddress,
				operatorAddress:  operator,
			}
			pairFinalUnlock, locked := pairFinalUnlocks[pair]
			if !locked {
				continue
			}
			aggregate.lockedUAtom += position.amountUAtom
			if aggregate.nextUnlock.IsZero() || pairFinalUnlock.Before(aggregate.nextUnlock) {
				aggregate.nextUnlock = pairFinalUnlock
			}
			if pairFinalUnlock.After(aggregate.finalUnlock) {
				aggregate.finalUnlock = pairFinalUnlock
			}
		}

		missed := 0
		eligible := 0
		for _, sample := range recentCommits {
			if !sample.eligible[consensusHex] {
				continue
			}
			eligible++
			if !sample.signers[consensusHex] {
				missed++
			}
		}
		if eligible > 0 {
			aggregate.recentMissRate = float64(missed) / float64(eligible)
		}
		result.aggregates = append(result.aggregates, aggregate)
	}

	result.loadOptionalRewards(s.restClient)
	return result, nil
}

func (s *ManagedDelegationScanner) queryRecentCommits(currentHeight int64) ([]sampledCommit, error) {
	if currentHeight < managedRecentCommitCount {
		return nil, fmt.Errorf("querying recent managed commits: current height %d is below sample size %d", currentHeight, managedRecentCommitCount)
	}
	commits := make([]sampledCommit, 0, managedRecentCommitCount)
	for offset := int64(0); offset < managedRecentCommitCount; offset++ {
		height := currentHeight - offset
		signers, err := s.rpcClient.QueryCommitSigners(height)
		if err != nil {
			return nil, fmt.Errorf("querying recent managed commit at height %d: %w", height, err)
		}
		eligible, err := s.rpcClient.QueryConsensusSetAtHeight(height)
		if err != nil {
			return nil, fmt.Errorf("querying recent managed consensus set at height %d: %w", height, err)
		}
		commits = append(commits, sampledCommit{signers: signers, eligible: eligible})
	}
	return commits, nil
}

func (result *managedScanResult) loadOptionalRewards(client *RESTClient) {
	annual, annualErr := client.QueryAnnualProvisions()
	communityTax, taxErr := client.QueryCommunityTax()
	bonded, bondedErr := client.QueryStakingPool()
	if annualErr != nil || taxErr != nil || bondedErr != nil || bonded <= 0 {
		log.Printf("WARN: managed delegation reward estimate omitted: %v", errorsForLog(annualErr, taxErr, bondedErr))
		return
	}
	for _, aggregate := range result.aggregates {
		if math.IsNaN(aggregate.commission) {
			log.Printf("WARN: managed delegation reward estimate omitted because validator %s has an invalid commission", aggregate.operatorAddress)
			return
		}
	}
	result.annualProvisions = annual
	result.communityTax = communityTax
	result.bondedTokens = bonded
	result.rewardsAvailable = true
}

func errorsForLog(errs ...error) string {
	messages := make([]string, 0, len(errs))
	for _, err := range errs {
		if err != nil {
			messages = append(messages, err.Error())
		}
	}
	if len(messages) == 0 {
		return "bonded token pool is not positive"
	}
	return strings.Join(messages, "; ")
}

func (s *ManagedDelegationScanner) publish(result managedScanResult, now time.Time) {
	s.metrics.managedMu.Lock()
	defer s.metrics.managedMu.Unlock()

	s.resetMetrics()

	sort.Slice(result.accounts, func(i, j int) bool {
		if result.accounts[i].accountName == result.accounts[j].accountName {
			return result.accounts[i].operatorAddress < result.accounts[j].operatorAddress
		}
		return result.accounts[i].accountName < result.accounts[j].accountName
	})
	metadata := make(map[string]managedAggregate, len(result.aggregates))
	for _, aggregate := range result.aggregates {
		metadata[aggregate.operatorAddress] = aggregate
	}
	for _, position := range result.accounts {
		aggregate := metadata[position.operatorAddress]
		s.metrics.ManagedDelegationAccountATOM.WithLabelValues(
			position.accountName,
			position.delegatorAddress,
			position.operatorAddress,
			aggregate.moniker,
			aggregate.entity,
			aggregate.consensusAddress,
		).Set(position.amountUAtom / uatomPerATOM)
	}

	for _, aggregate := range result.aggregates {
		labels := []string{
			aggregate.operatorAddress,
			aggregate.moniker,
			aggregate.entity,
			aggregate.consensusAddress,
			strings.Join(aggregate.accounts, ","),
		}
		amountATOM := aggregate.amountUAtom / uatomPerATOM
		lockedATOM := aggregate.lockedUAtom / uatomPerATOM
		redelegatableATOM := math.Max(amountATOM-lockedATOM, 0)
		blocksToJail := blocksUntilJail(result.slashing.MaxMissedBlocks, aggregate.missedBlocks)

		s.metrics.ManagedValidatorDelegationATOM.WithLabelValues(labels...).Set(amountATOM)
		if result.totalUAtom > 0 {
			s.metrics.ManagedValidatorPortfolioShare.WithLabelValues(labels...).Set(aggregate.amountUAtom / result.totalUAtom)
		}
		s.metrics.ManagedValidatorMissedBlocks.WithLabelValues(labels...).Set(aggregate.missedBlocks)
		s.metrics.ManagedValidatorMissedRatio.WithLabelValues(labels...).Set(aggregate.missedBlocks / float64(result.slashing.SignedBlocksWindow))
		s.metrics.ManagedValidatorJailProgress.WithLabelValues(labels...).Set(clamp(aggregate.missedBlocks/float64(result.slashing.MaxMissedBlocks), 0, 1))
		s.metrics.ManagedValidatorBlocksToJail.WithLabelValues(labels...).Set(blocksToJail)
		s.metrics.ManagedValidatorSecondsToJail.WithLabelValues(labels...).Set(blocksToJail * result.blockInterval)
		s.metrics.ManagedValidatorRecentMissRate.WithLabelValues(labels...).Set(aggregate.recentMissRate)
		s.metrics.ManagedValidatorActive.WithLabelValues(labels...).Set(boolGauge(aggregate.active))
		s.metrics.ManagedValidatorJailed.WithLabelValues(labels...).Set(boolGauge(aggregate.jailed))
		s.metrics.ManagedValidatorTombstoned.WithLabelValues(labels...).Set(boolGauge(aggregate.tombstoned))
		s.metrics.ManagedValidatorDowntimeSlashExposure.WithLabelValues(labels...).Set(amountATOM * result.slashing.SlashFractionDowntime)
		s.metrics.ManagedValidatorDoubleSignSlashExposure.WithLabelValues(labels...).Set(amountATOM * result.slashing.SlashFractionDoubleSign)
		s.metrics.ManagedValidatorRedelegationLocked.WithLabelValues(labels...).Set(lockedATOM)
		s.metrics.ManagedValidatorEstimatedRedelegatable.WithLabelValues(labels...).Set(redelegatableATOM)
		s.metrics.ManagedValidatorNextRedelegationUnlock.WithLabelValues(labels...).Set(unixOrZero(aggregate.nextUnlock))
		s.metrics.ManagedValidatorFinalRedelegationUnlock.WithLabelValues(labels...).Set(unixOrZero(aggregate.finalUnlock))
		if result.rewardsAvailable {
			rewardUAtomPerHour := result.annualProvisions * aggregate.amountUAtom / result.bondedTokens *
				(1 - result.communityTax) * (1 - aggregate.commission) / hoursPerYear
			s.metrics.ManagedValidatorEstimatedRewardsLost.WithLabelValues(labels...).Set(rewardUAtomPerHour / uatomPerATOM)
		}
	}
	s.metrics.ManagedDelegationScanSuccess.Set(1)
	s.metrics.ManagedDelegationScanLastSuccess.Set(float64(now.Unix()))
}

func (s *ManagedDelegationScanner) resetMetrics() {
	s.metrics.ManagedDelegationAccountATOM.Reset()
	s.metrics.ManagedValidatorDelegationATOM.Reset()
	s.metrics.ManagedValidatorPortfolioShare.Reset()
	s.metrics.ManagedValidatorMissedBlocks.Reset()
	s.metrics.ManagedValidatorMissedRatio.Reset()
	s.metrics.ManagedValidatorJailProgress.Reset()
	s.metrics.ManagedValidatorBlocksToJail.Reset()
	s.metrics.ManagedValidatorSecondsToJail.Reset()
	s.metrics.ManagedValidatorRecentMissRate.Reset()
	s.metrics.ManagedValidatorActive.Reset()
	s.metrics.ManagedValidatorJailed.Reset()
	s.metrics.ManagedValidatorTombstoned.Reset()
	s.metrics.ManagedValidatorDowntimeSlashExposure.Reset()
	s.metrics.ManagedValidatorDoubleSignSlashExposure.Reset()
	s.metrics.ManagedValidatorEstimatedRewardsLost.Reset()
	s.metrics.ManagedValidatorRedelegationLocked.Reset()
	s.metrics.ManagedValidatorEstimatedRedelegatable.Reset()
	s.metrics.ManagedValidatorNextRedelegationUnlock.Reset()
	s.metrics.ManagedValidatorFinalRedelegationUnlock.Reset()
}

func (s *ManagedDelegationScanner) entityName(operator, moniker string) string {
	if entity := s.entityMap[operator]; entity != "" {
		return entity
	}
	return moniker
}

func clamp(value, minimum, maximum float64) float64 {
	return math.Min(math.Max(value, minimum), maximum)
}

func unixOrZero(value time.Time) float64 {
	if value.IsZero() {
		return 0
	}
	return float64(value.Unix())
}

// RunManagedDelegations waits for initial endpoint resolution, then scans on an
// interval independent of both the snapshot and redelegation loops.
func (c *Collector) RunManagedDelegations() {
	if c.managedDelegations == nil || c.managedDelegationInterval <= 0 || len(c.managedDelegations.accounts) == 0 {
		return
	}
	<-c.managedDelegationReady

	run := func(now time.Time) {
		if err := c.managedDelegations.Scan(now); err != nil {
			log.Printf("Managed delegation scan failed: %v", err)
		}
	}
	run(time.Now())

	ticker := time.NewTicker(c.managedDelegationInterval)
	defer ticker.Stop()
	for now := range ticker.C {
		run(now)
	}
}
