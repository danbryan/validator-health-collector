package collector

import (
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	redelegationPageLimit = 100
	redelegationMaxPages  = 200
	redelegationTypeURL   = "/cosmos.staking.v1beta1.MsgBeginRedelegate"
)

type txSearchCoin struct {
	Denom  string `json:"denom"`
	Amount string `json:"amount"`
}

type txSearchMessage struct {
	TypeURL         string       `json:"@type"`
	SourceValidator string       `json:"validator_src_address"`
	DestValidator   string       `json:"validator_dst_address"`
	Amount          txSearchCoin `json:"amount"`
}

type txSearchBody struct {
	Messages []txSearchMessage `json:"messages"`
}

type txSearchTx struct {
	Body txSearchBody `json:"body"`
}

type txSearchResult struct {
	TxHash    string `json:"txhash"`
	Code      uint32 `json:"code"`
	Timestamp string `json:"timestamp"`
}

type txSearchResponse struct {
	Txs         []txSearchTx     `json:"txs"`
	TxResponses []txSearchResult `json:"tx_responses"`
	Pagination  Pagination       `json:"pagination"`
}

// QueryRedelegations scans successful MsgBeginRedelegate transactions newest
// first until it reaches since. The page bound is a correctness boundary: if a
// complete answer cannot be established within it, no events are returned.
func (c *RESTClient) QueryRedelegations(since time.Time) ([]RedelegationEvent, error) {
	seen := make(map[string]RedelegationEvent)
	var previousTimestamp time.Time

	for page := 1; page <= redelegationMaxPages; page++ {
		params := url.Values{}
		params.Set("query", "message.action='"+redelegationTypeURL+"'")
		params.Set("page", strconv.Itoa(page))
		params.Set("limit", strconv.Itoa(redelegationPageLimit))
		params.Set("order_by", "ORDER_BY_DESC")

		var response txSearchResponse
		path := "/cosmos/tx/v1beta1/txs?" + params.Encode()
		if err := c.get(path, &response); err != nil {
			return nil, fmt.Errorf("tx search page %d: %w", page, err)
		}
		if len(response.Txs) != len(response.TxResponses) {
			return nil, fmt.Errorf("tx search page %d returned %d txs and %d responses",
				page, len(response.Txs), len(response.TxResponses))
		}
		total, err := parseTxSearchTotal(response.Pagination.Total)
		if err != nil {
			return nil, fmt.Errorf("tx search page %d: %w", page, err)
		}
		rowsBefore := (page - 1) * redelegationPageLimit
		if len(response.TxResponses) == 0 {
			if total > rowsBefore {
				return nil, fmt.Errorf("tx search page %d was empty after %d of reported total %d rows", page, rowsBefore, total)
			}
			return sortedRedelegationEvents(seen), nil
		}

		reachedCutoff := false
		for i, result := range response.TxResponses {
			timestamp, err := time.Parse(time.RFC3339Nano, result.Timestamp)
			if err != nil {
				return nil, fmt.Errorf("tx search page %d result %d has invalid timestamp %q: %w",
					page, i, result.Timestamp, err)
			}
			if !previousTimestamp.IsZero() && timestamp.After(previousTimestamp) {
				return nil, fmt.Errorf("tx search results are not ordered descending at page %d result %d", page, i)
			}
			previousTimestamp = timestamp
			if timestamp.Before(since) {
				reachedCutoff = true
				continue
			}
			if result.Code != 0 {
				continue
			}

			for messageIndex, message := range response.Txs[i].Body.Messages {
				if message.TypeURL != redelegationTypeURL || message.Amount.Denom != "uatom" {
					continue
				}
				if result.TxHash == "" || message.SourceValidator == "" || message.DestValidator == "" {
					return nil, fmt.Errorf("successful redelegation on page %d result %d is missing hash or validator address", page, i)
				}
				amountUAtom, err := strconv.ParseUint(message.Amount.Amount, 10, 64)
				if err != nil || amountUAtom == 0 {
					return nil, fmt.Errorf("successful redelegation %s message %d has invalid uatom amount %q",
						result.TxHash, messageIndex, message.Amount.Amount)
				}
				event := RedelegationEvent{
					TxHash:       strings.ToUpper(result.TxHash),
					MessageIndex: messageIndex,
					Timestamp:    timestamp,
					Source:       message.SourceValidator,
					Destination:  message.DestValidator,
					AmountUAtom:  float64(amountUAtom),
				}
				seen[event.key()] = event
			}
		}

		if reachedCutoff {
			return sortedRedelegationEvents(seen), nil
		}

		rowsConsumed := rowsBefore + len(response.TxResponses)
		if total > 0 && rowsConsumed >= total {
			return sortedRedelegationEvents(seen), nil
		}
		if len(response.TxResponses) < redelegationPageLimit {
			if total > rowsConsumed {
				return nil, fmt.Errorf("tx search page %d was short after %d of reported total %d rows", page, rowsConsumed, total)
			}
			return sortedRedelegationEvents(seen), nil
		}
	}

	return nil, fmt.Errorf("tx search exceeded the %d-page bound before reaching cutoff or completion", redelegationMaxPages)
}

func parseTxSearchTotal(raw string) (int, error) {
	if raw == "" {
		return 0, nil
	}
	total, err := strconv.Atoi(raw)
	if err != nil || total < 0 {
		return 0, fmt.Errorf("invalid pagination total %q", raw)
	}
	return total, nil
}

func sortedRedelegationEvents(events map[string]RedelegationEvent) []RedelegationEvent {
	out := make([]RedelegationEvent, 0, len(events))
	for _, event := range events {
		out = append(out, event)
	}
	sortRedelegationEvents(out)
	return out
}

func sortRedelegationEvents(events []RedelegationEvent) {
	sort.Slice(events, func(i, j int) bool {
		if events[i].Timestamp.Equal(events[j].Timestamp) {
			return events[i].key() < events[j].key()
		}
		return events[i].Timestamp.Before(events[j].Timestamp)
	})
}
