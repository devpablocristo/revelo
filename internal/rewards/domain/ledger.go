// Package domain contains reward rules and ports, independent of transport and storage.
package domain

import (
	"context"
	"errors"
)

type Kind string

const (
	KindAward    Kind = "award"
	KindReversal Kind = "reversal"
)

// Entry is one immutable line of a member's points ledger.
type Entry struct {
	MemberID  string
	PaymentID string
	Kind      Kind
	Points    int64  // nonnegative for awards, nonpositive for reversals
	Month     string // payment settlement month, "2006-01" in UTC
}

// CappedAward returns the entry to persist, including a zero-point award.
func (e Entry) CappedAward(used, monthlyCap int64) Entry {
	e.Kind = KindAward
	e.Points = min(max(e.Points, 0), max(monthlyCap-used, 0))
	return e
}

// Reversal preserves the original payment, member and settlement month.
func (e Entry) Reversal() Entry {
	e.Kind = KindReversal
	e.Points = -e.Points
	return e
}

var ErrUnknownPayment = errors.New("rewards: reversal for unknown payment")

// Ledger is the reward write port. Each operation must be atomic across all
// replicas sharing storage; checking and writing in separate calls is unsafe.
type Ledger interface {
	// Award records a payment once, applying the monthly cap inside the same
	// transaction as the duplicate check and insert. Zero awards are recorded.
	Award(ctx context.Context, e Entry, monthlyCap int64) error
	// Reverse records the negative of the original award once, copying its
	// member and settlement month. Unknown payments return ErrUnknownPayment.
	Reverse(ctx context.Context, paymentID string) error
}
