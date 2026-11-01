// Package ledger stores points entries.
//
// In production the Store is backed by Postgres (table ledger_entries).
// MemoryStore is the stand-in used for local runs and tests.
package ledger

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/rentpoints/pointsd/internal/rewards/domain"
)

// Aliases preserve the starter's API while the reward model lives in the core.
type Kind = domain.Kind

const (
	KindAward    = domain.KindAward
	KindReversal = domain.KindReversal
)

// Entry is one line of a member's points ledger.
type Entry = domain.Entry

// ErrUnavailable is returned when the ledger cannot be reached. Retrying may succeed.
var ErrUnavailable = errors.New("ledger: unavailable")

// ErrUnknownPayment is returned when a reversal has no recorded award.
var ErrUnknownPayment = domain.ErrUnknownPayment

// Store owns the atomicity of reward writes, including across service replicas.
// Implementations must commit the duplicate check, cap calculation and entry
// together; separate HasEntry/AwardedInMonth/Append calls cannot do this.
type Store interface {
	domain.Ledger

	HasEntry(ctx context.Context, paymentID string, kind Kind) (bool, error)
	AwardedInMonth(ctx context.Context, memberID, month string) (int64, error)
	// Append is a raw write for fixtures and local setup, without reward rules.
	// Reward processing must use Award and Reverse instead.
	Append(ctx context.Context, e Entry) error
	Balance(ctx context.Context, memberID string) (int64, error)
}

type Option func(*MemoryStore)

// WithLatency delays every call, approximating a database round trip.
func WithLatency(d time.Duration) Option {
	return func(s *MemoryStore) { s.latency = d }
}

// WithFaults installs a hook run at the start of every call (after the
// round trip). A non-nil error is returned to the caller and the call has no effect.
func WithFaults(f func(op string) error) Option {
	return func(s *MemoryStore) { s.faults = f }
}

type MemoryStore struct {
	latency time.Duration
	faults  func(op string) error

	mu      sync.Mutex
	entries []Entry
}

var _ Store = (*MemoryStore)(nil)

func NewMemoryStore(opts ...Option) *MemoryStore {
	s := &MemoryStore{}
	for _, o := range opts {
		o(s)
	}
	return s
}

func (s *MemoryStore) roundTrip(ctx context.Context, op string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.latency > 0 {
		select {
		case <-time.After(s.latency):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if s.faults != nil {
		if err := s.faults(op); err != nil {
			return err
		}
	}
	return ctx.Err()
}

// Award models one database transaction. The simulated round trip is outside
// the lock so unrelated members do not wait on each other's I/O. This mutex is
// shared by all handlers using this store; it is not a production replica lock.
func (s *MemoryStore) Award(ctx context.Context, e Entry, monthlyCap int64) error {
	if err := s.roundTrip(ctx, "award"); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}

	var used int64
	for _, entry := range s.entries {
		if entry.Kind != KindAward {
			continue
		}
		if entry.PaymentID == e.PaymentID {
			return nil
		}
		if entry.MemberID == e.MemberID && entry.Month == e.Month {
			used += entry.Points
		}
	}
	s.entries = append(s.entries, e.CappedAward(used, monthlyCap))
	return nil
}

func (s *MemoryStore) Reverse(ctx context.Context, paymentID string) error {
	if err := s.roundTrip(ctx, "reverse"); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}

	var award Entry
	found := false
	for _, entry := range s.entries {
		if entry.PaymentID != paymentID {
			continue
		}
		if entry.Kind == KindReversal {
			return nil
		}
		if entry.Kind == KindAward {
			award, found = entry, true
		}
	}
	if !found {
		return ErrUnknownPayment
	}
	s.entries = append(s.entries, award.Reversal())
	return nil
}

func (s *MemoryStore) HasEntry(ctx context.Context, paymentID string, kind Kind) (bool, error) {
	if err := s.roundTrip(ctx, "has_entry"); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.entries {
		if e.PaymentID == paymentID && e.Kind == kind {
			return true, nil
		}
	}
	return false, nil
}

func (s *MemoryStore) AwardedInMonth(ctx context.Context, memberID, month string) (int64, error) {
	if err := s.roundTrip(ctx, "awarded_in_month"); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var total int64
	for _, e := range s.entries {
		if e.MemberID == memberID && e.Month == month && e.Kind == KindAward {
			total += e.Points
		}
	}
	return total, nil
}

func (s *MemoryStore) Append(ctx context.Context, e Entry) error {
	if err := s.roundTrip(ctx, "append"); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = append(s.entries, e)
	return nil
}

func (s *MemoryStore) Balance(ctx context.Context, memberID string) (int64, error) {
	if err := s.roundTrip(ctx, "balance"); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var total int64
	for _, e := range s.entries {
		if e.MemberID == memberID {
			total += e.Points
		}
	}
	return total, nil
}
