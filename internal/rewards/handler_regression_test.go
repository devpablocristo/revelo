package rewards_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rentpoints/pointsd/internal/ledger"
	"github.com/rentpoints/pointsd/internal/rewards"
)

func TestAward_TruncatesCents(t *testing.T) {
	for _, tc := range []struct {
		name   string
		cents  int64
		points int64
	}{
		{name: "less_than_one_dollar", cents: 99, points: 0},
		{name: "whole_dollars_and_cents", cents: 245099, points: 2450},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := ledger.NewMemoryStore()
			ev := settled("pay_1", "mbr_1", 1)
			ev.AmountCents = tc.cents
			deliver(t, newHandler(store), ev)
			if got := balance(t, store, "mbr_1"); got != tc.points {
				t.Fatalf("balance = %d, want %d", got, tc.points)
			}
		})
	}
}

func TestAward_ZeroPointPaymentIsRemembered(t *testing.T) {
	store := ledger.NewMemoryStore()
	h := newHandler(store)
	deliver(t, h, settled("pay_full", "mbr_1", 10_000))
	zero := settled("pay_zero", "mbr_1", 1000)
	deliver(t, h, zero)

	// A republish cannot turn an already processed zero award into a new award.
	zero.EventID = "evt_republished"
	zero.OccurredAt = oct.AddDate(0, 1, 0)
	deliver(t, h, zero)
	deliver(t, h, reversed("pay_zero", "mbr_1", 1000))

	for _, kind := range []ledger.Kind{ledger.KindAward, ledger.KindReversal} {
		found, err := store.HasEntry(context.Background(), "pay_zero", kind)
		if err != nil || !found {
			t.Fatalf("zero payment entry %s: found = %v, err = %v", kind, found, err)
		}
	}
	if got := balance(t, store, "mbr_1"); got != 10_000 {
		t.Fatalf("balance = %d, want 10000", got)
	}
}

func TestAward_RedeliveryAfterReversalDoesNotReaward(t *testing.T) {
	for _, tc := range []struct {
		name       string
		seedPoints int64
		payment    int64
		awarded    int64
	}{
		{name: "full_award", seedPoints: 0, payment: 1200, awarded: 1200},
		{name: "capped_award", seedPoints: 9500, payment: 1000, awarded: 500},
		{name: "zero_award", seedPoints: 10_000, payment: 1000, awarded: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := ledger.NewMemoryStore(ledger.WithLatency(dbLatency))
			replicas := []*rewards.Handler{newHandler(store), newHandler(store), newHandler(store)}
			if tc.seedPoints > 0 {
				deliver(t, replicas[0], settled("pay_seed", "mbr_1", tc.seedPoints))
			}
			payment := settled("pay_1", "mbr_1", tc.payment)
			deliver(t, replicas[0], payment)
			deliver(t, replicas[1], reversed("pay_1", "mbr_1", tc.payment))

			// Even a republish in a later month must keep the original award
			// and reversal, rather than creating a second lifecycle for the payment.
			payment.EventID = "evt_republished_after_reversal"
			payment.OccurredAt = oct.AddDate(0, 1, 0)
			var fns []func()
			for i := 0; i < 9; i++ {
				h := replicas[i%len(replicas)]
				fns = append(fns, func() { deliver(t, h, payment) })
			}
			concurrently(fns...)
			deliver(t, replicas[2], reversed("pay_1", "mbr_1", tc.payment))

			if got := balance(t, store, "mbr_1"); got != tc.seedPoints {
				t.Fatalf("balance = %d, want %d; redelivery must not resurrect a reversed payment", got, tc.seedPoints)
			}
			for _, month := range []struct {
				name string
				want int64
			}{
				{name: rewards.Month(oct), want: tc.seedPoints + tc.awarded},
				{name: rewards.Month(payment.OccurredAt), want: 0},
			} {
				got, err := store.AwardedInMonth(context.Background(), "mbr_1", month.name)
				if err != nil || got != month.want {
					t.Fatalf("awarded in %s = %d, err = %v; want %d", month.name, got, err, month.want)
				}
			}
		})
	}
}

func TestReversal_ConcurrentRedeliveryAcrossReplicasIsAppliedOnce(t *testing.T) {
	store := ledger.NewMemoryStore(ledger.WithLatency(dbLatency))
	replicas := []*rewards.Handler{newHandler(store), newHandler(store), newHandler(store)}
	deliver(t, replicas[0], settled("pay_1", "mbr_1", 2000))
	var fns []func()
	for i := 0; i < 12; i++ {
		h := replicas[i%len(replicas)]
		fns = append(fns, func() { deliver(t, h, reversed("pay_1", "mbr_1", 2000)) })
	}
	concurrently(fns...)
	if got := balance(t, store, "mbr_1"); got != 0 {
		t.Fatalf("balance = %d, want 0 after exactly one reversal", got)
	}
}

func TestReversal_UsesStoredAwardInsteadOfEventOrCurrentPolicy(t *testing.T) {
	store := ledger.NewMemoryStore()
	deliver(t, newHandler(store), settled("pay_1", "mbr_1", 800))
	changedPolicy := rewards.Policy{PointsPerDollar: 5, MonthlyCap: 10_000}
	h := rewards.NewHandler(
		store,
		changedPolicy,
		&fakeDLQ{},
		newHandlerLogger(),
	)
	ev := reversed("pay_1", "mbr_other", 5000)
	ev.OccurredAt = oct.AddDate(0, 1, 0)
	deliver(t, h, ev)
	for _, member := range []string{"mbr_1", "mbr_other"} {
		if got := balance(t, store, member); got != 0 {
			t.Fatalf("%s balance = %d, want 0", member, got)
		}
	}
}

func TestReversal_DoesNotRestoreMonthlyCap(t *testing.T) {
	store := ledger.NewMemoryStore()
	h := newHandler(store)
	deliver(t, h, settled("pay_1", "mbr_1", 9500))
	deliver(t, h, settled("pay_2", "mbr_1", 1000))
	deliver(t, h, reversed("pay_2", "mbr_1", 1000))
	deliver(t, h, settled("pay_3", "mbr_1", 1000))
	if got := balance(t, store, "mbr_1"); got != 9500 {
		t.Fatalf("balance = %d, want 9500; reversals must not free cap room", got)
	}
}

func TestAward_UsesUTCSettlementMonth(t *testing.T) {
	store := ledger.NewMemoryStore()
	h := newHandler(store)
	deliver(t, h, settled("pay_oct", "mbr_1", 10_000))
	ev := settled("pay_offset", "mbr_1", 1000)
	// November locally, but still October in UTC: the cap is already exhausted.
	ev.OccurredAt = time.Date(2026, 11, 1, 0, 30, 0, 0, time.FixedZone("UTC+2", 2*60*60))
	deliver(t, h, ev)
	if got := balance(t, store, "mbr_1"); got != 10_000 {
		t.Fatalf("balance = %d, want 10000", got)
	}
}

func TestHandle_LedgerErrorNacksWithoutWritingAndCanBeRetried(t *testing.T) {
	for _, tc := range []struct {
		name    string
		reverse bool
	}{
		{name: "award", reverse: false},
		{name: "reversal", reverse: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var unavailable atomic.Bool
			store := ledger.NewMemoryStore(ledger.WithFaults(func(string) error {
				if unavailable.Load() {
					return fmt.Errorf("maintenance: %w", ledger.ErrUnavailable)
				}
				return nil
			}))
			h := newHandler(store)
			ev := settled("pay_1", "mbr_1", 1200)
			before, after := int64(0), int64(1200)
			if tc.reverse {
				deliver(t, h, ev)
				ev = reversed("pay_1", "mbr_1", 1200)
				before, after = 1200, 0
			}

			unavailable.Store(true)
			msg := newMessage(t, ev)
			h.Handle(context.Background(), msg)
			if msg.outcome(t) != nacked {
				t.Fatal("ledger error must nack exactly once")
			}
			unavailable.Store(false)
			if got := balance(t, store, "mbr_1"); got != before {
				t.Fatalf("failed operation changed balance to %d, want %d", got, before)
			}
			deliver(t, h, ev)
			if got := balance(t, store, "mbr_1"); got != after {
				t.Fatalf("balance after retry = %d, want %d", got, after)
			}
		})
	}
}

func TestHandle_CanceledContextNacksWithoutWriting(t *testing.T) {
	store := ledger.NewMemoryStore()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	msg := newMessage(t, settled("pay_1", "mbr_1", 1200))
	newHandler(store).Handle(ctx, msg)
	if msg.outcome(t) != nacked {
		t.Fatal("canceled operation must nack exactly once")
	}
	if got := balance(t, store, "mbr_1"); got != 0 {
		t.Fatalf("balance = %d, want 0", got)
	}
}

type failingDLQ struct{}

func (failingDLQ) Publish(context.Context, []byte, map[string]string) error {
	return errors.New("dead-letter topic unavailable")
}

func TestHandle_DeadLetterFailureNacks(t *testing.T) {
	h := rewards.NewHandler(
		ledger.NewMemoryStore(),
		rewards.DefaultPolicy,
		failingDLQ{},
		newHandlerLogger(),
	)
	msg := &fakeMessage{id: "invalid", data: []byte(`{"type":"unknown"}`), attempt: 1}
	h.Handle(context.Background(), msg)
	if msg.outcome(t) != nacked {
		t.Fatal("failed dead-letter publication must nack exactly once")
	}
}

// lostCommitReplyStore models a committed transaction whose response is lost.
// The handler must retry safely even though the first write already happened.
type lostCommitReplyStore struct {
	ledger.Store
	failNext atomic.Bool
}

func (s *lostCommitReplyStore) Award(ctx context.Context, e ledger.Entry, cap int64) error {
	if err := s.Store.Award(ctx, e, cap); err != nil {
		return err
	}
	if s.failNext.Swap(false) {
		return ledger.ErrUnavailable
	}
	return nil
}

func (s *lostCommitReplyStore) Reverse(ctx context.Context, paymentID string) error {
	if err := s.Store.Reverse(ctx, paymentID); err != nil {
		return err
	}
	if s.failNext.Swap(false) {
		return ledger.ErrUnavailable
	}
	return nil
}

func TestHandle_LostCommitReplyCanBeRetriedWithoutDuplicateEffects(t *testing.T) {
	store := &lostCommitReplyStore{Store: ledger.NewMemoryStore()}
	h := newHandler(store)
	for _, ev := range []event{
		settled("pay_1", "mbr_1", 1200),
		reversed("pay_1", "mbr_1", 1200),
	} {
		store.failNext.Store(true)
		msg := deliver(t, h, ev)
		if msg.attempt != 2 {
			t.Fatalf("delivery attempt = %d, want 2 after lost commit reply", msg.attempt)
		}
		want := int64(1200)
		if ev.Type == "payment.reversed" {
			want = 0
		}
		if got := balance(t, store, "mbr_1"); got != want {
			t.Fatalf("balance = %d, want %d", got, want)
		}
	}
}
