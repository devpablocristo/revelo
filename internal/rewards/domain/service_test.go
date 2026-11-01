package domain_test

import (
	"context"
	"testing"
	"time"

	"github.com/rentpoints/pointsd/internal/ledger"
	"github.com/rentpoints/pointsd/internal/rewards/domain"
)

func TestService_WorksWithoutDeliveryAdapter(t *testing.T) {
	store := ledger.NewMemoryStore()
	var commands domain.Ledger = store
	service := domain.NewService(commands, domain.Policy{PointsPerDollar: 2, MonthlyCap: 3000})
	ctx := context.Background()
	settledAt := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)

	for _, paymentID := range []string{"pay_1", "pay_2"} {
		err := service.Award(ctx, domain.Settlement{
			PaymentID:   paymentID,
			MemberID:    "mbr_1",
			AmountCents: 100_000,
			SettledAt:   settledAt,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	// The second payment earns only the remaining 1,000 points under this policy.
	if err := service.Reverse(ctx, "pay_2"); err != nil {
		t.Fatal(err)
	}
	got, err := store.Balance(ctx, "mbr_1")
	if err != nil || got != 2000 {
		t.Fatalf("balance = %d, err = %v; want 2000", got, err)
	}
	used, err := store.AwardedInMonth(ctx, "mbr_1", "2026-10")
	if err != nil || used != 3000 {
		t.Fatalf("monthly awards = %d, err = %v; want 3000", used, err)
	}
}
