package domain_test

import (
	"testing"

	"github.com/rentpoints/pointsd/internal/rewards/domain"
)

func TestEntry_CappedAward(t *testing.T) {
	for _, tc := range []struct {
		name      string
		requested int64
		used      int64
		want      int64
	}{
		{name: "within_cap", requested: 1200, used: 2000, want: 1200},
		{name: "partially_capped", requested: 1000, used: 9500, want: 500},
		{name: "cap_exhausted", requested: 1000, used: 10_000, want: 0},
		{name: "no_negative_award", requested: 1000, used: 11_000, want: 0},
		{name: "zero_award", requested: 0, used: 0, want: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate := domain.Entry{
				MemberID: "mbr_1", PaymentID: "pay_1", Points: tc.requested, Month: "2026-10",
			}
			got := candidate.CappedAward(tc.used, 10_000)
			want := domain.Entry{
				MemberID: "mbr_1", PaymentID: "pay_1", Kind: domain.KindAward, Points: tc.want, Month: "2026-10",
			}
			if got != want {
				t.Fatalf("award = %+v, want %+v", got, want)
			}
			if candidate.Points != tc.requested || candidate.Kind != "" {
				t.Fatal("capping mutated the input entry")
			}
		})
	}
}

func TestEntry_ReversalPreservesOriginalMetadata(t *testing.T) {
	award := domain.Entry{
		MemberID: "mbr_1", PaymentID: "pay_1", Kind: domain.KindAward, Points: 500, Month: "2026-10",
	}
	want := domain.Entry{
		MemberID: "mbr_1", PaymentID: "pay_1", Kind: domain.KindReversal, Points: -500, Month: "2026-10",
	}
	if got := award.Reversal(); got != want {
		t.Fatalf("reversal = %+v, want %+v", got, want)
	}
	if award.Points != 500 || award.Kind != domain.KindAward {
		t.Fatal("reversing mutated the original award")
	}
}
