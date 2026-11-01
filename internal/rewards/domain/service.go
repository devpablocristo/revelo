package domain

import (
	"context"
	"time"
)

// Settlement is a validated payment, without any broker delivery metadata.
type Settlement struct {
	PaymentID   string
	MemberID    string
	AmountCents int64
	SettledAt   time.Time
}

// Service implements the reward use cases through an atomic ledger port.
type Service struct {
	ledger Ledger
	policy Policy
}

func NewService(ledger Ledger, policy Policy) *Service {
	return &Service{ledger: ledger, policy: policy}
}

func (s *Service) Award(ctx context.Context, payment Settlement) error {
	return s.ledger.Award(ctx, Entry{
		MemberID:  payment.MemberID,
		PaymentID: payment.PaymentID,
		Kind:      KindAward,
		Points:    s.policy.Points(payment.AmountCents),
		Month:     Month(payment.SettledAt),
	}, s.policy.MonthlyCap)
}

func (s *Service) Reverse(ctx context.Context, paymentID string) error {
	return s.ledger.Reverse(ctx, paymentID)
}
