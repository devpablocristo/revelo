// Package rewards turns payment events into points ledger entries.
package rewards

import (
	"context"
	"errors"
	"log/slog"

	"github.com/rentpoints/pointsd/internal/events"
	"github.com/rentpoints/pointsd/internal/ledger"
	"github.com/rentpoints/pointsd/internal/pubsub"
	"github.com/rentpoints/pointsd/internal/rewards/domain"
)

// ErrUnknownPayment is returned for a reversal of a payment that was never awarded.
var ErrUnknownPayment = domain.ErrUnknownPayment

type Handler struct {
	service    *domain.Service
	deadLetter pubsub.Publisher
	log        *slog.Logger
}

func NewHandler(
	store ledger.Store,
	policy Policy,
	deadLetter pubsub.Publisher,
	log *slog.Logger,
) *Handler {
	return &Handler{service: domain.NewService(store, policy), deadLetter: deadLetter, log: log}
}

// Handle processes one delivery. The subscriber calls it concurrently
// (MaxOutstandingMessages per replica), and pointsd runs several replicas.
func (h *Handler) Handle(ctx context.Context, msg pubsub.Message) {
	ev, err := events.Decode(msg.Data())
	if err != nil {
		h.log.Warn("dead-lettering invalid event", "msg_id", msg.ID(), "err", err)
		if err := h.deadLetter.Publish(ctx, msg.Data(), map[string]string{"reason": err.Error()}); err != nil {
			h.log.Error("dead-letter publish failed", "msg_id", msg.ID(), "err", err)
			msg.Nack()
			return
		}
		msg.Ack()
		return
	}

	if err := h.process(ctx, ev); err != nil {
		if errors.Is(err, ErrUnknownPayment) {
			h.log.Warn("reversal for unknown payment", "payment_id", ev.PaymentID, "err", err)
			msg.Ack()
			return
		}
		h.log.Error("processing failed", "payment_id", ev.PaymentID, "type", ev.Type, "err", err)
		msg.Nack()
		return
	}
	msg.Ack()
}

func (h *Handler) process(ctx context.Context, ev events.Payment) error {
	switch ev.Type {
	case events.TypeSettled:
		return h.service.Award(ctx, domain.Settlement{
			PaymentID:   ev.PaymentID,
			MemberID:    ev.MemberID,
			AmountCents: ev.AmountCents,
			SettledAt:   ev.OccurredAt,
		})
	case events.TypeReversed:
		return h.service.Reverse(ctx, ev.PaymentID)
	}
	return nil
}
