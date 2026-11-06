# NOTES

## What was wrong

Against the unchanged starter (`1012f97`), `go test -race ./...` fails four tests in
`internal/rewards`. Each failure traces to a design issue in `rewards.Handler`, not a
typo:

| Failing test | Root cause |
|---|---|
| `TestAward_ConcurrentRedeliveryAcrossReplicasIsAwardedOnce` (16,000 instead of 2,000) | `award` is check-then-act over three round trips (`HasEntry` → `AwardedInMonth` → `Append`). Concurrent deliveries of the same payment all see "not seen" and all append. |
| `TestAward_ConcurrentPaymentsRespectMonthlyCap` (15,000 instead of 10,000) | Same race on the cap: every concurrent payment reads the same `used` and gets the full room. |
| `TestReversal_ClawsBackOnlyWhatWasAwarded` (9,000 instead of 9,500) | The reversal recomputes points from the event (`amount_cents` × current policy) instead of negating the stored award, so it removes 1,000 when the cap only awarded 500. |
| `TestHandle_TransientLedgerErrorIsRetried` | `Handle` acks every message, even when processing fails. A transient ledger error drops the event. |

Problems that no starter test covered:

- A reversal entry takes `member_id` and the month from the reversal event (`occurred_at`
  is the reversal time, not the settlement time). It also uses the current policy. So the
  written entry can name the wrong member, the wrong month or the wrong amount.
- Reversals have the same check-then-act race, so concurrent redeliveries can reverse a
  payment twice.
- With no latency or fault hook, `MemoryStore` ignored a canceled context and still wrote.

## What changed (`e88dc4a`)

**Atomic write port.** `internal/rewards/domain` now holds the rules (`Policy`, `Entry`,
`Service`) and a `Ledger` port with two operations. Each one must be atomic across replicas:

- `Award(entry, monthlyCap)`: in one transaction, it checks for a duplicate by
  `payment_id`, computes the cap room for the member and settlement month, and inserts the
  entry. A zero-point award is still recorded, so a later republish cannot award it again.
- `Reverse(paymentID)`: in one transaction, it finds the stored award and inserts its
  negation once, copying the member and settlement month. It returns `ErrUnknownPayment`
  if no award exists.

`ledger.Store` embeds this port. `HasEntry`, `AwardedInMonth` and `Append` stay on `Store`
for reads and fixtures, but reward processing no longer uses them. `ledger` re-exports the
domain types as aliases, so the starter's public API and tests still compile unchanged.

**Ack/Nack contract.** `Handle` now acts as follows:

- Success: ack.
- `ErrUnknownPayment`: log and ack. This follows the README rule that finance reconciles
  these from the logs.
- Any other error, including context cancellation: nack.
- Invalid payload: dead-letter and ack. If the dead-letter publish fails, nack.

Because both writes are idempotent by `payment_id`, a nack after a commit whose reply was
lost is safe to retry (`TestHandle_LostCommitReplyCanBeRetriedWithoutDuplicateEffects`).

**`MemoryStore`.** `Award` and `Reverse` run their check and write under the store mutex.
The simulated round trip stays outside the lock, so `TestHandle_DistinctMembersAreProcessedConcurrently`
still passes. `roundTrip` checks `ctx.Err()` before and after the simulated I/O. One mutex
across all handlers stands in for the database transaction. It is not a model of a
cross-replica lock.

## Rules as implemented

- Points equal `amount_cents / 100 × PointsPerDollar`, with cents truncated.
- The cap bucket is the settlement month in UTC (`domain.Month`).
- The cap counts awards only. A reversal does not give cap room back
  (`TestReversal_DoesNotRestoreMonthlyCap`).
- Identity is `payment_id`. A republish with a new `event_id`, a different `occurred_at`
  or a different month is a no-op, including after a reversal. The first award wins.
- A reversal ignores the event's `member_id`, `amount_cents` and `occurred_at`. Its only
  input is `payment_id`.

## Verification

```
go vet ./...                      # clean
go test -race -count=20 ./...     # all packages pass
go run ./cmd/pointsd -events testdata/events.jsonl
curl localhost:8080/v1/members/mbr_raj/points   # {"member_id":"mbr_raj","points":6100}
curl localhost:8080/v1/members/mbr_ana/points   # {"member_id":"mbr_ana","points":2450}
```

`mbr_raj` gets 6,100 for `pay_1002` and 3,900 for `pay_1003`, because the cap leaves 3,900
of its 5,900. The reversal of `pay_1003` then removes exactly those 3,900.

New tests are in `internal/rewards/handler_regression_test.go`,
`internal/rewards/domain/ledger_test.go` and `internal/rewards/domain/service_test.go`.
They cover cent truncation, zero-point awards, redelivery after a reversal (full, capped
and zero awards, including a republish in a later month), concurrent reversal redelivery,
reversals that use the stored award, a cap that a reversal does not restore, the UTC month
for non-UTC timestamps, nack-without-write on ledger errors and canceled contexts, a
failed dead-letter publish, and lost commit replies.

## Postgres implementation (not in this repo)

The `Store` contract is designed to map onto a single transaction per call:

```sql
-- Duplicate protection does not depend on locks or isolation level.
ALTER TABLE ledger_entries ADD CONSTRAINT ledger_entries_payment_kind_key UNIQUE (payment_id, kind);

-- Award: serialize writers of one member-month, then insert once.
BEGIN;
SELECT pg_advisory_xact_lock(hashtextextended($member_id || ':' || $month, 0));
SELECT COALESCE(SUM(points), 0) FROM ledger_entries
 WHERE member_id = $member_id AND month = $month AND kind = 'award';
INSERT INTO ledger_entries (member_id, payment_id, kind, points, month)
VALUES ($member_id, $payment_id, 'award', LEAST($points, GREATEST($cap - $used, 0)), $month)
ON CONFLICT (payment_id, kind) DO NOTHING;
COMMIT;

-- Reverse: negate the stored award, once.
INSERT INTO ledger_entries (member_id, payment_id, kind, points, month)
SELECT member_id, payment_id, 'reversal', -points, month
  FROM ledger_entries WHERE payment_id = $payment_id AND kind = 'award'
ON CONFLICT (payment_id, kind) DO NOTHING;
-- If no award row exists, return ErrUnknownPayment.
-- A conflict means the reversal is already recorded: return nil.
```

A per-member-month counter row (`SELECT … FOR UPDATE`) could replace the advisory lock. It
avoids the `SUM` scan and makes the lock explicit in the schema. Either way, map connection
and serialization errors to `ErrUnavailable` (or any non-`ErrUnknownPayment` error) so the
message is nacked.

## Known limitations and open questions

- **Reversal before settlement.** If a reversal arrives first, it is acked as unknown. When
  the settlement arrives later, it earns permanently. The README rule covers this case
  ("acked and logged; finance reconciles"), but out-of-order delivery is possible on
  Pub/Sub. An alternative is to record a reversal tombstone, so a later settlement is
  stored as already reversed. This needs a product decision.
- **Poison messages.** A message that always fails at the ledger is nacked forever.
  `DeliveryAttempt()` is unused. Production should configure the subscription's
  dead-letter policy (`max_delivery_attempts`) and alert on it.
- **No per-message deadline.** `Handle` uses the subscriber's context. A
  `context.WithTimeout` shorter than the ack deadline would keep a hung ledger call from
  outliving the lease.
- **Event/award mismatch is silent.** A reversal whose `member_id` or `amount_cents`
  differs from the stored award is applied using the stored values without a warning. A
  log line would help finance reconciliation.
- **`MemoryStore` scales poorly.** It scans every entry on each call and uses one global
  mutex. That is acceptable for a local stand-in, but it is not a performance model.
- **`Append` bypasses the rules.** It stays on `Store` for fixtures, such as the API test.
  It could move to a test-only interface if the production adapter should not expose it.
