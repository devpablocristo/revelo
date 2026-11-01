package rewards

import (
	"time"

	"github.com/rentpoints/pointsd/internal/rewards/domain"
)

// Policy is the earning rule for rent payments.
type Policy = domain.Policy

var DefaultPolicy = Policy{PointsPerDollar: 1, MonthlyCap: 10_000}

// Month is the cap bucket of a settlement time.
func Month(t time.Time) string {
	return domain.Month(t)
}
