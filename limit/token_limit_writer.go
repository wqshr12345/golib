package limit

import (
	"fmt"
	"io"
	"math/rand"
	"time"
)

type RateLimiter struct {
	randomGen *rand.Rand

	refill_period_ms_        int64
	rate_bytes_per_sec_      float64
	refill_bytes_per_period_ float64
	balance_bytes_base_      float64
	balance_update_period_   int64

	balance_bytes_random_ float64
	balance_period_count_ int64
	available_bytes_      float64
	next_refill_ms_       int64

	w              io.Writer
	limitThreshold int64

	bandwidths []int64
	bwIdx      int
}

func NewRateLimiter(w io.Writer, bandwidth float64, balance float64, periodMs int64, balance_period_sec int64, limitThreshold int64) *RateLimiter {
	source := rand.NewSource(time.Now().UnixNano())
	bandwidths := make([]int64, 5)
	for i := 0; i < 5; i++ {
		bandwidths[i] = int64(bandwidth - balance + float64(i)*(2*balance)/4)
	}
	return &RateLimiter{
		randomGen:                rand.New(source),
		refill_period_ms_:        periodMs,
		rate_bytes_per_sec_:      bandwidth,
		refill_bytes_per_period_: bandwidth * float64(periodMs) / 1000,
		balance_bytes_base_:      balance * float64(periodMs) / 1000,
		balance_update_period_:   balance_period_sec * 1000 / periodMs,

		balance_bytes_random_: 0,
		balance_period_count_: balance_period_sec * 1000 / periodMs,

		available_bytes_: 0,
		next_refill_ms_:  time.Now().UnixNano() / 1e6,
		w:                w,
		limitThreshold:   limitThreshold,
		bandwidths:       bandwidths,
		bwIdx:            0,
	}
}

func (r *RateLimiter) Write(data []byte) (int, error) {
	len := len(data)
	startTime := time.Now()
	if len > int(r.limitThreshold) {
		r.Request(int64(len))
	}
	endTime := time.Now()
	n, err := r.w.Write(data)
	endTime2 := time.Now()
	if len > int(r.limitThreshold) {
		fmt.Println("len: ", len)
		fmt.Println("time1: ", endTime.Sub(startTime).Seconds())
		fmt.Println("time2: ", endTime2.Sub(endTime).Seconds())
	}
	return n, err
}
func (rl *RateLimiter) Request(bytes_user int64) {
	bytes := float64(bytes_user)
	if rl.available_bytes_ > 0 {
		bytes_through := min(rl.available_bytes_, bytes)
		rl.available_bytes_ -= bytes_through
		bytes -= bytes_through
	}

	for bytes > 0 {
		time_until_refill_ns := rl.next_refill_ms_*1e6 - time.Now().UnixNano()
		if time_until_refill_ns > 0 {
			time.Sleep(time.Duration(time_until_refill_ns))
		}
		rl.next_refill_ms_ = time.Now().UnixNano()/1e6 + rl.refill_period_ms_
		rl.balance_period_count_--
		if rl.balance_period_count_ == 0 {
			rl.balance_bytes_random_ = -rl.balance_bytes_base_ + rl.randomGen.Float64()*rl.balance_bytes_base_*2
			rl.balance_period_count_ = rl.balance_update_period_
		}
		rl.available_bytes_ = rl.refill_bytes_per_period_ + rl.balance_bytes_random_

		bytes_through := max(min(rl.available_bytes_, bytes), 0)
		rl.available_bytes_ -= bytes_through
		bytes -= bytes_through
	}
}

func (rl *RateLimiter) ChangeRate() {
	rl.bwIdx = (rl.bwIdx + 1) % 5
	// fmt.Println("update bandwitdh", rl.bandwidths[rl.bwIdx])
	rl.rate_bytes_per_sec_ = float64(rl.bandwidths[rl.bwIdx])
}
