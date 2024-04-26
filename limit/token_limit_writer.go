package limit

import (
	"fmt"
	"io"
	"time"
)

type RateLimiter struct {
	refill_period_ms_        int64
	rate_bytes_per_sec_      float64
	refill_bytes_per_period_ float64

	available_bytes_ float64
	next_refill_ms_  int64
	w                io.Writer
	limitThreshold   int64
}

func NewRateLimiter(w io.Writer, bandwidth float64, periodMs int64, limitThreshold int64) *RateLimiter {
	return &RateLimiter{
		refill_period_ms_:        periodMs,
		rate_bytes_per_sec_:      bandwidth,
		refill_bytes_per_period_: bandwidth * float64(periodMs) / 1000,

		available_bytes_: 0,
		next_refill_ms_:  time.Now().UnixNano() / 1e6,
		w:                w,
		limitThreshold:   limitThreshold,
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
		rl.available_bytes_ = rl.refill_bytes_per_period_

		bytes_through := min(rl.available_bytes_, bytes)
		rl.available_bytes_ -= bytes_through
		bytes -= bytes_through
	}
}
