package limit

import (
	"io"
	"math/rand"
	"sync/atomic"
	"time"
)

type RateLimitedWriter struct {
	w              io.Writer
	oriRate        int64
	rate           int64 // 每秒写入速率（字节）
	balance        int64 // 平衡
	limitThreshold int64
}

func NewRateLimitedWriter(w io.Writer, rate int64, balance int64, limitThreshold int64) *RateLimitedWriter {
	rateLimitedWriter := &RateLimitedWriter{
		w:              w,
		oriRate:        rate,
		rate:           rate,
		balance:        balance,
		limitThreshold: limitThreshold,
	}
	go rateLimitedWriter.ChangeRateTimely()
	return rateLimitedWriter
}

func (r *RateLimitedWriter) Write(data []byte) (int, error) {
	len := len(data)
	// fmt.Println("trans len: ", len)
	startTs := time.Now()
	n, err := r.w.Write(data)
	rate := atomic.LoadInt64(&r.rate)
	sleepTime := float64(len)/float64(rate) - time.Since(startTs).Seconds()
	// 元数据不sleep
	if sleepTime > 0 && n > int(r.limitThreshold) {
		// fmt.Println("sleepTime: ", sleepTime)
		time.Sleep(time.Duration(sleepTime * float64(time.Second)))
	}
	return n, err
}

func (r *RateLimitedWriter) ChangeRateTimely() {
	for {
		time.Sleep(time.Second * 3)
		rand.Seed(time.Now().UnixNano())
		if r.balance != 0 {
			atomic.StoreInt64(&r.rate, rand.Int63n(2*r.balance)+r.oriRate-r.balance)
		}
	}
}
