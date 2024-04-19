package limit

import (
	"io"
	"time"
)

type RateLimitedWriter struct {
	w          io.Writer
	rate       float64 // 每秒写入速率（字节）
	lastUpdate time.Time
}

func NewRateLimitedWriter(w io.Writer, rate float64) *RateLimitedWriter {
	return &RateLimitedWriter{
		w:    w,
		rate: rate,
	}
}

func (r *RateLimitedWriter) Write(data []byte) (int, error) {
	len := len(data)
	startTs := time.Now()
	n, err := r.w.Write(data)
	sleepTime := float64(len)/r.rate - time.Since(startTs).Seconds()
	if sleepTime > 0 {
		time.Sleep(time.Duration(sleepTime * float64(time.Second)))
	}
	return n, err
}
