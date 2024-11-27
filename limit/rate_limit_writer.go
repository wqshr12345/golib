package limit

import (
	"encoding/binary"
	"fmt"
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
	bandwidths     []int64
	bwIdx          int
}

func NewRateLimitedWriter(w io.Writer, rate int64, balance int64, limitThreshold int64) *RateLimitedWriter {
	// 根据rate和balance，将[rate-balance, rate+balance]之间的带宽均分为五份存放到bandwitdhs中
	bandwidths := make([]int64, 5)
	for i := 0; i < 5; i++ {
		bandwidths[i] = rate - balance + int64(i)*(2*balance)/4
	}
	rateLimitedWriter := &RateLimitedWriter{
		w:              w,
		oriRate:        rate,
		rate:           rate,
		balance:        balance,
		limitThreshold: limitThreshold,
		bandwidths:     bandwidths,
		bwIdx:          0,
	}
	// go rateLimitedWriter.ChangeRateTimely()
	return rateLimitedWriter
}

func (r *RateLimitedWriter) Write(data []byte) (int, error) {
	len := len(data)
	cmprThread := int(binary.LittleEndian.Uint64(data[50:58]))
	// fmt.Println("trans len: ", len)
	startTs := time.Now()
	n, err := r.w.Write(data)
	rate := atomic.LoadInt64(&r.rate)
	sleepTime := float64(len)/float64(rate) - time.Since(startTs).Seconds()
	sleepTime *= float64(cmprThread)
	fmt.Println("cmprThread", cmprThread)
	// fmt.Println("realTime", time.Since(startTs).Seconds())
	// 元数据不sleep
	if sleepTime > 0 && n > int(r.limitThreshold) {
		// fmt.Println("sleepTime: ", sleepTime)
		time.Sleep(time.Duration(sleepTime * float64(time.Second)))
	}
	return n, err
}

func (r *RateLimitedWriter) ChangeRateTimely() {
	rand.Seed(1111)
	for {
		time.Sleep(time.Second * 3)
		// 做固定波动实验
		// rand.Seed(time.Now().UnixNano())
		if r.balance != 0 {
			atomic.StoreInt64(&r.rate, rand.Int63n(2*r.balance)+r.oriRate-r.balance)
		}
	}
}

// 为了模拟固定的波动带宽
func (r *RateLimitedWriter) ChangeRate() {
	r.bwIdx = (r.bwIdx + 1) % 5
	// fmt.Println("update bandwitdh", r.bandwidths[r.bwIdx])
	atomic.StoreInt64(&r.rate, r.bandwidths[r.bwIdx])
}
