package statistics

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

func NewBandwidthStatistics(output *os.File) *bandwidth {
	b := bandwidth{
		done:           make(chan struct{}),
		report:         make(chan string, 10),
		cumulativeTime: 0,
		countBytes:     0,
		packages:       0,
		output:         io.Writer(output),
	}
	go b.printResult()
	return &b
}

type bandwidth struct {
	done   chan struct{}
	report chan string
	/* 在一个bandwidth的实例中，beginTime,cumulativeTime只有一个会被用到
	 * 使用beginTime的用AddBandWidth函数接口
	 * 使用cumulativeTime的用
	 */
	beginTime      time.Time
	cumulativeTime time.Duration
	countBytes     uint64
	packages       uint32
	output         io.Writer
}

func (bw *bandwidth) SetOutputFile(file *os.File) {
	bw.output = io.Writer(file)
}

func (bw *bandwidth) AddCompressBandwidth(bytes int, begin time.Time) {
	if bytes == 0 {
		return
	}
	t := time.Since(begin)
	atomic.AddUint64(&bw.countBytes, uint64(bytes))
	atomic.AddUint32(&bw.packages, 1)
	atomic.AddInt64((*int64)(&bw.cumulativeTime), int64(t))
	if atomic.LoadUint64(&bw.countBytes) > 100*1024*1024 {
		d := atomic.SwapInt64((*int64)(&bw.cumulativeTime), 0)
		result := atomic.SwapUint64(&bw.countBytes, 0)
		seconds := (time.Duration)(d).Seconds()
		bw.report <- fmt.Sprintf("Compress Period:%dms, BandWidth: %7sBits/s\n",
			(time.Duration)(d).Milliseconds(), bytesToRate(uint64(float64(result)/seconds)))
	}
}

func (bw *bandwidth) AddBandWidth(bytes int) {
	if atomic.LoadUint32(&bw.packages) == 0 {
		bw.beginTime = time.Now()
	}
	atomic.AddUint64(&bw.countBytes, uint64(bytes))
	atomic.AddUint32(&bw.packages, 1)

	if atomic.LoadUint64(&bw.countBytes) > 40*1024*1024 {
		d := time.Since(bw.beginTime)
		seconds := d.Seconds()
		result := atomic.SwapUint64(&bw.countBytes, 0)
		bw.report <- fmt.Sprintf("[%s] Period:%dms, BandWidth: %7sBits/s\n",
			bw.beginTime.Format("2006-01-02 15:04:05"), d.Milliseconds(),
			bytesToRate(uint64(float64(result)/seconds)))

		bw.beginTime = time.Now()
	}
}

func (bw *bandwidth) printResult() {
	for true {
		select {
		case <-bw.done:
			return
		case msg := <-bw.report:
			_, err := bw.output.Write([]byte(msg))
			if err != nil {
				panic(1)
			}
		}
	}
}

const (
	UNO  = 1
	KILO = 1000
	MEGA = 1000 * 1000
	GIGA = 1000 * 1000 * 1000
	TERA = 1000 * 1000 * 1000 * 1000
)

func bytesToRate(bytes uint64) string {
	bits := bytes * 8
	result := numberToUnit(bits)
	return result
}

func numberToUnit(num uint64) string {
	unit := ""
	value := float64(num)

	switch {
	case num >= TERA:
		unit = "T"
		value = value / TERA
	case num >= GIGA:
		unit = "G"
		value = value / GIGA
	case num >= MEGA:
		unit = "M"
		value = value / MEGA
	case num >= KILO:
		unit = "K"
		value = value / KILO
	}

	result := strconv.FormatFloat(value, 'f', 2, 64)
	result = strings.TrimSuffix(result, ".00")
	return result + unit
}
