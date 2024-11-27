package adaptive

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync/atomic"
	"time"
)

type Transporter struct {
	monitor        *Monitor
	w              io.Writer
	testTimes      int64
	limitThreshold int64
	needLimit      bool
	netBw          float64 // byte/s
}

func NewTransporter(monitor *Monitor, w io.Writer, limitThreshold int64, needLimit bool, netBw float64) *Transporter {
	return &Transporter{
		monitor:        monitor,
		w:              w,
		limitThreshold: limitThreshold,
		needLimit:      needLimit,
		netBw:          netBw,
	}
}

func (t *Transporter) Transport(input <-chan []byte, cmprBufSize *atomic.Int64, outputBufSize *atomic.Int64) {
	// 只负责传输一个个的package
	for {
		data := <-input
		// outputSize := outputBufSize.Load()
		// cmprSize := cmprBufSize.Load()
		// fmt.Println("Memory Size 1: ", outputSize)
		// fmt.Println("Memory Size 2: ", cmprSize)
		// fmt.Println("Memory Size Max: ", max(outputSize, cmprSize))
		// fmt.Println("Memory Size Sum: ", outputSize+cmprSize)
		binary.LittleEndian.PutUint64(data[34:42], uint64(time.Now().UnixNano()))
		// fmt.Println("netBw", t.netBw)
		binary.LittleEndian.PutUint64(data[42:50], uint64(t.netBw))
		outputBufSize.Add(-int64(len(data)))
		startTs := time.Now()
		if len(data) > 34 {
			// fmt.Println("第", t.testTimes, "个buffer传输开始,时间为: ", startTs.UnixNano())
		}
		n, err := t.w.Write(data)
		endTs := time.Now()
		if len(data) > 34 {
			// fmt.Println("第", t.testTimes, "个buffer传输结束,时间为: ", endTs.UnixNano())
		}
		// bw := float64(n) / float64(endTs.UnixNano()-startTs.UnixNano()) * float64(time.Second)
		bw := float64(n) / float64(endTs.Sub(startTs).Seconds())
		fmt.Println("transport bandwidth: ", bw)
		// fmt.Println("write data: ", n, "bytes")
		// 有些start package和end package，不应该更新bw。
		// if n > int(t.limitThreshold) {
		// fmt.Println("transport data len: ", n)
		t.monitor.UpdateBandwidth(bw)
		t.testTimes++
		// }
		// fmt.Println("transport package", t.testTimes)
		// 每当传输50个包，就更新一次bw
		// if t.testTimes%50 == 0 && t.needLimit {
		// 	t.w.(*limit.RateLimitedWriter).ChangeRate()
		// }
		if err != nil {
			panic(err)
		}
	}
}

// 用于上云实验，无需进行带宽变化波动
func (t *Transporter) Transport2(input <-chan []byte, outputBufSize *atomic.Int64) {
	// 只负责传输一个个的package
	for {
		data := <-input
		outputBufSize.Add(-int64(len(data)))
		fmt.Println("transport buffer size: ", outputBufSize.Load())
		startTs := time.Now()
		n, err := t.w.Write(data)
		endTs := time.Now()
		// bw := float64(n) / float64(endTs.UnixNano()-startTs.UnixNano()) * float64(time.Second)
		bw := float64(n) / float64(endTs.Sub(startTs).Seconds())
		// fmt.Println("transport bandwidth: ", bw)
		// fmt.Println("write data: ", n, "bytes")
		// 有些start package和end package，不应该更新bw。
		if n > int(t.limitThreshold) {
			// fmt.Println("transport bandwidth2: ", bw)
			// fmt.Println("transport data len: ", n)
			t.monitor.UpdateBandwidth(bw)
			t.testTimes++
		}
		// fmt.Println("transport package", t.testTimes)
		// 每当传输50个包，就更新一次bw

		if err != nil {
			panic(err)
		}
	}
}
