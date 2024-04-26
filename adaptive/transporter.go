package adaptive

import (
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
}

func NewTransporter(monitor *Monitor, w io.Writer, limitThreshold int64) *Transporter {
	return &Transporter{
		monitor:        monitor,
		w:              w,
		limitThreshold: limitThreshold,
	}
}

func (t *Transporter) Transport(input <-chan []byte, outputBufSize *atomic.Int64) {
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
			fmt.Println("transport bandwidth2: ", bw)
			fmt.Println("transport data len: ", n)
			t.monitor.UpdateBandwidth(bw)
		}
		// fmt.Println("transport package", t.testTimes)
		t.testTimes++
		if err != nil {
			panic(err)
		}
	}
}
