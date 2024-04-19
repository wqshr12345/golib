package adaptive

import (
	"io"
	"sync/atomic"
	"time"
)

type Transporter struct {
	monitor   *Monitor
	w         io.Writer
	testTimes int64
}

func NewTransporter(monitor *Monitor, w io.Writer) *Transporter {
	return &Transporter{
		monitor: monitor,
		w:       w,
	}
}

func (t *Transporter) Transport(input <-chan []byte, outputBufSize *atomic.Int64) {
	// 只负责传输一个个的package
	for {
		data := <-input
		outputBufSize.Add(-int64(len(data)))
		startTs := time.Now()
		n, err := t.w.Write(data)
		endTs := time.Now()
		// bw := float64(n) / float64(endTs.UnixNano()-startTs.UnixNano()) * float64(time.Second)
		bw := float64(n) / float64(endTs.Sub(startTs).Seconds())
		// fmt.Println("transport bandwidth: ", bw)
		t.monitor.UpdateBandwidth(bw)
		// fmt.Println("transport package", t.testTimes)
		t.testTimes++
		if err != nil {
			panic(err)
		}
	}
}
