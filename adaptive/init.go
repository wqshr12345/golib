package adaptive

import (
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/wqshr12345/golib/adaptive/aggregate"
	"github.com/wqshr12345/golib/common"
)

type Initer struct {
	w io.Writer

	monitor   *Monitor
	div       *Divider
	agg       *aggregate.Aggregator
	cmpr      *Compressor
	tsport    *Transporter
	inputBuf  chan []byte
	divideBuf chan common.DataWithInfo
	aggBuf    chan *aggregate.AggregateData
	outputBuf chan []byte

	outputBufSize atomic.Int64
}

func NewIniter(w io.Writer, bufferSize int, cpuUsage float64) *Initer {
	monitor := NewMonitor()
	i := &Initer{
		w:         w,
		inputBuf:  make(chan []byte, 10),
		divideBuf: make(chan common.DataWithInfo, 10),
		aggBuf:    make(chan *aggregate.AggregateData, 10),
		outputBuf: make(chan []byte, 10),
		monitor:   monitor,
		div:       NewDivider(bufferSize),
		agg:       aggregate.NewAggregator(),
		cmpr:      NewCompressor(monitor, cpuUsage),
		tsport:    NewTransporter(monitor, w),
	}
	// go i.agg.Aggregate(i.divideBuf, i.aggBuf)
	// go i.cmpr.Compress(i.aggBuf, i.outputBuf)
	return i
}

func (i *Initer) SendBinlogData(data []byte) {
	i.inputBuf <- data
}

// func (i *Initer) TestAgg() {
// 	i.agg.Aggregate(i.divideBuf, i.aggBuf)
// }

// func (i *Initer) TestCompress() {
// 	i.cmpr.Compress(i.aggBuf, i.outputBuf)
// }

// func (i *Initer) TestTransport() {
// 	i.tsport.Transport(i.outputBuf)
// }

// 不同线程开始运作
func (i *Initer) Start() {
	go i.div.divide(i.inputBuf, i.divideBuf)
	go i.agg.Aggregate(i.divideBuf, i.aggBuf)
	go i.cmpr.Compress(i.aggBuf, i.outputBuf, &i.outputBufSize)
	go i.tsport.Transport(i.outputBuf, &i.outputBufSize)
	go i.TestTransBufferSize()
}

// 始终用一个压缩方法
func (i *Initer) TestByCmprType(cmprType byte) {
	go i.div.divide(i.inputBuf, i.divideBuf)
	go i.cmpr.TestByCmprType(i.divideBuf, i.outputBuf, cmprType)
	go i.tsport.Transport(i.outputBuf, &i.outputBufSize)
}

// 自适应，每个buffer只能用一个压缩方法，最优的策略

func (i *Initer) TestOneBest() {
	go i.div.divide(i.inputBuf, i.divideBuf)
	go i.cmpr.TestOneBest(i.divideBuf, i.outputBuf)
	go i.tsport.Transport(i.outputBuf, &i.outputBufSize)
}

func (i *Initer) TestMultiBest() {
	go i.div.divide(i.inputBuf, i.divideBuf)
	go i.cmpr.TestMultiBest(i.divideBuf, i.outputBuf)
	go i.tsport.Transport(i.outputBuf, &i.outputBufSize)
}

func (i *Initer) TestRtcOneBest() {
	go i.div.divide(i.inputBuf, i.divideBuf)
	go i.agg.Aggregate(i.divideBuf, i.aggBuf)
	go i.cmpr.TestRtcOneBest(i.aggBuf, i.outputBuf, &i.outputBufSize)
	go i.tsport.Transport(i.outputBuf, &i.outputBufSize)
}

func (i *Initer) TestTransBufferSize() {
	for {
		fmt.Println("transport buffer size", i.outputBufSize.Load())
		// sleep 1s
		time.Sleep(time.Second * 5)
	}
}
