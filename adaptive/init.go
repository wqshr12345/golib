package adaptive

import (
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
	obBest    []byte
	mbBest    [][]common.CmprTypeData

	outputBufSize atomic.Int64
}

func NewIniter(w io.Writer, bufferSize int, packageSize int, cpuUsage float64, obBest []byte, mbBest [][]common.CmprTypeData, hybridBest [][]common.CompressionIntro, rate float64, limitThreshold int64, epochThreshold int64, needLimit bool) *Initer {
	monitor := NewMonitor(packageSize, rate, epochThreshold)
	i := &Initer{
		w:         w,
		inputBuf:  make(chan []byte, 10),
		divideBuf: make(chan common.DataWithInfo, 10),
		aggBuf:    make(chan *aggregate.AggregateData, 10), // IMP 应该大于bufferSize / packageSize
		outputBuf: make(chan []byte, 10),                   //TODOIMP 减少transmission buffer大小...
		obBest:    obBest,
		mbBest:    mbBest,
		monitor:   monitor,
		div:       NewDivider(bufferSize),
		agg:       aggregate.NewAggregator(),
		cmpr:      NewCompressor(monitor, cpuUsage, obBest, mbBest, hybridBest),
		tsport:    NewTransporter(monitor, w, limitThreshold, needLimit),
	}
	return i
}

func (i *Initer) SendBinlogData(data []byte) {
	i.inputBuf <- data
}

func (i *Initer) Ours(isFull bool) {
	if isFull {
		go i.div.divide2(i.inputBuf, i.divideBuf)
		go i.cmpr.TestOurs(i.divideBuf, i.outputBuf, &i.outputBufSize)
	} else {
		go i.div.divide(i.inputBuf, i.divideBuf)
		go i.agg.Aggregate(i.divideBuf, i.aggBuf)
		go i.cmpr.Compress(i.aggBuf, i.outputBuf, &i.outputBufSize)
	}

	go i.tsport.Transport(i.outputBuf, &i.outputBufSize)
}

// 始终用一个压缩方法
func (i *Initer) TestByCmprType(cmprType byte, isFull bool) {
	if isFull {
		go i.div.divide2(i.inputBuf, i.divideBuf)
	} else {
		go i.div.divide(i.inputBuf, i.divideBuf)
	}
	go i.cmpr.TestByCmprType(i.divideBuf, i.outputBuf, cmprType, &i.outputBufSize)
	go i.tsport.Transport(i.outputBuf, &i.outputBufSize)
}

// 自适应，每个buffer只能用一个压缩方法，最优的策略
func (i *Initer) TestOneBest(isFull bool) {
	if isFull {
		go i.div.divide2(i.inputBuf, i.divideBuf)
	} else {
		go i.div.divide(i.inputBuf, i.divideBuf)
	}

	go i.cmpr.TestOneBest(i.divideBuf, i.outputBuf, &i.outputBufSize)
	go i.tsport.Transport(i.outputBuf, &i.outputBufSize)
}

func (i *Initer) TestMultiBest(isFull bool) {
	if isFull {
		go i.div.divide2(i.inputBuf, i.divideBuf)
		go i.cmpr.TestMultiBest(i.divideBuf, i.outputBuf, &i.outputBufSize)
	} else {
		go i.div.divide(i.inputBuf, i.divideBuf)
		go i.agg.Aggregate(i.divideBuf, i.aggBuf)
		go i.cmpr.TestMultiSegmentBest(i.aggBuf, i.outputBuf, &i.outputBufSize)
	}

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
		time.Sleep(time.Millisecond * 200)
	}
}
