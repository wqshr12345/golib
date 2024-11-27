package adaptive

import (
	"io"
	"sync/atomic"

	"github.com/wqshr12345/golib/common"
)

type Initer struct {
	w io.Writer

	monitor    *Monitor
	div        *Divider
	agg        *Aggregator
	cmpr       *Compressor
	tsport     *Transporter
	inputBuf   chan []byte
	divideBuf  chan common.DataWithInfo
	aggBuf     chan *AggregateData
	outputBuf  chan []byte
	obBest     []byte
	mbBest     [][]common.CmprTypeData
	cmprThread int

	outputBufSize atomic.Int64
	cmprBufSize   atomic.Int64
}

func NewIniter(w io.Writer, bufferSize int, packageSize int, cmprThread int, readTime float64, obBest []byte, mbBest [][]common.CmprTypeData, hybridBest [][]common.CompressionIntro, rate float64, limitThreshold int64, epochThreshold int64, needLimit bool) *Initer {
	monitor := NewMonitor(packageSize, rate, epochThreshold, cmprThread, readTime/1000.0)
	i := &Initer{
		w:          w,
		inputBuf:   make(chan []byte, 10),
		divideBuf:  make(chan common.DataWithInfo, 10000), // 应该非常大，我们不希望数据阻塞在这里，这样等待时间就是压缩开始-分割结束
		aggBuf:     make(chan *AggregateData, 10),         // 应该大于bufferSize / packageSize
		outputBuf:  make(chan []byte, 10000),
		obBest:     obBest,
		mbBest:     mbBest,
		cmprThread: cmprThread,
		monitor:    monitor,
		div:        NewDivider(bufferSize, readTime),
		agg:        NewAggregator(monitor),
		cmpr:       NewCompressor(monitor, obBest, mbBest, hybridBest, cmprThread),
		tsport:     NewTransporter(monitor, w, limitThreshold, needLimit, rate),
	}
	return i
}

func (i *Initer) SendBinlogData(data []byte) {
	i.inputBuf <- data
}

// 自适应压缩+Binlog切割(取决于isFull的值)
func (i *Initer) Run(isFull bool, rate float64) {
	if isFull {
		go i.div.divideByFixSize(i.inputBuf, i.divideBuf, &i.cmprBufSize)
		// for t := 0; t < i.cmprThread; t++ {
		go i.cmpr.CompressFull(i.divideBuf, i.outputBuf, &i.cmprBufSize, &i.outputBufSize, rate)
		// }
	} else {
		go i.div.DivideByBound(i.inputBuf, i.divideBuf, &i.cmprBufSize)
		go i.agg.Aggregate(i.divideBuf, i.aggBuf)
		// for t := 0; t < i.cmprThread; t++ {
		go i.cmpr.CompressIncr(i.aggBuf, i.outputBuf, &i.cmprBufSize, &i.outputBufSize, rate)
		// }
	}

	go i.tsport.Transport(i.outputBuf, &i.cmprBufSize, &i.outputBufSize)
}

// 始终一个压缩方法压缩+Binlog切割(取决于isFull的值)
func (i *Initer) RunByCmprType(cmprType byte, isFull bool, rate float64) {
	if isFull {
		go i.div.divideByFixSize(i.inputBuf, i.divideBuf, &i.cmprBufSize)
		go i.cmpr.CompressByCmprType(i.divideBuf, i.outputBuf, cmprType, &i.cmprBufSize, &i.outputBufSize, rate)
	} else {
		go i.div.DivideByBound(i.inputBuf, i.divideBuf, &i.cmprBufSize)
		go i.agg.Aggregate(i.divideBuf, i.aggBuf)
		go i.cmpr.CompressIncrByCmprType(i.aggBuf, i.outputBuf, cmprType, &i.cmprBufSize, &i.outputBufSize, rate)
	}
	go i.tsport.Transport(i.outputBuf, &i.cmprBufSize, &i.outputBufSize)
}

// 单个压缩方法最优化+Binlog切割(取决于isFull的值)
func (i *Initer) RunOneBest(isFull bool) {
	if isFull {
		go i.div.divideByFixSize(i.inputBuf, i.divideBuf, &i.cmprBufSize)
		go i.cmpr.CompressOneBest(i.divideBuf, i.outputBuf, &i.cmprBufSize, &i.outputBufSize)
	} else {
		go i.div.DivideByBound(i.inputBuf, i.divideBuf, &i.cmprBufSize)
		go i.agg.Aggregate(i.divideBuf, i.aggBuf)
		go i.cmpr.CompressRtcOneBest(i.aggBuf, i.outputBuf, &i.cmprBufSize, &i.outputBufSize)
	}
	go i.tsport.Transport(i.outputBuf, &i.cmprBufSize, &i.outputBufSize)
}

// 多个压缩方法最优化+Binlog切割(取决于isFull的值)
func (i *Initer) RunMultiBest(isFull bool) {
	if isFull {
		go i.div.divideByFixSize(i.inputBuf, i.divideBuf, &i.cmprBufSize)
		go i.cmpr.CompressMultiBest(i.divideBuf, i.outputBuf, &i.cmprBufSize, &i.outputBufSize)
	} else {
		go i.div.DivideByBound(i.inputBuf, i.divideBuf, &i.cmprBufSize)
		go i.agg.Aggregate(i.divideBuf, i.aggBuf)
		// go i.cmpr.TestMultiSegmentBest(i.aggBuf, i.outputBuf, &i.outputBufSize)
	}

	go i.tsport.Transport(i.outputBuf, &i.cmprBufSize, &i.outputBufSize)
}

// 测试行转列后的分block
// TODO 10.13
func (i *Initer) TestForSingleColumnCompress() {
	// 默认Incr，我们需要做行转列后的测试
	go i.div.DivideByBound(i.inputBuf, i.divideBuf, &i.cmprBufSize)
	go i.agg.Aggregate(i.divideBuf, i.aggBuf)
	go i.cmpr.CompressByColumnAndPrintInfo(i.aggBuf)
}

// 测试不经过行转列的分block
func (i *Initer) TestForNoColumnCompress() {
	go i.div.divideByFixSize(i.inputBuf, i.divideBuf, &i.cmprBufSize)
	go i.cmpr.CompressByFixBlockAndPrintInfo(i.divideBuf)

}

func (i *Initer) TestForRtcNumber() {
	go i.div.DivideByBound(i.inputBuf, i.divideBuf, &i.cmprBufSize)

}

func (i *Initer) TestForDecompression() {
	go i.div.divideByFixSize(i.inputBuf, i.divideBuf, &i.cmprBufSize)

}
