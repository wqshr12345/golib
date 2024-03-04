package datacompadaptive

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/wqshr12345/golib/common"
	"github.com/wqshr12345/golib/statistics"
)

// 使用自定义格式+非流式压缩(块式压缩)
/*
Adaptive Encoding Format：
| compress_type(1 byte) | timestamp(8 bytes) | data_length(4 bytes) | (compressed )data(data_length bytes) |
*/

// TODO(wangqian): extract the const parameter to a config file.
const (
	// packageTimestampSize    = 16 // startTs + midTs
	packageCompressTypeSize = 1
	packageDataLenSize      = 4
	packageRawDataLenSize   = 4

	packageHeaderSize = packageCompressTypeSize + packageDataLenSize + packageRawDataLenSize
)

// customized protocol writer
type Writer struct {
	// the data will be transported to outW.
	outW io.Writer

	err error

	// iBuf是缓冲待压缩数据的缓冲区。数据会等到iBuf满了之后再压缩（如果开启了CompressTypeColumn，那么会等待iBuf达到Package的边界才会进行压缩...）
	iBuf []byte

	bufSize int

	stat *statistics.TcpStatistic

	predictor *Predictor
}

func NewWriter(w io.Writer, bufSize int, stat *statistics.TcpStatistic) *Writer {

	return &Writer{
		outW:      w,
		iBuf:      make([]byte, 0, bufSize),
		bufSize:   bufSize,
		stat:      stat,
		predictor: NewPredictor(),
	}
}

func (w *Writer) Write(p []byte) (nRet int, errRet error) {
	if w.iBuf == nil {
		return w.write(p, true)
	}
	for len(p) > (cap(w.iBuf)-len(w.iBuf)) && w.err == nil {
		var n int
		if len(w.iBuf) == 0 {
			// Large write, empty buffer.
			// Write directly from p to avoid copy.
			n, _ = w.write(p, true)
		} else {
			n = copy(w.iBuf[len(w.iBuf):cap(w.iBuf)], p)
			w.iBuf = w.iBuf[:len(w.iBuf)+n]
			w.Flush()
		}
		nRet += n
		p = p[n:]
	}
	if w.err != nil {
		return nRet, w.err
	}
	n := copy(w.iBuf[len(w.iBuf):cap(w.iBuf)], p)
	w.iBuf = w.iBuf[:len(w.iBuf)+n]
	nRet += n
	return nRet, nil
}

// 使用p中数据构造一个自适应格式(在rtc场景下，p中数据必须是有完整边界的package的集合)
func (w *Writer) write(p []byte, valid bool) (nRet int, errRet error) {
	if len(p) == 0 {
		return 0, nil
	}
	oBuf := make([]byte, packageHeaderSize)

	// TODO(wangqian): use a oBuf to avoid memory allocate.
	startTime := time.Now()

	// 预测压缩方法
	compressType := w.predictor.Predict(p)
	w.stat.FetchWriteCmprTimes(compressType)

	compressor := compression_reader_writer.NewCompressor(uint8(compressType))
	// fmt.Println("start once compression")
	compressedData := compressor.Compress(p)
	dataLen := len(compressedData)
	rawDataLen := len(p)
	fmt.Println("压缩率:", float64(dataLen)/float64(rawDataLen))
	fmt.Println("压缩时间:", time.Since(startTime))
	fmt.Println("压缩带宽:", float64(dataLen)/float64(time.Since(startTime).Nanoseconds())*1000*1000*1000/1024/1024, "MB/s")
	// fmt.Println("end once compression")

	w.stat.AddCompressionBandWidth(rawDataLen, dataLen, time.Since(startTime))

	oBuf[0] = uint8(compressType)

	binary.LittleEndian.PutUint32(oBuf[1:5], uint32(dataLen))
	binary.LittleEndian.PutUint32(oBuf[5:], uint32(rawDataLen))

	if _, err := w.outW.Write(oBuf); err != nil {
		w.err = err
		return nRet, err
	}

	midTime := time.Now()
	if _, err := w.outW.Write(compressedData); err != nil {
		w.err = err
		return nRet, err
	}
	w.stat.AddWritesBandWidth(dataLen, time.Since(midTime))

	eor := float64(len(p)) / (time.Since(startTime).Seconds())
	// 回归统计压缩方法
	w.predictor.Update(uint32(eor))

	nRet = len(p)
	return nRet, nil
}

func (w *Writer) Flush() error {
	if w.err != nil {
		return w.err
	}
	if len(w.iBuf) == 0 {
		return nil
	}
	w.write(w.iBuf, true)
	w.iBuf = w.iBuf[:0]
	return w.err
}

func (w *Writer) Report(info common.CompressInfo) error {
	// TODO(wangqian):Do more things.
	// w.compressInfo = append(w.compressInfo, info)
	return nil
}

func (w *Writer) Close() error {
	w.Flush()
	ret := w.err
	if w.err == nil {
		w.err = errors.New("Writer is closed")

	}
	return ret
}
