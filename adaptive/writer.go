package adaptive

// import (
// 	"encoding/binary"
// 	"errors"
// 	"fmt"
// 	"io"
// 	"time"

// 	"github.com/wqshr12345/golib/common"
// 	"github.com/wqshr12345/golib/compression/rtc"

// 	"github.com/wqshr12345/golib/statistics"
// )

// // 使用自定义格式+非流式压缩(块式压缩)
// /*
// Adaptive Encoding Format：
// | compress_type(1 byte) | timestamp(8 bytes) | data_length(4 bytes) | (compressed )data(data_length bytes) |
// */

// // TODO(wangqian): extract the const parameter to a config file.
// const (
// 	packageTimestampSize    = 16 // startTs + midTs
// 	packageCompressTypeSize = 1
// 	packageDataLenSize      = 4
// 	packageRawDataLenSize   = 4

// 	packageHeaderSize = packageTimestampSize + packageCompressTypeSize + packageDataLenSize + packageRawDataLenSize
// )

// // customized protocol writer
// type Writer struct {
// 	// the data will be transported to outW.
// 	outW io.Writer

// 	cmpr common.Compressor

// 	cmprType uint8

// 	err error

// 	// iBuf是缓冲待压缩数据的缓冲区。数据会等到iBuf满了之后再压缩（如果开启了CompressTypeColumn，那么会等待iBuf达到Package的边界才会进行压缩...）
// 	iBuf []byte

// 	bufSize int

// 	compressInfo []common.CompressInfo

// 	stat *statistics.TcpStatistic
// }

// func NewWriter(w io.Writer, bufSize int, cmprType uint8, stat *statistics.TcpStatistic) *Writer {
// 	cmpr := NewCompressor(cmprType)

// 	return &Writer{
// 		outW:     w,
// 		cmpr:     cmpr,
// 		cmprType: cmprType,
// 		iBuf:     make([]byte, 0, bufSize),
// 		bufSize:  bufSize,
// 		stat:     stat,
// 	}
// }

// func (w *Writer) Write(p []byte) (nRet int, errRet error) {
// 	if w.iBuf == nil {
// 		return w.write(p, true)
// 	}
// 	for len(p) > (cap(w.iBuf)-len(w.iBuf)) && w.err == nil {
// 		var n int
// 		if len(w.iBuf) == 0 {
// 			// Large write, empty buffer.
// 			// Write directly from p to avoid copy.
// 			n, _ = w.write(p, true)
// 		} else {
// 			n = copy(w.iBuf[len(w.iBuf):cap(w.iBuf)], p)
// 			w.iBuf = w.iBuf[:len(w.iBuf)+n]
// 			w.Flush()
// 		}
// 		nRet += n
// 		p = p[n:]
// 	}
// 	if w.err != nil {
// 		return nRet, w.err
// 	}
// 	n := copy(w.iBuf[len(w.iBuf):cap(w.iBuf)], p)
// 	w.iBuf = w.iBuf[:len(w.iBuf)+n]
// 	nRet += n
// 	return nRet, nil
// }

// // 使用p中数据构造一个自适应格式(在rtc场景下，p中数据必须是有完整边界的package的集合)
// func (w *Writer) write(p []byte, valid bool) (nRet int, errRet error) {
// 	if len(p) == 0 {
// 		return 0, nil
// 	}
// 	oBuf := make([]byte, packageHeaderSize)

// 	// TODO(wangqian): use a oBuf to avoid memory allocate.
// 	startTs := time.Now().UnixNano()

// 	startTime := time.Now()

// 	// var compressor common.Compressor

// 	compressor := w.cmpr
// 	// TODO(wangqian):现在的Rtc Compressor是有状态的...每次必须重新生成一个Compressor
// 	// 考虑增加Reset方法？或者在Finalize方法最后reset所有的状态。
// 	if w.cmprType == common.CompressTypeRtc {
// 		// compressor = NewCompressor(common.CompressTypeRtc)
// 		compressor.(*rtc.RtcCompressor).Reset()
// 	}

// 	compressType := w.cmprType

// 	if w.cmprType == common.CompressTypeRtc && !valid {
// 		compressor = NewCompressor(common.CompressTypeNone)
// 		compressType = common.CompressTypeNone
// 	}
// 	// fmt.Println("start once compression")
// 	compressedData := compressor.Compress(p)
// 	dataLen := len(compressedData)
// 	rawDataLen := len(p)
// 	fmt.Println("压缩率:", float64(dataLen)/float64(rawDataLen))
// 	fmt.Println("压缩时间:", time.Since(startTime))
// 	fmt.Println("压缩带宽:", float64(dataLen)/float64(time.Since(startTime).Nanoseconds())*1000*1000*1000/1024/1024, "MB/s")
// 	// fmt.Println("end once compression")

// 	// w.stat.AddCompressionBandWidth(rawDataLen, dataLen, time.Since(startTime))

// 	midTs := time.Now().UnixNano()

// 	oBuf[0] = compressType
// 	binary.LittleEndian.PutUint64(oBuf[1:9], uint64(startTs))
// 	binary.LittleEndian.PutUint64(oBuf[9:17], uint64(midTs))
// 	binary.LittleEndian.PutUint32(oBuf[17:21], uint32(dataLen))
// 	binary.LittleEndian.PutUint32(oBuf[21:], uint32(rawDataLen))

// 	if _, err := w.outW.Write(oBuf); err != nil {
// 		w.err = err
// 		return nRet, err
// 	}

// 	startTime = time.Now()
// 	if _, err := w.outW.Write(compressedData); err != nil {
// 		w.err = err
// 		return nRet, err
// 	}
// 	// w.stat.AddWritesBandWidth(dataLen, time.Since(startTime))

// 	nRet = len(p)
// 	return nRet, nil
// }

// func (w *Writer) Flush() error {
// 	n := 0
// 	defer func() {
// 		// 剩下的有效数据
// 		less := w.iBuf
// 		w.iBuf = make([]byte, 0, w.bufSize)
// 		write := copy(w.iBuf[:cap(w.iBuf)], less)
// 		w.iBuf = w.iBuf[:write]
// 	}()
// 	if w.err != nil {
// 		return w.err
// 	}
// 	if len(w.iBuf) == 0 {
// 		return nil
// 	}
// 	ok := true
// 	var valid bool
// 	for ok {
// 		ok, n, valid = w.check()
// 		if !ok {
// 			return nil
// 		}
// 		fmt.Println("本次压缩的数据长度:", n)
// 		w.write(w.iBuf[:n], valid)
// 		w.iBuf = w.iBuf[n:]
// 		// FIX(wangqian): 解决非RTC压缩下的死循环的临时解决方案...
// 		if len(w.iBuf) == 0 {
// 			break
// 		}
// 	}

// 	// w.write(w.iBuf, true)
// 	// w.iBuf = w.iBuf[:0]
// 	return w.err
// }

// // 返回当前是否可以压缩、可以压缩的数据长度、是否为binlog信息。在压缩类型非rtc场景下，返回值永远是ok，len(p)
// func (w *Writer) check() (bool, int, bool) {
// 	ok := true
// 	if w.cmprType != common.CompressTypeRtc {
// 		return ok, len(w.iBuf), true
// 	}

// 	// 压缩类型为CompressTypeRtc
// 	totalLength := 0
// 	isFirstPackageOk := false
// 	processedFirstPackage := false

// 	// 确保有足够的字节来获取长度
// 	if len(w.iBuf) < 3 {
// 		fmt.Println("数据不足以解析长度")
// 		ok = false
// 		return ok, 0, false
// 	}
// 	offset := 0
// 	for offset < len(w.iBuf) {
// 		if len(w.iBuf)-offset < 3 {
// 			fmt.Println("数据不足以解析长度")
// 			break
// 		}
// 		// 读取长度：假设长度字段是大端序
// 		bodylen := int(uint32(w.iBuf[offset])<<0 | uint32(w.iBuf[offset+1])<<8 | uint32(w.iBuf[offset+2])<<16)
// 		offset += 3
// 		// 确保数据包剩余部分足够
// 		if len(w.iBuf)-offset < bodylen+1 {
// 			// fmt.Println("数据不足以解析整个seq、flag和body")
// 			break
// 		}
// 		// skip sequenct
// 		offset += 1
// 		// 读取 flag
// 		pkgFlag := w.iBuf[offset]
// 		offset += 1
// 		// 判断是否是 ok package
// 		isOkPackage := (pkgFlag == 0x00)
// 		// FIX(wangqian): 在package 为ok的情况下，依然有非binlog package的可能性...暂时用长度做一个不完备判断
// 		if bodylen < 20 {
// 			isOkPackage = false
// 		}

// 		// 如果还没有处理第一个包，则记录它的状态
// 		if !processedFirstPackage {
// 			isFirstPackageOk = isOkPackage
// 			processedFirstPackage = true
// 		}

// 		// 如果当前包的状态与第一个包的状态不同，则停止处理
// 		if isOkPackage != isFirstPackageOk {
// 			break
// 		}

// 		offset += bodylen - 1 // body的长度是bodylen - 1，因为bodylen包括了body和flag
// 		// 增加当前包的总长度到计数器
// 		totalLength += 3 + 1 + bodylen // 加上 len, seq, flag 和 body 的长度（len就是flag和body的长度）
// 		// 移动指针，跳过当前处理的包
// 	}
// 	if totalLength == 0 {
// 		ok = false
// 	}
// 	return ok, totalLength, isFirstPackageOk
// }

// func (w *Writer) Report(info common.CompressInfo) error {
// 	// TODO(wangqian):Do more things.
// 	// w.compressInfo = append(w.compressInfo, info)
// 	return nil
// }

// func (w *Writer) Close() error {
// 	w.Flush()
// 	ret := w.err
// 	if w.err == nil {
// 		w.err = errors.New("Writer is closed")

// 	}
// 	return ret
// }
