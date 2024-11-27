package adaptive

import (
	// "fmt"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/wqshr12345/golib/common"
)

type Divider struct {
	bufferSize int
	testTimes  int64
	readTime   float64
}

func NewDivider(bufferSize int, readTime float64) *Divider {
	return &Divider{
		bufferSize: bufferSize,
		readTime:   readTime,
	}
}

// 按照固定bufferSize切割，用于传输全量数据时
func (d *Divider) divideByFixSize(input <-chan []byte, output chan<- common.DataWithInfo, cmprBufSize *atomic.Int64) {
	for {
		data := <-input
		offset := 0
		for offset < len(data) {
			if d.readTime > 0 {
				// fmt.Println("read time", d.readTime)
				time.Sleep(time.Duration(d.readTime) * time.Millisecond)
			}
			ts := time.Now().UnixNano()
			n := d.bufferSize
			if offset+n > len(data) {
				n = len(data) - offset
			}
			dataWithInfo := common.DataWithInfo{
				Data:        data[offset : offset+n],
				Ts:          ts,
				TotalEvents: 0,
			}
			// TODO(wangqian): 在这里sleep，模拟等待时间
			fmt.Println("第", d.testTimes, "个buffer分割结束,时间为: ", ts)
			cmprBufSize.Add(int64(len(dataWithInfo.Data)))
			// fmt.Println("CmprBufSize", cmprBufSize.Load())
			output <- dataWithInfo
			offset += n
			d.testTimes++
		}
	}
}

func (d *Divider) DivideByBound(input <-chan []byte, output chan<- common.DataWithInfo, cmprBufSize *atomic.Int64) {
	for {
		// TODOIMP 目前默认divide的input的数据都是一个个完整的binlog...未来如果需要集成到frp中，这里不能假设input全为完整binlog，需要在内部做buffer...
		data := <-input
		if len(data) == 0 || data == nil {
			continue
		}
		offset := 0
		ok := true
		for ok {
			if d.readTime > 0 {
				time.Sleep(time.Duration(d.readTime) * time.Millisecond)
			}
			ts := time.Now().UnixNano()
			var n int
			var totalEvents int64
			ok, n, _, totalEvents = d.check(data[offset:])
			if !ok {
				break
			}
			dataWithInfo := common.DataWithInfo{
				Data:        data[offset : offset+n],
				Ts:          ts, //+ endTs - startTs.UnixNano()
				TotalEvents: totalEvents,
			}
			// TODO(wangqian): 在这里sleep，模拟等待时间
			fmt.Println("第", d.testTimes, "个buffer分割结束,时间为: ", ts)
			cmprBufSize.Add(int64(len(dataWithInfo.Data)))
			output <- dataWithInfo
			offset += n
			d.testTimes++
		}
	}
}

func (d *Divider) DivideByBound2(input <-chan []byte, output chan<- common.DataWithInfo) {
	// for {
	// TODOIMP 目前默认divide的input的数据都是一个个完整的binlog...未来如果需要集成到frp中，这里不能假设input全为完整binlog，需要在内部做buffer...
	data := <-input
	// if len(data) == 0 || data == nil {
	// 	continue
	// }
	ts := time.Now().UnixNano()
	offset := 0
	ok := true
	for ok {
		if d.readTime > 0 {
			time.Sleep(time.Duration(d.readTime) * time.Millisecond)
		}
		var n int
		var totalEvents int64
		ok, n, _, totalEvents = d.check(data[offset:])
		if !ok {
			break
		}
		dataWithInfo := common.DataWithInfo{
			Data:        data[offset : offset+n],
			Ts:          ts, //+ endTs - startTs.UnixNano()
			TotalEvents: totalEvents,
		}
		// TODO(wangqian): 在这里sleep，模拟等待时间
		fmt.Println("第", d.testTimes, "个buffer分割结束,时间为: ", ts)
		output <- dataWithInfo
		offset += n
		d.testTimes++
	}
	// }
}

func (d *Divider) check(data []byte) (bool, int, bool, int64) {
	ok := true
	// 压缩类型为CompressTypeRtc
	totalLength := 0
	isFirstPackageOk := false
	processedFirstPackage := false

	// 确保有足够的字节来获取长度
	if len(data) < 3 {
		// 数据不足以解析长度
		ok = false
		return ok, 0, false, 0
	}
	offset := 0
	totalEvents := int64(0)
	for offset < len(data) {
		if offset > d.bufferSize {
			// 数据超过bufferSize
			break
		}
		bodylen := int(uint32(data[offset])<<0 | uint32(data[offset+1])<<8 | uint32(data[offset+2])<<16)
		if len(data) < offset+3+bodylen {
			// 数据不足以解析body
			break
		}
		offset += 3

		// skip sequenct
		offset += 1
		// 读取 flag
		pkgFlag := data[offset]
		offset += 1
		// 判断是否是 ok package
		isOkPackage := (pkgFlag == 0x00)
		// FIX(wangqian): 在package 为ok的情况下，依然有非binlog package的可能性...暂时用长度做一个不完备判断
		if bodylen < 20 {
			isOkPackage = false
		}

		// 如果还没有处理第一个包，则记录它的状态
		if !processedFirstPackage {
			isFirstPackageOk = isOkPackage
			processedFirstPackage = true
		}

		// 如果当前包的状态与第一个包的状态不同，则停止处理
		if isOkPackage != isFirstPackageOk {
			break
		}

		offset += bodylen - 1 // body的长度是bodylen - 1，因为bodylen包括了body和flag
		// 增加当前包的总长度到计数器
		totalLength += 3 + 1 + bodylen // 加上 len, seq, flag 和 body 的长度（len就是flag和body的长度）
		// 移动指针，跳过当前处理的包
		totalEvents++
	}
	if totalLength == 0 {
		ok = false
	}
	return ok, totalLength, isFirstPackageOk, totalEvents
}
