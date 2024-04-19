package adaptive

import (
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/wqshr12345/golib/adaptive/aggregate"
	allcompressor "github.com/wqshr12345/golib/adaptive/all_compressor"
	"github.com/wqshr12345/golib/common"
)

type Compressor struct {
	monitor   *Monitor
	allCmprs  *allcompressor.AllCompressor
	testTimes int64
	cpuUsage  float64
}

func NewCompressor(monitor *Monitor, cpuUsage float64) *Compressor {
	return &Compressor{
		monitor:  monitor,
		allCmprs: allcompressor.NewAllCompressor(),
		cpuUsage: cpuUsage,
	}
}

func (c *Compressor) TestByCmprType(input <-chan common.DataWithInfo, output chan<- []byte, cmprType byte) {
	for {
		dataWithInfo := <-input
		data := dataWithInfo.Data

		startBuffer := make([]byte, 2)
		binary.LittleEndian.PutUint16(startBuffer[0:2], 0)
		output <- startBuffer

		packageData := make([]byte, 18)
		binary.LittleEndian.PutUint16(packageData[0:2], 1)
		packageOriLen := len(data)

		cmpr := c.allCmprs.GetCompressorByType(cmprType)

		startTime := time.Now()
		packageData = append(packageData, cmpr.Compress(data)...)

		tempEndTime := time.Now()

		tempSumTime := tempEndTime.Sub(startTime).Seconds()
		if c.cpuUsage < 1 {
			fmt.Println("temp time", tempSumTime)
			fmt.Println("sleep time", time.Duration(tempSumTime*float64(time.Second)*(1-c.cpuUsage)*100))
			time.Sleep(time.Duration(tempSumTime * float64(time.Second) * (1 - c.cpuUsage) * 100))
		}
		// endTime := time.Now()

		packageCmprLen := len(packageData) - 18
		binary.LittleEndian.PutUint64(packageData[2:10], uint64(packageOriLen))
		binary.LittleEndian.PutUint64(packageData[10:18], uint64(packageCmprLen))

		// fmt.Println("compress package", c.testTimes)
		c.testTimes++
		output <- packageData

		endBuffer := make([]byte, 34)
		binary.LittleEndian.PutUint16(endBuffer[0:2], 2)
		binary.LittleEndian.PutUint64(endBuffer[2:10], uint64(dataWithInfo.Ts))
		// TODO divide的时候是否要传递totalEvents
		binary.LittleEndian.PutUint64(endBuffer[10:18], uint64(dataWithInfo.TotalEvents))
		binary.LittleEndian.PutUint64(endBuffer[18:26], uint64(packageOriLen))
		binary.LittleEndian.PutUint64(endBuffer[26:34], uint64(packageCmprLen))
		output <- endBuffer
	}
}

func (c *Compressor) TestOneBest(input <-chan common.DataWithInfo, output chan<- []byte) {
	for {
		dataWithInfo := <-input
		data := dataWithInfo.Data

		startBuffer := make([]byte, 2)
		binary.LittleEndian.PutUint16(startBuffer[0:2], 0)
		output <- startBuffer

		packageData := make([]byte, 18)
		binary.LittleEndian.PutUint16(packageData[0:2], 1)
		packageOriLen := len(data)

		// TODO 得到当前最优的压缩方法 遍历选择最优
		// 计算每一个压缩算法的压缩率
		maxAnswer := 0.0
		maxData := make([]byte, 0)
		totalTime := 0.0
		maxTime := 0.0
		for cmprType := common.INVALID_START + 1; cmprType < common.INVALID_END; cmprType++ {
			cmpr := c.allCmprs.GetCompressorByType(cmprType)
			startTime := time.Now()
			tempData := cmpr.Compress(data)
			// tempEndTime := time.Now()
			// tempSumTime := tempEndTime.Sub(startTime).Seconds()
			// if c.cpuUsage < 1 {
			// 	time.Sleep(time.Duration(tempSumTime * float64(time.Second) * (1 - c.cpuUsage) * 100))
			// }
			endTime := time.Now()
			totalTime += endTime.Sub(startTime).Seconds()
			// 压缩带宽 * 压缩增益
			tempAnswer := min(c.monitor.net_bandwitdh, float64(len(tempData))/float64(endTime.Sub(startTime).Seconds())*c.cpuUsage) * float64(len(data)) / float64(len(tempData))
			if tempAnswer > maxAnswer {
				maxAnswer = tempAnswer
				maxData = tempData
				maxTime = endTime.Sub(startTime).Seconds()
			}
		}

		if c.cpuUsage < 1 {
			fmt.Println("max time", maxTime)
			fmt.Println("sleep time", time.Duration(maxTime*float64(time.Second)*(1-c.cpuUsage)*100-totalTime+maxTime))
			time.Sleep(time.Duration(maxTime*float64(time.Second)*(1-c.cpuUsage)*100 - totalTime + maxTime))
		}
		// cmpr := c.allCmprs.GetCompressorByType(cmprType)

		// packageData = append(packageData, cmpr.Compress(data)...)
		packageData = append(packageData, maxData...)

		packageCmprLen := len(packageData) - 18
		binary.LittleEndian.PutUint64(packageData[2:10], uint64(packageOriLen))
		binary.LittleEndian.PutUint64(packageData[10:18], uint64(packageCmprLen))

		// fmt.Println("compress package", c.testTimes)
		c.testTimes++
		output <- packageData

		endBuffer := make([]byte, 34)
		binary.LittleEndian.PutUint16(endBuffer[0:2], 2)
		binary.LittleEndian.PutUint64(endBuffer[2:10], uint64(dataWithInfo.Ts))
		// TODO divide的时候是否要传递totalEvents
		binary.LittleEndian.PutUint64(endBuffer[10:18], uint64(dataWithInfo.TotalEvents))
		binary.LittleEndian.PutUint64(endBuffer[18:26], uint64(packageOriLen))
		binary.LittleEndian.PutUint64(endBuffer[26:34], uint64(packageCmprLen))
		output <- endBuffer
	}
}

func (c *Compressor) TestMultiBest(input <-chan common.DataWithInfo, output chan<- []byte) {
	for {
		dataWithInfo := <-input
		data := dataWithInfo.Data

		startBuffer := make([]byte, 2)
		binary.LittleEndian.PutUint16(startBuffer[0:2], 0)
		output <- startBuffer

		bufferOriLen := 0
		bufferCmprLen := 0

		offAndLen := make([]common.OffAndLen, 1)
		offAndLen[0].Offset = 0
		offAndLen[0].Len = int64(len(data))

		packageData := make([]byte, 18)
		binary.LittleEndian.PutUint16(packageData[0:2], 1)

		// 1. 调用monitor，计算得到当前压缩比
		cmprIntro := c.monitor.GetAlphaRatio(offAndLen)
		offset := 0
		// 2. 针对计算得到的压缩比，对每列做压缩
		for _, cmprintro := range cmprIntro {
			// 2.1 根据列名和bytes，得到应该压缩的数据列
			tempData := data[offset : offset+int(cmprintro.ByteNum)]
			offset += int(cmprintro.ByteNum)
			// 2.2 根据压缩算法名，得到压缩器,并更新offset
			compressor := c.allCmprs.GetCompressorByType(cmprintro.Point.Cmpr)
			// 2.3 压缩
			oriLen := len(tempData)
			bufferOriLen += len(tempData)

			cmprLen1 := len(packageData)
			startTime := time.Now()

			packageData = append(packageData, compressor.Compress(tempData)...)

			tempEndTime := time.Now()
			tempSumTime := tempEndTime.Sub(startTime).Seconds()
			if c.cpuUsage < 1 {
				time.Sleep(time.Duration(tempSumTime * float64(time.Second) * (1 - c.cpuUsage) * 100))
			}
			endTime := time.Now()
			cmprLen := len(packageData) - cmprLen1
			bufferCmprLen += cmprLen

			// 2.5 更新monitor中的cache，同时更新offset
			c.monitor.UpdateCompressionInfo(cmprintro.Point.Column, cmprintro.Point.Cmpr, oriLen, cmprLen, endTime.Sub(startTime).Seconds())
		}

		// ONLY TEST startTs, totalEvents, oriLen, cmprLen
		endBuffer := make([]byte, 34)
		binary.LittleEndian.PutUint16(endBuffer[0:2], 2)
		binary.LittleEndian.PutUint64(endBuffer[2:10], uint64(dataWithInfo.Ts))
		binary.LittleEndian.PutUint64(endBuffer[10:18], uint64(dataWithInfo.TotalEvents))
		binary.LittleEndian.PutUint64(endBuffer[18:26], uint64(bufferOriLen))
		binary.LittleEndian.PutUint64(endBuffer[26:34], uint64(bufferCmprLen))
		// 传输
		output <- endBuffer
	}
}

func (c *Compressor) TestRtcOneBest(input <-chan *aggregate.AggregateData, output chan<- []byte, outputBufSize *atomic.Int64) {
	for {
		aggData := <-input
		// 初始化本buffer内的各列数据起始offset及总长度len
		aggData.InitOffsetAndLen()

		// ONLY TEST buffer start | package i| buffer end
		startBuffer := make([]byte, 2)
		binary.LittleEndian.PutUint16(startBuffer[0:2], 0)
		output <- startBuffer

		offAndLens := aggData.GetOffsetAndLen()

		maxAnswer := 0.0
		maxData := make([]byte, 0)
		packageData := make([]byte, 18)
		packageOriLen := 0
		packageCmprLen := 0
		binary.LittleEndian.PutUint16(packageData[0:2], 1)
		totalTime := 0.0
		maxTime := 0.0

		for cmprType := common.INVALID_START + 1; cmprType < common.INVALID_END; cmprType++ {
			tempData := make([]byte, 0)
			tempOriLen := 0
			// 1. 调用monitor，计算得到当前压缩比

			// 2. 针对计算得到的压缩比，对每列做压缩
			startTime := time.Now()
			for column := byte(common.BodyLen); column < common.TotalColumnNums; column++ {
				// 2.1 根据列名和bytes，得到应该压缩的数据列
				data := aggData.GetColumnData2(column, offAndLens[column].Len)
				// 2.2 根据压缩算法名，得到压缩器,并更新offset
				compressor := c.allCmprs.GetCompressorByType(cmprType)
				// 2.3 压缩
				tempOriLen += len(data)

				tempData = append(tempData, compressor.Compress(data)...)

				// 2.5 更新monitor中的cache，同时更新offset
			}
			endTime := time.Now()
			totalTime += endTime.Sub(startTime).Seconds()

			tempAnswer := min(c.monitor.net_bandwitdh, float64(len(tempData))/float64(endTime.Sub(startTime).Seconds())*c.cpuUsage) * float64(tempOriLen) / float64(len(tempData))
			if tempAnswer > maxAnswer {
				maxAnswer = tempAnswer
				maxData = packageData
				packageOriLen = tempOriLen
				maxTime = endTime.Sub(startTime).Seconds()

			}
		}
		if c.cpuUsage < 1 {
			fmt.Println("sleep time", time.Duration(maxTime*float64(time.Second)*(1-c.cpuUsage)*100-totalTime+maxTime))
			time.Sleep(time.Duration(maxTime*float64(time.Second)*(1-c.cpuUsage)*100 - totalTime + maxTime))
		}
		packageCmprLen = len(maxData)
		packageData = append(packageData, maxData...)
		binary.LittleEndian.PutUint64(packageData[2:10], uint64(packageOriLen))
		binary.LittleEndian.PutUint64(packageData[10:18], uint64(packageCmprLen))

		output <- packageData
		// ONLY TEST startTs, totalEvents, oriLen, cmprLen
		endBuffer := make([]byte, 34)
		binary.LittleEndian.PutUint16(endBuffer[0:2], 2)
		binary.LittleEndian.PutUint64(endBuffer[2:10], uint64(aggData.StartTs))
		binary.LittleEndian.PutUint64(endBuffer[10:18], uint64(aggData.TotalEvents))
		binary.LittleEndian.PutUint64(endBuffer[18:26], uint64(packageOriLen))
		binary.LittleEndian.PutUint64(endBuffer[26:34], uint64(packageCmprLen))
		// 传输
		output <- endBuffer
		outputBufSize.Add(int64(len(endBuffer)))
	}
}

// 负责从队列中获得一个AggData，将其乱序压缩，然后交给Transporter
func (c *Compressor) Compress(input <-chan *aggregate.AggregateData, output chan<- []byte, outputBufSize *atomic.Int64) {
	for {
		aggData := <-input
		// 初始化本buffer内的各列数据起始offset及总长度len
		aggData.InitOffsetAndLen()
		// ONLY TEST buffer start | package i| buffer end
		startBuffer := make([]byte, 2)
		binary.LittleEndian.PutUint16(startBuffer[0:2], 0)
		output <- startBuffer
		outputBufSize.Add(int64(len(startBuffer)))
		bufferOriLen := 0
		bufferCmprLen := 0
		for {
			offAndLens := aggData.GetOffsetAndLen()

			packageData := make([]byte, 18)
			binary.LittleEndian.PutUint16(packageData[0:2], 1)
			packageOriLen := 0
			packageCmprLen := 0
			// 1. 调用monitor，计算得到当前压缩比
			cmprIntro := c.monitor.GetAlphaRatio(offAndLens)
			if len(cmprIntro) == 0 {
				break
			}
			// 2. 针对计算得到的压缩比，对每列做压缩
			all := float64(0)
			for _, cmprintro := range cmprIntro {
				// 2.1 根据列名和bytes，得到应该压缩的数据列
				data := aggData.GetColumnData(cmprintro.Point.Column, cmprintro.ByteNum)
				// 2.2 根据压缩算法名，得到压缩器,并更新offset
				compressor := c.allCmprs.GetCompressorByType(cmprintro.Point.Cmpr)
				// 2.3 压缩
				oriLen := len(data)
				packageOriLen += len(data)

				cmprLen1 := len(packageData)
				startTime := time.Now()

				packageData = append(packageData, compressor.Compress(data)...)

				tempEndTime := time.Now()
				tempSumTime := tempEndTime.Sub(startTime).Seconds()
				if c.cpuUsage < 1 {
					time.Sleep(time.Duration(tempSumTime * float64(time.Second) * (1 - c.cpuUsage) * 100))
				}
				endTime := time.Now()
				all += endTime.Sub(startTime).Seconds()
				cmprLen := len(packageData) - cmprLen1
				packageCmprLen += cmprLen

				// 2.5 更新monitor中的cache，同时更新offset
				c.monitor.UpdateCompressionInfo(cmprintro.Point.Column, cmprintro.Point.Cmpr, oriLen, cmprLen, endTime.Sub(startTime).Seconds())
			}

			fmt.Println("real compress bandwitdh", float64(packageCmprLen)/all)
			fmt.Println("record network bandwidth", c.monitor.net_bandwitdh)
			binary.LittleEndian.PutUint64(packageData[2:10], uint64(packageOriLen))
			binary.LittleEndian.PutUint64(packageData[10:18], uint64(packageCmprLen))
			bufferOriLen += packageOriLen
			bufferCmprLen += packageCmprLen
			// fmt.Println("compress package", c.testTimes)
			c.testTimes++
			output <- packageData
			outputBufSize.Add(int64(len(packageData)))
		}
		// ONLY TEST startTs, totalEvents, oriLen, cmprLen
		endBuffer := make([]byte, 34)
		binary.LittleEndian.PutUint16(endBuffer[0:2], 2)
		binary.LittleEndian.PutUint64(endBuffer[2:10], uint64(aggData.StartTs))
		binary.LittleEndian.PutUint64(endBuffer[10:18], uint64(aggData.TotalEvents))
		binary.LittleEndian.PutUint64(endBuffer[18:26], uint64(bufferOriLen))
		binary.LittleEndian.PutUint64(endBuffer[26:34], uint64(bufferCmprLen))
		// 传输
		output <- endBuffer
		outputBufSize.Add(int64(len(endBuffer)))
	}
}
