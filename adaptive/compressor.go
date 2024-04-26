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
	monitor    *Monitor
	allCmprs   *allcompressor.AllCompressor
	testTimes  int64
	cpuUsage   float64
	obBest     []byte
	mbBest     [][]common.CmprTypeData
	hybridBest [][]common.CompressionIntro
	idx        int
}

func NewCompressor(monitor *Monitor, cpuUsage float64, obBest []byte, mbBest [][]common.CmprTypeData, hybridBest [][]common.CompressionIntro) *Compressor {
	return &Compressor{
		monitor:    monitor,
		allCmprs:   allcompressor.NewAllCompressor(),
		cpuUsage:   cpuUsage,
		obBest:     obBest,
		mbBest:     mbBest,
		hybridBest: hybridBest,
		idx:        0,
	}
}

func (c *Compressor) TestByCmprType(input <-chan common.DataWithInfo, output chan<- []byte, cmprType byte, outputBufSize *atomic.Int64) {
	for {
		dataWithInfo := <-input
		data := dataWithInfo.Data

		startBuffer := make([]byte, 2)
		binary.LittleEndian.PutUint16(startBuffer[0:2], 0)
		output <- startBuffer
		outputBufSize.Add(int64(len(startBuffer)))

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

		fmt.Println("compress gain: ", float64(len(data))/float64(len(packageData)-18))
		fmt.Println("compress bandwitdh: ", float64(len(packageData)-18)/tempSumTime)
		packageCmprLen := len(packageData) - 18
		binary.LittleEndian.PutUint64(packageData[2:10], uint64(packageOriLen))
		binary.LittleEndian.PutUint64(packageData[10:18], uint64(packageCmprLen))

		// fmt.Println("compress package", c.testTimes)
		c.testTimes++
		output <- packageData
		outputBufSize.Add(int64(len(packageData)))

		endBuffer := make([]byte, 34)
		binary.LittleEndian.PutUint16(endBuffer[0:2], 2)
		binary.LittleEndian.PutUint64(endBuffer[2:10], uint64(dataWithInfo.Ts))
		// TODO divide的时候是否要传递totalEvents
		binary.LittleEndian.PutUint64(endBuffer[10:18], uint64(dataWithInfo.TotalEvents))
		binary.LittleEndian.PutUint64(endBuffer[18:26], uint64(packageOriLen))
		binary.LittleEndian.PutUint64(endBuffer[26:34], uint64(packageCmprLen))
		output <- endBuffer
		outputBufSize.Add(int64(len(endBuffer)))
	}
}

func (c *Compressor) TestOneBest(input <-chan common.DataWithInfo, output chan<- []byte, outputBufSize *atomic.Int64) {
	for {
		dataWithInfo := <-input
		data := dataWithInfo.Data

		startBuffer := make([]byte, 2)
		binary.LittleEndian.PutUint16(startBuffer[0:2], 0)
		output <- startBuffer
		outputBufSize.Add(int64(len(startBuffer)))

		packageData := make([]byte, 18)
		binary.LittleEndian.PutUint16(packageData[0:2], 1)
		packageOriLen := len(data)

		// TODO 得到当前最优的压缩方法 遍历选择最优
		// 计算每一个压缩算法的压缩率
		maxAnswer := 0.0
		maxData := make([]byte, 0)
		totalTime := 0.0
		maxTime := 0.0
		maxType := common.INVALID_START
		if len(c.obBest) == 0 || c.obBest == nil {
			for cmprType := common.INVALID_START + 1; cmprType < common.INVALID_END; cmprType++ {
				if cmprType == common.RLE || cmprType == common.DELTA {
					continue
				}
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
				// fmt.Println("compress type", cmprType)
				// fmt.Println("compress bw", float64(len(tempData))/float64(endTime.Sub(startTime).Seconds())*c.cpuUsage)
				// fmt.Println("compress gain", float64(len(data))/float64(len(tempData)))
				// fmt.Println("real bw", min(c.monitor.net_bandwitdh, float64(len(tempData))/float64(endTime.Sub(startTime).Seconds())*c.cpuUsage))
				tempAnswer := min(c.monitor.net_bandwitdh, float64(len(tempData))/float64(endTime.Sub(startTime).Seconds())*c.cpuUsage) * float64(len(data)) / float64(len(tempData))
				if tempAnswer > maxAnswer {
					maxAnswer = tempAnswer
					maxData = tempData
					maxTime = endTime.Sub(startTime).Seconds()
					maxType = cmprType
				}
			}
			fmt.Println("maxType", maxType)
			if c.cpuUsage < 1 {
				fmt.Println("max time", maxTime)
				fmt.Println("sleep time", time.Duration(maxTime*float64(time.Second)*(1-c.cpuUsage)*100-totalTime+maxTime))
				time.Sleep(time.Duration(maxTime*float64(time.Second)*(1-c.cpuUsage)*100 - totalTime + maxTime))
			}
		} else {
			cmpr := c.allCmprs.GetCompressorByType(c.obBest[c.idx])
			startTime := time.Now()
			maxData = cmpr.Compress(data)
			endTime := time.Now()
			maxTime = endTime.Sub(startTime).Seconds()
			if c.cpuUsage < 1 {
				time.Sleep(time.Duration(maxTime * float64(time.Second) * (1 - c.cpuUsage) * 100))
			}
			maxType = c.obBest[c.idx]
			c.idx++
			if c.idx == len(c.obBest) {
				c.idx = 0
				fmt.Println("reach end", maxType)
			}
		}

		// cmpr := c.allCmprs.GetCompressorByType(cmprType)

		// packageData = append(packageData, cmpr.Compress(data)...)
		fmt.Println("real compress bandwitdh", float64(len(maxData))/maxTime)
		fmt.Println("record network bandwidth", c.monitor.net_bandwitdh)
		fmt.Println("compress gain", float64(len(data))/float64(len(maxData)))
		fmt.Println("theoretical gain", min(c.monitor.net_bandwitdh, float64(len(maxData))/maxTime*c.cpuUsage)*float64(len(data))/float64(len(maxData)))
		packageData = append(packageData, maxData...)

		packageCmprLen := len(packageData) - 18
		binary.LittleEndian.PutUint64(packageData[2:10], uint64(packageOriLen))
		binary.LittleEndian.PutUint64(packageData[10:18], uint64(packageCmprLen))

		// fmt.Println("compress package", c.testTimes)
		c.testTimes++
		output <- packageData
		outputBufSize.Add(int64(len(packageData)))

		endBuffer := make([]byte, 34)
		binary.LittleEndian.PutUint16(endBuffer[0:2], 2)
		binary.LittleEndian.PutUint64(endBuffer[2:10], uint64(dataWithInfo.Ts))
		// TODO divide的时候是否要传递totalEvents
		binary.LittleEndian.PutUint64(endBuffer[10:18], uint64(dataWithInfo.TotalEvents))
		binary.LittleEndian.PutUint64(endBuffer[18:26], uint64(packageOriLen))
		binary.LittleEndian.PutUint64(endBuffer[26:34], uint64(packageCmprLen))
		output <- endBuffer
		outputBufSize.Add(int64(len(endBuffer)))
	}
}

func (c *Compressor) TestMultiBest(input <-chan common.DataWithInfo, output chan<- []byte, outputBufSize *atomic.Int64) {
	for {
		dataWithInfo := <-input
		data := dataWithInfo.Data

		startBuffer := make([]byte, 2)
		binary.LittleEndian.PutUint16(startBuffer[0:2], 0)
		output <- startBuffer
		outputBufSize.Add(int64(len(startBuffer)))

		bufferOriLen := 0
		bufferCmprLen := 0

		offAndLen := make([]common.OffAndLen, 1)
		offAndLen[0].Offset = 0
		offAndLen[0].Len = int64(len(data))

		packageData := make([]byte, 18)
		binary.LittleEndian.PutUint16(packageData[0:2], 1)

		totalTime := 0.0
		// 1. 调用monitor，计算得到当前压缩比
		if len(c.mbBest) == 0 || c.mbBest == nil {
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
				// fmt.Println("compress type", cmprType)
				// fmt.Println("compress bw", float64(len(tempData))/float64(endTime.Sub(startTime).Seconds())*c.cpuUsage)
				// fmt.Println("compress gain", float64(len(data))/float64(len(tempData)))
				// fmt.Println("real bw", min(c.monitor.net_bandwitdh, float64(len(tempData))/float64(endTime.Sub(startTime).Seconds())*c.cpuUsage))
				c.monitor.UpdateCompressionInfo(0, cmprType, len(data), len(tempData), float64(endTime.Sub(startTime).Seconds())*c.cpuUsage)
			}
			cmprIntro := c.monitor.GetAlphaRatio(offAndLen)
			offset := 0
			fmt.Println("maxType:")
			for _, cmprintro := range cmprIntro {
				fmt.Println("cmprType: ", cmprintro.Point.Cmpr, " byteNum: ", cmprintro.ByteNum)
			}
			for _, cmprintro := range cmprIntro {
				// 2.1 根据列名和bytes，得到应该压缩的数据列
				tempData := data[offset : offset+int(cmprintro.ByteNum)]
				offset += int(cmprintro.ByteNum)
				compressor := c.allCmprs.GetCompressorByType(cmprintro.Point.Cmpr)
				// 2.3 压缩
				bufferOriLen += len(tempData)

				cmprLen1 := len(packageData)
				startTime := time.Now()

				packageData = append(packageData, compressor.Compress(tempData)...)

				tempEndTime := time.Now()
				tempSumTime := tempEndTime.Sub(startTime).Seconds()
				if c.cpuUsage < 1 {
					time.Sleep(time.Duration(tempSumTime * float64(time.Second) * (1 - c.cpuUsage) * 100))
				}
				cmprLen := len(packageData) - cmprLen1
				bufferCmprLen += cmprLen
			}
			binary.LittleEndian.PutUint64(packageData[2:10], uint64(bufferOriLen))
			binary.LittleEndian.PutUint64(packageData[10:18], uint64(bufferCmprLen))
			output <- packageData
			outputBufSize.Add(int64(len(packageData)))
		} else {
			cmprIntro := c.mbBest[c.idx]
			c.idx++
			if c.idx == len(c.mbBest) {
				c.idx = 0
				fmt.Println("reach end", cmprIntro)
			}
			offset := 0
			allTime := float64(0)
			packageData := make([]byte, 18)
			binary.LittleEndian.PutUint16(packageData[0:2], 1)
			packageOriLen := 0
			packageCmprLen := 0
			for _, cmprintro := range cmprIntro {
				// temppackageData := make([]byte, 18)
				// binary.LittleEndian.PutUint16(temppackageData[0:2], 1)
				// tempPackageOriLen := 0
				// tempPackageCmprLen := 0
				// 2.1 根据列名和bytes，得到应该压缩的数据列
				tempData := data[offset : offset+int(cmprintro.ByteNum)]
				offset += int(cmprintro.ByteNum)
				// 2.2 根据压缩算法名，得到压缩器,并更新offset
				compressor := c.allCmprs.GetCompressorByType(byte(cmprintro.CmprType))
				// 2.3 压缩
				bufferOriLen += len(tempData)
				packageOriLen += len(tempData)

				cmprLen1 := len(packageData)
				startTime := time.Now()

				packageData = append(packageData, compressor.Compress(tempData)...)

				tempEndTime := time.Now()
				tempSumTime := tempEndTime.Sub(startTime).Seconds()
				allTime += tempSumTime
				if c.cpuUsage < 1 {
					time.Sleep(time.Duration(tempSumTime * float64(time.Second) * (1 - c.cpuUsage) * 100))
				}
				cmprLen := len(packageData) - cmprLen1
				packageCmprLen += cmprLen
				bufferCmprLen += cmprLen
			}
			binary.LittleEndian.PutUint64(packageData[2:10], uint64(packageOriLen))
			binary.LittleEndian.PutUint64(packageData[10:18], uint64(packageCmprLen))
			output <- packageData
			outputBufSize.Add(int64(len(packageData)))
			fmt.Println("real compress bandwitdh", float64(packageCmprLen)/float64(allTime))
			fmt.Println("record network bandwidth", c.monitor.net_bandwitdh)
			fmt.Println("compress gain", float64(packageOriLen)/float64(packageCmprLen))
			fmt.Println("theoretical gain", min(c.monitor.net_bandwitdh, float64(packageCmprLen)/allTime*c.cpuUsage)*float64(packageOriLen)/float64(packageCmprLen))
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
		outputBufSize.Add(int64(len(endBuffer)))
	}
}
func (c *Compressor) TestMultiSegmentBest(input <-chan *aggregate.AggregateData, output chan<- []byte, outputBufSize *atomic.Int64) {
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
			if len(c.hybridBest) == 0 || c.hybridBest == nil {
				for i, elem := range offAndLens {
					if (elem.Len - elem.Offset) > 0 {
						tempMin := min(int64(c.monitor.M), elem.Len-elem.Offset)
						data := aggData.GetColumnData(byte(i), tempMin)
						for j := common.INVALID_START + 1; j < common.INVALID_END; j++ {
							compressor := c.allCmprs.GetCompressorByType(j)
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
							cmprLen := len(packageData) - cmprLen1
							packageCmprLen += cmprLen
							c.monitor.UpdateCompressionInfo(byte(i), j, oriLen, cmprLen, endTime.Sub(startTime).Seconds())
						}
					}
				}
				cmprIntro := c.monitor.GetAlphaRatio(offAndLens)
				fmt.Println("maxType:")
				for _, cmprintro := range cmprIntro {
					fmt.Println("column: ", cmprintro.Point.Column, "cmprType: ", cmprintro.Point.Cmpr, " byteNum: ", cmprintro.ByteNum)
				}
				// fmt.Printf("%v\n", cmprIntro)
			} else {
				cmprIntro := c.hybridBest[c.idx]
				c.idx++
				if c.idx == len(c.hybridBest) {
					c.idx = 0
					fmt.Println("reach end", cmprIntro)
				}
				for _, cmprintro := range cmprIntro {
					// 2.1 根据列名和bytes，得到应该压缩的数据列
					data := aggData.GetColumnData(cmprintro.Point.Column, cmprintro.ByteNum)
					// 2.2 根据压缩算法名，得到压缩器,并更新offset
					compressor := c.allCmprs.GetCompressorByType(cmprintro.Point.Cmpr)
					// 2.3 压缩
					packageOriLen += len(data)

					cmprLen1 := len(packageData)
					startTime := time.Now()

					packageData = append(packageData, compressor.Compress(data)...)

					tempEndTime := time.Now()
					tempSumTime := tempEndTime.Sub(startTime).Seconds()
					if c.cpuUsage < 1 {
						time.Sleep(time.Duration(tempSumTime * float64(time.Second) * (1 - c.cpuUsage) * 100))
					}
					cmprLen := len(packageData) - cmprLen1
					packageCmprLen += cmprLen

				}

			}
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
func (c *Compressor) TestOurs(input <-chan common.DataWithInfo, output chan<- []byte, outputBufSize *atomic.Int64) {
	for {
		dataWithInfo := <-input
		data := dataWithInfo.Data

		startBuffer := make([]byte, 2)
		binary.LittleEndian.PutUint16(startBuffer[0:2], 0)
		output <- startBuffer
		outputBufSize.Add(int64(len(startBuffer)))

		bufferOriLen := 0
		bufferCmprLen := 0

		offAndLen := make([]common.OffAndLen, 1)
		offAndLen[0].Offset = 0
		offAndLen[0].Len = int64(len(data))
		for offAndLen[0].Offset < offAndLen[0].Len {

			// 1. 调用monitor，计算得到当前压缩比
			startTime := time.Now()
			cmprIntro := c.monitor.GetAlphaRatio(offAndLen)
			endTime := time.Now()
			fmt.Println("get alpha ratio time", endTime.Sub(startTime).Seconds())
			offset := 0
			all := float64(0)
			allCmprLen := 0
			// 2. 针对计算得到的压缩比，对每列做压缩
			for _, cmprintro := range cmprIntro {
				packageOriLen := 0
				packageCmprLen := 0
				packageData := make([]byte, 18)
				binary.LittleEndian.PutUint16(packageData[0:2], 1)
				// 2.1 根据列名和bytes，得到应该压缩的数据列
				tempData := data[offset : offset+int(cmprintro.ByteNum)]
				offset += int(cmprintro.ByteNum)
				// 2.2 根据压缩算法名，得到压缩器,并更新offset
				compressor := c.allCmprs.GetCompressorByType(cmprintro.Point.Cmpr)
				// 2.3 压缩
				oriLen := len(tempData)
				packageOriLen += len(tempData)

				cmprLen1 := len(packageData)
				startTime := time.Now()

				packageData = append(packageData, compressor.Compress(tempData)...)

				tempEndTime := time.Now()
				tempSumTime := tempEndTime.Sub(startTime).Seconds()
				all += tempSumTime
				if c.cpuUsage < 1 {
					time.Sleep(time.Duration(tempSumTime * float64(time.Second) * (1 - c.cpuUsage) * 100))
				}
				endTime := time.Now()
				cmprLen := len(packageData) - cmprLen1
				packageCmprLen += cmprLen

				// 2.5 更新monitor中的cache
				c.monitor.UpdateCompressionInfo(cmprintro.Point.Column, cmprintro.Point.Cmpr, oriLen, cmprLen, endTime.Sub(startTime).Seconds())
				binary.LittleEndian.PutUint64(packageData[2:10], uint64(packageOriLen))
				binary.LittleEndian.PutUint64(packageData[10:18], uint64(packageCmprLen))
				bufferOriLen += packageOriLen
				bufferCmprLen += packageCmprLen
				allCmprLen += packageCmprLen
				output <- packageData[0:]
				outputBufSize.Add(int64(len(packageData)))
			}
			offAndLen[0].Offset += int64(offset)

			fmt.Println("real compress bandwitdh", float64(allCmprLen)/all)
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
		outputBufSize.Add(int64(len(endBuffer)))
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
