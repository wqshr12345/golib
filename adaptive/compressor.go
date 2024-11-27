package adaptive

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	allcompressor "github.com/wqshr12345/golib/adaptive/all_compressor"
	"github.com/wqshr12345/golib/common"
)

type Compressor struct {
	monitor     *Monitor
	allCmprs    *allcompressor.AllCompressor
	allDecmprs  *allcompressor.AllDecompressor
	testTimes   int64
	dcmprTimes  int64
	obBest      []byte
	mbBest      [][]common.CmprTypeData
	hybridBest  [][]common.CompressionIntro
	idx         int
	mutex       sync.Mutex
	wg          sync.WaitGroup
	cmprThreads int
}

func NewCompressor(monitor *Monitor, obBest []byte, mbBest [][]common.CmprTypeData, hybridBest [][]common.CompressionIntro, cmprThreads int) *Compressor {
	return &Compressor{
		monitor:     monitor,
		allCmprs:    allcompressor.NewAllCompressor(),
		allDecmprs:  allcompressor.NewAllDecompressor(),
		obBest:      obBest,
		mbBest:      mbBest,
		hybridBest:  hybridBest,
		idx:         0,
		cmprThreads: cmprThreads,
	}
}

func (c *Compressor) CompressByCmprType(input <-chan common.DataWithInfo, output chan<- []byte, cmprType byte, cmprBufSize *atomic.Int64, outputBufSize *atomic.Int64, rate float64) {
	for {
		dataWithInfo := <-input
		// ts := time.Now()
		// fmt.Println("第", c.testTimes, "个buffer接受分割,时间为: ", ts.UnixNano())
		data := dataWithInfo.Data

		// startBuffer := make([]byte, 2)
		// binary.LittleEndian.PutUint16(startBuffer[0:2], 0)
		// output <- startBuffer
		// outputBufSize.Add(int64(len(startBuffer)))

		packageData := make([]byte, 58)
		binary.LittleEndian.PutUint16(packageData[0:2], 1)
		packageOriLen := len(data)

		cmpr := c.allCmprs.GetCompressorByType(cmprType)

		startTime := time.Now()
		// fmt.Println("第", c.testTimes, "个buffer压缩开始,时间为: ", startTime.UnixNano())
		// startRusage, _ := common.GetRusage()
		cmprData := cmpr.Compress(data)
		endTime := time.Now()

		packageData = append(packageData, cmprData...)

		// endRusage, _ := common.GetRusage()

		// fmt.Println("第", c.testTimes, "个buffer压缩结束,时间为: ", endTime.UnixNano())
		fmt.Println("column  Full", " cmprType ", cmprType, " cmprRatio ", float64(len(data))/float64(len(packageData)-58), " cmprBw ", float64(len(data))/endTime.Sub(startTime).Seconds())
		// userTime, _ :c= common.CpuTimeDiff(startRusage, endRusage)
		// endTime := time.Now()

		// fmt.Println("compress gain: ", float64(len(data))/float64(len(packageData)-18))
		// fmt.Println("compress bandwitdh: ", float64(len(packageData)-18)/tempSumTime)
		fmt.Println("压缩时间: ", endTime.Sub(startTime).Seconds())
		fmt.Println("传输时间: ", float64(len(cmprData))/rate)
		packageCmprLen := len(packageData) - 58
		binary.LittleEndian.PutUint64(packageData[2:10], uint64(packageOriLen))
		binary.LittleEndian.PutUint64(packageData[10:18], uint64(packageCmprLen))
		binary.LittleEndian.PutUint64(packageData[18:26], uint64(dataWithInfo.Ts))
		binary.LittleEndian.PutUint64(packageData[26:34], uint64(dataWithInfo.TotalEvents))
		binary.LittleEndian.PutUint64(packageData[50:58], uint64(c.cmprThreads))
		// fmt.Println("compress package", c.testTimes)
		atomic.AddInt64(&c.testTimes, 1)
		outputBufSize.Add(int64(len(packageData)))
		output <- packageData
		cmprBufSize.Add(-int64(len(data)))

		// endBuffer := make([]byte, 34)
		// binary.LittleEndian.PutUint16(endBuffer[0:2], 2)
		// binary.LittleEndian.PutUint64(endBuffer[2:10], uint64(dataWithInfo.Ts))
		// // TODO divide的时候是否要传递totalEvents
		// binary.LittleEndian.PutUint64(endBuffer[10:18], uint64(dataWithInfo.TotalEvents))
		// binary.LittleEndian.PutUint64(endBuffer[18:26], uint64(packageOriLen))
		// binary.LittleEndian.PutUint64(endBuffer[26:34], uint64(packageCmprLen))
		// output <- endBuffer
		// outputBufSize.Add(int64(len(endBuffer)))
	}
}

// TODO
func (c *Compressor) DecompressByCmprType(input <-chan common.DataWithInfo, cmprType byte) {
	for {
		dataWithInfo := <-input
		ts := time.Now()
		fmt.Println("第", c.testTimes, "个buffer接受,时间为: ", ts.UnixNano())
		data := dataWithInfo.Data
		dcmpr := c.allDecmprs.GetDecompressorByType(cmprType)
		dcmpr.Decompress(nil, data)
	}
}

func (c *Compressor) CompressOneBest(input <-chan common.DataWithInfo, output chan<- []byte, cmprBufSize *atomic.Int64, outputBufSize *atomic.Int64) {
	for {
		dataWithInfo := <-input
		data := dataWithInfo.Data

		// startBuffer := make([]byte, 2)
		// binary.LittleEndian.PutUint16(startBuffer[0:2], 0)
		// output <- startBuffer
		// outputBufSize.Add(int64(len(startBuffer)))

		packageData := make([]byte, 58)
		binary.LittleEndian.PutUint16(packageData[0:2], 1)
		packageOriLen := len(data)

		// TODO 得到当前最优的压缩方法 遍历选择最优
		// 计算每一个压缩算法的压缩率
		maxAnswer := 0.0
		maxData := make([]byte, 0)
		totalTime := 0.0
		maxType := common.INVALID_START
		if len(c.obBest) == 0 || c.obBest == nil {
			for cmprType := common.INVALID_START + 1; cmprType < common.INVALID_END; cmprType++ {
				// if cmprType == common.RLE || cmprType == common.DELTA {
				// 	continue
				// }
				cmpr := c.allCmprs.GetCompressorByType(cmprType)
				startTime := time.Now()
				// startRusage, _ := common.GetRusage()

				tempData := cmpr.Compress(data)
				// tempEndTime := time.Now()
				// tempSumTime := tempEndTime.Sub(startTime).Seconds()

				endTime := time.Now()
				// endRusage, _ := common.GetRusage()
				// userTime, systemTime := common.CpuTimeDiff(startRusage, endRusage)

				totalTime += endTime.Sub(startTime).Seconds()
				// 压缩带宽 * 压缩增益
				// fmt.Println("compress type", cmprType)
				// fmt.Println("compress bw", float64(len(tempData))/float64(endTime.Sub(startTime).Seconds())*c.cpuUsage)
				// fmt.Println("compress gain", float64(len(data))/float64(len(tempData)))
				// fmt.Println("real bw", min(c.monitor.net_bandwitdh, float64(len(tempData))/float64(endTime.Sub(startTime).Seconds())*c.cpuUsage))
				tempAnswer := min(c.monitor.net_bandwitdh, float64(len(tempData))/endTime.Sub(startTime).Seconds()) * float64(len(data)) / float64(len(tempData))
				if tempAnswer > maxAnswer {
					maxAnswer = tempAnswer
					maxData = tempData
					// maxTime = endTime.Sub(startTime).Seconds()
					maxType = cmprType
				}
				// fmt.Printf("User CPU time: %v\n", userTime)
				// fmt.Printf("System CPU time: %v\n", systemTime)
				// fmt.Println("cmprtype: ", cmprType, "compresionGain: ", float64(len(data))/float64(len(tempData)), "compressionBw: ", float64(len(tempData))/float64(endTime.Sub(startTime).Seconds()), "compressionTime", userTime.Seconds())
			}
			fmt.Println("maxType", maxType)
			// fmt.Println("record network bandwidth", c.monitor.net_bandwitdh)
			// fmt.Println("theoretical gain", maxAnswer)
		} else {
			cmpr := c.allCmprs.GetCompressorByType(c.obBest[c.idx])
			// startTime := time.Now()
			maxData = cmpr.Compress(data)
			// endTime := time.Now()
			// maxTime = endTime.Sub(startTime).Seconds()

			// maxType = c.obBest[c.idx]
			c.idx++
			// fmt.Println("cmpr type", maxType)
			// fmt.Println("real compress bandwitdh", float64(len(maxData))/maxTime)
			// fmt.Println("record network bandwidth", c.monitor.net_bandwitdh)
			// fmt.Println("theoretical gain", min(c.monitor.net_bandwitdh, float64(len(maxData))/maxTime*c.cpuUsage)*float64(len(data))/float64(len(maxData)))
			// if c.idx == len(c.obBest) {
			// 	c.idx = 0
			// 	fmt.Println("reach end", maxType)
			// }
		}

		// cmpr := c.allCmprs.GetCompressorByType(cmprType)

		// packageData = append(packageData, cmpr.Compress(data)...)
		// fmt.Println("record network bandwidth", c.monitor.net_bandwitdh)
		// fmt.Println("compress gain", float64(len(data))/float64(len(maxData)))
		packageData = append(packageData, maxData...)

		packageCmprLen := len(packageData) - 58
		binary.LittleEndian.PutUint64(packageData[2:10], uint64(packageOriLen))
		binary.LittleEndian.PutUint64(packageData[10:18], uint64(packageCmprLen))
		binary.LittleEndian.PutUint64(packageData[18:26], uint64(dataWithInfo.Ts))
		binary.LittleEndian.PutUint64(packageData[26:34], uint64(dataWithInfo.TotalEvents))
		binary.LittleEndian.PutUint64(packageData[50:58], uint64(c.cmprThreads))
		// fmt.Println("compress package", c.testTimes)
		c.testTimes++
		outputBufSize.Add(int64(len(packageData)))
		output <- packageData
		cmprBufSize.Add(-int64(len(data)))
		// endBuffer := make([]byte, 34)
		// binary.LittleEndian.PutUint16(endBuffer[0:2], 2)
		// binary.LittleEndian.PutUint64(endBuffer[2:10], uint64(dataWithInfo.Ts))
		// // TODO divide的时候是否要传递totalEvents
		// binary.LittleEndian.PutUint64(endBuffer[10:18], uint64(dataWithInfo.TotalEvents))
		// binary.LittleEndian.PutUint64(endBuffer[18:26], uint64(packageOriLen))
		// binary.LittleEndian.PutUint64(endBuffer[26:34], uint64(packageCmprLen))
		// output <- endBuffer
		// outputBufSize.Add(int64(len(endBuffer)))
	}
}

func (c *Compressor) CompressMultiBest(input <-chan common.DataWithInfo, output chan<- []byte, cmprBufSize *atomic.Int64, outputBufSize *atomic.Int64) {
	for {
		c.testTimes++
		dataWithInfo := <-input
		data := dataWithInfo.Data

		// startBuffer := make([]byte, 2)
		// binary.LittleEndian.PutUint16(startBuffer[0:2], 0)
		// output <- startBuffer
		// outputBufSize.Add(int64(len(startBuffer)))

		bufferOriLen := 0
		bufferCmprLen := 0

		offAndLen := make([]common.OffAndLen, 1)
		offAndLen[0].Offset = 0
		offAndLen[0].Len = int64(len(data))
		offAndLen[0].Name = "Full"

		packageData := make([]byte, 58)
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
				c.monitor.UpdateCompressionInfo("Full", cmprType, len(data), len(tempData), endTime.Sub(startTime).Seconds(), true) //float64(endTime.Sub(startTime).Seconds())
				// fmt.Printf("Compress Method: %d, Compress BandWidth: %f, Compress\n", cmprType, float64(oriLen)/endTime.Sub(startTime).Seconds())

			}
			// c.monitor.PrintCacheInfo()

			// fmt.Println("MultiBest Update Finished.")
			cmprIntro := c.monitor.GetAlphaRatio(offAndLen)
			offset := 0
			fmt.Println("maxType:")
			// for cmprType := common.INVALID_START + 1; cmprType < common.INVALID_END; cmprType++ {
			// fmt.Println("cmprType: ", cmprType, " byteNum: ", cmprIntro[cmprType].ByteNum)
			for _, cmprintro := range cmprIntro {
				for i := range cmprintro {
					fmt.Println("cmprType: ", cmprintro[i].CmprType, " byteNum: ", cmprintro[i].ByteNum)
				}
			}
			for _, cmprintro := range cmprIntro {
				// 2.1 根据列名和bytes，得到应该压缩的数据列
				for _, cmpr := range cmprintro {
					if cmpr.ByteNum == 0 {
						continue
					}
					tempData := data[offset : offset+int(cmpr.ByteNum)]
					offset += int(cmpr.ByteNum)
					compressor := c.allCmprs.GetCompressorByType(cmpr.CmprType)
					// 2.3 压缩
					bufferOriLen += len(tempData)

					cmprLen1 := len(packageData)

					packageData = append(packageData, compressor.Compress(tempData)...)

					cmprLen := len(packageData) - cmprLen1
					bufferCmprLen += cmprLen
				}
			}
			binary.LittleEndian.PutUint64(packageData[2:10], uint64(bufferOriLen))
			binary.LittleEndian.PutUint64(packageData[10:18], uint64(bufferCmprLen))
			binary.LittleEndian.PutUint64(packageData[18:26], uint64(dataWithInfo.Ts))
			binary.LittleEndian.PutUint64(packageData[26:34], uint64(dataWithInfo.TotalEvents))
			binary.LittleEndian.PutUint64(packageData[50:58], uint64(c.cmprThreads))
			outputBufSize.Add(int64(len(packageData)))
			output <- packageData
			cmprBufSize.Add(-int64(len(data)))
		} else {
			cmprIntro := c.mbBest[c.idx]
			c.idx++
			// if c.idx == len(c.mbBest) {
			// 	c.idx = 0
			// 	fmt.Println("reach end", cmprIntro)
			// }
			offset := 0
			allTime := float64(0)

			fmt.Println("start new package")
			packageData := make([]byte, 58)
			packageOriLen := 0
			packageCmprLen := 0
			for _, cmprintro := range cmprIntro {
				// extra := make([]byte, 18)
				// packageData = append(packageData, extra...)
				binary.LittleEndian.PutUint16(packageData[0:2], 1)
				if cmprintro.ByteNum == 0 {
					continue
				}
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
				tempCmprData := compressor.Compress(tempData)

				tempEndTime := time.Now()
				tempSumTime := tempEndTime.Sub(startTime).Seconds()
				allTime += tempSumTime
				packageData = append(packageData, tempCmprData...)
				cmprLen := len(packageData) - cmprLen1
				packageCmprLen += cmprLen
				bufferCmprLen += cmprLen

				// fmt.Println("cmpr type", cmprintro.CmprType)
				// fmt.Println("cmpr bytes", cmprintro.ByteNum)
				// fmt.Println("real compress bandwitdh", float64(packageCmprLen)/float64(allTime))
				// fmt.Println("record network bandwidth", c.monitor.net_bandwitdh)
				// fmt.Println("compress gain", float64(packageOriLen)/float64(packageCmprLen))
				// fmt.Println("theoretical gain", min(c.monitor.net_bandwitdh, float64(packageCmprLen)/allTime*c.cpuUsage)*float64(packageOriLen)/float64(packageCmprLen))
			}
			binary.LittleEndian.PutUint64(packageData[2:10], uint64(packageOriLen))
			binary.LittleEndian.PutUint64(packageData[10:18], uint64(packageCmprLen))
			binary.LittleEndian.PutUint64(packageData[18:26], uint64(dataWithInfo.Ts))
			binary.LittleEndian.PutUint64(packageData[26:34], uint64(dataWithInfo.TotalEvents))
			binary.LittleEndian.PutUint64(packageData[50:58], uint64(c.cmprThreads))
			outputBufSize.Add(int64(len(packageData)))
			output <- packageData
			cmprBufSize.Add(-int64(len(data)))
		}

		// ONLY TEST startTs, totalEvents, oriLen, cmprLen
		// endBuffer := make([]byte, 34)
		// binary.LittleEndian.PutUint16(endBuffer[0:2], 2)
		// binary.LittleEndian.PutUint64(endBuffer[2:10], uint64(dataWithInfo.Ts))
		// binary.LittleEndian.PutUint64(endBuffer[10:18], uint64(dataWithInfo.TotalEvents))
		// binary.LittleEndian.PutUint64(endBuffer[18:26], uint64(bufferOriLen))
		// binary.LittleEndian.PutUint64(endBuffer[26:34], uint64(bufferCmprLen))
		// // 传输
		// output <- endBuffer
		// outputBufSize.Add(int64(len(endBuffer)))
	}
}

// func (c *Compressor) TestMultiSegmentBest(input <-chan *AggregateData, output chan<- []byte, outputBufSize *atomic.Int64) {
// 	for {
// 		aggData := <-input
// 		// 初始化本buffer内的各列数据起始offset及总长度len
// 		aggData.InitOffsetAndLen()
// 		// ONLY TEST buffer start | package i| buffer end
// 		startBuffer := make([]byte, 2)
// 		binary.LittleEndian.PutUint16(startBuffer[0:2], 0)
// 		output <- startBuffer
// 		outputBufSize.Add(int64(len(startBuffer)))
// 		bufferOriLen := 0
// 		bufferCmprLen := 0
// 		testTotalByteNum := 0
// 		// for循环是压缩每个package
// 		for {
// 			offAndLens := aggData.GetOffsetAndLen()
// 			packageData := make([]byte, 18)
// 			binary.LittleEndian.PutUint16(packageData[0:2], 1)
// 			packageOriLen := 0
// 			packageCmprLen := 0
// 			if len(c.hybridBest) == 0 || c.hybridBest == nil {
// 				// 1. 首先利用最新数据更新cache
// 				for i, elem := range offAndLens {
// 					if (elem.Len - elem.Offset) > 0 {
// 						tempMin := min(int64(c.monitor.M), elem.Len-elem.Offset)
// 						data := aggData.GetColumnData2(byte(i), tempMin)
// 						for j := common.INVALID_START + 1; j < common.INVALID_END; j++ {
// 							compressor := c.allCmprs.GetCompressorByType(j)
// 							oriLen := len(data)
// 							startTime := time.Now()
// 							tempData := compressor.Compress(data)
// 							tempEndTime := time.Now()
// 							tempSumTime := tempEndTime.Sub(startTime).Seconds()
// 							if c.cpuUsage < 1 {
// 								time.Sleep(time.Duration(tempSumTime * float64(time.Second) * (1 - c.cpuUsage) * 100))
// 							}
// 							cmprLen := len(tempData)
// 							c.monitor.UpdateCompressionInfo(byte(i), j, oriLen, cmprLen, tempSumTime, true)
// 							tempData = nil
// 						}
// 					}
// 				}
// 				c.monitor.PrintCacheInfo()

// 				// 2. 计算得到最优比例，压缩传输
// 				cmprIntro := c.monitor.GetAlphaRatio(offAndLens)
// 				if len(cmprIntro) == 0 {
// 					fmt.Println("total ori len", bufferOriLen)
// 					fmt.Println("total bytenum", testTotalByteNum)
// 					break
// 				}
// 				fmt.Println("maxType:")
// 				for _, cmprintro := range cmprIntro {
// 					fmt.Println("column: ", cmprintro.Point.Column, "cmprType: ", cmprintro.Point.Cmpr, " byteNum: ", cmprintro.ByteNum)
// 				}
// 				all := float64(0)
// 				for _, cmprintro := range cmprIntro {
// 					// 2.1 根据列名和bytes，得到应该压缩的数据列
// 					data := aggData.GetColumnData(cmprintro.Point.Column, cmprintro.ByteNum)
// 					testTotalByteNum += int(cmprintro.ByteNum)
// 					// 2.2 根据压缩算法名，得到压缩器,并更新offset
// 					compressor := c.allCmprs.GetCompressorByType(cmprintro.Point.Cmpr)
// 					// 2.3 压缩
// 					oriLen := len(data)
// 					packageOriLen += len(data)

// 					cmprLen1 := len(packageData)
// 					startTime := time.Now()

// 					packageData = append(packageData, compressor.Compress(data)...)

// 					tempEndTime := time.Now()
// 					tempSumTime := tempEndTime.Sub(startTime).Seconds()
// 					if c.cpuUsage < 1 {
// 						time.Sleep(time.Duration(tempSumTime * float64(time.Second) * (1 - c.cpuUsage) * 100))
// 					}
// 					endTime := time.Now()
// 					all += endTime.Sub(startTime).Seconds()
// 					cmprLen := len(packageData) - cmprLen1
// 					packageCmprLen += cmprLen

// 					// 2.5 更新monitor中的cache，同时更新offset
// 					c.monitor.UpdateCompressionInfo(cmprintro.Point.Column, cmprintro.Point.Cmpr, oriLen, cmprLen, endTime.Sub(startTime).Seconds(), true)
// 				}
// 				c.monitor.PrintCacheInfo()

// 				// fmt.Printf("%v\n", cmprIntro)
// 			} else {
// 				cmprIntro := c.hybridBest[c.idx]
// 				c.idx++
// 				if c.idx == len(c.hybridBest) {
// 					c.idx = 0
// 					fmt.Println("reach end", cmprIntro) // 直接在这里break...
// 					break
// 				}
// 				for _, cmprintro := range cmprIntro {
// 					// 2.1 根据列名和bytes，得到应该压缩的数据列
// 					data := aggData.GetColumnData(cmprintro.Point.Column, cmprintro.ByteNum)
// 					// 2.2 根据压缩算法名，得到压缩器,并更新offset
// 					compressor := c.allCmprs.GetCompressorByType(cmprintro.Point.Cmpr)
// 					// 2.3 压缩
// 					packageOriLen += len(data)

// 					cmprLen1 := len(packageData)
// 					startTime := time.Now()

// 					packageData = append(packageData, compressor.Compress(data)...)

// 					tempEndTime := time.Now()
// 					tempSumTime := tempEndTime.Sub(startTime).Seconds()
// 					if c.cpuUsage < 1 {
// 						time.Sleep(time.Duration(tempSumTime * float64(time.Second) * (1 - c.cpuUsage) * 100))
// 					}
// 					cmprLen := len(packageData) - cmprLen1
// 					packageCmprLen += cmprLen

// 				}

//				}
//				fmt.Println("record network bandwidth", c.monitor.net_bandwitdh)
//				binary.LittleEndian.PutUint64(packageData[2:10], uint64(packageOriLen))
//				binary.LittleEndian.PutUint64(packageData[10:18], uint64(packageCmprLen))
//				bufferOriLen += packageOriLen
//				bufferCmprLen += packageCmprLen
//				// fmt.Println("compress package", c.testTimes)
//				c.testTimes++
//				output <- packageData
//				outputBufSize.Add(int64(len(packageData)))
//			}
//			// ONLY TEST startTs, totalEvents, oriLen, cmprLen
//			endBuffer := make([]byte, 34)
//			binary.LittleEndian.PutUint16(endBuffer[0:2], 2)
//			binary.LittleEndian.PutUint64(endBuffer[2:10], uint64(aggData.StartTs))
//			binary.LittleEndian.PutUint64(endBuffer[10:18], uint64(aggData.TotalEvents))
//			binary.LittleEndian.PutUint64(endBuffer[18:26], uint64(bufferOriLen))
//			binary.LittleEndian.PutUint64(endBuffer[26:34], uint64(bufferCmprLen))
//			// 传输
//			output <- endBuffer
//			outputBufSize.Add(int64(len(endBuffer)))
//		}
//	}
func (c *Compressor) CompressFull(input <-chan common.DataWithInfo, output chan<- []byte, cmprBufSize *atomic.Int64, outputBufSize *atomic.Int64, rate float64) {
	for {
		dataWithInfo := <-input
		data := dataWithInfo.Data

		// startBuffer := make([]byte, 2)
		// binary.LittleEndian.PutUint16(startBuffer[0:2], 0)
		// output <- startBuffer
		// outputBufSize.Add(int64(len(startBuffer)))

		bufferOriLen := 0
		bufferCmprLen := 0

		offAndLen := make([]common.OffAndLen, 1)
		offAndLen[0].Offset = 0
		offAndLen[0].Len = int64(len(data))
		offAndLen[0].Name = "Full"
		// ts := time.Now().UnixNano()
		// fmt.Println("第", c.testTimes, "个buffer压缩开始,时间为: ", ts)
		// for offAndLen[0].Offset < offAndLen[0].Len {
		// 1. 调用monitor，计算得到当前压缩比¥
		cmprIntros := c.monitor.GetAlphaRatio(offAndLen)
		offset := 0
		all := float64(0)
		allCmprLen := 0
		packageData := make([]byte, 58)
		binary.LittleEndian.PutUint16(packageData[0:2], 1)
		// 2. 针对计算得到的压缩比，对每列做压缩
		for key, cmprintros := range cmprIntros {
			for _, cmprintro := range cmprintros {
				if cmprintro.ByteNum == 0 {
					continue
				}
				packageOriLen := 0
				packageCmprLen := 0
				// 2.1 根据列名和bytes，得到应该压缩的数据列
				tempData := data[offset : offset+int(cmprintro.ByteNum)]
				offset += int(cmprintro.ByteNum)
				// 2.2 根据压缩算法名，得到压缩器,并更新offset
				compressor := c.allCmprs.GetCompressorByType(cmprintro.CmprType)
				// 2.3 压缩
				oriLen := len(tempData)
				packageOriLen += len(tempData)

				cmprLen1 := len(packageData)
				startTime := time.Now()

				packageData = append(packageData, compressor.Compress(tempData)...)
				// n := 4
				// c.wg.Add(n)
				// partSize := len(tempData) / n

				// for i := 0; i < n; i++ {
				// 	go func(index int) {
				// 		defer c.wg.Done()

				// 		// 计算当前协程要处理的数据切片范围
				// 		start := index * partSize
				// 		end := (index + 1) * partSize
				// 		if index == n-1 {
				// 			// 最后一个协程处理剩余的数据
				// 			end = len(tempData)
				// 		}

				// 		// 获取当前协程要处理的tempData切片
				// 		partTempData := tempData[start:end]

				// 		// 进行压缩
				// 		compressedData := compressor.Compress(partTempData)

				// 		// 加锁保护对packageData的并发访问
				// 		c.mutex.Lock()
				// 		packageData = append(packageData, compressedData...)
				// 		c.mutex.Unlock()

				// 	}(i)
				// }

				// c.wg.Wait()

				tempEndTime := time.Now()
				tempSumTime := tempEndTime.Sub(startTime).Seconds()
				all += tempSumTime

				endTime := time.Now()
				cmprLen := len(packageData) - cmprLen1
				packageCmprLen += cmprLen

				// 2.5 更新monitor中的cache
				c.monitor.UpdateCompressionInfo(key, cmprintro.CmprType, oriLen, cmprLen*4, endTime.Sub(startTime).Seconds(), false)
				fmt.Printf("Compress Method: %d, Compress BandWidth: %f\n", cmprintro.CmprType, float64(oriLen)/endTime.Sub(startTime).Seconds())

				bufferOriLen += packageOriLen
				bufferCmprLen += packageCmprLen
				allCmprLen += packageCmprLen
			}
			// }
			offAndLen[0].Offset += int64(offset)
			// fmt.Println("touseMLP", c.monitor.touseMLP)
			// s := time.Now()
			// if c.monitor.touseMLP == 1 {
			// 	c.monitor.UpdateCmprgainOfColumns(cmprIntro)
			// 	c.monitor.UpdateCmprbwOfColumns(cmprIntro)
			// 	c.monitor.touseMLP = 0
			// } else if c.monitor.touseMLP == 2 {
			// 	c.monitor.UpdateMLPgain(cmprIntro)
			// 	c.monitor.UpdateMLPbw(cmprIntro)
			// 	c.monitor.touseMLP = 0
			// }
			// e := time.Now()
			// elapsed := e.Sub(s)
			// fmt.Printf("代码运行时间: %v\n", elapsed)
			// fmt.Println("real compress bandwitdh", float64(allCmprLen)/all)
		}
		fmt.Println("压缩时间: ", all)
		fmt.Println("传输时间: ", float64(len(packageData))/rate)
		fmt.Println("最大时间: ", max(all, float64(len(packageData))/rate))
		binary.LittleEndian.PutUint64(packageData[2:10], uint64(bufferOriLen))
		binary.LittleEndian.PutUint64(packageData[10:18], uint64(bufferCmprLen))
		binary.LittleEndian.PutUint64(packageData[18:26], uint64(dataWithInfo.Ts))
		binary.LittleEndian.PutUint64(packageData[26:34], uint64(dataWithInfo.TotalEvents))
		binary.LittleEndian.PutUint64(packageData[50:58], uint64(c.cmprThreads))
		outputBufSize.Add(int64(len(packageData)))
		cmprBufSize.Add(-int64(len(data)))
		output <- packageData

		// ts = time.Now().UnixNano()
		// fmt.Println("第", c.testTimes, "个buffer压缩结束,时间为: ", ts)
		// c.monitor.PrintCacheInfo()
		// // ONLY TEST startTs, totalEvents, oriLen, cmprLen
		// endBuffer := make([]byte, 34)
		// binary.LittleEndian.PutUint16(endBuffer[0:2], 2)
		// binary.LittleEndian.PutUint64(endBuffer[2:10], uint64(dataWithInfo.Ts))
		// binary.LittleEndian.PutUint64(endBuffer[10:18], uint64(dataWithInfo.TotalEvents))
		// binary.LittleEndian.PutUint64(endBuffer[18:26], uint64(bufferOriLen))
		// binary.LittleEndian.PutUint64(endBuffer[26:34], uint64(bufferCmprLen))
		// 传输
		// output <- endBuffer
		// outputBufSize.Add(int64(len(endBuffer)))
		c.testTimes++
	}
}

func (c *Compressor) CompressRtcOneBest(input <-chan *AggregateData, output chan<- []byte, cmprBufSize *atomic.Int64, outputBufSize *atomic.Int64) {
	for {
		aggData := <-input
		// 初始化本buffer内的各列数据起始offset及总长度len
		aggData.InitOffsetAndLen()

		// // ONLY TEST buffer start | package i| buffer end
		// startBuffer := make([]byte, 2)
		// binary.LittleEndian.PutUint16(startBuffer[0:2], 0)
		// output <- startBuffer

		maxAnswer := 0.0
		maxData := make([]byte, 0)
		packageData := make([]byte, 58)
		packageOriLen := 0
		// packageCmprLen := 0
		binary.LittleEndian.PutUint16(packageData[0:2], 1)
		totalTime := 0.0
		maxType := common.NOCOMPRESSION
		if len(c.obBest) == 0 || c.obBest == nil {

			for cmprType := common.INVALID_START + 1; cmprType < common.INVALID_END; cmprType++ {
				// if cmprType == common.FLATE || cmprType == common.GZIP || cmprType == common.ZSTD1 || cmprType == common.ZSTD22 || cmprType == common.ZSTD8 || cmprType == common.LZO {
				// 	continue
				// }
				offAndLens := aggData.GetOffsetAndLen()
				tempData := make([]byte, 0)
				tempOriLen := 0

				// 2. 针对计算得到的压缩比，对每列做压缩
				// 针对每列做压缩
				for _, offandlen := range offAndLens {
					oriData := aggData.GetColumnData(offandlen.Name, offandlen.Len)
					tempOriLen += len(oriData)
					compressor := c.allCmprs.GetCompressorByType(cmprType)
					startTime := time.Now()
					cmprData := compressor.Compress(oriData)
					endTime := time.Now()
					totalTime += endTime.Sub(startTime).Seconds()
					tempData = append(tempData, cmprData...)
				}
				packageData = append(packageData, tempData...)

				// tempAnswer := min(c.monitor.net_bandwitdh, float64(tempOriLen)/totalTime)
				tempAnswer := min(c.monitor.net_bandwitdh, float64(len(tempData))/totalTime) * float64(tempOriLen) / float64(len(tempData))
				fmt.Println("压缩方法：", cmprType)
				fmt.Println("网络带宽：", c.monitor.net_bandwitdh)
				fmt.Println("压缩带宽：", float64(len(tempData))/totalTime)
				fmt.Println("压缩增益：", float64(tempOriLen)/float64(len(tempData)))
				if tempAnswer > maxAnswer {
					maxAnswer = tempAnswer
					maxData = packageData
					packageOriLen = tempOriLen
					// maxTime = totalTime
					maxType = cmprType

				}
				aggData.InitOffsetAndLen()
			}
			fmt.Println("maxType", maxType)
		} else {
			if c.idx >= len(c.obBest) {
				c.idx = 0
			}
			cmpr := c.allCmprs.GetCompressorByType(c.obBest[c.idx])
			// maxData = cmpr.Compress(data)
			offAndLens := aggData.GetOffsetAndLen()
			// tempData := make([]byte, 0)
			// tempOriLen := 0

			// 2. 针对计算得到的压缩比，对每列做压缩
			// 针对每列做压缩
			for _, offandlen := range offAndLens {
				oriData := aggData.GetColumnData(offandlen.Name, offandlen.Len)
				// tempOriLen += len(oriData)
				// compressor := c.allCmprs.GetCompressorByType(cmprType)
				// startTime := time.Now()
				cmprData := cmpr.Compress(oriData)
				maxData = append(maxData, cmprData...)
				// endTime := time.Now()
				// totalTime += endTime.Sub(startTime).Seconds()
				// tempData = append(tempData, cmprData...)
			}
			c.idx++
		}
		// packageCmprLen = len(maxData)
		packageData = append(packageData, maxData...)
		packageCmprLen := len(packageData) - 58
		binary.LittleEndian.PutUint64(packageData[2:10], uint64(packageOriLen))
		binary.LittleEndian.PutUint64(packageData[10:18], uint64(packageCmprLen))
		binary.LittleEndian.PutUint64(packageData[18:26], uint64(aggData.StartTs))
		binary.LittleEndian.PutUint64(packageData[26:34], uint64(aggData.TotalEvents))
		binary.LittleEndian.PutUint64(packageData[50:58], uint64(c.cmprThreads))

		outputBufSize.Add(int64(len(packageData)))
		output <- packageData
		cmprBufSize.Add(-int64(aggData.TotalLen))

		// ONLY TEST startTs, totalEvents, oriLen, cmprLen
		// endBuffer := make([]byte, 34)
		// binary.LittleEndian.PutUint16(endBuffer[0:2], 2)
		// binary.LittleEndian.PutUint64(endBuffer[2:10], uint64(aggData.StartTs))
		// binary.LittleEndian.PutUint64(endBuffer[10:18], uint64(aggData.TotalEvents))
		// binary.LittleEndian.PutUint64(endBuffer[18:26], uint64(packageOriLen))
		// binary.LittleEndian.PutUint64(endBuffer[26:34], uint64(packageCmprLen))
		// // 传输
		// output <- endBuffer
	}
}

// 负责从队列中获得一个AggData，将其乱序压缩，然后交给Transporter
func (c *Compressor) CompressIncr(input <-chan *AggregateData, output chan<- []byte, cmprBufSize *atomic.Int64, outputBufSize *atomic.Int64, rate float64) {
	for {
		aggData := <-input
		// 初始化本buffer内的各列数据起始offset及总长度len
		aggData.InitOffsetAndLen()
		// ONLY TEST buffer start | package i| buffer end
		// startBuffer := make([]byte, 2)
		// binary.LittleEndian.PutUint16(startBuffer[0:2], 0)
		// output <- startBuffer
		// outputBufSize.Add(int64(len(startBuffer)))
		bufferOriLen := 0
		bufferCmprLen := 0
		// ts := time.Now().UnixNano()
		// fmt.Println("第", c.testTimes, "个buffer压缩开始,时间为: ", ts)

		packageData := make([]byte, 58)
		binary.LittleEndian.PutUint16(packageData[0:2], 1)
		packageOriLen := 0
		packageCmprLen := 0
		all := float64(0)
		for {
			offAndLens := aggData.GetOffsetAndLen()

			// 1. 调用monitor，计算得到当前压缩比
			cmprIntro := c.monitor.GetAlphaRatio(offAndLens)
			if len(cmprIntro) == 0 {
				break
			}
			// 2. 针对计算得到的压缩比，对每列做压缩
			for key, cmprintros := range cmprIntro {
				for _, cmprintro := range cmprintros {
					// 2.1 根据列名和bytes，得到应该压缩的数据列
					// data := aggData.GetColumnData(cmprintro.Point.Column, cmprintro.ByteNum)
					if cmprintro.ByteNum == 0 {
						continue
					}
					data := aggData.GetColumnData(key, cmprintro.ByteNum)
					// 2.2 根据压缩算法名，得到压缩器,并更新offset
					compressor := c.allCmprs.GetCompressorByType(cmprintro.CmprType)
					// 2.3 压缩
					oriLen := len(data)
					packageOriLen += len(data)

					cmprLen1 := len(packageData)
					startTime := time.Now()
					// tempData := compressor.Compress(data[:oriLen/c.cmprThreads])
					packageData = append(packageData, compressor.Compress(data)...)
					endTime := time.Now()
					// tempData2 := make([]byte, len(tempData)*c.cmprThreads)
					// for i := 0; i < c.cmprThreads; i++ {
					// packageData = append(packageData, tempData...)
					// }
					// packageData = append(packageData, tempData2...)
					all += endTime.Sub(startTime).Seconds()
					cmprLen := len(packageData) - cmprLen1
					packageCmprLen += cmprLen

					// 2.5 更新monitor中的cache，同时更新offset
					c.monitor.UpdateCompressionInfo(key, byte(cmprintro.CmprType), oriLen, cmprLen, endTime.Sub(startTime).Seconds()/float64(c.cmprThreads), false)
				}
			}
			c.monitor.PrintCacheInfo()
		}
		if c.testTimes == 407 {
			c.monitor.PrintAverageMap()
		}
		fmt.Println("压缩时间: ", all)
		fmt.Println("传输时间: ", float64(len(packageData))/rate)
		fmt.Println("最大时间: ", max(all, float64(len(packageData))/rate))
		// fmt.Println("touseMLP", c.monitor.touseMLP)
		// if c.monitor.touseMLP == 1 {
		// 	c.monitor.UpdateCmprgainOfColumns(cmprIntro)
		// 	c.monitor.UpdateCmprbwOfColumns(cmprIntro)
		// 	c.monitor.touseMLP = 0
		// } else if c.monitor.touseMLP == 2 {
		// 	c.monitor.UpdateMLPgain(cmprIntro)
		// 	c.monitor.UpdateMLPbw(cmprIntro)
		// 	c.monitor.touseMLP = 0
		// }

		// fmt.Println("real compress bandwitdh", float64(packageCmprLen)/all)
		// fmt.Println("record network bandwidth", c.monitor.net_bandwitdh)
		binary.LittleEndian.PutUint64(packageData[2:10], uint64(packageOriLen))
		binary.LittleEndian.PutUint64(packageData[10:18], uint64(packageCmprLen))
		binary.LittleEndian.PutUint64(packageData[18:26], uint64(aggData.StartTs))
		binary.LittleEndian.PutUint64(packageData[26:34], uint64(aggData.TotalEvents))
		binary.LittleEndian.PutUint64(packageData[50:58], uint64(c.cmprThreads))
		bufferOriLen += packageOriLen
		bufferCmprLen += packageCmprLen
		// fmt.Println("compress package", c.testTimes)
		c.testTimes++
		outputBufSize.Add(int64(len(packageData)))
		output <- packageData
		cmprBufSize.Add(-int64(aggData.TotalLen))
		// }
		// ts = time.Now().UnixNano()
		// fmt.Println("第", c.testTimes, "个buffer压缩结束,时间为: ", ts)
		// ONLY TEST startTs, totalEvents, oriLen, cmprLen
		// endBuffer := make([]byte, 34)
		// binary.LittleEndian.PutUint16(endBuffer[0:2], 2)
		// binary.LittleEndian.PutUint64(endBuffer[2:10], uint64(aggData.StartTs))
		// binary.LittleEndian.PutUint64(endBuffer[10:18], uint64(aggData.TotalEvents))
		// binary.LittleEndian.PutUint64(endBuffer[18:26], uint64(bufferOriLen))
		// binary.LittleEndian.PutUint64(endBuffer[26:34], uint64(bufferCmprLen))
		// 传输
		// output <- endBuffer
		// outputBufSize.Add(int64(len(endBuffer)))
	}
}

func (c *Compressor) CompressIncrByCmprType(input <-chan *AggregateData, output chan<- []byte, cmprType byte, cmprBufSize *atomic.Int64, outputBufSize *atomic.Int64, rate float64) {
	for {
		aggData := <-input
		// 初始化本buffer内的各列数据起始offset及总长度len
		aggData.InitOffsetAndLen()
		bufferOriLen := 0
		bufferCmprLen := 0
		// ts := time.Now().UnixNano()
		// fmt.Println("第", c.testTimes, "个buffer压缩开始,时间为: ", ts)

		packageData := make([]byte, 58)
		binary.LittleEndian.PutUint16(packageData[0:2], 1)
		packageOriLen := 0
		packageCmprLen := 0
		// for {
		offAndLens := aggData.GetOffsetAndLen()

		// 1. 调用monitor，计算得到当前压缩比
		// cmprIntro := c.monitor.GetAlphaRatio(offAndLens)
		cmprIntro := make(map[string][]common.CompressionIntro)

		for _, offAndLen := range offAndLens {
			intro := common.CompressionIntro{
				CmprType: cmprType,
				ByteNum:  offAndLen.Len,
			}

			cmprIntro[offAndLen.Name] = append(cmprIntro[offAndLen.Name], intro)
		}
		// if len(cmprIntro) == 0 {
		// 	break
		// }
		// 2. 针对计算得到的压缩比，对每列做压缩
		all := float64(0)
		for key, cmprintros := range cmprIntro {
			for _, cmprintro := range cmprintros {
				// 2.1 根据列名和bytes，得到应该压缩的数据列
				// data := aggData.GetColumnData(cmprintro.Point.Column, cmprintro.ByteNum)
				if cmprintro.ByteNum == 0 {
					continue
				}
				data := aggData.GetColumnData(key, cmprintro.ByteNum)
				// 2.2 根据压缩算法名，得到压缩器,并更新offset
				compressor := c.allCmprs.GetCompressorByType(cmprintro.CmprType)
				// 2.3 压缩
				oriLen := len(data)
				packageOriLen += len(data)

				cmprLen1 := len(packageData)
				startTime := time.Now()

				packageData = append(packageData, compressor.Compress(data)...)
				endTime := time.Now()
				all += endTime.Sub(startTime).Seconds()
				cmprLen := len(packageData) - cmprLen1
				packageCmprLen += cmprLen

				// 2.5 更新monitor中的cache，同时更新offset
				c.monitor.UpdateCompressionInfo(key, byte(cmprintro.CmprType), oriLen, cmprLen, endTime.Sub(startTime).Seconds(), false)
			}
		}
		c.monitor.PrintCacheInfo()
		// }
		fmt.Println("压缩时间: ", all)
		fmt.Println("传输时间: ", float64(len(packageData))/rate)
		binary.LittleEndian.PutUint64(packageData[2:10], uint64(packageOriLen))
		binary.LittleEndian.PutUint64(packageData[10:18], uint64(packageCmprLen))
		binary.LittleEndian.PutUint64(packageData[18:26], uint64(aggData.StartTs))
		binary.LittleEndian.PutUint64(packageData[26:34], uint64(aggData.TotalEvents))
		binary.LittleEndian.PutUint64(packageData[50:58], uint64(c.cmprThreads))
		bufferOriLen += packageOriLen
		bufferCmprLen += packageCmprLen
		// fmt.Println("compress package", c.testTimes)
		c.testTimes++
		outputBufSize.Add(int64(len(packageData)))
		output <- packageData
		cmprBufSize.Add(-int64(aggData.TotalLen))
		// }
		// ts = time.Now().UnixNano()
		// fmt.Println("第", c.testTimes, "个buffer压缩结束,时间为: ", ts)
	}
}

// 负责从队列中获得一个AggData，将其分列压缩，查看压缩增益
func (c *Compressor) CompressByColumnAndPrintInfo(input <-chan *AggregateData) {
	for {
		aggData := <-input
		// 1.初始化本buffer内的各列数据起始offset及总长度len
		aggData.InitOffsetAndLen()
		for cmprType := common.ZSTD1; cmprType <= common.FLATE; cmprType++ {
			// if cmprType != common.ZSTD {
			// 	continue
			// }
			if cmprType == common.INVALID_END {
				continue
			}
			compressor := c.allCmprs.GetCompressorByType(cmprType)
			fmt.Println("Compress Method: ", common.GetCompressionTypeStr(cmprType))
			// 2. 打印非用户列的压缩信息
			common.CompressAndPrintInfo(reflect.ValueOf(aggData), "AggregateData", compressor)
			// 3. 打印用户列的压缩信息
			for key, vptr := range aggData.Writerows.Rows2 {
				value := *vptr
				// 遍历解引用后的二维切片
				for i, v := range value {
					originLen := len(v)
					if originLen == 0 {
						continue
					}
					// cmprData := compressor.Compress(v)
					// cmprLen := len(cmprData)
					fmt.Printf("Field: AggregateData.Writerows.%d.%d\n", key, i) //, oriLen: %v, cmprLen: %v, cmprRatio: %v  , originLen, cmprLen, float64(cmprLen)/float64(originLen)
					// bufferSize := 1 * 1024                                       //i := 100 * 1024; i <= 10*1024*1024; i += 100 * 1024
					bufferSizes := []int{1 * 1024, 5 * 1024, 10 * 1024, 50 * 1024, 100 * 1024, 500 * 1024, 1000 * 1024}
					for _, bufferSize := range bufferSizes {
						averageProcessor := common.NewAverageProcessor(bufferSize, 1)
						averageProcessor.AddData(v, compressor, nil, cmprType)
						averageProcessor.ComputeMedianAndVariance()
						fmt.Printf("\n")
						// if bufferSize < 10*1024 {
						// bufferSize += 4 * 1024
						// } else if bufferSize < 1*1024*1024 {
						// bufferSize += 100 * 1024
						// } else if bufferSize < 10*1024*1024 {
						// bufferSize += 1 * 1024 * 1024
						// } else {
						// break
						// }
					}
					// }
				}
			}
		}
	}
}

func (c *Compressor) CompressByFixBlockAndPrintInfo(input <-chan common.DataWithInfo) {
	for {
		dataWithInfo := <-input
		data := dataWithInfo.Data
		compressor := c.allCmprs.GetCompressorByType(common.SNAPPY)
		for i := 100; 1 < 1000; i += 100 {
			common.ComputeMedianAndVariance(data, i, compressor)
			fmt.Println()
		}
	}
}

// func (c *Compressor) RtcNumber(input <-chan common.DataWithInfo) {
// 	map[string][int]m
// 	for {
// 		dataWithInfo := <-input
// 		data := dataWithInfo.Data

// 	}
// }
