package rtc

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/wqshr12345/golib/compression/rtc/common"
	"github.com/wqshr12345/golib/compression/rtc/event"

	allcompressors "github.com/wqshr12345/golib/compression/rtc/all_compressors"
)

func NewRtcCompressor() *RtcCompressor {
	return &RtcCompressor{
		cmprs:    allcompressors.NewAllCompressors(),
		wrappers: make(chan event.EventWrapper, 1000),
		cmtype:   make([]byte, 15),
		//// perf:     common.R//tcPerfs,
	}
}

type RtcCompressor struct {
	cmprs    *allcompressors.AllCompressors
	wrappers chan event.EventWrapper
	cmtype   []byte
	// 存储当前的统计信息...
	//	eventData.NoUsed 1
	//	eventData.DbNameLen 2
	//	eventData.DbName 3
	//	eventData.TableInfo 4
	//
	// eventData.TableId 5
	//
	//	eventData.Reserved 6
	//	eventData.ExtraInfoLen 7
	//
	// eventData.ExtraInfo 8
	//
	//	eventData.ColumnNums 9
	//
	// eventData.IncludedColumns 10
	//
	//	eventData.NullColumns 11
	//
	// eventData.Rows 12
	// eventData.Checksum 13
	// eventData.All 14
	// // perf     *common.R//tcPerf
	// 我需要得到当前反序列化后的数据信息...
}

func (c *RtcCompressor) Reset() {
	c.cmprs.Reset()
}

// src代表一个或多个完整的event。这个语义应该由上层保证...
func (c *RtcCompressor) Compress(src []byte) []byte { //, testMeta *[]common.EventWrapperMeta
	start := 0
	total := len(src)
	totalEvents := 0
	totalDeserTime := int64(0)
	totalcolumnCompressTime := int64(0)
	totalFinalizeTime := int64(0)
	for start < total {
		totalEvents += 1
		// 1. 反序列化数据
		startTime := time.Now()
		// fmt.Println("start:", start, "total:", total, "totalEvents:", totalEvents)
		offset, wrapper, err := c.deserialize(src[start:])
		deserializeTime := time.Since(startTime).Nanoseconds()
		totalDeserTime += deserializeTime
		/// only used in test.
		// *testMeta = append(*testMeta, common.EventWrapperMeta{
		// 	Offset:    start,
		// 	EventType: wrapper.Event.Header.EventType,
		// })
		start += offset
		///

		// 2. 把数据聚在一起
		if err != nil {
			panic(err)
		}
		startTime = time.Now()
		c.columnCompress(wrapper)
		columnCompressTime := time.Since(startTime).Nanoseconds()
		totalcolumnCompressTime += columnCompressTime
	}
	// 3. 输出数据
	startTime := time.Now()
	var out []byte
	if common.Sync {
		out = c.finalizeSync(totalEvents, len(src))
	} else {
		out = c.finalize(totalEvents, len(src))
	}
	// out := c.finalize(totalEvents, len(src))
	finalizeTime := time.Since(startTime).Nanoseconds()
	totalFinalizeTime += finalizeTime
	return out
}

func (c *RtcCompressor) ComputeBetter() {
	// 根据当前每一列的数据长度，计算每一列应该用的压缩算法...
}

// / 反序列化相关函数
func (c *RtcCompressor) deserialize(src []byte) (offset int, wrapper event.EventWrapper, err error) {
	// 1. 构造event wrapper
	// compress body len.
	wrapper.BodyLen = src[:3]
	bodylen := int(uint32(wrapper.BodyLen[0])<<0 | uint32(wrapper.BodyLen[1])<<8 | uint32(wrapper.BodyLen[2])<<16)

	wrapper.SequenceNumber = src[3:4]
	wrapper.Flag = src[4:5]

	// offset = 3 + 1 + bodylen
	// return offset, wrapper, nil
	// TODO(wangqian)：这里基于的假设是——一个body中包含了一个完整的event，而不是两个body包含一个event...
	if src[4] != 0x00 {
		offset = 3 + 1 + bodylen
		return offset, wrapper, nil //errors.New("not a ok event(maybe handshake or err event")
	}
	wrapper.Event = c.deserializeEvent(src[5 : 5+bodylen-1])

	offset = 3 + 1 + bodylen

	return offset, wrapper, nil
}

// src中就是完整的一条event...
func (c *RtcCompressor) deserializeEvent(src []byte) (event event.Event) {
	// 1. 反序列化evnet header
	event.Header.Timestamp = src[:4]
	event.Header.EventType = src[4]
	event.Header.ServerId = src[5:9]
	event.Header.EventLen = src[9:13]
	event.Header.NextPos = src[13:17]
	// fmt.Println("event header next pos:", binary.LittleEndian.Uint32(event.Header.NextPos))
	event.Header.Flags = src[17:19]

	// 2. 反序列化event data
	event.Data = c.deserializeEventData(event.Header.EventType, src[19:])
	// event.Data = src[19:]
	return event
}

// src中就是一个完整的event data
func (c *RtcCompressor) deserializeEventData(eventType byte, src []byte) (eventData event.EventData) {
	switch eventType {
	case event.FORMAT_DESCRIPTION_EVENT: // 几乎不出现 11
		eventData = c.deserializeFormatDescriptionEventData(src)
		break
	case event.TRANSACTION_PAYLOAD_EVENT: // 偶尔出现 1622
		eventData = c.deserializeTransactionPayloadEventData(src)
		break
	case event.PREVIOUS_GTIDS_EVENT: // 几乎不出现 11
		eventData = c.deserializePreviousGTIDsEventData(src)
		break
	case event.ANONYMOUS_GTID_EVENT: // 偶尔出现 3250
		eventData = c.deserializeAnonymousGTIDEventData(src)
		break
	case event.ROTATE_EVENT: // 几乎不出现 21
		eventData = c.deserializeRotateEventData(src)
		break
	case event.QUERY_EVENT: // 偶尔出现 3250
		eventData = c.deserializeQueryEventData(src)
		break
	case event.TABLE_MAP_EVENT: // 经常出现 97258
		eventData = c.deserializeTableMapEventData(src)
		break
	case event.WRITE_ROWS_EVENTv0:
	case event.WRITE_ROWS_EVENTv1:
	case event.WRITE_ROWS_EVENTv2: // 经常出现 97258
		eventData = c.deserializeWriteRowsEventData(src)
		break
	case event.XID_EVENT: // 偶尔出现 3243
		eventData = c.deserializeXid(src)
		break
	default:
		panic("unknown event type." + string(eventType))
	}
	return eventData
}
func (c *RtcCompressor) deserializeRotateEventData(src []byte) (eventData event.RotateEventData) {
	eventData.Position = src[:8]
	eventData.NextName = src[8:]
	eventData.All = src
	c.cmtype[14] = 1
	return eventData
}

func (c *RtcCompressor) deserializeFormatDescriptionEventData(src []byte) (eventData event.FormatDescriptionEventData) {
	eventData.BinlogVersion = src[:2]
	eventData.ServerVersion = src[2:52]
	eventData.CreateTime = src[52:56]
	eventData.HeaderLen = src[56:57]
	eventData.All = src
	c.cmtype[14] = 1
	return eventData
}

func (c *RtcCompressor) deserializeStopEventData(src []byte) (eventData event.StopEventData) {
	eventData.All = src
	c.cmtype[14] = 1
	return eventData
}

func (c *RtcCompressor) deserializeTransactionPayloadEventData(src []byte) (eventData event.TransactionPayloadEventData) {
	eventData.All = src
	c.cmtype[14] = 1
	return eventData
}

func (c *RtcCompressor) deserializeAnonymousGTIDEventData(src []byte) (eventData event.AnonymousGTIDEventData) {
	eventData.GTID = src
	eventData.All = src
	c.cmtype[14] = 1
	return eventData
}

func (c *RtcCompressor) deserializePreviousGTIDsEventData(src []byte) (eventData event.PreviousGTIDsEventData) {
	eventData.Flags = src[:1]
	eventData.GTID = src[1:]
	eventData.All = src
	c.cmtype[14] = 1
	return eventData
}

func (c *RtcCompressor) deserializeQueryEventData(src []byte) (eventData event.QueryEventData) {
	eventData.ThreadId = src[:4]
	eventData.ExecTime = src[4:8]
	eventData.DbNameLen = src[8:9]
	eventData.ErrorCode = src[9:11]
	eventData.StatusVarLen = src[11:13]

	statusVarLen := int(eventData.StatusVarLen[0]) | int(eventData.StatusVarLen[1])<<8 // little end

	eventData.StatusVar = src[13 : 13+statusVarLen]
	eventData.DbName = src[13+statusVarLen : 13+statusVarLen+int(eventData.DbNameLen[0])]
	eventData.Sql = src[13+statusVarLen+int(eventData.DbNameLen[0]):]
	eventData.All = src
	c.cmtype[14] = 1
	return eventData
}

func (c *RtcCompressor) deserializeTableMapEventData(src []byte) (eventData event.TableMapEventData) {
	eventData.TableId = src[:6]
	eventData.NoUsed = src[6:8]
	eventData.DbNameLen = src[8:9]
	eventData.DbName = src[9 : 9+int(eventData.DbNameLen[0])]
	eventData.TableInfo = src[9+int(eventData.DbNameLen[0]):]
	// eventData.All = src[6:]
	c.cmtype[5] = 1
	c.cmtype[1] = 1
	c.cmtype[2] = 1
	c.cmtype[3] = 1
	c.cmtype[4] = 1
	tableId := uint64(eventData.TableId[0]) | uint64(eventData.TableId[1])<<8 | uint64(eventData.TableId[2])<<16 | uint64(eventData.TableId[3])<<24 | uint64(eventData.TableId[4])<<32 | uint64(eventData.TableId[5])<<40
	if _, ok := c.cmprs.TableInforMap[tableId]; !ok {
		// 在映射中不存在这个键，所以创建一个新的 TableInformation 实例并插入它
		c.cmprs.TableInforMap[tableId] = common.NewTableInfo(eventData.TableInfo)
	}
	return eventData
}

func (c *RtcCompressor) deserializeWriteRowsEventData(src []byte) (eventData event.WriteRowsEventData) {
	eventData.TableId = src[:6]
	c.cmtype[5] = 1
	eventData.Reserved = src[6:8]
	c.cmtype[6] = 1
	eventData.ExtraInfoLen = src[8:10]
	c.cmtype[7] = 1
	extraInfoLen := int(eventData.ExtraInfoLen[0]) | int(eventData.ExtraInfoLen[1])<<8 // little end

	eventData.ExtraInfo = src[10 : 10+extraInfoLen-2]
	c.cmtype[8] = 1
	eventData.ColumnNums = src[10+extraInfoLen-2 : 10+extraInfoLen-1]
	c.cmtype[9] = 1
	columnNums := int(eventData.ColumnNums[0])
	// TODO(wangqian): 只在debug模式使用
	if columnNums >= 251 {
		panic("number of columns is too large.")
	}
	//
	includedColumnsLen := (columnNums + 7) / 8

	eventData.IncludedColumns = src[10+int(eventData.ExtraInfoLen[0])-1 : 10+int(eventData.ExtraInfoLen[0])+includedColumnsLen-1]
	c.cmtype[10] = 1
	eventData.NullColumns = src[10+int(eventData.ExtraInfoLen[0])+includedColumnsLen-1 : 10+int(eventData.ExtraInfoLen[0])+includedColumnsLen+includedColumnsLen-1]
	c.cmtype[11] = 1
	// eventData.Rows = make([][]byte, columnNums)
	eventData.Rows = src[10+int(eventData.ExtraInfoLen[0])+includedColumnsLen+includedColumnsLen-1:]
	c.cmtype[12] = 1
	tableId := uint64(eventData.TableId[0]) | uint64(eventData.TableId[1])<<8 | uint64(eventData.TableId[2])<<16 | uint64(eventData.TableId[3])<<24 | uint64(eventData.TableId[4])<<32 | uint64(eventData.TableId[5])<<40

	it := c.cmprs.TableInforMap[tableId]
	n, _ := eventData.DecodeImage(src[10+int(eventData.ExtraInfoLen[0])+includedColumnsLen+includedColumnsLen-1:], it)
	eventData.Checksum = src[10+int(eventData.ExtraInfoLen[0])+includedColumnsLen+includedColumnsLen-1+n:]
	c.cmtype[13] = 1
	if len(eventData.Checksum) != 4 {
		panic("checksum is not 4.")
	}
	// eventData.All = src[10+int(eventData.ExtraInfoLen[0])+includedColumnsLen+includedColumnsLen-1:]
	return eventData
}

func (c *RtcCompressor) deserializeXid(src []byte) (eventData event.XidEventData) {
	eventData.Xid = src[:8]
	eventData.All = src
	c.cmtype[14] = 1
	return eventData
}

// 将wrapper中的数据按组区分 进行压缩/准备压缩
// 暂时用最简单的
func (c *RtcCompressor) columnCompress(wrapper event.EventWrapper) {
	// 1. 压缩event wrapper header
	c.cmprs.EventWrapperCmpr.BodyLenCmpr.Compress(wrapper.BodyLen)
	c.cmprs.EventWrapperCmpr.SequenceNumberCmpr.Compress(wrapper.SequenceNumber)
	c.cmprs.EventWrapperCmpr.FlagCmpr.Compress(wrapper.Flag)

	// 2. 压缩event header
	c.cmprs.EventHeaderCmpr.TimestampCmpr.Compress(wrapper.Event.Header.Timestamp)
	c.cmprs.EventHeaderCmpr.EventTypeCmpr.Compress([]byte{wrapper.Event.Header.EventType})
	c.cmprs.EventHeaderCmpr.ServerIdCmpr.Compress(wrapper.Event.Header.ServerId)
	c.cmprs.EventHeaderCmpr.EventLenCmpr.Compress(wrapper.Event.Header.EventLen)
	c.cmprs.EventHeaderCmpr.NextPosCmpr.Compress(wrapper.Event.Header.NextPos)
	c.cmprs.EventHeaderCmpr.FlagsCmpr.Compress(wrapper.Event.Header.Flags)

	// 3. 压缩event data
	switch wrapper.Event.Header.EventType {
	case event.ROTATE_EVENT:
		c.cmprs.RotateCmpr.AllCmpr.Compress(wrapper.Event.Data.(event.RotateEventData).All)
		break
	case event.TRANSACTION_PAYLOAD_EVENT:
		c.cmprs.TransactionPayloadCmpr.AllCmpr.Compress(wrapper.Event.Data.(event.TransactionPayloadEventData).All)
		break
	case event.PREVIOUS_GTIDS_EVENT:
		// c.cmprs.PreviousGTIDsCmpr.FlagsCmpr.Compress(wrapper.Event.Data.(event.PreviousGTIDsEventData).Flags)
		// c.cmprs.PreviousGTIDsCmpr.GTIDCmpr.Compress(wrapper.Event.Data.(event.PreviousGTIDsEventData).GTID)
		c.cmprs.PreviousGTIDsCmpr.AllCmpr.Compress(wrapper.Event.Data.(event.PreviousGTIDsEventData).All)
		break
	case event.ANONYMOUS_GTID_EVENT:
		// c.cmprs.AnonymousGTIDCmpr.GTIDCmpr.Compress(wrapper.Event.Data.(event.AnonymousGTIDEventData).GTID)
		c.cmprs.AnonymousGTIDCmpr.AllCmpr.Compress(wrapper.Event.Data.(event.AnonymousGTIDEventData).All)
		break
	case event.FORMAT_DESCRIPTION_EVENT:
		// c.cmprs.FormatDescriptionCmpr.BinlogVersionCmpr.Compress(wrapper.Event.Data.(event.FormatDescriptionEventData).BinlogVersion)
		// c.cmprs.FormatDescriptionCmpr.ServerVersionCmpr.Compress(wrapper.Event.Data.(event.FormatDescriptionEventData).ServerVersion)
		// c.cmprs.FormatDescriptionCmpr.CreateTimeCmpr.Compress(wrapper.Event.Data.(event.FormatDescriptionEventData).CreateTime)
		// c.cmprs.FormatDescriptionCmpr.HeaderLenCmpr.Compress(wrapper.Event.Data.(event.FormatDescriptionEventData).HeaderLen)
		c.cmprs.FormatDescriptionCmpr.AllCmpr.Compress(wrapper.Event.Data.(event.FormatDescriptionEventData).All)
		break
	case event.QUERY_EVENT:
		// if c.cmprs.QueryEventCmpr.ThreadIdCmpr == nil {
		// 	panic("thread id cmpr is nil.")
		// }
		// c.cmprs.QueryEventCmpr.ThreadIdCmpr.Compress(wrapper.Event.Data.(event.QueryEventData).ThreadId)
		// c.cmprs.QueryEventCmpr.ExecTimeCmpr.Compress(wrapper.Event.Data.(event.QueryEventData).ExecTime)
		// c.cmprs.QueryEventCmpr.DbNameLenCmpr.Compress(wrapper.Event.Data.(event.QueryEventData).DbNameLen)
		// c.cmprs.QueryEventCmpr.ErrorCodeCmpr.Compress(wrapper.Event.Data.(event.QueryEventData).ErrorCode)
		// c.cmprs.QueryEventCmpr.StatusVarLenCmpr.Compress(wrapper.Event.Data.(event.QueryEventData).StatusVarLen)
		// c.cmprs.QueryEventCmpr.StatusVarCmpr.Compress(wrapper.Event.Data.(event.QueryEventData).StatusVar)
		// c.cmprs.QueryEventCmpr.DbNameCmpr.Compress(wrapper.Event.Data.(event.QueryEventData).DbName)
		// c.cmprs.QueryEventCmpr.SqlCmpr.Compress(wrapper.Event.Data.(event.QueryEventData).Sql)
		c.cmprs.QueryEventCmpr.AllCmpr.Compress(wrapper.Event.Data.(event.QueryEventData).All)
		// break
	case event.TABLE_MAP_EVENT:
		c.cmprs.TableMapCmpr.TableIdCmpr.Compress(wrapper.Event.Data.(event.TableMapEventData).TableId)
		c.cmprs.TableMapCmpr.NoUsedCmpr.Compress(wrapper.Event.Data.(event.TableMapEventData).NoUsed)
		c.cmprs.TableMapCmpr.DbNameLenCmpr.Compress(wrapper.Event.Data.(event.TableMapEventData).DbNameLen)
		c.cmprs.TableMapCmpr.DbNameCmpr.Compress(wrapper.Event.Data.(event.TableMapEventData).DbName)
		c.cmprs.TableMapCmpr.TableInfoCmpr.Compress(wrapper.Event.Data.(event.TableMapEventData).TableInfo)
		// c.cmprs.TableMapCmpr.AllCmpr.Compress(wrapper.Event.Data.(event.TableMapEventData).All)
		break
	case event.WRITE_ROWS_EVENTv0:
	case event.WRITE_ROWS_EVENTv1:
	case event.WRITE_ROWS_EVENTv2:
		c.cmprs.WriteRowsCmpr.TableIdCmpr.Compress(wrapper.Event.Data.(event.WriteRowsEventData).TableId)
		c.cmprs.WriteRowsCmpr.ReservedCmpr.Compress(wrapper.Event.Data.(event.WriteRowsEventData).Reserved)
		c.cmprs.WriteRowsCmpr.ExtraInfoLenCmpr.Compress(wrapper.Event.Data.(event.WriteRowsEventData).ExtraInfoLen)
		c.cmprs.WriteRowsCmpr.ExtraInfoCmpr.Compress(wrapper.Event.Data.(event.WriteRowsEventData).ExtraInfo)
		c.cmprs.WriteRowsCmpr.ColumnNumsCmpr.Compress(wrapper.Event.Data.(event.WriteRowsEventData).ColumnNums)
		c.cmprs.WriteRowsCmpr.IncludedColumnsCmpr.Compress(wrapper.Event.Data.(event.WriteRowsEventData).IncludedColumns)
		c.cmprs.WriteRowsCmpr.NullColumnsCmpr.Compress(wrapper.Event.Data.(event.WriteRowsEventData).NullColumns)
		c.cmprs.WriteRowsCmpr.RowsCmpr.Compress(wrapper.Event.Data.(event.WriteRowsEventData).Rows)
		tableId := uint64(wrapper.Event.Data.(event.WriteRowsEventData).TableId[0]) | uint64(wrapper.Event.Data.(event.WriteRowsEventData).TableId[1])<<8 | uint64(wrapper.Event.Data.(event.WriteRowsEventData).TableId[2])<<16 | uint64(wrapper.Event.Data.(event.WriteRowsEventData).TableId[3])<<24 | uint64(wrapper.Event.Data.(event.WriteRowsEventData).TableId[4])<<32 | uint64(wrapper.Event.Data.(event.WriteRowsEventData).TableId[5])<<40
		ti := c.cmprs.TableInforMap[tableId]
		columnTypes := ti.ColumnTypes
		columnNums := int(wrapper.Event.Data.(event.WriteRowsEventData).ColumnNums[0])
		includedColumns := wrapper.Event.Data.(event.WriteRowsEventData).IncludedColumns
		c.cmprs.WriteRowsCmpr.Rows2Cmpr.Compress(tableId, columnNums, includedColumns, columnTypes, wrapper.Event.Data.(event.WriteRowsEventData).Rows2)
		// it := c.cmprs.WriteRowsCmpr.Rows2Cmpr[tableId]
		// for i := 0; i < len(wrapper.Event.Data.(event.WriteRowsEventData).Rows2); i++ {
		// 	it[i].Compress(wrapper.Event.Data.(event.WriteRowsEventData).Rows2[i])
		// }
		c.cmprs.WriteRowsCmpr.CheckSumCmpr.Compress(wrapper.Event.Data.(event.WriteRowsEventData).Checksum)
		// c.cmprs.WriteRowsCmpr.AllCmpr.Compress(wrapper.Event.Data.(event.WriteRowsEventData).All)
		break
	case event.XID_EVENT:
		c.cmprs.XidCmpr.AllCmpr.Compress(wrapper.Event.Data.(event.XidEventData).All)
		// fmt.Println("xid:", binary.LittleEndian.Uint64(wrapper.Event.Data.(event.XidEventData).Xid))
		// c.cmprs.XidCmpr.XidCmpr.Compress(wrapper.Event.Data.(event.XidEventData).Xid)
		break
	default:
		panic("unknown event type." + string(wrapper.Event.Header.EventType))
	}
}

// 根据
func (c *RtcCompressor) predictCompress() {

}

// 总体格式
// total_events |original_len | wrapper | header | query | table_map | write_rows | xid
// TODO(wangqian): 未来使用更优雅的方法，比如迭代器iterator...
func (c *RtcCompressor) finalize(totalEvents int, originalLen int) (out []byte) {
	startTime := time.Now()

	common.Wait.Add(24) // 必须在这里add，不然主线程可能没法等任何一个协程开始运行就Wait()成功了...
	// 1. 首先把非依赖函数通过异步执行完毕
	go c.cmprs.WriteRowsCmpr.Rows2Cmpr.FinalizeNoSync()

	go c.cmprs.EventWrapperCmpr.BodyLenCmpr.FinalizeNoSync()
	go c.cmprs.EventWrapperCmpr.SequenceNumberCmpr.FinalizeNoSync()
	go c.cmprs.EventWrapperCmpr.FlagCmpr.FinalizeNoSync()

	// go c.cmprs.EventHeaderCmpr.TimestampCmpr.FinalizeNoSync()
	go c.cmprs.EventHeaderCmpr.EventTypeCmpr.FinalizeNoSync()
	// go c.cmprs.EventHeaderCmpr.ServerIdCmpr.FinalizeNoSync()
	go c.cmprs.EventHeaderCmpr.EventLenCmpr.FinalizeNoSync()
	// go c.cmprs.EventHeaderCmpr.NextPosCmpr.FinalizeNoSync()
	go c.cmprs.EventHeaderCmpr.FlagsCmpr.FinalizeNoSync()

	go c.cmprs.RotateCmpr.AllCmpr.FinalizeNoSync()
	go c.cmprs.TransactionPayloadCmpr.AllCmpr.FinalizeNoSync()
	go c.cmprs.PreviousGTIDsCmpr.AllCmpr.FinalizeNoSync()
	go c.cmprs.AnonymousGTIDCmpr.AllCmpr.FinalizeNoSync()
	go c.cmprs.FormatDescriptionCmpr.AllCmpr.FinalizeNoSync()
	go c.cmprs.QueryEventCmpr.AllCmpr.FinalizeNoSync()

	// go c.cmprs.TableMapCmpr.TableIdCmpr.FinalizeNoSync()
	go c.cmprs.TableMapCmpr.NoUsedCmpr.FinalizeNoSync()
	go c.cmprs.TableMapCmpr.DbNameLenCmpr.FinalizeNoSync()
	go c.cmprs.TableMapCmpr.DbNameCmpr.FinalizeNoSync()
	go c.cmprs.TableMapCmpr.TableInfoCmpr.FinalizeNoSync()

	// go c.cmprs.WriteRowsCmpr.TableIdCmpr.FinalizeNoSync()
	go c.cmprs.WriteRowsCmpr.ReservedCmpr.FinalizeNoSync()
	go c.cmprs.WriteRowsCmpr.ExtraInfoLenCmpr.FinalizeNoSync()
	go c.cmprs.WriteRowsCmpr.ExtraInfoCmpr.FinalizeNoSync()
	go c.cmprs.WriteRowsCmpr.ColumnNumsCmpr.FinalizeNoSync()
	go c.cmprs.WriteRowsCmpr.IncludedColumnsCmpr.FinalizeNoSync()
	go c.cmprs.WriteRowsCmpr.NullColumnsCmpr.FinalizeNoSync()
	// go c.cmprs.WriteRowsCmpr.Row2sCmpr.FinalizeNoSync()
	// go c.cmprs.WriteRowsCmpr.CheckSumCmpr.FinalizeNoSync()

	go c.cmprs.XidCmpr.AllCmpr.FinalizeNoSync()

	common.Wait.Wait() // 等待所有异步函数执行完毕
	fmt.Println("finalize nosync time total", time.Since(startTime).Nanoseconds())
	startTime = time.Now()
	// total用来初始化out的大小
	out = make([]byte, originalLen) // TODOIMP(默认压缩后数据小于压缩前数据...)
	offset := 0
	// 1. 写入total_events
	binary.LittleEndian.PutUint32(out[offset:offset+4], uint32(totalEvents))
	offset += 4

	// 1.5 写入original_len
	binary.LittleEndian.PutUint32(out[offset:offset+4], uint32(originalLen))
	offset += 4

	// 2. finalize wrapper.

	// 2.1 finalize wrapper's bodylen.
	// fmt.Print("压缩列: BodyLen ")
	offset = c.cmprs.EventWrapperCmpr.BodyLenCmpr.FinalizeSync(&out, offset)
	// 2.2 finalize wrapper's sequence number.
	// fmt.Print("压缩列: SequenceNumber ")
	offset = c.cmprs.EventWrapperCmpr.SequenceNumberCmpr.FinalizeSync(&out, offset)
	// 2.3 finalize wrapper's flag.
	// fmt.Print("压缩列: Flag ")
	offset = c.cmprs.EventWrapperCmpr.FlagCmpr.FinalizeSync(&out, offset)

	// 3. finalize header.
	// 3.1 finalize header's timestamp.
	// fmt.Print("压缩列: Timestamp ")
	offset = c.cmprs.EventHeaderCmpr.TimestampCmpr.Finalize(&out, offset)
	// 3.2 finalize header's event type.
	// fmt.Print("压缩列: EventType ")
	offset = c.cmprs.EventHeaderCmpr.EventTypeCmpr.FinalizeSync(&out, offset)
	// 3.3 finalize header's server id.
	// fmt.Print("压缩列: ServerId ")
	offset = c.cmprs.EventHeaderCmpr.ServerIdCmpr.Finalize(&out, offset)
	// 3.4 finalize header's event len.
	// fmt.Print("压缩列: EventLen ")
	offset = c.cmprs.EventHeaderCmpr.EventLenCmpr.FinalizeSync(&out, offset)
	// 3.5 finalize header's next pos.
	// fmt.Print("压缩列: NextPos ")
	offset = c.cmprs.EventHeaderCmpr.NextPosCmpr.Finalize(&out, offset)
	// 3.6 finalize header's flags.
	// fmt.Print("压缩列: Flags ")
	offset = c.cmprs.EventHeaderCmpr.FlagsCmpr.FinalizeSync(&out, offset)

	// 4. finalize rotate event.
	// 4.0 finalize rotate type.
	out[offset] = event.ROTATE_EVENT
	offset += 1
	offset += 4           // 预留4个字节用于存储rotate event的长度
	startOffset := offset // 记录rotate event的起始位置，用于最后计算rotate event的长度
	// 4.1 finalize rotate event's all.
	// fmt.Print("压缩Event: Rotate 压缩列: RotateAll ")
	offset = c.cmprs.RotateCmpr.AllCmpr.FinalizeSync(&out, offset)
	binary.LittleEndian.PutUint32(out[startOffset-4:startOffset], uint32(offset-startOffset))

	// 5. finalize transaction payload event.
	out[offset] = event.TRANSACTION_PAYLOAD_EVENT
	offset += 1
	offset += 4          // 预留4个字节用于存储transaction payload event的长度
	startOffset = offset // 记录transaction payload event的起始位置，用于最后计算transaction payload event的长度
	// fmt.Print("压缩Event: TransactionPayload 压缩列: TransactionPayloadAll ")
	offset = c.cmprs.TransactionPayloadCmpr.AllCmpr.FinalizeSync(&out, offset)
	binary.LittleEndian.PutUint32(out[startOffset-4:startOffset], uint32(offset-startOffset))

	// 6. finalize previous gtid event.
	out[offset] = event.PREVIOUS_GTIDS_EVENT
	offset += 1
	offset += 4          // 预留4个字节用于存储previous gtid event的长度
	startOffset = offset // 记录previous gtid event的起始位置，用于最后计算previous gtid event的长度
	// offset = c.cmprs.PreviousGTIDsCmpr.FlagsCmpr.Finalize(&out, offset)
	// offset = c.cmprs.PreviousGTIDsCmpr.GTIDCmpr.Finalize(&out, offset)
	// fmt.Print("压缩Event: PreviousGTIDs 压缩列: PreviousGTIDsAll ")
	offset = c.cmprs.PreviousGTIDsCmpr.AllCmpr.FinalizeSync(&out, offset)
	binary.LittleEndian.PutUint32(out[startOffset-4:startOffset], uint32(offset-startOffset))

	// 7. finalize anonymous gtid event.
	out[offset] = event.ANONYMOUS_GTID_EVENT
	offset += 1
	offset += 4          // 预留4个字节用于存储anonymous gtid event的长度
	startOffset = offset // 记录anonymous gtid event的起始位置，用于最后计算anonymous gtid event的长度
	// fmt.Print("压缩Event: AnonymousGTID 压缩列: AnonymousGTIDAll ")
	offset = c.cmprs.AnonymousGTIDCmpr.AllCmpr.FinalizeSync(&out, offset)
	binary.LittleEndian.PutUint32(out[startOffset-4:startOffset], uint32(offset-startOffset))

	// 8. finalize format description event.
	out[offset] = event.FORMAT_DESCRIPTION_EVENT
	offset += 1
	offset += 4          // 预留4个字节用于存储format description event的长度
	startOffset = offset // 记录format description event的起始位置，用于最后计算format description event的长度
	// fmt.Print("压缩Event: FormatDescription 压缩列: FormatDescriptionAll ")
	offset = c.cmprs.FormatDescriptionCmpr.AllCmpr.FinalizeSync(&out, offset)
	binary.LittleEndian.PutUint32(out[startOffset-4:startOffset], uint32(offset-startOffset))
	// 4. finalize query event.
	// 4.0 finalize query type.
	out[offset] = event.QUERY_EVENT
	offset += 1
	offset += 4          // 预留4个字节用于存储query event的长度
	startOffset = offset // 记录query event的起始位置，用于最后计算query event的长度
	// fmt.Print("压缩Event: Query 压缩列: QueryAll ")
	offset = c.cmprs.QueryEventCmpr.AllCmpr.FinalizeSync(&out, offset)
	// // 4.1 finalize query event's thread id.
	// offset = c.cmprs.QueryEventCmpr.ThreadIdCmpr.Finalize(&out, offset)
	// // 4.2 finalize query event's exec time.
	// offset = c.cmprs.QueryEventCmpr.ExecTimeCmpr.Finalize(&out, offset)
	// // 4.3 finalize query event's db name len.
	// offset = c.cmprs.QueryEventCmpr.DbNameLenCmpr.Finalize(&out, offset)
	// // 4.4 finalize query event's error code.
	// offset = c.cmprs.QueryEventCmpr.ErrorCodeCmpr.Finalize(&out, offset)
	// // 4.5 finalize query event's status var len.
	// offset = c.cmprs.QueryEventCmpr.StatusVarLenCmpr.Finalize(&out, offset)
	// // 4.6 finalize query event's status var.
	// offset = c.cmprs.QueryEventCmpr.StatusVarCmpr.Finalize(&out, offset)
	// // 4.7 finalize query event's db name.
	// offset = c.cmprs.QueryEventCmpr.DbNameCmpr.Finalize(&out, offset)
	// // 4.8 finalize query event's sql.
	// offset = c.cmprs.QueryEventCmpr.SqlCmpr.Finalize(&out, offset)
	// 4.9 finalize query event's len.
	binary.LittleEndian.PutUint32(out[startOffset-4:startOffset], uint32(offset-startOffset))

	// 5. finalize table map event.
	// 5.0 finalize table map type.
	out[offset] = event.TABLE_MAP_EVENT
	offset += 1
	offset += 4          // 预留4个字节用于存储table map event的长度
	startOffset = offset // 记录table map event的起始位置，用于最后计算table map event的长度
	// // 5.1 finalize table map event's table id.
	// fmt.Print("压缩Event: TableMap 压缩列: TableMapAll ")
	offset = c.cmprs.TableMapCmpr.TableIdCmpr.Finalize(&out, offset)
	// fmt.Print("压缩Event: TableMap 压缩列: TableMapAll ")
	// offset = c.cmprs.TableMapCmpr.AllCmpr.Finalize(&out, offset)
	// 5.2 finalize table map event's no used.
	// fmt.Print("压缩Event: TableMap 压缩列: Noused ")
	offset = c.cmprs.TableMapCmpr.NoUsedCmpr.FinalizeSync(&out, offset)
	// 5.3 finalize table map event's db name len.
	// fmt.Print("压缩Event: TableMap 压缩列: DbNameLen ")
	offset = c.cmprs.TableMapCmpr.DbNameLenCmpr.FinalizeSync(&out, offset)
	// 5.4 finalize table map event's db name.
	// fmt.Print("压缩Event: TableMap 压缩列: DbName ")
	offset = c.cmprs.TableMapCmpr.DbNameCmpr.FinalizeSync(&out, offset)
	// 5.5 finalize table map event's table info.
	// fmt.Print("压缩Event: TableMap 压缩列: TableInfo ")
	offset = c.cmprs.TableMapCmpr.TableInfoCmpr.FinalizeSync(&out, offset)
	// 5.6 finalize table map event's len.
	binary.LittleEndian.PutUint32(out[startOffset-4:startOffset], uint32(offset-startOffset))
	// 6. finalize write rows event.
	// 6.0 finalize write rows type.
	out[offset] = event.WRITE_ROWS_EVENTv2
	offset += 1
	offset += 4          // 预留4个字节用于存储write rows event的长度
	startOffset = offset // 记录write rows event的起始位置，用于最后计算write rows event的长度
	// // 6.1 finalize write rows event's table id.
	// fmt.Print("压缩Event: WriteRows 压缩列: TableId ")
	offset = c.cmprs.WriteRowsCmpr.TableIdCmpr.Finalize(&out, offset)

	// 6.2 finalize write rows event's reserved.
	// fmt.Print("压缩Event: WriteRows 压缩列: Reserved ")
	offset = c.cmprs.WriteRowsCmpr.ReservedCmpr.FinalizeSync(&out, offset)
	// // 6.3 finalize write rows event's extra info len.
	// fmt.Print("压缩Event: WriteRows 压缩列: ExtraInfoLen ")
	offset = c.cmprs.WriteRowsCmpr.ExtraInfoLenCmpr.FinalizeSync(&out, offset)
	// // 6.4 finalize write rows event's extra info.

	// TODOIMP(wangqian): extrainfo如果是空的，不压缩也行？

	// fmt.Print("压缩Event: WriteRows 压缩列: ExtraInfo ")
	// offset = c.cmprs.WriteRowsCmpr.ExtraInfoCmpr.Finalize(&out, offset)
	// // 6.5 finalize write rows event's column nums.

	// TODO(wangqian):这块单独拿出来的元数据就远大于数据了...
	// fmt.Print("压缩Event: WriteRows 压缩列: ColumnNums ")
	offset = c.cmprs.WriteRowsCmpr.ColumnNumsCmpr.FinalizeSync(&out, offset)
	// // 6.6 finalize write rows event's included columns.
	// fmt.Print("压缩Event: WriteRows 压缩列: IncludedColumns ")
	offset = c.cmprs.WriteRowsCmpr.IncludedColumnsCmpr.FinalizeSync(&out, offset)
	// // 6.7 finalize write rows event's null columns.
	// fmt.Print("压缩Event: WriteRows 压缩列: NullColumns ")
	offset = c.cmprs.WriteRowsCmpr.NullColumnsCmpr.FinalizeSync(&out, offset)
	// 6.8 finalize write rows event's rows.
	// TODOIMP(思考一下 这里的rows应该怎么处理)
	// fmt.Print("压缩Event: WriteRows 压缩列: Rows ")
	// offset = c.cmprs.WriteRowsCmpr.RowsCmpr.Finalize(&out, offset)

	// fmt.Print("压缩Event: WriteRows 压缩列: Rows2 ")
	offset = c.cmprs.WriteRowsCmpr.Rows2Cmpr.FinalizeSync(&out, offset)

	// fmt.Print("压缩Event: WriteRows 压缩列: Checksum ")
	offset = c.cmprs.WriteRowsCmpr.CheckSumCmpr.Finalize(&out, offset)
	// fmt.Print("压缩Event: WriteRows 压缩列: WriteRowsAll ")
	// offset = c.cmprs.WriteRowsCmpr.AllCmpr.Finalize(&out, offset)
	// 6.9 finalize write rows event's len.
	binary.LittleEndian.PutUint32(out[startOffset-4:startOffset], uint32(offset-startOffset))

	// 7. finalize xid event.
	// 7.0 finalize xid type.
	out[offset] = event.XID_EVENT
	offset += 1
	offset += 4          // 预留4个字节用于存储xid event的长度
	startOffset = offset // 记录xid event的起始位置，用于最后计算xid event的长度
	// fmt.Print("压缩Event: Xid 压缩列: XidAll ")
	offset = c.cmprs.XidCmpr.AllCmpr.FinalizeSync(&out, offset)
	// // 7.1 finalize xid event's xid.
	// offset = c.cmprs.XidCmpr.XidCmpr.Finalize(&out, offset)
	// 7.2 finalize xid event's len.
	binary.LittleEndian.PutUint32(out[startOffset-4:startOffset], uint32(offset-startOffset))

	fmt.Println("finalize sync time total", time.Since(startTime).Nanoseconds())
	return out[:offset]
}
func (c *RtcCompressor) finalizeSync(totalEvents int, originalLen int) (out []byte) {

	// total用来初始化out的大小
	out = make([]byte, originalLen) // TODOIMP(默认压缩后数据小于压缩前数据...)
	offset := 0
	// 1. 写入total_events
	binary.LittleEndian.PutUint32(out[offset:offset+4], uint32(totalEvents))
	offset += 4

	// 1.5 写入original_len
	binary.LittleEndian.PutUint32(out[offset:offset+4], uint32(originalLen))
	offset += 4

	// 2. finalize wrapper.

	// 2.1 finalize wrapper's bodylen.
	// fmt.Print("压缩列: BodyLen ")
	offset = c.cmprs.EventWrapperCmpr.BodyLenCmpr.Finalize(&out, offset)
	// 2.2 finalize wrapper's sequence number.
	// fmt.Print("压缩列: SequenceNumber ")
	offset = c.cmprs.EventWrapperCmpr.SequenceNumberCmpr.Finalize(&out, offset)
	// 2.3 finalize wrapper's flag.
	// fmt.Print("压缩列: Flag ")
	offset = c.cmprs.EventWrapperCmpr.FlagCmpr.Finalize(&out, offset)

	// 3. finalize header.
	// 3.1 finalize header's timestamp.
	// fmt.Print("压缩列: Timestamp ")
	offset = c.cmprs.EventHeaderCmpr.TimestampCmpr.Finalize(&out, offset)
	// 3.2 finalize header's event type.
	// fmt.Print("压缩列: EventType ")
	offset = c.cmprs.EventHeaderCmpr.EventTypeCmpr.Finalize(&out, offset)
	// 3.3 finalize header's server id.
	// fmt.Print("压缩列: ServerId ")
	offset = c.cmprs.EventHeaderCmpr.ServerIdCmpr.Finalize(&out, offset)
	// 3.4 finalize header's event len.
	// fmt.Print("压缩列: EventLen ")
	offset = c.cmprs.EventHeaderCmpr.EventLenCmpr.Finalize(&out, offset)
	// 3.5 finalize header's next pos.
	// fmt.Print("压缩列: NextPos ")
	offset = c.cmprs.EventHeaderCmpr.NextPosCmpr.Finalize(&out, offset)
	// 3.6 finalize header's flags.
	// fmt.Print("压缩列: Flags ")
	offset = c.cmprs.EventHeaderCmpr.FlagsCmpr.Finalize(&out, offset)

	// 4. finalize rotate event.
	// 4.0 finalize rotate type.
	out[offset] = event.ROTATE_EVENT
	offset += 1
	offset += 4           // 预留4个字节用于存储rotate event的长度
	startOffset := offset // 记录rotate event的起始位置，用于最后计算rotate event的长度
	// 4.1 finalize rotate event's all.
	// fmt.Print("压缩Event: Rotate 压缩列: RotateAll ")
	offset = c.cmprs.RotateCmpr.AllCmpr.Finalize(&out, offset)
	binary.LittleEndian.PutUint32(out[startOffset-4:startOffset], uint32(offset-startOffset))

	// 5. finalize transaction payload event.
	out[offset] = event.TRANSACTION_PAYLOAD_EVENT
	offset += 1
	offset += 4          // 预留4个字节用于存储transaction payload event的长度
	startOffset = offset // 记录transaction payload event的起始位置，用于最后计算transaction payload event的长度
	// fmt.Print("压缩Event: TransactionPayload 压缩列: TransactionPayloadAll ")
	offset = c.cmprs.TransactionPayloadCmpr.AllCmpr.Finalize(&out, offset)
	binary.LittleEndian.PutUint32(out[startOffset-4:startOffset], uint32(offset-startOffset))

	// 6. finalize previous gtid event.
	out[offset] = event.PREVIOUS_GTIDS_EVENT
	offset += 1
	offset += 4          // 预留4个字节用于存储previous gtid event的长度
	startOffset = offset // 记录previous gtid event的起始位置，用于最后计算previous gtid event的长度
	// offset = c.cmprs.PreviousGTIDsCmpr.FlagsCmpr.Finalize(&out, offset)
	// offset = c.cmprs.PreviousGTIDsCmpr.GTIDCmpr.Finalize(&out, offset)
	// fmt.Print("压缩Event: PreviousGTIDs 压缩列: PreviousGTIDsAll ")
	offset = c.cmprs.PreviousGTIDsCmpr.AllCmpr.Finalize(&out, offset)
	binary.LittleEndian.PutUint32(out[startOffset-4:startOffset], uint32(offset-startOffset))

	// 7. finalize anonymous gtid event.
	out[offset] = event.ANONYMOUS_GTID_EVENT
	offset += 1
	offset += 4          // 预留4个字节用于存储anonymous gtid event的长度
	startOffset = offset // 记录anonymous gtid event的起始位置，用于最后计算anonymous gtid event的长度
	// fmt.Print("压缩Event: AnonymousGTID 压缩列: AnonymousGTIDAll ")
	offset = c.cmprs.AnonymousGTIDCmpr.AllCmpr.Finalize(&out, offset)
	binary.LittleEndian.PutUint32(out[startOffset-4:startOffset], uint32(offset-startOffset))

	// 8. finalize format description event.
	out[offset] = event.FORMAT_DESCRIPTION_EVENT
	offset += 1
	offset += 4          // 预留4个字节用于存储format description event的长度
	startOffset = offset // 记录format description event的起始位置，用于最后计算format description event的长度
	// fmt.Print("压缩Event: FormatDescription 压缩列: FormatDescriptionAll ")
	offset = c.cmprs.FormatDescriptionCmpr.AllCmpr.Finalize(&out, offset)
	binary.LittleEndian.PutUint32(out[startOffset-4:startOffset], uint32(offset-startOffset))

	// 4. finalize query event.
	// 4.0 finalize query type.
	out[offset] = event.QUERY_EVENT
	offset += 1
	offset += 4          // 预留4个字节用于存储query event的长度
	startOffset = offset // 记录query event的起始位置，用于最后计算query event的长度
	// fmt.Print("压缩Event: Query 压缩列: QueryAll ")
	offset = c.cmprs.QueryEventCmpr.AllCmpr.Finalize(&out, offset)
	// // 4.1 finalize query event's thread id.
	// offset = c.cmprs.QueryEventCmpr.ThreadIdCmpr.Finalize(&out, offset)
	// // 4.2 finalize query event's exec time.
	// offset = c.cmprs.QueryEventCmpr.ExecTimeCmpr.Finalize(&out, offset)
	// // 4.3 finalize query event's db name len.
	// offset = c.cmprs.QueryEventCmpr.DbNameLenCmpr.Finalize(&out, offset)
	// // 4.4 finalize query event's error code.
	// offset = c.cmprs.QueryEventCmpr.ErrorCodeCmpr.Finalize(&out, offset)
	// // 4.5 finalize query event's status var len.
	// offset = c.cmprs.QueryEventCmpr.StatusVarLenCmpr.Finalize(&out, offset)
	// // 4.6 finalize query event's status var.
	// offset = c.cmprs.QueryEventCmpr.StatusVarCmpr.Finalize(&out, offset)
	// // 4.7 finalize query event's db name.
	// offset = c.cmprs.QueryEventCmpr.DbNameCmpr.Finalize(&out, offset)
	// // 4.8 finalize query event's sql.
	// offset = c.cmprs.QueryEventCmpr.SqlCmpr.Finalize(&out, offset)
	// 4.9 finalize query event's len.
	binary.LittleEndian.PutUint32(out[startOffset-4:startOffset], uint32(offset-startOffset))

	// 5. finalize table map event.
	// 5.0 finalize table map type.
	out[offset] = event.TABLE_MAP_EVENT
	offset += 1
	offset += 4          // 预留4个字节用于存储table map event的长度
	startOffset = offset // 记录table map event的起始位置，用于最后计算table map event的长度
	// // 5.1 finalize table map event's table id.
	// fmt.Print("压缩Event: TableMap 压缩列: TableMapAll ")
	offset = c.cmprs.TableMapCmpr.TableIdCmpr.Finalize(&out, offset)
	// fmt.Print("压缩Event: TableMap 压缩列: TableMapAll ")
	// offset = c.cmprs.TableMapCmpr.AllCmpr.Finalize(&out, offset)
	// 5.2 finalize table map event's no used.
	// fmt.Print("压缩Event: TableMap 压缩列: Noused ")
	offset = c.cmprs.TableMapCmpr.NoUsedCmpr.Finalize(&out, offset)
	// 5.3 finalize table map event's db name len.
	// fmt.Print("压缩Event: TableMap 压缩列: DbNameLen ")
	offset = c.cmprs.TableMapCmpr.DbNameLenCmpr.Finalize(&out, offset)
	// 5.4 finalize table map event's db name.
	// fmt.Print("压缩Event: TableMap 压缩列: DbName ")
	offset = c.cmprs.TableMapCmpr.DbNameCmpr.Finalize(&out, offset)
	// 5.5 finalize table map event's table info.
	// fmt.Print("压缩Event: TableMap 压缩列: TableInfo ")
	offset = c.cmprs.TableMapCmpr.TableInfoCmpr.Finalize(&out, offset)
	// 5.6 finalize table map event's len.
	binary.LittleEndian.PutUint32(out[startOffset-4:startOffset], uint32(offset-startOffset))

	// 6. finalize write rows event.
	// 6.0 finalize write rows type.
	out[offset] = event.WRITE_ROWS_EVENTv2
	offset += 1
	offset += 4          // 预留4个字节用于存储write rows event的长度
	startOffset = offset // 记录write rows event的起始位置，用于最后计算write rows event的长度
	// // 6.1 finalize write rows event's table id.
	// fmt.Print("压缩Event: WriteRows 压缩列: TableId ")
	offset = c.cmprs.WriteRowsCmpr.TableIdCmpr.Finalize(&out, offset)

	// 6.2 finalize write rows event's reserved.
	// fmt.Print("压缩Event: WriteRows 压缩列: Reserved ")
	offset = c.cmprs.WriteRowsCmpr.ReservedCmpr.Finalize(&out, offset)
	// // 6.3 finalize write rows event's extra info len.
	// fmt.Print("压缩Event: WriteRows 压缩列: ExtraInfoLen ")
	offset = c.cmprs.WriteRowsCmpr.ExtraInfoLenCmpr.Finalize(&out, offset)
	// // 6.4 finalize write rows event's extra info.

	// TODOIMP(wangqian): extrainfo如果是空的，不压缩也行？

	// fmt.Print("压缩Event: WriteRows 压缩列: ExtraInfo ")
	// offset = c.cmprs.WriteRowsCmpr.ExtraInfoCmpr.Finalize(&out, offset)
	// // 6.5 finalize write rows event's column nums.

	// TODO(wangqian):这块单独拿出来的元数据就远大于数据了...
	// fmt.Print("压缩Event: WriteRows 压缩列: ColumnNums ")
	offset = c.cmprs.WriteRowsCmpr.ColumnNumsCmpr.Finalize(&out, offset)
	// // 6.6 finalize write rows event's included columns.
	// fmt.Print("压缩Event: WriteRows 压缩列: IncludedColumns ")
	offset = c.cmprs.WriteRowsCmpr.IncludedColumnsCmpr.Finalize(&out, offset)
	// // 6.7 finalize write rows event's null columns.
	// fmt.Print("压缩Event: WriteRows 压缩列: NullColumns ")
	offset = c.cmprs.WriteRowsCmpr.NullColumnsCmpr.Finalize(&out, offset)
	// 6.8 finalize write rows event's rows.
	// TODOIMP(思考一下 这里的rows应该怎么处理)
	// fmt.Print("压缩Event: WriteRows 压缩列: Rows ")
	// offset = c.cmprs.WriteRowsCmpr.RowsCmpr.Finalize(&out, offset)

	// fmt.Print("压缩Event: WriteRows 压缩列: Rows2 ")
	offset = c.cmprs.WriteRowsCmpr.Rows2Cmpr.Finalize(&out, offset)

	// fmt.Print("压缩Event: WriteRows 压缩列: Checksum ")
	offset = c.cmprs.WriteRowsCmpr.CheckSumCmpr.Finalize(&out, offset)
	// fmt.Print("压缩Event: WriteRows 压缩列: WriteRowsAll ")
	// offset = c.cmprs.WriteRowsCmpr.AllCmpr.Finalize(&out, offset)
	// 6.9 finalize write rows event's len.
	binary.LittleEndian.PutUint32(out[startOffset-4:startOffset], uint32(offset-startOffset))

	// 7. finalize xid event.
	// 7.0 finalize xid type.
	out[offset] = event.XID_EVENT
	offset += 1
	offset += 4          // 预留4个字节用于存储xid event的长度
	startOffset = offset // 记录xid event的起始位置，用于最后计算xid event的长度
	// fmt.Print("压缩Event: Xid 压缩列: XidAll ")
	offset = c.cmprs.XidCmpr.AllCmpr.Finalize(&out, offset)
	// // 7.1 finalize xid event's xid.
	// offset = c.cmprs.XidCmpr.XidCmpr.Finalize(&out, offset)
	// 7.2 finalize xid event's len.
	binary.LittleEndian.PutUint32(out[startOffset-4:startOffset], uint32(offset-startOffset))

	return out[:offset]
}

func (c *RtcCompressor) finalizeBetter(totalEvents int, originalLen int) (out []byte) {

	// total用来初始化out的大小
	out = make([]byte, originalLen) // TODOIMP(默认压缩后数据小于压缩前数据...)
	offset := 0
	// 1. 写入total_events
	binary.LittleEndian.PutUint32(out[offset:offset+4], uint32(totalEvents))
	offset += 4

	// 1.5 写入original_len
	binary.LittleEndian.PutUint32(out[offset:offset+4], uint32(originalLen))
	offset += 4

	// 2. finalize wrapper.

	// 2.1 finalize wrapper's bodylen.
	// fmt.Print("压缩列: BodyLen ")
	offset = c.cmprs.EventWrapperCmpr.BodyLenCmpr.Finalize(&out, offset)
	// 2.2 finalize wrapper's sequence number.
	// fmt.Print("压缩列: SequenceNumber ")
	offset = c.cmprs.EventWrapperCmpr.SequenceNumberCmpr.Finalize(&out, offset)
	// 2.3 finalize wrapper's flag.
	// fmt.Print("压缩列: Flag ")
	offset = c.cmprs.EventWrapperCmpr.FlagCmpr.Finalize(&out, offset)

	// 3. finalize header.
	// 3.1 finalize header's timestamp.
	// fmt.Print("压缩列: Timestamp ")
	offset = c.cmprs.EventHeaderCmpr.TimestampCmpr.Finalize(&out, offset)
	// 3.2 finalize header's event type.
	// fmt.Print("压缩列: EventType ")
	offset = c.cmprs.EventHeaderCmpr.EventTypeCmpr.Finalize(&out, offset)
	// 3.3 finalize header's server id.
	// fmt.Print("压缩列: ServerId ")
	offset = c.cmprs.EventHeaderCmpr.ServerIdCmpr.Finalize(&out, offset)
	// 3.4 finalize header's event len.
	// fmt.Print("压缩列: EventLen ")
	offset = c.cmprs.EventHeaderCmpr.EventLenCmpr.Finalize(&out, offset)
	// 3.5 finalize header's next pos.
	// fmt.Print("压缩列: NextPos ")
	offset = c.cmprs.EventHeaderCmpr.NextPosCmpr.Finalize(&out, offset)
	// 3.6 finalize header's flags.
	// fmt.Print("压缩列: Flags ")
	offset = c.cmprs.EventHeaderCmpr.FlagsCmpr.Finalize(&out, offset)

	// 4. finalize rotate event.
	// 4.0 finalize rotate type.
	out[offset] = event.ROTATE_EVENT
	offset += 1
	offset += 4           // 预留4个字节用于存储rotate event的长度
	startOffset := offset // 记录rotate event的起始位置，用于最后计算rotate event的长度
	// 4.1 finalize rotate event's all.
	// fmt.Print("压缩Event: Rotate 压缩列: RotateAll ")
	offset = c.cmprs.RotateCmpr.AllCmpr.Finalize(&out, offset)
	binary.LittleEndian.PutUint32(out[startOffset-4:startOffset], uint32(offset-startOffset))

	// 5. finalize transaction payload event.
	out[offset] = event.TRANSACTION_PAYLOAD_EVENT
	offset += 1
	offset += 4          // 预留4个字节用于存储transaction payload event的长度
	startOffset = offset // 记录transaction payload event的起始位置，用于最后计算transaction payload event的长度
	// fmt.Print("压缩Event: TransactionPayload 压缩列: TransactionPayloadAll ")
	offset = c.cmprs.TransactionPayloadCmpr.AllCmpr.Finalize(&out, offset)
	binary.LittleEndian.PutUint32(out[startOffset-4:startOffset], uint32(offset-startOffset))

	// 6. finalize previous gtid event.
	out[offset] = event.PREVIOUS_GTIDS_EVENT
	offset += 1
	offset += 4          // 预留4个字节用于存储previous gtid event的长度
	startOffset = offset // 记录previous gtid event的起始位置，用于最后计算previous gtid event的长度
	// offset = c.cmprs.PreviousGTIDsCmpr.FlagsCmpr.Finalize(&out, offset)
	// offset = c.cmprs.PreviousGTIDsCmpr.GTIDCmpr.Finalize(&out, offset)
	// fmt.Print("压缩Event: PreviousGTIDs 压缩列: PreviousGTIDsAll ")
	offset = c.cmprs.PreviousGTIDsCmpr.AllCmpr.Finalize(&out, offset)
	binary.LittleEndian.PutUint32(out[startOffset-4:startOffset], uint32(offset-startOffset))

	// 7. finalize anonymous gtid event.
	out[offset] = event.ANONYMOUS_GTID_EVENT
	offset += 1
	offset += 4          // 预留4个字节用于存储anonymous gtid event的长度
	startOffset = offset // 记录anonymous gtid event的起始位置，用于最后计算anonymous gtid event的长度
	// fmt.Print("压缩Event: AnonymousGTID 压缩列: AnonymousGTIDAll ")
	offset = c.cmprs.AnonymousGTIDCmpr.AllCmpr.Finalize(&out, offset)
	binary.LittleEndian.PutUint32(out[startOffset-4:startOffset], uint32(offset-startOffset))

	// 8. finalize format description event.
	out[offset] = event.FORMAT_DESCRIPTION_EVENT
	offset += 1
	offset += 4          // 预留4个字节用于存储format description event的长度
	startOffset = offset // 记录format description event的起始位置，用于最后计算format description event的长度
	// fmt.Print("压缩Event: FormatDescription 压缩列: FormatDescriptionAll ")
	offset = c.cmprs.FormatDescriptionCmpr.AllCmpr.Finalize(&out, offset)
	binary.LittleEndian.PutUint32(out[startOffset-4:startOffset], uint32(offset-startOffset))

	// 4. finalize query event.
	// 4.0 finalize query type.
	out[offset] = event.QUERY_EVENT
	offset += 1
	offset += 4          // 预留4个字节用于存储query event的长度
	startOffset = offset // 记录query event的起始位置，用于最后计算query event的长度
	// fmt.Print("压缩Event: Query 压缩列: QueryAll ")
	offset = c.cmprs.QueryEventCmpr.AllCmpr.Finalize(&out, offset)
	// // 4.1 finalize query event's thread id.
	// offset = c.cmprs.QueryEventCmpr.ThreadIdCmpr.Finalize(&out, offset)
	// // 4.2 finalize query event's exec time.
	// offset = c.cmprs.QueryEventCmpr.ExecTimeCmpr.Finalize(&out, offset)
	// // 4.3 finalize query event's db name len.
	// offset = c.cmprs.QueryEventCmpr.DbNameLenCmpr.Finalize(&out, offset)
	// // 4.4 finalize query event's error code.
	// offset = c.cmprs.QueryEventCmpr.ErrorCodeCmpr.Finalize(&out, offset)
	// // 4.5 finalize query event's status var len.
	// offset = c.cmprs.QueryEventCmpr.StatusVarLenCmpr.Finalize(&out, offset)
	// // 4.6 finalize query event's status var.
	// offset = c.cmprs.QueryEventCmpr.StatusVarCmpr.Finalize(&out, offset)
	// // 4.7 finalize query event's db name.
	// offset = c.cmprs.QueryEventCmpr.DbNameCmpr.Finalize(&out, offset)
	// // 4.8 finalize query event's sql.
	// offset = c.cmprs.QueryEventCmpr.SqlCmpr.Finalize(&out, offset)
	// 4.9 finalize query event's len.
	binary.LittleEndian.PutUint32(out[startOffset-4:startOffset], uint32(offset-startOffset))

	// 5. finalize table map event.
	// 5.0 finalize table map type.
	out[offset] = event.TABLE_MAP_EVENT
	offset += 1
	offset += 4          // 预留4个字节用于存储table map event的长度
	startOffset = offset // 记录table map event的起始位置，用于最后计算table map event的长度
	// // 5.1 finalize table map event's table id.
	// fmt.Print("压缩Event: TableMap 压缩列: TableMapAll ")
	offset = c.cmprs.TableMapCmpr.TableIdCmpr.Finalize(&out, offset)
	// fmt.Print("压缩Event: TableMap 压缩列: TableMapAll ")
	// offset = c.cmprs.TableMapCmpr.AllCmpr.Finalize(&out, offset)
	// 5.2 finalize table map event's no used.
	// fmt.Print("压缩Event: TableMap 压缩列: Noused ")
	offset = c.cmprs.TableMapCmpr.NoUsedCmpr.Finalize(&out, offset)
	// 5.3 finalize table map event's db name len.
	// fmt.Print("压缩Event: TableMap 压缩列: DbNameLen ")
	offset = c.cmprs.TableMapCmpr.DbNameLenCmpr.Finalize(&out, offset)
	// 5.4 finalize table map event's db name.
	// fmt.Print("压缩Event: TableMap 压缩列: DbName ")
	offset = c.cmprs.TableMapCmpr.DbNameCmpr.Finalize(&out, offset)
	// 5.5 finalize table map event's table info.
	// fmt.Print("压缩Event: TableMap 压缩列: TableInfo ")
	offset = c.cmprs.TableMapCmpr.TableInfoCmpr.Finalize(&out, offset)
	// 5.6 finalize table map event's len.
	binary.LittleEndian.PutUint32(out[startOffset-4:startOffset], uint32(offset-startOffset))

	// 6. finalize write rows event.
	// 6.0 finalize write rows type.
	out[offset] = event.WRITE_ROWS_EVENTv2
	offset += 1
	offset += 4          // 预留4个字节用于存储write rows event的长度
	startOffset = offset // 记录write rows event的起始位置，用于最后计算write rows event的长度
	// // 6.1 finalize write rows event's table id.
	// fmt.Print("压缩Event: WriteRows 压缩列: TableId ")
	offset = c.cmprs.WriteRowsCmpr.TableIdCmpr.Finalize(&out, offset)

	// 6.2 finalize write rows event's reserved.
	// fmt.Print("压缩Event: WriteRows 压缩列: Reserved ")
	offset = c.cmprs.WriteRowsCmpr.ReservedCmpr.Finalize(&out, offset)
	// // 6.3 finalize write rows event's extra info len.
	// fmt.Print("压缩Event: WriteRows 压缩列: ExtraInfoLen ")
	offset = c.cmprs.WriteRowsCmpr.ExtraInfoLenCmpr.Finalize(&out, offset)
	// // 6.4 finalize write rows event's extra info.

	// TODOIMP(wangqian): extrainfo如果是空的，不压缩也行？

	// fmt.Print("压缩Event: WriteRows 压缩列: ExtraInfo ")
	// offset = c.cmprs.WriteRowsCmpr.ExtraInfoCmpr.Finalize(&out, offset)
	// // 6.5 finalize write rows event's column nums.

	// TODO(wangqian):这块单独拿出来的元数据就远大于数据了...
	// fmt.Print("压缩Event: WriteRows 压缩列: ColumnNums ")
	offset = c.cmprs.WriteRowsCmpr.ColumnNumsCmpr.Finalize(&out, offset)
	// // 6.6 finalize write rows event's included columns.
	// fmt.Print("压缩Event: WriteRows 压缩列: IncludedColumns ")
	offset = c.cmprs.WriteRowsCmpr.IncludedColumnsCmpr.Finalize(&out, offset)
	// // 6.7 finalize write rows event's null columns.
	// fmt.Print("压缩Event: WriteRows 压缩列: NullColumns ")
	offset = c.cmprs.WriteRowsCmpr.NullColumnsCmpr.Finalize(&out, offset)
	// 6.8 finalize write rows event's rows.
	// TODOIMP(思考一下 这里的rows应该怎么处理)
	// fmt.Print("压缩Event: WriteRows 压缩列: Rows ")
	// offset = c.cmprs.WriteRowsCmpr.RowsCmpr.Finalize(&out, offset)

	// fmt.Print("压缩Event: WriteRows 压缩列: Rows2 ")
	offset = c.cmprs.WriteRowsCmpr.Rows2Cmpr.Finalize(&out, offset)

	// fmt.Print("压缩Event: WriteRows 压缩列: Checksum ")
	offset = c.cmprs.WriteRowsCmpr.CheckSumCmpr.Finalize(&out, offset)
	// fmt.Print("压缩Event: WriteRows 压缩列: WriteRowsAll ")
	// offset = c.cmprs.WriteRowsCmpr.AllCmpr.Finalize(&out, offset)
	// 6.9 finalize write rows event's len.
	binary.LittleEndian.PutUint32(out[startOffset-4:startOffset], uint32(offset-startOffset))

	// 7. finalize xid event.
	// 7.0 finalize xid type.
	out[offset] = event.XID_EVENT
	offset += 1
	offset += 4          // 预留4个字节用于存储xid event的长度
	startOffset = offset // 记录xid event的起始位置，用于最后计算xid event的长度
	// fmt.Print("压缩Event: Xid 压缩列: XidAll ")
	offset = c.cmprs.XidCmpr.AllCmpr.Finalize(&out, offset)
	// // 7.1 finalize xid event's xid.
	// offset = c.cmprs.XidCmpr.XidCmpr.Finalize(&out, offset)
	// 7.2 finalize xid event's len.
	binary.LittleEndian.PutUint32(out[startOffset-4:startOffset], uint32(offset-startOffset))

	return out[:offset]
}
