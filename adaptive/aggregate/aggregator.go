package aggregate

import (
	"time"

	"github.com/wqshr12345/golib/common"
	"github.com/wqshr12345/golib/compression/rtc/event"
)

type Aggregator struct {
	TableInforMap map[uint64]*common.TableInfo
	testTimes     int64
}

func NewAggregator() *Aggregator {
	return &Aggregator{
		TableInforMap: make(map[uint64]*common.TableInfo),
	}
}

// 存放每个buffer行转列后的结果
type AggregateData struct {
	// 其它元数据
	TotalEvents int
	StartTs     int64 // 发送时间，仅在测试中使用
	TotalLen    int

	// TODO 这些可以预先初始化，divider传入一个预先序列号的event个数

	// 1. Binlog Stream头信息
	BodyLens        []byte // 3
	SequenceNumbers []byte // 1
	Flags1          []byte // 1

	BodyLensOff int64
	BodyLensLen int64

	SeqNumsOff int64
	SeqNumsLen int64

	Flags1Off int64
	Flags1Len int64

	// 2. Binlog Event Header信息
	Timestamps []byte // 4
	EventTypes []byte // 1
	ServerIds  []byte // 4
	EventLens  []byte // 4
	NextPoses  []byte // 4
	Flags      []byte // 2

	TimestampsOff int64
	TimestampsLen int64

	EventTypesOff int64
	EventTypesLen int64

	ServerIdsOff int64
	ServerIdsLen int64

	EventLensOff int64
	EventLensLen int64

	NextPosesOff int64
	NextPosesLen int64

	FlagsOff int64
	FlagsLen int64

	// 3. Binlog Event Data信息

	formats   FormatDescriptionEventDatas
	trans     TransactionPayloadEventDatas
	prevs     PreviousGTIDsEventDatas
	anons     AnonymousGTIDEventDatas
	rotates   RotateEventDatas
	querys    QueryEventDatas
	tablemaps TableMapEventDatas
	writerows WriteRowsEventDatas
	xids      XidEventDatas

	// 存储了数据类型(e.g. int double)到mapInfo(table id && column id)的对应
	type2Cmpr map[byte]*columnInfo
}

func NewAggregaData(startTs int64) *AggregateData {
	aggData := &AggregateData{
		StartTs: startTs,
		writerows: WriteRowsEventDatas{
			Rows2: make(map[uint64]*[][]byte),
		},
		type2Cmpr: make(map[byte]*columnInfo),
	}
	return aggData
}

// 在每个compressor第一次处理AggregateData的时候被调用，初始化Off和Len并返回，供Monitor调用
func (a *AggregateData) InitOffsetAndLen() []common.OffAndLen {
	offAndLens := make([]common.OffAndLen, common.TotalColumnNums)
	a.BodyLensOff = 0
	a.BodyLensLen = int64(len(a.BodyLens))
	offAndLens[common.BodyLen] = common.OffAndLen{Offset: a.BodyLensOff, Len: a.BodyLensLen}

	a.SeqNumsOff = 0
	a.SeqNumsLen = int64(len(a.SequenceNumbers))
	offAndLens[common.SeqNum] = common.OffAndLen{Offset: a.SeqNumsOff, Len: a.SeqNumsOff}

	a.Flags1Off = 0
	a.Flags1Len = int64(len(a.Flags1))
	offAndLens[common.Flag1] = common.OffAndLen{Offset: a.Flags1Off, Len: a.Flags1Len}

	a.TimestampsOff = 0
	a.TimestampsLen = int64(len(a.Timestamps))
	offAndLens[common.Timestamp] = common.OffAndLen{Offset: a.TimestampsOff, Len: a.TimestampsLen}

	a.EventTypesOff = 0
	a.EventTypesLen = int64(len(a.EventTypes))
	offAndLens[common.EventType] = common.OffAndLen{Offset: a.EventTypesOff, Len: a.EventTypesLen}

	a.ServerIdsOff = 0
	a.ServerIdsLen = int64(len(a.ServerIds))
	offAndLens[common.ServerId] = common.OffAndLen{Offset: a.ServerIdsOff, Len: a.ServerIdsLen}

	a.EventLensOff = 0
	a.EventLensLen = int64(len(a.EventLens))
	offAndLens[common.EventLen] = common.OffAndLen{Offset: a.EventLensOff, Len: a.EventLensLen}

	a.NextPosesOff = 0
	a.NextPosesLen = int64(len(a.NextPoses))
	offAndLens[common.NextPos] = common.OffAndLen{Offset: a.NextPosesOff, Len: a.NextPosesLen}

	a.FlagsOff = 0
	a.FlagsLen = int64(len(a.Flags))
	offAndLens[common.Flag] = common.OffAndLen{Offset: a.FlagsOff, Len: a.FlagsLen}

	a.formats.AllsOff = 0
	a.formats.AllsLen = int64(len(a.formats.Alls))
	offAndLens[common.FormatDescriptionEventAll] = common.OffAndLen{Offset: a.formats.AllsOff, Len: a.formats.AllsLen}

	a.trans.AllsOff = 0
	a.trans.AllsLen = int64(len(a.trans.Alls))
	offAndLens[common.FormatDescriptionEventAll] = common.OffAndLen{Offset: a.trans.AllsOff, Len: a.trans.AllsLen}

	a.prevs.AllsOff = 0
	a.prevs.AllsLen = int64(len(a.prevs.Alls))
	offAndLens[common.PreviousGTIDsEventAll] = common.OffAndLen{Offset: a.prevs.AllsOff, Len: a.prevs.AllsLen}

	a.anons.AllsOff = 0
	a.anons.AllsLen = int64(len(a.anons.Alls))
	offAndLens[common.AnonymousGTIDEventAll] = common.OffAndLen{Offset: a.anons.AllsOff, Len: a.anons.AllsLen}

	a.rotates.AllsOff = 0
	a.rotates.AllsLen = int64(len(a.rotates.Alls))
	offAndLens[common.RotateEventAll] = common.OffAndLen{Offset: a.rotates.AllsOff, Len: a.rotates.AllsLen}

	a.querys.AllsOff = 0
	a.querys.AllsLen = int64(len(a.querys.Alls))
	offAndLens[common.QueryEventAll] = common.OffAndLen{Offset: a.querys.AllsOff, Len: a.querys.AllsLen}

	a.xids.AllsOff = 0
	a.xids.AllsLen = int64(len(a.xids.Alls))
	offAndLens[common.XidEventAll] = common.OffAndLen{Offset: a.xids.AllsOff, Len: a.xids.AllsLen}

	a.tablemaps.TableIdsOff = 0
	a.tablemaps.TableIdsLen = int64(len(a.tablemaps.TableIds))
	offAndLens[common.TableId] = common.OffAndLen{Offset: a.tablemaps.TableIdsOff, Len: a.tablemaps.TableIdsLen}

	a.tablemaps.NoUsedsOff = 0
	a.tablemaps.NoUsedsLen = int64(len(a.tablemaps.NoUseds))
	offAndLens[common.NoUsed] = common.OffAndLen{Offset: a.tablemaps.NoUsedsOff, Len: a.tablemaps.NoUsedsLen}

	a.tablemaps.DbNameLensOff = 0
	a.tablemaps.DbNameLensLen = int64(len(a.tablemaps.DbNameLens))
	offAndLens[common.DbNameLen] = common.OffAndLen{Offset: a.tablemaps.DbNameLensOff, Len: a.tablemaps.DbNameLensLen}

	a.tablemaps.DbNamesOff = 0
	a.tablemaps.DbNamesLen = int64(len(a.tablemaps.DbNames))
	offAndLens[common.DbName] = common.OffAndLen{Offset: a.tablemaps.DbNamesOff, Len: a.tablemaps.DbNamesLen}

	a.tablemaps.TableInfosOff = 0
	a.tablemaps.TableInfosLen = int64(len(a.tablemaps.TableInfos))
	offAndLens[common.TableInfo2] = common.OffAndLen{Offset: a.tablemaps.TableInfosOff, Len: a.tablemaps.TableInfosLen}

	a.writerows.TableIdsOff = 0
	a.writerows.TableIdsLen = int64(len(a.writerows.TableIds))
	offAndLens[common.TableId2] = common.OffAndLen{Offset: a.writerows.TableIdsOff, Len: a.writerows.TableIdsLen}

	a.writerows.ReservedsOff = 0
	a.writerows.ReservedsLen = int64(len(a.writerows.Reserveds))
	offAndLens[common.Reserved] = common.OffAndLen{Offset: a.writerows.ReservedsOff, Len: a.writerows.ReservedsLen}

	a.writerows.ExtraInfoLensOff = 0
	a.writerows.ExtraInfoLensLen = int64(len(a.writerows.ExtraInfoLens))
	offAndLens[common.ExtraInfoLen] = common.OffAndLen{Offset: a.writerows.ExtraInfoLensOff, Len: a.writerows.ExtraInfoLensLen}

	a.writerows.ExtraInfosOff = 0
	a.writerows.ExtraInfosLen = int64(len(a.writerows.ExtraInfos))
	offAndLens[common.ExtraInfo] = common.OffAndLen{Offset: a.writerows.ExtraInfosOff, Len: a.writerows.ExtraInfosLen}

	a.writerows.ColumnNumsOff = 0
	a.writerows.ColumnNumsLen = int64(len(a.writerows.ColumnNums))
	offAndLens[common.ColumnNum] = common.OffAndLen{Offset: a.writerows.ColumnNumsOff, Len: a.writerows.ColumnNumsLen}

	a.writerows.IncludedColumnsOff = 0
	a.writerows.IncludedColumnsLen = int64(len(a.writerows.IncludedColumns))
	offAndLens[common.IncludedColumn] = common.OffAndLen{Offset: a.writerows.IncludedColumnsOff, Len: a.writerows.IncludedColumnsLen}

	a.writerows.NullColumnsOff = 0
	a.writerows.NullColumnsLen = int64(len(a.writerows.NullColumns))
	offAndLens[common.NullColumn] = common.OffAndLen{Offset: a.writerows.NullColumnsOff, Len: a.writerows.NullColumnsLen}

	a.writerows.ChecksumsOff = 0
	a.writerows.ChecksumsLen = int64(len(a.writerows.Checksums))
	offAndLens[common.CheckSum] = common.OffAndLen{Offset: a.writerows.ChecksumsOff, Len: a.writerows.ChecksumsLen}

	// TODO2 做用户列到外部列的转换 这里应该初始化type2Cmpr中的每一个off和len...
	// 只有前面行转列中存在的user column才会在这里被初始化
	for key, value := range a.type2Cmpr {
		idx := value.index
		if idx < len(value.info) {
			tableId := value.info[idx].tableId
			columnId := value.info[idx].columnId
			value.info[idx].len = int64(len((*a.writerows.Rows2[tableId])[columnId]))
			offAndLens[key] = common.OffAndLen{Offset: value.info[idx].off, Len: value.info[idx].len}
		} else {
			offAndLens[key] = common.OffAndLen{Offset: 0, Len: 0}
		}
	}
	// TODO 验证长度的正确性
	// for key, value := range a.writerows.RowsOffLen {
	// 	datas := a.writerows.Rows2[key]
	// 	columnLen := len(*value)
	// 	for i := 0; i < columnLen; i++ {
	// 		data := make([]int64, 2)
	// 		data[0] = 0                       // offset
	// 		data[1] = int64(len((*datas)[i])) // len
	// 		(*value)[i] = data
	// 	}
	// }
	return offAndLens
}

func (a *AggregateData) GetOffsetAndLen() []common.OffAndLen {
	offAndLens := make([]common.OffAndLen, common.TotalColumnNums)
	offAndLens[common.BodyLen] = common.OffAndLen{Offset: a.BodyLensOff, Len: a.BodyLensLen}

	offAndLens[common.SeqNum] = common.OffAndLen{Offset: a.SeqNumsOff, Len: a.SeqNumsLen}

	offAndLens[common.Flag1] = common.OffAndLen{Offset: a.Flags1Off, Len: a.Flags1Len}

	offAndLens[common.Timestamp] = common.OffAndLen{Offset: a.TimestampsOff, Len: a.TimestampsLen}

	offAndLens[common.EventType] = common.OffAndLen{Offset: a.EventTypesOff, Len: a.EventTypesLen}

	offAndLens[common.ServerId] = common.OffAndLen{Offset: a.ServerIdsOff, Len: a.ServerIdsLen}

	offAndLens[common.EventLen] = common.OffAndLen{Offset: a.EventLensOff, Len: a.EventLensLen}

	offAndLens[common.NextPos] = common.OffAndLen{Offset: a.NextPosesOff, Len: a.NextPosesLen}

	offAndLens[common.Flag] = common.OffAndLen{Offset: a.FlagsOff, Len: a.FlagsLen}

	offAndLens[common.FormatDescriptionEventAll] = common.OffAndLen{Offset: a.formats.AllsOff, Len: a.formats.AllsLen}

	offAndLens[common.TransactionPayloadEventAll] = common.OffAndLen{Offset: a.trans.AllsOff, Len: a.trans.AllsLen}

	offAndLens[common.PreviousGTIDsEventAll] = common.OffAndLen{Offset: a.prevs.AllsOff, Len: a.prevs.AllsLen}

	offAndLens[common.AnonymousGTIDEventAll] = common.OffAndLen{Offset: a.anons.AllsOff, Len: a.anons.AllsLen}

	offAndLens[common.RotateEventAll] = common.OffAndLen{Offset: a.rotates.AllsOff, Len: a.rotates.AllsLen}

	offAndLens[common.QueryEventAll] = common.OffAndLen{Offset: a.querys.AllsOff, Len: a.querys.AllsLen}

	offAndLens[common.XidEventAll] = common.OffAndLen{Offset: a.xids.AllsOff, Len: a.xids.AllsLen}

	offAndLens[common.TableId] = common.OffAndLen{Offset: a.tablemaps.TableIdsOff, Len: a.tablemaps.TableIdsLen}

	offAndLens[common.NoUsed] = common.OffAndLen{Offset: a.tablemaps.NoUsedsOff, Len: a.tablemaps.NoUsedsLen}

	offAndLens[common.DbNameLen] = common.OffAndLen{Offset: a.tablemaps.DbNameLensOff, Len: a.tablemaps.DbNameLensLen}

	offAndLens[common.DbName] = common.OffAndLen{Offset: a.tablemaps.DbNamesOff, Len: a.tablemaps.DbNamesLen}

	offAndLens[common.TableInfo2] = common.OffAndLen{Offset: a.tablemaps.TableInfosOff, Len: a.tablemaps.TableInfosLen}

	offAndLens[common.TableId2] = common.OffAndLen{Offset: a.writerows.TableIdsOff, Len: a.writerows.TableIdsLen}

	offAndLens[common.Reserved] = common.OffAndLen{Offset: a.writerows.ReservedsOff, Len: a.writerows.ReservedsLen}

	offAndLens[common.ExtraInfoLen] = common.OffAndLen{Offset: a.writerows.ExtraInfoLensOff, Len: a.writerows.ExtraInfoLensLen}

	offAndLens[common.ExtraInfo] = common.OffAndLen{Offset: a.writerows.ExtraInfosOff, Len: a.writerows.ExtraInfosLen}

	offAndLens[common.ColumnNum] = common.OffAndLen{Offset: a.writerows.ColumnNumsOff, Len: a.writerows.ColumnNumsLen}

	offAndLens[common.IncludedColumn] = common.OffAndLen{Offset: a.writerows.IncludedColumnsOff, Len: a.writerows.IncludedColumnsLen}

	offAndLens[common.NullColumn] = common.OffAndLen{Offset: a.writerows.NullColumnsOff, Len: a.writerows.NullColumnsLen}

	offAndLens[common.CheckSum] = common.OffAndLen{Offset: a.writerows.ChecksumsOff, Len: a.writerows.ChecksumsLen}

	// TODO2 做用户列到外部列的转换 这里应该初始化type2Cmpr中的每一个off和len...
	// 只有前面行转列中存在的user column才会在这里被初始化
	for key, value := range a.type2Cmpr {
		idx := value.index
		if idx < len(value.info) {
			tableId := value.info[idx].tableId
			columnId := value.info[idx].columnId
			value.info[idx].len = int64(len((*a.writerows.Rows2[tableId])[columnId]))
			offAndLens[key] = common.OffAndLen{Offset: value.info[idx].off, Len: value.info[idx].len}
		} else {
			offAndLens[key] = common.OffAndLen{Offset: 0, Len: 0}
		}
	}
	// TODO 验证长度的正确性
	// for key, value := range a.writerows.RowsOffLen {
	// 	datas := a.writerows.Rows2[key]
	// 	columnLen := len(*value)
	// 	for i := 0; i < columnLen; i++ {
	// 		data := make([]int64, 2)
	// 		data[0] = 0                       // offset
	// 		data[1] = int64(len((*datas)[i])) // len
	// 		(*value)[i] = data
	// 	}
	// }
	return offAndLens
}

// 写一个struct
// index表明当前应该从第几个开始压缩
type columnInfo struct {
	// columnsType byte
	info  []mapInfo
	index int
}

type mapInfo struct {
	tableId  uint64
	columnId uint64
	off      int64
	len      int64
}

// 作用：根据column names && bytes，得到一个对应的字节数组，同时更新内部的offset
func (a *AggregateData) GetColumnData(column byte, bytes int64) []byte {
	switch column {
	case common.BodyLen:
		oriOff := a.BodyLensOff
		if oriOff+bytes > int64(len(a.BodyLens)) {
			a.BodyLensOff += int64(len(a.BodyLens)) - oriOff
			return a.BodyLens[oriOff:]
		}
		a.BodyLensOff += bytes
		return a.BodyLens[oriOff : oriOff+bytes]
	case common.SeqNum:
		oriOff := a.SeqNumsOff
		if oriOff+bytes > int64(len(a.SequenceNumbers)) {
			a.SeqNumsOff += int64(len(a.SequenceNumbers)) - oriOff
			return a.SequenceNumbers[oriOff:]
		}
		a.SeqNumsOff += bytes
		return a.SequenceNumbers[oriOff : oriOff+bytes]
	case common.Flag1:
		oriOff := a.Flags1Off
		if oriOff+bytes > int64(len(a.Flags1)) {
			a.Flags1Off += int64(len(a.Flags1)) - oriOff
			return a.Flags1[oriOff:]
		}
		a.Flags1Off += bytes
		return a.Flags1[oriOff : oriOff+bytes]
	case common.Timestamp:
		oriOff := a.TimestampsOff
		if oriOff+bytes > int64(len(a.Timestamps)) {
			a.TimestampsOff += int64(len(a.Timestamps)) - oriOff
			return a.Timestamps[oriOff:]
		}

		a.TimestampsOff += bytes

		return a.Timestamps[oriOff : oriOff+bytes]
	case common.EventType:
		oriOff := a.EventTypesOff
		if oriOff+bytes > int64(len(a.EventTypes)) {
			a.EventTypesOff += int64(len(a.EventTypes)) - oriOff
			return a.EventTypes[oriOff:]
		}
		a.EventTypesOff += bytes
		return a.EventTypes[oriOff : oriOff+bytes]
	case common.ServerId:
		oriOff := a.ServerIdsOff
		if oriOff+bytes > int64(len(a.ServerIds)) {
			a.ServerIdsOff += int64(len(a.ServerIds)) - oriOff
			return a.ServerIds[oriOff:]
		}
		a.ServerIdsOff += bytes
		return a.ServerIds[oriOff : oriOff+bytes]
	case common.EventLen:
		oriOff := a.EventLensOff
		if oriOff+bytes > int64(len(a.EventLens)) {
			a.EventLensOff += int64(len(a.EventLens)) - oriOff
			return a.EventLens[oriOff:]
		}
		a.EventLensOff += bytes
		return a.EventLens[oriOff : oriOff+bytes]
	case common.NextPos:
		oriOff := a.NextPosesOff
		if oriOff+bytes > int64(len(a.NextPoses)) {
			a.NextPosesOff += int64(len(a.NextPoses)) - oriOff
			return a.NextPoses[oriOff:]
		}
		a.NextPosesOff += bytes
		return a.NextPoses[oriOff : oriOff+bytes]
	case common.Flag:
		oriOff := a.FlagsOff
		if oriOff+bytes > int64(len(a.Flags)) {
			a.FlagsOff += int64(len(a.Flags)) - oriOff
			return a.Flags[oriOff:]
		}
		a.FlagsOff += bytes
		return a.Flags[oriOff : oriOff+bytes]
	case common.FormatDescriptionEventAll:
		oriOff := a.formats.AllsOff
		if oriOff+bytes > int64(len(a.formats.Alls)) {
			a.formats.AllsOff += int64(len(a.formats.Alls)) - oriOff
			return a.formats.Alls[oriOff:]
		}
		a.formats.AllsOff += bytes
		return a.formats.Alls[oriOff : oriOff+bytes]
	case common.TransactionPayloadEventAll:
		oriOff := a.trans.AllsOff
		if oriOff+bytes > int64(len(a.trans.Alls)) {
			a.trans.AllsOff += int64(len(a.trans.Alls)) - oriOff
			return a.trans.Alls[oriOff:]
		}
		a.trans.AllsOff += bytes
		return a.trans.Alls[oriOff : oriOff+bytes]
	case common.PreviousGTIDsEventAll:
		oriOff := a.prevs.AllsOff
		if oriOff+bytes > int64(len(a.prevs.Alls)) {
			a.prevs.AllsOff += int64(len(a.prevs.Alls)) - oriOff
			return a.prevs.Alls[oriOff:]
		}
		a.prevs.AllsOff += bytes
		return a.prevs.Alls[oriOff : oriOff+bytes]
	case common.AnonymousGTIDEventAll:
		oriOff := a.anons.AllsOff
		if oriOff+bytes > int64(len(a.anons.Alls)) {
			a.anons.AllsOff += int64(len(a.anons.Alls)) - oriOff
			return a.anons.Alls[oriOff:]
		}
		a.anons.AllsOff += bytes
		return a.anons.Alls[oriOff : oriOff+bytes]
	case common.RotateEventAll:
		oriOff := a.rotates.AllsOff
		if oriOff+bytes > int64(len(a.rotates.Alls)) {
			a.rotates.AllsOff += int64(len(a.rotates.Alls)) - oriOff
			return a.rotates.Alls[oriOff:]
		}
		a.rotates.AllsOff += bytes
		return a.rotates.Alls[oriOff : oriOff+bytes]
	case common.QueryEventAll:
		oriOff := a.querys.AllsOff
		if oriOff+bytes > int64(len(a.querys.Alls)) {
			a.querys.AllsOff += int64(len(a.querys.Alls)) - oriOff
			return a.querys.Alls[oriOff:]
		}
		a.querys.AllsOff += bytes
		return a.querys.Alls[oriOff : oriOff+bytes]
	case common.XidEventAll:
		oriOff := a.xids.AllsOff
		if oriOff+bytes > int64(len(a.xids.Alls)) {
			a.xids.AllsOff += int64(len(a.xids.Alls)) - oriOff
			return a.xids.Alls[oriOff:]
		}
		a.xids.AllsOff += bytes
		return a.xids.Alls[oriOff : oriOff+bytes]
	case common.TableId:
		oriOff := a.tablemaps.TableIdsOff
		if oriOff+bytes > int64(len(a.tablemaps.TableIds)) {
			a.tablemaps.TableIdsOff += int64(len(a.tablemaps.TableIds)) - oriOff
			return a.tablemaps.TableIds[oriOff:]
		}
		a.tablemaps.TableIdsOff += bytes
		return a.tablemaps.TableIds[oriOff : oriOff+bytes]
	case common.NoUsed:
		oriOff := a.tablemaps.NoUsedsOff
		if oriOff+bytes > int64(len(a.tablemaps.NoUseds)) {
			a.tablemaps.NoUsedsOff += int64(len(a.tablemaps.NoUseds)) - oriOff
			return a.tablemaps.NoUseds[oriOff:]
		}
		a.tablemaps.NoUsedsOff += bytes
		return a.tablemaps.NoUseds[oriOff : oriOff+bytes]
	case common.DbNameLen:
		oriOff := a.tablemaps.DbNameLensOff
		if oriOff+bytes > int64(len(a.tablemaps.DbNameLens)) {
			a.tablemaps.DbNameLensOff += int64(len(a.tablemaps.DbNameLens)) - oriOff
			return a.tablemaps.DbNameLens[oriOff:]
		}
		a.tablemaps.DbNameLensOff += bytes
		return a.tablemaps.DbNameLens[oriOff : oriOff+bytes]
	case common.DbName:
		oriOff := a.tablemaps.DbNamesOff
		if oriOff+bytes > int64(len(a.tablemaps.DbNames)) {
			a.tablemaps.DbNamesOff += int64(len(a.tablemaps.DbNames)) - oriOff
			return a.tablemaps.DbNames[oriOff:]
		}
		a.tablemaps.DbNamesOff += bytes
		return a.tablemaps.DbNames[oriOff : oriOff+bytes]
	case common.TableInfo2:
		oriOff := a.tablemaps.TableInfosOff
		if oriOff+bytes > int64(len(a.tablemaps.TableInfos)) {
			a.tablemaps.TableInfosOff += int64(len(a.tablemaps.TableInfos)) - oriOff
			return a.tablemaps.TableInfos[oriOff:]
		}
		a.tablemaps.TableInfosOff += bytes
		return a.tablemaps.TableInfos[oriOff : oriOff+bytes]
	case common.TableId2:
		oriOff := a.writerows.TableIdsOff
		if oriOff+bytes > int64(len(a.writerows.TableIds)) {
			a.writerows.TableIdsOff += int64(len(a.writerows.TableIds)) - oriOff
			return a.writerows.TableIds[oriOff:]
		}
		a.writerows.TableIdsOff += bytes
		return a.writerows.TableIds[oriOff : oriOff+bytes]
	case common.Reserved:
		oriOff := a.writerows.ReservedsOff
		if oriOff+bytes > int64(len(a.writerows.Reserveds)) {
			a.writerows.ReservedsOff += int64(len(a.writerows.Reserveds)) - oriOff
			return a.writerows.Reserveds[oriOff:]
		}
		a.writerows.ReservedsOff += bytes
		return a.writerows.Reserveds[oriOff : oriOff+bytes]
	case common.ExtraInfoLen:
		oriOff := a.writerows.ExtraInfoLensOff
		if oriOff+bytes > int64(len(a.writerows.ExtraInfoLens)) {
			a.writerows.ExtraInfoLensOff += int64(len(a.writerows.ExtraInfoLens)) - oriOff
			return a.writerows.ExtraInfoLens[oriOff:]
		}
		a.writerows.ExtraInfoLensOff += bytes
		return a.writerows.ExtraInfoLens[oriOff : oriOff+bytes]
	case common.ExtraInfo:
		oriOff := a.writerows.ExtraInfosOff
		if oriOff+bytes > int64(len(a.writerows.ExtraInfos)) {
			a.writerows.ExtraInfosOff += int64(len(a.writerows.ExtraInfos)) - oriOff
			return a.writerows.ExtraInfos[oriOff:]
		}
		a.writerows.ExtraInfosOff += bytes
		return a.writerows.ExtraInfos[oriOff : oriOff+bytes]
	case common.ColumnNum:
		oriOff := a.writerows.ColumnNumsOff
		if oriOff+bytes > int64(len(a.writerows.ColumnNums)) {
			a.writerows.ColumnNumsOff += int64(len(a.writerows.ColumnNums)) - oriOff
			return a.writerows.ColumnNums[oriOff:]
		}
		a.writerows.ColumnNumsOff += bytes
		return a.writerows.ColumnNums[oriOff : oriOff+bytes]
	case common.IncludedColumn:
		oriOff := a.writerows.IncludedColumnsOff
		if oriOff+bytes > int64(len(a.writerows.IncludedColumns)) {
			a.writerows.IncludedColumnsOff += int64(len(a.writerows.IncludedColumns)) - oriOff
			return a.writerows.IncludedColumns[oriOff:]
		}
		a.writerows.IncludedColumnsOff += bytes
		return a.writerows.IncludedColumns[oriOff : oriOff+bytes]
	case common.NullColumn:
		oriOff := a.writerows.NullColumnsOff
		if oriOff+bytes > int64(len(a.writerows.NullColumns)) {
			a.writerows.NullColumnsOff += int64(len(a.writerows.NullColumns)) - oriOff
			return a.writerows.NullColumns[oriOff:]
		}
		a.writerows.NullColumnsOff += bytes
		return a.writerows.NullColumns[oriOff : oriOff+bytes]
	case common.CheckSum:
		oriOff := a.writerows.ChecksumsOff
		if oriOff+bytes > int64(len(a.writerows.Checksums)) {
			a.writerows.ChecksumsOff += int64(len(a.writerows.Checksums)) - oriOff
			return a.writerows.Checksums[oriOff:]
		}
		a.writerows.ChecksumsOff += bytes
		return a.writerows.Checksums[oriOff : oriOff+bytes]
	// TODO1 这里返回用户列的数据
	case common.Int:
		return a.GetBufferByBytes(common.Int, bytes)

	case common.Double:
		return a.GetBufferByBytes(common.Double, bytes)
	case common.String:
		return a.GetBufferByBytes(common.String, bytes)
	case common.Long:
		return a.GetBufferByBytes(common.Long, bytes)
	case common.TimeStamp:
		return a.GetBufferByBytes(common.TimeStamp, bytes)
	case common.Tiny:
		return a.GetBufferByBytes(common.Tiny, bytes)
	default:
		panic("unknown column.")

	}
}

// 不修改原数据
func (a AggregateData) GetColumnData2(column byte, bytes int64) []byte {
	switch column {
	case common.BodyLen:
		oriOff := a.BodyLensOff
		return a.BodyLens[oriOff : oriOff+bytes]
	case common.SeqNum:
		oriOff := a.SeqNumsOff
		return a.SequenceNumbers[oriOff : oriOff+bytes]
	case common.Flag1:
		oriOff := a.Flags1Off
		return a.Flags1[oriOff : oriOff+bytes]
	case common.Timestamp:
		oriOff := a.TimestampsOff
		return a.Timestamps[oriOff : oriOff+bytes]
	case common.EventType:
		oriOff := a.EventTypesOff
		return a.EventTypes[oriOff : oriOff+bytes]
	case common.ServerId:
		oriOff := a.ServerIdsOff
		return a.ServerIds[oriOff : oriOff+bytes]
	case common.EventLen:
		oriOff := a.EventLensOff
		return a.EventLens[oriOff : oriOff+bytes]
	case common.NextPos:
		oriOff := a.NextPosesOff
		return a.NextPoses[oriOff : oriOff+bytes]
	case common.Flag:
		oriOff := a.FlagsOff
		return a.Flags[oriOff : oriOff+bytes]
	case common.FormatDescriptionEventAll:
		oriOff := a.formats.AllsOff
		return a.formats.Alls[oriOff : oriOff+bytes]
	case common.TransactionPayloadEventAll:
		oriOff := a.trans.AllsOff
		return a.trans.Alls[oriOff : oriOff+bytes]
	case common.PreviousGTIDsEventAll:
		oriOff := a.prevs.AllsOff
		return a.prevs.Alls[oriOff : oriOff+bytes]
	case common.AnonymousGTIDEventAll:
		oriOff := a.anons.AllsOff
		return a.anons.Alls[oriOff : oriOff+bytes]
	case common.RotateEventAll:
		oriOff := a.rotates.AllsOff
		return a.rotates.Alls[oriOff : oriOff+bytes]
	case common.QueryEventAll:
		oriOff := a.querys.AllsOff
		return a.querys.Alls[oriOff : oriOff+bytes]
	case common.XidEventAll:
		oriOff := a.xids.AllsOff
		return a.xids.Alls[oriOff : oriOff+bytes]
	case common.TableId:
		oriOff := a.tablemaps.TableIdsOff
		return a.tablemaps.TableIds[oriOff : oriOff+bytes]
	case common.NoUsed:
		oriOff := a.tablemaps.NoUsedsOff
		return a.tablemaps.NoUseds[oriOff : oriOff+bytes]
	case common.DbNameLen:
		oriOff := a.tablemaps.DbNameLensOff
		return a.tablemaps.DbNameLens[oriOff : oriOff+bytes]
	case common.DbName:
		oriOff := a.tablemaps.DbNamesOff
		return a.tablemaps.DbNames[oriOff : oriOff+bytes]
	case common.TableInfo2:
		oriOff := a.tablemaps.TableInfosOff
		return a.tablemaps.TableInfos[oriOff : oriOff+bytes]
	case common.TableId2:
		oriOff := a.writerows.TableIdsOff
		return a.writerows.TableIds[oriOff : oriOff+bytes]
	case common.Reserved:
		oriOff := a.writerows.ReservedsOff
		return a.writerows.Reserveds[oriOff : oriOff+bytes]
	case common.ExtraInfoLen:
		oriOff := a.writerows.ExtraInfoLensOff
		return a.writerows.ExtraInfoLens[oriOff : oriOff+bytes]
	case common.ExtraInfo:
		oriOff := a.writerows.ExtraInfosOff
		return a.writerows.ExtraInfos[oriOff : oriOff+bytes]
	case common.ColumnNum:
		oriOff := a.writerows.ColumnNumsOff
		return a.writerows.ColumnNums[oriOff : oriOff+bytes]
	case common.IncludedColumn:
		oriOff := a.writerows.IncludedColumnsOff
		return a.writerows.IncludedColumns[oriOff : oriOff+bytes]
	case common.NullColumn:
		oriOff := a.writerows.NullColumnsOff
		return a.writerows.NullColumns[oriOff : oriOff+bytes]
	case common.CheckSum:
		oriOff := a.writerows.ChecksumsOff
		return a.writerows.Checksums[oriOff : oriOff+bytes]
	// TODO1 这里返回用户列的数据
	case common.Int:
		return a.GetBufferByBytes2(common.Int, bytes)
	case common.Double:
		return a.GetBufferByBytes2(common.Double, bytes)
	case common.String:
		return a.GetBufferByBytes2(common.String, bytes)
	case common.Long:
		return a.GetBufferByBytes2(common.Long, bytes)
	case common.TimeStamp:
		return a.GetBufferByBytes2(common.TimeStamp, bytes)
	case common.Tiny:
		return a.GetBufferByBytes2(common.Tiny, bytes)
	default:
		panic("unknown column.")

	}
}

func (a *AggregateData) GetBufferByBytes(typ byte, bytes int64) []byte {
	idx := a.type2Cmpr[typ].index
	tableId := a.type2Cmpr[typ].info[idx].tableId
	columnId := a.type2Cmpr[typ].info[idx].columnId
	oriOff := a.type2Cmpr[typ].info[idx].off
	if oriOff+bytes > int64(len((*a.writerows.Rows2[tableId])[columnId])) {
		a.type2Cmpr[typ].info[idx].off += int64(len((*a.writerows.Rows2[tableId])[columnId])) - oriOff
	}
	a.type2Cmpr[typ].info[idx].off += bytes
	for (a.type2Cmpr[typ].info[idx].off >= a.type2Cmpr[typ].info[idx].len) && (a.type2Cmpr[typ].index < len(a.type2Cmpr[typ].info)-1) {
		// IMP 因为这里idx+1之后的列，其对应数据可能是nil，导致下一次读off和len的时候为空，导致过早退出循环...所以应该在这里判断
		a.type2Cmpr[typ].index += 1
		tableId2 := a.type2Cmpr[typ].info[a.type2Cmpr[typ].index].tableId
		columnId2 := a.type2Cmpr[typ].info[a.type2Cmpr[typ].index].columnId
		if len((*a.writerows.Rows2[tableId2])[columnId2]) != 0 {
			break
		}
	}
	if oriOff+bytes > int64(len((*a.writerows.Rows2[tableId])[columnId])) {
		if oriOff > int64(len((*a.writerows.Rows2[tableId])[columnId])) {
			minLen := min(bytes, int64(len((*a.writerows.Rows2[tableId])[columnId])))
			return (*a.writerows.Rows2[tableId])[columnId][:minLen]
		}
		return (*a.writerows.Rows2[tableId])[columnId][oriOff:]
	}
	return (*a.writerows.Rows2[tableId])[columnId][oriOff : oriOff+bytes]

}

func (a AggregateData) GetBufferByBytes2(typ byte, bytes int64) []byte {
	idx := a.type2Cmpr[typ].index
	tableId := a.type2Cmpr[typ].info[idx].tableId
	columnId := a.type2Cmpr[typ].info[idx].columnId
	oriOff := a.type2Cmpr[typ].info[idx].off
	// for (a.type2Cmpr[typ].info[idx].off == a.type2Cmpr[typ].info[idx].len) && (a.type2Cmpr[typ].index < len(a.type2Cmpr[typ].info)-1) {
	// 	// IMP 因为这里idx+1之后的列，其对应数据可能是nil，导致下一次读off和len的时候为空，导致过早退出循环...所以应该在这里判断
	// 	tempIdx := a.type2Cmpr[typ].index + 1
	// 	tableId := a.type2Cmpr[typ].info[tempIdx].tableId
	// 	columnId := a.type2Cmpr[typ].info[tempIdx].columnId
	// 	if len((*a.writerows.Rows2[tableId])[columnId]) != 0 {
	// 		break
	// 	}
	// }

	return (*a.writerows.Rows2[tableId])[columnId][oriOff : oriOff+bytes]
}

type AggregateMetaData struct {
	BodyLensOff int
	SeqNumsOff  int
	Flags1Off   int

	TimestampsOff int
	EventTypesOff int
	ServerIdsOff  int
	EventLensOff  int
	NextPosesOff  int
	FlagsOff      int
}

// input: 按照event边界切割好的binlog流的字节数组
// output: binlog流经过数据区分且行转列后的AggregateData结构体
func (a *Aggregator) Aggregate(input <-chan common.DataWithInfo, output chan<- *AggregateData) {
	for {
		// 获取一个切分好的binlog流
		dataWithInfo := <-input
		data := dataWithInfo.Data
		start := 0
		total := len(data)
		totalEvents := 0

		aggData := NewAggregaData(dataWithInfo.Ts)
		// aggData.StartTs = time.Now().UnixNano()

		for start < total {
			totalEvents += 1
			// 反序列化 && 行转列
			start = a.deserialize(data, aggData, start)
		}
		aggData.TotalEvents = totalEvents
		aggData.TotalLen = len(data)
		if dataWithInfo.TotalEvents != int64(totalEvents) {
			panic("total events not equal")
		}
		// TODOIMP 为了减少模拟实验的误差，剔除行转列的时间
		aggData.StartTs = time.Now().UnixNano()
		// fmt.Println("aggregate package", a.testTimes)
		a.testTimes++
		output <- aggData
		dataWithInfo.Data = nil
	}

}

func (a *Aggregator) deserialize(input []byte, aggData *AggregateData, start int) int {
	var bodyLen int
	start, bodyLen = a.deserializeBinlogHeader(input, aggData, start)

	var eventType byte
	start, eventType = a.deserializeEventHead(input, aggData, start)

	// -1表示flag长度，它也属于bodyLen的一部分; -19表示EventHeader长度
	start = a.deserializeEventData(input, aggData, eventType, start, bodyLen-1-19)

	return start
}

func (a *Aggregator) deserializeBinlogHeader(input []byte, aggData *AggregateData, start int) (int, int) {
	aggData.BodyLens = append(aggData.BodyLens, input[start:start+3]...)
	aggData.SequenceNumbers = append(aggData.SequenceNumbers, input[start+3:start+4]...)
	aggData.Flags1 = append(aggData.Flags1, input[start+4:start+5]...)

	bodyLen := int(uint32(input[start])<<0 | uint32(input[start+1])<<8 | uint32(input[start+2])<<16)

	// TODO(wangqian)：这里基于的假设是——一个body中包含了一个完整的event，而不是两个body包含一个event
	if input[4] != 0x00 {
		panic("not a ok event(maybe handshake or err event")
	}
	start += 5
	return start, bodyLen
}

func (a *Aggregator) deserializeEventHead(input []byte, aggData *AggregateData, start int) (int, byte) {
	eventType := input[start+4]
	aggData.Timestamps = append(aggData.Timestamps, input[start:start+4]...)
	aggData.EventTypes = append(aggData.EventTypes, input[start+4:start+5]...)
	aggData.ServerIds = append(aggData.ServerIds, input[start+5:start+9]...)
	aggData.EventLens = append(aggData.EventLens, input[start+9:start+13]...)
	aggData.NextPoses = append(aggData.NextPoses, input[start+13:start+17]...)
	aggData.Flags = append(aggData.Flags, input[start+17:start+19]...)
	start += 19
	// 第4个字节——eventType
	return start, eventType
}

// 此处的end为本binlog data的最后一个字节所处位置
func (a *Aggregator) deserializeEventData(input []byte, aggData *AggregateData, eventType byte, start int, len int) int {
	switch eventType {
	case event.FORMAT_DESCRIPTION_EVENT: // 几乎不出现 11
		aggData.formats.totalEvents += 1
		a.deserializeFormatDescriptionEventData(input[start:start+len], aggData)
	case event.TRANSACTION_PAYLOAD_EVENT: // 偶尔出现 1622
		aggData.trans.totalEvents += 1
		a.deserializeTransactionPayloadEventData(input[start:start+len], aggData)
	case event.PREVIOUS_GTIDS_EVENT: // 几乎不出现 11
		aggData.prevs.totalEvents += 1
		a.deserializePreviousGTIDsEventData(input[start:start+len], aggData)
	case event.ANONYMOUS_GTID_EVENT: // 偶尔出现 3250
		aggData.anons.totalEvents += 1
		a.deserializeAnonymousGTIDEventData(input[start:start+len], aggData)
	case event.ROTATE_EVENT: // 几乎不出现 21
		aggData.rotates.totalEvents += 1
		a.deserializeRotateEventData(input[start:start+len], aggData)
	case event.QUERY_EVENT: // 偶尔出现 3250
		aggData.querys.totalEvents += 1
		a.deserializeQueryEventData(input[start:start+len], aggData)
	case event.TABLE_MAP_EVENT: // 经常出现 97258
		aggData.tablemaps.totalEvents += 1
		a.deserializeTableMapEventData(input[start:start+len], aggData)
	case event.WRITE_ROWS_EVENTv0:
	case event.WRITE_ROWS_EVENTv1:
	case event.WRITE_ROWS_EVENTv2: // 经常出现 97258
		aggData.writerows.totalEvents += 1
		a.deserializeWriteRowsEventData(input[start:start+len], aggData)
	case event.XID_EVENT: // 偶尔出现 3243
		aggData.xids.totalEvents += 1
		a.deserializeXid(input[start:start+len], aggData)
	default:
		panic("unknown event type." + string(eventType))
	}
	start += len
	return start
}

func (a *Aggregator) deserializeFormatDescriptionEventData(input []byte, aggData *AggregateData) {
	// eventData.BinlogVersion = src[:2]
	// eventData.ServerVersion = src[2:52]
	// eventData.CreateTime = src[52:56]
	// eventData.HeaderLen = src[56:57]
	// eventData.All = src
	// c.cmtype[14] = 1
	aggData.formats.BinlogVersions = append(aggData.formats.BinlogVersions, input[:2]...)
	aggData.formats.ServerVersions = append(aggData.formats.ServerVersions, input[2:52]...)
	aggData.formats.CreateTimes = append(aggData.formats.CreateTimes, input[52:56]...)
	aggData.formats.HeaderLens = append(aggData.formats.HeaderLens, input[56:57]...)
	aggData.formats.Alls = append(aggData.formats.Alls, input...)
	// return eventData
}

func (a *Aggregator) deserializeTransactionPayloadEventData(input []byte, aggData *AggregateData) {
	aggData.trans.Alls = append(aggData.trans.Alls, input...)
}

func (a *Aggregator) deserializePreviousGTIDsEventData(input []byte, aggData *AggregateData) {
	aggData.prevs.Alls = append(aggData.prevs.Alls, input...)
}

func (a *Aggregator) deserializeAnonymousGTIDEventData(input []byte, aggData *AggregateData) {
	aggData.anons.Alls = append(aggData.anons.Alls, input...)
}

func (a *Aggregator) deserializeRotateEventData(input []byte, aggData *AggregateData) {
	aggData.rotates.Positions = append(aggData.rotates.Positions, input[:8]...)
	aggData.rotates.NextNames = append(aggData.rotates.NextNames, input[8:]...)
	aggData.rotates.Alls = append(aggData.rotates.Alls, input...)
}

func (a *Aggregator) deserializeQueryEventData(input []byte, aggData *AggregateData) {
	aggData.querys.ThreadIds = append(aggData.querys.ThreadIds, input[:4]...)
	aggData.querys.ExecTimes = append(aggData.querys.ExecTimes, input[4:8]...)
	aggData.querys.DbNameLens = append(aggData.querys.DbNameLens, input[8:9]...)
	aggData.querys.ErrorCodes = append(aggData.querys.ErrorCodes, input[9:11]...)
	aggData.querys.StatusVarLens = append(aggData.querys.StatusVarLens, input[11:13]...)
	statusVarLen := int(input[11]) | int(input[12])<<8 // little end
	// statusVarLen := int(aggData.querys.StatusVarLens[0]) | int(aggData.querys.StatusVarLens[1])<<8 // little end
	aggData.querys.StatusVars = append(aggData.querys.StatusVars, input[13:13+statusVarLen]...)
	// TODO 验证Dbnames的长度是否正确
	aggData.querys.DbNames = append(aggData.querys.DbNames, input[13+statusVarLen:13+statusVarLen+int(input[8])]...)
	aggData.querys.Sqls = append(aggData.querys.Sqls, input[13+statusVarLen+int(aggData.querys.DbNameLens[0]):]...)

	aggData.querys.Alls = append(aggData.querys.Alls, input...)
}

func (a *Aggregator) deserializeTableMapEventData(input []byte, aggData *AggregateData) {
	aggData.tablemaps.TableIds = append(aggData.tablemaps.TableIds, input[:6]...)
	aggData.tablemaps.NoUseds = append(aggData.tablemaps.NoUseds, input[6:8]...)
	aggData.tablemaps.DbNameLens = append(aggData.tablemaps.DbNameLens, input[8:9]...)
	aggData.tablemaps.DbNames = append(aggData.tablemaps.DbNames, input[9:9+int(input[8])]...)
	aggData.tablemaps.TableInfos = append(aggData.tablemaps.TableInfos, input[9+int(input[8]):]...)
	// TODO 验证tableId是否正确
	tableId := uint64(input[0]) | uint64(input[1])<<8 | uint64(input[2])<<16 | uint64(input[3])<<24 | uint64(input[4])<<32 | uint64(input[5])<<40

	// 讲
	if _, ok := a.TableInforMap[tableId]; !ok {
		a.TableInforMap[tableId] = common.NewTableInfo(input[9+int(input[8]):])
	}
}

func (a *Aggregator) deserializeWriteRowsEventData(input []byte, aggData *AggregateData) {

	aggData.writerows.TableIds = append(aggData.writerows.TableIds, input[:6]...)
	aggData.writerows.Reserveds = append(aggData.writerows.Reserveds, input[6:8]...)
	aggData.writerows.ExtraInfoLens = append(aggData.writerows.ExtraInfoLens, input[8:10]...)

	extraInfoLen := int(input[8]) | int(input[9])<<8 // little end

	aggData.writerows.ExtraInfos = append(aggData.writerows.ExtraInfos, input[10:10+extraInfoLen-2]...)
	aggData.writerows.ColumnNums = append(aggData.writerows.ColumnNums, input[10+extraInfoLen-2:10+extraInfoLen-1]...)
	columnNums := int(input[10+extraInfoLen-2])
	// TODOIMP 目前最多支持251个数据库列
	if columnNums >= 251 {
		panic("number of columns is too large.")
	}
	//
	includedColumnsLen := (columnNums + 7) / 8

	aggData.writerows.IncludedColumns = append(aggData.writerows.IncludedColumns, input[10+extraInfoLen-1:10+extraInfoLen+includedColumnsLen-1]...)
	aggData.writerows.NullColumns = append(aggData.writerows.NullColumns, input[10+extraInfoLen+includedColumnsLen-1:10+extraInfoLen+includedColumnsLen+includedColumnsLen-1]...)

	// aggData.writerows.Rows = append(aggData.writerows.Rows, input[10+extraInfoLen+includedColumnsLen+includedColumnsLen-1:]...)

	tableId := uint64(input[0]) | uint64(input[1])<<8 | uint64(input[2])<<16 | uint64(input[3])<<24 | uint64(input[4])<<32 | uint64(input[5])<<40

	// 获得当前TableId对应的元信息
	it := a.TableInforMap[tableId]
	// 第一次进入这行逻辑，需要确定列的个数从而初始化rows2
	if _, ok := aggData.writerows.Rows2[tableId]; !ok {
		// aggData.writerows.Rows2[tableId] = make([][]byte, columnNums)
		datas := make([][]byte, columnNums)
		aggData.writerows.Rows2[tableId] = &datas
		for i := 0; i < columnNums; i++ {
			// TODO3 根据类型初始化...
			if it.ColumnTypes[i] == common.MYSQL_TYPE_LONG {
				if _, ok := aggData.type2Cmpr[common.Long]; !ok {
					aggData.type2Cmpr[common.Long] = &columnInfo{info: make([]mapInfo, 0), index: 0}
				}
				aggData.type2Cmpr[common.Long].info = append(aggData.type2Cmpr[common.Long].info, mapInfo{tableId: tableId, columnId: uint64(i), off: 0, len: 0})
			} else if it.ColumnTypes[i] == common.MYSQL_TYPE_VARCHAR || it.ColumnTypes[i] == common.MYSQL_TYPE_BLOB {
				if _, ok := aggData.type2Cmpr[common.String]; !ok {
					aggData.type2Cmpr[common.String] = &columnInfo{info: make([]mapInfo, 0), index: 0}
				}
				aggData.type2Cmpr[common.String].info = append(aggData.type2Cmpr[common.String].info, mapInfo{tableId: tableId, columnId: uint64(i), off: 0, len: 0})

			} else if it.ColumnTypes[i] == common.MYSQL_TYPE_TIMESTAMP2 || it.ColumnTypes[i] == common.MYSQL_TYPE_DATETIME2 {
				if _, ok := aggData.type2Cmpr[common.TimeStamp]; !ok {
					aggData.type2Cmpr[common.TimeStamp] = &columnInfo{info: make([]mapInfo, 0), index: 0}
				}
				aggData.type2Cmpr[common.TimeStamp].info = append(aggData.type2Cmpr[common.TimeStamp].info, mapInfo{tableId: tableId, columnId: uint64(i), off: 0, len: 0})
			} else if it.ColumnTypes[i] == common.MYSQL_TYPE_TINY {
				if _, ok := aggData.type2Cmpr[common.Tiny]; !ok {
					aggData.type2Cmpr[common.Tiny] = &columnInfo{info: make([]mapInfo, 0), index: 0}
				}
				aggData.type2Cmpr[common.Tiny].info = append(aggData.type2Cmpr[common.Tiny].info, mapInfo{tableId: tableId, columnId: uint64(i), off: 0, len: 0})
			} else if it.ColumnTypes[i] == common.MYSQL_TYPE_DOUBLE {
				if _, ok := aggData.type2Cmpr[common.Double]; !ok {
					aggData.type2Cmpr[common.Double] = &columnInfo{info: make([]mapInfo, 0), index: 0}
				}
				aggData.type2Cmpr[common.Double].info = append(aggData.type2Cmpr[common.Double].info, mapInfo{tableId: tableId, columnId: uint64(i), off: 0, len: 0})
			} else if it.ColumnTypes[i] == common.MYSQL_TYPE_SHORT {
				if _, ok := aggData.type2Cmpr[common.Int]; !ok {
					aggData.type2Cmpr[common.Int] = &columnInfo{info: make([]mapInfo, 0), index: 0}
				}
				aggData.type2Cmpr[common.Int].info = append(aggData.type2Cmpr[common.Int].info, mapInfo{tableId: tableId, columnId: uint64(i), off: 0, len: 0})
			} else if it.ColumnTypes[i] == common.MYSQL_TYPE_DATE {
				if _, ok := aggData.type2Cmpr[common.TimeStamp]; !ok {
					aggData.type2Cmpr[common.TimeStamp] = &columnInfo{info: make([]mapInfo, 0), index: 0}
				}
				aggData.type2Cmpr[common.TimeStamp].info = append(aggData.type2Cmpr[common.TimeStamp].info, mapInfo{tableId: tableId, columnId: uint64(i), off: 0, len: 0})
			} else if it.ColumnTypes[i] == common.MYSQL_TYPE_TIME2 {
				if _, ok := aggData.type2Cmpr[common.TimeStamp]; !ok {
					aggData.type2Cmpr[common.TimeStamp] = &columnInfo{info: make([]mapInfo, 0), index: 0}
				}
				aggData.type2Cmpr[common.TimeStamp].info = append(aggData.type2Cmpr[common.TimeStamp].info, mapInfo{tableId: tableId, columnId: uint64(i), off: 0, len: 0})
			} else {
				panic("unknown column type.")
			}
		}
	}

	n := common.DecodeImage(input[10+extraInfoLen+includedColumnsLen+includedColumnsLen-1:], input[10+extraInfoLen-1:10+extraInfoLen+includedColumnsLen-1], input[10+extraInfoLen+includedColumnsLen-1:10+extraInfoLen+includedColumnsLen+includedColumnsLen-1], it, aggData.writerows.Rows2[tableId])
	aggData.writerows.Checksums = append(aggData.writerows.Checksums, input[10+extraInfoLen+includedColumnsLen+includedColumnsLen-1+n:]...)

	if len(aggData.writerows.Checksums)%4 != 0 {
		panic("checksum is not 4.")
	}
}

func (a *Aggregator) deserializeXid(input []byte, aggData *AggregateData) {
	aggData.xids.Alls = append(aggData.xids.Alls, input...)
}
