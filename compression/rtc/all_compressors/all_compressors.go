package allcompressors

import (
	"github.com/wqshr12345/golib/compression/rtc/common"

	lwcompression "github.com/wqshr12345/golib/compression/rtc/light_wight_compression"
)

// 生命周期仅仅和一次Compress()的调用相关

type AllCompressors struct {
	EventWrapperCmpr       *EventWrapperCompressors
	EventHeaderCmpr        *EventHeaderCompressors
	RotateCmpr             *RotateCompressors
	TransactionPayloadCmpr *TransactionPayloadCompressors
	PreviousGTIDsCmpr      *PreviousGTIDsCompressors
	AnonymousGTIDCmpr      *AonymousGTIDCompressors
	FormatDescriptionCmpr  *FormatDescCompressors
	QueryEventCmpr         *QueryEventCompressors
	TableMapCmpr           *TableMapCompressors
	WriteRowsCmpr          *WriteRowsCompressors
	XidCmpr                *XidCompressors
	StopCmpr               *StopCompressors

	// 生命周期和一次Compress相同，从TableMap中
	TableInforMap map[uint64]*common.TableInfo
}

func NewAllCompressors() *AllCompressors {
	return &AllCompressors{
		EventWrapperCmpr:       NewEventWrapperCompressors(),
		EventHeaderCmpr:        NewEventHeaderCompressors(),
		RotateCmpr:             NewRotateCompressors(),
		TransactionPayloadCmpr: NewTransactionPayloadCompressors(),
		PreviousGTIDsCmpr:      NewPreviousGTIDsCompressors(),
		AnonymousGTIDCmpr:      NewAonymousGTIDCompressors(),
		FormatDescriptionCmpr:  NewFormatDescCompressors(),
		QueryEventCmpr:         NewQueryEventCompressors(),
		TableMapCmpr:           NewTableMapCompressors(),
		WriteRowsCmpr:          NewWriteRowsCompressors(),
		XidCmpr:                NewXidCompressors(),
		StopCmpr:               NewStopCompressors(),
		TableInforMap:          make(map[uint64]*common.TableInfo),
	}
}

func (a *AllCompressors) Reset() {
	a.EventWrapperCmpr = NewEventWrapperCompressors()
	a.EventHeaderCmpr = NewEventHeaderCompressors()
	a.RotateCmpr = NewRotateCompressors()
	a.TransactionPayloadCmpr = NewTransactionPayloadCompressors()
	a.PreviousGTIDsCmpr = NewPreviousGTIDsCompressors()
	a.AnonymousGTIDCmpr = NewAonymousGTIDCompressors()
	a.FormatDescriptionCmpr = NewFormatDescCompressors()
	a.QueryEventCmpr = NewQueryEventCompressors()
	a.TableMapCmpr = NewTableMapCompressors()
	a.WriteRowsCmpr = NewWriteRowsCompressors()
	a.XidCmpr = NewXidCompressors()
	a.StopCmpr = NewStopCompressors()
}

type EventWrapperCompressors struct {
	BodyLenCmpr        *lwcompression.ZstdCompressor
	SequenceNumberCmpr *lwcompression.ZstdCompressor
	FlagCmpr           *lwcompression.ZstdCompressor
	AllCmpr            *lwcompression.ZstdCompressor
}

func NewEventWrapperCompressors() *EventWrapperCompressors {
	return &EventWrapperCompressors{
		BodyLenCmpr:        lwcompression.NewZstdCompressor(common.ZstdCompressor, common.DataLenFixed, 3, "eventwrapper.bodylen", common.RtcPerfs),
		SequenceNumberCmpr: lwcompression.NewZstdCompressor(common.ZstdCompressor, common.DataLenFixed, 1, "eventwrapper.sequencenumber", common.RtcPerfs),
		FlagCmpr:           lwcompression.NewZstdCompressor(common.ZstdCompressor, common.DataLenFixed, 1, "eventwrapper.flag", common.RtcPerfs),
		AllCmpr:            lwcompression.NewZstdCompressor(common.ZstdCompressor, common.DataLenVariable, 0, "eventwrapper.all", common.RtcPerfs),
	}
}

type EventHeaderCompressors struct {
	TimestampCmpr *lwcompression.DodCompressor
	EventTypeCmpr *lwcompression.ZstdCompressor
	ServerIdCmpr  *lwcompression.RleCompressor
	EventLenCmpr  *lwcompression.ZstdCompressor
	NextPosCmpr   *lwcompression.DeltaCompressor
	FlagsCmpr     *lwcompression.ZstdCompressor
	AllCmpr       *lwcompression.ZstdCompressor
}

func NewEventHeaderCompressors() *EventHeaderCompressors {
	return &EventHeaderCompressors{
		// TimestampCmpr: lwcompression.NewNoCompressor2(common.DataLenFixed, 4),
		TimestampCmpr: lwcompression.NewDodCompressor("eventheader.timestamp", common.RtcPerfs),
		EventTypeCmpr: lwcompression.NewZstdCompressor(common.ZstdCompressor, common.DataLenFixed, 1, "eventheader.eventtype", common.RtcPerfs),
		ServerIdCmpr:  lwcompression.NewRleCompressor(4, "eventheader.serverid", common.RtcPerfs),
		EventLenCmpr:  lwcompression.NewZstdCompressor(common.ZstdCompressor, common.DataLenFixed, 4, "eventheader.eventlen", common.RtcPerfs),
		NextPosCmpr:   lwcompression.NewDeltaCompressor(4, "eventheader.nextpos", common.RtcPerfs),
		FlagsCmpr:     lwcompression.NewZstdCompressor(common.ZstdCompressor, common.DataLenFixed, 2, "eventheader.flags", common.RtcPerfs),
		AllCmpr:       lwcompression.NewZstdCompressor(common.ZstdCompressor, common.DataLenFixed, 19, "eventheader.all", common.RtcPerfs),
	}
}

type RotateCompressors struct {
	AllCmpr *lwcompression.ZstdCompressor
}

func NewRotateCompressors() *RotateCompressors {
	return &RotateCompressors{
		AllCmpr: lwcompression.NewZstdCompressor(common.ZstdCompressor, common.DataLenVariable, 0, "rotate.all", common.RtcPerfs),
	}
}

type FormatDescCompressors struct {
	BinlogVersionCmpr *lwcompression.NoCompressor
	ServerVersionCmpr *lwcompression.NoCompressor
	CreateTimeCmpr    *lwcompression.NoCompressor
	HeaderLenCmpr     *lwcompression.NoCompressor
	AllCmpr           *lwcompression.ZstdCompressor
}

func NewFormatDescCompressors() *FormatDescCompressors {
	return &FormatDescCompressors{
		BinlogVersionCmpr: lwcompression.NewNoCompressor2(common.DataLenFixed, 2, "formatdesc.binlogversion", common.RtcPerfs),
		ServerVersionCmpr: lwcompression.NewNoCompressor2(common.DataLenFixed, 50, "formatdesc.serverversion", common.RtcPerfs),
		CreateTimeCmpr:    lwcompression.NewNoCompressor2(common.DataLenFixed, 4, "formatdesc.createtime", common.RtcPerfs),
		HeaderLenCmpr:     lwcompression.NewNoCompressor2(common.DataLenFixed, 1, "formatdesc.headerlen", common.RtcPerfs),
		AllCmpr:           lwcompression.NewZstdCompressor(common.ZstdCompressor, common.DataLenVariable, 0, "formatdesc.all", common.RtcPerfs),
	}
}

type TransactionPayloadCompressors struct {
	AllCmpr *lwcompression.ZstdCompressor
}

func NewTransactionPayloadCompressors() *TransactionPayloadCompressors {
	return &TransactionPayloadCompressors{
		AllCmpr: lwcompression.NewZstdCompressor(common.ZstdCompressor, common.DataLenVariable, 0, "transactionpayload.all", common.RtcPerfs),
	}
}

type PreviousGTIDsCompressors struct {
	FlagsCmpr *lwcompression.NoCompressor
	GTIDCmpr  *lwcompression.NoCompressor
	AllCmpr   *lwcompression.ZstdCompressor
}

func NewPreviousGTIDsCompressors() *PreviousGTIDsCompressors {
	return &PreviousGTIDsCompressors{
		FlagsCmpr: lwcompression.NewNoCompressor2(common.DataLenFixed, 8, "previousgtids.flags", common.RtcPerfs),
		GTIDCmpr:  lwcompression.NewNoCompressor2(common.DataLenFixed, 1, "previousgtids.gtid", common.RtcPerfs),
		AllCmpr:   lwcompression.NewZstdCompressor(common.ZstdCompressor, common.DataLenVariable, 0, "previousgtids.all", common.RtcPerfs),
	}
}

type AonymousGTIDCompressors struct {
	GTIDCmpr *lwcompression.NoCompressor
	AllCmpr  *lwcompression.ZstdCompressor
}

func NewAonymousGTIDCompressors() *AonymousGTIDCompressors {
	return &AonymousGTIDCompressors{
		GTIDCmpr: lwcompression.NewNoCompressor2(common.DataLenFixed, 1, "anonymousgtid.gtid", common.RtcPerfs),
		AllCmpr:  lwcompression.NewZstdCompressor(common.ZstdCompressor, common.DataLenVariable, 0, "anonymousgtid.all", common.RtcPerfs),
	}
}

type QueryEventCompressors struct {
	ThreadIdCmpr     *lwcompression.NoCompressor
	ExecTimeCmpr     *lwcompression.NoCompressor
	DbNameLenCmpr    *lwcompression.NoCompressor
	ErrorCodeCmpr    *lwcompression.NoCompressor
	StatusVarLenCmpr *lwcompression.NoCompressor
	StatusVarCmpr    *lwcompression.NoCompressor
	DbNameCmpr       *lwcompression.NoCompressor
	SqlCmpr          *lwcompression.NoCompressor
	AllCmpr          *lwcompression.ZstdCompressor
}

func NewQueryEventCompressors() *QueryEventCompressors {
	return &QueryEventCompressors{
		// ThreadIdCmpr:     lwcompression.NewNoCompressor2(common.DataLenFixed, 4),
		// ExecTimeCmpr:     lwcompression.NewNoCompressor2(common.DataLenFixed, 4),
		// DbNameLenCmpr:    lwcompression.NewNoCompressor2(common.DataLenFixed, 1),
		// ErrorCodeCmpr:    lwcompression.NewNoCompressor2(common.DataLenFixed, 2),
		// StatusVarLenCmpr: lwcompression.NewNoCompressor2(common.DataLenFixed, 2),
		// StatusVarCmpr:    lwcompression.NewNoCompressor2(common.DataLenVariable, 1),
		// DbNameCmpr:       lwcompression.NewNoCompressor2(common.DataLenVariable, 1),
		// SqlCmpr:          lwcompression.NewNoCompressor(),
		AllCmpr: lwcompression.NewZstdCompressor(common.ZstdCompressor, common.DataLenVariable, 0, "queryevent.all", common.RtcPerfs),
	}

}

type TableMapCompressors struct {
	TableIdCmpr   *lwcompression.RleCompressor
	NoUsedCmpr    *lwcompression.ZstdCompressor
	DbNameLenCmpr *lwcompression.ZstdCompressor
	DbNameCmpr    *lwcompression.ZstdCompressor
	TableInfoCmpr *lwcompression.ZstdCompressor
	AllCmpr       *lwcompression.ZstdCompressor
}

func NewTableMapCompressors() *TableMapCompressors {
	return &TableMapCompressors{
		TableIdCmpr:   lwcompression.NewRleCompressor(6, "tablemap.tableid", common.RtcPerfs),
		NoUsedCmpr:    lwcompression.NewZstdCompressor(common.ZstdCompressor, common.DataLenFixed, 2, "tablemap.noused", common.RtcPerfs),
		DbNameLenCmpr: lwcompression.NewZstdCompressor(common.ZstdCompressor, common.DataLenFixed, 1, "tablemap.dbnamelen", common.RtcPerfs),
		// DbNameCmpr:    lwcompression.NewNoCompressor2(common.DataLenVariable, 1, "tablemap.dbname", common.RtcPerfs),
		DbNameCmpr:    lwcompression.NewZstdCompressor(common.ZstdCompressor, common.DataLenVariable, 0, "tablemap.dbname", common.RtcPerfs),
		TableInfoCmpr: lwcompression.NewZstdCompressor(common.ZstdCompressor, common.DataLenVariable, 0, "tablemap.tableinfo", common.RtcPerfs),
		AllCmpr:       lwcompression.NewZstdCompressor(common.ZstdCompressor, common.DataLenVariable, 0, "tablemap.all", common.RtcPerfs),
	}
}

type WriteRowsCompressors struct {
	TableIdCmpr         *lwcompression.RleCompressor
	ReservedCmpr        *lwcompression.ZstdCompressor
	ExtraInfoLenCmpr    *lwcompression.ZstdCompressor
	ExtraInfoCmpr       *lwcompression.ZstdCompressor
	ColumnNumsCmpr      *lwcompression.ZstdCompressor
	IncludedColumnsCmpr *lwcompression.ZstdCompressor
	NullColumnsCmpr     *lwcompression.ZstdCompressor
	RowsCmpr            *lwcompression.ZstdCompressor
	Rows2Cmpr           *Row2Compressor
	CheckSumCmpr        *lwcompression.NoCompressor
	AllCmpr             *lwcompression.ZstdCompressor
}

func NewWriteRowsCompressors() *WriteRowsCompressors {
	return &WriteRowsCompressors{
		TableIdCmpr:         lwcompression.NewRleCompressor(6, "writerows.tableid", common.RtcPerfs),
		ReservedCmpr:        lwcompression.NewZstdCompressor(common.ZstdCompressor, common.DataLenFixed, 2, "writerows.reserved", common.RtcPerfs),
		ExtraInfoLenCmpr:    lwcompression.NewZstdCompressor(common.ZstdCompressor, common.DataLenFixed, 2, "writerows.extrainfolen", common.RtcPerfs),
		ExtraInfoCmpr:       lwcompression.NewZstdCompressor(common.ZstdCompressor, common.DataLenVariable, 0, "writerows.extrainfo", common.RtcPerfs),
		ColumnNumsCmpr:      lwcompression.NewZstdCompressor(common.ZstdCompressor, common.DataLenVariable, 2, "writerows.columnnums", common.RtcPerfs),
		IncludedColumnsCmpr: lwcompression.NewZstdCompressor(common.ZstdCompressor, common.DataLenVariable, 2, "writerows.includedcolumns", common.RtcPerfs),
		NullColumnsCmpr:     lwcompression.NewZstdCompressor(common.ZstdCompressor, common.DataLenVariable, 2, "writerows.nullcolumns", common.RtcPerfs),
		RowsCmpr:            lwcompression.NewZstdCompressor(common.ZstdCompressor, common.DataLenVariable, 9, "writerows.rows", common.RtcPerfs),
		Rows2Cmpr:           NewRow2Compressor(),
		CheckSumCmpr:        lwcompression.NewNoCompressor2(common.DataLenFixed, 4, "writerows.checksum", common.RtcPerfs),
		AllCmpr:             lwcompression.NewZstdCompressor(common.ZstdCompressor, common.DataLenVariable, 0, "writerows.all", common.RtcPerfs),
	}
}

type XidCompressors struct {
	XidCmpr *lwcompression.NoCompressor
	AllCmpr *lwcompression.ZstdCompressor
}

func NewXidCompressors() *XidCompressors {
	return &XidCompressors{
		XidCmpr: lwcompression.NewNoCompressor(),
		AllCmpr: lwcompression.NewZstdCompressor(common.ZstdCompressor, common.DataLenVariable, 12, "xid.all", common.RtcPerfs),
	}
}

type StopCompressors struct {
	AllCmpr *lwcompression.NoCompressor
}

func NewStopCompressors() *StopCompressors {
	return &StopCompressors{
		AllCmpr: lwcompression.NewNoCompressor(),
	}
}
