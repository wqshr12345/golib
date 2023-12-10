package allcompressors

import (
	"github.com/wqshr12345/golib/compression/column/common"
	lwcompression "github.com/wqshr12345/golib/compression/column/light_wight_compression"
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
	}
}

type EventWrapperCompressors struct {
	BodyLenCmpr        *lwcompression.NoCompressor
	SequenceNumberCmpr *lwcompression.NoCompressor
	FlagCmpr           *lwcompression.NoCompressor
	AllCmpr            *lwcompression.NoCompressor
}

func NewEventWrapperCompressors() *EventWrapperCompressors {
	return &EventWrapperCompressors{
		BodyLenCmpr:        lwcompression.NewNoCompressor2(common.DataLenFixed, 3),
		SequenceNumberCmpr: lwcompression.NewNoCompressor2(common.DataLenFixed, 1),
		FlagCmpr:           lwcompression.NewNoCompressor2(common.DataLenFixed, 1),
		AllCmpr:            lwcompression.NewNoCompressor2(common.DataLenVariable, 0),
	}
}

type EventHeaderCompressors struct {
	TimestampCmpr *lwcompression.DodCompressor
	EventTypeCmpr *lwcompression.NoCompressor
	ServerIdCmpr  *lwcompression.NoCompressor
	EventLenCmpr  *lwcompression.NoCompressor
	NextPosCmpr   *lwcompression.NoCompressor
	FlagsCmpr     *lwcompression.NoCompressor
	AllCmpr       *lwcompression.NoCompressor
}

func NewEventHeaderCompressors() *EventHeaderCompressors {
	return &EventHeaderCompressors{
		// TimestampCmpr: lwcompression.NewNoCompressor2(common.DataLenFixed, 4),
		TimestampCmpr: lwcompression.NewDodCompressor(),
		EventTypeCmpr: lwcompression.NewNoCompressor2(common.DataLenFixed, 1),
		ServerIdCmpr:  lwcompression.NewNoCompressor2(common.DataLenFixed, 4),
		EventLenCmpr:  lwcompression.NewNoCompressor2(common.DataLenFixed, 4),
		NextPosCmpr:   lwcompression.NewNoCompressor2(common.DataLenFixed, 4),
		FlagsCmpr:     lwcompression.NewNoCompressor2(common.DataLenFixed, 2),
		AllCmpr:       lwcompression.NewNoCompressor2(common.DataLenFixed, 19),
	}
}

type RotateCompressors struct {
	AllCmpr *lwcompression.NoCompressor
}

func NewRotateCompressors() *RotateCompressors {
	return &RotateCompressors{
		AllCmpr: lwcompression.NewNoCompressor2(common.DataLenVariable, 0),
	}
}

type FormatDescCompressors struct {
	BinlogVersionCmpr *lwcompression.NoCompressor
	ServerVersionCmpr *lwcompression.NoCompressor
	CreateTimeCmpr    *lwcompression.NoCompressor
	HeaderLenCmpr     *lwcompression.NoCompressor
	AllCmpr           *lwcompression.NoCompressor
}

func NewFormatDescCompressors() *FormatDescCompressors {
	return &FormatDescCompressors{
		BinlogVersionCmpr: lwcompression.NewNoCompressor2(common.DataLenFixed, 2),
		ServerVersionCmpr: lwcompression.NewNoCompressor2(common.DataLenFixed, 50),
		CreateTimeCmpr:    lwcompression.NewNoCompressor2(common.DataLenFixed, 4),
		HeaderLenCmpr:     lwcompression.NewNoCompressor2(common.DataLenFixed, 1),
		AllCmpr:           lwcompression.NewNoCompressor2(common.DataLenVariable, 0),
	}
}

type TransactionPayloadCompressors struct {
	AllCmpr *lwcompression.NoCompressor
}

func NewTransactionPayloadCompressors() *TransactionPayloadCompressors {
	return &TransactionPayloadCompressors{
		AllCmpr: lwcompression.NewNoCompressor2(common.DataLenVariable, 0),
	}
}

type PreviousGTIDsCompressors struct {
	FlagsCmpr *lwcompression.NoCompressor
	GTIDCmpr  *lwcompression.NoCompressor
	AllCmpr   *lwcompression.NoCompressor
}

func NewPreviousGTIDsCompressors() *PreviousGTIDsCompressors {
	return &PreviousGTIDsCompressors{
		FlagsCmpr: lwcompression.NewNoCompressor2(common.DataLenFixed, 8),
		GTIDCmpr:  lwcompression.NewNoCompressor2(common.DataLenFixed, 1),
		AllCmpr:   lwcompression.NewNoCompressor2(common.DataLenVariable, 0),
	}
}

type AonymousGTIDCompressors struct {
	GTIDCmpr *lwcompression.NoCompressor
	AllCmpr  *lwcompression.NoCompressor
}

func NewAonymousGTIDCompressors() *AonymousGTIDCompressors {
	return &AonymousGTIDCompressors{
		GTIDCmpr: lwcompression.NewNoCompressor2(common.DataLenFixed, 1),
		AllCmpr:  lwcompression.NewNoCompressor2(common.DataLenVariable, 0),
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
	AllCmpr          *lwcompression.NoCompressor
}

func NewQueryEventCompressors() *QueryEventCompressors {
	return &QueryEventCompressors{
		ThreadIdCmpr:     lwcompression.NewNoCompressor2(common.DataLenFixed, 4),
		ExecTimeCmpr:     lwcompression.NewNoCompressor2(common.DataLenFixed, 4),
		DbNameLenCmpr:    lwcompression.NewNoCompressor2(common.DataLenFixed, 1),
		ErrorCodeCmpr:    lwcompression.NewNoCompressor2(common.DataLenFixed, 2),
		StatusVarLenCmpr: lwcompression.NewNoCompressor2(common.DataLenFixed, 2),
		StatusVarCmpr:    lwcompression.NewNoCompressor2(common.DataLenVariable, 1),
		DbNameCmpr:       lwcompression.NewNoCompressor2(common.DataLenVariable, 1),
		SqlCmpr:          lwcompression.NewNoCompressor(),
		AllCmpr:          lwcompression.NewNoCompressor2(common.DataLenVariable, 0),
	}

}

type TableMapCompressors struct {
	TableIdCmpr   *lwcompression.NoCompressor
	NoUsedCmpr    *lwcompression.NoCompressor
	DbNameLenCmpr *lwcompression.NoCompressor
	DbNameCmpr    *lwcompression.NoCompressor
	TableInfoCmpr *lwcompression.NoCompressor
	AllCmpr       *lwcompression.NoCompressor
}

func NewTableMapCompressors() *TableMapCompressors {
	return &TableMapCompressors{
		TableIdCmpr:   lwcompression.NewNoCompressor(),
		NoUsedCmpr:    lwcompression.NewNoCompressor(),
		DbNameLenCmpr: lwcompression.NewNoCompressor(),
		DbNameCmpr:    lwcompression.NewNoCompressor(),
		TableInfoCmpr: lwcompression.NewNoCompressor(),
		AllCmpr:       lwcompression.NewNoCompressor2(common.DataLenVariable, 0),
	}
}

type WriteRowsCompressors struct {
	TableIdCmpr         *lwcompression.NoCompressor
	ReservedCmpr        *lwcompression.NoCompressor
	ExtraInfoLenCmpr    *lwcompression.NoCompressor
	ExtraInfoCmpr       *lwcompression.NoCompressor
	ColumnNumsCmpr      *lwcompression.NoCompressor
	IncludedColumnsCmpr *lwcompression.NoCompressor
	NullColumnsCmpr     *lwcompression.NoCompressor
	RowsCmpr            *lwcompression.NoCompressor
	AllCmpr             *lwcompression.NoCompressor
}

func NewWriteRowsCompressors() *WriteRowsCompressors {
	return &WriteRowsCompressors{
		TableIdCmpr:         lwcompression.NewNoCompressor(),
		ReservedCmpr:        lwcompression.NewNoCompressor(),
		ExtraInfoLenCmpr:    lwcompression.NewNoCompressor(),
		ExtraInfoCmpr:       lwcompression.NewNoCompressor(),
		ColumnNumsCmpr:      lwcompression.NewNoCompressor(),
		IncludedColumnsCmpr: lwcompression.NewNoCompressor(),
		NullColumnsCmpr:     lwcompression.NewNoCompressor(),
		RowsCmpr:            lwcompression.NewNoCompressor(),
		AllCmpr:             lwcompression.NewNoCompressor2(common.DataLenVariable, 0),
	}
}

type XidCompressors struct {
	XidCmpr *lwcompression.NoCompressor
	AllCmpr *lwcompression.NoCompressor
}

func NewXidCompressors() *XidCompressors {
	return &XidCompressors{
		XidCmpr: lwcompression.NewNoCompressor(),
		AllCmpr: lwcompression.NewNoCompressor2(common.DataLenVariable, 0),
	}
}

// 生命周期仅仅和一次DeCompress()的调用相关

// type AllDecompressors struct {
// 	EventWrapperDecmpr *EventWrapperDecompressors
// 	EventHeaderDecmpr  *EventHeaderDecompressors
// 	QueryEventDecmpr   *QueryEventDecompressors
// 	TableMapDecmpr     *TableMapDecompressors
// 	WriteRowsDecmpr    *WriteRowsDecompressors
// 	XidDecmpr          *XidDecompressors
// }

// func NewAllDecompressors() *AllDecompressors {
// 	return &AllDecompressors{
// 		EventWrapperDecmpr: NewEventWrapperDecompressors(),
// 		EventHeaderDecmpr:  NewEventHeaderDecompressors(),
// 		QueryEventDecmpr:   NewQueryEventDecompressors(),
// 		TableMapDecmpr:     NewTableMapDecompressors(),
// 		WriteRowsDecmpr:    NewWriteRowsDecompressors(),
// 		XidDecmpr:          NewXidDecompressors(),
// 	}
// }

// type EventWrapperDecompressors struct {
// 	BodyLenDecmpr        *lwdecompression.NoDecompressor
// 	SequenceNumberDecmpr *lwdecompression.NoDecompressor
// 	FlagDecmpr           *lwdecompression.NoDecompressor
// }

// func NewEventWrapperDecompressors() *EventWrapperDecompressors {
// 	return &EventWrapperDecompressors{
// 		BodyLenDecmpr:        lwdecompression.NewNoDecompressor(3, common.DataLenFixed),
// 		SequenceNumberDecmpr: lwdecompression.NewNoDecompressor(1, common.DataLenFixed),
// 		FlagDecmpr:           lwdecompression.NewNoDecompressor(1, common.DataLenFixed),
// 	}
// }

// type EventHeaderDecompressors struct {
// 	TimestampDecmpr *lwdecompression.NoDecompressor
// 	EventTypeDecmpr *lwdecompression.NoDecompressor
// 	ServerIdDecmpr  *lwdecompression.NoDecompressor
// 	EventLenDecmpr  *lwdecompression.NoDecompressor
// 	NextPosDecmpr   *lwdecompression.NoDecompressor
// 	FlagsDecmpr     *lwdecompression.NoDecompressor
// }

// func NewEventHeaderDecompressors() *EventHeaderDecompressors {
// 	return &EventHeaderDecompressors{
// 		TimestampDecmpr: lwdecompression.NewNoDecompressor(4, common.DataLenFixed),
// 		EventTypeDecmpr: lwdecompression.NewNoDecompressor(1, common.DataLenFixed),
// 		ServerIdDecmpr:  lwdecompression.NewNoDecompressor(4, common.DataLenFixed),
// 		EventLenDecmpr:  lwdecompression.NewNoDecompressor(4, common.DataLenFixed),
// 		NextPosDecmpr:   lwdecompression.NewNoDecompressor(4, common.DataLenFixed),
// 		FlagsDecmpr:     lwdecompression.NewNoDecompressor(2, common.DataLenFixed),
// 	}
// }

// type QueryEventDecompressors struct {
// 	ThreadIdDecmpr     *lwdecompression.NoDecompressor
// 	ExecTimeDecmpr     *lwdecompression.NoDecompressor
// 	DbNameLenDecmpr    *lwdecompression.NoDecompressor
// 	ErrorCodeDecmpr    *lwdecompression.NoDecompressor
// 	StatusVarLenDecmpr *lwdecompression.NoDecompressor
// 	StatusVarDecmpr    *lwdecompression.NoDecompressor
// 	DbNameDecmpr       *lwdecompression.NoDecompressor
// 	SqlDecmpr          *lwdecompression.NoDecompressor
// }

// func NewQueryEventDecompressors() *QueryEventDecompressors {
// 	return &QueryEventDecompressors{
// 		ThreadIdDecmpr:     lwdecompression.NewNoDecompressor(4, common.DataLenFixed),
// 		ExecTimeDecmpr:     lwdecompression.NewNoDecompressor(4, common.DataLenFixed),
// 		DbNameLenDecmpr:    lwdecompression.NewNoDecompressor(1, common.DataLenFixed),
// 		ErrorCodeDecmpr:    lwdecompression.NewNoDecompressor(2, common.DataLenFixed),
// 		StatusVarLenDecmpr: lwdecompression.NewNoDecompressor(2, common.DataLenFixed),
// 		// TODO(wangqian)：思考变长的解压应该怎么做...
// 		StatusVarDecmpr: lwdecompression.NewNoDecompressor(1, common.DataLenVariable),
// 		DbNameDecmpr:    lwdecompression.NewNoDecompressor(1, common.DataLenVariable),
// 		SqlDecmpr:       lwdecompression.NewNoDecompressor(1, common.DataLenVariable),
// 	}

// }

// type TableMapDecompressors struct {
// 	TableIdDecmpr   *lwdecompression.NoDecompressor
// 	NoUsedDecmpr    *lwdecompression.NoDecompressor
// 	DbNameLenDecmpr *lwdecompression.NoDecompressor
// 	DbNameDecmpr    *lwdecompression.NoDecompressor
// 	TableInfoDecmpr *lwdecompression.NoDecompressor
// }

// func NewTableMapDecompressors() *TableMapDecompressors {
// 	return &TableMapDecompressors{
// 		TableIdDecmpr:   lwdecompression.NewNoDecompressor(6, common.DataLenFixed),
// 		NoUsedDecmpr:    lwdecompression.NewNoDecompressor(2, common.DataLenFixed),
// 		DbNameLenDecmpr: lwdecompression.NewNoDecompressor(1, common.DataLenFixed),
// 		DbNameDecmpr:    lwdecompression.NewNoDecompressor(1, common.DataLenVariable),
// 		TableInfoDecmpr: lwdecompression.NewNoDecompressor(1, common.DataLenVariable),
// 	}
// }

// type WriteRowsDecompressors struct {
// 	TableIdDecmpr         *lwdecompression.NoDecompressor
// 	ReservedDecmpr        *lwdecompression.NoDecompressor
// 	ExtraInfoLenDecmpr    *lwdecompression.NoDecompressor
// 	ExtraInfoDecmpr       *lwdecompression.NoDecompressor
// 	ColumnNumsDecmpr      *lwdecompression.NoDecompressor
// 	IncludedColumnsDecmpr *lwdecompression.NoDecompressor
// 	NullColumnsDecmpr     *lwdecompression.NoDecompressor
// 	RowsDecmpr            *lwdecompression.NoDecompressor
// }

// func NewWriteRowsDecompressors() *WriteRowsDecompressors {
// 	return &WriteRowsDecompressors{
// 		TableIdDecmpr:         lwdecompression.NewNoDecompressor(6, common.DataLenFixed),
// 		ReservedDecmpr:        lwdecompression.NewNoDecompressor(2, common.DataLenFixed),
// 		ExtraInfoLenDecmpr:    lwdecompression.NewNoDecompressor(2, common.DataLenFixed),
// 		ExtraInfoDecmpr:       lwdecompression.NewNoDecompressor(1, common.DataLenVariable),
// 		ColumnNumsDecmpr:      lwdecompression.NewNoDecompressor(1, common.DataLenFixed),
// 		IncludedColumnsDecmpr: lwdecompression.NewNoDecompressor(1, common.DataLenVariable),
// 		NullColumnsDecmpr:     lwdecompression.NewNoDecompressor(1, common.DataLenVariable),
// 		RowsDecmpr:            lwdecompression.NewNoDecompressor(1, common.DataLenVariable),
// 	}
// }

// type XidDecompressors struct {
// 	XidDecmpr *lwdecompression.NoDecompressor
// }

// func NewXidDecompressors() *XidDecompressors {
// 	return &XidDecompressors{
// 		XidDecmpr: lwdecompression.NewNoDecompressor(8, common.DataLenFixed),
// 	}
// }
