package event

type EventData interface {
	Tags() string
}

type FormatDescriptionEventData struct {
	BinlogVersion []byte // 2 bytes
	ServerVersion []byte // 50 bytes
	CreateTime    []byte // 4 bytes
	HeaderLen     []byte // 1 byte
	All           []byte //Note(Wang Qian):We will compress all data in the event.
}

func (e FormatDescriptionEventData) Tags() string {
	return "BinlogVersion,ServerVersion,CreateTime,HeaderLen"
}

type StopEventData struct {
	All []byte //Note(Wang Qian):We will compress all data in the event.
}

func (e StopEventData) Tags() string {
	return "All"

}

type TransactionPayloadEventData struct {
	All []byte //Note(Wang Qian):We will compress all data in the event.
}

func (e TransactionPayloadEventData) Tags() string {
	return "All"
}

type PreviousGTIDsEventData struct {
	Flags []byte // 1 byte
	GTID  []byte // n bytes
	All   []byte //Note(Wang Qian):We will compress all data in the event.
}

func (e PreviousGTIDsEventData) Tags() string {
	return "Flags,GTID"
}

type AnonymousGTIDEventData struct {
	GTID []byte // n bytes
	All  []byte //Note(Wang Qian):We will compress all data in the event.
}

func (e AnonymousGTIDEventData) Tags() string {
	return "GTID"
}

type RotateEventData struct {
	Position []byte // 8 bytes noused
	NextName []byte // n bytes noused
	All      []byte //Note(Wang Qian):We will compress all data in the event.
}

func (e RotateEventData) Tags() string {
	return "Position,NextName"
}

type QueryEventData struct {
	ThreadId     []byte // 4 bytes
	ExecTime     []byte // 4 bytes
	DbNameLen    []byte // 1 byte
	ErrorCode    []byte // 2 bytes
	StatusVarLen []byte // 2 bytes
	StatusVar    []byte // StatusVarLen bytes
	DbName       []byte // DbNameLen bytes
	Sql          []byte // (EventLen - 18 - DbNameLen) bytes
	All          []byte //Note(Wang Qian):We will compress all data in the event.
}

func (e QueryEventData) Tags() string {
	return "ThreadId,ExecTime,DbNameLen,ErrorCode,StatusVarLen,StatusVar,DbName,Sql"
}

type TableMapEventData struct {
	TableId   []byte // 6 bytes
	NoUsed    []byte // 2 bytes
	DbNameLen []byte // 1 byte
	DbName    []byte // DbNameLen bytes
	TableInfo []byte
	All       []byte //Note(Wang Qian):We will compress all data in the event.
}

func (e TableMapEventData) Tags() string {
	return "TableId,NoUsed,DbNameLen,DbName"
}

type WriteRowsEventData struct {
	TableId         []byte // 6 bytes
	Reserved        []byte // 2 bytes
	ExtraInfoLen    []byte // 2 bytes
	ExtraInfo       []byte // ExtraInfoLen bytes
	ColumnNums      []byte // 1 byte
	IncludedColumns []byte // (column_nums + 7) / 8 bytes
	NullColumns     []byte // (column_nums + 7) / 8 bytes
	Rows            []byte // TODO 暂时用统一压缩...
	All             []byte //Note(Wang Qian):We will compress all data in the event.
}

func (e WriteRowsEventData) Tags() string {
	return "TableId,Reserved,ExtraInfoLen,ExtraInfo,ColumnNums,IncludedColumns,NullColumns,Rows"
}

type XidEventData struct {
	Xid []byte // 8 bytes
	All []byte //Note(Wang Qian):We will compress all data in the event.
}

func (e XidEventData) Tags() string {
	return "Xid"
}
