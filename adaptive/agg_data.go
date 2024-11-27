package adaptive

import "fmt"

type FormatDescriptionEventDatas struct {
	totalEvents int

	BinlogVersions []byte // 2
	ServerVersions []byte // 50
	CreateTimes    []byte // 4
	HeaderLens     []byte // 1
	Alls           []byte //

	AllsOff int64
	AllsLen int64
}

type StopEventDatas struct {
	totalEvents int

	Alls []byte
}

type TransactionPayloadEventDatas struct {
	totalEvents int

	Alls []byte

	AllsOff int64
	AllsLen int64
}

type PreviousGTIDsEventDatas struct {
	totalEvents int

	Flags []byte // 1
	GTIDs []byte // n
	Alls  []byte

	AllsOff int64
	AllsLen int64
}

type AnonymousGTIDEventDatas struct {
	totalEvents int

	GTIDs []byte // n bytes
	Alls  []byte

	AllsOff int64
	AllsLen int64
}

type RotateEventDatas struct {
	totalEvents int

	Positions []byte // 8
	NextNames []byte // n
	Alls      []byte

	AllsOff int64
	AllsLen int64
}

type QueryEventDatas struct {
	totalEvents int

	ThreadIds     []byte // 4
	ExecTimes     []byte // 4
	DbNameLens    []byte // 1
	ErrorCodes    []byte // 2
	StatusVarLens []byte // 2
	StatusVars    []byte // StatusVarLen
	DbNames       []byte // DbNameLen bytes
	Sqls          []byte // EventLen - 18 - DbNameLen
	Alls          []byte

	AllsOff int64
	AllsLen int64
}

type TableMapEventDatas struct {
	totalEvents int

	TableIds   []byte // 6
	NoUseds    []byte // 2
	DbNameLens []byte // 1
	DbNames    []byte // DbNameLen
	TableInfos []byte
	Alls       []byte

	TableIdsOff int64
	TableIdsLen int64

	NoUsedsOff int64
	NoUsedsLen int64

	DbNameLensOff int64
	DbNameLensLen int64

	DbNamesOff int64
	DbNamesLen int64

	TableInfosOff int64
	TableInfosLen int64

	AllsOff int64
	AllsLen int64
}

type WriteRowsEventDatas struct {
	totalEvents int

	TableIds        []byte // 6
	Reserveds       []byte // 2
	ExtraInfoLens   []byte // 2
	ExtraInfos      []byte // ExtraInfoLen
	ColumnNums      []byte // 1
	IncludedColumns []byte // (column_nums + 7) / 8
	NullColumns     []byte // (column_nums + 7) / 8
	// Rows            []byte
	// 作为一个map，存储tableId -> [][]byte的映射。在后者中，第二个[]byte表示一个数据本身，第一个[]表示一个table中有很多列数据
	// Rows2[6][1]实质上表示TableId为6的表的第一列的所有数据
	Rows2      map[uint64]*[][]byte
	RowsOffLen map[RowKey]*OffLen
	Checksums  []byte // 4
	Alls       []byte

	TableIdsOff int64
	TableIdsLen int64

	ReservedsOff int64
	ReservedsLen int64

	ExtraInfoLensOff int64
	ExtraInfoLensLen int64

	ExtraInfosOff int64
	ExtraInfosLen int64

	ColumnNumsOff int64
	ColumnNumsLen int64

	IncludedColumnsOff int64
	IncludedColumnsLen int64

	NullColumnsOff int64
	NullColumnsLen int64

	ChecksumsOff int64
	ChecksumsLen int64

	// RowsOffLen[6][1]代表TableId为6的表的第一列的数据的压缩offset和len和type
	// RowsOffLen map[uint64]*[][]int64
}

type RowKey struct {
	TableId  int
	ColumnId int
}

type OffLen struct {
	Off int
	Len int
}

func (r RowKey) ToString() string {
	return fmt.Sprintf("%d:%d", r.TableId, r.ColumnId)
}
func ToRowKey(s string) (RowKey, error) {
	var r RowKey
	_, err := fmt.Sscanf(s, "%d:%d", &r.TableId, &r.ColumnId)
	if err != nil {
		return RowKey{}, err
	}
	return r, nil
}

type XidEventDatas struct {
	totalEvents int

	Xids []byte // 8
	Alls []byte

	AllsOff int64
	AllsLen int64
}
