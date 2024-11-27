package event

import (
	"encoding/binary"
	"fmt"
)

type EventWrapper struct {
	BodyLen        []byte // 3 bytes
	SequenceNumber []byte // 1 byte
	Flag           []byte // 1 byte

	Event Event
}

func (e *EventWrapper) ToString() string {

	bodyLen := make([]byte, len(e.BodyLen)+1)
	copy(bodyLen, e.BodyLen)
	bodyLen[len(e.BodyLen)] = 0x00
	return "bodylen" + fmt.Sprint(binary.LittleEndian.Uint32(e.BodyLen)) + "seq num" + string(e.SequenceNumber)
}

type Event struct {
	Header EventHeader
	Data   EventData
	// Header []byte
	// Data []byte
}

type EventHeader struct {
	Timestamp []byte // 4 bytes

	EventType byte   //1 byte
	ServerId  []byte // 4 bytes
	EventLen  []byte // 4 bytes
	NextPos   []byte // 4 bytes
	Flags     []byte // 2 bytes
	AlL       []byte // 19 bytes TODO(wangqian):temporary solution
}

const (
	UNKNOWN_EVENT byte = iota
	START_EVENT_V3
	QUERY_EVENT
	STOP_EVENT
	ROTATE_EVENT
	INTVAR_EVENT
	LOAD_EVENT
	SLAVE_EVENT
	CREATE_FILE_EVENT
	APPEND_BLOCK_EVENT
	EXEC_LOAD_EVENT
	DELETE_FILE_EVENT
	NEW_LOAD_EVENT
	RAND_EVENT
	USER_VAR_EVENT
	FORMAT_DESCRIPTION_EVENT
	XID_EVENT
	BEGIN_LOAD_QUERY_EVENT
	EXECUTE_LOAD_QUERY_EVENT
	TABLE_MAP_EVENT
	WRITE_ROWS_EVENTv0
	UPDATE_ROWS_EVENTv0
	DELETE_ROWS_EVENTv0
	WRITE_ROWS_EVENTv1
	UPDATE_ROWS_EVENTv1
	DELETE_ROWS_EVENTv1
	INCIDENT_EVENT
	HEARTBEAT_EVENT
	IGNORABLE_EVENT
	ROWS_QUERY_EVENT
	WRITE_ROWS_EVENTv2
	UPDATE_ROWS_EVENTv2
	DELETE_ROWS_EVENTv2
	GTID_EVENT
	ANONYMOUS_GTID_EVENT
	PREVIOUS_GTIDS_EVENT
	TRANSACTION_CONTEXT_EVENT
	VIEW_CHANGE_EVENT
	XA_PREPARE_LOG_EVENT
	PARTIAL_UPDATE_ROWS_EVENT
	TRANSACTION_PAYLOAD_EVENT
	HEARTBEAT_LOG_EVENT_V2
)

const (
	// MariaDB event starts from 160
	MARIADB_ANNOTATE_ROWS_EVENT byte = 160 + iota
	MARIADB_BINLOG_CHECKPOINT_EVENT
	MARIADB_GTID_EVENT
	MARIADB_GTID_LIST_EVENT
	MARIADB_START_ENCRYPTION_EVENT
	MARIADB_QUERY_COMPRESSED_EVENT
	MARIADB_WRITE_ROWS_COMPRESSED_EVENT_V1
	MARIADB_UPDATE_ROWS_COMPRESSED_EVENT_V1
	MARIADB_DELETE_ROWS_COMPRESSED_EVENT_V1

	ALL_TYPES_EVENT
)
