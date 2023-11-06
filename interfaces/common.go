package interfaces

import (
	"io"

	"github.com/wqshr12345/golib/common"
)

type Reporter interface {
	Report(common.CompressInfo) error
}

type ReadReporter interface {
	io.Reader
	Reporter
}

type Flusher interface {
	Flush() error
}

type WriteFlusher interface {
	io.Writer
	Flusher
}

type WriteFlusherReporter interface {
	io.Writer
	Flusher
	Reporter
}

type ReadWriteCloseReportFlusher interface {
	io.Reader
	WriteFlusherReporter
	io.Closer
}

type WriterFlusher2 interface {
	Write2(p []byte) (n int, cmprInfo common.CompressInfo, isNil bool, err error)
	Flush2() (cmprInfo common.CompressInfo, err error)
}

type Reader2 interface {
	Read2(p []byte) (n int, cmprInfo common.CompressInfo, isNil bool, err error)
}
