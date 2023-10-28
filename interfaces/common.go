package interfaces

import (
	"io"

	"github.com/wqshr12345/golib/adaptive"
)

type Reporter interface {
	Report(adaptive.CompressInfo) error
}

type ReadReporter interface {
	io.Reader
	Reporter
}

type Flusher interface {
	Flush() error
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
