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

type WriteFlushCloser interface {
	io.Writer
	Flusher
	io.Closer
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

type MockWriteCloserFlusher struct {
	w io.WriteCloser
}

func NewMockWriteCloserFlusher(w io.WriteCloser) *MockWriteCloserFlusher {
	return &MockWriteCloserFlusher{
		w: w,
	}
}

func (m *MockWriteCloserFlusher) Write(p []byte) (n int, err error) {
	return m.w.Write(p)
}

func (m *MockWriteCloserFlusher) Close() error {
	return m.w.Close()
}

func (m *MockWriteCloserFlusher) Flush() error {
	return nil
}
