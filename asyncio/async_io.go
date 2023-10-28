package asyncio

import (
	"errors"
	"io"
	"sync"
	"time"

	"github.com/wqshr12345/golib/interfaces"
)

type Resource struct {
	Buffer []byte
}

func NewResource(size int) Resource {
	return Resource{
		Buffer: make([]byte, size),
	}
}

func NewResource2(buffer []byte) Resource {
	return Resource{
		Buffer: buffer,
	}
}

type AsyncIo struct {
	src        io.Reader
	dst        interfaces.WriteFlusherReporter
	bufferChan chan (Resource)

	written int64

	bufSize int

	timeOut int
	ticker  *time.Ticker
	reset   bool

	wait sync.WaitGroup
	err  error
}

func NewAsyncIo(src io.Reader, dst interfaces.WriteFlusherReporter, chanSize int, timeOut int, bufSize int) *AsyncIo {
	return &AsyncIo{
		src:        src,
		dst:        dst,
		bufferChan: make(chan Resource, chanSize),

		written: 0,

		bufSize: bufSize,

		timeOut: timeOut,
		ticker:  time.NewTicker(time.Duration(timeOut) * time.Millisecond),
		reset:   false,

		err: nil,
	}
}

func (a *AsyncIo) Copy() (int64, error) {
	a.wait.Add(2)
	go a.WriteToChan()
	go a.ReadFromChan()
	a.wait.Wait()
	return a.written, a.err
}

func (a *AsyncIo) WriteToChan() {
	defer a.wait.Done()
	for {
		// TODO(wangqian):Get buffer from pool.
		buffer := make([]byte, a.bufSize)
		nr, er := a.src.Read(buffer)
		// read bytes to resource and transport to bufferChan.
		if nr > 0 {
			resource := NewResource2(buffer[:nr])
			a.bufferChan <- resource
		}
		// read to EOF and break the loop.
		if er != nil {
			if er != io.EOF {
				a.err = er
			}
			break
		}
	}
}

func (a *AsyncIo) ReadFromChan() {
	defer a.wait.Done()
	for {
		select {
		case resource := <-a.bufferChan:

			// reset ticker.
			a.reset = true
			nw, ew := a.dst.Write(resource.Buffer)

			nr := len(resource.Buffer)
			if nw < 0 || nr < nw {
				nw = 0
				if ew == nil {
					ew = errors.New("invalid write result")
				}
			}

			a.written += int64(nw)

			if ew != nil {
				a.err = ew
				break
			}

			if nr != nw {
				a.err = io.ErrShortWrite
				break
			}
		case <-a.ticker.C:

			if a.reset {
				a.reset = false
				continue
			}

			a.dst.Flush()
		}

	}
}
