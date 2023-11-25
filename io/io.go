// Copyright 2017 fatedier, fatedier@gmail.com
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io

import (
	"io"
	"sync"

	"github.com/golang/snappy"
	"github.com/wqshr12345/golib/adaptive"
	"github.com/wqshr12345/golib/adaptive3"
	"github.com/wqshr12345/golib/asyncio"
	"github.com/wqshr12345/golib/common"
	"github.com/wqshr12345/golib/crypto"
	"github.com/wqshr12345/golib/interfaces"
	"github.com/wqshr12345/golib/pool"
)

// Join two io.ReadWriteCloser and do some operations.
func Join(c1 io.ReadWriteCloser, c2 io.ReadWriteCloser) (inCount int64, outCount int64, errors []error) {
	var wait sync.WaitGroup
	recordErrs := make([]error, 2)
	pipe := func(number int, to io.ReadWriteCloser, from io.ReadWriteCloser, count *int64) {
		defer wait.Done()
		defer to.Close()
		defer from.Close()

		buf := pool.GetBuf(16 * 1024)
		defer pool.PutBuf(buf)
		*count, recordErrs[number] = io.CopyBuffer(to, from, buf)
	}

	wait.Add(2)
	go pipe(0, c1, c2, &inCount)
	go pipe(1, c2, c1, &outCount)
	wait.Wait()

	for _, e := range recordErrs {
		if e != nil {
			errors = append(errors, e)
		}
	}
	return
}

func AsyncJoin(c1 interfaces.ReadWriteCloseReportFlusher, c2 interfaces.ReadWriteCloseReportFlusher, chanSize int, timeOut int, bufSize int) (inCount int64, outCount int64, errors []error) {
	var wait sync.WaitGroup
	recordErrs := make([]error, 2)
	pipe := func(number int, to interfaces.ReadWriteCloseReportFlusher, from interfaces.ReadWriteCloseReportFlusher, count *int64) {
		defer wait.Done()
		defer to.Close()
		defer from.Close()

		asyncIo := asyncio.NewAsyncIo(from, to, chanSize, timeOut, bufSize)
		*count, recordErrs[number] = asyncIo.Copy()
	}
	wait.Add(2)
	go pipe(0, c1, c2, &inCount)
	go pipe(1, c2, c1, &outCount)
	wait.Wait()

	for _, e := range recordErrs {
		if e != nil {
			errors = append(errors, e)
		}
	}
	return
}

func WithEncryption(rwc io.ReadWriteCloser, key []byte) (io.ReadWriteCloser, error) {
	w, err := crypto.NewWriter(rwc, key)
	if err != nil {
		return nil, err
	}
	return WrapReadWriteCloser(crypto.NewReader(rwc, key), w, func() error {
		return rwc.Close()
	}), nil
}

func WithCompression(rwc io.ReadWriteCloser) io.ReadWriteCloser {
	sr := snappy.NewReader(rwc)
	sw := snappy.NewWriter(rwc)
	return WrapReadWriteCloser(sr, sw, func() error {
		err := rwc.Close()
		return err
	})
}

// WithCompressionFromPool will get snappy reader and writer from pool.
// You can recycle the snappy reader and writer by calling the returned recycle function, but it is not necessary.
func WithCompressionFromPool(rwc io.ReadWriteCloser) (out io.ReadWriteCloser, recycle func()) {
	sr := pool.GetSnappyReader(rwc)
	sw := pool.GetSnappyWriter(rwc)
	out = WrapReadWriteCloser(sr, sw, func() error {
		err := sw.Close()
		err = rwc.Close()
		return err
	})
	recycle = func() {
		pool.PutSnappyReader(sr)
		pool.PutSnappyWriter(sw)
	}
	return
}

func WithAdaptiveEncoding3(rwc io.ReadWriteCloser, compressType uint8) (out interfaces.ReadWriteCloseReportFlusher) {
	sr := adaptive3.NewReader(rwc, compressType)
	sw := adaptive3.NewWriter(rwc, compressType)
	return WrapReadWriteCloseReportFlusher3(sr, sw, func() error {
		err := sw.Close()
		err = rwc.Close()
		return err
	})
}

// use normal compression interface.
func WithAdaptiveEncoding(rwc io.ReadWriteCloser, reportFunc common.ReportFunction, bufSize int, compressType uint8) (out interfaces.ReadWriteCloseReportFlusher, recycle func()) {
	sr := adaptive.NewReader(rwc, reportFunc)
	sw := adaptive.NewWriter(rwc, bufSize, compressType)
	out = WrapReadWriteCloseReportFlusher(sr, sw, func() error {
		err := sw.Close()
		err = rwc.Close()
		return err
	})
	return
}

type ReadWriteCloser struct {
	r       io.Reader
	w       io.Writer
	closeFn func() error

	closed bool
	mu     sync.Mutex
}

// closeFn will be called only once
func WrapReadWriteCloser(r io.Reader, w io.Writer, closeFn func() error) io.ReadWriteCloser {
	return &ReadWriteCloser{
		r:       r,
		w:       w,
		closeFn: closeFn,
		closed:  false,
	}
}

func (rwc *ReadWriteCloser) Read(p []byte) (n int, err error) {
	return rwc.r.Read(p)
}

func (rwc *ReadWriteCloser) Write(p []byte) (n int, err error) {
	return rwc.w.Write(p)
}

func (rwc *ReadWriteCloser) Close() error {
	rwc.mu.Lock()
	if rwc.closed {
		rwc.mu.Unlock()
		return nil
	}
	rwc.closed = true
	rwc.mu.Unlock()

	if rwc.closeFn != nil {
		return rwc.closeFn()
	}
	return nil
}

type ReadWriteCloseReportFlusher struct {
	r       io.Reader
	w       interfaces.WriteFlusherReporter
	closeFn func() error

	closed bool
	mu     sync.Mutex
}

// closeFn will be called only once
func WrapReadWriteCloseReportFlusher(r io.Reader, w interfaces.WriteFlusherReporter, closeFn func() error) interfaces.ReadWriteCloseReportFlusher {
	return &ReadWriteCloseReportFlusher{
		r:       r,
		w:       w,
		closeFn: closeFn,
		closed:  false,
	}
}

type MockWriteFlusherReporter2 struct {
	w interfaces.WriteFlusher
}

func NewMockWriteFlusherReporter2(w interfaces.WriteFlusher) *MockWriteFlusherReporter2 {
	return &MockWriteFlusherReporter2{
		w: w,
	}
}

func (m *MockWriteFlusherReporter2) Write(p []byte) (n int, err error) {
	return m.w.Write(p)
}

func (m *MockWriteFlusherReporter2) Flush() error {
	return m.w.Flush()
}

func (m *MockWriteFlusherReporter2) Report(info common.CompressInfo) error {
	return nil
}

type MockWriteFlusherReporter struct {
	w io.Writer
}

func NewMockWriteFlusherReporter(w io.Writer) *MockWriteFlusherReporter {
	return &MockWriteFlusherReporter{
		w: w,
	}
}

func (m *MockWriteFlusherReporter) Write(p []byte) (n int, err error) {
	return m.w.Write(p)
}

func (m *MockWriteFlusherReporter) Flush() error {
	return nil
}

func (m *MockWriteFlusherReporter) Report(info common.CompressInfo) error {
	return nil
}

func WrapReadWriteCloseReportFlusher2(rwc io.ReadWriteCloser) interfaces.ReadWriteCloseReportFlusher {
	return &ReadWriteCloseReportFlusher{
		r:       rwc,
		w:       NewMockWriteFlusherReporter(rwc),
		closeFn: rwc.Close,
		closed:  false,
	}
}

func WrapReadWriteCloseReportFlusher3(r io.Reader, w interfaces.WriteFlusher, closeFn func() error) interfaces.ReadWriteCloseReportFlusher {
	return &ReadWriteCloseReportFlusher{
		r:       r,
		w:       NewMockWriteFlusherReporter2(w),
		closeFn: closeFn,
		closed:  false,
	}
}

func (rwcrf *ReadWriteCloseReportFlusher) Read(p []byte) (n int, err error) {
	return rwcrf.r.Read(p)
}

func (rwcrf *ReadWriteCloseReportFlusher) Write(p []byte) (n int, err error) {
	return rwcrf.w.Write(p)
}

func (rwcrf *ReadWriteCloseReportFlusher) Close() error {
	rwcrf.mu.Lock()
	if rwcrf.closed {
		rwcrf.mu.Unlock()
		return nil
	}
	rwcrf.closed = true
	rwcrf.mu.Unlock()

	if rwcrf.closeFn != nil {
		return rwcrf.closeFn()
	}
	return nil
}

func (rwcrf *ReadWriteCloseReportFlusher) Report(info common.CompressInfo) error {
	return rwcrf.w.Report(info)
}

func (rwcrf *ReadWriteCloseReportFlusher) Flush() error {
	return rwcrf.w.Flush()
}
