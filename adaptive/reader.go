// Copyright 2023 wqshr12345, wqshr12345@gmail.com
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

package adaptive

import (
	"errors"
	"io"
	"time"

	"github.com/wqshr12345/golib/compression"
	"github.com/wqshr12345/golib/compression/snappy"
)

func NewReader(r io.Reader, reportFunc ReportFunction) *Reader {
	return &Reader{
		inR:        r,
		cmpr:       snappy.NewDecompressor(),
		reportFunc: reportFunc,
		pkgID:      0,
	}
}

type Reader struct {
	// the data will be transported from inR.
	inR io.Reader

	cmpr compression.Decompressor

	err error

	// buffered decompressed data.
	oBuf []byte

	// oBuf[start:] represent valid data.
	start int

	reportFunc ReportFunction

	pkgID int
}

func (r *Reader) Read(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}

	if err := r.fill(); err != nil {
		return 0, err
	}

	n := copy(p, r.oBuf[r.start:])
	r.start += n
	return n, nil
}

func (r *Reader) readFull(p []byte, allowEOF bool) (ok bool) {
	if _, r.err = io.ReadFull(r.inR, p); r.err != nil {
		if r.err == io.ErrUnexpectedEOF || (r.err == io.EOF && !allowEOF) {
			r.err = errors.New("LANDS: corrupt input")
		}
		return false
	}
	return true
}

func (r *Reader) fill() error {
	if r.start > len(r.oBuf) {
		panic("LANDS: start > end")
	}
	if r.start < len(r.oBuf) {
		return nil
	}
	// 1. read package header.
	iBuf := make([]byte, packageHeaderSize)
	if !r.readFull(iBuf, true) {
		return r.err
	}
	compressType := iBuf[0]
	startTs := int64(iBuf[1]) | int64(iBuf[2])<<8 | int64(iBuf[3])<<16 | int64(iBuf[4])<<24 | int64(iBuf[5])<<32 | int64(iBuf[6])<<40 | int64(iBuf[7])<<48 | int64(iBuf[8])<<56
	midTs := int64(iBuf[9]) | int64(iBuf[10])<<8 | int64(iBuf[11])<<16 | int64(iBuf[12])<<24 | int64(iBuf[13])<<32 | int64(iBuf[14])<<40 | int64(iBuf[15])<<48 | int64(iBuf[16])<<56
	dataLen := int(iBuf[17]) | int(iBuf[18])<<8 | int(iBuf[19])<<16 | int(iBuf[20])<<24

	compressedData := make([]byte, dataLen)

	// 2. read compressed data.
	if !r.readFull(compressedData, false) {
		return r.err
	}

	// 3. decompress compressed data.
	// TODO(wangqian): Use compressType to choose different decompressor.
	// TODO(wangqian): Should we avoid memory allocate every times?
	mid2Ts := time.Now().UnixNano()
	r.oBuf = r.cmpr.Decompress(nil, compressedData)
	endTs := time.Now().UnixNano()
	compressInfo := CompressInfo{
		PkgId:          r.pkgID,
		DataLen:        len(r.oBuf),
		CompressType:   int(compressType),
		CompressTime:   midTs - startTs,
		TranportTime:   mid2Ts - midTs,
		DecompressTime: endTs - mid2Ts,
		CompressRatio:  float64(dataLen) / float64(len(r.oBuf)),
	}
	r.reportFunc(compressInfo)
	r.start = 0
	r.pkgID += 1
	return nil
}
