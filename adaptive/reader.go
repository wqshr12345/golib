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

	"github.com/golang/snappy"
)

func NewReader(r io.Reader) *Reader {
	return &Reader{
		inR:   r,
		cmprR: snappy.NewReader(r),
		iBuf:  make([]byte, packageHeaderSize),
	}
}

type Reader struct {

	// the data will be transported from inR.
	inR io.Reader

	cmprR io.Reader

	err error

	//iBuf is a buffer for the incoming adaptiveencoded header.
	iBuf []byte

	// Note(wangqian):The actually decompressed data is stored in cmprR's buf.

	dataOff int

	dataLen int

	compressType byte

	timestamp int64

	// Note(wangqian): We should not use a oBuf, because here we will not do some decompress operations.We should not use memR as well, because in read logic,adaptive reader is behind the snappy reader.
}

func (r *Reader) Reset(reader io.Reader) {
	r.inR = reader
	r.cmprR = snappy.NewReader(reader)
	r.iBuf = make([]byte, packageHeaderSize)
	r.dataOff = 0
	r.dataLen = 0
	r.compressType = 0
	r.timestamp = 0
	r.err = nil

}

func (r *Reader) Read(p []byte) (n int, err error) {
	// 1. read package header to iBuf.

	// Based on the premise that a package with an adaptive encoding format must encapsulate a Snappy chunk
	if r.dataOff > r.dataLen {
		return 0, errors.New("LANDS: off large than len")
	}
	if r.dataOff == r.dataLen {
		if !r.readFull(r.iBuf, true) {
			return 0, r.err
		}
		r.compressType = r.iBuf[0]
		r.timestamp = int64(r.iBuf[1]) | int64(r.iBuf[2])<<8 | int64(r.iBuf[3])<<16 | int64(r.iBuf[4])<<24 | int64(r.iBuf[5])<<32 | int64(r.iBuf[6])<<40 | int64(r.iBuf[7])<<48 | int64(r.iBuf[8])<<56
		r.dataLen = int(r.iBuf[9]) | int(r.iBuf[10])<<8 | int(r.iBuf[11])<<16 | int(r.iBuf[12])<<24
		r.dataOff = 0
	}

	// 2. read package header to dataBuf.

	switch r.compressType {
	case compressTypeSnappy:
		if n, r.err = r.cmprR.Read(p); err != nil {
			return n, r.err
		}
		r.dataOff += n
	default:
		panic("LANDS: unsupported compression type")
	}
	return n, r.err
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
