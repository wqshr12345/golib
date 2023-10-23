// Copyright 2019 wqshr12345, wqshr12345@gmail.com
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

	"github.com/golang/snappy"
)

const (
	compressTypeNone   = 0x00
	compressTypeSnappy = 0x01
	compressTypeZstd   = 0x02
)

/*
Adaptive Encoding Format：
| compress_type(1 byte) | timestamp(8 bytes) | data_length(4 bytes) | (compressed )data(data_length bytes) |
*/

// TODO(wangqian): extract the const parameter to a config file.
const (
	// magicHeader             = "LANDS"
	packageTimestampSize    = 8
	packageCompressTypeSize = 1
	packageDataLenSize      = 4

	maxOriginalDataSize = 65536

	// temoparily is work only on Snappy.
	maxCompressedDataSize = 76490

	packageHeaderSize = packageTimestampSize + packageCompressTypeSize + packageDataLenSize
)

// customized protocol writer
type Writer struct {
	// the data will be transported to outW.
	outW io.Writer

	// only used to compress data, we will not transport data to w2.
	cmprW io.Writer

	err error

	// iBuf is a buffer for the incoming unadaptiveencoded data.If it's nil, we will not buffer the incoming data.
	iBuf []byte

	// oBuf is a buffer for the outgoing adaptiveencoded header.
	// Note(wangqian): The actually compressed data is stored in cmprW.
	oBuf []byte
}

func NewWriter(w io.Writer, bufSize int) *Writer {
	if bufSize > maxOriginalDataSize {
		panic("lands: the buffer is larger than the maximum of snappy's compression data.")
	}
	return &Writer{
		outW:  w,
		cmprW: snappy.NewWriter(w),
		iBuf:  make([]byte, 0, bufSize),
		oBuf:  make([]byte, packageHeaderSize),
	}
}

func (w *Writer) Write(p []byte) (nRet int, errRet error) {
	if w.iBuf == nil {
		return w.write(p)
	}
	for len(p) > (cap(w.iBuf)-len(w.iBuf)) && w.err == nil {
		var n int
		if len(w.iBuf) == 0 {
			// Large write, empty buffer.
			// Write directly from p to avoid copy.
			n, _ = w.write(p)
		} else {
			n = copy(w.iBuf[len(w.iBuf):cap(w.iBuf)], p)
			w.iBuf = w.iBuf[:len(w.iBuf)+n]
			w.Flush()
		}
		nRet += n
		p = p[n:]
	}
	if w.err != nil {
		return nRet, w.err
	}
	n := copy(w.iBuf[len(w.iBuf):cap(w.iBuf)], p)
	w.iBuf = w.iBuf[:len(w.iBuf)+n]
	nRet += n
	return nRet, nil
}

// contruct a adaptive encodeing format use datas in p.
func (w *Writer) write(p []byte) (nRet int, errRet error) {

	if w.err != nil {
		return 0, w.err
	}
	compressType := uint8(compressTypeSnappy)
	timestamp := time.Now().UnixNano()

	// Note(wangqian): We should use original data len instead of compressed data len.
	dataLen := len(p)

	w.oBuf[0] = compressType
	w.oBuf[1] = uint8(timestamp >> 0)
	w.oBuf[2] = uint8(timestamp >> 8)
	w.oBuf[3] = uint8(timestamp >> 16)
	w.oBuf[4] = uint8(timestamp >> 24)
	w.oBuf[5] = uint8(timestamp >> 32)
	w.oBuf[6] = uint8(timestamp >> 40)
	w.oBuf[7] = uint8(timestamp >> 48)
	w.oBuf[8] = uint8(timestamp >> 56)
	w.oBuf[9] = uint8(dataLen >> 0)
	w.oBuf[10] = uint8(dataLen >> 8)
	w.oBuf[11] = uint8(dataLen >> 16)
	w.oBuf[12] = uint8(dataLen >> 24)

	if _, err := w.outW.Write(w.oBuf[:packageHeaderSize]); err != nil {
		w.err = err
		return nRet, err
	}

	if _, err := w.cmprW.Write(p); err != nil {
		w.err = err
		return nRet, err
	}

	nRet = len(p)
	return nRet, nil
}

func (w *Writer) Flush() error {
	if w.err != nil {
		return w.err
	}
	if len(w.iBuf) == 0 {
		return nil
	}
	w.write(w.iBuf)
	w.iBuf = w.iBuf[:0]
	return w.err
}

func (w *Writer) Close() error {
	w.Flush()
	ret := w.err
	if w.err == nil {
		w.err = errors.New("LANDS: Writer is closed")

	}
	return ret
}
