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

	"github.com/wqshr12345/golib/compression"
	"github.com/wqshr12345/golib/compression/snappy"
	"github.com/wqshr12345/golib/compression/zstd"
)

/*
Adaptive Encoding Format：
| compress_type(1 byte) | timestamp(8 bytes) | data_length(4 bytes) | (compressed )data(data_length bytes) |
*/

// TODO(wangqian): extract the const parameter to a config file.
const (
	// magicHeader             = "LANDS"
	packageTimestampSize    = 16 // startTs + midTs
	packageCompressTypeSize = 1
	packageDataLenSize      = 4

	// maxOriginalDataSize = 65536

	// temoparily is not used, because there is no reusable oBuf in reader.
	maxCompressedDataSize = 76490

	packageHeaderSize = packageTimestampSize + packageCompressTypeSize + packageDataLenSize
)

// customized protocol writer
type Writer struct {
	// the data will be transported to outW.
	outW io.Writer

	cmprs map[uint8]compression.Compressor

	cmprType uint8

	err error

	// iBuf is a buffer for the incoming unadaptiveencoded data.If it's nil, we will not buffer the incoming data.
	iBuf []byte

	compressInfo []CompressInfo
}

func NewWriter(w io.Writer, bufSize int, cmprType uint8) *Writer {
	// if bufSize > maxOriginalDataSize {
	// 	panic("lands: the buffer is larger than the maximum of snappy's compression data.")
	// }

	cmprs := make(map[uint8]compression.Compressor)
	cmprs[CompressTypeSnappy] = snappy.NewCompressor()
	cmprs[CompressTypeZstd] = zstd.NewCompressor()

	return &Writer{
		outW:     w,
		cmprs:    cmprs,
		cmprType: cmprType,
		iBuf:     make([]byte, 0, bufSize),
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

	oBuf := make([]byte, packageHeaderSize)

	// TODO(wangqian): use a oBuf to avoid memory allocate.
	startTs := time.Now().UnixNano()
	compressedData := w.cmprs[w.cmprType].Compress(p)
	midTs := time.Now().UnixNano()
	dataLen := len(compressedData)
	compressType := w.cmprType

	oBuf[0] = compressType
	oBuf[1] = uint8(startTs >> 0)
	oBuf[2] = uint8(startTs >> 8)
	oBuf[3] = uint8(startTs >> 16)
	oBuf[4] = uint8(startTs >> 24)
	oBuf[5] = uint8(startTs >> 32)
	oBuf[6] = uint8(startTs >> 40)
	oBuf[7] = uint8(startTs >> 48)
	oBuf[8] = uint8(startTs >> 56)
	oBuf[9] = uint8(midTs >> 0)
	oBuf[10] = uint8(midTs >> 8)
	oBuf[11] = uint8(midTs >> 16)
	oBuf[12] = uint8(midTs >> 24)
	oBuf[13] = uint8(midTs >> 32)
	oBuf[14] = uint8(midTs >> 40)
	oBuf[15] = uint8(midTs >> 48)
	oBuf[16] = uint8(midTs >> 56)
	oBuf[17] = uint8(dataLen >> 0)
	oBuf[18] = uint8(dataLen >> 8)
	oBuf[19] = uint8(dataLen >> 16)
	oBuf[20] = uint8(dataLen >> 24)

	if _, err := w.outW.Write(oBuf); err != nil {
		w.err = err
		return nRet, err
	}

	if _, err := w.outW.Write(compressedData); err != nil {
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

func (w *Writer) Report(info CompressInfo) error {
	// TODO(wangqian):Do more things.
	w.compressInfo = append(w.compressInfo, info)
	return nil
}

func (w *Writer) Close() error {
	w.Flush()
	ret := w.err
	if w.err == nil {
		w.err = errors.New("LANDS: Writer is closed")

	}
	return ret
}
