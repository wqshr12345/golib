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

package adaptive3

import (
	"io"

	"compress/bzip2"
	"compress/flate"
	"compress/gzip"
	"compress/lzw"
	"compress/zlib"

	"github.com/golang/snappy"
	"github.com/wqshr12345/compress/zstd"
	"github.com/wqshr12345/golib/common"
)

func NewReader(r io.Reader, compressType uint8) io.Reader {
	var r2 io.Reader
	if compressType == common.CompressTypeSnappy {
		r2 = snappy.NewReader(r)
	}
	if compressType == common.CompressTypeZstd {
		r2, _ = zstd.NewReader(r)
	}
	if compressType == common.CompressTypeGzip {
		r2, _ = gzip.NewReader(r)
	}
	if compressType == common.CompressTypeBzip2 {
		r2 = bzip2.NewReader(r)
	}
	if compressType == common.CompressTypeFlate {
		r2 = flate.NewReader(r)
	}
	if compressType == common.CompressTypeZlib {
		r2, _ = zlib.NewReader(r)
	}
	if compressType == common.CompressTypeLzw {
		r2 = lzw.NewReader(r, lzw.LSB, 8)
	}
	if compressType == common.CompressTypeNone {
		r2 = r
	}

	return r2
}
