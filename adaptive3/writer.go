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
	"compress/flate"
	"compress/gzip"
	"compress/lzw"
	"compress/zlib"
	"io"

	"github.com/golang/snappy"
	"github.com/wqshr12345/compress/zstd"
	"github.com/wqshr12345/golib/common"
	"github.com/wqshr12345/golib/interfaces"
)

// 单纯流式压缩
// TODO(wangqian): 流式与否、是否支持buffer(即是否支持flush)都是正交的关系
func NewWriter(w io.Writer, compressType uint8) interfaces.WriteFlushCloser {
	var w2 interfaces.WriteFlushCloser
	if compressType == common.CompressTypeSnappy {
		w2 = snappy.NewBufferedWriter(w)
	}
	if compressType == common.CompressTypeZstd {
		w2, _ = zstd.NewWriter(w)
	}
	if compressType == common.CompressTypeGzip {
		w2 = gzip.NewWriter(w)
	}
	if compressType == common.CompressTypeBzip2 {
		// w2 = bzip2.NewWriter(w)
		panic("not implemented")
	}
	if compressType == common.CompressTypeFlate {
		w2, _ = flate.NewWriter(w, 3)
	}
	if compressType == common.CompressTypeZlib {
		w2 = zlib.NewWriter(w)
	}
	if compressType == common.CompressTypeLzw {
		w2 = interfaces.NewMockWriteCloserFlusher(lzw.NewWriter(w, lzw.LSB, 8))
	}
	if compressType == common.CompressTypeNone {
		panic("not implemented")
	}
	return w2
}
