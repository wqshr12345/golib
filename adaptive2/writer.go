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

package adaptive2

import (
	"fmt"
	"io"

	"github.com/wqshr12345/compress/zstd"
	"github.com/wqshr12345/golib/common"
	"github.com/wqshr12345/golib/interfaces"
)

// 使用自定义格式+流式压缩
// Compressing data in adaptive foramt(stream compression).
type Writer struct {
	// Writer2 is suitable for the stream compression.
	cmprs map[uint8]interfaces.WriterFlusher2

	cmprType uint8

	compressInfo []common.CompressInfo
}

func NewWriter(w io.Writer, cmprType uint8) *Writer {

	cmprs := make(map[uint8]interfaces.WriterFlusher2)
	// cmprs[CompressTypeSnappy] = snappy.NewCompressor()
	cmprs[common.CompressTypeZstd], _ = zstd.NewWriter(w)

	return &Writer{
		cmprs:    cmprs,
		cmprType: cmprType,
	}
}

func (w *Writer) Write(p []byte) (nRet int, errRet error) {
	nRet, cmprInfo, isNil, errRet := w.cmprs[w.cmprType].Write2(p)
	if !isNil {
		// TODO(do sth)
		fmt.Println(cmprInfo)
		w.compressInfo = append(w.compressInfo, cmprInfo)
	}
	return nRet, errRet
}

func (w *Writer) Flush() error {
	cmprInfo, err := w.cmprs[w.cmprType].Flush2()
	w.compressInfo = append(w.compressInfo, cmprInfo)
	return err
}

func (w *Writer) Report(info common.CompressInfo) error {
	// TODO(wangqian):Do more things.
	w.compressInfo = append(w.compressInfo, info)
	return nil
}

func (w *Writer) Close() error {
	w.Flush()
	return nil
}
