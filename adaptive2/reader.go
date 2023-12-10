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
	"io"

	"github.com/wqshr12345/compress/zstd"
	"github.com/wqshr12345/golib/common"
	"github.com/wqshr12345/golib/interfaces"
)

func NewReader(r io.Reader, reportFunc common.ReportFunction) *Reader {
	cmprs := make(map[uint8]interfaces.Reader2)
	// cmprs[CompressTypeSnappy] = snappy.NewDecompressor()
	cmprs[common.CompressTypeZstd], _ = zstd.NewReader(r)
	return &Reader{
		cmprs:      cmprs,
		reportFunc: reportFunc,
	}
}

type Reader struct {
	cmprs map[uint8]interfaces.Reader2

	reportFunc common.ReportFunction
}

func (r *Reader) Read(p []byte) (int, error) {
	n, cmprInfo, isNil, err := r.cmprs[common.CompressTypeZstd].Read2(p)
	if !isNil {
		r.reportFunc(cmprInfo)
	}
	return n, err
}
