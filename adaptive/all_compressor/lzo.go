package allcompressor

import (
	"bytes"

	"github.com/cyberdelia/lzo"
)

func NewLzoCompressor() *LzoCompressor {

	return &LzoCompressor{}
}

type LzoCompressor struct {
	encoder *lzo.Writer
}

// this will return a lzo format data.
func (l *LzoCompressor) Compress(src []byte) []byte {
	var buf bytes.Buffer
	encoder := lzo.NewWriter(&buf)
	encoder.Write(src)
	encoder.Close()
	return buf.Bytes()
}
