package allcompressor

import (
	"bytes"

	"github.com/ulikunitz/xz"
)

func NewXzCompressor() *XzCompressor {

	return &XzCompressor{}
}

type XzCompressor struct {
	// encoder *xz.Writer
}

// this will return a lzo format data.
func (l *XzCompressor) Compress(src []byte) []byte {
	var buf bytes.Buffer
	encoder, _ := xz.NewWriter(&buf)
	encoder.Write(src)
	encoder.Close()
	return buf.Bytes()
}
