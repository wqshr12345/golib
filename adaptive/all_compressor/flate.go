package allcompressor

import (
	"bytes"
	"compress/flate"
)

func NewFlateCompressor() *FlateCompressor {

	return &FlateCompressor{}
}

type FlateCompressor struct {
	encoder *flate.Writer
}

func (f *FlateCompressor) Compress(src []byte) []byte {
	var buf bytes.Buffer
	encoder, _ := flate.NewWriter(&buf, flate.DefaultCompression)
	encoder.Write(src)
	encoder.Close()
	return buf.Bytes()
}
