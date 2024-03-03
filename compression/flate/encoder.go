package flate

import (
	"bytes"

	"compress/flate"
)

func NewCompressor() *FlateCompressor {
	return &FlateCompressor{}
}

type FlateCompressor struct {
}

func (c *FlateCompressor) Compress(src []byte) []byte {
	var buf bytes.Buffer
	w, _ := flate.NewWriter(&buf, flate.DefaultCompression)
	w.Write(src)
	w.Close()
	return buf.Bytes()
}
