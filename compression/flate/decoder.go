package flate

import (
	"bytes"

	"compress/flate"
)

func NewDecompressor() *FlateDecompressor {
	return &FlateDecompressor{}
}

type FlateDecompressor struct {
}

func (c *FlateDecompressor) Decompress(dst, src []byte) []byte {
	r := flate.NewReader(bytes.NewReader(src))
	n, err := r.Read(dst)
	if err != nil {
		panic(err)
	}
	return dst[:n]
}
