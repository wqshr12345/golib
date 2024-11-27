package brotil

import (
	"bytes"

	"github.com/andybalholm/brotli"
)

func NewDecompressor() *BrotliDecompressor {
	return &BrotliDecompressor{}
}

type BrotliDecompressor struct {
}

func (c *BrotliDecompressor) Decompress(dst, src []byte) []byte {
	r := brotli.NewReader(bytes.NewReader(src))
	n, err := r.Read(dst)
	if err != nil {
		panic(err)
	}
	return dst[:n]
}
