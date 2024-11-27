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

func NewFlateDecompressor() *FlateDecompressor {
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
	totalLen := n
	for n != 0 {
		n, _ = r.Read(dst[totalLen:])
		// if err != nil && err != {
		// panic(err)
		// }
		totalLen += n
	}
	// r.Close()
	return dst[:totalLen]
}
