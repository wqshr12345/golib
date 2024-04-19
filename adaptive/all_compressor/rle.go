package allcompressor

import "bytes"

type RleCompressor struct {
	len int
}

func NewRleCompressor(len int) *RleCompressor {
	return &RleCompressor{
		len: len,
	}
}

func (r *RleCompressor) Compress(src []byte) []byte {
	var result []byte
	count := 1
	for i := r.len; i < len(src); i += r.len {
		if bytes.Equal(src[i-r.len:i], src[i:i+r.len]) {
			count++
		} else {
			result = append(result, src[i-r.len:i]...)
			result = append(result, byte(count))
			count = 1
		}
	}
	result = append(result, src[len(src)-r.len:]...)
	result = append(result, byte(count))
	return result
}
