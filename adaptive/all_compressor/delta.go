package allcompressor

import "encoding/binary"

type DeltaCompressor struct {
	len int
}

func NewDeltaCompressor(len int) *DeltaCompressor {
	return &DeltaCompressor{
		len: len,
	}
}

func (d *DeltaCompressor) Compress(src []byte) []byte {
	var compressed []byte
	var prev int32
	for i := 0; i < len(src); i += d.len {
		val := binary.LittleEndian.Uint32(src[i : i+d.len])
		delta := int32(val) - prev
		prev = int32(val)
		compressed = append(compressed, d.encodeVarInt(delta)...)
	}
	return compressed
}

func (d *DeltaCompressor) encodeVarInt(val int32) []byte {
	var buf []byte
	for val >= 0x80 {
		buf = append(buf, byte(val)|0x80)
		val >>= 7
	}
	buf = append(buf, byte(val))
	return buf
}
