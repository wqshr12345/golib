package lightwightdecompression

import (
	"encoding/binary"
	"errors"

	"github.com/wqshr12345/golib/compression/rtc/common"
	"github.com/wqshr12345/golib/compression/rtc/event"
)

// TODO(wangqian): 抽象到common中
const (
	firstDeltaBits = 32
)

type DodDecompressor struct {
	src []byte // 存储解压前的数据...

	t      int32 // 当前timestamp
	tDelta int32 // 当前timestamp和上一个timestamp的差值

	count   uint8 // 还有多少bit就可以读完一个字节
	sOffset int   // 当前读到了src的哪个位置
	dOffset int   // 当前写到了dst的哪个位置

}

func NewDodDecompressor() *DodDecompressor {
	return &DodDecompressor{
		// 默认压缩timestamp，所以这里 dataLenType是DataLenFixed 且 dataLen是8。

		t: 0,

		//	times: 0,

		count:   8,
		sOffset: 0,
	}
}

func (d *DodDecompressor) ColumnDecompress(src []byte, dst *[]byte, metas *[]common.EventWrapperMeta, eventDataType byte, isEventType bool, dataLenType byte, dataLen int, rowNumbers int) {
	d.src = src
	// 1. 从src中解压Dod的数据
	out := d.decompress(rowNumbers, dataLen)

	// 2. 将out中的数据分列放到dst中的每一行里面
	offset := 0
	idx := 0
	for i := 0; i < len(*metas); i++ {
		if eventDataType != event.ALL_TYPES_EVENT && eventDataType != (*metas)[i].EventType {
			continue
		}
		copy((*dst)[(*metas)[i].Offset:(*metas)[i].Offset+dataLen], out[offset:offset+dataLen])
		if isEventType {
			(*metas)[i].EventType = out[offset]
		}
		offset += dataLen
		(*metas)[i].Offset += dataLen
		idx += 1
	}
}

func (d *DodDecompressor) decompress(rowNumbers int, dataLen int) []byte {
	uncmprLen := rowNumbers * dataLen

	dst := make([]byte, uncmprLen, uncmprLen)

	// 1. 读取第1个timestamp
	if rowNumbers > 0 {
		d.t = int32(d.readBits(32))
		binary.LittleEndian.PutUint32(dst[d.dOffset:d.dOffset+4], uint32(d.t))
		d.dOffset += 4

		rowNumbers -= 1
	}
	// 2. 读取第2个timestamp
	if rowNumbers > 0 {
		d.tDelta = int32(d.readBits(firstDeltaBits))
		d.t = d.t + d.tDelta
		binary.LittleEndian.PutUint32(dst[d.dOffset:d.dOffset+4], uint32(d.t))
		d.dOffset += 4

		rowNumbers -= 1
	}
	// 3. 读取第3到第n个timestamp
	i := 0
	for i < rowNumbers {
		i++
		n := d.readDodBits()
		if n == 0 {
			// do nothing
		} else {
			bits := d.readBits(n)
			// dod := d.readBits2(n, true)
			var dod int64 = int64(bits)
			// TODO(wangqian): 未来弄明白why
			if n != 32 && 1<<(n-1) < int64(bits) {
				dod = int64(bits - 1<<n)
			}
			d.tDelta += int32(dod)
		}
		d.t += d.tDelta
		binary.LittleEndian.PutUint32(dst[d.dOffset:d.dOffset+4], uint32(d.t))
		d.dOffset += 4
	}
	return dst
}

func (d *DodDecompressor) readBit() common.Bit {
	if d.count == 0 {
		d.sOffset += 1
		d.count = 8
	}
	d.count--
	return (d.src[d.sOffset]>>d.count)&1 != 0
}

// readBits constructs a uint64 with the nbits right-most bits
// read from the stream, and any other bits 0.
func (d *DodDecompressor) readByte() byte {
	if d.count == 0 {
		d.sOffset += 1

		return d.src[d.sOffset]
	}

	byt := d.src[d.sOffset]

	d.sOffset += 1

	// 将第一个字节的后 num 位左移 (8 - num) 位
	leftPart := (byt & (1<<d.count - 1)) << (8 - d.count)
	// 将第二个字节的前 (8 - num) 位右移 num 位
	rightPart := d.src[d.sOffset] >> d.count
	// 将左右两部分进行按位或操作，得到合并后的字节
	return leftPart | rightPart
}

func (d *DodDecompressor) readBits(nbits int) uint64 {
	var u uint64

	for 8 <= nbits {
		byt := d.readByte()

		u = (u << 8) | uint64(byt)
		nbits -= 8
	}

	for nbits > 0 {
		byt := d.readBit()
		u <<= 1
		if byt {
			u |= 1
		}
		nbits--
	}

	return u
}

func (d *DodDecompressor) readBits2(nbits int, haveNegative bool) int64 {
	var u int64
	// if isFull {
	// 	for 8 <= nbits {
	// 		byt := d.readByte()

	// 		u = (u << 8) | int64(byt)
	// 		nbits -= 8
	// 	}
	// 	return u
	// }
	isNegative := false
	if haveNegative {
		// 1. 读取符号位
		isNegative = d.readBit() == common.One
		// nbits -= 1
	}
	// 2. 读取value

	for 8 <= nbits {
		byt := d.readByte()

		u = (u << 8) | int64(byt)
		nbits -= 8
	}

	for nbits > 0 {
		byt := d.readBit()
		u <<= 1
		if byt {
			u |= 1
		}
		nbits--
	}
	if isNegative {
		u = -u
	}
	return u
}

func (d *DodDecompressor) readDodBits() int {
	var dod byte
	for i := 0; i < 4; i++ {
		dod <<= 1
		b := d.readBit()
		if b {
			dod |= 1
		} else {
			break
		}
	}

	switch dod {
	case 0x00: // 0
		return 0
	case 0x02: // 10
		return 7
	case 0x06: // 110
		return 9
	case 0x0E: // 1110
		return 12
	case 0x0F: // 1111
		return 32
	default:
		panic(errors.New("invalid dod"))
	}
}
