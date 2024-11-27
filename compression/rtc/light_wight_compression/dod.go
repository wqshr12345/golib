package lightwightcompression

import (
	"encoding/binary"
	"time"

	"github.com/wqshr12345/golib/compression/rtc/common"
)

// 算法源自Gorrila论文。代码参考 https://github.com/keisku/gorilla/

const (
	firstDeltaBits = 32
)

type DodCompressor struct {
	dataLenType byte // compressor接收到的每个src[]是定长还是变长
	dataLen     int  // compressor接收到的每个src[]的固定长度，仅在dataLenType为定长时有效

	t      int32 // 当前timestamp
	tDelta int64 // 当前timestamp和上一个timestamp的差值
	times  int32 // 当前Compress()被调用次数

	count uint8 // 还有多少bit就可以写完一个字节

	buf []byte

	rowNumbers uint32

	name string
	perf *common.RtcPerf
}

func NewDodCompressor(name string, perf *common.RtcPerf) *DodCompressor {
	return &DodCompressor{
		// 默认压缩timestamp，所以这里 dataLenType是DataLenFixed 且 dataLen是4。
		dataLenType: common.DataLenFixed,
		dataLen:     4,

		t: 0,

		times: 0,
		count: 8,

		buf: make([]byte, 1, 1),

		rowNumbers: 0,

		name: name,

		perf: perf,
	}
}

func (d *DodCompressor) Compress(src []byte) {
	var startTime int64
	if common.Perf {
		startTime = time.Now().UnixNano()
	}
	d.rowNumbers += 1
	// 1. 获得当前timestamp
	timestamp := int32(binary.LittleEndian.Uint32(src))
	if d.times == 0 {
		// d.writeBits2(int64(timestamp), 32, true)
		d.writeBits(uint64(timestamp), 32)
		d.t = timestamp
		d.times++
	} else if d.times == 1 {
		// TODO(wangqian): delta 是否<=0？要验证吗？
		delta := int64(timestamp) - int64(d.t)
		// TODO(wangqian): writeBits改成int64，然后delta允许
		// d.writeBits(uint64(delta), firstDeltaBits)
		d.writeInt64Bits(int64(delta), firstDeltaBits)
		// d.writeBits2(delta, firstDeltaBits)

		d.t = timestamp
		d.tDelta = delta

		d.times++
	} else {
		delta := int64(timestamp) - int64(d.t)
		dod := int64(delta) - int64(d.tDelta) // delta of delta

		d.t = timestamp
		d.tDelta = delta
		// | DoD         | Header value | Value bits | Total bits |
		// |-------------|------------- |------------|------------|
		// | 0           | 0            | 0          | 1          |
		// | -63, 64     | 10           | 7          | 9          |
		// | -255, 256   | 110          | 9          | 12         |
		// | -2047, 2048 | 1110         | 12         | 16         |
		// | > 2048      | 1111         | 32         | 36         |
		switch {
		case dod == 0:
			d.writeBit(common.Zero)

		case -63 <= dod && dod <= 64:
			// 0x02 == '10'
			d.writeBits(0x02, 2)
			// d.writeBits2(dod, 6, true)
			d.writeInt64Bits(dod, 7)

		case -255 <= dod && dod <= 256:
			// 0x06 == '110'
			d.writeBits(0x06, 3)
			// d.writeBits2(dod, 8, true)
			d.writeInt64Bits(dod, 9)
		case -2047 <= dod && dod <= 2048:
			// 0x0E == '1110'
			d.writeBits(0x0E, 4)
			// d.writeBits2(dod, 11, true)
			d.writeInt64Bits(dod, 12)

		default:
			// 0x0F == '1111'
			d.writeBits(0x0F, 4)
			d.writeInt64Bits(dod, 32)
			// d.writeBits2(dod, 32, true)
		}
	}
	if common.Perf {
		d.perf.AddCpTime(d.name, time.Now().UnixNano()-startTime)
	}
}

func (d *DodCompressor) writeBit(bit common.Bit) {
	if bit {
		d.buf[len(d.buf)-1] |= 1 << (d.count - 1)
	}

	d.count--

	if d.count == 0 {
		d.buf = append(d.buf, 0)
		d.count = 8
	}

}

// writeByte writes a single byte to the stream, regardless of alignment
func (d *DodCompressor) writeByte(byt byte) {
	// Complete the last byte with the leftmost b.buffer bits from byt.
	d.buf[len(d.buf)-1] |= byt >> (8 - d.count)

	d.buf = append(d.buf, 0)
	d.buf[len(d.buf)-1] = byt << d.count

}

// writeBits writes the nbits right-most bits of u64 to the buffer in left-to-right order.
func (d *DodCompressor) writeBits(u64 uint64, nbits uint) {
	u64 <<= (64 - uint(nbits))
	for nbits >= 8 {
		byt := byte(u64 >> 56)
		d.writeByte(byt)
		u64 <<= 8
		nbits -= 8
	}

	for nbits > 0 {
		d.writeBit((u64 >> 63) == 1)
		u64 <<= 1
		nbits--
	}
}

func (d *DodCompressor) writeBits2(value int64, bits uint, haveNegative bool) {

	if haveNegative {
		// 1. 判断正负
		isNegative := value < 0
		if isNegative {
			value = -value
		}
		// 2. 写入符号位
		if isNegative {
			d.writeBit(common.One)
		} else {
			d.writeBit(common.Zero)
		}
	}
	// 3. 写入value(此时默认value为正数) TODO(wangqian)
	value2 := uint64(value) << (64 - bits)
	for bits >= 8 {
		byt := byte(value2 >> 56)
		d.writeByte(byt)
		value2 <<= 8
		bits -= 8
	}
	for bits > 0 {
		d.writeBit((value2 >> 63) == 1)
		value2 <<= 1
		bits--
	}
}

// 写入i的最低nbits位到buf中。
// 如果i是负数，那么写入的是i的补码。
func (d *DodCompressor) writeInt64Bits(i int64, nbits uint) {
	var u uint64
	if i >= 0 || nbits >= 64 {
		u = uint64(i)
	} else {
		u = uint64(1<<nbits + i)
	}
	d.writeBits(u, nbits)
}

func (d *DodCompressor) Finalize(out *[]byte, offset int) int {
	var startTime int64
	if common.Perf {
		startTime = time.Now().UnixNano()
	}

	// 1. column foramt.
	offset = d.finalizeColumnFormat(out, offset, d.dataLenType)

	// 2. compressed data.

	copy((*out)[offset:], d.buf)
	offset += len(d.buf)
	if common.Perf {
		d.perf.AddFTime(d.name, time.Now().UnixNano()-startTime)
	}
	return offset
}

func (d *DodCompressor) finalizeColumnFormat(out *[]byte, offset int, dataLenType byte) int {
	// 1. compression type.
	(*out)[offset] = common.ColumnDod
	offset += 1

	// 2. compressed data len.
	binary.LittleEndian.PutUint32((*out)[offset:offset+4], uint32(len(d.buf)))
	offset += 4

	// 3. variable or fixed.
	(*out)[offset] = dataLenType
	offset += 1

	// 4. row numbers.
	binary.LittleEndian.PutUint32((*out)[offset:offset+4], d.rowNumbers)
	offset += 4

	// 5. fixed data len.
	if dataLenType == common.DataLenFixed {
		binary.LittleEndian.PutUint32((*out)[offset:offset+4], uint32(d.dataLen))
		offset += 4
	} else {
		panic("not support yet")
	}

	return offset
}
