package adaptive

import (
	"encoding/binary"
	"io"

	"github.com/wqshr12345/golib/common"
)

func NewReader(r io.Reader) *Reader { // stat *statistics.TcpStatistic
	return &Reader{
		inR:   r,
		pkgID: 0,
		// stat:  stat,
	}
}

type Reader struct {
	// the data will be transported from inR.
	inR io.Reader

	dcmpr common.Decompressor

	err error

	// buffered decompressed data.
	oBuf []byte

	// oBuf[start:] represent valid data.
	start int

	// reportFunc common.ReportFunction

	pkgID int

	// stat *statistics.TcpStatistic
}

func (r *Reader) Read(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}

	if err := r.fill(); err != nil {
		return 0, err
	}

	n := copy(p, r.oBuf[r.start:])
	r.start += n
	return n, nil
}

func (r *Reader) readFull(p []byte, allowEOF bool) (ok bool) {
	if _, r.err = io.ReadFull(r.inR, p); r.err != nil {
		if r.err == io.ErrUnexpectedEOF || (r.err == io.EOF && !allowEOF) {
			// r.err = errors.New("LANDS: corrupt input")
			panic("LANDS: corrupt input")
		}
		return false
	}
	return true
}

func (r *Reader) fill() error {
	if r.start > len(r.oBuf) {
		panic("LANDS: start > end")
	}
	if r.start < len(r.oBuf) {
		return nil
	}
	// 1. read package header.
	iBuf := make([]byte, packageHeaderSize)
	if !r.readFull(iBuf, true) {
		return r.err
	}
	compressType := iBuf[0]

	dataLen := int(binary.LittleEndian.Uint32(iBuf[17:21]))
	rawDataLen := int(binary.LittleEndian.Uint32(iBuf[21:]))

	// TODO(wangqian): Should we avoid memory allocate every times?
	compressedData := make([]byte, dataLen)

	// 2. read compressed data.
	// startTime := time.Now()

	if !r.readFull(compressedData, false) {
		return r.err
	}
	// r.stat.AddReadsBandWidth(dataLen, time.Since(startTime))

	// 3. decompress compressed data.
	r.dcmpr = NewDecompressor(compressType)

	// mid2Ts := time.Now().UnixNano()

	// Note(wangqian): 哥们儿真的服了。有的接口要求传的buflen为0，有的要求len和cap一样
	if compressType == common.CompressTypeLz4 || compressType == common.CompressTypeFlate || compressType == common.CompressTypeBrotli {
		r.oBuf = make([]byte, rawDataLen)
	} else {
		r.oBuf = make([]byte, 0, rawDataLen)
	}

	// startTime = time.Now()
	r.oBuf = r.dcmpr.Decompress(r.oBuf, compressedData)
	// r.stat.AddDecompressionBandWidth(dataLen, rawDataLen, time.Since(startTime))

	r.start = 0
	r.pkgID += 1

	return nil
}
