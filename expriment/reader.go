package expriment

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

// 用于模拟TCP的Reader
type mockReader struct {
	reader    io.Reader
	timer     *time.Timer
	totalData int
	tempData  int
}

func NewMockReader(reader io.Reader, totalData int) *mockReader {
	return &mockReader{
		reader:    reader,
		timer:     time.NewTimer(1000 * time.Second),
		totalData: totalData,
		tempData:  0,
	}
}

func (r *mockReader) readFull(p []byte, allowEOF bool) {
	if _, err := io.ReadFull(r.reader, p); err != nil {
		if err == io.ErrUnexpectedEOF || (err == io.EOF && !allowEOF) {
			panic("read err")
		}
		// return false, err
		// panic(err)
	}
}

func (r *mockReader) Panic() {
	for {
		select {
		case <-r.timer.C:
			panic("timeout")
		}
	}
}

func (r *mockReader) fill() {
	// 1. read buffer start
	buffer := make([]byte, 2)
	r.readFull(buffer, true)

	tag := binary.LittleEndian.Uint16(buffer)
	if tag != 0 {
		panic("package error")
	}
	// 2. read package data
	for {
		buffer = make([]byte, 2)
		r.readFull(buffer, true)

		tag := binary.LittleEndian.Uint16(buffer)
		if tag == 1 {
			buffer = make([]byte, 16)
			r.readFull(buffer, true)

			// oriLen := binary.LittleEndian.Uint64(buffer[0:8])
			cmprLen := binary.LittleEndian.Uint64(buffer[8:16])
			data := make([]byte, int(cmprLen))
			r.readFull(data, false)
			// TODOIMP 理论上这里应该对data中数据做解压

			// 继续读package数据
			continue
		} else if tag == 2 {
			// 读取bufferEnd
			buffer = make([]byte, 32)
			r.readFull(buffer, true)
			startTs := int64(binary.LittleEndian.Uint64(buffer[0:8]))
			totalEvents := int(binary.LittleEndian.Uint64(buffer[8:16]))
			oriLen := int(binary.LittleEndian.Uint64(buffer[16:24]))
			cmprLen := int(binary.LittleEndian.Uint64(buffer[24:32]))
			fmt.Println("Read Buffer")
			fmt.Println("latency: ", time.Duration(time.Now().UnixNano()-startTs).Seconds())
			fmt.Println("totalEvents: ", totalEvents)
			fmt.Println("oriLen: ", oriLen)
			fmt.Println("cmprLen: ", cmprLen)
			r.tempData += cmprLen
			if r.tempData >= r.totalData {
				panic("read done")
			}
			break
		}
	}
}

func (r *mockReader) Read(p []byte) (n int, err error) {
	r.timer.Reset(1000 * time.Second)
	r.fill()

	// TODO 把数据拷贝到p中
	// 	n := copy(p, r.oBuf[r.start:])
	// 	r.start += n
	// 	return n, nil
	return 0, nil
}
