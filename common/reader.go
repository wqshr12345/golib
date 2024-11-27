package common

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
		timer:     time.NewTimer(50 * time.Second),
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
	// buffer := make([]byte, 2)
	// r.readFull(buffer, true)

	// tag := binary.LittleEndian.Uint16(buffer)
	// if tag != 0 {
	// panic("package error")
	// }
	// 2. read package data
	for {
		r.timer.Reset(100 * time.Second)
		buffer := make([]byte, 2)
		r.readFull(buffer, true)

		tag := binary.LittleEndian.Uint16(buffer)
		if tag == 1 {
			buffer = make([]byte, 56)
			r.readFull(buffer, true)

			// oriLen := binary.LittleEndian.Uint64(buffer[0:8])
			oriLen := int(binary.LittleEndian.Uint64(buffer[0:8]))
			cmprLen := int(binary.LittleEndian.Uint64(buffer[8:16]))
			startTs := int64(binary.LittleEndian.Uint64(buffer[16:24]))
			totalEvents := int(binary.LittleEndian.Uint64(buffer[24:32]))
			tranStartTs := int64(binary.LittleEndian.Uint64(buffer[32:40]))
			netBws := int64(binary.LittleEndian.Uint64(buffer[40:48]))
			cmprThreads := int64(binary.LittleEndian.Uint64(buffer[48:56]))
			data := make([]byte, int(cmprLen))
			fmt.Println("Read Buffer")
			fmt.Println("latency: ", time.Duration(tranStartTs-startTs).Seconds()+float64(cmprLen)*float64(cmprThreads)/float64(netBws))
			fmt.Println("totalEvents: ", totalEvents)
			fmt.Println("oriLen: ", oriLen)
			fmt.Println("cmprLen: ", cmprLen)
			r.readFull(data, false)
			// TODOIMP 理论上这里应该对data中数据做解压
			r.tempData += oriLen
			if r.tempData >= r.totalData {
				fmt.Println("temp len: ", r.tempData, "all len: ", r.totalData)
				panic("read done")
			}
			// 继续读package数据
			continue
		} else if tag == 2 {
			// 读取bufferEnd
			buffer = make([]byte, 32)
			r.readFull(buffer, true)
			// startTs := int64(binary.LittleEndian.Uint64(buffer[0:8]))
			// totalEvents := int(binary.LittleEndian.Uint64(buffer[8:16]))
			oriLen := int(binary.LittleEndian.Uint64(buffer[16:24]))
			// cmprLen := int(binary.LittleEndian.Uint64(buffer[24:32]))
			// fmt.Println("Read Buffer")
			// fmt.Println("latency: ", time.Duration(time.Now().UnixNano()-startTs).Seconds())
			// fmt.Println("totalEvents: ", totalEvents)
			// fmt.Println("oriLen: ", oriLen)
			// fmt.Println("cmprLen: ", cmprLen)
			r.tempData += oriLen
			if r.tempData >= r.totalData {
				fmt.Println("temp len: ", r.tempData, "all len: ", r.totalData)
				panic("read done")
			}
			// data := make([]byte, int(cmprLen))
			// r.readFull(data, false)
			break
		}
	}
}

func (r *mockReader) Read(p []byte) (n int, err error) {
	r.fill()

	// TODO 把数据拷贝到p中
	// 	n := copy(p, r.oBuf[r.start:])
	// 	r.start += n
	// 	return n, nil
	return 0, nil
}
