package main

import (
	"bufio"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/wqshr12345/golib/common"
)

type CmprInfo struct {
	Gain float64
	Bw   float64
}

func parseFile(filePath string) ([]map[int]CmprInfo, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	datas := make([]map[int]CmprInfo, 0)
	cacheInfo := 0
	data := make(map[int]CmprInfo)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "cache info") {
			cacheInfo = 7
			if len(data) > 0 {
				datas = append(datas, data)
			}
			data = make(map[int]CmprInfo)
			continue
		}

		if cacheInfo > 0 {
			fields := strings.Fields(line)
			if len(fields) >= 6 {
				cmprType, _ := strconv.Atoi(fields[1])
				cmprGain, _ := strconv.ParseFloat(fields[3], 64)
				cmprBw, _ := strconv.ParseFloat(fields[5], 64)
				if cmprType == 151 || cmprGain == 0 || cmprBw == 0 {
					continue
				}
				data[cmprType] = CmprInfo{
					Gain: cmprGain,
					Bw:   cmprBw,
				}
			}
			cacheInfo--
		}
	}
	if len(data) > 0 {
		datas = append(datas, data)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return datas, nil
}

func calculateErrorsTotal(data1, data2 []map[int]CmprInfo) (map[int]float64, map[int]float64, int) {
	// var totalGainError, totalBwError float64
	times := 0
	totalMapGainError := make(map[int]float64)
	totalMapBwError := make(map[int]float64)
	for i := 0; i < len(data1); i++ {
		mapGainError, mapBwError := calculateErrors(data1[i], data2[i])
		for cmprType, gainError := range mapGainError {
			// if gainError < 0.10 {
			// totalMapGainError[cmprType] += 1
			// fmt.Println("Gain Error:", gainError)
			// }
			totalMapGainError[cmprType] += gainError
		}
		for cmprType, bwError := range mapBwError {
			// if bwError < 0.05 {
			// totalMapBwError[cmprType] += 1
			// }
			totalMapBwError[cmprType] += bwError
		}
		// totalGainError += gainError
		// totalBwError += bwError
		times++
	}

	return totalMapGainError, totalMapBwError, times
}

func calculateErrors(data1, data2 map[int]CmprInfo) (map[int]float64, map[int]float64) {
	var totalGainError, totalBwError float64
	mapGainError := make(map[int]float64)
	mapBwError := make(map[int]float64)
	for cmprType, info1 := range data1 {
		if info2, exists := data2[cmprType]; exists {
			gainError := info1.Gain - info2.Gain
			gainErrorAbs := math.Abs(gainError)
			bwError := info1.Bw - info2.Bw
			bwErrorAbs := math.Abs(bwError)
			// totalGainError += math.Pow(gainError, 2)
			totalGainError += gainErrorAbs / info1.Gain
			totalBwError += bwErrorAbs / info1.Bw
			// totalBwError += math.Pow(bwError, 2)
			// mapGainError[cmprType] = math.Pow(gainError, 2)
			// mapBwError[cmprType] = math.Pow(bwError, 2)\
			mapGainError[cmprType] = gainErrorAbs / info1.Gain
			mapBwError[cmprType] = bwErrorAbs / info1.Bw
		}
	}

	return mapGainError, mapBwError
}

func main() {
	// if len(os.Args) != 3 {
	// 	log.Fatalf("Usage: %s <file1> <file2>", os.Args[0])
	// }
	prefix1 := "/home/wq/golib/test/cache_err/all/multiBest/"
	prefix2 := "/home/wq/golib/test/cache_err/all/ours-nomlp/"
	prefix3 := "/home/wq/golib/test/cache_err/all/ours-mlp/"
	// file1 := os.Args[1]
	// file2 := os.Args[2]
	file1 := "1_10*1024*1024_10*1024*1024_64*1024*1024_0*1024*1024_100000000_ufflea"
	file2 := "1_10*1024*1024_10*1024*1024_64*1024*1024_0*1024*1024_100_ufflea"
	file3 := "1_10*1024*1024_10*1024*1024_64*1024*1024_0*1024*1024_100_ufflea"
	// file1 := "/home/wq/golib/test/cache_err/all/multiBest/1_200*1024*1024_200*1024*1024_32*1024*1024_0*1024*1024_100000000_uffleaa.cache"
	// file2 := "/home/wq/golib/test/cache_err/all/ours-nomlp/1_200*1024*1024_200*1024*1024_32*1024*1024_0*1024*1024_100000000_uffleaa.cache"
	suffix := []string{"a.cache"} //, "b.cache", "c.cache", "d.cache", "e.cache", "f.cache", "g.cache", "h.cache", "i.cache", "j.cache", "k.cache"}
	totalGainError1 := make(map[int]float64)
	totalBwError1 := make(map[int]float64)
	totalGainError2 := make(map[int]float64)
	totalBwError2 := make(map[int]float64)
	totalTimes1 := 0
	totalTimes2 := 0
	for _, suf := range suffix {
		realFile1 := prefix1 + file1 + suf
		realFile2 := prefix2 + file2 + suf
		realFile3 := prefix3 + file3 + suf
		data1, err := parseFile(realFile1)
		if err != nil {
			log.Fatalf("Error parsing file1: %v", err)
		}
		// data2和data3的第一轮都被剔除了，cache中全为0
		// 额外剔除data1的第一轮
		// data1 = data1[1:]
		data2, err := parseFile(realFile2)
		if err != nil {
			log.Fatalf("Error parsing file2: %v", err)
		}
		data3, err := parseFile(realFile3)
		if err != nil {
			log.Fatalf("Error parsing file3: %v", err)
		}
		tempGainError1, tempBwError1, tempTimes1 := calculateErrorsTotal(data1, data2)
		// totalGainError += tempGainError
		// totalBwError += tempBwError
		for cmprType, gainError := range tempGainError1 {
			totalGainError1[cmprType] += gainError
		}
		for cmprType, bwError := range tempBwError1 {
			totalBwError1[cmprType] += bwError
		}
		// fmt.Println("file", suf, "Gain Error:", tempGainError, "Bw Error:", tempBwError, "Times:", tempTimes)
		totalTimes1 += tempTimes1

		tempGainError2, tempBwError2, tempTimes2 := calculateErrorsTotal(data1, data3)
		for cmprType, gainError := range tempGainError2 {
			totalGainError2[cmprType] += gainError
		}
		for cmprType, bwError := range tempBwError2 {
			totalBwError2[cmprType] += bwError
		}
		totalTimes2 += tempTimes2
	}

	// fmt.Printf("Total Gain Error: %.6f\n", totalGainError)
	// fmt.Printf("Total Bw Error: %.6f\n", totalBwError)
	fmt.Println("Error1")

	// 对切片进行排序
	for cmprType := common.INVALID_START + 2; cmprType < common.INVALID_END; cmprType++ {
		fmt.Println("cmprType:", cmprType, "Gain Error:", math.Sqrt(totalGainError1[int(cmprType)]/float64(totalTimes1)))
	}
	// for cmprType, gainError := range totalGainError1 {
	// 	fmt.Println("cmprType:", cmprType, "Gain Error:", math.Sqrt(gainError/float64(totalTimes1)))
	// }
	for cmprType := common.INVALID_START + 2; cmprType < common.INVALID_END; cmprType++ {
		fmt.Println("cmprType:", cmprType, "Bw Error:", math.Sqrt(totalBwError1[int(cmprType)]/float64(totalTimes1)))
	}
	// for cmprType, bwError := range totalBwError1 {
	// 	fmt.Println("cmprType:", cmprType, "Bw Error:", math.Sqrt(bwError/float64(totalTimes1)))
	// }
	fmt.Println("Error2")
	for cmprType := common.INVALID_START + 2; cmprType < common.INVALID_END; cmprType++ {
		fmt.Println("cmprType:", cmprType, "Gain Error:", math.Sqrt(totalGainError2[int(cmprType)]/float64(totalTimes2)))
	}
	// for cmprType, gainError := range totalGainError2 {
	// 	fmt.Println("cmprType:", cmprType, "Gain Error:", math.Sqrt(gainError/float64(totalTimes2)))
	// }
	for cmprType := common.INVALID_START + 2; cmprType < common.INVALID_END; cmprType++ {
		fmt.Println("cmprType:", cmprType, "Bw Error:", math.Sqrt(totalBwError2[int(cmprType)]/float64(totalTimes2)))
	}
	// for cmprType, bwError := range totalBwError2 {
	// 	fmt.Println("cmprType:", cmprType, "Bw Error:", math.Sqrt(bwError/float64(totalTimes2)))
	// }
	// fmt.Println("Standard Deviation Gain Error:", math.Sqrt(totalGainError/float64(totalTimes)))
	// fmt.Println("Standard Deviation Bw Error:", math.Sqrt(totalBwError/float64(totalTimes)))
}
