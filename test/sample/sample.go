package main

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"path/filepath"

	allcompressor "github.com/wqshr12345/golib/adaptive/all_compressor"
	"github.com/wqshr12345/golib/common"
)

func compressAndMeasure(data []byte, compressor common.Compressor) (float64, float64) {
	// start := time.Now()
	startRusage, _ := common.GetRusage()

	compressedData := compressor.Compress(data)
	endRusage, _ := common.GetRusage()
	userTime, _ := common.CpuTimeDiff(startRusage, endRusage)
	duration := userTime.Seconds()

	compressionGain := float64(len(data)) / float64(len(compressedData))
	compressionBandwidth := float64(len(compressedData)) / (1024 * 1024) / duration // MB/s

	return compressionGain, compressionBandwidth
}

func SampleData(data []byte, size float64) []byte {
	s := int(float64(len(data)) * size)
	startIndex := rand.Intn(len(data) - s)
	return data[startIndex : startIndex+s]
}

func main() {
	dirPath := "/home/wq/parsecsv/sql" // 指定目录路径
	files, err := ioutil.ReadDir(dirPath)
	if err != nil {
		fmt.Println("Error reading directory:", err)
		return
	}

	names := []string{"lz4", "zstd", "snappy", "lzo", "gzip", "flate"}
	cmprTypes := []byte{common.LZ4, common.ZSTD, common.SNAPPY, common.LZO, common.GZIP, common.FLATE}
	chunkSize := 100 * 1024 * 1024 // 100 MB
	Corrects := make([]int, len(names))
	numSamples := 0

	for _, file := range files {
		if file.IsDir() {
			continue // 跳过目录
		}
		filePath := filepath.Join(dirPath, file.Name())
		fmt.Println("Processing file:", filePath)
		fileData, err := ioutil.ReadFile(filePath)
		if err != nil {
			fmt.Println("Error reading file:", err)
			continue
		}

		for startIndex := 0; startIndex < len(fileData); startIndex += chunkSize {
			endIndex := startIndex + chunkSize
			if endIndex > len(fileData) {
				endIndex = len(fileData)
			}
			sampleData := fileData[startIndex:endIndex]
			fmt.Println("begin")
			allCompressor := allcompressor.NewAllCompressor()

			for i, name := range names {
				fullGain, fullBandwidth := compressAndMeasure(sampleData, allCompressor.GetCompressorByType(cmprTypes[i]))
				subSampleData := SampleData(sampleData, 0.5)
				subGain, subBandwidth := compressAndMeasure(subSampleData, allCompressor.GetCompressorByType(cmprTypes[i]))

				gainError := (fullGain - subGain) / fullGain
				bandwidthError := (fullBandwidth - subBandwidth) / fullBandwidth
				if gainError < 0.05 && bandwidthError < 0.05 {
					Corrects[i]++
				}
				fmt.Printf("%s - Full Gain: %.2f, Full Bandwidth: %.2f, Sample Gain: %.2f, Sample Bandwidth: %.2f\n", name, fullGain, fullBandwidth, subGain, subBandwidth)
			}
			numSamples++
		}
		fmt.Println("Finished processing file:", filePath)
	}

	fmt.Println("Number of samples:", numSamples)
	for index, correct := range Corrects {
		averageCorrect := float64(correct) / float64(numSamples)
		fmt.Printf("Average correct for %s: %.2f\n", names[index], averageCorrect)
	}
}
