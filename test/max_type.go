package main

// import (
// 	"bufio"
// 	"fmt"
// 	"os"
// 	"path/filepath"
// 	"strconv"
// )

// func getMaxType(dirPath string) []uint8 {

// 	// 定义一个切片数组来存储所有文件中的数字
// 	var allNumbers []uint8

// 	// 遍历指定目录下的所有文件
// 	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
// 		if err != nil {
// 			fmt.Println("Error accessing file:", path, "-", err)
// 			return err
// 		}

// 		// 检查是否是文件，而不是目录
// 		if !info.IsDir() {
// 			// 打开文件
// 			file, err := os.Open(path)
// 			if err != nil {
// 				fmt.Println("Error opening file:", path, "-", err)
// 				return err
// 			}
// 			defer file.Close()

// 			// 创建一个用于读取文件的扫描器
// 			scanner := bufio.NewScanner(file)

// 			// 遍历文件中的每一行
// 			for scanner.Scan() {
// 				line := scanner.Text()
// 				// 将字符串转换为整数
// 				number, err := strconv.Atoi(line)
// 				if err != nil {
// 					fmt.Println("Error converting line to number in file:", path, "-", err)
// 					continue
// 				}
// 				// 将数字添加到数组中
// 				allNumbers = append(allNumbers, uint8(number))
// 			}

// 			// 检查是否出现读取错误
// 			if err := scanner.Err(); err != nil {
// 				fmt.Println("Error reading file:", path, "-", err)
// 			}
// 		}
// 		// 继续遍历其他文件
// 		return nil
// 	})

// 	if err != nil {
// 		fmt.Println("Error walking through directory:", err)
// 		return allNumbers
// 	}

// 	return allNumbers
// }

// // func main() {
// //     // 指定要处理的目录路径（请根据需要更改目录路径）
// //     dirPath := "/home/wq/golib/test/maxtype"

// //     all := getMaxType(dirPath)
// //     fmt.Println("Numbers read from all files:", all)
// // }
