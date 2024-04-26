package main

import (
    "bufio"
    "fmt"
    "os"
    "path/filepath"
    "strconv"
    "strings"
)

// 定义一个结构体来存储 "cmprType:" 数据
type CmprTypeData struct {
    CmprType int
    ByteNum  int
}

// 读取文件并提取数据
func extractDataFromFile(filename string) [][]CmprTypeData {
    // 打开文件
    file, err := os.Open(filename)
    if err != nil {
        fmt.Printf("无法打开文件: %s\n", filename)
        return nil
    }
    defer file.Close()

    var data [][]CmprTypeData
    var currentMaxTypeData []CmprTypeData

    scanner := bufio.NewScanner(file)
    currentMaxTypeFound := false

    // 逐行读取文件
    for scanner.Scan() {
        line := scanner.Text()

        // 检查是否是 "maxType:" 行
        if strings.HasPrefix(line, "maxType:") {
            if currentMaxTypeFound {
                // 将当前的 "maxType:" 数据添加到结果数组中
                data = append(data, currentMaxTypeData)
                // 清空当前的 "maxType:" 数据
                currentMaxTypeData = nil
            }
            // 标记找到新的 "maxType:"
            currentMaxTypeFound = true
        } else if strings.HasPrefix(line, "cmprType:") {
            // 提取 "cmprType:" 数据
            parts := strings.Fields(line)
            if len(parts) >= 4 {
                // 将 "cmprType" 和 "byteNum" 转换为整数类型
                cmprType, err1 := strconv.Atoi(parts[1])
                byteNum, err2 := strconv.Atoi(parts[3])
                
                if err1 == nil && err2 == nil {
                    // 创建 CmprTypeData 结构体
                    cmprData := CmprTypeData{
                        CmprType: cmprType,
                        ByteNum:  byteNum,
                    }
                    
                    // 将数据添加到当前 "max_type:" 数据列表中
                    currentMaxTypeData = append(currentMaxTypeData, cmprData)
                } else {
                    fmt.Printf("解析数据出错: %s\n", line)
                }
            }
        }
    }

    // 将最后一个 "max_type:" 数据添加到结果数组中
    if currentMaxTypeFound {
        data = append(data, currentMaxTypeData)
    }

    // 检查读取文件时是否出错
    if err := scanner.Err(); err != nil {
        fmt.Printf("读取文件时出错: %s\n", filename)
    }

    return data
}

// 遍历文件夹中的所有文件并提取数据
func extractDataFromFolder(folderPath string) [][]CmprTypeData {
    var allData [][]CmprTypeData

    // 遍历文件夹中的所有文件
    err := filepath.Walk(folderPath, func(path string, info os.FileInfo, err error) error {
        if err != nil {
            fmt.Printf("无法访问路径: %s\n", path)
            return err
        }

        // 只处理以 ".txt" 为后缀的文件
        if !info.IsDir() && strings.HasSuffix(info.Name(), ".txt") {
            data := extractDataFromFile(path)
            if data != nil {
                allData = append(allData, data...)
            }
        }

        return nil
    })

    if err != nil {
        fmt.Printf("遍历文件夹时出错: %s\n", folderPath)
    }

    return allData
}

func main() {
    // 指定文件夹路径
    folderPath := "/home/wq/golib/test/all_shuffle/multiBest"

    // 提取数据
    data := extractDataFromFolder(folderPath)

    // 打印提取的数据
    for i, maxTypeData := range data {
        fmt.Printf("Max Type %d:\n", i + 1)
        for _, cmprData := range maxTypeData {
            fmt.Printf("  Cmpr Type: %d, Byte Num: %d\n", cmprData.CmprType, cmprData.ByteNum)
        }
    }
}

