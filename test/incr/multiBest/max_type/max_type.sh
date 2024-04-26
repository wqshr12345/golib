#!/bin/bash

# 指定文件夹路径
input_folder="/home/wq/golib/test/incr/multiBest"

# 遍历文件夹中的所有文件
for file in "$input_folder"/*; do
    # 检查文件是否是常规文件
    if [ -f "$file" ] && [[ "$file" == *.txt ]]; then
        # 获取文件的前缀（去掉文件后缀）
        filename=$(basename "$file")
        prefix="${filename%.*}"
        
        # 指定输出文件名
        output_file="$input_folder/max_type/$prefix.maxtype"
        
        # 提取以 "max_type:" 或 "cmprType:" 开头的行并保存到输出文件中
        grep -E '^(maxType:|column:)' "$file" > "$output_file"
    fi
done
