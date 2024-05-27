#!/bin/bash

# 获取当前文件夹下所有以".txt"结尾的文件名
txt_files=$(ls /home/wq/golib/test/all/oneBest/*txt)

# 遍历每个文件并将内容输出到新的文件中
for file in $txt_files
do
    # 提取文件名（不含扩展名）
    filename=$(basename "$file" .txt)

    maxtype_filename="/home/wq/golib/test/all/oneBest/max_type/$filename.maxtype"
    cat "$file" | grep maxType | awk '{printf $2 "\n"}' > "$maxtype_filename"
done
