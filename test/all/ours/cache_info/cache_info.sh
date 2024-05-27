#!/bin/bash

if [ $# -ne 2 ]; then
  echo "Usage: $0 <input_filename>"
  exit 1
fi

input_file="$1"
output_file="$2"
file_length=${#input_file}
# output_file="${input_file%.*}.cache"

awk '
/^cache info/ {
  print
  found=0
}
/^cmprType/ && found < 7 {
  print
  found++
}
' "$input_file" > "$output_file"

