#!/bin/bash
dir="/home/wq/golib/test/all_shuffle/" # output directory
cpuUsage_values=("1")

rate_values=("10*1024*1024" "500*1024*1024" "90*1024*1024" "170*1024*1024" "330*1024*1024" "410*1024*1024" "490*1024*1024")
balance_values=("3*1024*1024" "50*1024*1024" "27*1024*1024" "51*1024*1024" "99*1024*1024" "121*1024*1024" "147*1024*1024")
type_name_values=("snappy" "zstd" "lz4"  "multiBest" "oneBest") #"snappy" "zstd" "lz4"  "multiBest"
buffer_size_values=("10*1024*1024")
#binlog_name_values=( "/home/wq/parsecsv/sql/Arade_1.sql" "/home/wq/parsecsv/sql/Hatred_1.sql" "/home/wq/parsecsv/sql/TrainsUK1_4.sql" "/home/wq/parsecsv/sql/Common1" "/home/wq/parsecsv/sql/Common2" "/home/wq/parsecsv/sql/Common3" "/home/wq/parsecsv/sql/Common4" "/home/wq/parsecsv/sql/Eixo111" "/home/wq/parsecsv/sql/Eixo222" "/home/wq/parsecsv/sql/Eixo333" "/home/wq/parsecsv/sql/Eixo444" "/home/wq/parsecsv/sql/Eixo555" "/home/wq/parsecsv/sql/Medica1" "/home/wq/parsecsv/sql/Medica2") #  
binlog_name_values=("/home/wq/parsecsv/Shuffleaa" "/home/wq/parsecsv/Shuffleab" "/home/wq/parsecsv/Shuffleac" "/home/wq/parsecsv/Shufflead" "/home/wq/parsecsv/Shuffleae" "/home/wq/parsecsv/Shuffleaf" "/home/wq/parsecsv/Shuffleag" "/home/wq/parsecsv/Shuffleah" "/home/wq/parsecsv/Shuffleai" "/home/wq/parsecsv/Shuffleaj" "/home/wq/parsecsv/Shuffleak")
for cpuUsage in "${cpuUsage_values[@]}"
do
  for i in "${!rate_values[@]}"
  do
      for type_name in "${type_name_values[@]}"
      do
        for buffer_size in "${buffer_size_values[@]}"
        do
            for binlog_name in "${binlog_name_values[@]}"
            do
                rate="${rate_values[$i]}"
                balance="${balance_values[$i]}"
                # balance="${pair[1]}"
                actual_binlog_name="${binlog_name: -7}"
                output_file="${dir}${type_name}_${cpuUsage}_${buffer_size}_${rate}_${balance}_${actual_binlog_name}.txt"
                #maxtype_name="/home/wq/golib/test/maxtype/${type_name}_${cpuUsage}_${buffer_size}_${rate}_${balance}_${actual_binlog_name}.maxtype"
                maxtype_name="abc"
                echo "start $output_file"
                timeout 800 ./test_all -cpuUsage $cpuUsage -bufferSize $buffer_size -rate $rate -balance $balance -typeName $type_name -binlogName $binlog_name -obName $maxtype_name> $output_file || continue
                echo "finished $output_file"
            done
        done
    done
  done
done
