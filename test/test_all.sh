#!/bin/bash
set +e

dir="/home/wq/golib/test/" # output directory
cpuUsage_values=("1")
# rate与balance绑定
rate_values=("75*1024*1024")
balance_values=("0*1024*1024")
#rate_values=("90*1024*1024" "170*1024*1024" "250*1024*1024" "330*1024*1024" "410*1024*1024" "490*1024*1024" "50*1024*1024" "10*1024*1024") # "10*1024*1024" "500*1024*1024" 
#balance_values=("27*1024*1024" "51*1024*1024" "75*1024*1024" "99*1024*1024" "121*1024*1024" "147*1024*1024" "0*1024*1024" "0*1024*1024") # "10*1024*1024" "50*1024*1024"
type_name_values=("multiBest") #ours zstd lz4 snappy

package_size_values=("10*1024*1024") #"100*1024*1024" "1*1024*1024*1024" "1*1024*1024"  "100*1024*1024" "1*1024*1024*1024" "1*1024*1024"
buffer_size_values=("6*1024*1024*1024")
limit_values=("50*1024")
#binlog_name_values=( "/home/wq/parsecsv/sql/Arade_1.sql" "/home/wq/parsecsv/sql/Hatred_1.sql" "/home/wq/parsecsv/sql/TrainsUK1_4.sql" "/home/wq/parsecsv/sql/Common1" "/home/wq/parsecsv/sql/Common2" "/home/wq/parsecsv/sql/Common3" "/home/wq/parsecsv/sql/Common4" "/home/wq/parsecsv/sql/Eixo111" "/home/wq/parsecsv/sql/Eixo222" "/home/wq/parsecsv/sql/Eixo333" "/home/wq/parsecsv/sql/Eixo444" "/home/wq/parsecsv/sql/Eixo555" "/home/wq/parsecsv/sql/Medica1" "/home/wq/parsecsv/sql/Medica2") #  
#binlog_name_values=("/home/wq/parsecsv/Shuffleaa" "/home/wq/parsecsv/Shuffleab" "/home/wq/parsecsv/Shuffleac" "/home/wq/parsecsv/Shufflead" "/home/wq/parsecsv/Shuffleae" "/home/wq/parsecsv/Shuffleaf" "/home/wq/parsecsv/Shuffleag" "/home/wq/parsecsv/Shuffleah" "/home/wq/parsecsv/Shuffleai" "/home/wq/parsecsv/Shuffleaj" "/home/wq/parsecsv/Shuffleak")
#binlog_name_values=("/home/wq/parsecsv/sql/Common1")
full_file_name_values=("/home/lluvia/go/src/github.com/go-mysql/binlog1-50.2txt" ) #"/home/wq/parsecsv/sql/Common1"
incr_file_name_values=("/home/lluvia/go/src/github.com/go-mysql/binlog1-50.2txt")
full_values=("false")

for full in "${full_values[@]}"
do
  for cpuUsage in "${cpuUsage_values[@]}"
  do
    for i in "${!rate_values[@]}"
    do
      for type_name in "${type_name_values[@]}"
      do
        for j in "${!package_size_values[@]}"
        do
          for buffer_size in "${buffer_size_values[@]}"
          do
              # rate与balance绑定
              rate="${rate_values[$i]}"
              balance="${balance_values[$i]}"

              # limit与package_size绑定
              package_size="${package_size_values[$j]}"
              limit="${limit_values[$j]}"
              before_buffer_size=${buffer_size}
              buffer_size=${package_size}
              if [ $type_name == "ours" ] && [ $full == "false" ] ; then
                buffer_size=${before_buffer_size}
              fi
              if [ $type_name == "multiBest" ] && [ $full == "false" ] ; then
                buffer_size=${before_buffer_size}
              fi
              # if [ $type_name != "ours" ] || [ $full != "false" ] ; then
              #   buffer_size=${package_size}
              # fi
              #full
              if [ $full = "true" ] ; then
                for file_name in "${full_file_name_values[@]}"
                do
                  file_name_last="${file_name: -7}"
                  output_file="${dir}all/${type_name}/${cpuUsage}_${buffer_size}_${package_size}_${rate}_${balance}_${file_name_last}.txt"
                  # echo "start $output_file"
                  # oneBest和multiBest都需要跑两轮
                  if [ $type_name = "oneBest" ] ; then
                    # 第一轮，确定最优
                    ob_name="wq"
                    echo "start $output_file first"
                    timeout 800 ./test_all -cpuUsage $cpuUsage -bufferSize $buffer_size -packageSize $package_size -limitThreshold ${limit} -rate $rate -balance $balance -typeName $type_name -fileName $file_name -obName $ob_name -isFull=$full>$output_file
                    echo "finished $output_file first"
                    # 第二轮，使用最优
                    max_type_sh="/home/wq/golib/test/all/oneBest/max_type/max_type.sh"
                    bash $max_type_sh
                    echo "start $output_file second"
                    ob_name="/home/wq/golib/test/all/oneBest/max_type/${cpuUsage}_${buffer_size}_${package_size}_${rate}_${balance}_${file_name_last}.maxtype"
                    timeout 800 ./test_all -cpuUsage $cpuUsage -bufferSize $buffer_size -packageSize $package_size -limitThreshold ${limit} -rate $rate -balance $balance -typeName $type_name -fileName $file_name -obName $ob_name -isFull=$full>$output_file
                    echo "finished $output_file second"
                  elif [ $type_name = "multiBest" ] ; then
                    # 第一轮，确定最优
                    mb_name="wq"
                    echo "start $output_file first"
                    timeout 800 ./test_all -cpuUsage $cpuUsage -bufferSize $buffer_size -packageSize $package_size -limitThreshold ${limit} -rate $rate -balance $balance -typeName $type_name -fileName $file_name -mbName $mb_name -isFull=$full>$output_file
                    echo "finished $output_file first"
                    # 第二轮，使用最优
                    max_type_sh="/home/wq/golib/test/all/multiBest/max_type/max_type.sh"
                    bash $max_type_sh
                    echo "start $output_file second"
                    mb_name="/home/wq/golib/test/all/multiBest/max_type/${cpuUsage}_${buffer_size}_${package_size}_${rate}_${balance}_${file_name_last}.maxtype"
                    timeout 800 ./test_all -cpuUsage $cpuUsage -bufferSize $buffer_size -packageSize $package_size -limitThreshold ${limit} -rate $rate -balance $balance -typeName $type_name -fileName $file_name -mbName $mb_name -isFull=$full>$output_file
                    echo "finished $output_file second"
                  else 
                    echo "start $output_file"
                    timeout 800 ./test_all -cpuUsage $cpuUsage -bufferSize $buffer_size -packageSize $package_size -limitThreshold ${limit} -rate $rate -balance $balance -typeName $type_name -fileName $file_name -isFull=$full>$output_file
                    echo "finished $output_file"
                  fi
                done
              # incr  
              else
                for file_name in "${incr_file_name_values[@]}"
                do
                  file_name_last="${file_name: -7}"
                  output_file="${dir}incr/${type_name}/${cpuUsage}_${buffer_size}_${package_size}_${rate}_${balance}_${file_name_last}.txt"
                  # echo "start $output_file"
                  # TODO 4.26 晚上
                  # oneBest和multiBest都需要跑两轮
                  if [ $type_name = "oneBest" ] ; then
                    # 第一轮，确定最优
                    ob_name="wq"
                    echo "start $output_file first"
                    timeout 800 ./test_all -cpuUsage $cpuUsage -bufferSize $buffer_size -packageSize $package_size -limitThreshold ${limit} -rate $rate -balance $balance -typeName $type_name -fileName $file_name -obName $ob_name -isFull=$full>$output_file
                    echo "finished $output_file first"
                    # 第二轮，使用最优
                    max_type_sh="/home/wq/golib/test/incr/oneBest/max_type/max_type.sh"
                    bash $max_type_sh
                    echo "start $output_file second"
                    ob_name="/home/wq/golib/test/incr/oneBest/max_type/${cpuUsage}_${buffer_size}_${package_size}_${rate}_${balance}_${file_name_last}.maxtype"
                    timeout 800 ./test_all -cpuUsage $cpuUsage -bufferSize $buffer_size -packageSize $package_size -limitThreshold ${limit} -rate $rate -balance $balance -typeName $type_name -fileName $file_name -obName $ob_name -isFull=$full>$output_file
                    echo "finished $output_file second"
                  elif [ $type_name = "multiBest" ] ; then
                    # 第一轮，确定最优
                    mb_name="wq"
                    echo "start $output_file first"
                    timeout 800 ./test_all -cpuUsage $cpuUsage -bufferSize $buffer_size -packageSize $package_size -limitThreshold ${limit} -rate $rate -balance $balance -typeName $type_name -fileName $file_name -mbName $mb_name -isFull=$full>$output_file
                    echo "finished $output_file first"
                    # 第二轮，使用最优
                    #max_type_sh="/home/wq/golib/test/incr/multiBest/max_type/max_type.sh"
                    #bash $max_type_sh
                    #echo "start $output_file second"
                    #mb_name="/home/wq/golib/test/incr/multiBest/max_type/${cpuUsage}_${buffer_size}_${package_size}_${rate}_${balance}_${file_name_last}.maxtype"
                    #timeout 800 ./test_all -cpuUsage $cpuUsage -bufferSize $buffer_size -packageSize $package_size -limitThreshold ${limit} -rate $rate -balance $balance -typeName $type_name -fileName $file_name -mbName $mb_name -isFull=$full>$output_file
                    #echo "finished $output_file second"
                  else 
                    echo "start $output_file"
                    timeout 800 ./test_all -cpuUsage $cpuUsage -bufferSize $buffer_size -packageSize $package_size -limitThreshold ${limit} -rate $rate -balance $balance -typeName $type_name -fileName $file_name -isFull=$full>$output_file
                    echo "finished $output_file"
                  fi
                done
              fi
            done
          done
      done
    done
  done
done
