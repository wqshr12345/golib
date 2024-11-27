#!/bin/bash
set +e

dir="./" # output directory
cmprThreads_values=("1")
readTime_values=("100")
rate_values=("25" "20" "15" "10" "5" "1")
balance_values=("0" "0" "0" "0" "0" "0")
package_size_values=("10")
limit_values=("50")
epoch_values=("100")
incr_file_name_values=("test_data") 
full_file_name_values=("test_data") 
full_values=("false")

for full in "${full_values[@]}"; do
    for cmprThread in "${cmprThreads_values[@]}"; do
        for i in "${!rate_values[@]}"; do
            for type_name in "${type_name_values[@]}"; do
                for j in "${!package_size_values[@]}"; do
                        for epoch in "${epoch_values[@]}"; do
                            for readTime in "${readTime_values[@]}"; do
                                rate="${rate_values[$i]}"
                                balance="${balance_values[$i]}"
                                package_size="${package_size_values[$j]}"
                                limit="${limit_values[$j]}"
                                before_buffer_size=${buffer_size}
                                buffer_size=${package_size}
                                if [ "$full" = "true" ]; then
                                    for file_name in "${full_file_name_values[@]}"; do
                                        file_name_last="${file_name: -7}"
                                        output_file="${dir}all/${type_name}/${cmprThread}_${readTime}_${buffer_size}_${package_size}_${rate}_${balance}_${epoch}_${file_name_last}"
                                        if [ "$type_name" = "oneBest" ]; then
                                            # epoch=100000000
                                            # 第一轮，确定最优
                                            ob_name="wq"
                                            echo "start $output_file first"
                                            timeout 8000 ./example -cmprThread $cmprThread -readTime $readTime -bufferSize $buffer_size -packageSize $package_size -limitThreshold ${limit} -rate $rate -balance $balance -typeName $type_name -epochThreshold $epoch -fileName $file_name -obName $ob_name -isFull=$full > $output_file
                                            echo "finished $output_file first"
                                            max_type_sh="/root/wq/golib/test/all/oneBest/max_type/max_type.sh"
                                            bash $max_type_sh
                                            # # # # 第二轮，使用最优
                                            echo "start $output_file second"
                                            ob_name="/root/wq/golib/test/all/oneBest/max_type/${cmprThread}_${readTime}_${buffer_size}_${package_size}_${rate}_${balance}_${epoch}_${file_name_last}.maxtype"
                                            timeout 8000 ./example -cmprThread $cmprThread -readTime $readTime -bufferSize $buffer_size -packageSize $package_size -limitThreshold ${limit} -rate $rate -balance $balance -typeName $type_name -epochThreshold $epoch -fileName $file_name -obName $ob_name -isFull=$full > $output_file
                                            echo "finished $output_file second"
                                        elif [ "$type_name" = "multiBest" ]; then
                                            # epoch=100000000
                                            # 第一轮，确定最优
                                            mb_name="wq"
                                            echo "start $output_file first"
                                            timeout 8000 ./example -cmprThread $cmprThread -readTime $readTime -bufferSize $buffer_size -packageSize $package_size -limitThreshold ${limit} -rate $rate -balance $balance -typeName $type_name -epochThreshold $epoch -fileName $file_name -mbName $mb_name -isFull=$full > $output_file
                                            echo "finished $output_file first"
                                            max_type_sh="/root/wq/golib/test/all/multiBest/max_type/max_type.sh"
                                            bash $max_type_sh
\\\                                            # 第二轮，使用最优
                                            echo "start $output_file second"
                                            # mb_name="/root/wq/golib/test/all/multiBest/max_type/${cmprThread}_${readTime}_${buffer_size}_${package_size}_${rate}_${balance}_${epoch}_${file_name_last}.maxtype"
                                            # timeout 8000 ./example -cmprThread $cmprThread -bufferSize $buffer_size -packageSize $package_size -limitThreshold ${limit} -rate $rate -balance $balance -typeName $type_name -epochThreshold $epoch -fileName $file_name -mbName $mb_name -isFull=$full > $output_file
                                            echo "finished $output_file second"
                                        else
                                            echo "start $output_file"
                                            timeout 8000 ./example -cmprThread $cmprThread -readTime $readTime -bufferSize $buffer_size -packageSize $package_size -limitThreshold ${limit} -rate $rate -balance $balance -typeName $type_name -epochThreshold $epoch -fileName $file_name -isFull=$full > $output_file
                                            echo "finished $output_file"
                                            if [ "$type_name" = "normal" ]; then
                                                cache_info_sh="/root/wq/golib/test/all/normal/cache_info/cache_info.sh"
                                                bash $cache_info_sh $output_file ${output_file%.*}.cache
                                                # TODO MLP这里随时改
                                                mv /root/wq/golib/test/all/normal/*.cache /root/wq/golib/test/cache_err/all/normal-nomlp/
                                            fi
                                        fi
                                    done
                                else
                                    for file_name in "${incr_file_name_values[@]}"; do
                                        file_name_last="${file_name: -7}"
                                        output_file="${dir}incr/${type_name}/${cmprThread}_${readTime}_${buffer_size}_${package_size}_${rate}_${balance}_${epoch}_${file_name_last}"
                                        if [ "$type_name" = "oneBest" ]; then
                                            # epoch=100000000
                                            # 第一轮，确定最优
                                            ob_name="wq"
                                            echo "start $output_file first"
                                            timeout 8000 ./example -cmprThread $cmprThread -readTime $readTime -bufferSize $buffer_size -packageSize $package_size -limitThreshold ${limit} -rate $rate -balance $balance -typeName $type_name -epochThreshold $epoch -fileName $file_name -obName $ob_name -isFull=$full > $output_file
                                            echo "finished $output_file first"
                                            # 第二轮，使用最优
                                            max_type_sh="/root/wq/golib/test/incr/oneBest/max_type/max_type.sh"
                                            bash $max_type_sh
                                            echo "start $output_file second"
                                            ob_name="/root/wq/golib/test/incr/oneBest/max_type/${cmprThread}_${readTime}_${buffer_size}_${package_size}_${rate}_${balance}_${epoch}_${file_name_last}.maxtype"
                                            timeout 8000 ./example -cmprThread $cmprThread -readTime $readTime -bufferSize $buffer_size -packageSize $package_size -limitThreshold ${limit} -rate $rate -balance $balance -typeName $type_name -epochThreshold $epoch -fileName $file_name -obName $ob_name -isFull=$full > $output_file
                                            echo "finished $output_file second"
                                        elif [ "$type_name" = "multiBest" ]; then
                                            # epoch=100000000
                                            # 第一轮，确定最优
                                            mb_name="wq"
                                            echo "start $output_file first"
                                            timeout 8000 ./example -cmprThread $cmprThread -readTime $readTime -bufferSize $buffer_size -packageSize $package_size -limitThreshold ${limit} -rate $rate -balance $balance -typeName $type_name -epochThreshold $epoch -fileName $file_name -mbName $mb_name -isFull=$full > $output_file
                                            echo "finished $output_file first"
                                            # 第二轮，使用最优
                                            max_type_sh="/root/wq/golib/test/incr/multiBest/max_type/max_type.sh"
                                            bash $max_type_sh
                                            echo "start $output_file second"
                                            mb_name="/root/wq/golib/test/incr/multiBest/max_type/${cmprThread}_${buffer_size}_${package_size}_${rate}_${balance}_${epoch}_${file_name_last}.maxtype"
                                            # timeout 8000 ./example -cmprThread $cmprThread -bufferSize $buffer_size -packageSize $package_size -limitThreshold ${limit} -rate $rate -balance $balance -typeName $type_name -epochThreshold $epoch -fileName $file_name -mbName $mb_name -isFull=$full > $output_file
                                            echo "finished $output_file second"
                                        else
                                            echo "start $output_file"
                                            timeout 8000 ./example -cmprThread $cmprThread -readTime $readTime -bufferSize $buffer_size -packageSize $package_size -limitThreshold ${limit} -rate $rate -balance $balance -typeName $type_name -epochThreshold $epoch -fileName $file_name -isFull=$full > $output_file
                                            echo "finished $output_file"
                                        fi
                                    done
                                fi
                            done
                        done
                    # done
                done
            done
        done
    done
done