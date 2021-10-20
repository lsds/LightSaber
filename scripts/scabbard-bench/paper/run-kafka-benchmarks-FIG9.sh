#!/bin/bash

echo "Start running scalability benchmarks"

batchSize=(524288 1048576 2097152 4194304)
iterations=(1 2 3 4 5)

path="$HOME/LightSaber/build/test/benchmarks/kafka-flink/"
#path="/tmp/tmp.0lki8nQd4R/cmake-build-debug/test/benchmarks/kafka-flink/"

if [ -n "$path" ]; then
  echo "The build path is set to $path"
else
  echo "Set the application build path. Exiting..."
  exit
fi

cd $path # If the application doesn't run from the build folder, it breaks. This happens because of the where the code files are generated.
echo $PWD

echo "CM1" >> kafka_bench_res.txt
for b in ${batchSize[@]};
do
    for it in ${iterations[@]};
    do
     ./kafka_benchmarks --disk-block-size $b --threads 4 --query 0 --latency true --disk-buffer 16 --batch-size $b --bundle-size $b >> kafka_bench_res.txt
     #./kafka_benchmarks --disk-block-size $b --threads 4 --query 0 --latency true --disk-buffer 2 --batch-size $b --bundle-size $b >> kafka_bench_res.txt
    done
done

echo "CM2" >> kafka_bench_res.txt
for b in ${batchSize[@]};
do
    bb=$((2*b))
    for it in ${iterations[@]};
    do
     ./kafka_benchmarks --disk-block-size $b --threads 10 --query 1 --latency true --disk-buffer 32 --bundle-size $bb --batch-size $bb >> kafka_bench_res.txt
     #./kafka_benchmarks --disk-block-size $b --threads 10 --query 1 --latency true --disk-buffer 4 --bundle-size $bb --batch-size $bb >> kafka_bench_res.txt
    done
done

echo "SG1" >> kafka_bench_res.txt
for b in ${batchSize[@]};
do
    for it in ${iterations[@]};
    do
     ./kafka_benchmarks --disk-block-size $b --threads 1 --query 2 --latency true --disk-buffer 32 >> kafka_bench_res.txt
     #./kafka_benchmarks --disk-block-size $b --threads 1 --query 2 --latency true --disk-buffer 2 >> kafka_bench_res.txt
    done
done

echo "SG2" >> kafka_bench_res.txt
for b in ${batchSize[@]};
do
    bb=$((8*b))
    for it in ${iterations[@]};
    do
     ./kafka_benchmarks --disk-block-size $b --threads 16 --query 3 --latency true --bundle-size $bb --batch-size $bb >> kafka_bench_res.txt
     #./kafka_benchmarks --disk-block-size $b --threads 16 --query 3 --latency true --bundle-size $bb --batch-size $bb --disk-buffer 2 >> kafka_bench_res.txt
    done
done

echo "LRB1" >> kafka_bench_res.txt
for b in ${batchSize[@]};
do
    bb=$((2*b))
    for it in ${iterations[@]};
    do
     ./kafka_benchmarks --disk-block-size $b --threads 16 --query 5 --latency true --disk-buffer 16 --batch-size $bb --bundle-size $bb >> kafka_bench_res.txt
     #./kafka_benchmarks --disk-block-size $b --threads 16 --query 5 --latency true --disk-buffer 2 --batch-size $bb --bundle-size $bb >> kafka_bench_res.txt
    done
done

echo "LRB2" >> kafka_bench_res.txt
for b in ${batchSize[@]};
do
    bb=$((8*b))
    for it in ${iterations[@]};
    do
     ./kafka_benchmarks --disk-block-size $b --threads 16 --query 6 --latency true --disk-buffer 8 --batch-size $bb --bundle-size $bb >> kafka_bench_res.txt
     #./kafka_benchmarks --disk-block-size $b --threads 16 --query 6 --latency true --disk-buffer 2 --batch-size $bb --bundle-size $bb >> kafka_bench_res.txt
    done
done

echo "YSB" >> kafka_bench_res.txt
for b in ${batchSize[@]};
do
    for it in ${iterations[@]};
    do
     ./kafka_benchmarks --disk-block-size $b --threads 16 --query 8 --latency true >> kafka_bench_res.txt
     #./kafka_benchmarks --disk-block-size $b --threads 16 --query 8 --latency true --disk-buffer 1 >> kafka_bench_res.txt
    done
done

echo "NXB5" >> kafka_bench_res.txt
for b in ${batchSize[@]};
do
    for it in ${iterations[@]};
    do
     ./kafka_benchmarks --disk-block-size $b --threads 16 --query 10 --latency true --disk-buffer 16 >> kafka_bench_res.txt
    done
done

echo "All done..."