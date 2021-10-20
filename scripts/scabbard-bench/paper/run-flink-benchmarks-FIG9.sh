#!/bin/bash

echo "Start running scalability benchmarks"

batchSize=(524288 1048576 2097152 4194304)
iterations=(1 2 3 4 5)
buffers=(1 2 4 8)

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

echo "YSB" >> flink_bench_res.txt
for b in ${batchSize[@]};
do
    for it in ${iterations[@]};
    do
      for buf in ${buffers[@]};
      do
       ./yahoo_benchmark_flink --disk-block-size $b --threads 16 --latency true --disk-buffer $buf --use-checkpoints true >> flink_bench_res.txt
      done
    done
done

echo "YSB-Kafka" >> flink_bench_res.txt
for b in ${batchSize[@]};
do
    for it in ${iterations[@]};
    do
      for buf in ${buffers[@]};
      do
       ./yahoo_benchmark_flink --disk-block-size $b --threads 16 --latency true --disk-buffer $buf --use-checkpoints true --use-kafka true >> flink_bench_res.txt
       done
    done
done

echo "All done..."