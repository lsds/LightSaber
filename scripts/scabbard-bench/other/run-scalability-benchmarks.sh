#!/bin/bash

echo "Start running scalability benchmarks"

allThreads=(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)
iterations=(1 2 3 4 5)

path="$HOME/LightSaber/build/test/benchmarks/applicationsWithCheckpoints/"

if [ -n "$path" ]; then
  echo "The build path is set to $path"
else
  echo "Set the application build path. Exiting..."
  exit
fi

cd $path # If the application doesn't run from the build folder, it breaks. This happens because of the where the code files are generated.
echo $PWD


echo "YSB-chk" >> scal_bench_res.txt
for t in ${allThreads[@]};
do
    for it in ${iterations[@]};
    do
     ./yahoo_benchmark_checkpoints --circular-size 33554432 --slots 128 --batch-size 1048576 \
     --bundle-size 1048576 --disk-block-size 32768 --latency true --checkpoint-compression tru \
     --persist-input true --lineage true --threads $t >> scal_bench_res.txt
    done
done

echo "YSB-ALL-chk" >> scal_bench_res.txt
for t in ${allThreads[@]};
do
    for it in ${iterations[@]};
    do
     ./yahoo_benchmark_checkpoints --circular-size 33554432 --slots 128 --batch-size 1048576 \
     --bundle-size 1048576 --disk-block-size 32768 --latency true --checkpoint-compression true \
     --persist-input true --lineage true --threads $t >> scal_bench_res.txt
    done
done

echo "All done..."