#!/bin/bash

echo "Start running benchmarks for LightSaber"

allThreads=(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)
iterations=(1 2 3 4 5)
t10=(10)
t14=(14)
t15=(15)

path="$HOME/LightSaber/build/test/benchmarks/applicationsWithCheckpoints/"

if [ -n "$path" ]; then
  echo "The build path is set to $path"
else
  echo "Set the application build path. Exiting..."
  exit
fi

cd $path # If the application doesn't run from the build folder, it breaks. This happens because of the where the code files are generated.
echo $PWD

echo "CM1" >> ls_bench_res.txt
for t in ${t15[@]};
do
    for it in ${iterations[@]};
    do
     ./cluster_monitoring_checkpoints \
     --circular-size 16777216 --unbounded-size 524288 --batch-size 524288 --bundle-size 524288 \
     --query 1 --latency true --threads $t >> ls_bench_res.txt
    done
done

echo "CM2" >> ls_bench_res.txt
for t in ${t14[@]}; # 8
do
    for it in ${iterations[@]};
    do
     ./cluster_monitoring_checkpoints \
     --circular-size 16777216 --unbounded-size 1048576 --batch-size 524288 --bundle-size 524288 \
     --query 2 --latency true --threads $t >> ls_bench_res.txt
    done
done

echo "SG1" >> ls_bench_res.txt
for t in ${t10[@]}; #5
do
    for it in ${iterations[@]};
    do
     ./smartgrid_checkpoints \
      --query 1 --unbounded-size 262144 --batch-size 1048576 --circular-size 33554432 \
      --bundle-size 1048576 --slots 128 --latency true --threads $t >> ls_bench_res.txt
#     --query 1 --unbounded-size 262144 --batch-size 524288 --circular-size 16777216 \
#     --bundle-size 524288 --slots 128 --latency true --threads $t >> ls_bench_res.txt
    done
done

echo "SG2" >> ls_bench_res.txt
for t in ${t15[@]};
do
    for it in ${iterations[@]};
    do
     ./smartgrid_checkpoints \
     --query 2 --hashtable-size 512 --unbounded-size 1048576 --circular-size 16777216 \
     --bundle-size 524288  --slots 128 --batch-size 524288 --unbounded-size 4194304 \
     --create-merge true --parallel-merge true --latency true --threads $t >> ls_bench_res.txt
    done
done

# increase the circular-size or decouple memory buffering from storage
echo "SG3" >> ls_bench_res.txt
for t in ${t15[@]};
do
    for it in ${iterations[@]};
    do
     ./smartgrid_checkpoints \
     --query 3 --hashtable-size 512 --unbounded-size 1048576 --circular-size 16777216 \
     --bundle-size 524288 --slots 128 --batch-size 524288 --unbounded-size 4194304 \
     --create-merge true --parallel-merge true --latency true --threads $t >> ls_bench_res.txt
    done
done

echo "LRB1" >> ls_bench_res.txt
for t in ${t15[@]};
do
    for it in ${iterations[@]};
    do
     ./linear_road_benchmark_checkpoints \
     --unbounded-size 8388608 --circular-size 16777216 \
     --batch-size 524288 --bundle-size 524288 --query 1 --hashtable-size 256 \
     --create-merge true --parallel-merge true --latency true --threads $t >> ls_bench_res.txt
    done
done

echo "LRB2" >> ls_bench_res.txt
for t in ${t15[@]};
do
    for it in ${iterations[@]};
    do
     ./linear_road_benchmark_checkpoints \
     --unbounded-size 16777216 --circular-size 16777216 \
     --batch-size 262144 --bundle-size 262144 --query 2 \
     --create-merge true --parallel-merge true --latency true --threads $t >> ls_bench_res.txt
    done
done

echo "LRB3" >> ls_bench_res.txt
for t in ${t14[@]};
do
    for it in ${iterations[@]};
    do
     ./linear_road_benchmark_checkpoints \
     --unbounded-size 16777216 --circular-size 16777216 \
     --batch-size 262144 --bundle-size 262144 --query 3 \
     --create-merge true --parallel-merge true --latency true --threads $t >> ls_bench_res.txt
    done
done

echo "YSB" >> ls_bench_res.txt
for t in ${t15[@]}; #10
do
    for it in ${iterations[@]};
    do
     ./yahoo_benchmark_checkpoints --circular-size 16777216 --slots 128 --batch-size 1048576 \
     --bundle-size 1048576 --latency true --threads $t >> ls_bench_res.txt
    done
done

echo "NBQ5" >> ls_bench_res.txt
for t in ${t15[@]};
do
    for it in ${iterations[@]};
    do
     ./nexmark_checkpoints --circular-size 33554432 --batch-size 1048576 --bundle-size 1048576 \
     --unbounded-size 262144 --latency true --parallel-merge true --threads $t >> ls_bench_res.txt
    done
done

echo "All done..."