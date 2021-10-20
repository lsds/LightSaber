#!/bin/bash

echo "Start running checkpointing benchmarks"

allThreads=(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)
iterations=(1 2 3 4 5)
t12=(12)
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

echo "CM1" >> bench_res.txt
for t in ${t15[@]};
do
    for it in ${iterations[@]};
    do
     ./cluster_monitoring_checkpoints \
     --circular-size 33554432 --unbounded-size 524288 --batch-size 524288 --bundle-size 524288 \
     --query 1 --disk-block-size 65536 --latency true --checkpoint-duration 1000 --threads $t >> bench_res.txt
    done
done

echo "CM2" >> bench_res.txt
for t in ${t14[@]};
do
    for it in ${iterations[@]};
    do
     ./cluster_monitoring_checkpoints \
     --circular-size 16777216 --unbounded-size 524288 --batch-size 524288 --bundle-size 524288 \
     --query 2 --disk-block-size 131072 --latency true --checkpoint-duration 1000 --threads $t >> bench_res.txt
    done
done

echo "SG1" >> bench_res.txt
for t in ${t12[@]};
do
    for it in ${iterations[@]};
    do
     ./smartgrid_checkpoints \
     --query 1 --unbounded-size 262144 --batch-size 1048576 --circular-size 33554432 \
     --bundle-size 1048576 --slots 128 --latency true --checkpoint-compression true \
     --disk-block-size 131072 --persist-input tru --lineage true \
     --checkpoint-duration 1000 --threads $t >> bench_res.txt
#     --query 1 --unbounded-size 262144 --batch-size 524288 --circular-size 16777216 \
#     --bundle-size 524288 --slots 128 --latency true --checkpoint-compression true \
#     --disk-block-size 131072 --persist-input tru --lineage true \
#     --threads $t >> bench_res.txt
    done
done

echo "SG2" >> bench_res.txt
for t in ${t15[@]};
do
    for it in ${iterations[@]};
    do
     ./smartgrid_checkpoints \
     --query 2 --hashtable-size 512 --unbounded-size 1048576 --circular-size 16777216 \
     --bundle-size 524288  --slots 128 --batch-size 524288 --unbounded-size 4194304 \
     --disk-block-size 4194304 --create-merge true --checkpoint-duration 1000 \
      --parallel-merge true --latency true --threads $t >> bench_res.txt
    done
done

echo "SG3" >> bench_res.txt
for t in ${t15[@]};
do
    for it in ${iterations[@]};
    do
     ./smartgrid_checkpoints \
     --query 3 --hashtable-size 512 --unbounded-size 1048576 --circular-size 16777216 \
     --bundle-size 524288  --slots 128 --batch-size 524288 --unbounded-size 4194304 \
     --disk-block-size 4194304 --create-merge true  --checkpoint-duration 1000\
      --parallel-merge true --latency true --threads $t >> bench_res.txt
    done
done

echo "LRB1" >> bench_res.txt
for t in ${t15[@]};
do
    for it in ${iterations[@]};
    do
     ./linear_road_benchmark_checkpoints \
     --unbounded-size 8388608 --circular-size 16777216 \
     --batch-size 524288 --bundle-size 524288 --query 1 --hashtable-size 256 \
     --disk-block-size 16777216 --checkpoint-duration 1000 \
     --create-merge true --parallel-merge true --latency true --threads $t >> bench_res.txt
    done
done

echo "LRB2" >> bench_res.txt
for t in ${t15[@]};
do
    for it in ${iterations[@]};
    do
     ./linear_road_benchmark_checkpoints \
     --unbounded-size 16777216 --circular-size 16777216 \
     --batch-size 262144 --bundle-size 262144 --query 2 \
     --disk-block-size 8388608 --checkpoint-duration 1000 \
     --create-merge true --parallel-merge true --latency true --threads $t >> bench_res.txt
    done
done

echo "LRB3" >> bench_res.txt
for t in ${t15[@]};
do
    for it in ${iterations[@]};
    do
     ./linear_road_benchmark_checkpoints \
     --unbounded-size 16777216 --circular-size 16777216 \
     --batch-size 262144 --bundle-size 262144 --query 3 \
     --disk-block-size 8388608 --checkpoint-duration 1000 \
     --create-merge true --parallel-merge true --latency true --threads $t >> bench_res.txt
    done
done


echo "YSB" >> bench_res.txt
for t in ${t15[@]};
do
    for it in ${iterations[@]};
    do
     ./yahoo_benchmark_checkpoints \
     --circular-size 33554432 --slots 128 --batch-size 524288 --bundle-size 524288 \
     --disk-block-size 32768 --latency true --threads --checkpoint-duration 1000 $t >> bench_res.txt
    done
done

echo "NBQ5-ALL" >> bench_res.txt
for t in ${t15[@]};
do
    for it in ${iterations[@]};
    do
     ./nexmark_checkpoints --circular-size 33554432 --batch-size 1048576 --bundle-size 1048576 \
     --unbounded-size 262144 --disk-block-size 1048576 --checkpoint-compression true --persist-input tru \
     --lineage true --latency true --parallel-merge true --checkpoint-duration 1000 --threads $t >> bench_res.txt
    done
done

echo "All done..."