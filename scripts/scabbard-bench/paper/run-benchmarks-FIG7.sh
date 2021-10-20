#!/bin/bash

echo "Start running benchmarks"

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

echo "CM1-ALL" >> scb_bench_res.txt
for t in ${t15[@]};
do
    for it in ${iterations[@]};
    do
     ./cluster_monitoring_checkpoints \
     --circular-size 33554432 --unbounded-size 524288 --batch-size 524288 --bundle-size 524288 \
     --query 1 --checkpoint-duration 1000 --disk-block-size 65536 --checkpoint-compression true \
     --persist-input true --lineage true --latency true --threads $t >> scb_bench_res.txt
    done
done

echo "CM2-ALL" >> scb_bench_res.txt
for t in ${t14[@]};
do
    for it in ${iterations[@]};
    do
     ./cluster_monitoring_checkpoints \
     --circular-size 16777216 --unbounded-size 1048576 --batch-size 524288 --bundle-size 524288 \
     --query 2 --checkpoint-duration 1000 --disk-block-size 131072 --latency true --checkpoint-compression true \
     --checkpoint-duration 1000 --persist-input true --lineage true --threads $t >> scb_bench_res.txt
    done
done

echo "SG1-ALL" >> scb_bench_res.txt
for t in ${t12[@]};
do
    for it in ${iterations[@]};
    do
     ./smartgrid_checkpoints \
     --query 1 --unbounded-size 262144 --batch-size 1048576 --circular-size 33554432 \
     --bundle-size 1048576 --slots 128 --latency true --checkpoint-compression true \
     --disk-block-size 131072 --persist-input true --lineage true \
     --checkpoint-duration 1000 --threads $t >> scb_bench_res.txt
#    --query 1 --unbounded-size 262144 --batch-size 524288 --circular-size 16777216 \
#    --bundle-size 524288 --slots 128 --latency true --checkpoint-compression true \
#    --disk-block-size 131072 --persist-input true --lineage true \
    done
done

echo "SG2-ALL" >> scb_bench_res.txt
for t in ${t15[@]};
do
    for it in ${iterations[@]};
    do
     ./smartgrid_checkpoints \
     --query 2 --hashtable-size 512 --unbounded-size 1048576 --circular-size 16777216 \
     --bundle-size 524288  --slots 128 --batch-size 524288 --unbounded-size 4194304 \
     --checkpoint-duration 1000 --disk-block-size 4194304 --create-merge true --persist-input true \
      --checkpoint-compression true --lineage true --parallel-merge true --latency true --threads $t >> scb_bench_res.txt
    done
done

# increase the circular-size or decouple memory buffering from storage
echo "SG3-ALL" >> scb_bench_res.txt
for t in ${t15[@]};
do
    for it in ${iterations[@]};
    do
     ./smartgrid_checkpoints \
     --query 3 --hashtable-size 512 --unbounded-size 1048576 --circular-size 16777216 \
     --bundle-size 524288  --slots 128 --batch-size 524288 --unbounded-size 4194304 \
     --checkpoint-duration 1000 --disk-block-size 4194304 --create-merge true --persist-input true \
     --checkpoint-compression true --lineage tru --parallel-merge true --latency true --threads $t >> scb_bench_res.txt
    done
done

#echo "LRB1-ALL" >> scb_bench_res.txt
#for t in ${t15[@]};
#do
#    for it in ${iterations[@]};
#    do
#     ./linear_road_benchmark_checkpoints \
#     --unbounded-size 8388608 --circular-size 16777216 \
#     --batch-size 524288 --bundle-size 524288 --query 1 --hashtable-size 256 \
#     --checkpoint-duration 1000 --disk-block-size 16777216 --persist-input true \
#     --checkpoint-compression tru --lineage true \
#     --create-merge true --parallel-merge true --latency true --threads $t >> scb_bench_res.txt
#    done
#done

echo "LRB1-ALL-CMP" >> scb_bench_res.txt
for t in ${t15[@]};
do
    for it in ${iterations[@]};
    do
     ./linear_road_benchmark_checkpoints \
     --unbounded-size 8388608 --circular-size 16777216 \
     --batch-size 524288 --bundle-size 524288 --query 1 --hashtable-size 256 \
     --checkpoint-duration 1000 --disk-block-size 16777216 --persist-input true \
     --checkpoint-compression true --lineage true \
     --create-merge true --parallel-merge true --latency true --threads $t >> scb_bench_res.txt
    done
done

#echo "LRB2-ALL" >> scb_bench_res.txt
#for t in ${t15[@]};
#do
#    for it in ${iterations[@]};
#    do
#     ./linear_road_benchmark_checkpoints \
#     --unbounded-size 16777216 --circular-size 16777216 \
#     --batch-size 262144 --bundle-size 262144 --query 2 \
#     --checkpoint-duration 1000 --disk-block-size 8388608 \
#     --persist-input true \
#     --checkpoint-compression tru --lineage true \
#     --create-merge true --parallel-merge true --latency true --threads $t >> scb_bench_res.txt
#    done
#done

echo "LRB2-ALL-CMP" >> scb_bench_res.txt
for t in ${t15[@]};
do
    for it in ${iterations[@]};
    do
     ./linear_road_benchmark_checkpoints \
     --unbounded-size 16777216 --circular-size 16777216 \
     --batch-size 262144 --bundle-size 262144 --query 2 \
     --checkpoint-duration 1000 --disk-block-size 8388608 \
     --persist-input true \
     --checkpoint-compression true --lineage true \
     --create-merge true --parallel-merge true --latency true --threads $t >> scb_bench_res.txt
    done
done

#echo "LRB3-ALL" >> scb_bench_res.txt
#for t in ${t14[@]};
#do
#    for it in ${iterations[@]};
#    do
#     ./linear_road_benchmark_checkpoints \
#     --unbounded-size 16777216 --circular-size 16777216 \
#     --batch-size 262144 --bundle-size 262144 --query 3 \
#     --disk-block-size 8388608 \
#     --persist-input true \
#     --checkpoint-compression tru --lineage true \
#     --create-merge true --parallel-merge true --latency true --threads $t >> scb_bench_res.txt
#    done
#done

echo "LRB3-ALL-CMP" >> scb_bench_res.txt
for t in ${t14[@]};
do
    for it in ${iterations[@]};
    do
     ./linear_road_benchmark_checkpoints \
     --unbounded-size 16777216 --circular-size 16777216 \
     --batch-size 262144 --bundle-size 262144 --query 3 \
     --disk-block-size 8388608 --persist-input true \
     --checkpoint-compression true --lineage true \
     --create-merge true --parallel-merge true --latency true --threads $t >> scb_bench_res.txt
    done
done

echo "YSB-ALL" >> scb_bench_res.txt
for t in ${t15[@]};
do
    for it in ${iterations[@]};
    do
     ./yahoo_benchmark_checkpoints --circular-size 33554432 --slots 128 --batch-size 1048576 \
     --bundle-size 1048576 --disk-block-size 32768 --latency true --checkpoint-compression true \
     --persist-input true --lineage true --checkpoint-duration 1000 --threads $t >> scb_bench_res.txt
    done
done

echo "NBQ5-ALL" >> scb_bench_res.txt
for t in ${t15[@]};
do
    for it in ${iterations[@]};
    do
     ./nexmark_checkpoints --circular-size 33554432 --batch-size 1048576 --bundle-size 1048576 \
     --unbounded-size 262144 --disk-block-size 1048576 --checkpoint-compression true --persist-input true \
     --lineage true --latency true --parallel-merge true --checkpoint-duration 1000 --threads $t >> scb_bench_res.txt
    done
done

echo "All done..."