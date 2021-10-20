#!/bin/bash

echo "Start running ingestion benchmarks"

allThreads=(7 8 9 10 14 15)
empty=()
iterations=(1 2 3 4 5)

# Before running the following experiments, set m_doProcessing variable in OperatorKernel to false
path="$HOME/LightSaber/build/test/benchmarks/applicationsWithCheckpoints/"

if [ -n "$path" ]; then
  echo "The build path is set to $path"
else
  echo "Set the application build path. Exiting..."
  exit
fi

cd $path # If the application doesn't run from the build folder, it breaks. This happens because of the where the code files are generated.
echo $PWD

echo "CM1-ALL" >> ingestion_res.txt
for t in ${allThreads[@]};
do
    for it in ${iterations[@]};
    do
     ./cluster_monitoring_checkpoints \
     --circular-size 33554432 --unbounded-size 524288 --batch-size 524288 --bundle-size 524288 \
     --query 1 --disk-block-size 65536 --checkpoint-compression true \
     --persist-input true --lineage true --latency true --threads $t >> ingestion_res.txt
    done
done

echo "CM2-ALL" >> ingestion_res.txt
for t in ${allThreads[@]};
do
    for it in ${iterations[@]};
    do
     ./cluster_monitoring_checkpoints \
     --circular-size 16777216 --unbounded-size 1048576 --batch-size 524288 --bundle-size 524288 \
     --query 2 --disk-block-size 131072 --latency true --checkpoint-compression true \
     --persist-input true --lineage true --threads $t >> ingestion_res.txt
    done
done

echo "SG1-ALL" >> ingestion_res.txt
for t in ${allThreads[@]};
do
    for it in ${iterations[@]};
    do
     ./smartgrid_checkpoints \
     --query 1 --unbounded-size 262144 --batch-size 1048576 --circular-size 33554432 \
     --bundle-size 1048576 --slots 128 --latency true --checkpoint-compression true \
     --disk-block-size 131072 --persist-input true --lineage true \
     --threads $t >> ingestion_res.txt
#     --query 1 --unbounded-size 262144 --batch-size 524288 --circular-size 16777216 \
#     --bundle-size 524288 --slots 128 --latency true --checkpoint-compression true \
#     --disk-block-size 131072 --persist-input true --lineage true \
#     --threads $t >> ingestion_res.txt
    done
done

echo "SG2-ALL" >> ingestion_res.txt
for t in ${allThreads[@]};
do
    for it in ${iterations[@]};
    do
     ./smartgrid_checkpoints \
     --query 2 --hashtable-size 512 --unbounded-size 1048576 --circular-size 16777216 \
     --bundle-size 524288  --slots 128 --batch-size 524288 --unbounded-size 4194304 \
     --disk-block-size 4194304 --create-merge true --persist-input true \
      --checkpoint-compression true --lineage true --parallel-merge tru --latency true --threads $t >> ingestion_res.txt
    done
done

echo "LRB1-ALL" >> ingestion_res.txt
for t in ${allThreads[@]};
do
    for it in ${iterations[@]};
    do
     ./linear_road_benchmark_checkpoints \
     --unbounded-size 8388608 --circular-size 16777216 \
     --batch-size 524288 --bundle-size 524288 --query 1 --hashtable-size 256 \
     --disk-block-size 16777216 --persist-input true \
     --checkpoint-compression tru --lineage true \
     --create-merge true --parallel-merge tru --latency true --threads $t >> ingestion_res.txt
    done
done

echo "LRB1-ALL-CMP" >> ingestion_res.txt
for t in ${allThreads[@]};
do
    for it in ${iterations[@]};
    do
     ./linear_road_benchmark_checkpoints \
     --unbounded-size 8388608 --circular-size 16777216 \
     --batch-size 524288 --bundle-size 524288 --query 1 --hashtable-size 256 \
     --disk-block-size 16777216 --persist-input true \
     --checkpoint-compression true --lineage true \
     --create-merge true --parallel-merge tru --latency true --threads $t >> ingestion_res.txt
    done
done

echo "LRB2-ALL" >> ingestion_res.txt
for t in ${allThreads[@]};
do
    for it in ${iterations[@]};
    do
     ./linear_road_benchmark_checkpoints \
     --unbounded-size 16777216 --circular-size 16777216 \
     --batch-size 262144 --bundle-size 262144 --query 2 \
     --disk-block-size 8388608 \
     --persist-input true \
     --checkpoint-compression tru --lineage true \
     --create-merge true --parallel-merge tru --latency true --threads $t >> ingestion_res.txt
    done
done

echo "LRB2-ALL-CMP" >> ingestion_res.txt
for t in ${allThreads[@]};
do
    for it in ${iterations[@]};
    do
     ./linear_road_benchmark_checkpoints \
     --unbounded-size 16777216 --circular-size 16777216 \
     --batch-size 262144 --bundle-size 262144 --query 2 \
     --disk-block-size 8388608 \
     --persist-input true \
     --checkpoint-compression true --lineage true \
     --create-merge true --parallel-merge tru --latency true --threads $t >> ingestion_res.txt
    done
done

echo "YSB-ALL" >> ingestion_res.txt
for t in ${allThreads[@]};
do
    for it in ${iterations[@]};
    do
     ./yahoo_benchmark_checkpoints --circular-size 33554432 --slots 128 --batch-size 1048576 \
     --bundle-size 1048576 --disk-block-size 32768 --latency true --checkpoint-compression true \
     --persist-input true --lineage true --threads $t >> ingestion_res.txt
    done
done

echo "NBQ5-CH" >> ingestion_res.txt
for t in ${allThreads[@]};
do
    for it in ${iterations[@]};
    do
     ./nexmark_checkpoints --circular-size 33554432 --batch-size 1048576 --bundle-size 1048576 \
     --unbounded-size 262144 --disk-block-size 1048576 --checkpoint-compression true --persist-input true \
     --lineage true --latency true --parallel-merge true --threads $t >> ingestion_res.txt
    done
done

echo "All done..."