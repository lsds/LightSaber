#!/bin/bash

echo "Start running recovery benchmarks"

# Set SystemConf::PERFORMANCE_MONITOR_INTERVAL = 100 or 10
# Adjust manually the throughput in test/benchmarks/applications/BenchmarkQuery.h by setting the sleep time of line 156
# Set in LRB1.cpp, line 610 to false => if (true) {...}
# Increase the sleep time if the recovering application can't access the pmem files locks
path="$HOME/LightSaber/build/test/benchmarks/applicationsWithCheckpoints/"
#path="/home/ubuntu/tmp/cmake-build-debug-aws/test/benchmarks/applicationsWithCheckpoints/"
#path=/tmp/tmp.0lki8nQd4R/cmake-build-debug/test/benchmarks/applicationsWithCheckpoints

if [ -n "$path" ]; then
  echo "The build path is set to $path"
else
  echo "Set the application build path. Exiting..."
  exit
fi

cd $path # If the application doesn't run from the build folder, it breaks. This happens because of the where the code files are generated.
echo $PWD

bash -c "exec -a LRBRecover ./linear_road_benchmark_checkpoints --unbounded-size 8388608 --circular-size 16777216 --batch-size 524288 --bundle-size 524288 --query 1 --hashtable-size 256 --checkpoint-duration 1000 --disk-block-size 16777216 --persist-input true --checkpoint-compression true --lineage true --create-merge true --parallel-merge true --latency true --threads 15 --ingestion 300 --performance-monitor-interval 100 >> recovery.txt" &

sleep 7
pkill -f LRBRecover
sleep 0.07


bash -c "exec -a LRBRecover ./linear_road_benchmark_checkpoints --unbounded-size 8388608 --circular-size 16777216 --batch-size 524288 --bundle-size 524288 --query 1 --hashtable-size 256 --checkpoint-duration 1000 --disk-block-size 16777216 --persist-input true --checkpoint-compression true --lineage true --create-merge true --parallel-merge true --latency true --recover true --threads 15 --ingestion 300 --performance-monitor-interval 100 >> recovery.txt" &
sleep 15
pkill -f LRBRecover

echo "All done..."