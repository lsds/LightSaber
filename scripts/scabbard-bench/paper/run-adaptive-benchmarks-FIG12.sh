#!/bin/bash

echo "Start running adaptive benchmarks"

path="$HOME/LightSaber/build/test/benchmarks/applicationsWithCheckpoints/"

if [ -n "$path" ]; then
  echo "The build path is set to $path"
else
  echo "Set the application build path. Exiting..."
  exit
fi

cd $path # If the application doesn't run from the build folder, it breaks. This happens because of the where the code files are generated.
echo $PWD

# run with adaptive compression
echo "ME1 adaptive" >> adapt_res.txt
./manufacturing_equipment_checkpoints --unbounded-size 32768 --circular-size 16777216 --batch-size 262144 --bundle-size 262144 --disk-block-size 32768 --latency tru --checkpoint-compression true --persist-input true --lineage true --threads 10 --adaptive-compression true --adaptive-data true >> adapt_res.txt

# run without adaptive compression
echo "ME1" >> adapt_res.txt
./manufacturing_equipment_checkpoints --unbounded-size 32768 --circular-size 16777216 --batch-size 262144 --bundle-size 262144 --disk-block-size 32768 --latency tru --checkpoint-compression true --persist-input true --lineage true --threads 10 --adaptive-data true >> adapt_res.txt

echo "All done..."