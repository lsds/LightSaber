#!/bin/bash

echo "Start running benchmarks"

allThreads=(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)
iterations=(1) # 2 3 4 5)
t12=(12)
t14=(14)
t15=(15)

# sudo nano /sys/devices/system/cpu/smt/control (off)
# cat /sys/devices/system/cpu/smt/control
# install pcm https://software.opensuse.org/download/package?package=pcm&project=home%3Aopcm
# 1) Login as root
# 2) Execute the command 'modprobe msr'

# qperf (server)
# qperf -t 10 <ip> rc_bw
# qperf -t 10 <ip> --rem_id mlx5_1 rc_bw
# server: iperf -s
# client: iperf -c 10.0.0.40

# ibdev2netdev : check status
# sudo ifconfig ib0 10.0.0.30/24 up
# sudo ifconfig ib1 11.0.0.31/24 up

# if run in wallaby, use the second socket => change the core mapping in Utils.cpp
# unset the DHAVE_NUMA FLAG
# use args.push_back("--gcc-toolchain=/usr/local/gcc/7.5.0"); in OperatorJit.cpp => https://stackoverflow.com/questions/40486053/selecting-a-specific-libstdc-version-with-clang

#path="$HOME/LightSaber/cmake-build-debug/test/benchmarks/applicationsWithCheckpoints/"
path="/tmp/tmp.Ogl5pzLSii/cmake-build-debug-wallaby/test/benchmarks/applicationsWithCheckpoints/"

if [ -n "$path" ]; then
  echo "The build path is set to $path"
else
  echo "Set the application build path. Exiting..."
  exit
fi

cd $path # If the application doesn't run from the build folder, it breaks. This happens because of the where the code files are generated.
echo $PWD

echo "CM1" >> remote_scb_bench_res.txt
for t in ${t15[@]};
do
    for it in ${iterations[@]};
    do
     echo "Starting Sink"
     nohup ssh -t kea04 '/tmp/tmp.VuzIsR9Kxl/cmake-build-debug-kea04/test/benchmarks/applications/remoteRDMASink --batch-size 1048576 --bundle-size 1048576' &

     echo "Starting app"
     nohup bash -c "./cluster_monitoring_checkpoints \
     --circular-size 33554432 --unbounded-size 524288 --batch-size 524288 --bundle-size 524288 \
     --query 1 --checkpoint-duration 1000 --disk-block-size 65536 --checkpoint-compression true \
     --persist-input true --lineage true --latency true --threads 15 >> remote_scb_bench_res.txt" &

     echo "Starting Source"
     nohup ssh -t kea03 '/tmp/tmp.42rD7Z5vpA/cmake-build-debug-kea03/test/benchmarks/applications/remoteRDMASource --batch-size 1048576 --bundle-size 1048576 --query 0' &
    done
done


echo "YSB" >> remote_scb_bench_res.txt
for t in ${t15[@]};
do
    for it in ${iterations[@]};
    do
     echo "Starting Sink"
     nohup ssh -t kea04 '/tmp/tmp.VuzIsR9Kxl/cmake-build-debug-kea04/test/benchmarks/applications/remoteRDMASink --batch-size 1048576 --bundle-size 1048576' &

     echo "Starting app"
     nohup bash -c " ./yahoo_benchmark_checkpoints \
     --circular-size 33554432 --slots 128 --batch-size 1048576 --bundle-size 1048576 \
     --disk-block-size 32768 --latency true --checkpoint-compression true --persist-input true --lineage true \
     --checkpoint-duration 1000 --threads 15 >> remote_scb_bench_res.txt" &

     echo "Starting Source"
     nohup ssh -t kea03 '/tmp/tmp.42rD7Z5vpA/cmake-build-debug-kea03/test/benchmarks/applications/remoteRDMASource --batch-size 1048576 --bundle-size 1048576 --query 8' &
    done
done


echo "All done..."