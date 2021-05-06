#!/bin/bash
PMFILE="/data/pmem0/dash"
YCSBDIR="/data/large_data/"
PASSWORDS="siton-2009"

BIN="../cmake-build-remotedebug/test_dash"
#BIN="../build/test_dash"
for i in $(seq 1 1 1); do
    for workload_type in  e; do
        #72 68 64 60 56 52 48 44 40 36 32 28 24 20 16 12 8 4 1
        #72 56 48 36 32 28 24 20 16 8 4 1  32 16 8 4 1
        for thread_num in 36 32 16 8 4 1; do
             if [ -f "$PMFILE" ]; then
                 echo $PASSWORDS | sudo -S rm "$PMFILE"
             fi

             echo "ROUND:$i Workload_type:$workload_type ThreadNum:$thread_num"

             LD_PRELOAD="../cmake-build-remotedebug/pmdk/src/PMDK/src/nondebug/libpmemobj.so.1 \
       ../cmake-build-remotedebug/pmdk/src/PMDK/src/nondebug/libpmem.so.1"\
             $BIN workload$workload_type 36 $YCSBDIR$workload_type\_load.txt load

              LD_PRELOAD="../cmake-build-remotedebug/pmdk/src/PMDK/src/nondebug/libpmemobj.so.1 \
       ../cmake-build-remotedebug/pmdk/src/PMDK/src/nondebug/libpmem.so.1"\
             $BIN workload$workload_type $thread_num $YCSBDIR$workload_type\_run.txt run
        done
    done
done
