#!/bin/bash
set -e

# Example:
#   sudo ./aquafs_base_performance.sh nvme2n1 [ /mnt ]

DEV=$1
MNT=$2

if [[ "$MNT" == "" ]]; then
    echo deadline > /sys/class/block/$DEV/queue/scheduler
    MKFS_ARG="--zbd="$DEV
    FS_URI="aquafs://dev:$DEV"
else
    MKFS_ARG="--zonefs="$MNT
    FS_URI="aquafs://zonefs:$MNT"
fi

GOOD_PARAMS=$(sudo ./get_good_db_bench_params_for_aquafs.sh $DEV)

AUXPATH=$DEV-aux

sudo rm -rf /tmp/$AUXPATH && sudo ../util/aquafs mkfs $MKFS_ARG --aux_path=/tmp/$AUXPATH --finish_threshold=10 --force

echo "Using URI "$FS_URI

NAME="aquafs-$DEV-baseline_performance"

echo "$(tput setaf 4)Running AquaFS baseline performance tests, results will be stored in results/$NAME $(tput sgr 0)"

DB_BENCH_EXTRA_PARAMS="$GOOD_PARAMS" FS_PARAMS="--fs_uri=$FS_URI" ./run.sh $NAME quick_performance
DB_BENCH_EXTRA_PARAMS="$GOOD_PARAMS" FS_PARAMS="--fs_uri=$FS_URI" ./run.sh $NAME long_performance
