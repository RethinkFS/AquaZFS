#!/bin/bash
set -e

# Example:
#   sudo ./aquafs_base_crashtest.sh nvme0n1

DEV=$1

echo deadline > /sys/class/block/$DEV/queue/scheduler

GOOD_PARAMS=$(sudo ./get_good_db_bench_params_for_aquafs.sh $DEV)
GOOD_CRASH_PARAMS=""
for PARAM in $GOOD_PARAMS
do
  if [[ $PARAM == --target_file_size_base=* ]]; then
    GOOD_CRASH_PARAMS="${GOOD_CRASH_PARAMS} ${PARAM}"
  elif [[ $PARAM ==  --write_buffer_size=* ]]; then 
    SIZE=$(echo "$PARAM" | tr -dc '0-9')
    if [[ $SIZE -gt "2147483647" ]]; then
      echo "$PARAM not supported. Using --write_buffer_size=2147483647"
      GOOD_CRASH_PARAMS="${GOOD_CRASH_PARAMS} --write_buffer_size=2147483647"
    else
      GOOD_CRASH_PARAMS="${GOOD_CRASH_PARAMS} ${PARAM}"
    fi
  fi
done

AUXPATH=$DEV-aux
sudo rm -rf /tmp/$AUXPATH && sudo ../util/aquafs mkfs --zbd=$DEV --aux_path=/tmp/$AUXPATH --force

FS_URI="aquafs://dev:$DEV"
TIMESTAMP=$(date +"%d-%m-%Y_%H-%M-%S")
NAME="aquafs-$DEV-base_crashtest$TIMESTAMP"
echo "$(tput setaf 4)Running AquaFS baseline crashtests, results will be stored in results/$NAME $(tput sgr 0)"

CRASHTEST_EXTRA_PARAMS="$GOOD_CRASH_PARAMS" FS_PARAMS="--fs_uri=$FS_URI" ./run.sh $NAME crashtest
