#!/bin/bash

# Format a ZBD and create a AquaFS filesystem

rm -rf /tmp/aquafs-aux
$AQUAFS_DIR/aquafs mkfs --zbd=$ZDEV --aux-path=/tmp/aquafs-aux --force --finish-threshold=5 > $TEST_OUT
RES=$?
if [ $RES -ne 0 ]; then
  exit $RES
fi

exit 0
