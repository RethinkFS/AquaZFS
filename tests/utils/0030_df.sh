#!/bin/bash

# Verify aquafs df command works as expected, simple sanity check.

$AQUAFS_DIR/aquafs df --zbd=$ZDEV >> $TEST_OUT
RES=$?
if [ $RES -ne 0 ]; then
  echo "Error: df failed to run" >> $TEST_OUT
  exit $RES
fi

exit 0
