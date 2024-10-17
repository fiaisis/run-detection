#!/usr/bin/env bash

# Check for heartbeat file
CURRENT_TIME=$(date +%s)
FILE_TIME=$(date -r /tmp/heartbeat +%s)
DIFF=$((CURRENT_TIME - FILE_TIME))
if [ $DIFF -lt 20 ]; then
  exit 0
else
  exit 1
fi

# Test for a readable file the archive
FILE_LIST="/archive/NDXLOQ/Instrument/data/cycle_24_3/LOQ00110782.nxs /archive/NDXOSIRIS/Instrument/data/cycle_24_3/OSIRIS00149339.nxs /archive/NDXTOSCA/Instrument/data/cycle_24_3/TSC30650.nxs /archive/NDXMARI/Instrument/data/cycle_23_4/MAR29177.nxs"
for file in $FILE_LIST; do
  if ! [ -r "$file" ]; then
    exit 1
  fi
done

# All checks previously passed
exit 0