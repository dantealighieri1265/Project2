#!/bin/bash
export JOB_ID=$(flink list | grep Queries |sed 's/: /\n/g' | sed -n 2p)
flink cancel $JOB_ID