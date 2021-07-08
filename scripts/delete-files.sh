#!/bin/bash
cd ..
rm -r results/*
rm -r benchmark/*
rm -r flink-config/pid-values
echo "Directories deleted!"
cd scripts || exit