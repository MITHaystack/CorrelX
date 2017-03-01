#!/bin/bash
# Run: ./examples/run_example_vgos.sh
# 
python src/mapred_cx.py -c conf/correlx.ini -f exper=examples/test_dataset_vgos,serial=1,parallel=0
