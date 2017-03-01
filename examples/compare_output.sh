#!/bin/bash
# Run: ./examples/compare_output.sh <path_to_output_file>
# Example: ./examples/compare_output.sh examples/test_dataset_vgos/cx_20170228_144701/OUT_s0_v0.out
#
python src/vis_compare.py examples/test_dataset_vgos/example_output/OUT_s0_v0.out $1
