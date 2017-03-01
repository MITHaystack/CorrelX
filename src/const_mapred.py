# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

#!/usr/bin/env python
#
#The MIT CorrelX Correlator
#
#https://github.com/MITHaystack/CorrelX
#Contact: correlX@haystack.mit.edu
#Project leads: Victor Pankratius, Pedro Elosegui Project developer: A.J. Vazquez Alvarez
#
#Copyright 2017 MIT Haystack Observatory
#
#Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
#
#The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
#
#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
#
#------------------------------
#------------------------------
#Project: CorrelX.
#File: const_mapred.py.
#Author: A.J. Vazquez Alvarez (ajvazquez@haystack.mit.edu)
#Description:
"""
 Constants for mapper and reducer (including interface between both).

"""
#History:
#initial version: 2015.12 ajva
#MIT Haystack Observatory



KEY_SEP="\t"     # Key separator
FIELD_SEP="-"    # Field separator for hadoop
SF_SEP="."       # Sub-field separator (must be different than FIELD_SEP)


# TO DO: update this or delete
# Lenght of metadata header in vector with data (hadoop lines)
#  Example (META_LEN = 4): 
#     #|<--   key       -->||<---  metadata  -->||<-- data -->...
#                               0   1      2  3      
#     px-0.0-1.0-a0.0-sxa26   0.0 16000.0 2 0.0 (8.11965811966+0j) (7.43480419641+5.60940655801j) [...]


# 2016.06.13: px-A.A-A.A-a-11-0-11-f68255.00000000000000.11-s0.0-

HADOOP_NUM_KEY_FIELDS=9

# These keys ids. reference the fields in the key defined in msvf.get_pair_str()
# All baselines per task (partining and sorting configuration)
HADOOP_PARTITION_ALL_BASELINES_PER_TASK_STR="-k5,5"
COMMON_SORT_ALL_BASELINES_PER_TASK_STR="-k1 -k2 -k3 -k4 -k5 -k6 -k7 -k8 -k9"
#COMMON_SORT_ALL_BASELINES_PER_TASK_STR="-k6 -k7 -k8 -k9"

# One baseline per task (partining and sorting configuration)
HADOOP_PARTITION_ONE_BASELINE_PER_TASK_STR="-k1,4"
COMMON_SORT_ONE_BASELINE_PER_TASK_STR="-k5 -k6 -k7 -k8 -k9"


# Position of the channel id in the key splitting by SF_SEP (used by zoom band processor)
INDEX_KEY_CHANNEL=5


# Adding a parameter:
#  -Increase META_LEN
#  -Add new parameter in list below
#  -Add paramter in msvf.get_pair_str()
#  -Add parameter in rsvf.extract_params_split()
#  -Modify documentation as applicable

# METADATA (header after the key)
META_LEN = 28

# (!) Important: keep one variable per line to allow reading by debugging function cx2d_lib.get_list_meta().
[INDEX_ST_POL,\
 INDEX_SHIFT_DELAY,\
 INDEX_FRAC_DELAY,\
 INDEX_ABS_DELAY,\
 INDEX_RATE_DELAY_0,\
 INDEX_RATE_DELAY_1,\
 INDEX_RATE_DELAY_2,\
 INDEX_RATE_DELAY_REF,\
 INDEX_RATE_CLOCK_0,\
 INDEX_RATE_CLOCK_1,\
 INDEX_RATE_ZC_0,\
 INDEX_RATE_ZC_1,\
 INDEX_RATE_CLOCK_REF,\
 INDEX_RATE_M_ONLY,\
 INDEX_RATE_C_ONLY,\
 INDEX_RATE_DIFF_FRAC,\
 INDEX_NUM_SAMPLES,\
 INDEX_FS,\
 INDEX_BITS_PER_SAMPLE,\
 INDEX_FIRST_SAMPLE,\
 INDEX_DATA_TYPE,\
 INDEX_NBINS_PCAL,\
 INDEX_PCAL_FREQ,\
 INDEX_CHANNEL_INDEX,\
 INDEX_CHANNEL_FREQ,\
 INDEX_ACC_TIME,\
 INDEX_ENCODING,\
 INDEX_SIDEBAND] = list(range(META_LEN))
 
 



# Logging moved as an argument for the mapper and reducer (config file)
# Generates lines of log with information on the processed data for verification. Lines start with "z",
#  so they will be by-passed by the reducer.



# List of values for data_type header [0,1]
DATA_TYPE_LIST = ['r','c']







# Encode map output / reduce input as base64
#ENCODE_INT=0
ENCODE_B64=1

# Padding for sample number in key (fixed length, padding with zeros for sorting)
# This is to avoid requiring numerical sorting in Hadoop
PAD_S=14

# Use bitarrays reading vdif frames
USE_BITARRAYS=0


# Error codes for mapper reader
C_M_READ_SUCCESS =           0
C_M_READ_ERR_HEADER_NONE =  -1
C_M_READ_ERR_NO_SAMPLES =   -2
C_M_READ_ERR_DELAY_ABS =    -3
C_M_READ_ERR_DELAY_SHIFT =  -4




MAP_INPUT_FILE="map_input_file"

