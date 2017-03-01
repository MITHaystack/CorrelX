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
#File: lib_debug.py.
#Author: A.J. Vazquez Alvarez (ajvazquez@haystack.mit.edu)
#Description:
"""
 Debugging configuration.

"""
#History:
#initial version: 2016.11 ajva
#MIT Haystack Observatory

import imp
import const_mapred
imp.reload(const_mapred)
from const_mapred import *


###########################################
#           Configuration Mapper
###########################################

# Tabulated information on processed frames--------------------------------------
DEBUG_ALIGN=0         # default
#DEBUG_ALIGN=1        # Display debugging information for alignment of streams

# Do not create output records, use for clear reading of debugging information---
SILENT_OUTPUT=0       # default
#SILENT_OUTPUT=1      # Hide output (usually to be used when DEBUG_ALIGN=1

# General info
DEBUG_GENERAL_M=0     # default
#DEBUG_GENERAL_M=1    # general information mapper

# VDIF reader verbose
VERBOSE_MAPPER_IO=0   # default
#VERBOSE_MAPPER_IO=1  # Verbose mode for VDIF reader

# VDIF reader errors
SHOW_ERRORS=0
#SHOW_ERRORS=1        # Print error messages in reader

# By-pass reducer
BYPASS_REDUCER=0      # default
#BYPASS_REDUCER=1     # By-pass reducer (so that mapper output is reducer output, use this to debug the mapper in Hadoop,
#                        and also to debug the lines that reach each reducer [useful for partitioning, sorting, etc.])

###########################################
#           Configuration Reducer
###########################################

# Delay computation -----------------------
DEBUG_DELAYS=0        # default
#DEBUG_DELAYS=1       # print offsets and computed delays for fringe rotation and frac. samp. correction

# Data stacking ---------------------------
DEBUG_HSTACK=0        # default
#DEBUG_HSTACK=1       # print indices for pointers to stored data

# Fractional sample shift / overflow ------
DEBUG_FRAC_OVER=0     # deafult
#DEBUG_FRAC_OVER=1    # print values for computations for overflow in fractional sample correction

# Verbose delay computation at reducer----
DEBUG_LIB_DELAY=0     # default
#DEBUG_LIB_DELAY=1    # print inputs to computation of delays (initial calculation should match those done later at the rsvf
    
# General info
DEBUG_GENERAL_R=0     # default
#DEBUG_GENERAL_R=1    # general information reducer    
    

