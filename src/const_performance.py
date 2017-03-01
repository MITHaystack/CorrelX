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
#File: const_performance.py.
#Author: A.J. Vazquez Alvarez (ajvazquez@haystack.mit.edu)
#Description:
"""
 Constants for performance configuration: multi-threading, optimizations, approximations...

"""
#History:
#initial version: 2016.12 ajva
#MIT Haystack Observatory

# -------------------------------------------------------------------------------------------------- Application layer
#                                                                                                            Map
###########################################################
#           Mapper general optimzations
###########################################################

# Superframes: group information for multiple frames in mapper output
#   This is to reduce overhad in MapReduce interface
# TO DO: this needs debugging, keep <=1 to avoid superframes
#NUM_FRAMES_PER_LINE = 10
NUM_FRAMES_PER_LINE = -1 # Keep -1. Needs debugging for >1



#                                                                                                            Reduce
###########################################################
#           Reducer general optimizations
###########################################################

# Compute FX after sub-accumulation periods (trade-off performance vs. memory)
#   This is problematic for delay correction if delay greater than number of samples stored...)
#   1 for compute at every acc period
#   TO DO: this needs testing
COMPUTE_FOR_SUB_ACC_PERIOD = -1 # Keep -1
#COMPUTE_FOR_SUB_ACC_PERIOD = 100
#COMPUTE_FOR_SUB_ACC_PERIOD = 400



# -------------------------------------------------------------------------------------------------- Libraries
#                                                                                                            FX
###########################################################
#           FX library general optimzations
###########################################################

# Try to compute rotations for different polarizations of the same station only once.
#    TO DO: Keep 0, debug for 1
SAVE_TIME_ROTATIONS = 0 # Keep 0. Needs debugging for 1


###########################################################
#           FX library approximations
###########################################################

# Performance vs. precision in fringe rotation
#FULL_TIMESCALE=0  # Evaluate delays only for the first sample
FULL_TIMESCALE=1  # Evaluate delays for the full timescale
#FULL_TIMESCALE=2  # Interpolate linearly based on delays for first and last samples


###########################################################
#           FX library multithreading
###########################################################

# PyFFTW 
#   (https://pypi.python.org/pypi/pyFFTW)
#   Using scipy fft by default.
#   TO DO: This is under development.
USE_FFTW =     0
THREADS_FFTW = 1

# Numexpr
#   (https://pypi.python.org/pypi/numexpr)
#   TO DO: This is under development.
USE_NE =       0
THREADS_NE =   1
if USE_NE:
    USE_NE_EXP=    1 # Use numexpr computing exponential
    USE_NE_FRINGE= 1 # Use numexpr in fringe rotation
    USE_NE_MULT=   1 # Use numexpr in multiplication (unused)
    USE_NE_F=      1 # Use numexpr in freq domain operations (unused)
else:
    USE_NE_EXP=    0
    USE_NE_FRINGE= 0
    USE_NE_MULT=   0
    USE_NE_F=      0

# Python multiprocessing.Pool
#   (https://docs.python.org/2/library/multiprocessing.html#using-a-pool-of-workers)
#   Use multi-threading, currently for fringe rotation.
#   TO DO: This is under development.
USE_MP =      0
MP_THREADS =  1


# <codecell>


