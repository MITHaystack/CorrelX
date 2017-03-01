#!/bin/bash
#
# The MIT CorrelX Correlator
#
# https://github.com/MITHaystack/CorrelX
# Contact: correlX@haystack.mit.edu
# Project leads: Victor Pankratius, Pedro Elosegui Project developer: A.J. Vazquez Alvarez
#
# Copyright 2017 MIT Haystack Observatory
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
#
#----------------------
#----------------------
# Project: CorrelX.
# File: cx2d.sh.
# Author: A.J. Vazquez Alvarez (ajvazquez@haystack.mit.edu)
# Description:
#
#  Script for converting CorrelX output into DiFX output.
#
# Parameters
# ----------
#
#  1 :  
#
# Known issues 
# ------------
#  Requires to copy ini files into output folder if using zoom bands
#
#History:
#initial version: 2016 ajva
#MIT Haystack Observatory
#

CX=$1                     # Sources folder (correlx/src)
OUT_DIR=`readlink -e $2`  # Output folder (test_dataset/cx_20161201_1645)
OUT_FILE=$3               # Output file (OUT_....out)
AVG=$4                    # Average components (32)

MEDIA=$OUT_DIR/../media.ini

CURRENT=`pwd`
#echo "Running cx2d..."
cd $OUT_DIR
echo $MEDIA
if grep -q zoom_post "$MEDIA"; then
 echo "Zoom bands"
 cp ../*.ini .
 python $CX/process_zoom.py . $OUT_FILE ${OUT_FILE}_zoom $AVG
 echo "Convert format"
 python $CX/convert_cx2d.py . .. ${OUT_FILE}_zoom
else
 echo "Convert format"
 python $CX/convert_cx2d.py . .. $OUT_FILE
fi
cd $CURRENT
