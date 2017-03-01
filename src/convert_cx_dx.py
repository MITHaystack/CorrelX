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
#File: convert_cx_dx.py.
#Author: A.J. Vazquez Alvarez (ajvazquez@haystack.mit.edu)
#Description:
"""
Script to convert cx output into dx at the cluster. Need a folder with the media.ini, correlation.ini and output file.

Parameters
----------
 1 : str
    input/output folder.
 2 : str
    input file (cx output).
 3 : str
    output file (swin format).
 4 : str(int)
    if 1 only first half of the FFT is taken.
 5 : str(float)
    accumulation period value to be overwritten.
 6 : chr
    polarization char to be overwritten (first station).
 7 : chr
    polarization char to be overwritten (second station)

Example
-------
 head -n500 /nobackup1b/users/ajva/tests_cx_20160610_1000/out_cx_eht_20160610.txt > /nobackup1b/users/ajva/tests_cx_20160610_1000/head_500_out.txt
 python convert_cx_dx.py /nobackup1b/users/ajva/tests_cx_20160610_1000 processed_head_500.txt processed_head_500__ 1 0.32 R L

"""
#History:
#initial version: 2016.06 ajva
#MIT Haystack Observatory

import imp

import cx2d_lib
imp.reload(cx2d_lib)
from cx2d_lib import *
import sys

def main():

    inout_folder=sys.argv[1]+"/" 
    file_in=sys.argv[2] 
    file_out=sys.argv[3] 

    only_half=int(sys.argv[4])
    forced_accumulation_period=float(sys.argv[5])
    forced_pol_list=forced_pol_list=[sys.argv[6],sys.argv[7]]

    correlation_ini_file="correlation.ini"
    media_ini_file="media.ini"
    v=0
    convert_cx2d(doutput_file=inout_folder+file_out,cxoutput_file=inout_folder+file_in,\
                 correlation_ini_file=inout_folder+correlation_ini_file,media_ini_file=inout_folder+media_ini_file,\
                 forced_pol_list=forced_pol_list,only_half=only_half,v=v,back_compat=0,\
                 forced_accumulation_period=forced_accumulation_period)


if __name__ == '__main__':
    main()

