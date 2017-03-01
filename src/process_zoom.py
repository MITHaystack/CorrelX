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
#File: process_zoom.py.
#Author: A.J. Vazquez Alvarez (ajvazquez@haystack.mit.edu)
#Description: 
"""
Script to process zoom bands (during post-processing).

"""
#History:
#initial version: 2016 ajva
#MIT Haystack Observatory

import imp
import cx2d_lib
imp.reload(cx2d_lib)
from cx2d_lib import *
import sys


def main():
    """
    Process zoom band.
    Input folder with input file, media.ini and correlation.ini
 
    Example
    -------
       python process_zoom.py /nobackup1b/users/ajva/tests_cx_20160610_1000 head_500_out.txt processed_head_500.txt 32

    """
    inout_folder= sys.argv[1]+"/"     #"/media/sf_shared_ubu2/partial_results_eht_20160531_1002/"
    file_in=      sys.argv[2]         #"part-00000_20160609"
    file_out=     sys.argv[3]         # "part_processed_z9"
    average_channels=int(sys.argv[4]) #-1   (or 32)
    #correlation_ini_file="correlation.ini"
    #media_ini_file="media.ini"
    
    print(file_in)
    print(file_out)
    print(average_channels)
    
    #print(" Forcing number of acc periods!!!")
    #filter_acc_periods=[0,1,2]
    filter_acc_periods=[]
    print(filter_acc_periods)
    
    process_zoom_band(inout_folder,file_in,file_out,average_channels=average_channels,v=1,filter_acc_periods=filter_acc_periods)




if __name__ == '__main__':
    main()

