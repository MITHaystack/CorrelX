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
#File: convert_cx2d.py.
#Author: A.J. Vazquez Alvarez (ajvazquez@haystack.mit.edu)
#Description:
"""
Script to convert cx output into dx at the cluster. Need a folder with the media.ini, correlation.ini and output file.

Parameters
----------
 1 : str
    path to input/output folder (containining CorrelX/CX file, and where newly created DiFX/SWIN+PCAL files will be placed.
 2 : str
    path (relative to input/output folder) containing .ini files for the experiment.
 3 : str
    CorrelX/CX file name

More info
---------
 See cx2d_lib.convert_cx2dpc() for more info.

(!) Known issues / TO DO
------------------------
 Currently conjugating phase calibration results.
 Currently scaling phase calibration amplitudes.

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

    folder_inout=   sys.argv[1]+"/"
    ini_folder=     sys.argv[2]+"/"
    file_in=        sys.argv[3]


    # Forced parameters
    forced_file_list=[]
    v=0

    pcal_scaling=1/float(370)
    conjugate_pcal_values=1

    divide_vis_by=1

    forced_acc=-1
    file_out=file_in+".out2swin2scaled"

    print("Manually configured values:")
    print(" pcal_scaling="+str(pcal_scaling))
    
    print("")
    print("Converting cx2d...")


    correlation_ini_file= folder_inout+ini_folder+"correlation.ini"
    media_ini_file=       folder_inout+ini_folder+"media.ini"
    stations_ini_file=    folder_inout+ini_folder+"stations.ini"



    pcal_file_list = convert_cx2dpc(inout_folder=folder_inout,file_out=file_out,file_in=file_in,\
                            correlation_ini_file=correlation_ini_file,\
                            media_ini_file=media_ini_file,stations_ini_file=stations_ini_file,v=v,\
                            back_compat=0,forced_accumulation_period=forced_acc,divide_vis_by=divide_vis_by,\
                            forced_file_list=forced_file_list,\
                            pcal_scaling=pcal_scaling,conjugate_pcal_values=conjugate_pcal_values)
    print("")
    print("Output files:")
    print(" "+file_out)
    if pcal_file_list!=[]:
        for i in pcal_file_list:
            print(" "+i)

if __name__ == '__main__':
    main()

