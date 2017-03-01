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
#File: const_hadoop.py.
#Author: A.J. Vazquez Alvarez (ajvazquez@haystack.mit.edu)
#Description:
"""
 Script to convert .input and .im files into CorrelX configuration files.

Example
-------
 python ~/convert_im_cx.py . bm434aYs20_20 BM434A-BR-No0017-Aa3m.vdif,BM434A-LA-No0017_3m.vdif

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
    
    inout_folder=   sys.argv[1]            #+"/"
    file_in_im=     sys.argv[2]+".im"
    file_in_input=  sys.argv[2]+".input"
    if len(sys.argv)>3:
        forced_files=sys.argv[3]
    else:
        forced_files= ""
    
    file_out_dm=    "delay_model.ini"
    file_out_st=    "stations.ini"
    file_out_so=    "sources.ini"
    file_out_md=    "media.ini"
    file_out_co=    "correlation.ini"
    
    filter_sources=[0]
    forced_stations=[]

    print("Generating delay_model.ini...")
    im_to_delay_model(inout_folder,file_in_im,file_out_dm,filter_sources)
    
    print("Generating stations.ini...")
    input_to_stations(inout_folder,file_in_input,file_out_st,forced_stations)
    
    print("Generating media.ini...")
    im_to_sources(inout_folder,file_in_im,file_out_so,forced_stations)
    
    print("Generating correlation.ini...")
    input_to_correlation(inout_folder,file_in_input,file_out_co)
    
    print("Generating media.ini...")
    input_to_media(inout_folder,file_in_input,file_out_md,forced_files)
    
    print("Done.")

    
if __name__ == '__main__':
    main()

