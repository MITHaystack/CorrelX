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
#File: vis_compare.py.
#Author: A.J. Vazquez Alvarez (ajvazquez@haystack.mit.edu)
#Description: 
"""
Script for providing a metric on the comparison between two CorrelX output files. Use only for debugging.

"""
#History:
#initial version: 2016.12 ajva
#MIT Haystack Observatory

import cx2d_lib
import argparse
import os
import sys

def main():

    cparser = argparse.ArgumentParser(description='CorrelX visibilities comparator:'+\
                                      ' computes the sum of the L2-norm for all visibilities from two files.'+
                                      ' It will stop if headers or number of visibilities differ. Use only for debugging.')
    cparser.add_argument('file_1',
                         help="Reference file with visibilities.")
    cparser.add_argument('file_2',
                         help="Test file with visibilities.")
    cparser.add_argument('-f', action="store_true",\
                         dest="force",default="0",\
                         help="Force execution even if headers or number of visibilities differ.")


    args =     cparser.parse_args()
    file_1 =   args.file_1
    file_2 =   args.file_2
    force =    int(args.force)
    path_src = os.path.dirname(sys.argv[0])


    cx2d_lib.get_error_indicator(file_1,file_2,force,path_src)

if __name__ == '__main__':
    main()

