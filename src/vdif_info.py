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
#File: vdif_info.py.
#Author: A.J. Vazquez Alvarez (ajvazquez@haystack.mit.edu)
#Description: 
"""
Script for reading metadata from VDIF file. 

"""
#History:
#initial version: 2016.12 ajva
#MIT Haystack Observatory

import lib_vdif
import argparse
import sys

def main():

    cparser = argparse.ArgumentParser(description='VDIF info')
    cparser.add_argument('file_vdif',nargs='?',
                         help="VDIF file.")
    cparser.add_argument('-n', action="store",\
                         dest="limit_frames",default="-1",\
                         help="Maximum number of frames.")

    cparser.add_argument('-s', action="store",\
                         dest="skip_frames",default="0",\
                         help="Number of frames to skip.")

    cparser.add_argument('-b', action="store_true",\
                         dest="brief",default="0",\
                         help="Brief output (no legend for columns).")
    
    cparser.add_argument('--summary', action="store_true",\
                         dest="summary",default="0",\
                         help="Display only a summary for the whole file.")


    args =          cparser.parse_args()
    file_vdif =     args.file_vdif
    limit_frames =  int(args.limit_frames)
    skip_frames =   int(args.skip_frames)
    brief =         int(args.brief)
    summary =       int(args.summary)

    if summary:
        lib_vdif.get_vdif_stats(file_vdif,short_output=brief)
    else:
        lib_vdif.show_headers_vdif(file_vdif,limit_frames,skip_frames,brief)

if __name__ == '__main__':
    main()

