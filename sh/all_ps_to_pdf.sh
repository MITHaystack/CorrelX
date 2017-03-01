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
# File: all_ps_to_pdf.sh.
# Author: A.J. Vazquez Alvarez (ajvazquez@haystack.mit.edu)
# Description:
# 
#    Script for converting multiple .ps files into .pdf and creating a single .pdf.
# 
#History:
#initial version: 2016 ajva
#MIT Haystack Observatory
#
for i in $1/*.ps; do ps2pdf $i; done
pdfjoin $1/*.pdf
