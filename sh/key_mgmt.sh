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
# File: key_mgmt.sh.
# Author: A.J. Vazquez Alvarez (ajvazquez@haystack.mit.edu)
# Description:
#
#    Script for setting up ssh pwd-less connection between master and workers.
#    This code is left on a separate script so that it can be run only by the master node.
#
#    Arguments:
#    ----------
#     $1: list of nodes
#     $2: path to known hosts
#
#History:
#initial version: 2016.10 ajva
#MIT Haystack Observatory
#

for v in `echo $1|sed s/,/"\n"/g`; 
 do 
  ssh-keygen -f $2 -R $v
  ssh-keyscan $v >> $2
  #ssh $v uptime
  #echo "Testing:"$v
done

