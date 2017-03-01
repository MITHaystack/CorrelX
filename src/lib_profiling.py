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
#File: lib_profiling.py.
#Author: A.J. Vazquez Alvarez (ajvazquez@haystack.mit.edu)
#Description: 
"""
Routines for profiling and generation of call graphs (using pycallgrah).

"""
#History:
#initial version: 2016.10 ajva
#MIT Haystack Observatory

C_PROFILE_OPTS="-m cProfile -s cumulative"
C_PROFILE_CONVERT_CMD="lib_profiling.py cprof "


import pstats
import sys


###################################
#            cProfile
###################################

def process_cprof_file(file_cprof):
    """
    Convert cProfile output file into text format, see Script section below for an usage example.
    """
    pstats.Stats(file_cprof).strip_dirs().sort_stats("cumulative").print_stats()

    
    
    
################################### 
#            PyCallGraph
###################################

def get_include_functions(input_py,avoid_v=["main"],plus_v=["lib_*"],exclude_v=["*.<module>"],profile_memory=0):
    """
    Get str with include argument for pycallgraph.
    
    Parameters
    ----------
     input_py : str
         python script to scan functions in.
     avoid_v : list of str
         list of funcionts to be excluded.
     profile_memory : int
         [0 by default] if 1 will include memory profiling information (see notes below).
     
    Returns
    -------
     i_args : str 
         arguments on functions to be included in pycallgraph profiling.
     
    Notes
    -----
     -Memory profiling (not used by default):
       Reported as experimental by pycallgrpah docs. 
       Install psutil and python-psutil, otherwise it will be slow.
    """
    func_name_v=[]
    test_def="def "
    with open(input_py, 'r') as f:
        lines = f.readlines()
        for line in lines:
            if line[:len(test_def)]==test_def:
                func_name=line[len(test_def):].split('(')[0]
                func_name_v.append(func_name)
    
    i_args=""    
    if profile_memory:
        i_args+="-m "                       # Memory profiling
    for i in func_name_v+plus_v:
        if i not in avoid_v:
            i_args+="-i \""+i+"\" "         # Include if not in avoid_v
    for i in exclude_v:
        i_args+="-e \""+i+"\" "             # Exclude
    
    return(i_args)
    
    
def get_pycallgraph_str(i_args,out_png):
    """
    Get caller to pycallgraph.
    
    Parameters
    ----------
     i_args : str
         generated wit get_include_functions().
     out_png : str
         output image (.png).
     
    Returns
    -------
     pycallgraph_str : str
         caller.
    """
    pycallgraph_str = "pycallgraph "+i_args+"graphviz -o "+out_png+" -- "
    return(pycallgraph_str)



################################### 
#            Script
###################################



if __name__ == '__main__':
    
    # Example: python lib_profiling.py cprof cProfile_output_file > cProfile_text_file
    if sys.argv[1] == "cprof":
        process_cprof_file(sys.argv[2])

