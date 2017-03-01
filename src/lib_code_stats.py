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
#File: lib_code_stats.py.
#Author: A.J. Vazquez Alvarez (ajvazquez@haystack.mit.edu)
#Description
"""     
CorrelX line counter tools.

Usage
-----     
     get_cx_code_stats()

"""
#History:
#initial version: 2016.11 ajva
#MIT Haystack Observatory

from __future__ import print_function
import time


WIDTH_NUM=14
FILELIST_V=[["Launcher and Filesystem Management",\
                     "mapred_cx.py,"+\
                     "lib_mapredcorr.py,"+\
                     "lib_profiling.py,"+\
                     "lib_net_stats.py,"+\
                     "lib_ini_exper.py,"+\
                     "lib_hadoop_hdfs.py,"+\
                     "lib_config.py,"+\
                     "const_config.py,"+\
                     "const_hadoop.py"],\
            ["Application",\
                     "msvf.py,"+\
                     "rsvf.py"],\
            ["Libraries",\
                     "const_mapred.py,"+\
                     "const_quant.py,"+\
                     "lib_quant.py,"+\
                     "lib_vdif.py,"+\
                     "const_ini_files.py,"+\
                     "lib_ini_files.py,"+\
                     "lib_acc_comp.py,"+\
                     "lib_delay_model.py,"+\
                     "lib_fx_stack.py,"+\
                     "lib_pcal.py,"+\
                     "const_debug.py,"+\
                     "const_performance.py,"+\
                     "lib_debug.py"],\
            ["Tools",\
                     "lib_code_stats.py,"+\
                     "vdif_info.py,"+\
                     "vdif_generator.py,"+\
                     "vis_compare.py,"+\
                     "cx2d_lib.py,"+\
                     "convert_im_cx.py,"+\
                     "process_zoom.py,"+\
                     "convert_cx2d.py"]]





def get_file_stats(full_input_file,full_debug_file,debug_doc=0):
    """
    Get statistics for a single file.
    
    Parameters
    ----------
     full_input_file : str
         path to file to process (.py).
     full_debug_file : str
         path to file to debug doc section.
     debug_doc : int
         1 to debug.
     
    Returns
    -------
     c_line : int
         number of lines (total).
     c_code : int
         number of lines with code.
     c_comment : int
         number of comment-only lines.
     c_empty : int
         number of blank lines.
     c_ne : int
         number of non-blank lines.
     
    Notes
    -----
    |
    | **Assumptions:**
    |
    |  It is assumed that the three quotations marks are only used to delimit 
    |
    |
    | **Notes:**
    |
    | All non-empty lines in the doc section of the functions is accounted as comments.
    | Lines of code containing both code and comment are counted as code only
    """
    c_empty=0
    c_ne=0
    c_comment=0
    c_line=0
    lines_debug=[]
    comment_mode=False # used to count lines in initial doc section.
    
    with open(full_input_file, 'r') as f:
        lines = f.readlines()
        for line in lines:
            line_strip=line.strip()
            c_line+=1
            
            if ("\"\"\"" in line_strip or "\'\'\'" in line_strip): # switch doc-section mode
                if line_strip[0]!="#":
                    comment_mode=not(comment_mode)
                    lines_debug.append("---> comment mode = "+str(comment_mode))
            
            
            if comment_mode:                                     # doc section
                if len(line_strip)==0:                           #                      empty line
                    c_empty+=1
                else:
                    c_comment+=1
                if debug_doc:
                    lines_debug.append(line_strip)
            else:                                                # non doc section
                if len(line_strip)==0:                           #                      empty line
                    c_empty+=1
                elif line_strip[0]=="#":                         #                      only-comment line
                    c_comment+=1
                

    if debug_doc and lines_debug!=[]:
        with open(full_debug_file,'a') as f_dd:
            for line_debug in lines_debug:
                print(line_debug,file=f_dd)
    
    c_code=c_line-c_comment-c_empty
    c_ne=c_line-c_empty
    return([c_line,c_code,c_comment,c_empty,c_ne])


def get_out_line_stats(str_v):
    """
    Generate output line with statistics.
    
    Parameters
    ----------
     str_v : list of str
         ["str_identifier", str1, str2, ...] or ["str_identifier", int1, int2, ...]
    
    Returns
    -------
     str
         tabulated output line.
    
    Example    
    -------
     lines_out.append(" "+"[File]".ljust(20)+str("[Total]").rjust(WIDTH_NUM)+\
              str("[Code]").rjust(WIDTH_NUM)+\
              str("[Comments]").rjust(WIDTH_NUM)+\
              str("[Empty]").rjust(WIDTH_NUM)+\
              str("[Non-empty]").rjust(WIDTH_NUM))
    """
    return(" "+str_v[0].ljust(20)+''.join([str(i).rjust(WIDTH_NUM) for i in str_v[1:]]))


def get_cx_code_stats(filelist_v=FILELIST_V,p="./",output_file="stats_code",debug_doc_file="stats_code_debug"):
    """
    Generate statistics for CorrelX source code.
    
    Parameters
    ----------
     filelist_v
         list of lists following the format ["group identifier", "lib1.py,lib2.py,...,libN.py"]
     p : str
         path to source file.
     output_file : str
         prefix for output file.
     
    Returns
    -------
     None
    """
    debug_doc=1
    
    suffix_file="_"+time.strftime("%Y%m%d")+".txt"
    
    output_file+=    suffix_file
    debug_doc_file+= suffix_file
    
    with open(debug_doc_file,'w') as f_dd:
        print("",end="",file=f_dd)
    
    
    all_c_code=0
    all_c_empty=0
    all_c_ne=0
    all_c_comment=0
    all_c_line=0
    
    lines_out=[]
    
    # Main title
    lines_out.append("Statistics for CorrelX Python sources on "+time.strftime("%Y/%m/%d")+":")
    
    # Iterate all groups of files
    for filelist_two in filelist_v:
        [section,filelist]=filelist_two
        
        t_c_code=0
        t_c_empty=0
        t_c_ne=0
        t_c_comment=0
        t_c_line=0
        
        file_v=sorted(filelist.split(','))
        
        # Section title
        lines_out.append("")
        lines_out.append(section+":")
        lines_out.append("-"*(len(section)+1))
        lines_out.append("")
        
        # Header for stats
        lines_out.append(get_out_line_stats(["[File]","[Total]","[Code]","[Doc+Comm]","[Empty]","[Non-empty]"]))
        
        for file_i in file_v:
            
            # Statistics for current file
            full_input_file=p+file_i
            [c_line,c_code,c_comment,c_empty,c_ne] = get_file_stats(full_input_file,debug_doc_file,debug_doc)
            
            # Cumulated statistics
            t_c_code+=c_code
            t_c_line+=c_line
            t_c_comment+=c_comment
            t_c_empty+=c_empty
            t_c_ne+=c_ne
            
            # Current file stats
            lines_out.append(get_out_line_stats([file_i,c_line,c_code,c_comment,c_empty,c_ne]))
        
        # Totals for current group
        lines_out.append(get_out_line_stats(["<Totals>",t_c_line,t_c_code,t_c_comment,t_c_empty,t_c_ne]))
        
        lines_out.append("")
        all_c_code+=t_c_code
        all_c_line+=t_c_line
        all_c_comment+=t_c_comment
        all_c_empty+=t_c_empty
        all_c_ne+=t_c_ne
        
    # Totals
    lines_out.append(get_out_line_stats(["<TOTALS>",all_c_line,all_c_code,all_c_comment,all_c_empty,all_c_ne]))
    
    
    # Summary
    lines_sum = []
    lines_sum.append("")
    lines_sum.append("*Summary:")
    lines_sum.append("")
    lines_sum.append("  Lines of code:".ljust(20)+str(all_c_code).rjust(WIDTH_NUM))
    lines_sum.append("  Lines of comments:".ljust(20)+str(all_c_comment).rjust(WIDTH_NUM))
    lines_sum.append("")
    lines_sum.append("")
    lines_sum.append("*Details:")
    
    lines_out=[lines_out[0]]+lines_sum[:]+lines_out[1:]
    
    
    for i in lines_out:
        print(i)
    
    with open(output_file, 'w') as f:
        for i in lines_out:
            print(i,file=f)
        
    print("")
    print("Results written to "+output_file)
    print("Debug doc written to "+debug_doc_file)

