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
#File: cx2d_lib.py.
#Author: A.J. Vazquez Alvarez (ajvazquez@haystack.mit.edu)
#Description:
"""
Module with basic functions to convert output from CorrelX/CX to DiFX/SWIN+PCAL.

 The objective of this library is to connect the CorrelX processing chain with:
 | *vex2difx+calcif2 (input): for generating the configuration files.
 | *difx2mark4 (output): for generating a mark4 fileset that can be processed with fourfit.

Conventions CorrelX
-------------------
    Make sure that the same file const_mapred.py used for the correlation is imported here, since the fields in the header are
      accesed using the constants const_mapred.INDEX_*.

Conventions DiFX
----------------
    Conventions are as defined in:
    |  DiFX/SWIN [DIFX_*]: CSIRO - http://www.atnf.csiro.au/vlbi/dokuwiki/doku.php/difx/files#the_swin_output_data_format (2016.01.12)
    |  DiFX/PCAL [PCAL_*]: [Br14] NRAO - A Guide to the DiFX Software Correlator (Version 2.2), Section 6.8.2. Pulse cal data files. (2014.06.23)
    |  DiFX/SWIN [*.im,*.input]: CSIRO - http://www.atnf.csiro.au/vlbi/dokuwiki/doku.php/difx/files (2016.01.12)

Sections of code
----------------
    | CX output (read)
    | CX output (debug)
    | DiFX/SWIN (write)
    | DiFX/SWIN (read)
    | Processing zoombands: CX -> CX_zoomed
    | Conversion CorrelX/CX -> DiFX/SWIN
    | Conversion CorrelX/CX -> DiFX/PCAL
    | Conversion CorrelX/CX -> DiFX/SWIN+PCAL
    | DiFX/.im,.input parser tools
    | Conversion DiFX/.im -> CorrelX/delay_model+sources
    | Conversion DiFX/.input -> CorrelX/stations+correlation+media
    
(!) Limitations
---------------
    | Single source. source_index=0
    | Single configuration. configuration_index=0
    | Single phase_centre. phase_centre=0
    | Single pulsar_bin. pulsar_bin=0
    | Data weight is forced to be 1
    | u,v,w are forced to be 0
    | Averaging currently only implemented for zoom bands
    | Zoom bands currently implemented during postprocessing

Notes
-----
 All constants are kept in this file due to strong dependence on external references.
 
TO DO
-----
 | Create separate library and move there the CX output file processing functions.
 | Configuration conversion libraries: experimental, see limitations for input_to_media().
 | Print remainder and do checks for CX_IMPORT_CONST_MAPRED.
 | Remove back_compat option (replace by custom const_mapred import).

"""
#History:
#initial version: 2016.01 ajva
#MIT Haystack Observatory

from __future__ import print_function



###########################################
#           Matplotlib support
###########################################
ENABLE_PLOTTING=1                                        # Disable plotting if running on a machine without matplotlib
#                                                        # Currently: use 0 for Python2, 1 for Python3





###################################################################################### CorrelX/version

###########################################
#         CorrelX/CX version
###########################################
CX_IMPORT_CONST_MAPRED="latest"                          # Latest header format (const_mapred.py)
#CX_IMPORT_CONST_MAPRED="2016.08.04"                      # Legacy header format (const_mapred_legacy_20160804.py)
CX_OVERRIDE_META_LEN=-1                                  # if <=0 take META_LEN from const_mapred
#CX_OVERRIDE_META_LEN=27                                  # if >0 take this value for META_LEN (number of header metadata fields)






###################################################################################### CorrelX/ini

###########################################
#         CorrelX/ini
###########################################
CX_DEFAULT_MEDIA_DIR="media"                             # Folder relative to ini folder to place symbolyc links to media






###################################################################################### DiFX/version

###########################################
#   DiFX/SWIN+PCAL version (only display)
###########################################
REFERENCE_SWIN_WEB_VERSION=  "2016.01.12"                # Report this on top of this file.
REFERENCE_DIFX_GUIDE_VERSION="2014.06.23"                # Report this on top of this file.






###################################################################################### DiFX/SWIN

###############################################
#          DiFX/SWIN - filename
###############################################
DIFX_FILENAME_PREFIX=         "DIFX"
DIFX_FILENAME_SEP=            "_"
DIFX_FILENAME_SUFFIX=         ".s0000.b0000"             

###########################################
#  DiFX/SWIN - forced values for writting
###########################################
SYNC_WORD=             b'\x00\xff\x00\xff'               # This sync word is for little endiannes
FORCED_HEADER_VERSION= 1                                 # Header version
FORCED_SOURCE_INDEX=   0                                 # Source id
FORCED_CONFIG_INDEX=   0                                 # Configuration index
FORCED_PULSAR_BIN=     0                                 # Pulsar bin
FORCED_WEIGHT=         1.0                               # Weight
FORCED_U=              0.0                               # U [m]
FORCED_V=              0.0                               # V [m]
FORCED_W=              0.0                               # W [m]

###########################################
#   DiFX/SWIN - endiannes for writting
###########################################
END_SWIN_INT=         "<I"                               # Integer (used in header metadata
END_SWIN_DOUBLE=      "<d"                               # Double  (used in header metadata)
END_SWIN_CHAR=        "<c"                               # Char    (used in header polarization pair)
END_SWIN_FLOAT=       "<f"                               # Foat    (used in visibilities)

###########################################
#  DiFX/SWIN - Number of bytes for reading
###########################################
NUM_BYTES_SWIN_INT=    4
NUM_BYTES_SWIN_DOUBLE= 8
NUM_BYTES_SWIN_FLOAT=  4
NUM_BYTES_SWIN_SYNC=   4
NUM_BYTES_SWIN_HEADER=74

###############################################
#  DiFX/SWIN - positions of fields for reading
###############################################
POS_SWIN_SYNC=         0                                # Note that reference (web) starts numbering at 1, here at 0
POS_SWIN_VERSION=      4
POS_SWIN_BASELINE=     8
POS_SWIN_MJD=         12
POS_SWIN_SECONDS=     16
POS_SWIN_CONFIG=      24
POS_SWIN_SOURCE=      28
POS_SWIN_FREQ=        32
POS_SWIN_POL0=        36
POS_SWIN_POL1=        37
POS_SWIN_PULSAR=      38
POS_SWIN_WEIGHT=      42
POS_SWIN_U=           50
POS_SWIN_V=           58
POS_SWIN_W=           66




###################################################################################### DiFX/PCAL

###############################################
#          DiFX/PCAL - filename
###############################################
PCAL_FILENAME_PREFIX=         "PCAL"
PCAL_FILENAME_SEP=            "_"
# fixed length seconds
PCAL_FILENAME_SECONDS_ZFILL=  6                      

###############################################
#         DiFX/PCAL - header lines
###############################################
PCAL_HEADER_TITLE=            "# cx-derived pulse cal data"
PCAL_HEADER_VERSION=          "# File version = "
PCAL_HEADER_MJD=              "# Start MJD = "
PCAL_HEADER_SECONDS=          "# Start seconds = "
PCAL_HEADER_TELESCOPE=        "# Telescope name = "
PCAL_HEADER_VERSION_VAL=      1

###############################################
#         DiFX/PCAL - record format
###############################################
# float point value format
PCAL_SECONDS_FLOAT_FORMAT=     "%.7f"
PCAL_TONE_SCI_FORMAT=          "%.5e"
# invalid record
PCAL_INVALID_RECORD_STR=       "-1 0 0 0"




###################################################################################### DiFX/.im+input

###########################################
#  DiFX/.im+input - general
###########################################
# separator parameter: value
C_DIFX_SEPARATOR=             ":"                      

###########################################
#   DiFX/.im - patterns to find lines
###########################################
# delay model info
C_DIFX_IM_DELAY_US=           "DELAY (us)"
C_DIFX_IM_DRY_US=             "DRY (us)"
C_DIFX_IM_INTERVAL_SECS=      "INTERVAL (SECS)"
C_DIFX_IM_MJD=                "MJD"
C_DIFX_IM_POLY=               "POLY"
C_DIFX_IM_SCAN=               "SCAN"
C_DIFX_IM_SEC=                "SEC"
C_DIFX_IM_WET_US=             "WET (us)"
# sources info
C_DIFX_IM_POINTING_SRC=       "POINTING SRC"

###########################################
#  DiFX/.input - patterns to find lines
###########################################
# stations info
C_DIFX_INPUT_CLOCK_COEFF=     "CLOCK COEFF"
C_DIFX_INPUT_CLOCK_REF_MJD=   "CLOCK REF MJD"
# correlation info
C_DIFX_INPUT_TELESCOPES=      "TELESCOPE ENTRIES"
C_DIFX_INPUT_EXECUTE_TIME=    "EXECUTE TIME (SEC)"
C_DIFX_INPUT_INT_TIME=        "INT TIME (SEC)"
C_DIFX_INPUT_NUM_CHANNELS=    "NUM CHANNELS 0"
C_DIFX_INPUT_PHASE_CALS=      "PHASE CALS 0 OUT"
C_DIFX_INPUT_START_MJD=       "START MJD"
C_DIFX_INPUT_START_SECONDS=   "START SECONDS"
# media info
C_DIFX_INPUT_BW=              "BW (MHZ)"
C_DIFX_INPUT_FREQ=            "FREQ (MHZ)"
C_DIFX_INPUT_BASELINE_TABLE=  "BASELINE TABLE"
C_DIFX_INPUT_COMPLEX=         "COMPLEX"
C_DIFX_INPUT_DATA_SAMPLING=   "DATA SAMPLING"
C_DIFX_INPUT_DATA_SOURCE=     "DATA SOURCE"
C_DIFX_INPUT_FILE=            "FILE "
C_DIFX_INPUT_FILES=           "FILES"
C_DIFX_INPUT_INDEX=           "INDEX"
C_DIFX_INPUT_PHASE_CAL_INT=   "PHASE CAL INT (MHZ)"
C_DIFX_INPUT_POL=             "POL"
C_DIFX_INPUT_REC_BAND=        "REC BAND"
C_DIFX_INPUT_REC_FREQ=        "REC FREQ"
C_DIFX_INPUT_SIDEBAND=        "SIDEBAND"
C_DIFX_INPUT_TELESCOPE_INDEX= "TELESCOPE INDEX"
C_DIFX_INPUT_TELESCOPE_NAME=  "TELESCOPE NAME"
C_DIFX_INPUT_TELESCOPE_TABLE= "TELESCOPE TABLE"



###########################################
#          Import
###########################################
import imp
# Constants for mapper and reducer

if CX_IMPORT_CONST_MAPRED=="latest":
    import const_mapred              # CX output separators and field locations (!) Make sure it is the same as used in correlation.
    imp.reload(const_mapred)
    from const_mapred import *

elif CX_IMPORT_CONST_MAPRED=="2016.08.04":
    import const_mapred_legacy_20160804            
    imp.reload(const_mapred_legacy_20160804)
    from const_mapred_legacy_20160804 import *

if CX_OVERRIDE_META_LEN>0:
    META_LEN=CX_OVERRIDE_META_LEN

#print("CX header version: "+CX_IMPORT_CONST_MAPRED)
#print("CX meta len: "+str(META_LEN))

import lib_ini_files                 # CX ini files
imp.reload(lib_ini_files)
from lib_ini_files import *

import lib_acc_comp                  # CX accumulation periods
imp.reload(lib_acc_comp)
from lib_acc_comp import *

import numpy as np
import struct
import os
import operator

                                     # Plotting
try:
    import matplotlib as mpl
except ImportError:
    #print("Matplotlib not available, proceeding with no plots!")
    ENABLE_PLOTTING=0

if ENABLE_PLOTTING:
    #mpl.use('PS')
    mpl.use('Agg')
    #mpl.use("pgf")
    import matplotlib.pyplot as plt
    import matplotlib.font_manager as font_manager
    from matplotlib.legend import Legend


    
    
###########################################
#          CX output (read)
###########################################    
    
    
  
def split_line_correct_tab(line):
    """
    Separate key and value in CX line.
    
    Update if changes to msvf.get_pair_str are done (key separator FIELD_SEP+KEY_SEP).
    """
    line_split = line.strip().split(KEY_SEP)
    if len(line_split)>2:
        line_split[1]+=(FIELD_SEP+line_split[2])
    #print(line_split)
    meta = line_split[0]
    try:
        data = line_split[1]
    except IndexError:
        print("IndexError splitting line")
        print(line_split)
    return([meta,data])  
    
  


def read_line_cx(line,fft_size=-1):
    """
    Read a line from a CX file (and check number of visibilities if required).
    
    Parameters
    ----------
     line : str
          line from CX file.
     fft_size : int,optional
          number of coefficients in the visibilities (or pcal bins).
     
    Returns
    -------
     meta : str
         line header.
     st0 : int
         first station of the baseline.
     st1 : int
         second station of the baseline.
     key : int
         absolute key (see msvf.get_pair_str().key_value).
     vis : int
         accumulation period.
     chan : int
         band id.
     pol0 : int
         polarization id for st0.
     pol1 : int
         polariation id for st1.
     predata : str
         metadata fields.
     datac : complex 1D np.array
         visibilities.
    """
    
    [meta,data] = split_line_correct_tab(line)


    [pxs,st0pol0,st1pol1,chanvis,acctot] = meta.split(FIELD_SEP)
    [st0s,pol0s] = st0pol0.split(SF_SEP)
    [st1s,pol1s] = st1pol1.split(SF_SEP)
    [keys,viss,chans] = chanvis[2:].split(SF_SEP)
    accs = acctot[3:]
    
    st0 = int(st0s)
    pol0 = int(pol0s)
    st1 = int(st1s)
    pol1 = int(pol1s)
    key = int(keys)
    chan = int(chans)
    vis = int(viss)
    acc = int(accs)
    
    predata=' '.join(data.split(' ')[:META_LEN])
    if fft_size>0 and len(data.split(' ')[META_LEN:])<fft_size:
        datac=None
    else:
        datac=np.asarray(data.split(' ')[META_LEN:]).astype(complex)
    
    header_data_split = data.split(' ')[:META_LEN]
    n_bins = int(header_data_split[INDEX_NBINS_PCAL])
    pcal_freq = int(float(header_data_split[INDEX_PCAL_FREQ])//1)
    chan_index = int(header_data_split[INDEX_CHANNEL_INDEX])
    acc_period = vis  
    
    fs = float(data.split(' ')[INDEX_FS])
    
    return([meta,st0,st1,key,vis,chan,pol0,pol1,n_bins,pcal_freq,chan_index,acc_period,fs,predata,datac])


def read_cxoutput(cxoutput_file,v=1,back_compat=0):
    """
    Read cx output file into list.
    
    Parameters
    ----------
     cxoutput_file : str
         path to cx file.
     v : int
         verbose if 1.
     back_compat : int,optional
         [default 0][remove]
    
    Returns
    -------
     list_output : list
                             list of [st0,st1,vis,chan,pol0,pol1,diff_st] elements where:
                                 | st0:      first station in the baseline.
                                 | st1:      second station in the baseline.
                                 | vis:      accumulation period id.
                                 | chan:     channel id.
                                 | pol0:     polarization id for st0.
                                 | pol1:     polarization id for st1.
                                 | datac:    complex 1D np.array with visibilities.
                                 | diff_st:  field used for sorting.
    
    Notes
    -----
    |
    | **TO DO**
    |
    |    Sorting convention based on sorting based on actual SWIN files. Check method and find proper reference.
    |    Remove back_compat in libraries calling this function.
    """
    
    list_output=[]
    reduced_list=[]
    
    with open(cxoutput_file, 'r') as f:
        lines = f.readlines()
        for line in lines:
            if "px" in line[:2]:
                
                print(line[:80]) 
                [meta,st0,st1,key,vis,chan,pol0,pol1,n_bins,\
                      pcal_freq,chan_index,acc_period,fs,predata,datac] = read_line_cx(line)
        
               
                # for sorting
                diff_st=1000000-np.abs(st0-st1)
                list_output+=[[st0,st1,vis,chan,pol0,pol1,datac,diff_st]]
                
                # for display
                reduced_list+=[[st0,st1,vis,chan,pol0,pol1,diff_st]]
                
                if v==1:
                    print(meta)
                    print(" Sts.:  "+str(st0)+","+str(st1))
                    print(" Vis.: "+str(vis)) 
                    print(" Chan.: "+str(chan))
                    print(" Pols.: "+str(pol0)+","+str(pol1))
              

    if v==1:
        print(sorted(reduced_list, key = lambda x: (x[:7])))
    
    #sorted_list=list_output
    

    return(list_output)    
    

###########################################
#          CX output (debug)
###########################################    


def get_list_meta(const_file="const_mapred.py",path_src=""):
    """
    Get list with metadata fields from source file const_mapred.py.
    
    Use for debugging.
    
    Parameters
    ----------
     const_file : str
         path to const_mapred file.
     path_src : str
         path to location of source file const_mapred.py.
     
    Returns
    -------
     line_st_v : list of str
         all metadata fields (without INDEX_).
     
    Notes
    -----
     Hardcoded values based on const_mapred.py, use only for debugging.
    """
    const_file = path_src+"/"+const_file
    mode_read=0
    line_st_v=[]
    filter_str="INDEX_"
    with open(const_file, 'r') as f:
        lines = f.readlines()
        line_counter=0
        for line in lines:
            line_st=line.strip()
            
            if filter_str in line_st and "[" in line:
                mode_read=1
            
            if filter_str in line_st and mode_read:
                if "[" in line_st:
                    line_st=line_st.split("[")[1]
                if "," in line_st:
                    line_st=line_st.split(",")[0]
                if "]" in line_st:
                    line_st=line_st.split("]")[0]
                line_st=line_st.split("INDEX_")[1]
                line_st_v.append(line_st)
            
            if filter_str in line_st and "]" in line:
                mode_read=0
                
    return(line_st_v)
      
    
    
def show_line_cx(file_in,line_start,line_count,filter_line="px",v=1,path_src=""):
    """
    Display metadata fields and number of coefficients in the visibilities. 
    
    Use for debugging.
    
    Parameters
    ----------
     file_in : str
         path to CX file.
     line_start : int
         first line to read.
     line_count : int
         number of lines to read (-1 for no limit).
     filter_line : str
         pattern to filter lines (exact match at line start) to be displayed.
     v : int
         verbose if 1.
     path_src : str
         path to location of source file const_mapred.py.
     
    Returns
    -------
     results : list of [str,list of float]
         keys and the associated visibilities.
    """
    const_file="const_mapred.py"
    ljust_len=25
    if v:
        print("cx2d configuration:")
        print("-"*19)
        print("CX_IMPORT_CONST_MAPRED: ".ljust(ljust_len)+str(CX_IMPORT_CONST_MAPRED))
        print("CX_OVERRIDE_META_LEN: ".ljust(ljust_len)+str(CX_OVERRIDE_META_LEN))
        print("META_LEN:".ljust(ljust_len)+str(META_LEN))
        print("Metadata fields from: ".ljust(ljust_len)+str(const_file))
        print("")
        print("Lines:")
        print("-"*6)
    list_meta=get_list_meta(const_file="const_mapred.py",path_src=path_src)
    
    results=[]
    len_filter_line=len(filter_line)
    with open(file_in, 'r') as f:
        lines = f.readlines()
        line_counter=0
        for line in lines:
            if filter_line in line[:len_filter_line]:
                if (line_counter>=line_start):
                    if line_counter<=line_start+line_count or line_count<0:
                        #print(line.strip())
                        [meta,data] = split_line_correct_tab(line)
                        data_split=data.split(" ")
                        if v:
                            print("Line id ".ljust(ljust_len)+str(line_counter))
                            print("Key: ".ljust(ljust_len)+meta)
                            for i in range(len(list_meta)):
                                print((list_meta[i]+": ").ljust(ljust_len)+data_split[i])
                            print("Num. visibilities: ".ljust(ljust_len)+str(len(data_split[len(list_meta):])))
                            print(" ")
                        results.append([meta,np.array(list(map(complex,data_split[len(list_meta):])))])
                        line_count-=1
                line_counter+=1
                if line_count==0:
                    break
    return(results)


def get_error_indicator(file_vis_1,file_vis_2,force=0,path_src=""):
    """
    Provides the sum of the L2-norm between all pairs of visibilities.
    Use only for debugging comparing two CorrelX output files (e.g. testing changes).
    
    Parameters
    ----------
     file_vis_1 : str
         path to the reference file with visibilities.
     file_vis_2 : str
         path to the test file with visibilities.
     force : int
         continue execution even if metadata differ.
     path_src : str
         path to location of source file const_mapred.py.
     
    Returns
    -------
     None
     
    Notes
    -----
    |
    | **Assumptions / TO DO**
    |
    |    Currently assuming that then number of coefficients does not change for lines in the same file.
    |    Currently no error checking if forcing execution (e.g. different number lines).
    """

    error_diff=0
    acc_res=0
    i=-1
    jv=30
    if force:
        print(" WARNING! Forcing execution even if headers differ!")

    print(" File 1:".ljust(jv)+file_vis_1)
    print(" File 2:".ljust(jv)+file_vis_2)

    vis1 = show_line_cx(file_vis_1,line_start=0,line_count=-1,v=0,path_src=path_src)
    vis2 = show_line_cx(file_vis_2,line_start=0,line_count=-1,v=0,path_src=path_src)
    len_vis1=len(vis1)
    len_vis2=len(vis2)
    if len_vis1!=len_vis2:
        print("  Different number of lines: "+str(len_vis1)+" "+str(len_vis2))
        error_diff=1

    for i in range(len(vis1)):
        if force or not(error_diff):
            meta1=vis1[i][0]
            meta2=vis2[i][0]
            if meta1!=meta2:
                print(" DIFF: Different headers. (Line "+str(i)+"): "+meta1+" "+meta2)
                error_diff=1
                if not force:
                    break
            len1=len(vis1[i][1])
            len2=len(vis2[i][1])
            if len1!=len2:
                print(" DIFF: Different number of coefficients. (Line "+str(i)+"): "+str(len1)+" "+str(len2))
                error_diff=1
                if not force:
                    break
            
            sub=np.subtract(np.array(vis1[i][1]),np.array(vis2[i][1]))
            acc_res+=np.real(np.dot(sub,np.conj(sub))) # Imaginary part will be zero, do not show warning

    print(" Visibilities compared:".ljust(jv)+str(i+1))
    
    if force or not(error_diff):
        print(" Num. coefficients per vis.:".ljust(jv)+str(len1))
        print(" Total error:".ljust(jv)+str(acc_res))
    else:
        print(" Num. coefficients per vis.:".ljust(jv)+"N/A")
        print(" Total error:".ljust(jv)+"N/A")



def plot_vis_cx(file_input,title_figure,mode_in="px",max_lines=-2,interval_start=0,\
             interval_end=-1,filter_str=""):    
    """
    Basic function for plotting output, one plot per band.
    
    Parameters
    ----------
     file_input : str
         path to the visibilities file.
     title_figure : str
         prefix for the title of the figures
     mode_in : str
         prefix of cx file lines.
     max_lines : int,optional
         maximum number of lines to read.
     interval_start: int,optional
         first coefficient to plot.
     interval_end : int,optional
         last coefficient to plot.
     filter_str : str,optional
         filter lines with this string (for example only a certain baseline).
     
    Returns
    -------
     N/A
    """
    
    
    #Prepare files
    f_in = open(file_input)
    
    #Figure
    
    index_plt=3
    it_lines=-1
    for line in f_in:
        if filter_str!="":
            if filter_str not in line:
                continue
        if max_lines!=-1:
            it_lines+=1
        if it_lines==max_lines:
            break
        if mode_in in line[:len(mode_in)]:
            line = line.strip()
            
            try:
                key, vector = line.split('\t',1)
            except ValueError:
                #['']
                continue
            
            
            index_plt+=1
            fig = plt.figure(index_plt)
            fig.clf()
            plt.subplot(121)
            
            yf=[complex(i) for i in vector.split(' ')[META_LEN:]]
            
            freq_sample=(complex(vector.split(' ')[1])).real
            
            N=len(yf)
            
            interval_end_mod=interval_end
            if interval_end_mod==-1:
                interval_end_mod=N
            
            x = list(range(interval_start,interval_end_mod))
            
            plt.subplot(121)
            plt.plot(x,np.abs(yf[interval_start:interval_end_mod]),'o-')
            plt.xticks(rotation='vertical')
            plt.title('Magnitude')
            plt.grid()
    
            plt.subplot(122)
            plt.plot(x,np.angle(yf[interval_start:interval_end_mod]),'o-')
            plt.xticks(rotation='vertical')
            plt.title('Phase')
            plt.grid()
    
    
            #Show results
            fig.suptitle(title_figure + " " + key+" ["+str(interval_start)+":"+\
                         str(interval_end_mod)+"]", fontsize=12)
            plt.show()
    f_in.close()

###########################################
#          DiFX/SWIN (write)
###########################################

def compute_baseline_num_swin(st0,st1):
    """
    Takes two integers with IDs for stations are computes baselines number as 256*([st0]+1)+([st1]+1).
    
    Parameters
    ----------
     st0,st1 : int
         values for station 0 and station 1 starting at zero.
         
    Returns
    -------
     baseline_num : int
         baseline number
    """
    baseline_num = 256*(st0+1)+(st1+1)
    return(baseline_num)



def sort_swin_records(output_list):
    """
    Sort SWIN output records.
    
    Parameters
    ----------
     output_list :  list of [st0,st1,vis,chan,pol0,pol1,header,values_bytes,diff_st]
         see convert_cx2d.output_list.
    
    Notes
    -----
    |
    | **Sorting is as follows:**
    |
    |    1. accumulation period
    |    2. term based on stations ids (see read_cxoutput())
    |    3. first station of the baseline
    |    4. second station of the baseline
    |    5. band
    |    6. polarization for first station
    |    7. polarization for second station
    |
    | **TO DO:**
    |
    |    x[2] duplicated, remove second one 
    """
    #  0   1   2   3     4    5    6         7          8
    #[st0,st1,vis,chan,pol0,pol1,header,values_bytes,diff_st]
    
    #                                           vis diff_st st0   st1  vis chan pol0 pol1
    #return(sorted(output_list, key = lambda x: (x[2], x[8], x[0],x[1],x[2],x[3],x[4],x[5])))
    return(sorted(output_list, key = lambda x: (x[2], x[8], x[0],x[1],x[2],x[4],x[5],x[3])))



def create_header_swin(st0,st1,vis,chan,pol0,pol1,mjd,seconds_start,accumulation_period,polarization_chars_list,\
                       source_index=FORCED_SOURCE_INDEX,config_index=FORCED_CONFIG_INDEX,pulsar_bin=FORCED_PULSAR_BIN,\
                       header_version=FORCED_HEADER_VERSION,forced_weight=FORCED_WEIGHT,forced_u=FORCED_U,forced_v=FORCED_V,\
                       forced_w=FORCED_W):
    """
    Byte representation for one header.
    
    Parameters
    ----------
     st0 : int
         first station in the baseline.
     st1 : int
         second station in the baseline.
     vis : int
         accumulation period id.
     chan : int
         channel id.
     pol0 : char
         polarization id for st0.
     pol1 : char
         polarization id for st1.
     mjd : int
         MJD for this accumulation period.
     seconds_start : float
         seconds corresponding to the middle of the first accumulation period.
     accumulation_period : float
         duration of the accumulation period in seconds.
     polarization_chars_list : list of str
         polarization characters to accessed by id (pol0 and pol1).
     
     source_index : int,optional
         See constants on top of this file.
     config_index : int,optional
     pulsar_bin : int,optional
     header_version : int,optional
     forced_weight : float,optional
     forced_u : float,optional
     forced_v : float,optional
     forced_w : float,optional
    
    Returns
    -------
     header_list : byte array 
         header ready to be written to a file.
    
    Notes
    -----
    | Optional arguments are replaced by forced values.
    | Note that secnods start is not at the start of the first accumulation period but at the middle!
    | Values for st0,st1,vis,chan,pol0,pol1 starting at 0.
    | 
    |
    | **TO DO:**
    |
    |    Change input floats by double as applicable.
    """
    
    # Computations
    baseline_num = compute_baseline_num_swin(st0,st1)
    seconds = seconds_start+accumulation_period*vis
    
    # Display tabulated info
    print(str(vis                          ).ljust(5)+\
          str(seconds                      ).rjust(10)+\
          str(accumulation_period          ).rjust(7)+\
          str(chan                         ).rjust(7)+"    "+\
          str(polarization_chars_list[pol0])+\
          str(polarization_chars_list[pol1]))
    

    header_bytes =                  struct.pack(END_SWIN_INT,   FORCED_HEADER_VERSION)
    baseline_bytes =                struct.pack(END_SWIN_INT,   baseline_num)
    version_bytes =                 struct.pack(END_SWIN_INT,   header_version)
    mjd_bytes =                     struct.pack(END_SWIN_INT,   mjd)
    seconds_bytes =                 struct.pack(END_SWIN_DOUBLE,seconds)
    config_index_bytes =            struct.pack(END_SWIN_INT,   config_index)
    source_index_bytes =            struct.pack(END_SWIN_INT,   source_index)
    freq_index_bytes =              struct.pack(END_SWIN_INT,   chan)
    

    #if ENABLE_PLOTTING:
    if sys.version_info[0]>2:
        # Python3
        polarization_pair_bytes =   struct.pack(END_SWIN_CHAR,  bytes(polarization_chars_list[pol0],'utf-8'))
        polarization_pair_bytes +=  struct.pack(END_SWIN_CHAR,  bytes(polarization_chars_list[pol1],'utf-8'))
    else:
        # Python2
        polarization_pair_bytes =   struct.pack(END_SWIN_CHAR,  polarization_chars_list[pol0])
        polarization_pair_bytes +=  struct.pack(END_SWIN_CHAR,  polarization_chars_list[pol1])

    
    pulsar_bin_bytes =              struct.pack(END_SWIN_INT,   pulsar_bin)
    forced_weight_bytes =           struct.pack(END_SWIN_DOUBLE,forced_weight)
    forced_u_bytes =                struct.pack(END_SWIN_DOUBLE,forced_u)
    forced_v_bytes =                struct.pack(END_SWIN_DOUBLE,forced_v)
    forced_w_bytes =                struct.pack(END_SWIN_DOUBLE,forced_w)
    
    
    # Header elements
    header_list =  b''
    header_list += SYNC_WORD
    header_list += version_bytes
    header_list += baseline_bytes
    header_list += mjd_bytes
    header_list += seconds_bytes
    header_list += config_index_bytes
    header_list += source_index_bytes
    header_list += freq_index_bytes
    header_list += polarization_pair_bytes
    header_list += pulsar_bin_bytes
    header_list += forced_weight_bytes
    header_list += forced_u_bytes
    header_list += forced_v_bytes
    header_list += forced_w_bytes
    

    return(header_list)


def create_bytes_list_visibilities_swin(data_complex_list,only_half=0,duplicate=0,divide_vis_by=1,conjugate_vis_values=0):
    """
    Generate binary representation for one set of visibilities.
    
    Parameters
    ----------
     data_complex_list : 1D numpy arrayof complex
         visibilities (components from 0 to N-1).
     
     only_half : optional,testing
         if 1 takes components [0,N/2-1], if 2 it takes [N/2,N-1].
     duplicate : optional,testing
         duplicate components [consider removing].
     divide_vis_by : optional,testing
         divide all coefficients by this value.
     conjugate_vis_values : optional,testing
         conjugate all visibilities.
    
    Returns
    -------
     header_list : byte array
         visibilities ready to be written to a file.
     
    Notes
    -----
    |
    | **TO DO:**
    |
    |    Consider removing duplicate.
    """    
    
    N=len(data_complex_list)
    values_list=b''
    
    # For old files with FFT not double of FFT in configuration...
    # Will duplicate first half of the samples to extend up to the FFT size
    if only_half==1:
        data_complex_list=data_complex_list[:(N//2)]
    elif only_half==2:
        data_complex_list=data_complex_list[(N//2):]
        
    #plt.plot([i.real for i in data_complex_list])
    data_complex_list=np.divide(data_complex_list,divide_vis_by)
    
    if conjugate_vis_values==1:
        data_complex_list=np.conj(data_complex_list)
    
    for i in data_complex_list:
        values_list += struct.pack(END_SWIN_FLOAT,i.real)
        values_list += struct.pack(END_SWIN_FLOAT,i.imag)
        if duplicate:
            values_list += struct.pack(END_SWIN_FLOAT,i.real)
            values_list += struct.pack(END_SWIN_FLOAT,i.imag)
    return(values_list)


###########################################
#          DiFX/SWIN (read)
###########################################

def compute_stations_num_swin(baseline_num):
    """
    Returns two integers with IDs for station based on SWIN header baseline number.
    See compute_baseline_num_swin().
    
    Notes
    -----
     Values for st0 and st1 starting at zero.
    """
    st0 = int(baseline_num//256)-1
    st1 = baseline_num - (st0+1)*256 -1
    return([st0,st1])


def read_bytes_from_file(f,n_values,read_type="bytes",v=0):
    """
    Read groups of one or four bytes from a binary file.
    
    Parameters
    ----------
     f : handlder to binary file ('rb')
         binary file to read from.
     n_values : int
         number of groups to read.
     read_type : { "bytes" , "floats" }
         "bytes" to read unsigned integer 8 bits, "floats" to read unsigned integer 32 bits.
     v : int
         verbose if 1.
     
    Returns
    -------
     words_array : numpy array of unsigned int of 8 or 32 bits (as configured in read_type).
         read bytes.
     
    Notes
    -----
    |
    | **TO DO:**
    |
    |    Change notation for floats.
    """

    words_array = []
    try:
        #words_array.fromfile(f,n_words)
        if read_type=="floats":
            words_array = np.fromfile(file = f,dtype=np.uint32, count=n_values)
        else:
            words_array = np.fromfile(file = f,dtype=np.uint8, count=n_values)
    except EOFError:
        return([])
    return(words_array)


def get_int_from_header(header,i):
    """
    Read integer from a np.uint8 array (header) starting at position i. 
    """
    return(int(list(       struct.unpack(END_SWIN_INT,   header[i:i+NUM_BYTES_SWIN_INT]))[0]))

def get_double_from_header(header,i):
    """
    Read double from a np.uint8 array (header) starting at position i.
    """
    return(np.float64(list(struct.unpack(END_SWIN_DOUBLE,header[i:i+NUM_BYTES_SWIN_DOUBLE]))[0]))

def get_char_from_header(header,i):
    """
    Read char from a np.uint8 array (header) starting at position i.
    """
    return(str(list(       struct.unpack(END_SWIN_CHAR,  header[i:i+1]))[0])[2])

def get_float_from_header(header,i):
    """
    Read float from a np.uint8 array (header) starting at position i.
    """
    return(float(list(     struct.unpack(END_SWIN_FLOAT,  header[i:i+NUM_BYTES_SWIN_FLOAT]))[0]))


def read_doutput(doutput_file,complex_vector_length,vis_limit=10,filter_bands=[],filter_pols=[],filter_seconds=[],v=0,\
                 interval_start=0,interval_end=-1):
    """
    Plot visibilities from a SWIN file.
    
    Parameters
    ----------
     doutput_file : str
         path to SWIN file.
     complex_vector_length : int
         number of coefficients in the visibilities.
     
     vis_limit : int, optional
         number of visibilities to read (-1 for no limit).
     filter_bands : list of int, optional
         band ids to include ([] to include all). E.g.: [0,1]
     filter_pols : list of str, optional
         band ids to include ([] to include all). E.g.: ["LR","RL"]
     filter_seconds : list of ints, optional
         band ids to include ([] to include all). E.g.: ["0.16","0.48"]
     v : int, optional
         verbose if 1.
    
    Returns
    -------
     N/A
    
    Notes
    -----
    |
    | **Configuration:**
    |
    |    Constant ENABLE_PLOTTING=1 to display plots.
    |
    |
    | **Notes:**
    |
    |    Visbilities are displayed into two figures: magnitude and phase.
    |
    |
    | **TO DO:**
    |
    |    Add checks for header.
    |    Consider automating the computation of complex_vector_length.
    """
    with open(doutput_file,'rb') as f:
        
        continue_reading =1
        visibilities_v = []
        
        while continue_reading:
            
            header =             read_bytes_from_file(f,       NUM_BYTES_SWIN_HEADER)
            if (header != [])and(vis_limit!=0):
                sync_word =                           header[POS_SWIN_SYNC:NUM_BYTES_SWIN_SYNC]
                header_version = get_int_from_header(header,   POS_SWIN_VERSION)
                baseline_num =   get_int_from_header(header,   POS_SWIN_BASELINE)
                mjd =            get_int_from_header(header,   POS_SWIN_MJD)
                seconds =        get_double_from_header(header,POS_SWIN_SECONDS)
                config_index =   get_int_from_header(header,   POS_SWIN_CONFIG)
                source_index =   get_int_from_header(header,   POS_SWIN_SOURCE)
                freq_index =     get_int_from_header(header,   POS_SWIN_FREQ)
                pol0 =           get_char_from_header(header,  POS_SWIN_POL0)
                pol1 =           get_char_from_header(header,  POS_SWIN_POL1)
                pulsar_bin =     get_int_from_header(header,   POS_SWIN_PULSAR)
                data_weight =    get_double_from_header(header,POS_SWIN_WEIGHT)
                u_meters =       get_double_from_header(header,POS_SWIN_U)
                v_meters =       get_double_from_header(header,POS_SWIN_V)
                w_meters =       get_double_from_header(header,POS_SWIN_W)
                
                [st0,st1] = compute_stations_num_swin(baseline_num)
                identifier = str("src"+str(source_index)+" dx-"+str(st0)+"."+pol0+"-"+str(st1)+"."+pol1+"-a"+str(seconds)+\
                                 "."+str(freq_index))
                if v==1:
                    
                    print("".rjust(40)+\
                          str(header_version).rjust(3)+\
                          str(baseline_num  ).rjust(5)+\
                          str(mjd           ).rjust(8)+\
                          str(seconds       ).rjust(15)+\
                          str(config_index  ).rjust(2)+\
                          str(freq_index    ).rjust(2)+\
                          str(pol0          ).rjust(2)+\
                          str(pol1          ).rjust(2)+\
                          str(pulsar_bin    ).rjust(2)+" "+\
                          str(sync_word     ))
                          
                    
                visibilities = []
                data_vis = read_bytes_from_file(f,complex_vector_length*8)
                
                for i in range(complex_vector_length):
                    re_part = get_float_from_header(data_vis,i*8)
                    im_part = get_float_from_header(data_vis,4+i*8)
                    visibilities.append(re_part+1j*im_part)
                
                #print(visibilities)
                visibilities_v.append(visibilities)
                
                id_explain="src<src> dx-<st0>.<pol0>-<st1>.<pol1>-a<seconds>.<freq_index>"
                float_seconds=str(seconds)
                if ((filter_bands!=[])and(freq_index in filter_bands))or(filter_bands==[]):
                    if ((pol0+pol1) in filter_pols) or filter_pols==[]:
                        if ((filter_seconds!=[])and(seconds in filter_seconds))or(filter_seconds==[]):
                            if vis_limit>0:
                                vis_limit-=1
                            if v==1:
                                print(identifier)
                            if ENABLE_PLOTTING:
                                plt.figure(1)
                                plt.plot([np.abs(i)   for i in visibilities[interval_start:interval_end]],label=identifier)
                                plt.figure(2)
                                plt.plot([np.angle(i) for i in visibilities[interval_start:interval_end]],label=identifier)
                            else:
                                print("WARNING: plotting disabled in cx2d_lib")
                if ENABLE_PLOTTING:
                    plt.figure(1)
                    plt.title("Magnitude")
                    plt.figure(2)
                    plt.title("Phase")
            else:
                if vis_limit==0:
                    if v==1:
                        print("Reached requested limit")
                continue_reading=0
        
        if ENABLE_PLOTTING:
            for i in [1,2]:
                plt.figure(i)
                legendfig=plt.legend(title=id_explain,bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
            
###########################################
#   Processing zoombands: CX -> CX_zoomed
###########################################  


def get_pos_in_fft(f,fz,fft_size,bw):
    """
    Get the coefficient corresponding to a certain frequency in the visibilities.
    
    Parameters
    ----------
     f : float
         lower edge frequency of the band.
     fz : float
         frequency between f and f+bw (which associated coefficient is to be found).
     fft_size : int
         number of coefficients of the visibilities.
     bw : float
         bandwidth of the full band.
    
    Returns
    -------
     output : int
         position of the first coefficient.
    
    Notes
    -----
    |
    | **TO DO:**
    |
    |    LSB/USB.
    """
    return(int(((fft_size)*float(fz-f)/bw)//1))
    #return(int((fft_size//2)+((fft_size//2)*float(fz-f)/bw)//1))


def get_zoom_list(params_array_media,params_array_corr,v=0,average_channels=-1):
    """
    Get the information required to process the zoom bands.
    
    Parameters
    ----------
     params_array_media : list
         list with media configuration (see lib_ini_files.py).
     params_array_corr : list
         list with correlation configuration (see lib_ini_files.py).
     v : int
         verbose if 1.
     average_channels : int
         number of coefficients that are average into one (here only used for reporting).
     
    Returns
    -------
     z_list : list
         list of elements [band_id, first_sample_fft, last_sample_fft, new_band_id] where:
                      |  band_id:           id of the processed band.
                      |  first_sample_fft:  position of the first element of the zoom in the full fft.
                      |  last_sample_fft:   position of the last element of the zoom in the full fft.
                      |  new_band_id:       id of the newly created band (zoom).
     
    Notes
    -----
    |
    | **Assumptions:**
    |
    |    It is assumed that a channel has only one associated sideband (this is checked when computing the
    |      limits of the band, if lower sideband the upper edge is given.
    |    It is assumed that zoom bands are always upper side band.
    |
    |
    | **TO DO:**
    |
    |    LSB/USB for zoom band definitions...
    """
    str_megahz=" MHz"
    ljustid=7
    ljustz=15
    
    # Lower edges for zoom frequency windows
    zoom_freq_v=[int(float(val)//1) for val in get_param_eq_vector(params_array_media,C_INI_MEDIA_ZOOM_POST,\
                                                                   C_INI_MEDIA_ZP_FREQ,modein="str")]
    # Bandwidths for zoom frequency windows
    zoom_bw_v=  [int(float(val)//1) for val in get_param_eq_vector(params_array_media,C_INI_MEDIA_ZOOM_POST,\
                                                                   C_INI_MEDIA_ZP_BW,modein="str")]
    # Upper edges for zoom frequency windows
    zoom_freq_v_e=list(np.add(zoom_freq_v,zoom_bw_v))
    
    
    file_list=get_val_vector(params_array_media,C_INI_MEDIA_S_FILES,C_INI_MEDIA_LIST)
    channels_asoc_vector=[]
    sideband_asoc_vector=[]
    for fi in file_list:
        channels_asoc_vector+=[val for val in get_val_vector(params_array_media,fi,C_INI_MEDIA_CHANNELS)]
        sideband_asoc_vector+=[val for val in get_val_vector(params_array_media,fi,C_INI_MEDIA_SIDEBANDS)]

    # Set of channels
    channels_set_v=list(set(channels_asoc_vector))
    # Sidebands associated to those channels
    sideband_set_v=[sideband_asoc_vector[y] for y in \
                      [channels_asoc_vector.index(x) for x in channels_set_v]]
    sideband_mult=[int(x=='L') for x in sideband_set_v]
    
    # Edges for full bands
    freq_v = [int(float(get_param_serial(params_array_media,C_INI_MEDIA_FREQUENCIES,i))//1) for i in channels_set_v]
    
    # Bandwidths for full bands
    bw_v = [int(float(get_param_serial(params_array_media,C_INI_MEDIA_BANDWIDTHS,i))//1) for i in channels_set_v]
    
    # Lower edges for full bands (subtract BW to edge if LSB)
    freq_v = [x-y*z for (x,y,z) in zip(freq_v,bw_v,sideband_mult)]
    

    # Upper edges for full bands
    freq_v_e=list(np.add(freq_v,bw_v))
    
    

    # Number of coefficients in full band
    fft_size = int(get_param_serial(params_array_corr,C_INI_CR_S_COMP,C_INI_CR_FFT))
    
    # Number of bands
    all_channels = get_all_params_serial(params_array_media,C_INI_MEDIA_FREQUENCIES)
    tot_channels = len(all_channels)
    first_index_zoom = tot_channels
    
    # For every zoom window, iterate on all bands, and if the zoom window corresponds to the band, store the required info
    z_list = []
    index_zoom = first_index_zoom
    
    if v==1:
        print(zoom_freq_v)
        print(zoom_freq_v_e)
        print(channels_asoc_vector)
        print(channels_set_v)
        print(freq_v)
        print(freq_v_e)
        print(bw_v)
    
    
    print("Band".ljust(ljustid)+\
          "Zoom".ljust(ljustid)+\
          "Band BW".ljust(ljustz)+\
          "Zoom BW".ljust(ljustz)+\
          "Band fft".ljust(ljustz)+\
          "Zoom fft".ljust(ljustz)+\
          "Zoom avg fft".ljust(ljustz)+\
          "Band freq".ljust(ljustz)+\
          "Zoom freq".ljust(ljustz))
    
    for i_z in range(len(zoom_freq_v)):
        for i_f in range(len(freq_v)):
            z_item = []
            if zoom_freq_v[i_z]>=freq_v[i_f] and zoom_freq_v_e[i_z]<=freq_v_e[i_f]:
                
                # Zoom positions
                zoom_start=get_pos_in_fft(freq_v[i_f],zoom_freq_v[i_z],  fft_size,bw_v[i_f])
                zoom_end=  get_pos_in_fft(freq_v[i_f],zoom_freq_v_e[i_z],fft_size,bw_v[i_f])
                
                # Display zoom band info
                zoom_bw_str= str(float(zoom_freq_v_e[i_z]-zoom_freq_v[i_z])/1e6)+str_megahz
                zoom_len_str=str(zoom_end-zoom_start)
                zoom_avg_str=zoom_len_str
                if average_channels>0:
                    zoom_avg_str=str((zoom_end-zoom_start)//average_channels)
                bw_str=      str(float(bw_v[i_f]                          )/1e6)+str_megahz
                freq_str=    str(float(freq_v[i_f]                        )/1e6)+str_megahz
                freq_z_str=  str(float(zoom_freq_v[i_z]                   )/1e6)+str_megahz
                print(str(i_f).ljust(ljustid)+str(index_zoom).ljust(ljustid)+\
                      bw_str.ljust(ljustz)+\
                      zoom_bw_str.ljust(ljustz)+\
                      str(fft_size).ljust(ljustz)+\
                      zoom_len_str.ljust(ljustz)+\
                      zoom_avg_str.ljust(ljustz)+\
                      freq_str.ljust(ljustz)+\
                      freq_z_str.ljust(ljustz))

                # Create output list element
                z_item.append(all_channels.index(channels_set_v[i_f]))
                z_item.append(zoom_start)
                z_item.append(zoom_end)
                z_item.append(index_zoom)
                index_zoom+=1
                z_list.append(z_item)
    if v==1:
        print(z_list)
    
    return(z_list)




def replace_channel_in_key(meta,new_band_id):
    """
    Replace the band id in the CX header key.
    
    Parameters
    ----------
     meta : str
         CX line header.
     new_band_id : int
         id for the new (zoom) band.
     
    Returns
    -------
     new_meta : str
         new CX line header.
     
    Notes
    -----
    |
    | **Configuration:**
    |
    |    INDEX_KEY_CHANNEL: const_mapred.py (location of the channel id in the key (SF_SEP), to be replaced by new channel id [zoom])
    |
    |
    | **TO DO:**
    |
    |    Create general funcionts to create and read key.
    """
    meta_split = meta.split(SF_SEP)
    new_meta = SF_SEP.join(meta_split[0:INDEX_KEY_CHANNEL])+SF_SEP+str(new_band_id)+\
                    FIELD_SEP+meta_split[INDEX_KEY_CHANNEL:][0].split(FIELD_SEP)[1]
    return(new_meta)


def process_zoom_band(inout_folder,file_in,file_out,correlation_ini_file="correlation.ini",media_ini_file="media.ini",\
                      stations_ini_file="stations.ini",v=1,average_channels=-1,filter_acc_periods=[]):
    """
    Generate a new CX file with the zoom bands from an existing CX file with results for the full band.
    
    Parameters
    ----------
     inout_folder : str
         path to folder containing CX file, and where newly created CX_zoom file will be placed.
     file_in : str
         CX filename.
     file_out : str
         filename for new CX file with zoom bands results.
     correlation_ini_file : str
         path to correlation.ini.
     media_ini_file : str
         path to media.ini.
     correlation_ini_file : str
         path to correlation.ini.
     stations_ini_file : str
         path to stations.ini.
     v : int
         verbose if 1.
     average_channels : int
         number of coefficients to average (-1 for no averaging).
     filter_acc_periods : list of int
         ids (starting at 0) for accumulation periods to process. Will process all if [].
    
    Returns
    -------
     fft_read : int
         number of coefficients in the last visibilities read from the file.
    
    Notes
    -----
    |
    | **Assumptions:**
    |
    |    Assuming a regular configuration where all the stations have the same zoom bands (i.e.: missmatched bands not suppported).
    |
    |
    | **TO DO:**
    |
    |    Migrate this functionality into lib_fx_stack.py so that missmatched band support can be provided.
    |    "px" harcoded, move to const_mapred.
    """

    
    
    file_in =            inout_folder+file_in
    file_out =           inout_folder+file_out
    
    media_ini_file=      inout_folder+media_ini_file
    correlation_ini_file=inout_folder+correlation_ini_file
    stations_ini_file=   inout_folder+stations_ini_file
    
    serial_media=serialize_config(sources_file=media_ini_file)
    serial_corr=serialize_config(sources_file=correlation_ini_file)
    
    params_array_corr=serial_params_to_array(serial_corr)
    params_array_media=serial_params_to_array(serial_media)
    
    fft_size = int(get_param_serial(params_array_corr,C_INI_CR_S_COMP,C_INI_CR_FFT))
    
    
    print("Processing metadata for zoom bands...")
    zoom_list = get_zoom_list(params_array_media,params_array_corr,average_channels=average_channels)
    
    
    print("Processing zoom bands...")
    fft_read=0
    with open(file_in, 'r') as f:
        with open(file_out,'w') as f_out:
                
            if v==1:
                print("id".ljust(30)+"read".rjust(10)+"fft".rjust(10)+"z_i".rjust(10)+"z_e".rjust(10))
            lines = f.readlines()
            for line in lines:
                if "px" in line[:2]:
                    [meta,st0,st1,key,vis,chan,pol0,pol1,n_bins,\
                      pcal_freq,chan_index,acc_period,fs,predata,datac] = read_line_cx(line,fft_size)
                    if datac is not None:
                        if (filter_acc_periods==[] or (vis in filter_acc_periods)):
    
                            
                            for i_zoom in zoom_list:
                                if i_zoom[0]==chan:
                                    
                                    # Update header metadata
                                    new_meta = replace_channel_in_key(meta,i_zoom[3])
 
                                    # Apply zoom for this channel
                                    datazoom = datac[i_zoom[1]:i_zoom[2]]
                                    
                                    if v==1:
                                        if ENABLE_PLOTTING:
                                            zerodata=np.zeros(datac.shape)
                                            zerodata[i_zoom[1]:i_zoom[2]]=datac[i_zoom[1]:i_zoom[2]]
                                            plt.figure(i_zoom[0])
                                            plt.plot(zerodata,label=meta+" ("+str(i_zoom[2]-i_zoom[1])+")")
                                   
                                    if average_channels>0:
                                        dzavg=datazoom.reshape(-1,average_channels)
                                        datazoom=np.average(dzavg,axis=1)

                                    new_line = new_meta+"\t"+predata+" "+' '.join(map(str,datazoom))
                                    # TO DO: add space after \t?
                                    if v==1:
                                        print(str(new_meta).ljust(30)+str(len(datac)).rjust(10)+str(len(datazoom)).rjust(10)+str(i_zoom[1]).rjust(10)+str(i_zoom[2]).rjust(10))
                    
                                    print(new_line,file=f_out)
                                    fft_read=len(datac)
                    else:
                        print("Skipping visibilities, not enough coefficients (st"+str(st0)+"-st"+str(st1)+",a"+str(vis)+",b"+str(chan)+")")
    if ENABLE_PLOTTING:
        plt.legend(bbox_to_anchor=(2, 1))
    
    print(fft_read)



###########################################
#    Conversion CorrelX/CX -> DiFX/SWIN
###########################################

def get_difx_filename(mjd_start,seconds_start):
    """
    Get filename for SWIN file.
    
    Notes
    -----
    |
    | **TO DO:**
    |
    |    Add missing funcionality for filling DIFX_FILENAME_SUFFIX.
    """
    return(DIFX_FILENAME_PREFIX+DIFX_FILENAME_SEP+str(    mjd_start)+\
                                DIFX_FILENAME_SEP+str(int(seconds_start))+\
                                DIFX_FILENAME_SUFFIX)




def convert_cx2d(doutput_file,cxoutput_file,correlation_ini_file,media_ini_file,forced_pol_list=[],only_half=0,\
                 duplicate=0,freq_ids=[],v=1,back_compat=1,forced_accumulation_period=-1,divide_vis_by=1,\
                 conjugate_vis_values=0):
    """
    Convert visibilities from an output file from CorrelX/CX to DiFX/SWIN format.
    
    Parameters
    ----------
     doutput_file : str
         output file name (will append DIFX_...) for new SWIN file.
     cxoutput_file : str
         path to CX file to be converted
     correlation_ini_file : str
         path to correlation.ini.
     media_ini_file : str
         path to media.ini.
     
     forced_pol_list : list of str, optional
         used to override polarizations in ini file.
     only_half : optional
         see create_bytes_list_visibilities_swin().
     duplicate : optional
         see create_bytes_list_visibilities_swin().
     freq_ids : unused
         [remove]
     v : int, optional
         verbose if 1.
     back_compat : int
         [remove] see notes.
     forced_accumulation_period : int, optional 
         see create_bytes_list_visibilities_swin().
     divide_vis_by : int
         see create_bytes_list_visibilities_swin().
     conjugate_vis_values : int
         see create_bytes_list_visibilities_swin().
    
    Returns
    -------
     doutput_file : str
         path to newly created SwIN output file.
    
    Notes
    -----
    | CX accumulation periods referenced by start time, SWIN by middle time.
    | It is assumed that all the polarizations [0,1,2,...] (as many as used) are defined in the media.ini file.
    | 
    | **(!) Limitations:**
    |
    |     See limitations on top of this file (forced values...).
    |
    |
    | **TO DO:**
    |
    |    Remove freq_ids.
    |    Remove back_compat and add version for CX file header instead.
    """

    # Read ini files
    serial_media=       serialize_config(sources_file=media_ini_file)
    serial_correlation= serialize_config(sources_file=correlation_ini_file)
    
    params_array_media=       serial_params_to_array(serial_media)
    params_array_correlation= serial_params_to_array(serial_correlation)
    
    
    # Polarizations
    pol_chars=          get_all_params_serial(params_array_media,C_INI_MEDIA_S_POLARIZATIONS)
    values=[]
    for i in pol_chars:
        values+= [int(  get_val_vector(params_array_media,C_INI_MEDIA_S_POLARIZATIONS,i)[0])]
    if v==1:
        print(values)
        print(pol_chars)
    values, pol_chars = zip(*sorted(zip(values, pol_chars)))
    pol_chars=list(pol_chars)
    if forced_pol_list!=[]:
        pol_chars=forced_pol_list
    
    # MJD and seconds
    mjd_start =       int(get_val_vector(params_array_correlation,C_INI_CR_S_TIMES,C_INI_CR_MJD)[0])
    seconds_start = float(get_val_vector(params_array_correlation,C_INI_CR_S_TIMES,C_INI_CR_START)[0])
    #fft_size =        int(get_val_vector(params_array_correlation,C_INI_CR_S_COMP,C_INI_CR_FFT)[0])
    
    # Accumulation period
    if forced_accumulation_period==-1:
        accumulation_period_str = (get_val_vector(params_array_correlation,C_INI_CR_S_COMP,C_INI_CR_ACC)[0])
        if "/" in accumulation_period_str:
            accumulation_period_split = accumulation_period_str.split("/")
            accumulation_period = float(accumulation_period_split[0])/int(accumulation_period_split[1])
        else:
            accumulation_period=float(accumulation_period_str)
    else:
        accumulation_period=forced_accumulation_period
    
    # Start time offset (half accumulation period)
    seconds_offset = float(accumulation_period)/2
    seconds_start += float(seconds_offset)


    # Output file name
    doutput_file+=get_difx_filename(mjd_start,seconds_start)

    

    if v==1:
        print("MJD: "+str(mjd_start))
        print("Seconds start [s]: "+str(seconds_start))
        print("Accumulation [s]: "+str(accumulation_period))
        print("Opening " + doutput_file + " for writing binary swin info")

    # Read CX file
    list_output = read_cxoutput(cxoutput_file,v,back_compat)
    output_list = []
    
    print("ac_id".ljust(5)+"ac_s".rjust(10)+"ap".rjust(7)+"chan".rjust(7)+"    "+"pol")
    
    # Create headers, pack data and sort results
    for dataset in list_output:

        
        [st0,st1,vis,chan,pol0,pol1,datac,diff_st] = dataset
        
        if v==1:
            print("Writing data for IDs:")
            print([st0,st1,vis,chan,pol0,pol1])
        
        # Create header
        header = create_header_swin(st0,st1,vis,chan,pol0,pol1,mjd_start,seconds_start,\
                   accumulation_period,pol_chars)

        # Crate data
        values_bytes = create_bytes_list_visibilities_swin(datac,only_half,duplicate,divide_vis_by,conjugate_vis_values)
        
        # Append output list
        output_list.append([st0,st1,vis,chan,pol0,pol1,header,values_bytes,diff_st])

    # Sort SWIN records
    output_list = sort_swin_records(output_list)
    
    # Write SWIN file
    with open(doutput_file,'wb') as f_out:
        for output_item in output_list:
            f_out.write(output_item[6])     # Write header
            f_out.write(output_item[7])     # Write visibilities

    # Display output file name
    if v==1:
        print(doutput_file)
    
    return(doutput_file)
    




###########################################
#    Conversion CorrelX/CX -> DiFX/PCAL
###########################################


def get_pcal_filename(mjd_start_str,seconds_start,station_name_str):
    """
    Get filename for PCAL file.
    """
    seconds_start_fill = str(int(seconds_start//1)).zfill(PCAL_FILENAME_SECONDS_ZFILL)
    
    return(PCAL_FILENAME_PREFIX+PCAL_FILENAME_SEP+mjd_start_str+\
                                PCAL_FILENAME_SEP+seconds_start_fill+\
                                PCAL_FILENAME_SEP+station_name_str)
    

def get_lines_pcal_header(mjd_start_str,seconds_start_str,stations_name_str):
    """
    Get header lines for PCAL file.
    
    Parameters
    ----------
     mjd_start_str : str
         start MJD for this accumulation period.
     seconds_start_str: str
         start seconds for this accumulation period.
     station_name_str : str
         name of the station.
    
    Returns
    -------
     header_pcal_v : list of str
         lines for the PCAL file header.
    
    """
    header_pcal_v=[]
    header_pcal_v.append(PCAL_HEADER_TITLE)
    header_pcal_v.append(PCAL_HEADER_VERSION+str(PCAL_HEADER_VERSION_VAL))
    header_pcal_v.append(PCAL_HEADER_MJD+        mjd_start_str)
    header_pcal_v.append(PCAL_HEADER_SECONDS+    seconds_start_str)
    header_pcal_v.append(PCAL_HEADER_TELESCOPE+  stations_name_str)
    return(header_pcal_v)


def get_pcal_meta(mjd_start_str,seconds_start,acc_period,seconds_duration,station_name,st0,tot_channels,num_tones_pcal):
    """
    Create metadata for PCAL record.
    
    Parameters
    ----------
     mjd_start_str : str
         start MJD for the scan.
     seconds_start : float
         start seconds for the scan.
     acc_period : int
         accumulation period id.
     seconds_duration : float
         accumulation period duration
     station_name : two char
         station name.
     st0 : int
         station id.
     tot_channels : int
         number of channels or bands.
     num_tones_pcal : int
         number of phase calibration tones per band.
    
    Returns
    -------
     meta_pcal : str
         metadata preceding the records for one accumulation period.
    
    Notes
    -----
    |
    | **TO DO:**
    |
    |    Check offset +seconds_duration/2.
    |    st0 should be datastream id?
    """
        
    seconds_duration_frac = seconds_duration/(24*60*60)
    seconds_duration_str = PCAL_SECONDS_FLOAT_FORMAT % seconds_duration_frac

    seconds_start_frac = (seconds_start+acc_period*seconds_duration+seconds_duration/2)/(24*60*60)
    seconds_start_str  = PCAL_SECONDS_FLOAT_FORMAT % seconds_start_frac
    
    timestamp_str      = mjd_start_str+seconds_start_str[1:]
    
    meta_pcal =      station_name         +" "          # 1. andId
    meta_pcal +=     timestamp_str        +" "          # 2. day
    meta_pcal +=     seconds_duration_str +" "          # 3. dur
    meta_pcal += str(st0)                 +" "          # 4. datastreamId
    meta_pcal += str(tot_channels)        +" "          # 5. nRecBand
    meta_pcal += str(num_tones_pcal)      +" "          # 6. nTone

    return(meta_pcal)


def get_pcal_record_valid(pcal_value,tone_freq_mega,pol_char):
    """
    Get record with valid result for phase calibration tone.
    
    Parameters
    ----------
     pcal_value : complex
         phase calibration tone.
     tone_freq_mega: int or float
         pcal tone freq [MHz].
     pol_char : char
         polarization.
     
    """
    add_space_0 =""
    if np.real(pcal_value)>0:
        add_space_0 = " "
    add_space_1 = ""
    if np.imag(pcal_value)>0:
        add_space_1 = " "
    record = str(tone_freq_mega)+" "+pol_char+" "+add_space_0+\
                str(PCAL_TONE_SCI_FORMAT % float(np.real(pcal_value)))+" "+add_space_1+\
                str(PCAL_TONE_SCI_FORMAT % float(np.imag(pcal_value)))
    return(record)



def append_pcal_records(records,chan_freq_out_mega,pcal_freq_out_mega,pcal_ind,datac,n_bins,pol_char,\
                                              conjugate_pcal_values,pcal_scaling):
    """
    Append a set of new phase calibration tone records for this band.
    Record is the "group of four numbers" in [Br14].
    
    Parameters
    ----------
     records : list of str
         records (previously generated for other bands for the same accumulation period.
     chan_freq_out_mod_mega : int or float
         frequency of the first phase calibration tone in this band[Mhz].
     pcal_freq_out_mega : int or float
         phase calibration tone frequency separation [Mhz].
     pcal_ind : list of ints
         positions with phase calibration indices.
     datac : complex 1D numpy array
         phase calibration results (DFT).
     n_bins : int
         number of bins in the phase calibration DFT.
     pol_char : char
         polarization.
     conjugate_pcal_values : int
         conjugate all valid results if 1.
     pcal_scaling : float
         multiply all valid results by this value.
    
    Returns
    -------
     records : list of str
         appended list of str including (the new elements are the records for this band).
         
    Notes
    -----
    |
    | **TO DO:**
    |
    |    Check if N == n_bins and simplify code.
    |    Add offset from configuration.
    """
    # Iterate through indices for phase calibration tones
    pcal_diff=pcal_freq_out_mega
    for pcal_index in pcal_ind:
        pcal_diff-=pcal_freq_out_mega
        chan_freq_out_mod_mega = chan_freq_out_mega + pcal_diff
        if (pcal_index<(-1)*n_bins)or(pcal_index>n_bins):
            # Invalid tone
            record = PCAL_INVALID_RECORD_STR
        else:
            # Valid tone
            pcal_value = datac[pcal_index]
            
            if conjugate_pcal_values:
                pcal_value = np.conj(pcal_value)

            if pcal_scaling!=0:
                pcal_value *= pcal_scaling

            record = get_pcal_record_valid(pcal_value,chan_freq_out_mod_mega,pol_char)
                    
        records.append(record)
    
    return(records)


def get_pcal_line(meta_pcal,records):
    """
    Get a line for the PCAL file.
    
    Parameters
    ----------
     meta_pcal : str
         metadata.
     records : list of str
         records.
    """
    return(meta_pcal+' '.join(records))

def write_pcal_file(pcal_file,mjd_start_str,seconds_start,station_name_str,records_v):
    """
    Write PCAL file.
    
    Parameters
    ----------
     pcal_file : str
         path to pcal file.
     mjd_start_str : str
         start MJD.
     seconds_start : str
         start seconds. 
     station_name_str : two char.
         Two-character code for the station.
     records_v : list of str
         lines with pcal records.
    """
    
    seconds_start_str = str(int(seconds_start))
    
    with open(pcal_file,'w') as f_out:
        
        # Write PCAL header
        header_pcal_v=get_lines_pcal_header(mjd_start_str,seconds_start_str,station_name_str)
        for header_pcal_line in header_pcal_v:
            print(header_pcal_line,file=f_out)

        for record_line in records_v:
            print(record_line,file=f_out)


def get_pcal_tone_positions(N,bw,chan_freq,pcal_freq,num_tones_pcal):
    """
    Get positions of the phase calibration tones in the results.
    
    Parameters
    ----------
     N : int
         number of coefficients in the results (DFT of accumulated windows).
     bw : int or float
         bandwidth of the channel [Hz].
     chan_freq : int or float
         lower edge frequency of the channel [Hz].
     pcal_freq : int or float
         phase calibration tone frequency.
     num_tones_pcal: number of phase calibration tones expected for this band.
     
    Returns
    -------
     pcal_ind_mod : list of int
         position for the coeffients containing the phase calibration tones.
     extreme_value : int
         value used to indicate invalid index.
    """

    
    # TO DO: spec_res. LSB hardcoded...
    # if LSB:
    lsb_band=1
    chan_freq_mod=chan_freq
    if lsb_band:
        chan_freq_mod-=bw
        spec_res=bw/N

    freq_base=int((chan_freq_mod//pcal_freq)*pcal_freq)
    first_tone = int((freq_base-chan_freq_mod)//spec_res)
    total_tones= int(bw//pcal_freq)
    pcal_sep = int(pcal_freq*(N/bw))
    if first_tone<0:
        first_tone+=pcal_sep

    pcal_ind_mod = list(range(first_tone,pcal_sep*(total_tones+1),pcal_sep))

    #in case index greater than fft size
    extreme_value=-100*N
    for i_extreme in [0,-1]:
        if pcal_ind_mod[i_extreme]>N:
            pcal_ind_mod[i_extreme]=extreme_value

    
    if len(pcal_ind_mod)<num_tones_pcal:
        pcal_ind_mod = [extreme_value]+pcal_ind_mod

    

    pcal_ind_mod=list(reversed(sorted(pcal_ind_mod))) 
    
    return([pcal_ind_mod,extreme_value])


def plot_pcal_tones(datac,pcal_ind,extreme_value):
    """
    Plot phase calibration tones in red, overlaying the DFT with all results. Use for debugging.
    
    Parameters
    ----------
     datac : numpy array of complex
         DFT with pcal results.
     pcal_ind : list of int
         positions of the phase calibration tones.
     extreme_value : int
         value used to indicate invalid index.
    """
    pcal_ind_plot=[0]*len(pcal_ind)
    pcal_ind_plot[:]=pcal_ind
    if extreme_value in pcal_ind_plot:
        pcal_ind_plot.remove(extreme_value)
    zerodata=np.zeros(datac.shape)
    zerodata[pcal_ind_plot]=np.abs(datac[pcal_ind_plot])
    plt.figure()
    plt.plot(np.abs(datac))
    plt.plot(zerodata,'r')


def cxpcal2d(doutput_folder,cxoutput_file,correlation_ini_file,media_ini_file,stations_ini_file,\
             forced_file_list=[],pcal_scaling=0,conjugate_pcal_values=0,v=1):
    """
    Generate phase calibration tone files ("pulse cal data files") DiFX/SWIN from CorrelX/CX.
    
    Parameters
    ----------
     doutput_folder : str
         path to place new PCAL files.
     cxoutput_file : str
         path to CX file to read.
     correlation_ini_file : str
         path to correlation.ini.
     media_ini_file : str
         path to media.ini.
     stations_ini_file : str
         path to stations.ini.
     forced_file_list : optional,[testing]
         consider only these media files (consider all if []).
     pcal_scaling : float, optional
         if not 0, multiply all phase calibration tones by this value.
     conjugate_pcal_values : optional,[testing]
         conjugate phase calibration tone values.
     v : int
         verbose if 1.
     
    Returns
    -------
     name_file_list : list of str
         names of the newly created PCAL files (no path).
    
    Notes
    -----
    |
    | **TO DO:**
    |
    |    (!) This function needs to be simplified.
    |    Assuming that number of channels and polarizations equals the max id plus one.
    |    Phase calibration line string currently hardcoded (pc).
    |    Untested for real data.
    |    fs=2fs, check this, consider reading one frame of the media to determine if complex/real data.
    |    LSB/USB.
    |    Offset for pcal.
    """
    name_file_list=[]
    
    cxoutput_file_report = cxoutput_file+"_pcal_report.txt"   # report filename
    num_plots=3
    
    # Read ini files
    serial_correlation= serialize_config(sources_file=correlation_ini_file)
    serial_stations=    serialize_config(sources_file=stations_ini_file)
    serial_media=       serialize_config(sources_file=media_ini_file)
    
    params_array_correlation=          serial_params_to_array(serial_correlation)
    params_array_stations=    np.array(serial_params_to_array(serial_stations))     # TO DO: np.array?
    params_array_media=                serial_params_to_array(serial_media)
    
    # Process ini files -                                                                             correlation.ini
    mjd_start_str =          get_val_vector(params_array_correlation,C_INI_CR_S_TIMES,C_INI_CR_MJD)[0]
    seconds_start = float(   get_val_vector(params_array_correlation,C_INI_CR_S_TIMES,C_INI_CR_START)[0])
    seconds_duration = float(get_val_vector(params_array_correlation,C_INI_CR_S_COMP, C_INI_CR_ACC)[0])

    # Process ini files -                                                                             stations.ini
    stations_v = list([i.upper() for i in params_array_stations[:,0].flatten().tolist()])
    
    if v==1:
        print(stations_v)
    
    # Process ini files -                                                                             media.ini
    pol_chars=                  get_all_params_serial(params_array_media,C_INI_MEDIA_S_POLARIZATIONS)
    channel_str_v =             get_all_params_serial(params_array_media,C_INI_MEDIA_FREQUENCIES)    
    file_list=                  get_val_vector(       params_array_media,C_INI_MEDIA_S_FILES,C_INI_MEDIA_LIST)
    if forced_file_list==[]:
        forced_file_list=file_list
    eq_channels=[]
    eq_polarizations=[]
    for i in forced_file_list:
        eq_channels.append(     get_param_eq_vector(  params_array_media,i,C_INI_MEDIA_S_CHANNELS))
        eq_polarizations.append(get_param_eq_vector(  params_array_media,i,C_INI_MEDIA_S_POLARIZATIONS))
    
    # TO DO: simplify this?
    # Get list of channels and sort by id
    f_id_v = []
    f_val_v = []
    for channel_str in channel_str_v:
        f_id_v.append(      int(get_param_serial(     params_array_media,C_INI_MEDIA_S_CHANNELS,channel_str)))
        f_val_v.append(   float(get_param_serial(     params_array_media,C_INI_MEDIA_FREQUENCIES,channel_str)))
    
    f_id_v, f_val_v = zip(*sorted(zip(f_id_v, f_val_v)))
    f_id_v = list(f_id_v)     # List of frequency ids
    f_val_v = list(f_val_v)   # List of frequency values
    
    if v==1:
        print(f_val_v)        # f_val_v has list of frequencies that can be accessed by channel id
    
    

    list_output=[]
    
    max_chan = -1
    max_pol = -1
    
    
    # Read pcal results from CX file
    check_pc=0
    with open(cxoutput_file, 'r') as f:
        lines = f.readlines()
        for line in lines:
            if "pc" in line[:2]: 
                # Process only lines for phase calibration 
                # One line per station and accumulation period
                # TO DO: str pc HARDCODED, create constant in const_mapred.py
                check_pc=1
                [meta,st0,st1,key,vis,chan,pol0,pol1,n_bins,\
                      pcal_freq,chan_index,acc_period,fs,predata,datac] = read_line_cx(line)
                
                # (!) chan_index has to follow the same order as defined in the media file! no re-sorting!
                # TO DO: hardcoded!
                # This is only for complex data!
                fs=2*fs
                
                # Find proper ids for channel and polarization (from the metadata associated to the media file)
                [search_chan,search_pol]=[chan,pol0]
                global_index_chan=-1
                count_position=-1
                for i,j in zip(eq_channels[st0],eq_polarizations[st0]):
                    count_position+=1
                    if [search_chan,search_pol]==[i,j]:
                        global_index_chan=count_position
                        break

                list_output+=[[st0,acc_period,global_index_chan,chan_index,meta,chan,pol0,n_bins,pcal_freq,fs,datac]]
                
                # TO DO: for channels and polarization ids expecting total = max(id)+1
                if chan>max_chan:
                    max_chan=chan
                if pol0>max_pol:
                    max_pol=pol0
    
    
    
    # Process pcal results
    if check_pc==0:
        print("No phase calibration results found.")
    else:
    
        tot_channels = max_chan+1
        tot_pols = max_pol+1
        tot_channels = tot_channels*tot_pols
        
        # Sort by station, accumulation period and band
        list_output = sorted(list_output, key=operator.itemgetter(0,1,2)) #(0,1,2)) #4,5))
        

        records = []
        records_v=[]
        first_element = 1
        st0_pre=-1
        acc_period_pre=-1
          
        with open(cxoutput_file_report, 'w') as f_report_pcal:   # phase calibration report used for debugging
            for list_item in list_output:
                [st0,acc_period,global_index_chan,chan_index,meta,chan,pol0,n_bins,pcal_freq,fs,datac] = list_item
                
                # Number of pcal tones
                num_tones_pcal = int(np.ceil(fs/(2*pcal_freq)))
                
                if ((first_element==0)and((st0_pre!=st0)or(acc_period_pre!=acc_period))):
                    
                    # If not first iteration and new station / acc period:
                    #  -prepare file headers for previous results
                    #  -write previous results
                    #  -reset structures for new results
                    
                    records_v += [get_pcal_line(meta_pcal,records)]
                    
                    if (st0_pre!=st0):
                        # If new station, print previous results to file
                        name_file = get_pcal_filename(mjd_start_str,seconds_start,stations_v[st0_pre])
                        write_pcal_file(doutput_folder+"/"+name_file,mjd_start_str,seconds_start,\
                                        stations_v[st0_pre],records_v)
                        name_file_list+=[name_file]
                        records_v=[]
                            
                    records = []
       
                if ((st0_pre!=st0)or(acc_period_pre!=acc_period)):
                    
                    # If new station / acc period, prepare metadata for records
                    meta_pcal = get_pcal_meta(mjd_start_str,seconds_start,acc_period,seconds_duration,\
                                              stations_v[st0],st0,tot_channels,num_tones_pcal)
                
                first_element=0
                
                st0_pre = st0
                acc_period_pre = acc_period
                
                
                if v==1:
                    print(meta)
                    print(" Sts.:  "+str(st0)+","+str(st1))
                    print(" Vis.: "+str(vis)) 
                    print(" Chan.: "+str(chan))
                    print(" Pols.: "+str(pol0)+","+str(pol1))
                
                chan_freq = f_val_v[chan]
                chan_freq_out_mega = int(chan_freq//1e6)
                pcal_freq_out_mega = int(pcal_freq//1e6)
                
                chan_freq_out_mega = (chan_freq_out_mega//pcal_freq_out_mega)*pcal_freq_out_mega
                
                if v==1:
                    print(chan_freq)
                
            
                # Compute locations of pcal tones
                N =len(datac)
                bw = fs/2
                [pcal_ind_mod,extreme_value] = get_pcal_tone_positions(N,bw,chan_freq,pcal_freq,num_tones_pcal)
                pcal_ind = pcal_ind_mod
                    
                print(str(st0)+" "+ str(acc_period) + " " +str(chan)+" " +str(chan_freq)+" " +str(chan_freq_out_mega)+\
                      " "+pol_chars[pol0]+" "+ str(pcal_ind_mod),file = f_report_pcal )   
                
               


                if ENABLE_PLOTTING and num_plots>0:
                    plot_pcal_tones(datac,pcal_ind,extreme_value)
                    num_plots-=1

                records = append_pcal_records(records,chan_freq_out_mega,pcal_freq_out_mega,pcal_ind,datac,\
                                              n_bins,pol_chars[pol0],conjugate_pcal_values,pcal_scaling)
    
             
             
            # Last write
            records_v += [get_pcal_line(meta_pcal,records)]
                
            # Print to file
            name_file = get_pcal_filename(mjd_start_str,seconds_start,stations_v[st0_pre])
            write_pcal_file(doutput_folder+"/"+name_file,mjd_start_str,seconds_start,stations_v[st0_pre],records_v)
            name_file_list+=[name_file]
            
    return(name_file_list)




###########################################
# Conversion CorrelX/CX -> DiFX/SWIN+PCAL
###########################################


def convert_cx2dpc(inout_folder,file_in,file_out,correlation_ini_file,media_ini_file,stations_ini_file,v=0,\
                   only_half=0,duplicate=0,freq_ids=[],back_compat=1,forced_accumulation_period=-1,divide_vis_by=1,\
                   forced_file_list=[],pcal_scaling=0,conjugate_pcal_values=0,conjugate_vis_values=0):
    
    """
    Main routine to convert CorrelX/CX into DiFX/SWIN+PCAL.
    
    Parameters
    ----------
     inout_folder : str
         path to folder containing CX file, and where newly created SWIN+PCAL files will be placed.
     file_in : str
         CX filename.
     file_out : str
         filename for new SwIN file.
     correlation_ini_file : str
         path to correlation.ini associated with CX file.
     media_ini_file : str
         path to media.ini associated with CX file.
     stations_ini_file:    path to stations.ini associated with CX file.
     
     See convert_cx2d() for the rest of the arguments.
     
    Returns
    -------
     pcal_file_list : list of str
         filenames for new PCAL files (None if error).
    """
    
    # Check errors in .ini files
    fp_v=[media_ini_file,correlation_ini_file]
    fn_v=["Media","Correlation"]
    error_files=0
    for (fp,fn) in zip(fp_v,fn_v):
        if not os.path.isfile(media_ini_file):
            print("ERROR: "+fn+" file "+fp+" does not exist!")
            error_files=1
    
    if error_files==0:
        # Convert cx output to dx
        convert_cx2d(inout_folder+file_out,inout_folder+file_in,correlation_ini_file,media_ini_file,forced_pol_list=[],\
                     only_half=only_half,duplicate=duplicate,freq_ids=freq_ids,v=v,back_compat=back_compat,\
                     forced_accumulation_period=forced_accumulation_period,divide_vis_by=divide_vis_by,\
                     conjugate_vis_values=conjugate_vis_values)
        
        # Convert cx output phase cal
        pcal_file_list=cxpcal2d(inout_folder,inout_folder+file_in,correlation_ini_file,media_ini_file,stations_ini_file,\
                                forced_file_list,pcal_scaling,conjugate_pcal_values,v=v)
    else:
        pcal_file_list=None
    return(pcal_file_list)






#################################################################
#               DiFX/.im,.input parser tools
################################################################# 

def get_field_im(line):
    """
    Get the value+units from a line of DiFX configuration file.
    """
    return(line.strip().split(C_DIFX_SEPARATOR)[1])

def get_value_im(line):
    """
    Get the value (no units) from a line of DiFX configuration file.
    """
    return(get_field_im(line).split()[0])

def get_vector_im(line):
    """
    Get a list of str with polynomial coefficients from line in .im file.
    """
    return([ii.strip() for ii in get_field_im(line).split()])

def get_src_ant_im(line):
    """
    Get two str, one with source id and another with station id from the line in .im file.
    """
    param_im_v = line.strip().split(C_DIFX_SEPARATOR)[0].split()
    src=param_im_v[1]
    ant=param_im_v[3]
    return([src,ant])

def get_header_dm(mjd,seconds,interval,src,ant):
    """
    Create header for delay_model.ini.
    
    TO DO: consider moving into lib_ini_files.py.
    """
    end_seconds=str(int(seconds)+int(interval))
    return(INI_HF+mjd+"-"+seconds+"-"+end_seconds+INI_SUB+"so"+src+INI_SUB+"st"+ant+INI_HL)

def sort_str_list(list_in):
    """
    Return sorted set of elements of a list.
    """
    return(list(map(str,sorted(list(map(int,list(set(list_in))))))))

def get_id_param(line,num):
    """
    Get the id that is in the parameter, in the position num.
    """
    return(line.strip().split(C_DIFX_SEPARATOR)[0].split()[num])


def get_coeff(line):
    """
    Process station clock information from .input file.
    """
    value = get_value_im(line)
    st = line.strip().split(C_DIFX_SEPARATOR)[0].split()[2].split('/')[0]
    order = line.strip().split(C_DIFX_SEPARATOR)[0].split()[2].split('/')[1]
    return([st,order,value])

def get_last_num(line):
    """
    Get the id that is in the parameter, in the last position.
    """
    last_num = line.strip().split(C_DIFX_SEPARATOR)[0].split()[-1]
    return(last_num)

def write_lines_to_f(lines_out,full_output_file,str_info="results"):
    """
    Write a list of strings into a file, one per line, and report.
    """
    print(" Writing "+str_info+" to "+full_output_file+" ...")
    with open(full_output_file, 'w') as f_out:
        for ii in lines_out:
            print(ii,file=f_out)



#################################################################
#      Conversion DiFX/.im -> CorrelX/delay_model+sources
################################################################# 

def im_to_delay_model(inout_folder,file_in,file_out,filter_sources=[],v=0):
    """
    Convert DiFX/.im into CorrelX/delay_model.ini.
    
    Parameters
    ----------
     inout_folder : str
         path to folder containing .im file, and where newly created delay_model.ini file will be placed.
     file_in : str
         .im filename.
     file_out: str
         delay_model.ini filename.
     filter_sources: list of str
         source names. If not [], information for sources that are not in this list will be dismissed.
     v : int
         verbose if 1.
     
    Returns
    -------
     None
    """
    DELAY_FIRST = C_INI_MODEL_DELAY+INI_SEP
    DRY_FIRST   = C_INI_MODEL_DRY+INI_SEP
    WET_FIRST   = C_INI_MODEL_WET+INI_SEP
    
    
    lines_out=[]
    header_list=[]
    skip_data=0
    not_first=0
    
    list_mjd=[]
    list_seconds=[]
    list_so=[]
    list_st=[]
    
    full_input_file=inout_folder+"/"+file_in
    full_output_file=inout_folder+"/"+file_out
    summary_file=inout_folder+"/"+file_out+"_report"
    
    if filter_sources!=[]:
        print(" Filtering sources: "+','.join(list(map(str,set(filter_sources)))))
    
    print(" Processing "+full_input_file+" ...")
        #                                                                     ### --- Process .im and prepare delay_model.ini ---
    with open(full_input_file, 'r') as f:
        lines = f.readlines()
        for line in lines:
            if C_DIFX_IM_INTERVAL_SECS in line:                               # Interval for the polynomial
                interval=get_value_im(line)
                if v==1:
                    print(line)
                    print(interval)
            
            elif C_DIFX_IM_SCAN in line and\
                 C_DIFX_IM_POLY in line and\
                 C_DIFX_IM_MJD in line:
                mjd=get_value_im(line)                                        # Start MJD for the polynomial
                list_mjd.append(mjd)
                if v==1:
                    print(line)
                    print(mjd)
                
            elif C_DIFX_IM_SCAN in line and\
                 C_DIFX_IM_POLY in line and\
                 C_DIFX_IM_SEC in line:
                seconds=get_value_im(line)                                    # Start seconds for the polynomial
                list_seconds.append(seconds)
                if v==1:
                    print(line)
                    print(seconds)
            
            elif C_DIFX_IM_DELAY_US in line:                                  # Total delay polynomial
                poly_read = get_vector_im(line)
                poly_str=INI_VEC.join(poly_read) #:
                [src,ant] = get_src_ant_im(line)
                if filter_sources==[] or (filter_sources!=[] and int(src) in filter_sources):
                    skip_data=0
                    list_so.append(src)
                    list_st.append(ant)
                    header_dm = get_header_dm(mjd,seconds,interval,src,ant)
                    if header_dm not in header_list:
                        if not_first:
                            lines_out.append("")
                        not_first=1
                        header_list.append(header_dm)               
                        lines_out.append(header_dm)
                    lines_out.append(DELAY_FIRST+poly_str)
                    if v==1:
                        print(line)
                        print(poly_read)
                        print(poly_str)
                        print(header_dm)
                else:
                    skip_data=1
                    
            elif C_DIFX_IM_DRY_US in line:                                   # Dry component delay polynomial
                if skip_data==0:
                    poly_read = get_vector_im(line)
                    poly_str=INI_VEC.join(poly_read) #:
                    lines_out.append(DRY_FIRST+poly_str)
                    
            elif C_DIFX_IM_WET_US in line:                                   # Wet component delay polynomial
                if skip_data==0:
                    poly_read = get_vector_im(line)
                    poly_str=INI_VEC.join(poly_read) #:
                    lines_out.append(WET_FIRST+poly_str)
            
    
    if v==1:
        print(" ")
        print(" ")
        for ii in lines_out:
            print(ii)
    
    summary_lines=[]
    summary_lines.append("Summary:")
    summary_lines.append(" Input:    "+full_input_file)
    summary_lines.append(" Sources:  "+', '.join(list(set(list_so))))
    summary_lines.append(" Stations: "+', '.join(list(set(list_st))))
    summary_lines.append(" Interval: "+interval)
    summary_lines.append(" MJDs:     "+', '.join(list(set(list_mjd))))
    summary_lines.append(" seconds:  "+', '.join(sort_str_list(list_seconds)))
    
    if v==1:
        for ii in summary_lines:
            print(ii) 
    
    write_lines_to_f(lines_out,full_output_file)
    write_lines_to_f(summary_lines,summary_file,"summary")
    
    
    if v==1:
        print("Done.")   
    
    


    
    
def im_to_sources(inout_folder,file_in,file_out,v=0):
    """
    Convert DiFX/.im into CorrelX/sources.ini.
    
    Parameters
    ----------
     inout_folder : str
         path to folder containing .im file, and where newly created sources.ini file will be placed.
     file_in : str
         .im filename.
     file_out : str
         sources.ini filename.
     v : int
         verbose if 1.
     
    Returns
    -------
     None
    """
    
    SRC_ID_PARAM = 1
    
    full_input_file=inout_folder+"/"+file_in
    full_output_file=inout_folder+"/"+file_out
    summary_file=inout_folder+"/"+file_out+"_report"
    
    lines_out=[]
   
    print(" Processing "+full_input_file+" ...")
    #                                                                            ### --- Process .im and prepare sources.ini ---    
    with open(full_input_file, 'r') as f:
        lines = f.readlines()
        for line in lines:
            if C_DIFX_IM_POINTING_SRC in line:                                   # Source
                src_id = get_id_param(line,SRC_ID_PARAM)
                src_name = get_value_im(line)
                lines_out.append(INI_HF+str(src_name)+INI_HL)
                lines_out.append(C_INI_SRC_ID+INI_SEP+src_id)
    
    write_lines_to_f(lines_out,full_output_file)
    
    if v==1:
        print("File contents:")
        for ii in lines_out:
            print(" "+ii)    
        print("Done.") 



#################################################################
#  Conversion DiFX/.input -> CorrelX/stations+correlation+media
#################################################################


def input_to_stations(inout_folder,file_in,file_out,forced_stations=[],v=0):
    """
    Convert DiFX/.input into CorrelX/stations.ini.
    
    Parameters
    ----------
     inout_folder : str
         path to folder containing .input file, and where newly created stations.ini file will be placed.
     file_in : str
         .input filename.
     file_out : str
         stations.ini filename.
     forced_files : list of str,optional
         list of str with station names (for overriding values from .input).
     v : int
         verbose if 1.
     
    Returns
    -------
     None
    """
    
    clock_line = ""
    lines_out=[]
    not_first=0
    
    full_input_file=inout_folder+"/"+file_in
    full_output_file=inout_folder+"/"+file_out
    summary_file=inout_folder+"/"+file_out+"_report"
    
    if forced_stations!=[]:
        print(" Forcing station names: "+','.join(list(map(str,set(forced_stations)))))

    print(" Processing "+full_input_file+" ...")
    #                                                                              ### --- Process .input and prepare stations.ini ---
    with open(full_input_file, 'r') as f:
        lines = f.readlines()
        for line in lines:
            if C_DIFX_INPUT_TELESCOPE_NAME in line:                                # Station name
                st_id = get_last_num(line)
                st_name = get_value_im(line)
                if forced_stations!=[]:
                    st_name=forced_stations[int(st_id)]
                if not_first:
                    lines_out.append("")
                lines_out.append(INI_HF+st_name+INI_HL)
                lines_out.append(C_INI_ST_ID+INI_SEP+st_id)
                not_first=1

            elif C_DIFX_INPUT_CLOCK_REF_MJD in line:                              # Station clock epoch
                clock_ref = get_value_im(line)
                lines_out.append(C_INI_ST_CLOCK_REF+INI_SEP+clock_ref)


            elif C_DIFX_INPUT_CLOCK_COEFF in line:                                # Station clock polynomial
                [st,order,value]=get_coeff(line)
                if order=="0":
                    value0=value
                else:
                    clock_line = C_INI_ST_CLOCK_POLY+INI_SEP+value0+INI_VEC+value
                    lines_out.append(clock_line)
    
    write_lines_to_f(lines_out,full_output_file)
    
    if v==1:
        print("File contents:")
        for ii in lines_out:
            print(" "+ii)    
        print("Done.") 
        


def input_to_correlation(inout_folder,file_in,file_out,v=0):
    """
    Convert DiFX/.input into CorrelX/correlation.ini.
    
    Parameters
    ----------
     inout_folder : str
         path to folder containing .input file, and where newly created correlation.ini file will be placed.
     file_in : str
         .input filename.
     file_out : str
         correlation.ini filename.
     v : int
         verbose if 1.
     
    Returns
    -------
     None
    
    Notes
    -----
    |
    | **Assumptions:**
    |
    |    Assuming one data stream per station for computing the number of stations.
    """
    full_input_file=inout_folder+"/"+file_in
    full_output_file=inout_folder+"/"+file_out
    summary_file=inout_folder+"/"+file_out+"_report"
    
    lines_out=[]

    print(" Processing "+full_input_file+" ...")
    #                                                                              ### ---- Process .input ----   
    with open(full_input_file, 'r') as f:
        lines = f.readlines()
        for line in lines:
            if C_DIFX_INPUT_INT_TIME in line:                                      # Accumulation period
                int_time = get_value_im(line)
                
            elif C_DIFX_INPUT_EXECUTE_TIME in line:                                # Scan duration
                duration = get_value_im(line)
                
            elif C_DIFX_INPUT_START_MJD in line:                                   # Start MJD
                mjd = get_value_im(line)
                
            elif C_DIFX_INPUT_START_SECONDS in line:                               # Start seconds
                start_s = get_value_im(line)
                
            elif C_DIFX_INPUT_TELESCOPES in line:                                 # Number of stations
                stations = get_value_im(line)
                
            elif C_DIFX_INPUT_NUM_CHANNELS in line:                                # Number of coefficients in visibilities
                fft_size = get_value_im(line)
                
            elif C_DIFX_INPUT_PHASE_CALS in line:                                  # Phase calibration
                pcal_val = get_value_im(line)
                if pcal_val=="0":
                    pcal="no"
                else:
                    pcal="yes"
    
    #                                                                             ### ---- Prepare lines correlation.ini ----
    
    lines_out.append(INI_HF+C_INI_CR_S_ELEMENTS+INI_HL)                           # Elements
    lines_out.append(C_INI_CR_STATIONS+INI_SEP+stations)
    lines_out.append(C_INI_CR_AUTO_ST+INI_SEP+"yes")                                             # (!)forced
    lines_out.append(C_INI_CR_CROSS_POL+INI_SEP+"yes")                                           # (!)forced
    lines_out.append("")
    lines_out.append(INI_HF+C_INI_CR_S_COMP+INI_HL)                               # Computation
    lines_out.append(C_INI_CR_FFT+INI_SEP+fft_size)
    lines_out.append(C_INI_CR_ACC+INI_SEP+str(float(int_time)))
    lines_out.append(C_INI_CR_WINDOW+INI_SEP+C_INI_CR_WINDOW_SQUARE)                             # (!)forced
    lines_out.append(C_INI_CR_PC+INI_SEP+pcal)
    lines_out.append("")
    lines_out.append(INI_HF+C_INI_CR_S_TIMES+INI_HL)                              # Times
    lines_out.append(C_INI_CR_MJD+INI_SEP+mjd)
    lines_out.append(C_INI_CR_START+INI_SEP+start_s)
    lines_out.append(C_INI_CR_DURATION+INI_SEP+duration)
    
    write_lines_to_f(lines_out,full_output_file)
    
    if v==1:
        print("File contents:")
        for ii in lines_out:
            print(" "+ii)    
        print("Done.")
    

def create_symb_media(inout_folder,file_input_v,forced_files="",symb_dir=None):
    """
    Create symbolic links for media. This links are also part of the configuration.
    
    Parameters
    ----------
     inout_folder : str
         path to folder containing newly created ini files.
     file_input_v : list of str
         paths to media files.
     forced_files :  str
         comma separated list of paths to media files to override file paths from .input file.
         If "", then the pahts from the .input file are used.
     symb_dir : str,optional
         path relative to inout_folder where links will be created.
     
    Returns
    -------
     file_list : list of str
         filenames to be used in media.ini.
     files_str : str
         comma separated list of filenames.
    """
    

    if forced_files!="":
        print("(!) Forcing file names: "+forced_files)
        file_list_paths = forced_files.split(',')
        if len(file_input_v)!=len(file_list_paths):
            print("(!) WARNING: number of forced file paths and found paths in .input differ!")
            if len(file_input_v)>len(file_list_paths):
                print("(!) WARNING: missing information for the following files:")
                for i in file_input_v[-1*(len(file_input_v)-len(file_list_paths)):]:
                    print("(!)  "+i)
    else:
        file_list_paths=file_input_v
    
    # only filename, no path
    file_list=[i.split("/")[-1] for i in file_list_paths]  
    files_str = ','.join(file_list)
    
    if symb_dir is None:
        symb_dir = CX_DEFAULT_MEDIA_DIR
    dir_link = inout_folder+"/"+symb_dir
    
    # Display info and prepare commands
    print("Creating symbolic links for media in "+dir_link)
    cmd_unlink_v=[]
    cmd_link_v=[]
    print(" "+"Link".ljust(50)+"Source")
    for (i,j) in zip(file_list,file_list_paths):
        print(" "+i.ljust(50)+j)
        new_link = dir_link+"/"+i
        cmd_unlink = "unlink "+new_link
        cmd_unlink_v.append(cmd_unlink)
        cmd_link =   "ln -s "+j+" "+dir_link+"/"+i
        cmd_link_v.append(cmd_link)
    
    # Launch commands
    os.system("mkdir "+dir_link)
    for (cmd_unlink,cmd_link) in zip(cmd_unlink_v,cmd_link_v):
        print(cmd_unlink)
        os.system(cmd_unlink)
        print(cmd_link)
        os.system(cmd_link)
        
    print("(!) If moving ini folder do: cp -r --preserve=links "+inout_folder+" dir_ini_destination")
    return([file_list,files_str])
        

def input_to_media(inout_folder,file_in,file_out,forced_files="",v=0):
    """
    Convert DiFX/.input into CorrelX/media.ini, create symbolic links for media files
      and generate report with summary (for reporting/debugging).
    
    Parameters
    ----------
     inout_folder : str
         path to folder containing .input file, and where newly created media.ini file will be placed.
     file_in : str
         .input filename.
     file_out : str
         media.ini filename.
     forced_files : str, required
         list of files to be used in the media files
     v : int
         verbose if 1.
     
    Returns
    -------
     None
    
    Notes
    -----
    |
    | **Limitations:**
    |
    |    Currently limited to sets of files with the same configuration (i.e. no missmatched bands).
    |
    |
    | **Assumptions:**
    |
    |    The function is strongly dependent on the current order of the fields in the .input file.
    |    The zoom bands are expected to appear after the normal bands.
    |    Assuming incremental id starting at 0 in input file.
    |
    |
    | **TO DO:**
    |
    |    Read forced files from the .input file.
    """
    
    pol_lr=['L','R']
    pol_xy=['X','Y']
    
    full_input_file=inout_folder+"/"+file_in
    full_output_file=inout_folder+"/"+file_out
    summary_file=inout_folder+"/"+file_out+"_report"
    
    first_bw_set = 0
    first_bw = 0
    
    lines_out=[]
    
    channel_freq_v=[]
    channel_bw_v=[]
    sideband_v=[]
    
    st_names=[]
    st_names_ind=[]
    list_z=[]
    list_zb=[]
    
    zoom_v=[]
    zoom_flag=0
    
    pol_v=[]

    lines_ch = []
    lines_f = []
    lines_bw = []
    lines_z = []
    lines_zb = []

    ch_v=[]
    bands_v=[]
    
    ch_v_v=[]
    pol_v_v=[]
    side_v_v=[]
    ch_v_v=[]
    f_v_v=[]

    print(" Processing "+full_input_file+" ...")
    
    file_input_v=[]
    
    #                                                                                         ### ---- Process .input ----
    
    with open(full_input_file, 'r') as f:
        lines = f.readlines()
        for line in lines:
            if C_DIFX_INPUT_FREQ in line:                                                     # Append group info: sampling freq
                channel_freq_v.append(float(get_value_im(line))) #*1e6)
            
            elif C_DIFX_INPUT_BW in line:                                                     # Append group info: bandwidth / zoom
                channel_bw = float(get_value_im(line)) #*1e6
                channel_bw_v.append(channel_bw)
                if first_bw_set and channel_bw<first_bw:
                    # zoom band
                    zoom_v.append(1)
                else:
                    zoom_v.append(0)
                if first_bw_set==0:
                    first_bw+=channel_bw
                    first_bw_set=1
            elif C_DIFX_INPUT_SIDEBAND in line:                                               # Append group info: sideband
                sideband = get_value_im(line)
                sideband_v.append(sideband)
                
            elif C_DIFX_INPUT_TELESCOPE_TABLE in line:                                        # Prepare media lists (all group info)
                # close vectors
                n_id=-1
                for (ch_f,ch_bw,ch_side,ch_z) in zip(channel_freq_v,channel_bw_v,sideband_v,zoom_v): 
                    n_id+=1
                    str_id=str(n_id)
                    if ch_z==0:
                        last_n_id=n_id
                        lines_ch.append(C_INI_MEDIA_CH+str_id+INI_SEP+str_id)
                        lines_f.append(C_INI_MEDIA_CH+str_id+INI_SEP+str(ch_f))
                        lines_bw.append(C_INI_MEDIA_CH+str_id+INI_SEP+str(ch_bw))
                    else:
                        str_id=str(n_id-last_n_id-1)
                        lines_z.append(C_INI_MEDIA_ZF+str_id+INI_SEP+str(ch_f))
                        lines_zb.append(C_INI_MEDIA_ZB+str_id+INI_SEP+str(ch_bw))
                        list_z.append(C_INI_MEDIA_ZF+str_id)
                        list_zb.append(C_INI_MEDIA_ZB+str_id)
                        zoom_flag=1
            
            elif C_DIFX_INPUT_TELESCOPE_NAME in line:                                          # Station names
                station_name = get_value_im(line)
                st_names.append(station_name)
            
            elif C_DIFX_INPUT_TELESCOPE_INDEX in line or C_DIFX_INPUT_BASELINE_TABLE in line: # Group of group info (one per station)
                # new station, prepare vectors with channels
                
                if C_DIFX_INPUT_TELESCOPE_INDEX in line:                                      # Station id for this datastream
                    st_names_ind.append(int(get_value_im(line)))
                
                if ch_v!=[]:
                    ch_v_v.append(ch_v)
                    pol_v_v.append(pol_v)
                    side_v_v.append(side_v)
                    f_v_v.append(f_samp)
                ch_v=[]
                pol_v=[]
                side_v=[]
                bands_v=[]
            
            elif C_DIFX_INPUT_DATA_SAMPLING in line:                                          # Adjust sampling freq (complex)
                data_type=get_value_im(line)
                # to be used later once first frequency is read
                #if data_type==C_DIFX_INPUT_COMPLEX:
                #    f_samp=first_bw
                #else:
                #   f_samp=2*first_bw
                
            elif C_DIFX_INPUT_REC_BAND in line and C_DIFX_INPUT_POL in line:                  # Append group info: polarization
                pol = get_value_im(line)
                pol_v.append(pol)
                if pol in pol_lr:
                    pol_list=pol_lr
                else:
                    pol_list=pol_xy
            
            elif C_DIFX_INPUT_REC_FREQ in line and C_DIFX_INPUT_INDEX in line:               # Record freq index (accesed by channel)
                bands_v.append(get_value_im(line))
            
            elif C_DIFX_INPUT_REC_BAND in line and C_DIFX_INPUT_INDEX in line:               # Append group info: channel, sideband
                id_ch = int(get_value_im(line))
                #ch = C_INI_MEDIA_CH+get_value_im(line)
                ch = C_INI_MEDIA_CH+bands_v[id_ch]
                ch_v.append(ch)
                side_v.append(sideband_v[id_ch])
                f_samp=channel_bw_v[int(bands_v[id_ch])]
                if data_type!=C_DIFX_INPUT_COMPLEX:
                    f_samp*=2
            
            elif C_DIFX_INPUT_PHASE_CAL_INT in line:                                         # Phase calibration
                pcal_val=str(float(get_value_im(line))) #*1e6)
    
            elif C_DIFX_INPUT_FILE in line and \
                 C_DIFX_INPUT_FILES not in line and\
                 C_DIFX_INPUT_DATA_SOURCE not in line:                                       # Files paths
                file_input_v.append(get_value_im(line))
    
    
    
    #                                                                                        ### ---- Create symbolic links ---
    [file_list,files_str] = create_symb_media(inout_folder,file_input_v,forced_files)
        

    #                                                                                         ### ---- Prepare lines media.ini ----
    
    lines_out.append(INI_HF+C_INI_MEDIA_S_CHANNELS+INI_HL)                                    # List of channels
    for ii in lines_ch:
        lines_out.append(ii)
    lines_out.append("")
    lines_out.append(INI_HF+C_INI_MEDIA_FREQUENCIES+INI_HL)                                   # List of frequencies
    for ii in lines_f:
        lines_out.append(ii+"e6")
    lines_out.append("")
    if zoom_flag:
        lines_out.append(INI_HF+C_INI_MEDIA_BANDWIDTHS+INI_HL)                                # List of bandwidths
        for ii in lines_bw:
            lines_out.append(ii+"e6")
        lines_out.append("")
    lines_out.append(INI_HF+C_INI_MEDIA_S_POLARIZATIONS+INI_HL)                               # List of polarizations
    jj=-1
    for ii in pol_list:
        jj+=1
        lines_out.append(ii+INI_SEP+str(jj))
    lines_out.append("")
    if zoom_flag:                                                                             # List of zoom bands (if applicable)
        lines_out.append(INI_HF+C_INI_MEDIA_ZP_FREQ+INI_HL)
        for ii in lines_z:
            lines_out.append(ii+"e6")
        lines_out.append("")
        lines_out.append(INI_HF+C_INI_MEDIA_ZP_BW+INI_HL)
        for ii in lines_zb:
            lines_out.append(ii+"e6")
        lines_out.append("")
        lines_out.append(INI_HF+C_INI_MEDIA_ZOOM_POST+INI_HL)
        lines_out.append(C_INI_MEDIA_ZP_FREQ+INI_SEP+INI_VEC.join(list_z))
        lines_out.append(C_INI_MEDIA_ZP_BW+INI_SEP+INI_VEC.join(list_zb))
        lines_out.append("")
    lines_out.append(INI_HF+C_INI_MEDIA_S_FILES+INI_HL)                                       # List of files
    lines_out.append(C_INI_MEDIA_LIST+INI_SEP+files_str)

    for (i_file,st_name_ind,ch_v,pol_v,f,side_v) in zip(file_list,st_names_ind,ch_v_v,pol_v_v,f_v_v,side_v_v): 
        st_name=st_names[st_name_ind]
        lines_out.append("")                                                                  # Information for all files
        lines_out.append(INI_HF+i_file+INI_HL)  
        lines_out.append(C_INI_MEDIA_STATION+INI_SEP+st_name)
        lines_out.append(C_INI_MEDIA_CHANNELS+INI_SEP+INI_VEC.join(ch_v))
        lines_out.append(C_INI_MEDIA_S_POLARIZATIONS+INI_SEP+INI_VEC.join(pol_v))
        lines_out.append(C_INI_MEDIA_FRAMEBYTES+INI_SEP+"0")                                       # (!) forced
        lines_out.append(C_INI_MEDIA_FREQ_SAMPLE+INI_SEP+str(f)+"e6")
        lines_out.append(C_INI_MEDIA_FORMAT+INI_SEP+C_INI_MEDIA_F_VDIF)                            # (!) forced
        lines_out.append(C_INI_MEDIA_VERSION+INI_SEP+C_INI_MEDIA_V_CUSTOM)                         # (!) forced
        lines_out.append(C_INI_MEDIA_F_PCAL+INI_SEP+pcal_val+"e6")
        lines_out.append(C_INI_MEDIA_O_PCAL+INI_SEP+"0"+"e6")                                      # (!) forced
        lines_out.append(C_INI_MEDIA_FREQUENCIES+INI_SEP+INI_VEC.join(ch_v))
        lines_out.append(C_INI_MEDIA_SIDEBANDS+INI_SEP+INI_VEC.join(side_v))
    
    
    write_lines_to_f(lines_out,full_output_file)
    
    if v==1:
        print("File contents:")
        for ii in lines_out:
            print(" "+ii)    
        print("Done.")
        
        

# <codecell>


