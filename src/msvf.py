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
#File: msvf.py.
#Author: A.J. Vazquez Alvarez (ajvazquez@haystack.mit.edu)
#Description: 
"""
Mapper: reads VDIF frames from std.input and generates one mapreduce line per band per frame.

Parameters
----------
 See lib_mapredcorr.get_mapper_params_str()
    
Returns
-------
 See msvf.get_pair_str()
    
Notes
-----
|
| **Reader:**
|
|  See msvf.read_frame()
|
|
| **Configuration:**
|
|  See const_mapred.py
|
|
| **Debugging:**
|
|  See const_debug.py

"""
#History:
#initial version: 2015.12 ajva
#MIT Haystack Observatory

from __future__ import print_function,division
import sys
import base64
import imp
import os
import numpy as np
import scipy.fftpack

# Library for reading VDIF files
import lib_vdif
imp.reload(lib_vdif)
from lib_vdif import *

import lib_pcal
imp.reload(lib_pcal)
from lib_pcal import *

# Basic (de)quantizer
# TO DO: use newer implementation for dequantizer
import lib_quant
imp.reload(lib_quant)
from lib_quant import *

# (De)serializer for info on scenario
import lib_ini_files
imp.reload(lib_ini_files)
from lib_ini_files import *

# Vector quantization                           # VQ disabled
#import lib_vq
#imp.reload(lib_vq)
#from lib_vq import *

# Constants for mapper and reducer
import const_mapred
imp.reload(const_mapred)
from const_mapred import *

# Constants for performance
import const_performance
imp.reload(const_performance)
from const_performance import *

#from bitarray import bitarray

import lib_acc_comp
imp.reload(lib_acc_comp)
from lib_acc_comp import *

import lib_delay_model
imp.reload(lib_delay_model)
from lib_delay_model import *

import lib_debug
imp.reload(lib_debug)
from lib_debug import *





# TO DO: bring debug flags to configuration file
# TO DO: define constants for "zM" and "zR"




###########################################
#           Haddop environmet variables
###########################################


def get_current_filename():
    """
    Get the name of the file currently being processed from the environment variable.
    Note this is only for hadoop, thus the variable has to be created if running in pipeline, (which
        is already done in lib_mapredcorr.py.
    """
    
    file_name = str(os.environ.get(MAP_INPUT_FILE))
    if file_name!="None":
        file_name=file_name.split('/')[-1]
    return(file_name)



###########################################
#           MapReduce key management
###########################################



def get_pair_str(char_p,pair,accu_block,mod_channel,num_channels,seconds_fr,first_sample_signal,station_id,\
                 mod_polarization_id,freq_sample,bits_per_sample,data_type_char,encoding,\
                 n_bins_pcal_val,pcal_freq,one_baseline_per_task,task_scaling_stations,\
                 id_pair=0,tot_pairs=0,tot_accu_blocks=1,num_samples=0,abs_delay=0.0,rate_delay=[],\
                 freq_channel=0.0,fractional_sample_delay=0.0,accumulation_time=0.0,shift_int=0,sideband="L"):
    """
    Build output string with key and first part of value (metadata) for map output.
    
    Parameters
    ----------
     char_p : char {'x','r','y'}
         identifies the mode of operation:
            |       'x' for all-baselines-per-task,
            |       'r' for linear scaling with the number of stations,
            |       'y' for one-baseline-per-task.
     pair : str
         A.A-A.A for all-baselines-per-task, station0.polarization0-station1.polarization1 for single-baseline-per-task.
     accu_block : int
         accumulation period id.
     mod_channel : int
         channel id.
     num_channels : int
         number of channels.
     seconds_fr : int
         [repeated] currently same as accu_block.
     first_sample_signal : int
         sample number for the first sample in this chunk of samples.
     station_id : int
         station identifier corresponding to this chunk of samples.
     mod_polarization_id : int
         polarization identifier corresponding to this chunk of samples.
     freq_sample : int
         sampling frequency [Hz].
     bits_per_sample : int
         number of bits per sample component.
     data_type_char : char {'r','c'}
         'r' for real, 'c' for complex.
     encoding : str
         type of encoding compression used (C_INI_MEDIA_C_*).
     n_bins_pcal_val : int
         number of bins for the phase calibration window.
     pcal_freq : int
         phase calibration tone separation [Hz].
     one_baseline_per_task : int
         [default 0], 1 for one baseline per task (work in progress)
     task_scaling_stations : int
         [default 0], 1 for linear scaling with stations (work in progress)
     id_pair
         [only used if one_baseline_per_task==1]
     tot_pairs 
         [currently unused, but should be used if one-baseline-per-task]
     tot_accu_blocks : int
         number of accumulation periods in the scan.
     num_samples : int
         number of samples in this chunk (required in reducer in case last bytes not filled with samples).
     abs_delay : float
         absolute delay for this chunk.
     rate_delay : list
         list with delay polynomials (see get_absolute_delay()).
     freq_channel : float
         edge frequency [Hz] corresponding to the samples in this chunk.
     fractional_sample_delay : float
         fractional sample delay for the first sample in this chunk.
     accumulation_time : float
         accumulation period duration [s].
     shift_int : int
         number of sample components used to offset these samples (integer delay).
     sideband : char {'L','U'}
         'L' for lower-sideband 'U' for upper-sideband.  
    
    Returns
    -------
     pair_str : str
         complete header for the current set of samples being processed.
    
    Notes
    -----
    |
    | **Output formatting and conventions:**
    |
    |  The fields in the key below (k1,k2,...) separated by FIELD_SEP are referenced in const_mapred.py for the sorting configuration.
    |   The metadata must follow the same order as defined in const_mapred.py.
    |
    |
    | **Notes:**
    |
    |  The full output corresponds to the output string generated here and the samples packed in base64 (done outside).
    |
    |
    | **TO DO:**
    |
    |  Simplify interface.
    |  seconds_fr, delete and take accu block... 
    |  Define constants for real and complex chars, and for lower and upper sideband.
    |  Define constant for initial "p" in line, initially for pair.
    """
    # Num channels spec : number of channels specified in ini file, used for partitioning
    #char_p="x"
    ## Check indices LSB
    #pcal_ind_row=pcal_ind_row[pcal_ind_row>(-1*n_bins_pcal_val)]
    ## Check indices USB
    #pcal_ind_row=pcal_ind_row[pcal_ind_row<(n_bins_pcal_val)]
    
    #If not autocorr do not print pcal info
    if one_baseline_per_task:
        if not((pair[0]==pair[2])and(pair[1]==pair[3])):
            #pcal_ind_row=[0]
            # TO DO: n_bins_pcal_val ?
            #n_bins_pcal=0
            pcal_freq=0
            

    
    # Using this key for full control on the partitioning (one key for reducer...)
    key_value=accu_block*num_channels+mod_channel
    if (one_baseline_per_task)or(task_scaling_stations):
        key_value=id_pair*tot_accu_blocks*num_channels+key_value
    
    first_sample_signal = int(first_sample_signal)
    
    # Station polarization
    st_pol = str(station_id) + SF_SEP + str(mod_polarization_id)
    
    # Delay information (lib_ini_files.get_rates_cache())
    [delay_rate_0,\
     delay_rate_1,\
     delay_rate_2,\
     delay_rate_ref,\
     clock_rate_0,\
     clock_rate_1,\
     clock_abs_rate_0,\
     clock_abs_rate_1,\
     clock_rate_ref,\
     model_only_delay,\
     clock_only_delay,\
     diff_frac] = rate_delay
    
    # Following format from const_mapred.py
    metadata_v = [st_pol,\
                  shift_int,\
                  fractional_sample_delay,\
                  abs_delay,\
                  delay_rate_0,\
                  delay_rate_1,\
                  delay_rate_2,\
                  delay_rate_ref,\
                  clock_rate_0,\
                  clock_rate_1,\
                  clock_abs_rate_0,\
                  clock_abs_rate_1,\
                  clock_rate_ref,\
                  model_only_delay,\
                  clock_only_delay,\
                  diff_frac,\
                  num_samples,\
                  freq_sample,\
                  bits_per_sample,\
                  first_sample_signal,\
                  data_type_char,\
                  n_bins_pcal_val,\
                  pcal_freq,\
                  mod_channel,\
                  freq_channel,\
                  accumulation_time,\
                  encoding,\
                  sideband]
    
    
    # Generation of KEY and VALUE in the same line
    #  FIELD_SEP: field separator
    #  SF_SEP:    sub-field separator
    # TO DO: define constant for hard-coded chars
    #                                                                           KEY
    #                                                                               Mode of operation         [k1]
    pair_str =  "p"+char_p
    #                                                                               Baseline (or all)         [k2,k3]
    pair_str +=     FIELD_SEP+str(pair[0])+SF_SEP+str(pair[1])+\
                    FIELD_SEP+str(pair[2])+SF_SEP+str(pair[3])+FIELD_SEP
    #                                                                               Accumulation              [k4,k5,k6,k7]
    pair_str += "a"+FIELD_SEP+str(key_value)+\
                    FIELD_SEP+str(accu_block)+\
                    FIELD_SEP+str(mod_channel)
    #                                                                               First sample id.          [k8]
    pair_str +=     FIELD_SEP+"f"+str(seconds_fr)+\
                       SF_SEP+str(first_sample_signal).zfill(PAD_S)+\
                       SF_SEP+str(mod_channel)
    #                                                                               Station id.               [k9]
    pair_str +=     FIELD_SEP+"s"+str(station_id)+\
                       SF_SEP+str(mod_polarization_id)
    #                                                                           SEPARATOR between key and value
    pair_str +=     FIELD_SEP+KEY_SEP
    #                                                                           VALUE
    pair_str+=' '.join(map(str,metadata_v))
    #                                                                                Metadata
    pair_str+=" "
    #                                                                                (Samples added outside)
    
    return(pair_str)                       
                                



def calculate_corr_pairs_one_baseline_per_task(tot_stations=3,tot_pols=1,auto_stations=0,auto_pols=1):
    """
    Create array with identificators for all the correlation pairs for all the stations p<station_i><station_j>.
    
    Only used in one-baseline-per-task mode.
    
    Parameters
    ----------
     tot_stations : int
         number of stations.
     tot_pols : int
         number of polarizations
     auto_stations : int
         |   0 : only different stations (default)
         |   1 : allow same station
         |   2 : only same station
     auto_pols : int
         |   0 : only different polarizations
         |   1 : allow same polarization (default)
         |   2 : only same polarization
    
    Returns
    -------
     pairs_list : list of lists [station_a,polarization_a,station_b,polarization_b]
    
    Notes
    -----
    |
    | **Limitations:**
    |
    |  Currently assuming that all the stations have the same number of polarizations.
    |
    |
    | **TO DO:**
    |
    |  This is only used in one-baseline-per-task mode, but still called from main and used in logging.
    """
    
    pairs_list=[]
    for s0 in range(0,tot_stations):
        for s1 in range(s0,tot_stations):
            for t0 in range(0,tot_pols):
                for t1 in range(0,tot_pols):
                    
                    # Stations (note use of < instead of != to avoid generating duplicates)
                    if auto_stations==0:
                        # Only different stations
                        condition_station=(s0<s1)
                    elif auto_stations==1:
                        # Allow same station
                        #condition_station=(s0<=s1)
                        condition_station=True
                    else: #auto_stations==2:
                        # Only same station
                        condition_station=(s0==s1)
                    
                    # Polarizations
                    #if auto_pols==0:
                    #    # Only different polarizations
                    #    condition_pols=(t0!=t1)
                    #elif auto_pols==1:
                    if auto_pols==1:
                        # Allow same thread
                        #condition_thread=True
                        condition_thread=True
                    else: #auto_pols==2:
                        # Only same thread (e.g. if threads are bands)
                        condition_thread=(t0==t1)
                    
                    # Generate correlation pairs based on previous conditions
                    if (condition_station)and(condition_thread):
                        # avoid duplicates (switching pairs location)
                        #if [s1,t1,s0,t0] not in pairs_list:
                        # TO DO: check this
                        if not((s0==s1)and(t0>t1)):
                            pairs_list.append([s0,t0,s1,t1])
                        #if (condition_thread==(s0==s1))and(t0!=t0):
                        #    pairs_list.append([s0,t1,s1,t0])
    return(pairs_list)




def get_pair_all_baselines_per_task():
    """
    Get pair for all baselines per tasks.
    """
    pair=["A","A","A","A"]
    return(pair)


def get_pair_linear_scaling(s0,t0):
    """
    Get pair for linear scaling with number of stations.
    
    Parameters
    ----------
     s0 : int
         station id
     t0 : int
         polarization id
    """
    pair=[str(s0),str(t0),"A","A"]
    return(pair)


def get_alloc_tasks_linear_scaling(num_pairs):
    """
    Get allocation of stations into tasks. It computes the matrix that defines which pairs are associated to each task.
    Only used in station-based-splitting (linear scaling with number of stations).
    
    Parameters
    ----------
     num_pairs : int
         number of pairs.
    
    Returns
    -------
     a : binary 2D square array
         task allocation.
    
    Notes
    -----
    |
    | **TODO:**
    |
    |  Detail the algorithm.
    """
    a=np.ones((num_pairs,num_pairs),dtype=int)
    a=np.triu(a)
    for i in range(1,int(np.ceil(num_pairs/2))):
        for j in range(num_pairs-i):
            if j<=num_pairs:
                a[j+i][j],a[j][j+i]=a[j][j+i],a[j+i][j]
    return(a)





###########################################
#           Input reader
###########################################


def read_frame(reader,show_errors,forced_frame_length=0,forced_format=C_INI_MEDIA_F_VDIF,forced_version=C_INI_MEDIA_V_CUSTOM):
    """
    It returns the header and samples in the frame, based on the information from the media.ini file. If this information
    is not available then it assumes that it is a vdif frame.
    
    Parameters
    ----------
     reader : file handle
         sys.stdin.
     show_errors : int
         [0 by default] 1 for verbose mode.
     forced_frame_length : int
         [0 by default] >0 to force the number of bytes per frame (if ==0 frame length is read in header)
     forced_format
         [leave deafult value] use only for new implementations of readers.
     forced_version
         [leave deafult value] use only for new implementations of readers.
    
    Returns
    -------
     header : list
         frame header following the format: 
           |     [seconds_frame,invalid_marker,legacy_marker,reference_epoch,frame_number,
           |     vdif_version,log_2_channels,frame_length,data_type,bits_per_sample,thread_id,station_id] where:
           |          seconds_frame:     integer value with seconds for this frame.
           |          invalid_marker:    VDIF invalid bit.
           |          legacy_marker:     VDIF legacy bit.
           |          reference_epoch:   VDIF frame epoch (float with MJD (TBC)).
           |          frame_number:      VDIF frame number (integer).
           |          vdif_version:      VDIF frame version (integer).
           |          log_2_channels:    VDIF logarithm in base 2 of the number of channels in the frame.
           |          frame_length:      VDIF frame length (integer with number of bytes).
           |          data_type:         VDIF frame data type bit.
           |          bits_per_sample:   number of bits per sample.
           |          thread_id:         VDIF thread identifier field (integer).
           |          station_id:        VDIF station id field (integer).
     allsamples : 1d numpy array of int
         sample components (see below for details).
     check_size_samples : int
         1 if read as many samples as expected from the frame length (and rest of metadata), 0 otherwise.
    
    Notes
    -----
    |
    | **Adding new libraries:**
    |
    |  -Each library will be identified by "format" and "version", to be specified in the media.ini file.
    |  -Add the new format and version into const_ini_media.py
    |  -Add the check for new format in the if structure below (if forced format==...)
    |  -See the definition of the header to be returned above.
    |  -The samples should be in a 1D numpy array of integers. E.g. for VDIF complex frame, [I0, Q0, I1, Q1, ...]
    |  -The implementation is currently tied to the VDIF format, so samples corresponding to different channels will be interleaved.
    |  -If multiple bands or polarizations per file see VDIF specification (multiple thread or multiple channels).
    |  -If simple case with single band and single polarization then log_2_channels=0 and thread_id=0 (and configure media.ini accordingly)
    |  -See lib_vdif.py for more info.
    |
    |
    | **TO DO:**
    |
    |  Consider providing a general interface to allow easy development of new libraries.
    |  use_ini_info always used, remove option.
    |  forced_frame_length is not taken into account, remove it (length is read in the VDIF frame).
    |  Remove option for bitarray_structures, no longer used.
    """
    check_size_samples = 0
    
    
    header=[]
    allsamples=[]
    
    other_cases=1
    # VDIF
    if forced_format == C_INI_MEDIA_F_VDIF:
        # (custom version)
        if forced_version == C_INI_MEDIA_V_CUSTOM:
            [header,allsamples,check_size_samples] = lib_vdif.read_vdif_frame(f=reader,show_errors=show_errors,forced_frame_length=forced_frame_length,v=VERBOSE_MAPPER_IO)
            other_cases=0
    
    # Other cases: f,show_errors=0,forced_frame_length=0,offset_bytes=0,encode_int=0
    if other_cases==1:
        [header,allsamples,check_size_samples] = lib_vdif.read_vdif_frame(f=reader,show_errors=show_errors,forced_frame_length=forced_frame_length,v=VERBOSE_MAPPER_IO)
    
    
    return([header,allsamples,check_size_samples])




###########################################
#      Delay/offsets/samples management
###########################################



def get_num_samples_per_frame(allsamples,num_channels,data_type):
    """
    Based on number of channels and data type (complex or real, get number of samples per frame.
    
    Parameters
    ----------
     allsamples : int
         total number of sample components in the frame.
     num_channels : int
         number of channels in the frame.
     data_type : int
         0 for real, 1 for complex.
    
    Returns
    -------
     tot_samples_per_channel_and_frame_full : int
         number of samples per channel per frame (full samples, i.e. R or R+jI)
     totsamples_per_channel_and_frame_single : int
         number of sample components (i.e. R, I).
    """
    tot_samples_per_channel_and_frame=len(allsamples)//num_channels
    # number of full samples (real and imag for complex)
    if data_type==1:
        tot_samples_per_channel_and_frame_full=tot_samples_per_channel_and_frame//2
    else:
        tot_samples_per_channel_and_frame_full=tot_samples_per_channel_and_frame
    
    return([tot_samples_per_channel_and_frame,tot_samples_per_channel_and_frame_full])







def check_time_frame(accu_block,rel_pos_frame,actual_frame_time,seconds_ref,seconds_duration):
    """
    Check if actual timestamp of this frame is inside the experiment time wnidow.
    
    Parameters
    ----------
     accu_block
         accumulation period id (-1 if outside scan window).
     rel_pos_frame
         relative frame position in accumulation period.
     actual_frame_time
         actual timestamp (considering delays) for the first sample of the frame.
     seconds_ref
         start of the scan [s].
     seconds_duration
         duration of the scan [s].
     
    Returns
    -------
     process_frame : int
         1 if frame inside scan.
     after_end_time : int
         1 if frame after end of defined window.
    """
    process_frame=1
    after_end_time=0
    if accu_block<0:
        process_frame=0
    elif actual_frame_time<seconds_ref:
        if rel_pos_frame!=0: # if zero then split frame
            process_frame=0
    elif actual_frame_time>(seconds_ref+seconds_duration):
        process_frame=0
        after_end_time=1


    return([process_frame,after_end_time])



def get_seconds_fr_front(front_time,vector_seconds_ref,seconds_frame):
    """
    Find frontier seconds only if not available.
    
    Parameters
    ----------
     front_time
         frontier seconds (i.e. timestamp for start time polynomials).
     vector_seconds_ref
         list of floats with seconds for delay information (start time polynomials).
     seconds_frame
         seconds corresponding to this frame.
     
    Returns
    -------
     seconds_fr_nearest
         frontier seconds.
    """
    if (front_time is None)or(front_time==-1):
        seconds_fr_nearest=find_nearest_seconds(vector_seconds_ref,seconds_frame)
    else:
        seconds_fr_nearest=front_time
    return(seconds_fr_nearest)
    
    

def compute_shift_delay_samples(params_delays,vector_seconds_ref,freq_sample,seconds_frame,pair_st_so,data_type=0,\
                                front_time=None,cache_rates=[],cache_delays=[]):
    """
    Compute number of samples to shift signal (always positive since reference station is closest to source).
    
    Parameters
    ----------
     params_delays
         delay model ini file.
     vector_seconds_ref
         list of floats with seconds for delay information (start time polynomials).
     freq_sample
         sampling frequency [Hz].
     seconds_frame
         seconds corresponding to the frame to be processed.
     station_id
         corresponds to id number in stations ini file.
     source_id
         [default 0], see limitations.
     pair_st_so
     data_type
         0 for real, 1 for complex.
     front_time
         frontier time, that is, time corresponding to the start of the integration period (takes priority over the seconds of the frame)
     cache_rates
         temporary information on delays to avoid reprocessing of the input files (see lib_ini_files.get_rates_delays()).
     cache_delays
         list with [seconds_fr_nearest,pair_st_so,delay] from previous computation.
         
    Returns
    -------
     shift_int
         number of sample components to offset (integer delay).
     delay
         total delay (=freq_sample*(shift_int+fractional_sample_delay)).
     fractional_sample_delay
     error_out
         0 if sucess, -1 if error (e.g. accumulation period not found in ini file)
     cache_rates
         updated cache_rates (input).
     
    Notes
    -----
    |
    | **Limitations:**
    |
    |  Currently assuming single source (source_id always zero
    |
    |
    | **TO DO:**
    |
    |  Simplify code, no need for params_delays nor find_nearest().
    """
    
    

    #print("ft: "+str(front_time))
    seconds_fr_nearest=get_seconds_fr_front(front_time,vector_seconds_ref,seconds_frame)
    #seconds_fr_nearest=front_time
    if front_time is None:
        seconds_fr_nearest=find_nearest_seconds(vector_seconds_ref,seconds_frame)
    
    if seconds_fr_nearest>=-1:
        #rel_epoch=DELAY_MODEL_REL_MARKER+str(seconds_fr_nearest)
        #found_delay=1
        try:
            #delay = float(get_param_serial(params_delays,pair_st_so,rel_epoch))
            [delay,cache_delays] = get_delay_cache(seconds_fr_nearest,pair_st_so,params_delays,cache_delays)
        except ValueError:
            #found_delay=0
            print("zM\tWarning: could not get delay for pair "+pair_st_so+", "+str(seconds_fr_nearest)+", skipping frame")
        
            seconds_fr_nearest=-2
    
    if seconds_fr_nearest>=-1:    
        [shift_int,fractional_sample_delay]=get_delay_shift_frac(delay,freq_sample,data_type)
        
        error_out=0
    else:
        shift_int=-1
        delay=-1
        fractional_sample_delay=-1
        error_out=1
    
    
    
    return([shift_int,delay,fractional_sample_delay,error_out,cache_delays])



def get_absolute_delay(params_delays,vector_seconds_ref,seconds_frame,pair_st_so,front_time=None,cache_rates=[]):
    """
    Get all the delay information structures associated to the processed station, source and integration period.
    
    Parameters
    ----------
     params_delays
         delay model ini file.
     vector_seconds_ref : list of float
         seconds for delay information (start time polynomials).
     seconds_frame
         seconds corresponding to the frame to be processed.
     station_id
         corresponds to id number in stations ini file.
     source_id
         [default 0], see limitations.
     front_time
         frontier time, that is, time corresponding to the start of the integration period (takes priority over the seconds of the frame)
     cache_rates
         temporary information on delays to avoid reprocessing of the input files (see lib_ini_files.get_rates_delays()).
                         
    Returns
    -------
     abs_delay
         initial absolute delay.
     rate_delay
         delay polynomials.
     ref_delay
         delay for the "reference" station.
     error_out
         -1 if station, source and accumulation period not found, 0 if sucess.
     cache_rates
         updated cache_rates (input).
        
    Notes
    -----
    |
    | **Configuration:**
    |
    |  VERBOSE_INI_DELAYS: from lib_ini_delays.py.
    |
    |
    | **TO DO:**
    |
    |  Merge code with compute_shift_delay_samples to avoid repetition.
    """
    ref_delay=0.0
    # station_id,source_id(0),params_delay,seconds_frame,freq_sample
    #pair_st_so = "st"+str(station_id)+"-so"+str(source_id)
    
    if (front_time is None)or(front_time==-1):
        seconds_fr_nearest=find_nearest_seconds(vector_seconds_ref,seconds_frame)
    else:
        seconds_fr_nearest=front_time
       
    if seconds_fr_nearest>=-1:
        #abs_epoch=DELAY_MODEL_ABS_MARKER+str(seconds_fr_nearest)

        try:
            #abs_delay = float(get_param_serial(params_delays,pair_st_so,abs_epoch))
            [rate_delay,ref_delay,abs_delay,cache_rates]=get_rates_cache(seconds_fr_nearest,pair_st_so,params_delays,cache_rates)
            error_out=0
        except ValueError:
            abs_delay=-1
            rate_delay=-1
            error_out=1   
            
    else:
        abs_delay=-1
        rate_delay=-1
        error_out=1
    
    return([abs_delay,rate_delay,ref_delay,error_out,cache_rates])


def adjust_frame_num_and_seconds(fs,samples_per_channel_in_frame_full,samples_per_channel_in_frame_single,\
                                 second_frame,frame_num,shift_int,acc_time_str):
    """
    Get adjusted number of frame and shift inside frame considering delays. This is to determine which accumulation
       period the data corresponding to this frame should go to.
       
    Parameters
    ----------
     fs : float
         sampling frequency [Hz].
     samples_per_channel_in_frame_full : int
         number of samples per channel per frame (full samples, i.e. R or R+jI)
     samples_per_channel_in_frame_single : int
         number of sample components (i.e. R, I).
     second_frame : float
         seconds corresponding to the first sample of the frame.
     frame_num : int
         frame number.
     shift_int : int
         shift in number of sample components.
     acc_time_str : str(float)
         accumulation period duration [s].
    
    Returns
    -------
     frame_num_adjusted
         frame number considering shift (forced to be positive).
     frame_num_adjusted_neg
         frame number considering shift (may be negative)
     adjusted_shift_inside_frame
         first sample component inside the frame considering the shift.
     
    Notes
    -----
    |
    | **TO DO:**
    |
    |  Remove acc_time_str.
    |  Check for potential issues in second change... acc_time_str and second_frame are not used!
    |  (!) Old implementation, may not take into account non-integer multiples of second for acc. period, check (!)
    """

    adjusted_shift_inside_frame = 0
    
    frames_per_second=int((fs/samples_per_channel_in_frame_full)//1)

    frame_num_offset=shift_int//samples_per_channel_in_frame_single
    frame_num_adjusted=frame_num-frame_num_offset
    frame_num_adjusted_neg=frame_num_adjusted
    
    
    if frame_num_adjusted<0:
        # Then they correspond to previous second
        frame_num_adjusted=frame_num_adjusted%frames_per_second
    
    adjusted_shift_inside_frame=shift_int%samples_per_channel_in_frame_single
    
    return([frame_num_adjusted,frame_num_adjusted_neg,adjusted_shift_inside_frame])



def get_pointers_samples(tot_samples_one_frame,adjusted_shift_inside_frame,accu_block,\
                           rel_pos_frame,tot_samples_sup_frame,ref_offset=0):
    """
    Compute metadata relative to sample organization (sample ids, offsets, chunk sizes...).
    
    Parameters
    ----------
     tot_samples_one_frame
         number of samples per channel per frame.
     adjusted_shift_inside_frame
         shift (number of samples) for the first sample inside the frame.
     accu_block
         accumulation block corresponding to these samples (last sample).
     rel_pos_frame
         number of frame relative to the accumulation period.
     tot_samples_sup_frame
         number of samples per frame [see notes below].
     ref_offset
         integer shift (number of samples) for the first sample of the stream
     
    Returns
    -------
     tot_samples_v
         number of samples in this chunk.
     seconds_v
         seconds corresponding to the start of the accumulation period (reference is zero).
     offset_first_sample_iterator_v
         offset used to fetch samples from the array with the samples.
     offset_first_sample_signal_v
         offset used to compute the position of this chunk in the complete stream.
     chunk_size_v
         same as tot_samples_v [remove].
     acc_v
         same as seconds_v [remove].
     
    Notes
    -----
    |
    | **TO DO:**
    |
    |  Superframe functionality needs debugging.
    |  Check ref_offset.
    |  Remove chunk_size_v and acc_v.
    """
                 
    if rel_pos_frame==0:
        # If first frame of the accumulation period, simply take the part of the data corresponding to this acc period
        # TO DO: The first part is dismissed, consider adding to previous acc period (?)
        tot_samples_v = [tot_samples_sup_frame-adjusted_shift_inside_frame]
        seconds_v = [accu_block]
        # Disregard first part...
        offset_first_sample_iterator_v=[adjusted_shift_inside_frame]
        offset_first_sample_signal_v=[ref_offset]
        chunk_size_v = tot_samples_v[:]
        acc_v=seconds_v
        

    else:
        tot_samples_v = [tot_samples_sup_frame]
        seconds_v = [accu_block]
        offset_first_sample_iterator_v=[0]
        offset_first_sample_signal_v=[ref_offset+rel_pos_frame*tot_samples_one_frame-adjusted_shift_inside_frame]
        chunk_size_v=[tot_samples_sup_frame]
        acc_v=seconds_v
    
    return([tot_samples_v,seconds_v,offset_first_sample_iterator_v,offset_first_sample_signal_v,\
                        chunk_size_v,acc_v])



###########################################
#           Output data packing
###########################################


def pack_samples(signal_chunk_fft,bits_per_sample):
    """
    Pack the sample components into bytes to avoid data storage overhead.
    
    Parameters
    ----------
     signal_chunk_fft : 1D numpy array
         sample components (integer values).
     bits_per_sample : int
         number of bits per sample component.
     
    Return
    -------
     signal_chunk_fft_out : 1D numpy array
         bytes containing packed sample components.
    """
    range_offsets = range(bits_per_sample-1,-1,-1)
    bits_offset = [((signal_chunk_fft)>>i) & (1) for i in range_offsets]
    bits_extended = np.concatenate(bits_offset)
    
    bits_trans = np.transpose(np.reshape(bits_extended,(bits_per_sample,-1)),(1,0))
    signal_chunk_fft_out = np.packbits(bits_trans.reshape(1,-1))
    
    return(signal_chunk_fft_out)


def encode_samples(signal_chunk_fft_out,encode_b64,apply_compression):
    """
    Encode packed samples into base64.
    
    Parameters
    ----------
     signal_chunk_fft_out : 1D numpy array
         bytes containing packed sample components.
     encode_b64 : int
         use base64 encoding, 1 by default.
     apply_compression : int
         0 by default.
     
    Returns
    -------
     signal_chunk_fft_out : str
         signal encoded into base64.
    """
    if (encode_b64==1)and(apply_compression==0):
        signal_chunk_fft_out = base64.b64encode((signal_chunk_fft_out))
    elif apply_compression==1:
        signal_chunk_fft_out = ' '.join(map(str,signal_chunk_fft_out))
    else:
        signal_chunk_fft_out = ' '.join(signal_chunk_fft_out) 
    return(signal_chunk_fft_out)


def pack_and_encode_samples(signal_chunk_fft,use_bitarrays,encode_b64,apply_compression,bits_per_sample):
    """
    Encode signal chunk for output of mapper.
    
    Parameters
    ----------
     signal_chunk_fft : 1D numpy array of int
         sample components (integer values).
     use_bitarrays : int
         0 by default.
     encode_b64 : int
         use base64 encoding, 1 by default.
     apply_compression : int
         0 by default.
     bits_per_sample : int
         number of bits per sample component.
  
    Returns
    -------
     signal_chunk_fft_out : str
         signal encoded into base64.
    
    Notes
    -----
    |
    | **TO DO:**
    |
    |  Place together with decode_samples_b64() for better organization.
    """
    

    if (use_bitarrays==0):
        if (encode_b64==0):
            signal_chunk_fft_out = ''.join(map(str,signal_chunk_fft))
        else:
            signal_chunk_fft_out = pack_samples(signal_chunk_fft,bits_per_sample)
            
    else:
        signal_chunk_fft_out = signal_chunk_fft
        
    signal_chunk_fft_out = encode_samples(signal_chunk_fft_out,encode_b64,apply_compression)
        
    return(signal_chunk_fft_out)




###########################################
#           Compression
###########################################


def get_codebook_info(codecs_serial,params_media,current_file_name,station_name,chunk_size,apply_compression):
    """
    Get information for compression library (e.g. vector quantization) if used.
    
    Parameters
    ----------
     codecs_serial : str
         serialized codecs for compression.
     params_media : str
         with serialized media.ini.
     current_file_name : str
         name of the file currently being processed.
     station_name : str
         name of the station.
     chunk_size : int
         number of samples per chunk.
     apply_compression : bool
         whether to apply compression or not
     
    Returns
    -------
     encoding
         [HARDCODED TO NO COMPRESSION]
     codebook
         codebook extracted from the codecs (vector quantization).
     codebook_name
         codebook name from .ini.
     apply_compression
         updated version of input based on configuration in .ini.
    
    Notes
    -----
    |
    | **TO DO:**
    |
    |  Harcoded output.
    """
    
    #encoding = get_param_serial(params_media,current_file_name,C_INI_MEDIA_COMPRESSION)
    # TO DO: Hardcoded value!! Compression disabled
    encoding=C_INI_MEDIA_C_NO
    
    if encoding==C_INI_MEDIA_C_NO:
        codebook_name = ""
        codebook = []
    else:
        codebook = get_codebook_from_serial(codecs_serial,station_name,"-1",-1,chunk_size)
        codebook_name = get_param_serial(params_media,current_file_name,C_INI_MEDIA_C_CODECS)
    if codebook!=[]:
        apply_compression=True
    
    return([encoding,codebook,codebook_name,apply_compression])
                    









###########################################
#            Main
###########################################


def main():

    read_rest=0
    first_sample_signal=0


    # Read parameters                                  # See lib_mapredcorr.get_mapper_params_str() for interface documentation.
    tot_stations =            int(sys.argv[1])                 
    tot_pols =                int(sys.argv[2])                     
    chunk_size_in =           int(sys.argv[3])               
    accumulation_time_str =       sys.argv[4]
    seconds_ref =             int(sys.argv[5])                
    seconds_duration =      float(sys.argv[6])
    first_frame_num =         int(sys.argv[7])
    num_frames =              int(sys.argv[8])
    auto_stations =           int(sys.argv[9]) 
    auto_pols =               int(sys.argv[10])
    use_ini_info =            int(sys.argv[11])
    ini_stations =                sys.argv[12]
    ini_media =                   sys.argv[13]
    ini_delays =                  sys.argv[14]
    codecs_serial =               sys.argv[15]
    FFT_HERE =                int(sys.argv[16])
    INTERNAL_LOG =            int(sys.argv[17]) 
    FFTS_PER_CHUNK =          int(sys.argv[18]) 
    WINDOWING =                   sys.argv[19]
    ONE_BASELINE_PER_TASK =   int(sys.argv[20])
    PHASE_CALIBRATION =       int(sys.argv[21])
    MIN_MAPPER_CHUNK =        int(sys.argv[22])
    MAX_MAPPER_CHUNK =        int(sys.argv[23])
    TASK_SCALING_STATIONS =   int(sys.argv[24])
    SINGLE_PRECISION =        int(sys.argv[25]) # Currently not used. TO DO: use for FFT at mapper
    
    
    
    # Experiment configuration
    # Initialization files
    stations_serial_str=serialize_config(ini_stations)
    media_serial_str=serialize_config(ini_media)
    delays_serial_str=serialize_config(ini_delays)
    # Serializations into vectors
    params_stations=serial_params_to_array(stations_serial_str)
    params_media=serial_params_to_array(media_serial_str)
    params_delays=serial_params_to_array(delays_serial_str)


    
    # Frame number ranges
    # Check frame range if values specified greater than 0
    last_frame_num = first_frame_num+num_frames
    check_frame_range=True
    if first_frame_num<0 or last_frame_num<0:
        check_frame_range=False

    
    
    # Chunk size
    # Adjust chunk size (may be negative)
    chunk_size_in = chunk_size_in * FFTS_PER_CHUNK
    # Limit chunk size to avoid problems with line lengths
    # Minimum size is enforced
    if MAX_MAPPER_CHUNK>0:
        if (MAX_MAPPER_CHUNK<chunk_size_in)or(chunk_size_in<0):
            chunk_size_in=MAX_MAPPER_CHUNK
    if MIN_MAPPER_CHUNK>0:
        if (MIN_MAPPER_CHUNK>chunk_size_in):
            chunk_size_in=MIN_MAPPER_CHUNK
    


    # Name of file currently being processed
    current_file_name = get_current_filename()
    


    print_el=0               # used in debugging
    count_print=-1
    last_pair_st_so=""       # used in superframes
    sup_frame_id=0
    apply_compression=False  # other parameters
    mod_channel=-1
    frame_num_adjusted = 0
    tot_samples_per_channel_and_frame = 0
    actual_num_samples=0
    # data type initials (real / complex)
    data_type_chars = DATA_TYPE_LIST
    ref_offset = 0
    freq_sample_in=0
    cache_rates=[]
    cache_delays=[]
    pairs=[]
    tot_pairs=0

    
    # Frontiers for accumulation periods
    tot_accu_blocks = get_tot_acc_blocks(accumulation_time_str,seconds_duration)
    accumulation_time=get_acc_float(accumulation_time_str)
    list_acc_frontiers = get_list_acc_frontiers(accumulation_time,seconds_duration,seconds_ref)
    
    
    # TO DO: add checks for initialization (?)
    success_init=1
    
    
    
    if DEBUG_ALIGN:
        print_debug_m_align_header()
        
        
        
    
    # Hadoop reads from stdin
    reader = sys.stdin
    

    ###################################
    #   Process experiment .ini info
    ###################################

    # Read information from ini files (default)
    if use_ini_info==1: 
        
        forced_frame_length =                   int(get_param_serial(   params_media,current_file_name,C_INI_MEDIA_FRAMEBYTES))
        forced_format =                             get_param_serial(   params_media,current_file_name,C_INI_MEDIA_FORMAT)
        forced_version =                            get_param_serial(   params_media,current_file_name,C_INI_MEDIA_VERSION)
        tot_pols =                                  get_param_total(    params_media,current_file_name,C_INI_MEDIA_POLARIZATIONS)
        
        pols_assoc_vector=     [int(val) for val in get_param_eq_vector(params_media,current_file_name,C_INI_MEDIA_POLARIZATIONS)]
        channels_assoc_vector= [int(val) for val in get_param_eq_vector(params_media,current_file_name,C_INI_MEDIA_CHANNELS)]
        freqs_assoc_vector = [float(val) for val in get_param_eq_vector(params_media,current_file_name,C_INI_MEDIA_FREQUENCIES,modein="str")]
        sidebands_assoc_vector=                     get_val_vector(     params_media,current_file_name,C_INI_MEDIA_SIDEBANDS)
        freq_sample_in =                   int(float(get_param_serial(  params_media,current_file_name,C_INI_MEDIA_FREQ_SAMPLE)))
        station_name =                              get_param_serial(params_media,current_file_name,C_INI_MEDIA_STATION)
        
        station_id =                            int(get_param_serial(params_stations,station_name,C_INI_ST_ID))
        
        num_channels_spec=len(set(channels_assoc_vector))

        #TODO: generalize for multiple sources...

        
        # Phase calibration: compute number of bins
        # TO DO: do specific check instead of exception   
        try:
            fs =                          int(float(get_param_serial(   params_media,current_file_name,C_INI_MEDIA_FREQ_SAMPLE))//1)
            f_pcal =                      int(float(get_param_serial(   params_media,current_file_name,C_INI_MEDIA_F_PCAL))//1)
            o_pcal =                      int(float(get_param_serial(   params_media,current_file_name,C_INI_MEDIA_O_PCAL))//1)
        except ValueError:
            success_init=0
            #print("zM"+KEY_SEP+" File "+current_file_name+" not found in configuration."+current_file_name)
        if success_init:
            [n_bins_pcal,pcal_freq] = get_pcal_ind(freqs_assoc_vector,sidebands_assoc_vector,fs,f_pcal,o_pcal)



    # If error simply read all input (to avoid errors in Hadoop) and exit
    if success_init==1:
        # Generate array with correlation pairs.
        #TO DO: uniformize for different modes
        if ONE_BASELINE_PER_TASK:
            pairs = calculate_corr_pairs_one_baseline_per_task(tot_stations=tot_stations,tot_pols=tot_pols,\
                                                        auto_stations=auto_stations,auto_pols=auto_pols)
            tot_pairs = len(pairs)
        
        # If scaling with stations get vectors with allocation.
        tasks_pairs=np.array([])
        if TASK_SCALING_STATIONS:
            tasks_pairs=get_alloc_tasks_linear_scaling(tot_stations*tot_pols)
            
            
        
        ######################################################
        #   Loop for reading and processing  VDIF frames
        ######################################################
        
        keep_reading=1
        while keep_reading==1:
            
            # Get header and samples from frame 
            [header,allsamples,check_size_samples] = read_frame(reader,SHOW_ERRORS,forced_frame_length,forced_format,forced_version)
            
            error_frame = C_M_READ_SUCCESS
            
            if not(header is None):
                
                # Decode header
                [seconds_fr,invalid,legacy,ref_epoch,frame_num,vdif_version,log_2_channels,
                    frame_length,data_type,bits_per_sample,thread_id,station_id_frame] = header
                
                actual_num_samples=len(allsamples)
                
                #TO DO: this may not be necessary, used only if compression?
                # Adjust chunk size if complex data
                chunk_size=chunk_size_in
                if data_type==1:
                    chunk_size=2*chunk_size_in
                
                process_frame = 1
                
                # If less samples than expected dismiss frame
                if check_size_samples==0:
                    process_frame=0
                    error_frame = C_M_READ_ERR_NO_SAMPLES
                
                # Override data from the header with serialized info (if applicable)
                if process_frame and use_ini_info:
                    # TO DO: currently only supporting single source!!
                    # HARDCODED VALUE: fix this, add support for multiple sources...
                    source_id=0 
                    
                    # TO DO: consider removing
                    # Not used by default (initially added for compression)
                    [encoding,codebook,codebook_name,apply_compression] = get_codebook_info(codecs_serial,params_media,current_file_name,\
                                                                                   station_name,chunk_size,apply_compression)
            
                
                pair_st_so = get_pair_st_so(station_id,source_id)
                # If same station anad source do not look again for .ini info
                # TO DO: support for multiple sources...
                if last_pair_st_so!=pair_st_so:
                    vector_params_delay =  get_all_params_serial(params_delays,pair_st_so)
                    vector_seconds_ref = get_vector_delay_ref(vector_params_delay)
                    vector_params_delay=[]
                    last_pair_st_so=pair_st_so
                    sup_frame_id=0
                    
                # Get number of samples per channel
                num_channels = 2**log_2_channels
                [tot_samples_per_channel_and_frame,tot_samples_per_channel_and_frame_full]=get_num_samples_per_frame(allsamples,num_channels,data_type)
                

                ###########################
                #   Delay computations
                ###########################

                
                # Get the timestamp for the first sample in this frame (i.e. adjust integer second with offset of frame number)
                adjusted_frame_time=adjust_seconds_fr(tot_samples_per_channel_and_frame_full,\
                                                      freq_sample_in,seconds_fr,frame_num)
                
                
                # Get absolute delay for this frame, based on its timestamp
                [i_f,front_time]=get_acc_block_for_time(adjusted_frame_time,list_acc_frontiers)
                [abs_delay,rate_delay,ref_delay,error_delay,cache_rates] = get_absolute_delay(params_delays=params_delays,\
                                                                        vector_seconds_ref=vector_seconds_ref,seconds_frame=adjusted_frame_time,\
                                                                        pair_st_so=pair_st_so,\
                                                                        front_time=front_time,cache_rates=cache_rates)
                
                # Shift
                ref_offset=ref_delay*fs
                ref_offset=int(np.modf(ref_offset)[1])
                if data_type==1:
                    ref_offset*=2

                
                if error_delay!=0:
                    error_frame = C_M_READ_ERR_DELAY_ABS
                    process_frame=0

                if process_frame==1 and error_delay==0:
                    [i_f,front_time]=get_acc_block_for_time(adjusted_frame_time,list_acc_frontiers)
                    [shift_int,delay,fractional_sample_delay,error_delay,cache_delays] = compute_shift_delay_samples(params_delays,\
                                        vector_seconds_ref,freq_sample_in,adjusted_frame_time,pair_st_so,data_type,\
                                        front_time,cache_rates,cache_delays)

                
                    if error_delay!=0:
                        process_frame=0
                        error_frame = C_M_READ_ERR_DELAY_SHIFT
                
                
            
                print_el=0
                if process_frame:
                    print_el=1
                    [frame_num_adjusted,frame_num_adjusted_neg,adjusted_shift_inside_frame]= adjust_frame_num_and_seconds(fs,\
                                                                                    tot_samples_per_channel_and_frame_full,\
                                                                                    tot_samples_per_channel_and_frame,\
                                                                                    seconds_fr,\
                                                                                    frame_num,\
                                                                                    shift_int,accumulation_time)

                    actual_frame_time=adjusted_frame_time-delay
    
                    # Frontier with uncorrected times
                    [i_front_unc,acc_block_x_unc] = get_acc_block_for_time(adjusted_frame_time, list_acc_frontiers)
            
                    # Frontier with actual times
                    [i_front,acc_block_x] =         get_acc_block_for_time(actual_frame_time,   list_acc_frontiers)
                    
                    
                    [accu_block,rel_pos_frame,unused_sec2,unused_seconds_previous_frame,unused_frame_num_last] =\
                                            get_frame_acc(seconds_fr,frame_num_adjusted,fs,\
                                                       tot_samples_per_channel_and_frame_full,\
                                                       list_acc_frontiers,accumulation_time)
                    
                    # If the computed frontiers differ, then the frame is not aligned,
                    #     i.e. it does not correspond to this acc. period.
                    aligned_frame=1
                    if i_front_unc!=i_front:
                        if i_front_unc==-1:
                            process_frame=0
                        else:
                            
                            # If the relative possition of the frame is not zero, it is a remote station
                            #if rel_pos_frame!=0:
                            # make sure it is a remote station (delay is 0 for ref station) # fix for bug on ref station first frame
                            if (rel_pos_frame!=0)and(delay!=0):
                                aligned_frame=0
                            
                                # TO DO: check this
                                # Force time to be in previous acc period for getting the proper delay model
                                re_adjusted_frame_time = adjusted_frame_time-accumulation_time
                                
                                # Recompute based on model for previous acc period
                                [i_f,front_time]=get_acc_block_for_time(re_adjusted_frame_time,list_acc_frontiers)
                                
                                [shift_int,delay,fractional_sample_delay,\
                                 error_delay,cache_delays] = compute_shift_delay_samples(params_delays,vector_seconds_ref,\
                                                freq_sample_in,re_adjusted_frame_time,pair_st_so,data_type,\
                                                front_time,cache_rates,cache_delays)
                                
                                [frame_num_adjusted,frame_num_adjusted_neg,\
                                 adjusted_shift_inside_frame] = adjust_frame_num_and_seconds(fs,\
                                                            tot_samples_per_channel_and_frame_full,\
                                                            tot_samples_per_channel_and_frame,\
                                                            seconds_fr,frame_num,shift_int,accumulation_time)
    
                                
                                actual_frame_time = adjusted_frame_time-delay
                                
                                [i_front_unc,acc_block_x_unc] = get_acc_block_for_time(adjusted_frame_time,list_acc_frontiers)
                                [i_front,acc_block_x] =         get_acc_block_for_time(actual_frame_time,list_acc_frontiers)

                                [unused_accu_block_no,rel_pos_frame,unused_sec2no,\
                                     unused_seconds_previous_frameno,\
                                     unused_frame_num_lastno] = get_frame_acc(actual_frame_time,0,fs,\
                                                                tot_samples_per_channel_and_frame_full,\
                                                                list_acc_frontiers,accumulation_time) 

                    
                    
                    # Correct acc period for aligned frame with offset (due to references always to first sample of the frame)
                    #if (rel_pos_frame==0)and(adjusted_shift_inside_frame!=0):
                    # fix for bug offset lesser than one sample
                    if (rel_pos_frame==0)and((adjusted_shift_inside_frame!=0)or(delay>0)):
                        i_front+=1

                    elif (rel_pos_frame==0):
                        # Correct also delay lesser than one sample in first frame of acc block
                        # TO DO: ned more elegant solution to these fixes
                        [i_front_second_sample,acc_block_second_sample]=get_acc_block_for_time(actual_frame_time+1/float(fs),list_acc_frontiers)
                        if i_front_second_sample==0:
                            i_front=i_front_second_sample
                            acc_block_x=acc_block_second_sample

                    accu_block=i_front
                    
                    # Finally get polynomials for this acc period (for -1 will get last, but will not be taken to output)
                    front_acc = list_acc_frontiers[accu_block]
                    if front_acc==list_acc_frontiers[-1]:
                        front_acc = -1
                    [shift_int,delay,fractional_sample_delay,error_delay,cache_delays] = compute_shift_delay_samples(params_delays,\
                                                                                    vector_seconds_ref,freq_sample_in,\
                                                                                    front_acc,pair_st_so,\
                                                                                    data_type,\
                                                                                    front_acc,cache_rates,cache_delays)

                    
                    if frame_num_adjusted_neg<0:
                        [unused_accu_block_no,rel_pos_frame,unused_sec2no,
                             unused_seconds_previous_frameno,unused_frame_num_lastno] = get_frame_acc(actual_frame_time,0,fs,\
                                                                                         tot_samples_per_channel_and_frame_full,\
                                                                                         list_acc_frontiers,accumulation_time)                    
                    
                    
                    if DEBUG_ALIGN:
                        print_debug_m_align_no_end(station_name,station_id,tot_samples_per_channel_and_frame,data_type,\
                                        tot_samples_per_channel_and_frame_full,seconds_fr,frame_num,adjusted_frame_time,\
                                        delay,actual_frame_time,rel_pos_frame,i_front,accu_block,frame_num_adjusted_neg,\
                                        frame_num_adjusted,shift_int,adjusted_shift_inside_frame,fractional_sample_delay,\
                                        process_frame,aligned_frame,count_print)
                        
                    count_print=0  # For debugging, number of output lines per frame, updated below
                    
                    

                if process_frame==0:
                    if DEBUG_ALIGN:
                        if print_el:
                            print("")
                            
                            
                if process_frame==1:
                    
                    ###########################
                    #   Corner-turning
                    ###########################
                    
                    # Reshape array with samples (corner turning)
                    samples = lib_vdif.reshape_samples(allsamples,data_type,tot_samples_per_channel_and_frame,num_channels)
                    

                        
                    # read media.ini file to figure out correspondences between channels and band/polarizations
                    # Change these with the new values after processing media.ini
                    mod_polarization_id = thread_id
                    mod_channels = range(0,num_channels)
                    if use_ini_info==1:
                        mod_polarization_ids = pols_assoc_vector
                        mod_channels = channels_assoc_vector


                    ###########################
                    #   Super-frames
                    ###########################
                    
                    # TO DO: add support for superframes (i.e. multiple frames per output) here
                    # Currently disabled
                    
                    do_sup_frame=0
                    if num_channels>1:
                        # Multichannel and thus few samples per frame for each channel
                        # Will do superframe grouping NUM_FRAMES_PER_LINE frames
                        if NUM_FRAMES_PER_LINE>1:
                            #accu_block
                            # Get acc block for last frame in super frame
                            offset_num_frames=NUM_FRAMES_PER_LINE*tot_samples_per_channel_and_frame_full/float(fs)
                            [accu_block_last_sup,acc_unused]=get_acc_block_for_time(actual_frame_time+offset_num_frames,list_acc_frontiers)
                            whole_sup_frame=0
                            if accu_block==accu_block_last_sup:
                                whole_sup_frame=1
                                if whole_sup_frame:
                                    if sup_frame_id==0:
                                        do_sup_frame=1
                                        # initialize
                                        
                                        first_in_sup_info = [adjusted_shift_inside_frame,accu_block,rel_pos_frame,\
                                                             actual_frame_time,seconds_ref,seconds_duration]
                                        u_tot_samples_per_channel_and_frame = tot_samples_per_channel_and_frame
                                        u_samples=np.copy(samples)
                                        
                                        
                                    elif sup_frame_id==(NUM_FRAMES_PER_LINE-1):
                                        # last frame, write
                                        do_sup_frame=1
                                        
                                        [adjusted_shift_inside_frame,accu_block,rel_pos_frame,\
                                         actual_frame_time,seconds_ref,seconds_duration] = first_in_sup_info
                                        u_tot_samples_per_channel_and_frame+=tot_samples_per_channel_and_frame
                                        
                                        tot_samples_sup_frame=u_tot_samples_per_channel_and_frame
                                        u_samples=np.hstack((u_samples,samples))
                                        samples=np.copy(u_samples)
                                        
                                        
                                        
                                    else:
                                        # continue storing
                                        u_tot_samples_per_channel_and_frame+=tot_samples_per_channel_and_frame
                                        
                                        u_samples=np.hstack((u_samples,samples))
                                        
                                        do_sup_frame=1
                                        

                            if do_sup_frame:
                                sup_frame_id=(sup_frame_id+1)%NUM_FRAMES_PER_LINE
                    
                    if not(do_sup_frame):
                        tot_samples_sup_frame=tot_samples_per_channel_and_frame
                    
                    
                    # sup_frame_id has been increased, thus check for 0, not last 
                    if not(do_sup_frame==0 or (do_sup_frame and sup_frame_id==0)):
                        # Superframe, will not print until last frame in superframe
                        if DEBUG_ALIGN:
                            if print_el:
                                print_sup_frame(sup_frame_id)
                    
                    else:
                        # Normal processing
                        tot_samples=tot_samples_per_channel_and_frame
                        # TO DO: Delete?
                        if tot_samples<0:
                            tot_samples=0
                        
                        if not(do_sup_frame):
                            tot_samples_sup_frame=tot_samples
                        
                        ##################################
                        #   Offsets for accessing samples
                        ##################################
                        
                        # Get pointers to the samples
                        [tot_samples_v,seconds_v,offset_first_sample_iterator_v,\
                             offset_first_sample_signal_v,chunk_size_v,acc_v] = \
                                    get_pointers_samples(tot_samples,adjusted_shift_inside_frame,\
                                                    accu_block,rel_pos_frame,tot_samples_sup_frame,ref_offset)
                                                               
    
                        if DEBUG_ALIGN:
                            print_debug_m_align_last(tot_samples_v,seconds_v,offset_first_sample_iterator_v,offset_first_sample_signal_v,\
                                              chunk_size_v,acc_v,do_sup_frame,sup_frame_id)
                
                    
                    
                    
                        # Iterate on groups (or chunks) of samples for this frame, always one group with the data for this
                        #    initially devised to consider the first group too (corresponding to the previous acc period).
                        for (tot_samples,seconds_fr,offset_first_sample_iterator,\
                              offset_first_sample_signal,chunk_size,accu_block) in zip(tot_samples_v,seconds_v,\
                                                                offset_first_sample_iterator_v,offset_first_sample_signal_v,chunk_size_v,acc_v):
                        
                            
                            # Check if samples will be processed (i.e. if inside of requested time window)
                            [process_frame,after_end_time] = check_time_frame(accu_block,rel_pos_frame,actual_frame_time,seconds_ref,seconds_duration)
                            
                            # Stop reading if already passed end time
                            # TO DO: make this configurable
                            if after_end_time>0:
                                keep_reading=0
                                # (!) This must be done to avoid "cat: write error: Broken pipe"
                                # All the data from stdin must be read
                                try:
                                    read_rest=1
                                    rest_reading = reader.read()
                                except EOFError:
                                    error_reading_rest=1
                                    #print("zM"+KEY_SEP+"EOF reading rest of file "+current_file_name)
                            
                            
                            # Otherwise check frame number range
                            elif check_frame_range:
                                if (frame_num<first_frame_num)or(frame_num>=last_frame_num):
                                    process_frame=0
                                    # Keep reading, but do not process this frame
                            
                            
                            if process_frame==1:
                            
                                tot_chunks=1
                                check_tot_chunks=-1
                                
                                #Generate one new line for each chunk
                                # Default case: one chunk, to avoid overhead
                                # TO DO: simplify code, tot_chunks=1
                                for i in range(tot_chunks):
                                    #Calculate iterators for limits of the chunk (indices in array "samples")
                                    iterator_first_sample_base=i*chunk_size
                                    
                                    # Pointers to data (to be used when fetching samples from array)
                                    iterator_first_sample=iterator_first_sample_base+offset_first_sample_iterator
                                    iterator_last_sample=iterator_first_sample+chunk_size
                            
                                    # Sample number (to be used later at the reducer)
                                    first_sample_signal=iterator_first_sample_base+offset_first_sample_signal
                            
                                    re_signal_chunk_quantized=samples[:,iterator_first_sample:iterator_last_sample]
                                
                                    #Loop here for multiple channels (if single channel, check number of thread for channel id)
                                    mod_channel_index = -1
                                    for signal_chunk_quantized in re_signal_chunk_quantized:
                                
                                        ###########################
                                        #   Samples for one band
                                        ###########################
                                
                                        # TODO: this assumes that only threads or bands are used! Need to add option.
                                        #if thread_id>0:
                                        if num_channels==1:
                                            mod_channel_index = thread_id
                                        else:
                                            mod_channel_index += 1
                                        
                                        # metadata for this band
                                        mod_polarization_id =  mod_polarization_ids[mod_channel_index]
                                        mod_channel =          mod_channels[mod_channel_index]
                                        n_bins_pcal_val =      n_bins_pcal[mod_channel_index]
                                        freq_channel =         freqs_assoc_vector[mod_channel_index]
                                        sideband =             sidebands_assoc_vector[mod_channel_index]
                                
                                        # By default fft in reducer
                                        # Consider removing and doing DFT always in reducer
                                        if FFT_HERE:
                                            # No bypass:
                                            signal_chunk_quantized_int = map(int,signal_chunk_quantized)
                                            signal_chunk = simple_dequantizer(signal_chunk_quantized_int,bits_per_sample)
                                            # Complex if applicable
                                            if data_type==1:
                                                signal_chunk=group_pairs_complex(signal_chunk)
                                                
                                            # TO DO: Changes untested
                                            # TO DO: windowing
                                            signal_chunk_fft=map(str,scipy.fftpack.fft(signal_chunk))
                                        else:
                                            # Bypass:
                                            signal_chunk_fft=signal_chunk_quantized
                                        
                                        ###########################
                                        #   Encode samples
                                        ###########################
                                        
                                        # VQ: Compression
                                        # No need to specify station, already in info (id)
                                        if apply_compression==1:
                                            [encoded,codeb]=encode_vq(input_signal=signal_chunk_fft.to01(),chunk_size=0,num_codes=0,whitten=0,code_book=codebook,process_fraction=1)
                                            
                                            # Changes untested!
                                            signal_chunk_fft=[chunk_size]+map(str,list(encoded))
                                
                                        #For each pair where the station belongs, create a line in stdout 
                                        num_samples_in_chunk=len(signal_chunk_fft)
                                        signal_chunk_fft_out = pack_and_encode_samples(signal_chunk_fft,USE_BITARRAYS,ENCODE_B64,apply_compression,bits_per_sample)
                                        
                                        
                                        ###########################
                                        #   Generate output
                                        ###########################
                                        
                                        # TO DO: unify all modes
                                        if (ONE_BASELINE_PER_TASK==0):
                                            if TASK_SCALING_STATIONS==0:
                                                
                                                # All-baselines-per-task
                                                pair=get_pair_all_baselines_per_task()
                                                
                                                # Char to identify mode
                                                char_p="x"
                                                pair_str = get_pair_str(char_p,pair,accu_block,mod_channel,num_channels_spec,\
                                                                        accu_block,first_sample_signal,\
                                                                        station_id,mod_polarization_id,\
                                                                        freq_sample_in,bits_per_sample,data_type_chars[data_type],\
                                                                        encoding,n_bins_pcal_val,pcal_freq,\
                                                                        ONE_BASELINE_PER_TASK,TASK_SCALING_STATIONS,\
                                                                        0,0,0,\
                                                                        num_samples_in_chunk,abs_delay,rate_delay,freq_channel,\
                                                                        fractional_sample_delay,accumulation_time,shift_int,sideband)
                                                str_print = pair_str+signal_chunk_fft_out
                                                if SILENT_OUTPUT==0:
                                                    print(str_print)
                                                else:
                                                    count_print+=1
                                            else: 
                                                
                                                # Linear scaling stations
                                                
                                                #tot_tasks=tot_pols*tot_stations
                                                index_task_col=station_id*tot_pols+mod_polarization_id
                                                for s0 in range(0,tot_stations):
                                                    for t0 in range(0,tot_pols):
                                                        index_task_row=s0*tot_pols+t0
                                                        if (tasks_pairs[index_task_row][index_task_col]==1):
                                                            pair=get_pair_linear_scaling(s0,t0)
                                                            # Char to identify mode
                                                            char_p="r"
                                                            pair_str = get_pair_str(char_p,pair,accu_block,mod_channel,\
                                                                         num_channels_spec,accu_block,first_sample_signal,\
                                                                         station_id,mod_polarization_id,\
                                                                         freq_sample_in,bits_per_sample,data_type_chars[data_type],\
                                                                         encoding,n_bins_pcal_val,pcal_freq,\
                                                                         ONE_BASELINE_PER_TASK,TASK_SCALING_STATIONS,\
                                                                         index_task_row,tot_stations*tot_pols,tot_accu_blocks,\
                                                                         num_samples_in_chunk,abs_delay,rate_delay,freq_channel,\
                                                                         fractional_sample_delay,accumulation_time,shift_int,sideband)
                                                            str_print = pair_str+signal_chunk_fft_out
                                                            print(str_print)
                                            
                                        else:
                                        
                                            # One-baseline-per-task
                                            
                                            id_pair = -1
                                            for pair in pairs:
                                                id_pair+=1
                                        
                                                # If station-thread in pair:
                                                #TO DO: move fft and heavy computations here (after if) to avoid unnecessary processing...
                                                if (station_id==pair[0] and mod_polarization_id==pair[1]) or (station_id==pair[2] and mod_polarization_id==pair[3]):
                    
                                                    # keys
                                                    char_p="y"
                                                    pair_str = get_pair_str(char_p,pair,accu_block,mod_channel,num_channels_spec,\
                                                                        accu_block,first_sample_signal,\
                                                                        station_id,mod_polarization_id,\
                                                                        freq_sample_in,bits_per_sample,data_type_chars[data_type],\
                                                                        encoding,n_bins_pcal_val,pcal_freq,\
                                                                        ONE_BASELINE_PER_TASK,TASK_SCALING_STATIONS,\
                                                                        id_pair,tot_pairs,tot_accu_blocks,\
                                                                        num_samples_in_chunk,abs_delay,rate_delay,freq_channel,\
                                                                        fractional_sample_delay,accumulation_time,shift_int,sideband)
                                                    str_print = pair_str+signal_chunk_fft_out
                                                    print(str_print)

            else:
                error_frame = C_M_READ_ERR_HEADER_NONE
                keep_reading = 0
                #print("zM"+KEY_SEP+"Invalid header or data for file "+current_file_name)
                if read_rest==0:
                    try:
                        rest_reading = reader.read()
                    except EOFError:
                        error_reading_rest=1
                        #print("zM"+KEY_SEP+"EOF reading rest of file "+current_file_name)
                
        

    else:
        # Read input (to avoid errors) and exit
        try:
            read_rest=1
            rest_reading = reader.read()
        except EOFError:
            error_reading_rest=1
            #print("zM"+KEY_SEP+"EOF reading rest of file "+current_file_name)

            
                        
if __name__ == '__main__':
    main()

# <codecell>


