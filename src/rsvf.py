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
#File: rsvf.py.
#Author: A.J. Vazquez Alvarez (ajvazquez@haystack.mit.edu)
#Description: 
"""
Reducer: performs VLBI correlation from a text file with the lines for all the stations, 
      sorted as defined in const_mapred.py based on the key and format defined in msvf.get_pair_str().

Parameters
----------
 See lib_mapredcorr.get_reducer_params_str()
    
Returns
-------
 See rsvf.get_lines_out_for_all() for visibilities.
 See rsvf.get_lines_stats() for statistics.

Notes
-----
|
| **Reader:
|
|  Expecting lines with 
|  See rsvf.split_input_line()
     
"""   
#History:
#initial version: 2015.12 ajva
#MIT Haystack Observatory

from __future__ import print_function,division
import sys
import base64
import imp

import lib_quant
imp.reload(lib_quant)
from lib_quant import *



import lib_fx_stack
imp.reload(lib_fx_stack)
from lib_fx_stack import *

# Constants for mapper and reducer
import const_mapred
imp.reload(const_mapred)
from const_mapred import *

import lib_pcal
imp.reload(lib_pcal)
from lib_pcal import *

# Vector quantization                           # VQ disabled
#import lib_vq
#imp.reload(lib_vq)
#from lib_vq import *

import lib_debug
imp.reload(lib_debug)
from lib_debug import *

import const_performance
imp.reload(const_performance)
from const_performance import *

from const_ini_files import *

#import bitarray
import numpy as np    



# -Debugging-
#    By default: 0
#    If 1: inputs are by-passed to the output
DEBUGGING = BYPASS_REDUCER # 0


###########################################
#           Input
###########################################


def split_input_line(line):
    """
    Get sub-keys from read line.
    
    Parameters
    ----------
     line : str
         whole line read from input. 
    
    Returns
    -------
     key_pair_accu : str
         part of the key with pair (station-pol A and station-pol B) and accumulation period.
     key_sample : str
         part of they key with sample number.
     key_station
         station identifier
     vector_split
         second part of the header with information necessary for the processing of the samples.
     is_autocorr
         used in one-baseline-per-task mode, indicates that this pair is an autocorrelation and therefore
                      these samples will be correlated with themselves.
     key_station_pol
         station-polarization identifier.
     char_type
         identifies the mode of operation, as defined in the mapper:
            |      'x' for all-baselines-per-task,
            |      'r' for linear scaling with the number of stations,
            |      'y' for one-baseline-per-task.
     accu_block
         integration period number.
    
    
    Notes
    -----
    |
    | **TO DO:**
    |
    |  Move char_type (char_p in mapper) to constants section.
    """
    # Multi-key separator into common format (Hadoop will change last FIELD_SEP of key into KEY_SEP
    line=line.replace(FIELD_SEP+KEY_SEP,KEY_SEP)
    line=line.replace(KEY_SEP+KEY_SEP,KEY_SEP)
    
    key, vector = line.split(KEY_SEP,1)
    key_pair_accu_sample, key_station_pol = key.split('s',1)
    key_station = key_station_pol.split(SF_SEP)[0]
    key_pair_accu, key_sample = key_pair_accu_sample.split('f',1)
    vector_split = vector.split(' ')
    key_pair_accu_split = key_pair_accu.split(FIELD_SEP)
    is_autocorr=0
    if key_pair_accu_split[1]==key_pair_accu_split[2]:
        is_autocorr=1
    char_type=key[1]
    accu_block=float(key_pair_accu_split[5])
    
    return([key_pair_accu, key_sample, key_station, vector_split,is_autocorr,key_station_pol,char_type,accu_block])



def extract_params_split(vector_split):
    """
    Get paramters from line header (header of the data, not part of the key).
    
    Parameters
    ----------
     vector_split : str
         second part of the header with all the parameters provided by the mapper, 
                      associated to the samples to be processed.
                      
    Returns
    -------
    bits_per_sample
        number of bits for each component of the sample.
    block_first_sample
        accumulation period. TO DO: check this.
    data_type
        sample type, 'r' for real, 'c' for complex.
    encoding
        [unused] initially used for introducing compression in the data (VQ), currently not used.
    encoding_width
        [unused] also associated to compression.
    n_bins_pcal
        number of samples for the windows to be accumulated for the pcal signal.
    num_samples
        number of samples in this line.
    abs_delay
        absolute delay.
    rate_delay
        delay information corresponding these samples (polynomials, etc).
    fs
        sampling frequency.
    fs_pcal
        [unused] phase calibration signal frequency spacing.
    freq_channel
        sky frequency.
    first_sample
        first sample number (integer starting at 0).
    fractional_sample_delay
        fractional sample delay corresponding to the sample 0 of this stream.
    accumulation_time
        time duration of the integration period.
    shift_delay
        integer number of samples offset for the sample 0 of this stream.
    sideband
        single side band side, 'l' for LSB, 'u' for USB.
    
    Notes
    -----
    |
    | **Conventions:**
    |
    |  See const_mapred.py for constants positions and descriptions.  
    """
    
    shift_delay=               int(vector_split[INDEX_SHIFT_DELAY])
    fractional_sample_delay= float(vector_split[INDEX_FRAC_DELAY])
    abs_delay=               float(vector_split[INDEX_ABS_DELAY])
    rate_delay=             [float(vector_split[INDEX_RATE_DELAY_0]),\
                             float(vector_split[INDEX_RATE_DELAY_1]),\
                             float(vector_split[INDEX_RATE_DELAY_2]),\
                             float(vector_split[INDEX_RATE_DELAY_REF]),\
                             float(vector_split[INDEX_RATE_CLOCK_0]),\
                             float(vector_split[INDEX_RATE_CLOCK_1]),\
                             float(vector_split[INDEX_RATE_ZC_0]),\
                             float(vector_split[INDEX_RATE_ZC_1]),\
                             float(vector_split[INDEX_RATE_CLOCK_REF]),\
                             float(vector_split[INDEX_RATE_M_ONLY]),\
                             float(vector_split[INDEX_RATE_C_ONLY]),
                             float(vector_split[INDEX_RATE_DIFF_FRAC])] 
    num_samples=               int(vector_split[INDEX_NUM_SAMPLES])
    fs=                      float(vector_split[INDEX_FS])
    bits_per_sample =          int(vector_split[INDEX_BITS_PER_SAMPLE])
    first_sample=              int(vector_split[INDEX_FIRST_SAMPLE])
    data_type =                    vector_split[INDEX_DATA_TYPE]
    n_bins_pcal =              int(vector_split[INDEX_NBINS_PCAL])
    fs_pcal=                 float(vector_split[INDEX_PCAL_FREQ])
    channel_index_str =            vector_split[INDEX_CHANNEL_INDEX]
    freq_channel =           float(vector_split[INDEX_CHANNEL_FREQ])
    accumulation_time =      float(vector_split[INDEX_ACC_TIME])
    encoding =                     vector_split[INDEX_ENCODING]
    sideband=                      vector_split[INDEX_SIDEBAND]
    
    
    block_first_sample = vector_split[INDEX_FIRST_SAMPLE]+SF_SEP+channel_index_str
    

    if data_type=='c':
        num_samples=num_samples//2
        shift_delay=int(shift_delay//2)
    
    if encoding == C_INI_MEDIA_C_VQ:
        encoding_width = int(vector_split[META_LEN])
    else:
        encoding_width=0
    return([bits_per_sample,block_first_sample,data_type,encoding, encoding_width,n_bins_pcal,num_samples,abs_delay,rate_delay,fs,fs_pcal,freq_channel,first_sample,fractional_sample_delay,accumulation_time,shift_delay,sideband])



def decode_samples_b64(vector_split_samples,vector_split_encoding):
    """
    Decode base64.
    
    Parameters
    ----------
     vector_split_samples
         string with the samples (that is a component of the list vector_split).
     vector_split_encoding
         compression (VQ) encoding, disabled by default.
    
    Returns
    -------
     out: 1D numpy array
         samples (components if complex), packed in binary format (uint8). Samples still need to be "dequantized".
    """
    
    if (ENCODE_B64==1)and(vector_split_encoding==C_INI_MEDIA_C_NO):
        return(np.fromstring(base64.b64decode(vector_split_samples),dtype=np.uint8))
    else:
        return([])



###########################################
#           Output
###########################################

def get_key_all_out(char_type,F_ind_s0,F_ind_s1,acc_str):
    """
    Get key for reducer output.
    
    Parameters
    ------
     char_type : char
         operation mode (see split_input_line())
     F_ind_s0
         first station-polarization for this baseline.
     F_ind_s1
         second station-polarization for this baseline.
     acc_str : str
         multi-key for output line.
     
    Returns
    -------
     output : str
         key for output line.
    """
    return("p"+char_type+FIELD_SEP+F_ind_s0+FIELD_SEP+F_ind_s1+FIELD_SEP+acc_str+FIELD_SEP)

def get_str_r_out(current_key_pair_accu,count_acc,current_vector_split,current_block_first_sample,accu_prod_div):
    """
    Get output string for reducer.
    
    Parameters
    ----------
     current_key_pair_accu
         part of the key with the baseline and the accumulation multi-key.
     count_acc
         number of accumulations.
     current_vector_split
         list with metadata.
     current_block_first_sample
         <first_sample>.<channel_index>
     accu_prod_div : complex 1D np.array
         visibilities for one baseline, one band and one accumulation period.
     
    Returns
    -------
     str_print : str
         output line with visibilities.
    """
    current_vector_split_sub_print = current_vector_split[:(META_LEN-1)]
    current_vector_split_sub_print[INDEX_PCAL_FREQ] = str(0)
    current_vector_split_sub_print[INDEX_NBINS_PCAL] = str(0)                    
    str_print = current_key_pair_accu+'sxa'+str(count_acc)+KEY_SEP+' '.join(current_vector_split_sub_print)+\
          ' '+current_block_first_sample+' '+' '.join(map(str, accu_prod_div))
    return(str_print)
                        

def get_str_pcal_out(acc_pcal,current_n_bins_pcal,count_acc_pcal,current_key_pair_accu,current_vector_split,current_block_first_sample):
    """
    [Only used in one-baseline-per-task mode]
    Get output string for phase calibration.
    
    Parameters
    ----------
     acc_pcal : complex 1D np.array
         phase calibration results for one baseline, one band and one accumulation period.
     current_n_bins_pcal
         number of bins (number of elements in acc_pcal).
     count_acc_pcal
         number of accumulations performed to get pcal results.
     current_key_pair_accu
         part of the key with the baseline and the accumulation multi-key.
     current_vector_split
         metadata as in the input line.
     current_block_first_sample
         <first_sample>.<channel_index>.
    
    Returns
    -------
     str_print : str
         output line with phase calibration results.
    """
    str_print = "pcal"+current_key_pair_accu[2:]+'sxa'+str(count_acc_pcal)+KEY_SEP+' '.join(current_vector_split[:(META_LEN-1)])+' '+current_block_first_sample+' '+' '.join(map(str, acc_pcal))
    return(str_print)                      

def get_str_pcal_out_all(sp,acc_pcal,current_n_bins_pcal,count_acc_pcal,current_key_pair_accu,current_vector_split,current_block_first_sample):
    """
    Get output string for phase calibration (all-baselines-per-task).
    
    Parameters
    ------
     sp
         station-polarization
     acc_pcal : complex 1D np.array
         phase calibration results for one baseline, one band and one accumulation period.
     current_n_bins_pcal
         number of bins (number of elements in acc_pcal).
     count_acc_pcal
         number of accumulations performed to get pcal results.
     current_key_pair_accu
         part of the key with the baseline and the accumulation multi-key.
     current_vector_split
         metadata as in the input line.
     current_block_first_sample
         <first_sample>.<channel_index>.
     
    Returns
    -------
     str_print : str
         output line with phase calibration results.
    """
    str_print = "pcal"+FIELD_SEP+sp+FIELD_SEP+sp+FIELD_SEP+current_key_pair_accu+FIELD_SEP+'sxa'+str(count_acc_pcal)+KEY_SEP+' '.join(current_vector_split[:(META_LEN-1)])+' '+current_block_first_sample+' '+' '.join(map(str, acc_pcal))
    return(str_print)                      


def get_lines_out_for_all(char_type,n_sp,F_ind,current_acc_str,count_acc,acc_mat,current_block_first_sample,current_vector_split,\
                          acc_pcal,count_acc_pcal,scaling_pair="A.A"):
    """
    Get output lines for all results in accumulation matrix.
    
    Parameters
    ----------
     char_type
         operation mode (see split_input_line()).
     n_sp
         number of station-polarizations.
     F_ind
         structure with ids for station-polarizations.
     current_acc_str
         multi-key
     count_acc
         number of accumulations for the visibilities.
     acc_mat : complex 3D array
         visibilities for all baselines for this acc period and band. See lib_fx_stack.compute_x_all() for more info.
     current_block_first_sample
         <first_sample>.<channel_index>.
     current_vector_split
         metadata as in the input line.
     acc_pcal : complex 2D array
         phase calibration results for all stations for this acc period and band. See lib_pcal.accumulate_pcal_all() for more info.
     count_acc_pcal
         number of accumulations for the phase calibration results.
     scaling_pair
         station-polarization for this task (used in linear-scaling, "A.A" by default (all-baseslines-per-task).
    
    Returns
    -------
     lines_out
         list of lines with output results (visibilities and phase calibration).
    """
    
    # TO DO: need more elegant solution to get key, currently hardcoded.
    current_acc_str=SF_SEP.join(current_acc_str[3:7])
    lines_out=[]
    if acc_mat is not None:
        if scaling_pair=="A.A":
            for s0 in range(n_sp):
                for s1 in range(s0,n_sp):
                    try:
                        new_key_pair_accu=get_key_all_out(char_type,F_ind[s0],F_ind[s1],current_acc_str)
                        str_print = get_str_r_out(new_key_pair_accu,count_acc,current_vector_split,\
                                                  current_block_first_sample,acc_mat[s0,s1])
                    except TypeError:
                        str_print = "zR\tError getting output data for "+str(s0)+"/"+str(s1)+" in "+str(current_acc_str)
                    lines_out+=[str_print]
        else:
            s0 = F_ind.index(scaling_pair)
            for s1 in range(n_sp):
                new_key_pair_accu=get_key_all_out(char_type,F_ind[s0],F_ind[s1],current_acc_str)
                str_print = get_str_r_out(new_key_pair_accu,count_acc,current_vector_split,\
                                              current_block_first_sample,acc_mat[s1])
                lines_out+=[str_print]
        
        if acc_pcal!=[]:
            current_n_bins_pcal=acc_pcal.shape[1]
            pcal_fft = window_and_fft(acc_pcal,current_n_bins_pcal,C_INI_CR_WINDOW_SQUARE,flatten_chunks=0)
            
            # TO DO: check
            acc_pcal_div = normalize_pcal(pcal_fft,count_acc_pcal)
            
            for sp in range(n_sp):
                str_print = get_str_pcal_out_all(F_ind[sp],acc_pcal_div[sp][0],current_n_bins_pcal,count_acc_pcal,current_acc_str,current_vector_split,current_block_first_sample)
                lines_out+=[str_print]
    else:
        str_print = "zR\tEmpty acc mat in "+str(current_acc_str)
        lines_out+=[str_print]
    
    return(lines_out)

    
    
###########################################
#       Data structures storage/mgmt
###########################################


def update_stored_samples(v_dequant,F1,F_ind,key_station_pol,F_delays,F_rates,F_fs,F_fs_pcal,abs_delay,rate_delay,\
                          fs,fs_pcal,F_first_sample,first_sample,data_type,F_frac,fractional_sample_delay,\
                          shift_delay,F_side,sideband,fft_size_in):
    """
    Store samples and metadata, to be processed later.
    
    Parameters
    ----------
     *For data structures see output below.
     *For metadata parameters see extract_params_split().
     v_dequant :numpy 1D array of complex
         dequantized samples.
    
    Returns
    -------
     F_*: lists where each element correspond to one read line. All these lists are related, i.e. the n-th element
                      of all lists correspond to the same read line.
    
    Notes
    -----
    |
    | **TO DO:**
    |
    |  Add checks.
    """
    
    # TO DO: move to extract_params
    if data_type=='c':
        first_sample_adjusted=int(first_sample//2)
        #TO DO: need more elegant solution to support real and complex...
        fft_size_out=fft_size_in
    else:
        first_sample_adjusted=first_sample
        #TO DO: need more elegant solution to support real and complex...
        fft_size_out=2*fft_size_in
    
    
    if F1 is None:
        F1=[v_dequant]
        F_ind=[key_station_pol]
        F_delays=[abs_delay]
        F_rates=[rate_delay]
        F_frac=[[fractional_sample_delay,shift_delay]]
        F_fs=[fs]
        F_fs_pcal=[fs_pcal]
        F_first_sample=[first_sample_adjusted]
        F_side=[[sideband,data_type]]
    else:
        F1+=[v_dequant]
        F_ind+=[key_station_pol]
        F_delays+=[abs_delay]
        F_rates+=[rate_delay]
        F_frac+=[[fractional_sample_delay,shift_delay]]
        F_fs+=[fs]
        F_fs_pcal+=[fs_pcal]
        F_first_sample+=[first_sample_adjusted]
        F_side+=[[sideband,data_type]]

    return([F1,F_ind,F_delays,F_rates,F_fs,F_fs_pcal,F_first_sample,F_frac,F_side,fft_size_out])


def restore_Fs(last_F_delays,last_F_rates,last_F_frac,last_F_fs,last_F_fs_pcal,last_F_side,last_F_first_sample,\
               F_delays,F_rates,F_frac,F_fs,F_fs_pcal,F_side,F_first_sample):
    """
    Keep previous structures in case there is no data for all stationpols.
    """
    
    #if len(last_F_delays)>len(F_delays):
    if 1:
        return([last_F_delays,last_F_rates,last_F_frac,last_F_fs,last_F_fs_pcal,last_F_side,last_F_first_sample])
    else:
        return([F_delays,F_rates,F_frac,F_fs,F_fs_pcal,F_side,F_first_sample])



###########################################
#          Data structures display
###########################################


def get_shapes_F1(F1):
    """
    Get string showing shapes of F1.
    
    Parameters
    ----------
     F1:    list of multidimensional np.arrays
         (each elment has the samples for each station-poliarization.
     
    Returns
    -------
     out : str
    """
    return(str([(F1[i].shape) for i in range(len(F1))]))


def str_list(F_list,sep_c=','):
    """
    Get string with representation of list.
    """
    return("["+sep_c.join(map(str,F_list))+"]")



def get_lines_stats(current_key_pair_accu,F_stack_shift,F_adj_shift_partial,F_lti,F_ind,failed_acc_count,\
                                            current_block_first_sample,dismissed_acc_count):
    """
    Get list of lines with stats for this accumulation period including:
    -Number of dropped/added samples (for fractional sample overflows) (stack)
    -Number of fractional sample overflows (shift)
    -For each stationpol: last sample, total samples, missing/invalid samples (lti)
    -Number of failed accumulations (will be one if some data is uncorrelated, which may be 
       simply due to missalignment from delays.
       
    Parameters
    ------
     current_key_pair_accu
         part of the key with pair (station-pol A and station-pol B) and accumulation period.
     F_stack_shift
         [unused?] see lib_fx_stack().
     F_adj_shift_partial
         [unused?] see lib_fx_stack().
     F_lti
         list with last sample (l), total number of samples processed (t), invalid samples (i), and 
             adjuted samples for each stream.
     F_ind
         list with station-polarizations.
     failed_acc_count
         number of failed accumulations.
     current_block_first_sample
         <first_sample>.<channel_index>
     dismissed_acc_count
         number of dismissed accumulations.
     
    Returns
    -------
     lines_stats : list of str
         lines with stats.
     
    Notes
    -----
    |
    | **TO DO:**
    |
    |  Remove unused.
    """
    
    lines_stats=[]
    lines_stats+=["zR"+KEY_SEP+"kpa="+current_key_pair_accu+",Adjusted stack="+str_list(F_stack_shift)]
    lines_stats+=["zR"+KEY_SEP+"kpa="+current_key_pair_accu+",Adjusted shifts="+str_list(F_adj_shift_partial)]
    for (i_lti,i_ind) in zip(F_lti,F_ind):
        lines_stats+=["zR"+KEY_SEP+"st="+str(i_ind)+",lti_stats="+str_list(i_lti)]
        
    if (failed_acc_count>0)or(dismissed_acc_count>0):
        lines_stats+=["zR"+KEY_SEP+"Failed accs="+str(failed_acc_count)+",dismissed accs="+str(dismissed_acc_count)+",in=a"+current_block_first_sample]
    return(lines_stats)



###########################################
#                   Main
###########################################

def main():
    
    no_data=1
    
    # Moved into lib_fx_stack
    #if use_fftw:
    #    pyfftw.interfaces.cache.enable()
    #    pyfftw.interfaces.cache.set_keepalive_time(20)
    
    
    # Read parameters                       # See lib_mapredcorr.get_reducer_params_str() for interface documentation.
    codecs_serial=        sys.argv[1]
    FFT_HERE=         int(sys.argv[2])
    INTERNAL_LOG=     int(sys.argv[3])
    FFT_SIZE_IN=      int(sys.argv[4])         
    WINDOWING=            sys.argv[5]
    PHASE_CALIBRATION=int(sys.argv[6])
    SINGLE_PRECISION= int(sys.argv[7])
    
    # FFT size
    FFT_SIZE=FFT_SIZE_IN                    # For real data will use 2x fft_size, assuming all data is real xor complex
    #                                         See update_stored_samples where this is overriden
    
    # Precision (Approximation) configurable
    # TO DO: currently always double precision, change.
    DTYPE_COMPLEX=np.complex64 if SINGLE_PRECISION else np.complex128


    last_F_delays=[]
    last_F_rates=[]
    last_F_frac=[]
    last_F_fs=[]
    last_F_fs_pcal=[]
    last_F_side=[]
    last_F_first_sample=[]


    F1=None
    F_ind=[]
    F_delays=[]
    F_rates=[]
    F_fs=[]
    F1_partial=np.array([])
    F_lti=[]
    F_ind_partial=[]
    F_first_sample=[]
    F_first_sample_partial=[]
    F_frac=[]
    F_adj_shift_partial=[]
    F_adj_shift_pcal=[]
    F_stack_shift=[]
    F_stack_shift_pcal=[]
    F_pcal_fix=[]
    F_fs_pcal=[]
    F_side=[]
    F_first_sample=[]
    acc_mat=None
    last_F_ind=None
    n_sp=0
    failed_acc_count=0
    dismissed_acc_count=0
    
    # Debugging headers
    if DEBUG_DELAYS:
        print_debug_r_delays_header()
    if DEBUG_HSTACK:
        print_debug_r_hstack_header()
    if DEBUG_FRAC_OVER:
        print_debug_r_frac_over_header()
   

    
    # Default: use stdin
    f_in=sys.stdin
    
    counter_sub_acc=0

    #Variables for storing key, and metadata plus samples
    current_key_pair_accu = None
    current_key_sample = None
    current_vector_split = []
    current_block_first_sample = None
    current_bits_per_sample = None
    current_data_type = None
    current_encoding = None
    current_block_time = None
    current_freq_channel = None
    count_acc=0
    count_acc_pcal=0
    
    # Define constants for these
    scaling_pair="A.A"
    current_scaling_pair="A.A"
    
    # Saved samples for next iteration
    pre_pcal = np.array([])
    # Accumulated pcal results
    acc_pcal = np.array([])
    
    
    # List of codebooks to avoid decoding every time
    codebook_list = []
    
           
    #For debugging, just copy lines to output file
    if DEBUGGING:
        
        #####################
        #   Debugging mode
        #####################
    
        # Just bypass lines into output and exit
        for line in f_in:
            line = line.strip()  
            print("zR-Debug mode"+KEY_SEP+line)
    
    
    else:
        
        #########################
        #   Process input lines
        #########################
        
       
        for line in f_in:
            
            
            #########################
            #   Bypass logging
            #########################
        
            # Process line
            # Checks for bypassing previous log/error lines
            if line[0]=='z':
                # Error Message, just copy to output and keep processing
                print(line.strip())
                continue
            elif line[0]!='p':
                # Ignore line
                print("zRz"+KEY_SEP+"Ignored line:"+line.strip())
                continue
            

            #########################
            #      Decode line
            #########################

            # Decode line
            line = line.strip()
            try:
                [key_pair_accu, key_sample, key_station, vector_split,is_autocorr,key_station_pol,char_type,accu_block] = split_input_line(line)
                samples_quant = decode_samples_b64(vector_split[-1],vector_split[INDEX_ENCODING])
    
                
            except ValueError:
                # Error in key
                print("zRz"+KEY_SEP+"Value error (kv):"+line.strip())
                continue
            except TypeError:
                # Error in base64 decoding
                print("zRz"+KEY_SEP+"Type error (b64):"+line.strip())
                continue
            
           
            no_data=0
            

            #########################
            #      Get metadata
            #########################

            # Extract parameters from key
            [bits_per_sample,block_first_sample,data_type,encoding,\
                 encoding_width,n_bins_pcal,num_samples,abs_delay,\
                 rate_delay,fs,fs_pcal,freq_channel,first_sample,\
                 fractional_sample_delay,accumulation_time,shift_delay,sideband] = extract_params_split(vector_split[:-1])
            block_time=accu_block*accumulation_time
            # Get pair associated to this line
            
            pairs=key_pair_accu.split(FIELD_SEP)
            if current_key_pair_accu!=None:
                current_pairs=current_key_pair_accu.split(FIELD_SEP)
            
            # Check mode for this line
            if pairs[2]=="A.A": 
                
                
                # Multiple baselines in the same task
                if pairs[1]=="A.A":
                    # All-baselines-per-task
                    TASK_SCALING_STATIONS=0
                else:
                    # Linear scaling stations
                    TASK_SCALING_STATIONS=1
                scaling_pair=pairs[1]
                

                if key_pair_accu!=current_key_pair_accu:
                    
                    ######################################
                    #   New accumulation period and band
                    ######################################
                    #
                    # Thus no more samples for current processing, process stored data, then store new samples
                    
                    if current_key_pair_accu!=None:

                        ######################################
                        #   This is not the first iteration
                        ######################################
                        #
                        # (This should be equivalent to "Last write" below).
                        # Write accumulated products to file (if it's not the first block, i.e., there's no data yet)
                        
                        [F_delays,F_rates,F_frac,F_fs,F_fs_pcal,F_side,F_first_sample]=restore_Fs(last_F_delays,last_F_rates,last_F_frac,last_F_fs,last_F_fs_pcal,last_F_side,last_F_first_sample,F_delays,F_rates,F_frac,F_fs,F_fs_pcal,F_side,F_first_sample)

                        ########
                        #  FX
                        ########
                        normalize_after_compute=True
                        [acc_mat,count_acc,count_sub_acc,n_sp,last_F_ind,\
                         failed_acc_count,dismissed_acc_count,F1_partial,F_ind_partial,\
                         acc_pcal,pre_pcal,count_acc_pcal,F_first_sample_partial,\
                         F_adj_shift_partial,F_stack_shift,F_adj_shift_pcal,F_stack_shift_pcal,F_pcal_fix,F_lti] = compute_fx_for_all(F1_partial,F_ind_partial,F1,\
                                                                                         FFT_SIZE,WINDOWING,acc_mat,count_acc,\
                                                                                         normalize_after_compute,F_ind,\
                                                                                         last_F_ind,n_sp,failed_acc_count,\
                                                                                         dismissed_acc_count,\
                                                                                         current_scaling_pair,DTYPE_COMPLEX,\
                                                                                         acc_pcal,pre_pcal,n_bins_pcal,\
                                                                                         count_acc_pcal,PHASE_CALIBRATION,\
                                                                                         0,F_delays,F_rates,F_fs,current_freq_channel,\
                                                                                         F_first_sample,F_first_sample_partial,\
                                                                                         F_frac,current_block_time,F_adj_shift_partial,\
                                                                                         F_stack_shift,F_adj_shift_pcal,F_stack_shift_pcal,\
                                                                                         F_pcal_fix,F_side,F_lti)

                    
                        #########
                        #  Pcal
                        #########
                        if PHASE_CALIBRATION>0:
                            acc_pcal = adjust_shift_acc_pcal(acc_pcal,F_pcal_fix,v=DEBUG_GENERAL_R)

                        
                        #########
                        #  Out
                        #########
                        lines_out = get_lines_out_for_all(char_type,n_sp,last_F_ind,current_pairs,count_acc,acc_mat,\
                                                          current_block_first_sample,current_vector_split,acc_pcal,\
                                                          count_acc_pcal,current_scaling_pair)
                        if lines_out!=[]:
                            for line_out in lines_out:
                                print(line_out)
                        
                        
                        ##########
                        #  Stats
                        ##########
                        # If stored samples left unprocessed count them as failure
                        if len(F1_partial)>0:
                            failed_acc_count+=1
                        lines_stats = get_lines_stats(current_key_pair_accu,F_stack_shift,\
                                            F_adj_shift_partial,F_lti,last_F_ind,failed_acc_count,\
                                            current_block_first_sample,dismissed_acc_count)
                        if lines_stats!=[]:
                            for line_stats in lines_stats:
                                print(line_stats)
                        
   
                        failed_acc_count=0
                        dismissed_acc_count=0
                        acc_mat=None
                        last_F_ind=None
                    
      
                    if len(last_F_delays)<=len(F_delays):
                        last_F_delays=F_delays[:]
                        last_F_rates=F_rates[:]
                        last_F_frac=F_frac[:]
                        last_F_fs=F_fs[:]
                        last_F_fs_pcal=F_fs_pcal[:]
                        last_F_side=F_side[:]
                        last_F_first_sample=F_first_sample[:]
                    
                    
                    ######################################
                    #       Restart data structures
                    ######################################
                    # TO DO: Missing: print phase calibration results
                    current_scaling_pair = scaling_pair
                    current_key_pair_accu = key_pair_accu
                    current_block_first_sample = block_first_sample
                    current_data_type = data_type
                    current_encoding = encoding
                    current_freq_channel = freq_channel
                    count_acc=0
                    count_acc_pcal = 0
                    current_key_sample = key_sample
                    current_vector_split = vector_split
                    current_block_time = block_time
                    F1=None
                    F_ind=[]
                                       
                    F_delays=[]
                    F_Frac=[]
                    F_rates=[]
                    F_fs=[]
                    F_fs_pcal=[]
                    F_stack_shift=[]
                    F_adj_shift_partial=[]
                    F_adj_shift_pcal=[]
                    F_pcal_fix=[]
                    F_side=[]

                    #last_F_delays=[]
                    #last_F_rates=[]
                    #last_F_frac=[]
                    #last_F_fs=[]
                    #last_F_fs_pcal=[]
                    #last_F_side=[]
                    #last_F_first_sample=[]

                    #Also stored samples since results have been written
                    F1_partial=np.array([])
                    F_lti=[]
                    F_first_sample_partial=[]
                    F_ind_partial=[]
                    #pcal
                    pre_pcal = np.array([])
                    acc_pcal = np.array([])
                    
                    # Read data
                    # TODO: VQ (vector quantization) not supported yet for all baselines in same task        
                    
                    if DEBUG_DELAYS or DEBUG_HSTACK or DEBUG_FRAC_OVER:
                        print_key(key_pair_accu)
                        
                    ######################################
                    #       Update data structures
                    ######################################
                    #
                    # No processing yet, simply store samples
                    v_dequant = get_samples(samples_quant,bits_per_sample,data_type,num_samples,SINGLE_PRECISION)
                    [F1,F_ind,F_delays,F_rates,\
                        F_fs,F_fs_pcal,F_first_sample,\
                        F_frac,F_side,FFT_SIZE]=update_stored_samples(v_dequant,F1,F_ind,key_station_pol,\
                                                            F_delays,F_rates,F_fs,F_fs_pcal,\
                                                            abs_delay,rate_delay,fs,fs_pcal,\
                                                            F_first_sample,first_sample,data_type,\
                                                            F_frac,fractional_sample_delay,shift_delay,\
                                                            F_side,sideband,FFT_SIZE_IN)
                        
                       
                    
                else:
                    
                    ######################################
                    #   Same accumulation period and band
                    ######################################
                    #
                    # There will be one line per station. If the station for this line already has data, append it.
                    
                    if key_station_pol not in F_ind:
                        
                        ######################################
                        #        Update data structures
                        ######################################
                        # TODO: vector quantization not supported yet for all baselines in same task        
                        v_dequant = get_samples(samples_quant,bits_per_sample,data_type,num_samples,SINGLE_PRECISION)    
                        [F1,F_ind,F_delays,F_rates,F_fs,F_fs_pcal,\
                         F_first_sample,F_frac,F_side,FFT_SIZE]=update_stored_samples(v_dequant,F1,F_ind,\
                                                                             key_station_pol,F_delays,F_rates,\
                                                                             F_fs,F_fs_pcal,abs_delay,rate_delay,fs,\
                                                                             fs_pcal,F_first_sample,first_sample,\
                                                                             data_type,F_frac,fractional_sample_delay,\
                                                                             shift_delay,F_side,sideband,FFT_SIZE_IN)
                        current_vector_split = vector_split
                        current_block_time = block_time
 
                    else:
                        
                        ######################################
                        #         Append new samples
                        ######################################
                        # new sub-accumulation period, multiply accumulate data and restart data structures
                        #removed this call
                        #[F_delays,F_rates,F_frac,F_fs,F_fs_pcal,F_side,F_first_sample]=restore_Fs(last_F_delays,last_F_rates,last_F_frac,last_F_fs,last_F_fs_pcal,last_F_side,last_F_first_sample,F_delays,F_rates,F_frac,F_fs,F_fs_pcal,F_side,F_first_sample)
                        # This call includes not(COMPUTE_FOR_SUB_ACC_PERIOD) to bypass fx, so fx is only done before writting output
                        
                        #compute_for_sub_acc_period=not(COMPUTE_FOR_SUB_ACC_PERIOD)
                        if COMPUTE_FOR_SUB_ACC_PERIOD>0:
                            counter_sub_acc+=1
                            compute_for_sub_acc_period_check=counter_sub_acc%COMPUTE_FOR_SUB_ACC_PERIOD
                            compute_for_sub_acc_period=True
                            if compute_for_sub_acc_period_check==0:
                                compute_for_sub_acc_period=False
                        else:
                            compute_for_sub_acc_period=False

                        ######################################
                        #  Process samples in data structures
                        ######################################
                        #
                        # Note that FX computations are bypassed
                        [acc_mat,count_acc,count_sub_acc,n_sp,last_F_ind,\
                         failed_acc_count,dismissed_acc_count,F1_partial,F_ind_partial,\
                         acc_pcal,pre_pcal,count_acc_pcal,F_first_sample_partial,\
                         F_adj_shift_partial,F_stack_shift,F_adj_shift_pcal,F_stack_shift_pcal,F_pcal_fix,F_lti] = compute_fx_for_all(F1_partial,F_ind_partial,F1,\
                                                                                         FFT_SIZE,WINDOWING,acc_mat,count_acc,0,\
                                                                                         F_ind,last_F_ind,n_sp,failed_acc_count,\
                                                                                         dismissed_acc_count,\
                                                                                         current_scaling_pair,DTYPE_COMPLEX,\
                                                                                         acc_pcal,pre_pcal,n_bins_pcal,\
                                                                                         count_acc_pcal,PHASE_CALIBRATION,\
                                                                                         compute_for_sub_acc_period,\
                                                                                         F_delays,F_rates,F_fs,current_freq_channel,\
                                                                                         F_first_sample,F_first_sample_partial,\
                                                                                         F_frac,current_block_time,F_adj_shift_partial,\
                                                                                         F_stack_shift,F_adj_shift_pcal,\
                                                                                         F_stack_shift_pcal,F_pcal_fix,F_side,F_lti)


                        
                        if len(last_F_delays)<=len(F_delays):
                            last_F_delays=F_delays[:]
                            last_F_rates=F_rates[:]
                            last_F_frac=F_frac[:]
                            last_F_fs=F_fs[:]
                            last_F_fs_pcal=F_fs_pcal[:]
                            last_F_side=F_side[:]
                            last_F_first_sample=F_first_sample[:]

                        
                        # Reinitialize variables
                        F_ind=[]
                        F_delays=[]
                        F_rates=[]
                        F_frac=[]
                        F_fs=[]
                        F_fs_pcal=[]
                        F_side=[]
                        F_first_sample=[]



                        F1=None
                        
                        current_scaling_pair = scaling_pair
                        current_key_sample = key_sample
                        current_key_station = key_station
                        current_n_bins_pcal = n_bins_pcal
                        current_vector_split = vector_split
                        current_block_time = block_time
                        current_freq_channel = freq_channel
                        
                        ######################################
                        #        Update data structures
                        ######################################
                        # Read data
                        v_dequant = get_samples(samples_quant,bits_per_sample,data_type,num_samples,SINGLE_PRECISION)
                        [F1,F_ind,F_delays,F_rates,F_fs,F_fs_pcal,F_first_sample,F_frac,F_side,FFT_SIZE]=update_stored_samples(v_dequant,F1,F_ind,key_station_pol,F_delays,F_rates,F_fs,F_fs_pcal,abs_delay,rate_delay,fs,fs_pcal,F_first_sample,first_sample,data_type,F_frac,fractional_sample_delay,shift_delay,F_side,sideband,FFT_SIZE_IN)
                


            else:
                # TO DO: Discontinued, untested. Update.
                # One baseline per task
        
                # If it is an autocorrelation iterate again
                for iter_process_pcal in range(0,1+is_autocorr):
                    #If new pair and first sample, store data; otherwise do product and write to file          
                    if key_pair_accu!=current_key_pair_accu:
                        #New accumulation block:
                        # Write accumulated products to file (if it's not the first block, i.e., there's no data yet)
                        if current_key_pair_accu!=None:
                            #Write to stdout (ONLY AFTER ACCUMULATION)
                            
                            # Check if previous partition was all-baselines-per-task
                            if not(F1 is None):
                                normalize_after_compute=True
                                # This call includes not(COMPUTE_FOR_SUB_ACC_PERIOD) to bypass fx, so fx is only done before writting output
                                # TO DO: untested!
                                [F_delays,F_rates,F_frac,F_fs,F_fs_pcal,F_side,F_first_sample]=restore_Fs(last_F_delays,last_F_rates,last_F_frac,last_F_fs,last_F_fs_pcal,last_F_side,last_F_first_sample,F_delays,F_rates,F_frac,F_fs,F_fs_pcal,F_side,F_first_sample)
                                [acc_mat,count_acc,count_sub_acc,n_sp,last_F_ind,\
                                 failed_acc_count,dismissed_acc_count,F1_partial,F_ind_partial,\
                                 acc_pcal,pre_pcal,count_acc_pcal,F_first_sample_partial,\
                                 F_adj_shift_partial,F_stack_shift,F_adj_shift_pcal,F_stack_shift_pcal,F_pcal_fix,F_lti] = compute_fx_for_all(F1_partial,F_ind_partial,F1,\
                                                                                       FFT_SIZE,WINDOWING,\
                                                                                       acc_mat,count_acc,\
                                                                                       normalize_after_compute,F_ind,\
                                                                                       last_F_ind,n_sp,failed_acc_count,\
                                                                                       dismissed_acc_count,\
                                                                                       current_scaling_pair,DTYPE_COMPLEX,\
                                                                                       acc_pcal,pre_pcal,n_bins_pcal,count_acc_pcal,\
                                                                                       PHASE_CALIBRATION,\
                                                                                       not(COMPUTE_FOR_SUB_ACC_PERIOD),\
                                                                                       F_delays,F_rates,F_fs,current_freq_channel,\
                                                                                       F_first_sample,F_first_sample_partial,\
                                                                                       F_frac,current_block_time,F_adj_shift_partial,\
                                                                                       F_stack_shift,F_adj_shift_pcal,F_stack_shift_pcal,\
                                                                                       F_pcal_fix,F_side,F_lti)
                                
                                # Adjust pcal rotation due to initial alignment with delay model
                                if PHASE_CALIBRATION>0:
                                    acc_pcal = adjust_shift_acc_pcal(acc_pcal,F_pcal_fix,v=DEBUG_GENERAL_R)

                                
                                lines_out = get_lines_out_for_all(char_type,n_sp,last_F_ind,current_pairs,count_acc,acc_mat,\
                                                                  current_block_first_sample,acc_pcal,count_acc_pcal,current_vector_split)
                                if lines_out!=[]:
                                    for line_out in lines_out:
                                        print(line_out)
                                

                                print("zR"+KEY_SEP+"kpa="+current_key_pair_accu+",Adjusted stack=["+','.join(map(str,map(int,F_stack_shift)))+"]")
                                print("zR"+KEY_SEP+"kpa="+current_key_pair_accu+",Adjusted shifts=["+','.join(map(str,map(int,F_adj_shift_partial)))+"]")
                                
                                if (failed_acc_count>0)or(dismissed_acc_count>0):
                                    print("zR"+KEY_SEP+"Failed accs="+str(failed_acc_count)+",dismissed accs="+str(dismissed_acc_count)+",in=a"+current_block_first_sample)
                                          
                                    failed_acc_count=0
                                    dismissed_acc_count=0
                                acc_mat=None
                                last_F_ind=None
                                F1=None
                            else:
                            
                                accu_prod_div = normalize_mat(accu_prod,count_acc)
                                str_print = get_str_r_out(current_key_pair_accu,count_acc,current_vector_split,current_block_first_sample,accu_prod_div)
                                print(str_print)
                                
                                # Print phase calibration results
                                if acc_pcal!=[]:
                                    # TO DO: phase calibration currently disabled?
                                    ##pcal_fft = window_and_fft(acc_pcal,current_n_bins_pcal,C_INI_CR_WINDOW_SQUARE)
                                    ##acc_pcal_div = np.divide(acc_pcal,count_acc)
                                    ### values for LSB (start counting from last, take indices from mapper)
                                    ###pcal_L=
                                    ### values for USB
                                    ##str_print = "pcal"+current_key_pair_accu[2:]+'sxa'+str(count_acc)+'\t'+' '.join(current_vector_split[:(META_LEN-1)])+' '+current_block_first_sample+' '+' '.join(map(str, acc_pcal_div))
                                    ##
                                    str_print = get_str_pcal_out(acc_pcal,current_n_bins_pcal,count_acc_pcal,current_key_pair_accu,current_vector_split,current_block_first_sample)
                                    print(str_print)


                    

                        accu_prod=[]
                        current_key_pair_accu = key_pair_accu
                        current_key_station = key_station
                        current_block_first_sample = block_first_sample
                        current_bits_per_sample = bits_per_sample
                        current_n_bins_pcal = n_bins_pcal
                        current_data_type = data_type
                        current_encoding = encoding
                        current_freq_channel = freq_channel
                        count_acc=0
                        count_acc_pcal = 0
                        acc_pcal=np.array([])
                        pre_pcal = np.array([])
                        if (ENCODE_B64==1)and(encoding==C_INI_MEDIA_C_NO):
                            current_samples_quant = samples_quant
                        elif encoding == C_INI_MEDIA_C_VQ:
                            current_encoding_width = encoding_width
                        current_key_sample = key_sample
                        current_vector_split = vector_split
                    else:
                        if current_key_sample==key_sample:
                       
                            
                            if not(FFT_HERE):
                                # TO DO: untested?
                                v1fft=np.asarray(current_vector_split[META_LEN:]).astype(complex)
                                v2fft=np.asarray(vector_split[META_LEN:]).astype(complex)
                            else:
                                # If doing FFT here
                                
                                #if current_data_type=='c':
                                # TO DO: check encoding / current_encoding use...
                                if (ENCODE_B64==1)and(encoding==C_INI_MEDIA_C_NO):
                                    
                                    v1_dequant = get_samples(current_samples_quant,current_bits_per_sample,current_data_type,num_samples,SINGLE_PRECISION)
                                    if is_autocorr:
                                        v2_dequant = v1_dequant
                                    else:
                                        v2_dequant = get_samples(samples_quant,bits_per_sample,data_type,num_samples,SINGLE_PRECISION)
                                    

                                
    
                                # TO DO: Need to work on this...
                                if (encoding==C_INI_MEDIA_C_VQ)or(current_encoding==C_INI_MEDIA_C_VQ):
                                    
                                    # TO DO: untestesd after changes
                                    [v1_dequant,v2_dequant] = get_vq_decoded_samples(codecs_serial,codebook_list,key_station,current_key_station,\
                                                                                  encoding_width,current_encoding_width,v1_quant,v2_quant,\
                                                                                  vector_split,dictc01bit)

                                # TO DO: add data type for calling window_and_fft
                                # FFT and FFT*
                                v1fft = window_and_fft(v1_dequant,FFT_SIZE,WINDOWING,1,DTYPE_COMPLEX)
                                if is_autocorr:
                                    v2fft = v1fft
                                else:
                                    v2fft = window_and_fft(v2_dequant,FFT_SIZE,WINDOWING,1,DTYPE_COMPLEX)
                                                        
                                
                            # Conjugate second one
                            v2fft=np.conjugate(v2fft)
                            # Multiply and accumulate     
                            accu_prod = multiply_accumulate(accu_prod,v1fft,v2fft)
                            count_acc+=len(v1fft)
                            # TO DO: raies error y num_chunks1 differ from num_chunks2? Work on this and simplify
                            
                            
                            
                            # Phase calibration (only on second iteration if is_autocorr)
                            if (iter_process_pcal)and(PHASE_CALIBRATION):
                                # TO DO: migrate this functionality to multiple-baselines-per-task modes
                                pre_pcal = np.append(pre_pcal,v1_dequant)
                                pre_pcal = np.array(pre_pcal).flatten()
                                index_saved_pcal = (len(pre_pcal)//n_bins_pcal)*n_bins_pcal
                                pcal_process=pre_pcal[:index_saved_pcal]
                                pre_pcal=pre_pcal[index_saved_pcal:]
                                pcal_reshaped = reshape_pcal(pcal_process,n_bins_pcal)
                                [acc_pcal,count_acc_pcal] = accumulate_pcal(acc_pcal,pcal_reshaped,count_acc_pcal)
                                
                                
        
                                
                        else:
                            current_key_sample = key_sample
                            current_key_station = key_station
                            current_n_bins_pcal = n_bins_pcal
                            current_vector_split = vector_split
                            current_block_time = block_time
                            current_freq_channel = freq_channel
                            if (ENCODE_B64==1)and(vector_split[INDEX_ENCODING]==C_INI_MEDIA_C_NO):
                                current_samples_quant = samples_quant
                            elif encoding == C_INI_MEDIA_C_VQ:
                                current_encoding_width = encoding_width
                                

        ######################################
        #         Last write
        ######################################    
        #
        # This has to be equivalent to the writes above
        # TO DO: group this into function to avoid potential errors
        if current_key_pair_accu != None:
            
            
            if pairs[2]=="A.A": 
                
                if count_acc>0:
                    normalize_after_compute=True
                    [F_delays,F_rates,F_frac,F_fs,F_fs_pcal,F_side,F_first_sample]=restore_Fs(last_F_delays,last_F_rates,last_F_frac,last_F_fs,last_F_fs_pcal,last_F_side,last_F_first_sample,F_delays,F_rates,F_frac,F_fs,F_fs_pcal,F_side,F_first_sample)
                    
                    ########
                    #  FX
                    ########
                    [acc_mat,count_acc,count_sub_acc,n_sp,last_F_ind,\
                         failed_acc_count,dismissed_acc_count,F1_partial,F_ind_partial,\
                         acc_pcal,pre_pcal,count_acc_pcal,F_first_sample_partial,\
                         F_adj_shift_partial,F_stack_shift,F_adj_shift_pcal,F_stack_shift_pcal,F_pcal_fix,F_lti] = compute_fx_for_all(F1_partial,F_ind_partial,F1,\
                                                                                             FFT_SIZE,WINDOWING,acc_mat,count_acc,\
                                                                                             normalize_after_compute,F_ind,\
                                                                                             last_F_ind,n_sp,failed_acc_count,\
                                                                                             dismissed_acc_count,\
                                                                                             current_scaling_pair,DTYPE_COMPLEX,\
                                                                                             acc_pcal,pre_pcal,n_bins_pcal,\
                                                                                             count_acc_pcal,PHASE_CALIBRATION,\
                                                                                             0,F_delays,F_rates,F_fs,current_freq_channel,\
                                                                                             F_first_sample,F_first_sample_partial,\
                                                                                             F_frac,current_block_time,F_adj_shift_partial,\
                                                                                             F_stack_shift,F_adj_shift_pcal,F_stack_shift_pcal,\
                                                                                             F_pcal_fix,F_side,F_lti)
                    
                    #########
                    #  Pcal
                    #########
                    if PHASE_CALIBRATION>0:
                        acc_pcal = adjust_shift_acc_pcal(acc_pcal,F_pcal_fix,v=DEBUG_GENERAL_R)

                    
                    #########
                    #  Out
                    #########
                    lines_out = get_lines_out_for_all(char_type,n_sp,last_F_ind,current_pairs,count_acc,acc_mat,\
                                                          current_block_first_sample,current_vector_split,acc_pcal,count_acc_pcal,current_scaling_pair)
                    if lines_out!=[]:
                        for line_out in lines_out:
                            print(line_out)
                    
                    
                    ##########
                    #  Stats
                    ##########
                    lines_stats = get_lines_stats(current_key_pair_accu,F_stack_shift,\
                                            F_adj_shift_partial,F_lti,last_F_ind,failed_acc_count,\
                                            current_block_first_sample,dismissed_acc_count)   
                    if lines_stats!=[]:
                        for line_stats in lines_stats:
                            print(line_stats)
                    

                    failed_acc_count=0
                    dismissed_acc_count=0 
                    acc_mat=None
                    last_F_ind=None
                    
                else:
                    str_print = line
                    print(str_print) 
                    

                
                # TODO: last print for pcal?? Already in previous block?
                
            else:
                # One baseline per task
                # TO DO: discontinued, untested
                if count_acc>0:
                    accu_prod_div = normalize_mat(accu_prod,count_acc)
                    str_print = get_str_r_out(current_key_pair_accu,count_acc,current_vector_split,current_block_first_sample,accu_prod_div)
                
                else:
                    str_print = line
                print(str_print)
                # Print phase calibration results
                if acc_pcal!=[]:
                    str_print = get_str_pcal_out(acc_pcal,current_n_bins_pcal,count_acc_pcal,current_key_pair_accu,current_vector_split,current_block_first_sample)
                    print(str_print)
                    acc_pcal=np.array([])
                    pre_pcal = np.array([])


    if no_data==1:
        print("zR"+KEY_SEP+"No data")


        
        
if __name__ == '__main__':
    main()


# <codecell>


