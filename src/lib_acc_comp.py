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
#File: lib_acc_comp.py.
#Author: A.J. Vazquez Alvarez (ajvazquez@haystack.mit.edu)
#Description:
"""
Functions for accumulation period management: computations of identifiers based on timestamps, etc.

"""
#History:
#initial version: 2016.09 ajva
#MIT Haystack Observatory

from __future__ import division
import numpy as np


def get_num_den_accs(acc_time_str):
    """
    Get num and den for accumulation period.
    
    Parameters
    ----------
     acc_time_str : str
         accumulation time (e.g. "1/3").
     
    Returns
    -------
     num : int
         numerator of the fraction.
     den : int
         denominator of the fraction.
     
    Notes
    -----
    |
    | **TO DO:**
    |
    |  Consider removing along with get_acc_float(), initially devised to support fractions as integration times.
    """
    num=1
    den=1
    acc_split=acc_time_str.split("/")
    if len(acc_split)>1:
        den=int(acc_split[1])
    else:
        num=int(acc_split[0])
    return([num,den])


def get_acc_float(acc_time_str):
    """
    Converts a fraction of integers into a float. Initially devised to support fractions in accumulation time, no longer needed.
    
    Parameters
    ----------
     acc_time_str : str
         accumulation time as str of float (e.g. "0.25") or fraction (e.g. "1/4").
     
    Returns
    -------
     acc_float : float
         accumulation time (e.g. 0.25).
    
    Notes
    -----
    |
    | **TO DO:**
    |
    |  Consider removing along with get_num_den_accs(), initially devised to support fractions as integration times.
    |  To be replaced with simply float(acc_time_str).
    """
    if "." in acc_time_str:
        acc_float=float(acc_time_str)
    else:
        [num,den]=get_num_den_accs(acc_time_str)
        acc_float=float(num)/float(den)
    return(acc_float)


def get_tot_acc_blocks(acc_time_str,signal_duration):
    """
    Returns number of accumulation blocks.
    
    Parameters
    ----------
     acc_time_str : str(float)
         accumulation time.
     signal_duration : str(float)
         duration of the scan.
     
    Returns
    -------
     tot_acc_blocks : int
         number of accumulation periods within the scan.
    """
    if "." in acc_time_str:
        tot_acc_blocks=int(np.ceil(signal_duration/float(acc_time_str)))
    else:
        [num,den]=get_num_den_accs(acc_time_str)
        tot_acc_blocks=int(np.ceil((float(signal_duration)*den/num)/1))
    return(tot_acc_blocks)


def get_list_acc_frontiers(acc_time,signal_duration,seconds_ref):
    """
    Returns list of times of separators for accumulation periods, i.e. start times of the accumulation periods, 
       plus another float with the end time of the last accumulation period.
    
    Parameters
    ----------
     acc_time : float
         accumulation period duration [s].
     signal_duration : float
         duration of the experiment [s].
     seconds_ref : float
         number of seconds corresponding to the start of the experiment [s].
     
    Returns
    -------
     out : list of float
         timestamps [s] from 0 to signal_duration with separators for acc periods.
    """
    #return(np.arange(float(seconds_ref),seconds_ref+signal_duration,acc_time))
    # If beyond last element, discard
    return(np.arange(float(seconds_ref),seconds_ref+signal_duration+acc_time,acc_time))



def adjust_seconds_fr(samples_per_channel_in_frame,fs,seconds_fr,num_frame):
    """
    Get the timestamp for the first sample in this frame.
    
    Parameters
    ----------
     samples_per_channel_in_frame : int
         number of sample components per channel.
     fs : int or float
         sampling frequency.
     seconds_fr : int or float
         seconds for this frame (from frame header)
     num_frame : int
         frame number (from frame header).
     
    Returns
    -------
     time_first_frame : float
         timestamp [s] corresponding to the first sample of this frame.
    """
    seconds_per_frame=samples_per_channel_in_frame/float(fs)
    time_first_sample=float(seconds_fr)+num_frame*seconds_per_frame
    return(time_first_sample)

def get_frame_acc(seconds_fr,num_frame,fs,samples_per_channel_in_frame,list_acc_frontiers,acc_time):
    """
    Returns the index of the accumulation period corresponding to this frame, the relative number of the frame into the acc period,
    and the seconds corresponding to the first sample.
    
    Parameters
    ----------
     seconds_fr
         seconds for this frame (from frame header)
     num_frame
         frame number (from frame header).
     fs
         sampling frequency.
     samples_per_channel_in_frame
         number of sample components per channel.
     list_acc_frontiers
         list generated with get_list_acc_frontiers().
     acc_time
         accumulation time.
    
    Returns
    -------
     index_front
         index to the list of accumulation period frontiers.
     frame_rel_pos_time
         number of frame relative to the accumulation period.
     time_first_sample
         timestamp for the first sample of this frame.
     seconds_previous frame
         timestamp for the first sample of the previous frame.
     frames_per_acc
         number of frames per accumulation period.
    
    Notes
    -----
    |
    | **TO DO:**
    |
    |  Merge with get_acc_block_for_time(), avoid repeated code.
    """

    seconds_per_frame=samples_per_channel_in_frame/float(fs)
    time_first_sample=float(seconds_fr)+num_frame*seconds_per_frame
    frames_per_acc=int((acc_time/seconds_per_frame)//1)
    index_front=-2
    beyond_last=1
    for i in list_acc_frontiers:
        index_front+=1
        if time_first_sample<i:
            #if list_acc_frontiers[index_front]==list_acc_frontiers[-1]:
            #    beyond_last=1
            beyond_last=0
            break
    
    if beyond_last:
        frame_rel_pos=-1
        index_front=-1
    elif index_front>=0:
        #frame_rel_pos=int(np.round((time_first_sample-list_acc_frontiers[index_front])/seconds_per_frame))
        frame_rel_pos=int(np.round((time_first_sample-list_acc_frontiers[index_front])*float(fs)/float(samples_per_channel_in_frame)))
        #frame_rel_pos=num_frame%frames_per_acc
    else:
        frame_rel_pos=-1
    seconds_previous_frame=int((time_first_sample-seconds_per_frame)//1)
        
    return([index_front,frame_rel_pos,time_first_sample,seconds_previous_frame,frames_per_acc])
    
    
    
def get_acc_block(seconds_fr,seconds_ref,acc_time_str,tot_samples,num_channels,fs,frame_num,datatype='c'):
    """
    Get acc_block id for this frame.
    
    Parameters
    ----------
     seconds_fr
         seconds in the header of the processed frame.
     seconds_ref : float
         seconds corresponding to the start of the current accumulation period.
     acc_time_str : str
         accumualation time. Expected format is a string from a float.
     tot_samples
         total number of sample components in the frame (for all the channels).
     num_channels
         number of channels in this frame.
     fs
         sampling frequency.
     frame_num
         frame number in the header of the processed frame.
     datatype: {'r' , 'c'}
         'r' for real, 'c' for complex.
     
     Notes
     -----
     | datatype is required for computing the number of sample components per sample (2 for complex, 1 for real).
     | acc_time str was initially devised to support fractions (e.g. "1/3") before supporting float accumulation times (e.g. 0.32), 
     |    but this feature (fractions) is no longer needed.
    """ 
    seconds_from_ref=seconds_fr-seconds_ref
    base_block = get_tot_acc_blocks(acc_time_str,seconds_from_ref)

    [num,den]=get_num_den_accs(acc_time_str)
    seconds_ext_num=seconds_from_ref//num
    if den>1:
        seconds_ext_num=seconds_from_ref*den
        samples_per_channel=tot_samples/num_channels
        if datatype=='c':
            samples_per_channel=samples_per_channel//2
        offset_block_samples=int(((float(frame_num*samples_per_channel)/fs)*den)//1)
        seconds_ext_num+=offset_block_samples

    return(seconds_ext_num)


def get_acc_block_for_time(time_first_sample,list_acc_frontiers):
    """
    Get accumulation block id.
    
    Parameters
    ----------
     time_first_sample : float
         [s].
     list_acc_frontiers : list of floats
         generated with get_list_acc_frontiers().
    
    Returns
    -------
     index_front
         accumulation block id.
     i
         list_acc_frontiers[index_front].
    """
    i=-1
    index_front=-2
    beyond_last=1
    for i in list_acc_frontiers:
        index_front+=1
        if time_first_sample<i:
            #if list_acc_frontiers[index_front]==list_acc_frontiers[-1]:
            #    beyond_last=1
            beyond_last=0
            break
    if beyond_last:
        index_front=-1
        i=-1
    elif index_front<0:
        i=index_front
    else:
        i=list_acc_frontiers[index_front]
    
    if i==list_acc_frontiers[-1]:
        i=-1
    
    return([index_front,i])

