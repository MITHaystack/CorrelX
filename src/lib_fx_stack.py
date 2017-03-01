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
#File: lib_fx_stack.py.
#Author: A.J. Vazquez Alvarez (ajvazquez@haystack.mit.edu)
#Description: 
"""
CorrelX FX correlation and samples-stack routines.

"""
#History:
#initial version: 2016.10 ajva
#MIT Haystack Observatory

from __future__ import print_function,division
import scipy.fftpack as scfft
import numpy as np 
import imp

import const_mapred
imp.reload(const_mapred)
from const_mapred import *

import lib_pcal
imp.reload(lib_pcal)
from lib_pcal import *

import lib_delay_model
imp.reload(lib_delay_model)
from lib_delay_model import *

import lib_debug
imp.reload(lib_debug)
from lib_debug import *

import const_performance
imp.reload(const_performance)
from const_performance import *


if USE_FFTW:
    import pyfftw 
    pyfftw.interfaces.cache.enable()
    #pyfftw.interfaces.cache.set_keepalive_time(20)
    

# use numexpr
if USE_NE:
    import numexpr as ne
    ne.set_num_threads(THREADS_NE)

# multiprocessing
if USE_MP:
    import multiprocessing
    
    

    
###########################################
#           Samples stacking
########################################### 


def hstack_new_samples(F1_partial,F_ind_partial,F_ind,F1,F_adj_shift_partial,F_stack_shift,F_lti_in,F_first_sample,\
                       mode_str="",F_frac_over_ind=[]):
    """
    Concatenation of new samples with previously saved.
    
    Parameters
    ----------
     F1_partial
         previously saved samples.
     F_ind_partial
         identifiers for each row of saved samples.
     F_ind
         identifiers for rows of new samples.
     F1
         new samples.
     F_adj_shift_partial
         [unused] previously used for keeping track of the number of samples to add/drop, superseded by F_frac_over.
     F_refs
         indices for F1 based on F1_partial, used for other lists obtained in update_stored_samples(): (F_delay,F_rates,etc).
     F_stack_shift
         [unused] previously used to keep track of added/dropped samples.
     F_lti
         list with last sample (l), total number of samples processed (t), invalid samples (i), and adjuted samples for each stream.
     F_adj_shift_partial
         number of samples that have to be adjusted
     F_first_sample
         list with first sample corresponding to the streams in F1.
     mode_str
         "f" for normal operation model, "pcal" for saving the samples for phase calibration.
     F_frac_over
         list with the number of positions of the samples added/dropped due to fractional sample correction overflow,
                              see get_frac_over_ind() and fix_frac_over() for more details.
                              
    Returns
    -------
     F1_partial_out
         F1_partial with samples from F1 added.
     F_ind_partial_out
         station-polarization identifiers for the streams in F1_partial_out (same format as in keys).
     F_refs_out
         indices to F_frac etc structures based on F1_partial.
     F_stack_shift_out
         [unused] previously used to keep track of added/dropped samples.
     F_lti_out
         F_lti updated.
     F1_out
         Emtpy list of arrays.
    
    
    Notes
    -----
    |
    | **Procedure:**
    |
    |  If there are stored samples, iterate over list of stored samples:
    |   1. [currently disabled] Add zero padding if first sample does not match the expected first sample number.
    |   2. Concatenate new samples (accesed at F1 via F_ref) with stored samples.
    |  Otherwise initialize structures and store new samples.
    |
    |
    | **Approximations:**
    |
    |  -Zero-padding currently disabled, new samples are simply stored. This due to the need to debug yet the offsets in the 
    |    update of added/dropped samples from fix_frac_over(). Although these numbers are obtained in the mapper, there may
    |    be invalid frames that thus leave a gap.
    |  -It is not checked if the first sample number is lesser than the last sample stored, but this should not be necessary.
    |
    |
    | **Debugging:**
    |
    |  Activate DEBUG_HSTACK for tabulated output of the concatenations.
    |
    |
    | **TO DO:**
    |
    |  Group repeated code, and create functions for managing F_lti.
    """
    F_lti=np.copy(F_lti_in)
    failed_hstack=0   
    F_stack_shift_out=F_stack_shift
    reset_structures=0
    F_ind_partial_out=[]
    F1_partial_out=[]
    F_lti_out=[]
    F_refs_out=list(range(len(F1))) # This is for indices to other structures, to avoid moving data
    adjusted=0
    for i in range(max(len(F_ind),len(F_ind_partial))):
        F_ind_partial_out.append(-1)
        
        F1_partial_out.append(np.array([]).reshape(0))
        
        if i<len(F_lti):
            F_lti_out.append(F_lti[i])
        else:
            F_lti_out.append([-1,0,0,0]) # last, total, invalid, adjusted
    if (len(F_ind_partial)==0):
        reset_structures=1
    else:
        #TO DO: resort if necessary based on F_ind and F_ind_partial, and check for errors
        # Currently simply checking that stored are new are equal, if not dismiss old data and take new
        
        if F_ind_partial!=[]:
            if F_ind_partial!=[]:
                for i in range(len(F_ind)):
                    
                    # Check if missing data
                    
                    tot_samples=len(F1[i])
                    last_sample=F_first_sample[i]+tot_samples
                    # There may be a different sorting due to delay correction...
                    if F_ind[i] in F_ind_partial:
                        index_in_partial=F_ind_partial.index(F_ind[i])
                        
                        
                        diff_first=F_first_sample[i]-(F_lti_out[index_in_partial][0])
                        # TO DO: they should be equal, add check for other inequality
                        
                        offset_frac=0
                        if F_frac_over_ind!=[]:
                            offset_frac=F_frac_over_ind[i][0]
                            #diff_first-=offset_frac
                            #diff_first-=F_lti[index_in_partial][3]
                            
                        F_lti_out[index_in_partial][3]+=offset_frac
                        
                        
                        ##if diff_first>0:
                        ## TO DO: disabled padding for missing samples...
                        #    # Missing samples
                        #    F1_partial_out[index_in_partial]=np.hstack((F1_partial[index_in_partial],np.zeros(diff_first,dtype=complex),F1[i]))
                        #    F_lti_out[index_in_partial][2]+=diff_first
                        #    print("zR\Warning: Inserted "+str(diff_first)+" samples at ls "+str(last_sample)+" for st "+str(F_ind[i]))
                        #else:
                        
                        try:
                            F1_partial_out[index_in_partial]=np.hstack((F1_partial[index_in_partial],F1[i]))
                        except IndexError:
                            print("Failed hstack "+str(F_first_sample[i]))
                            failed_hstack=1
                        
                        F_ind_partial_out[index_in_partial]=F_ind[i]
                        F_refs_out[index_in_partial]=i
                        
                        if DEBUG_HSTACK:
                            print_debug_r_hstack(mode_str,index_in_partial,F_ind_partial[index_in_partial],\
                                                 F_ind_partial_out[index_in_partial],i,F_ind[i])
                        
                        F_lti_out[index_in_partial][0]=last_sample
                        F_lti_out[index_in_partial][1]+=tot_samples
                    else:
                        # new record
                        diff_first=F_first_sample[i]-(F_lti_out[i][0])
                        offset_frac=0
                        if F_frac_over_ind!=[]:
                            offset_frac=F_frac_over_ind[i][0]
                            #diff_first-=offset_frac
                            # This record does not exist yet
                            #diff_first-=F_lti[i][3]
                        
                        F_lti_out[i][3]+=offset_frac
                        
                        ##if diff_first>0:
                        ## HARDCODED: disabled padding...
                        #    F1_partial_out[i]=np.hstack((np.zeros(diff_first,dtype=complex),F1[i]))
                        #    F_lti_out[i][2]+=diff_first
                        #    #F_lti_out[i][3]+=offset_frac
                        #    print("zR\Warning: Inserted "+str(diff_first)+" samples at ls "+str(last_sample)+" for st "+str(F_ind[i]))
                        #else:
                        
                        F1_partial_out[i]=np.copy(F1[i])
                        
                        F_ind_partial_out[i]=F_ind[i]
                        F_refs_out[i]=i
                
                        if DEBUG_HSTACK:
                            print_debug_r_hstack(mode_str,i,F_ind_partial[i],F_ind_partial_out[i],i,F_ind[i])
                    
                        F_lti_out[i][0]=last_sample
                        F_lti_out[i][1]+=tot_samples
                        #if F_frac_over_ind!=[]:
                        #    F_lti_out[index_in_partial][3]=offset_frac

            else:
                reset_structures=1
        else:
            reset_structures=1
    
    

    
    tried=0
    if DEBUG_FRAC_OVER:
        print("zR"+KEY_SEP+"oai"+str(len(F_stack_shift)).rjust(10)+str(len(F_adj_shift_partial)).rjust(10))
        print("zR"+KEY_SEP+"oi".ljust(8)+"  "+','.join(map(str,F_stack_shift))+"  "+','.join(map(str,F_adj_shift_partial)))
    # TO DO: this needs to be checked
    
    if len(F_stack_shift)<len(F_adj_shift_partial):
        F_stack_shift_out=[0]*len(F_adj_shift_partial)
        
    if DEBUG_FRAC_OVER:
        print("zR"+KEY_SEP+"o T="+str(tried)+",A="+str(adjusted))

    if reset_structures:
        
        #F1_partial_out=F1[:]
        F_lti_out=[]
        for i in range(len(F1)):
            F_lti_out.append([0,0,0,0])
            offset_frac=0
            if F_frac_over_ind!=[]:
                offset_frac=F_frac_over_ind[i][0]
            F_lti_out[i][3]=offset_frac
            tot_samples=len(F1[i])

            last_sample=F_first_sample[i]+tot_samples
            
            diff_first=F_first_sample[i]-(F_lti_out[i][0])
            
            ##if diff_first>0:
            #if 1==0:
            #    F1_partial_out[i]=np.hstack((np.zeros(diff_first,dtype=complex),F1[i]))
            #    F_lti_out[i][2]+=diff_first
            #else:
            
            F1_partial_out[i]=np.copy(F1[i])
            
            F_ind_partial_out[i]=F_ind[i]
            F_refs_out[i]=i
            
            F_lti_out[i][0]=last_sample
            F_lti_out[i][1]+=tot_samples
            
            if DEBUG_HSTACK:
                print_debug_r_hstack(mode_str,i,None,F_ind_partial_out[i],i,F_ind[i])

        F_stack_shift_out=[0]*len(F_ind_partial_out)
        #F_ind_partial_out=F_ind[:]
        #F_refs_out=list(range(len(F1)))
        #if (F_stack_shift!=[])and(F_adj_shift_partial!=[]):
        #    F_stack_shift_out=F_adj_shift_partial[:]
        #else:
        #    F_stack_shift_out=[0]*len(F_adj_shift_partial)

    if DEBUG_FRAC_OVER:
        print("zR"+KEY_SEP+"oao"+str(len(F_stack_shift_out)).rjust(10)+str(len(F_adj_shift_partial)).rjust(10))
    
    if DEBUG_HSTACK:
        print_debug_r_hstack_arrow(F1_partial,F1,F1_partial_out,F_lti_out)

    if DEBUG_HSTACK:
        print_debug_r_hstack_separator()
        
    #if mode_str=="f":
    # reset F1
    len_F1=len(F1)
    F1_out=[]
    for i in range(len_F1):
        F1_out.append(np.array([]))
      
    
    return([F1_partial_out,F_ind_partial_out,F_refs_out,F_stack_shift_out,F_lti_out,F1_out])




###########################################
#        Fractional sample overflow
########################################### 


def get_frac_over_ind(F_first_sample,F1,F_rates,F_fs,F_ind,F_side):
    """
    Get locations of samples to be added/dropped due to fractional bit shift.
    
    Parameters
    ----------
     F_first_sample
         list with the first sample number for each vector of data.
     F1
         list of vectors with new data for each stpol (only lengths are read).
     F_rates
         model/clock delay information for computing delays.
     F_fs
         sampling frequency for each vector with new data.
     F_ind
         list with identifiers for stations (for logging).
     F_side
         list with sidebands
    
    Returns
    -------
     F_frac_over_ind
         list of [number_of_samples_to_be_added_or_dropped,[list_of_locations_for_these_changes]] for each element in F1.
    
    
    Notes
    -----
    |
    | **Algorithm:**
    |
    |  Compute integer+fractional sample delay at both extremes of the vector with data, then find intersection
    |   with changes in fractional delay (given a fractional sample correction between 0 and 1, it checks for crossing at 0.5).
    |
    |
    | **Debugging:**
    |
    |  Activate DEBUG_FRAC_OVER for tabulated output of the fractional overflow corrections.
    |
    |
    | **TO DO:**
    |
    |  Add support for multiple samples.
    """
    F_frac_over_ind=[]
    for i in range(len(F1)):
        fs=F_fs[i]
        Ts=1/fs
        [sideband,data_type]=F_side[i]
        
        #sample0=F_first_sample[F_refs[stpol]]-len(F1[F_refs[stpol]])
        first_sample=float(F_first_sample[i])
        num_samples=len(F1[i])
        timescale=Ts*np.array([first_sample,first_sample+num_samples])

        [delay_rate_0,delay_rate_1,delay_rate_2,delay_rate_ref,clock_rate_0,\
               clock_rate_1,clock_abs_rate_0,clock_abs_rate_1,clock_rate_ref,\
               model_only_delay,clock_only_delay,diff_frac]=F_rates[i]
        #diff_frac=0

        clock_diff = [clock_rate_0,clock_rate_1]
        poly_diff = [delay_rate_0,delay_rate_1,delay_rate_2]
        clock_abs = [clock_abs_rate_0,clock_abs_rate_1]
        

        seconds_offset=0

        [r_recalc,m_unused,c_recalc,r_unused,a_unused] = get_delay_val(\
                       clock_diff=clock_diff,\
                       poly_diff=poly_diff,\
                       seconds_ref_clock=clock_rate_ref,\
                       seconds_ref_poly=delay_rate_ref,\
                       seconds=timescale,\
                       seconds_offset=seconds_offset,\
                       v=DEBUG_LIB_DELAY,diff_pol=DIFF_POLY)
        
        
        [full_fractional_recalc_f,fractional_recalc_f] = get_full_frac_val(r_recalc[0],fs,bypass_correction=1)
        [full_fractional_recalc_l,fractional_recalc_l] = get_full_frac_val(r_recalc[1],fs,bypass_correction=1)
        r_shift_v=[fractional_recalc_f,fractional_recalc_l]
        # TO DO: fix this
        r_shift_v_int=[0]*len(r_shift_v)
        delta_shift=0
        delta_shift_frac=0
        
        # TO DO: check this
        if r_shift_v[0]<=0.5 and r_shift_v[1]>0.5 and poly_diff[1]>0:
            delta_shift=1
            #delta_shift=-1
        elif r_shift_v[0]>=0.5 and r_shift_v[1]<0.5 and poly_diff[1]<0:
            delta_shift=-1
            #delta_shift=1
        #elif full_fractional_recalc_f//1 != full_fractional_recalc_l//1:
        #    print("Warning: change in sample not taken into account!")
        
        #if delta_shift!=0:
        #    if sideband=='L':
        #        delta_shift=-delta_shift
        

        delta_shift_frac=r_shift_v[1]-r_shift_v[0]

        if delta_shift>num_samples:
            print("zR\Warning: frac - delta shift too high: "+str(delta_shift)+" for "+str(num_samples)+\
                  " samples, skipping frac shift at first sample "+str(first_sample))
            delta_shift=0
        shift_v=[]

        if delta_shift==1 or delta_shift==-1:
            shift_v.append(int(np.round((0.5-r_shift_v[0])*float(num_samples)/(float(delta_shift_frac)))))
        elif delta_shift!=0:
            for i_d in range(abs(delta_shift)):
                # Linear approx., intersection with 0.5
                x=(0.5-r_shift_v[0])*float(num_samples)/(float(delta_shift_frac))
                x=int(np.round(x))
                shift_v.append(x)
       
        if DIFF_POLY==0:
            delta_shift=-delta_shift
            
                
        F_frac_over_ind.append([delta_shift,shift_v]) 
    
        fractional_sample_correction=0
    
    
    #if DEBUG_FRAC_OVER:
    #    print("zR"+KEY_SEP+"getfr: "+str(F_first_sample)+" "+str(F_frac_over_ind))
    
    
    
    return(F_frac_over_ind)




def fix_frac_over(F1,F_frac_over_ind,F_ind,F_first_sample):
    """
    Add or drop samples for overflow in fractional sample correction based on info from get_frac_over_ind().
    
    Parameters
    ----------
     F1
         list of vectors with new data.
     F_frac_over
         structure obtained in get_frac_over_ind() with locations of samples to be added/dropped.
     F_ind
         list of stpols corresponding to F1 (only for logging).
     F_first_sample
         list of first sample number to F1 (only for logging).
    
    Returns
    -------
     F1
         modified list of vectors with new data.
    """
    fixed_frac=0
    for i in range(len(F_frac_over_ind)):
        update_samp=F_frac_over_ind[i][0]
        v_ind=F_frac_over_ind[i][1]
        if update_samp!=0:
            if fixed_frac==0:
                if DEBUG_GENERAL_R:
                    print("zR\tFixing frac: "+str(F_ind)+" "+str(F_first_sample)+" "+str(F_frac_over_ind))
            fixed_frac=1
            if update_samp>0:
                if len(v_ind)==1:
                    F1[i]=np.delete(F1[i],v_ind[0])
                else:
                    F1[i]=np.delete(F1[i],v_ind)
            else:
                if len(v_ind)==1:
                    F1[i]=np.insert(F1[i],v_ind[0],0)
                else:
                    F1[i]=np.insert(F1[i],v_ind,[0]*len(v_ind))
    #if fixed_frac:
    #    print("zR\tFixed frac: "+str(F_ind)+" "+str(F_first_sample)+" "+str(F_frac_over_ind))

    return(F1)



###########################################
#             Exponential
########################################### 



def get_exp(x):
    """
    Get exponential based on fractional part of input, see Output below for details.
    
    Parameters
    ----------
     x : numpy array of float.
     
    Returns
    -------
     y : 1D numpy array of complex
         complex rotation (exponential of 2*j*pi*fractional_part(x))
     nr : bool
         do not rotate (1 if all elements in y are 1, 0 otherwise)
    
    Notes
    -----
    |
    | **Precision:**
    |
    |  Integer part is removed to avoid problems with precision, rotation (j2pi).
    |
    |
    | **Approximations:**
    |
    |  -numpy.exp is not called in the trivial cases.
    | -IMPORTANT!: For arrays of more than 1 element, if the first, second and last elements are equal to zero, then
    |      all the elements are assumed to be zero too. Need to replace this by some check on the polynomial.
    """
    nr=0
    
    # Check if it is only one element
    if len(x)==1:
        
        # Check if no rotation
        if x==0:
            y=1+0j
            nr=1
        else:
            if USE_NE_EXP:
                modf_val= np.modf(x)[0]
                pi_val = np.pi
                y=ne.evaluate("exp(1j*2*pi_val*modf_val)")
            else:
                y=np.exp(1j*2*np.pi*np.modf(x)[0])
           
    
    # (!) Check if first, second and last sample are equal, if so, compute only once and repeat
    # TO DO: check this approach
    elif x[0]==x[-1] and x[0]==x[1]:
        # Check if no rotation
        if x[0]==0:
            y=1+0j
            nr=1
        else:
            if USE_NE_EXP:
                modf_val= np.modf(x[0])[0]
                pi_val = np.pi
                y=ne.evaluate("exp(1j*2*pi_val*modf_val)")
            else:
                y=np.exp(1j*2*np.pi*np.modf(x[0])[0])
            
    
    # Otherwise compute for all samples
    #return(np.exp(1j*2*np.pi*np.modf(np.float64(x))[0]))

    else:
        if USE_NE_EXP:
            modf_val= np.modf(x)[0]
            pi_val = np.pi
            y=ne.evaluate("exp(1j*2*pi_val*modf_val)")
        else:
            y=np.exp(1j*2*np.pi*np.modf(x)[0])
        nr=0

    return([y,nr])
    #return(np.exp(1j*2*np.pi*np.modf(x)[0]))



def get_rotator(x):
    """
    Get a single complex rotator from a list of rotators.
    
    Parameters
    ----------
     x : list of complex.
     
    Returns
    -------
     rotator
         product of the elements in x.
    
    Notes
    -----
     TO DO: consider removing, devised to apply many rotators, but no longer needed.
    """
    rotator=1+0j
    for i in x:
        rotator=np.multiply(rotator,i)
    return(rotator)



###########################################
#           FX correlation
########################################### 



def window_and_fft(v1_dequant,fft_size,windowing,flatten_chunks=1,dtype_complex=complex,rfft_data_type='c'):
    """
    Apply window and do FFT of set of samples, to be grouped into chunks of FFT size.
    
    Parameters
    ----------
     v1_dequant
         numpy arrays with complex samples in the time domain (fringe rotation already applied to them).
     fft_size
         number of coefficients in the FFT.
     windowing
         shape of the window to be applied prior to FFT, square by default.
     flatten_chunks
         1 if one-baseline-per-task mode, otherwise all-baselines-per-task.
     dtype_complex
         complex type to be used in initialization of arrays.
     rfft_data_type
         [unused] initially devised to use rfft, currently not used.
     
    Returns
    -------
     v1_fft
         numpy array of arrays, with as many rows as the ratio between number of samples and fft_size (forced to
                       be integer), and as many columns of fft_size.
                       
    Notes
    -----
    |
    | **Performance:**
    |
    |  Using scipy fft, which yielded the highest performance on preliminary benchmarking with single thread reducer.
    |  PyFFTW implemented, but needs to be tested and benchmarked.
    |
    |
    | **TO DO: **
    |
    |  Add a more elegant implementation for windowing, and add more windows.
    |  Currently no windowing by default, may need to migrate functionality from this to windowing modes.
    """
                   
    num_chunks1=len(v1_dequant)//fft_size
    len_v1_dequant=len(v1_dequant)
    v1_dequant=np.vstack(v1_dequant)
    if flatten_chunks==1:
        # One-baseline-per-task 
        # Windowing:
        if windowing==C_INI_CR_WINDOW_HANNING:
            # -Hanning
            if num_chunks1==1:
                window_v1=np.hanning(fft_size)
                if rfft_data_type=='c':
                    v1fft=[scfft.fft(np.multiply(v1_dequant,window_v1))]
                else:
                    v1fft=[scfft.rfft(np.multiply(v1_dequant,window_v1))]
            else:
                window_v1=[np.hanning(fft_size)]*num_chunks1
                if rfft_data_type=='c':
                    v1fft=scfft.fft(np.multiply(np.reshape(v1_dequant,(-1,fft_size)),window_v1))
                else:
                    v1fft=scfft.rfft(np.multiply(np.reshape(v1_dequant,(-1,fft_size)),window_v1))
        else:
            # -Square
            if num_chunks1==0:
                v1fft=np.array([])
            elif num_chunks1==1:
                if USE_FFTW==0:
                    if rfft_data_type=='c':
                        v1fft=np.array([scfft.fft(v1_dequant)])
                    else:
                        v1fft=np.array([scfft.rfft(v1_dequant)])
                else:
                    fftw_input = pyfftw.empty_aligned(v1_dequant.shape, dtype=dtype_complex)
                    fftw_input[:] = v1_dequant
                    v1fft = pyfftw.interfaces.numpy_fft.fft(fftw_input,threads=THREADS_FFTW)
            else:
                reshaped_dequant = np.reshape(v1_dequant,(-1,fft_size))
                if USE_FFTW==0:
                    if rfft_data_type=='c':
                        v1fft=scfft.fft(reshaped_dequant)
                    else:
                        v1fft=scfft.rfft(reshaped_dequant)
                else:    
                    fftw_input = pyfftw.empty_aligned(reshaped_dequant.shape, dtype=dtype_complex)
                    fftw_input[:] = reshaped_dequant
                    v1fft = pyfftw.interfaces.numpy_fft.fft(fftw_input,threads=THREADS_FFTW)

    else:
        #TO DO: windowing untested for not flattened chunks (used if all baselines in same task)
        #if windowing==C_INI_CR_WINDOW_HANNING:
        #    window_v1=[np.hanning(fft_size)]*num_chunks1
        #    v1fft=fft(np.multiply(np.reshape(v1_dequant,(-1,fft_size)),window_v1))
        #else:
        #print(v1_dequant.shape[1])
        if (v1_dequant.shape[1]//fft_size)==0:
            v1fft=np.array([])
        else:
            # TO DO: need to reshape, but based on ordering of first samples (currently padding sample number in key...)
            reshaped_dequant = np.reshape(v1_dequant,(len_v1_dequant,-1,fft_size))
            if USE_FFTW==0:
                if rfft_data_type=='c':
                    v1fft=scfft.fft(reshaped_dequant)
                else:
                    v1fft=scfft.rfft(reshaped_dequant)
            else:
                fftw_input = pyfftw.empty_aligned(reshaped_dequant.shape, dtype=dtype_complex)
                fftw_input[:] = reshaped_dequant
                v1fft = pyfftw.interfaces.scipy_fftpack.fft(fftw_input,threads=THREADS_FFTW)

    return(v1fft)


def multiply_accumulate(accu_prod,v1fft,v2fft):
    """
    [Only used in one-baseline-per-task mode.]
    Multiply and accumulate two ffts (one for each station).
    """
    if accu_prod==[]:
        if len(v1fft)==1:
            accu_prod=np.multiply(v1fft[0],v2fft[0])
        else:
            accu_prod=np.sum(np.multiply(v1fft,v2fft),axis=0)
    else:
        if len(v1fft)==1:
            accu_prod+=np.multiply(v1fft[0],v2fft[0])
        else:
            accu_prod+=np.sum(np.multiply(v1fft,v2fft),axis=0)
    return(accu_prod)




def normalize_mat(acc_mat,count_acc,using_autocorrs=1):
    """
    Normalice results in accumulation matrix.
    
    Parameters
    ----------
     acc_mat
         accumulation matrix (see compute_x_all for the definition of the matrix).
     count_acc
         number of accumulations.
     using_autocorrs
         1 by default, normalization using auto-correlations.
    
    Returns
    -------
     acc_mat_out
         normalized accumulation matrix.
    
    Notes
    -----
    |
    | **Known issues:**
    |
    |  Need to check scaling.
    |
    | **TO DO:**
    |
    |  Take into account valid samples!
    """
    bypass_normalize=0

    if not(bypass_normalize):
        acc_mat_norm=acc_mat
        #acc_mat_out=np.divide(acc_mat,count_acc*acc_mat.shape[1])
        #acc_mat_norm=np.divide(acc_mat,count_acc*acc_mat.shape[1])
        acc_mat_out=acc_mat_norm
        multiplier_mat=np.ones(acc_mat_norm.shape)
        power_auto=np.zeros(acc_mat_norm.shape[0])
        for i in range(acc_mat_norm.shape[0]):
            power_auto[i]=np.sum(np.abs(acc_mat_norm[i,i]))
        for i in range(acc_mat_norm.shape[0]):
            for k in range(i,acc_mat_norm.shape[0]):
                multiplier_mat[i,k]=1/np.sqrt(power_auto[i]*power_auto[k])
        acc_mat_out=np.multiply(acc_mat_norm,multiplier_mat)
        #acc_mat_out=np.divide(np.multiply(acc_mat_norm,multiplier_mat),count_acc*acc_mat.shape[1])
    else:
        acc_mat_out=acc_mat
    
    return(acc_mat_out)



def get_val_for_fringe_exp(sideband,data_type,freq_channel,fs,r_recalc):
    """
    Compute vector 'x' with values that will go into e^j(2.pi.x) to be used in fringe rotation.
    
    Parameters
    ----------
     sideband : char {'L','U'}
         'L' for LSB, 'U' for USB.
     datatype : char {'c','r'}
         'c' for complex samples, 'r' for real samples.
     freq_channel : float
         channel frequency [Hz] as in configuration file.
     fs : float
         sampling frequency [Hz].
     r_recalc : 1D np.array of float
         delay values.
     
    Returns
    -------
     val : 1D np.array of float
         results.
     
    Notes
    -----
    |
    | **Configuration:**
    |
    |  Enable USE_NE_FRINGE for using numexpr, and if so, configure THREADS_NE for multithreading.
    |
    |
    | **TO DO:**
    |
    |  This should be checked.
    |  Case USB complex not tested yet.
    """
    
    # TO DO: check this
    #                                                               # [LSB/USB], [real/complex]
    if sideband=='L':
        if data_type=='c': #                                           #   LSB       complex
            if USE_NE_FRINGE:
                val=ne.evaluate("(freq_channel-fs)*r_recalc")
            else:
                val=(freq_channel-fs)*r_recalc
        else:  #                                                       #   LSB       real    
            if USE_NE_FRINGE:
                #val=ne.evaluate("(freq_channel-fs/2)*r_recalc")
                val=ne.evaluate("freq_channel*r_recalc")
            else:
                #val=(freq_channel-fs/2)*r_recalc
                val=freq_channel*r_recalc
    else: #                                                            #   USB       both
        if USE_NE_FRINGE:
            val=ne.evaluate("freq_channel*r_recalc")
        else:
            val=freq_channel*r_recalc
    return(val)



def fringe_rotation_wrap(args):
   return(fringe_rotation_work(*args))


def fringe_rotation_work(clock_diff,poly_diff,seconds_ref_clock,delay_rate_ref,timescale,seconds_offset,n_samples,sideband,\
                             last_n_samples,str_st,last_str_st,data_type,last_data_type,first_iteration,freq_channel,fs,F1_i,\
                             nr,rotation):
    """
    Worker for fringe rotation, see fringe_rotation() for more details.
    """
    #os.system("taskset -p 0xff %d" % os.getpid())
    nr=0
    [r_recalc,m_unused,c_unused,r_unused,a_unused] = get_delay_val(\
                               clock_diff=clock_diff,\
                               poly_diff=poly_diff,\
                               seconds_ref_clock=seconds_ref_clock,\
                               seconds_ref_poly=delay_rate_ref,\
                               seconds=timescale,\
                               seconds_offset=seconds_offset,\
                               v=DEBUG_LIB_DELAY,\
                               diff_pol=DIFF_POLY)
    
    first_delay=r_recalc[0]
    last_delay=r_recalc[-1]
    step_delay=(last_delay-first_delay)/float(n_samples)
    rate_interval=step_delay*fs
    if FULL_TIMESCALE==2:
        # Linear interpolation
        
        
        base_recalc=np.arange(0,float(n_samples))
        r_recalc=first_delay+base_recalc*step_delay
        if DEBUG_DELAYS:
            diff_last_first=last_delay-first_delay
            
    #else:
    #    rate_interval=0
    
    # Avoid recomputing if same delay, same number of samples and same type. Streams for the same station
    #  should be consecutive.
    computed=0
    
    if (str_st!=last_str_st and ( last_data_type!=data_type or last_n_samples!=n_samples)) or first_iteration:
        computed=1
        if SAVE_TIME_ROTATIONS:
            first_iteration=0

        val=get_val_for_fringe_exp(sideband,data_type,freq_channel,fs,r_recalc) # get vector x
        [ru,nr] = get_exp(val)                                                  # get vector e^j(2.pi.x)
        if not(nr):
            #rotation=get_rotator([ru])
            rotation=ru
    
    
    #last_r_recalc=r_recalc
    last_data_type=data_type
    last_str_st=str_st
    

    if not nr:
        #if USE_NE_FRINGE:
        #    interm=F1[i]
        #F1[i]=ne.evaluate("interm*rotation") # not faster
        #else:
        np.multiply(F1_i,rotation,F1_i)
    return([F1_i,rotation,last_data_type,last_str_st,first_iteration,first_delay,last_delay,rate_interval,computed])


def fringe_rotation(F1,F_first_sample,F_rates,freq_channel,F_fs,F_delays,F_refs,block_time,F_frac,F_adj_shift_partial,F_side,F_ind,F_lti):
    """
    Fringe rotation correction (previously doppler_correction()).
    
    Parameters
    ----------
     F1 : list
         list with stored samples to be processed, which number of samples is an integer multiple of the fft length.
     F1_first_sample : list
         list with first sample (integer) corresponding to the samples in F1.
     F_rates : list
         list with the delay information for the latest data received. Note that if follows a different ordering as F1 here,
                         and thus its elements has to be accessed through F_refs.
     freq_channel : float
         sky frequency [Hz].
     F_fs : list
         list with the sampling frequency corresponding to the samples in F1 (access through F_refs).
     F_delays
         [unused] absolute delays of the streams in F1 (access through F_refs).
     F_refs : list
         list of indices for accessing F_rates, F_fs, etc (those filled in update_stored_samples()).
     block_time
         time corresponding to this accumulation period (unused).
     F_frac
         fractional and integer delay corresponding to F1 (access through F_refs).
     F_adj_shift_partial
         [unused] previously, list with information on added/dropped samples based on fractional sample correction
                        overflows, currently unused.
     F_side : list
         list with sideband for each of the streams in F1 ('l' for LSB, 'u' for USB) (access through F_refs).
     F_ind : list
         list of station identifiers in the format used in the key (e.g. 0.0, 0.1)
    
    Returns
    -------
     F1
         F1 (input) with applied rotations if required.
     F_first_sample
         list with updated first samples (added number of samples in each element of F1).
    
    Notes
    -----
    |
    | **Configuration for trade-off performance vs. precision:**
    |
    |  There are three main modes, configuration in const_performance.py:
    |  FULL_TIMESCALE=0 -> Min. precision, Max. performance: only a phase rotation is applied, based on the delay computed for
    |                                                         the first sample.
    |  FULL_TIMESCALE=1 (default) -> Max. precision, Min. performance: a frequency shift is applied with a complete rotator, that is,
    |                                                         delays are computed for all the samples.
    |  FULL_TIMESCALE=2 -> Trade-off solution: delay is computed for the first and last sample, and a linear interpolation is 
    |                                                         done for obtaining the rest of delays. 
    |
    |
    | **Approximations:**
    |
    |  For modes FULL_TIMESCALE 1 and 2, the time scale array is computed only once, assuming the same sampling frequency for 
    |    all the streams.
    | 
    | The processing currently performed in a loop iterating over the streams in F1, that should be sorted. Given this, for each
    |    stream, it is checked if the previous processed stream corresponded to the same station, and if so, the previous rotators
    |    are used.
    |
    |
    | **Debugging:**
    |
    |  Activate DEBUG_DELAYS for tabulated output of the delay computations.
    |
    |
    | **References:**
    |
    |  [Th04] p172-173
    |
    |
    | **TO DO:**
    |
    |  F_fs: assuming single sampling frequency
    |  Bring configuration constants to configuration files.
    |  Delete unused arguments.
    | 
    |  Check if necessary to apply fractional offsets.
    """
    nr=0
    rotation=np.array([])
    if USE_MP:
        jobs = []
        arg_list=[]
        p = multiprocessing.Pool(MP_THREADS)
        
    
    F1_out=[]
    F_first_sample_out=[]
    first_iteration=1
    #last_r_recalc=0
    last_data_type='x'
    last_n_samples=0
    last_str_st=""
    
    
    
    # Assuming same length for all rows
    n_samples=len(F1[0])
    fs=F_fs[0]
    Ts=1/fs
    if FULL_TIMESCALE==1:
        # Evaluate delay for all samples
        #timescale_base_no_offset=np.array(list(range(n_samples)),dtype=float)
        timescale_base_no_offset=np.arange(n_samples,dtype=float)

        # Timescale in seconds
        timescale=np.multiply(timescale_base_no_offset,Ts)
    elif FULL_TIMESCALE==2:
        # Interpolate (linear based on first and last sample)
        timescale=np.array([0,float(n_samples)*Ts])
    
    else:
        timescale=np.array([0])
    
    
  
    for i in range(len(F1)):
        str_st=F_ind[i].split('.')[0]

        first_sample=F_first_sample[i]
        
        last_sample=first_sample+n_samples
        
        # Delay polynomials for this station
        [delay_rate_0,delay_rate_1,delay_rate_2,delay_rate_ref,clock_rate_0,clock_rate_1,clock_abs_rate_0,\
                                clock_abs_rate_1,clock_rate_ref,model_only_delay,clock_only_delay,diff_frac]=F_rates[F_refs[i]]
        [fractional_sample_correction,shift_delay]=F_frac[F_refs[i]]
        
        fs=F_fs[F_refs[i]]
        Ts=1/fs
        [sideband,data_type]=F_side[F_refs[i]]
        
        # Integer delay already applied (multiple of Ts)
        shift_delay=0
        error_f_frac=0
        
        # Delays at each sample
        first_sample_s=first_sample*Ts
 
        seconds_offset = first_sample_s
        
        clock_diff = [clock_rate_0,clock_rate_1]
        poly_diff = [delay_rate_0,delay_rate_1,delay_rate_2]
        clock_abs = [clock_abs_rate_0,clock_abs_rate_1]
        seconds_ref_clock=clock_rate_ref
        
        # clock_diff,poly_diff,seconds_ref_clock,delay_rate_ref,timescale,seconds_offset,
        #   DEBUG_LIB_DELAY,DIFF_POLY,FULL_TIMESCALE
        # n_samples,last_n_samples,str_st,last_str_st,data_type,last_data_type,first_iteration,freq_channel,fs,F1[i]
        
        
        if not USE_MP:
            # Serial execution
            [F1[i],rotation,last_data_type,last_str_st,first_iteration,first_delay,last_delay,\
                             rate_interval,computed] = fringe_rotation_work(clock_diff,poly_diff,\
                                 seconds_ref_clock,delay_rate_ref,timescale,seconds_offset,n_samples,sideband,\
                                 last_n_samples,str_st,last_str_st,data_type,last_data_type,first_iteration,\
                                 freq_channel,fs,F1[i],nr,rotation)
            #if SAVE_TIME_ROTATIONS:
            #    first_iteration=0

        
            if DEBUG_DELAYS:
                print_debug_r_delays_d(i,F_refs[i],F_ind[i],first_sample,n_samples,timescale,timescale[0],timescale[-1],\
                                         seconds_offset,first_delay,last_delay,rate_interval,fractional_sample_correction,\
                                         diff_frac,computed,nr)
        else:
            # Parallel
            arg_list.append([clock_diff,poly_diff,seconds_ref_clock,delay_rate_ref,timescale,seconds_offset,\
                             n_samples,sideband,last_n_samples,str_st,last_str_st,data_type,last_data_type,\
                             first_iteration,freq_channel,fs,F1[i],nr,rotation])
    
    
    if USE_MP: 
        F1_and_rotations = p.map(fringe_rotation_wrap,arg_list)
        p.close()
        p.join()
        F1=[F1_and_rot[0] for F1_and_rot in F1_and_rotations]
 
     
    return([F1,F_first_sample])


def compute_f_all(F1,fft_size,windowing,dtype_complex,F_frac=[],F_fs=[],F_refs=[],freq_channel=0,\
                      F_first_sample=[],F_rates=[],F_pcal_fix=[],F_side=[],F_ind=[],F_lti=[]):
    """
    Compute FFTs for all stations (all-baselines-per-task mode), and correct for fractional sample correction (linear phase).

    Parameters
    ----------
     F1
         list of stored samples (corresponding actually to F1_partial). Each element of the list is a numpy array
               with the complex samples in the time domain, with a number of samples that is a multiply of the FFT length.
     fft_size : int
         number of coefficients in the FFT.
     windowing : str
         shape of the window before FFT, currently 'square' by default.
     dtype_complex: type of data for initialization of the rotators.
     F_frac
         fractional and integer offsets applied at the mapper (acces via F_refs).
     F_fs
         sampling frequency for each stream in F1.
     F_refs
         indices to acces F_frac etc based on F_ind, i.e. from stored to new.
     freq_channel
         sky frequency.
     F_first_sample
         first sample number (actually last sample number plus one, it has to be corrected by subtracting the number of samples in F1.
     F_rates
         delay information for each of the streams (access via F_refs).
     F_pcal_fix
         offset for pcal accumulation results (due to the initial offset applied in the mapper). Later the pcal
              signals will be realigned as if no delay was applied to them.
     F_side
         list of single side band side for each stream, 'l' LSB or 'u' USB (access via F_refs).
     F_ind
         list of station-polarization identifiers corresponding to the streams in F1 (this actually corresponds
               to F1_ind_partial.
    
    Returns
    -------
     F1_fft
         list of array of arrays with FFTs with rotations applied.
     None
         [unused] previously outputing the conjugate of F1_fft, removed for efficiency.
     F_adj_shift_partial_out
         [unused] previously used to keep track of the number of samples to 
                    add/drop due to fractional sample overflows, superseded for F_frac_over.
     F_adj_shift_pcal_out
         [unused] previously used to keep track of the number of samples to 
                    roll the phase calibration results prior to FFT them, superseded for F_pcal_fix_out.
     F_pcal_fix_out
         list with number of samples to roll the pcal streams prior to FFT them.
     F_first_sample_out
         first sample for each stream (actually last sample number plus one).
    
    Notes
    -----
    |
    | **Procedure:**
    |
    |  For each element in F1:
    |   1. Create an array of arrays with the FFTs of the samples grouped into arrays of fft_size samples.
    |   2. Create a frequency scale of fft_size (linear from 0 to (n-1)/n).
    |   3a. If the computations have already been done for the same station, take the results.
    |   3b. Otherwise:
    |       Compute delay for the first sample, then fractional part of this delay, then scale frequency scale, then exponential.
    |       Rotate the FFT using the previous rotator.
    |
    |
    | **References:**
    |
    |  [Th04] p363
    |
    |
    | **TO DO:**
    |
    |  Detail where in the FFT the fractional sample for the rotator is evaluated.
    |    
    |  Check correction to phase in p363.   
    """
    F_adj_shift_partial_out=[]
    F_adj_shift_partial_mon=[]
    F_adj_shift_pcal_out=[]
    F_pcal_fix_out=[]
    F_first_sample_out=[]
    # TO DO: assuming all data same type for now
    [sideband,data_type]=F_side[0] 
    # Windowing and FFT
    first_iteration=1
    last_fractional_recalc=0
    last_str_st=""
    F1_fft = window_and_fft(F1,fft_size,windowing,flatten_chunks=0,dtype_complex=dtype_complex) # ,rfft_data_type=data_type)
    
    # If real samples take only half FFT (LSB or USB as applicable)
    if data_type=='r':
        if sideband=='L':
            F1_fft = np.delete(F1_fft,np.s_[:fft_size//2],2) 
        else:
            F1_fft = np.delete(F1_fft,np.s_[fft_size//2:],2)

    shift_int=0
    # Fractional sample correction
    F1_fft_rot=np.zeros(F1_fft.shape,dtype=dtype_complex)
    error_f_frac=1
    if F_rates!=[]:
        
        if data_type=='c':
            freqscale2 = np.arange(0,1,1/float(fft_size))
            fft_size_comp=fft_size
        else:
            if sideband=='L':
                freqscale2 = float(-1)*np.arange(0.5,0,float(-1)/float(fft_size)) # First half of the full vector (e.g. [-0.5 -0.375 -0.25 -0.125] with fft_size=8)
            else:
                freqscale2 = np.arange(0,1,1/float(fft_size))[:fft_size//2] # Second half the full vector (e.g. [ 0. 0.125 0.25 0.375] with fft_size=8)

            fft_size_comp=fft_size//2
        
        # p363

        for stpol in range(F1_fft.shape[0]):
            fs=F_fs[F_refs[stpol]]
            Ts=1/fs
            [sideband,data_type]=F_side[F_refs[stpol]]
            #str_st=F_ind[F_refs[stpol]].split('.')[0]
            str_st=F_ind[stpol].split('.')[0]
                            
            #sample0=F_first_sample[F_refs[stpol]]-len(F1[F_refs[stpol]])
            
            sample0=F_first_sample[F_refs[stpol]]
            ##adjustments (padding) from hstack_samples...
            #sample0+=F_lti[stpol][3]
            
            num_samples=len(F1[F_refs[stpol]])
            F_first_sample_out.append(sample0+num_samples)
            computed=0
            error_f_frac=0
           
            i_row=-1
            
            if last_str_st!=str_st or first_iteration:
                if SAVE_TIME_ROTATIONS:
                    first_iteration=0

                first_sample=sample0
                first_sample_s=first_sample*Ts
                
                
                [delay_rate_0,delay_rate_1,delay_rate_2,delay_rate_ref,clock_rate_0,\
                       clock_rate_1,clock_abs_rate_0,clock_abs_rate_1,clock_rate_ref,\
                       model_only_delay,clock_only_delay,diff_frac]=F_rates[F_refs[stpol]]
                diff_frac=0
                [fractional_sample_correction,shift_delay]=F_frac[F_refs[stpol]]
                shift_s=shift_delay*fs
                

                frtot_v=[]
                
                first_iteration_recalc=1

                # TO DO: consider centering in the interval (N//2)
                timescale=[0]
                clock_diff = [clock_rate_0,clock_rate_1]
                poly_diff = [delay_rate_0,delay_rate_1,delay_rate_2]
                clock_abs = [clock_abs_rate_0,clock_abs_rate_1]          
                seconds_ref_clock = clock_rate_ref
                
                #if USE_NE_F:
                #    npr1 = np.arange(F1_fft.shape[1])
                #    total_timescale = ne.evaluate("Ts*(sample0+fft_size*npr1)") # slower
                #else:
                total_timescale =Ts*(sample0+fft_size*np.arange(F1_fft.shape[1]))
                
                total_seconds_offset=0
                [r_recalc,m_unused,c_recalc,r_unused,a_unused] = get_delay_val(\
                                   clock_diff=clock_diff,\
                                   poly_diff=poly_diff,\
                                   seconds_ref_clock=seconds_ref_clock,\
                                   seconds_ref_poly=delay_rate_ref,\
                                   seconds=total_timescale,\
                                   seconds_offset=total_seconds_offset,\
                                   v=DEBUG_LIB_DELAY,diff_pol=DIFF_POLY)

                [full_fractional_recalc,fractional_recalc] = get_full_frac_val(r_recalc,fs)

                for row in range(F1_fft.shape[1]):
                
                    i_row+=1
                    fsr=sample0+i_row*fft_size
                    lsr=fsr+fft_size

                    
                    if DEBUG_DELAYS:
                        print_debug_r_delays_f(stpol,F_refs[stpol],F_ind[stpol],fsr,num_samples,len(timescale),\
                                     total_timescale[row],0.0,\
                                     total_seconds_offset,r_recalc[row],r_unused,a_unused,fractional_sample_correction,\
                                     full_fractional_recalc[row],fractional_recalc[row],diff_frac)
                        
                    
                    computed=0
                    if last_fractional_recalc!=fractional_recalc[row] or first_iteration_recalc:
                        if SAVE_TIME_ROTATIONS:
                            first_iteration_recalc=0
                        computed=1

                        #print(str_st)
                        #print(freqscale2*(fractional_recalc[row]))

                        [fr6,nr]=get_exp(freqscale2*(fractional_recalc[row]))
                        
                        if not(nr):
                            #frtot=get_rotator([fr6])
                            frtot=fr6
                            frtot_v.append([frtot])
                        else:
                            frtot_v.append([1.0])
                    else:
                        # Skipping, copy last value
                        if not(nr):
                            frtot_v.append([frtot])
                        else:
                            frtot_v.append([1.0])

                    last_fractional_recalc=fractional_recalc[row]
                
            last_str_st=str_st   
            

            for row in range(F1_fft.shape[1]):
                if not nr:
                    try:
                        np.multiply(F1_fft[stpol,row,:],frtot_v[row][0],F1_fft[stpol,row,:])
                    except IndexError:
                        print("Error in rotation: "+str(len(frtot_v))+", "+str(F1_fft.shape))

            if DEBUG_DELAYS: 
                print("zR"+KEY_SEP+"f  "+str(stpol).rjust(5)+str(F_refs[stpol]).rjust(8)+F_ind[stpol].rjust(8)+\
                              str(fsr).rjust(10)+str(num_samples).rjust(10) +\
                              " C,R >>>>".ljust(191)+str(computed).rjust(3)+str(int(not(nr))).rjust(3))
            if error_f_frac==0:
                if DEBUG_FRAC_OVER:
                    # TO DO: create functions in lib_debug(?)
                    print("zR"+KEY_SEP+"o".ljust(35)+str(stpol).rjust(5)+str(F_refs[stpol]).rjust(8)+F_ind[stpol].rjust(8)+\
                          str(fsr).rjust(10)+str(num_samples).rjust(10)+str(len(timescale)).rjust(10)+\
                          str(timescale[0]).rjust(16)+str(r_recalc[0]).rjust(20)+\
                          str(full_fractional_recalc[row]).rjust(20)+\
                          str(fractional_recalc[row]).rjust(20)+\
                          #str(frac_re).rjust(10)+\
                          #str(total_frac_delay_int).rjust(10)+\
                          "".rjust(10)+\
                          #str(clock_frac_delay_diff).rjust(20)+\
                          "".rjust(20)+\
                          #str(clock_frac_delay_int).rjust(10))
                          "".rjust(10))
            else:
                if DEBUG_FRAC_OVER:
                    print("zR"+KEY_SEP+"o  "+"error")

            # Correction for pcal
            F_pcal_fix_out.append(shift_delay)  
            
         
            
    else:
        
        F_first_sample_out=F_first_sample
        #for stpol in range(F1_fft.shape[0]):
        #    for row in range(F1_fft.shape[1]):
        #        F1_fft_rot[stpol,row,:]=F1_fft[F_refs[stpol],row,:]
        print("zR\tWarning: no rotation: first sample "+str(F_first_sample))
    
    if (len(F_pcal_fix)>=len(F_pcal_fix_out))or(error_f_frac==1):
        F_pcal_fix_out=F_pcal_fix #[:]
    

    #F2_fft_rot = np.conj(F1_fft_rot)
    if DEBUG_DELAYS or DEBUG_LIB_DELAY: #or DEBUG_FRAC_OVER :
        print("zR"+KEY_SEP+"oj".ljust(20)+str(len(F_adj_shift_partial_out))+"  "+\
              ','.join(map(str,F_adj_shift_partial_out))+"  "+\
              "   mon "+','.join(map(str,F_adj_shift_partial_mon)))
        print("zR"+KEY_SEP+"---------------")
        
    return([F1_fft,None,F_adj_shift_partial_out,F_adj_shift_pcal_out,F_pcal_fix_out,F_first_sample_out])



def compute_x_all(F1_fft,F2_fft,count_acc,acc_mat,index_scaling_pair=-1,dtype_complex=complex):
    """
    Compute multiply-accumulate for all baselines (all-baselines-per-task-mode)
    
    Parameters
    ----------
     F1_fft : list of np arrays of arrays 
         FFTs (left term). 
     F2_fft : list of np arrays of arrays with
         FFTs (right term)If None, then it will take the conjugate of F1_fft.
     count_acc
         number of accumulations performed previously during this integration period.
     acc_mat
         accumulation matrix with results accumulated during this integration period.
     index_scaling_pair
         -1 for all-baselines-per-task mode, positive integer for other modes.
     dtype_complex
         type to initialize accumulation matrix.
     
    Returns
    -------
     acc_mat
         accumulation matrix with the new results accumulated (3D numpy array with one row (and one column)
                      per station-polarization, and fft_size layers/pages with the results of the accumulation 
                       for each pair). Note that the first two dimensions of the matrix are upper triangular, 
                       with the auto-correlations in the main diagonal.
     count_acc
         total number of accumulation performed including those in this call.
     count_sub_acc
         number of accumulation performed only in this call.
     n_sp
         number of station-polarizations (that is also the number of rows/columns of the matrix.
     
    Notes
    -----
    |
    | **TO DO:**
    |
    |  Add counters for invalid data.
    """
    fft_size_comp=F1_fft.shape[2]
    
    if F2_fft is None:
        F2_fft = np.conj(F1_fft)
    n_sp=len(F1_fft)
    count_acc+=len(F1_fft[0])  
    count_sub_acc=len(F1_fft[0])
    if index_scaling_pair==-1:
        #All baselines per task
        if acc_mat is None:
            acc_mat=np.zeros([n_sp,n_sp,fft_size_comp],dtype=dtype_complex)
        for i1 in range(n_sp):
            acc_mat[i1,i1:]+=np.sum(np.multiply(F1_fft[i1,:],F2_fft[i1:,:]),axis=1)
    else:
        # Linear-scaling-stations
        if acc_mat is None:
            acc_mat=np.zeros([n_sp,fft_size_comp],dtype=dtype_complex)
        acc_mat+=np.sum(np.multiply(F1_fft[index_scaling_pair,:],F2_fft),axis=1)
    
    return([acc_mat,count_acc,count_sub_acc,n_sp])





def shortest_row_F(F1):
    """
    Get the minimum and maximum length of the elements of F1.
    
    Parameters
    ----------
     F1 : list of numpy 1D arrays.
     
    Returns
    -------
     shortest_row
         length of the shortest element in F1.
     longest_row
         length of the longest element in F1.
    """
    shortest_row=-1
    longest_row=-1
    for i in range(len(F1)):
        if (shortest_row<0)or(len(F1[i])<shortest_row):
            shortest_row=len(F1[i])
        if (shortest_row<0)or(len(F1[i])>longest_row):
            longest_row=len(F1[i])
    return([shortest_row,longest_row])


       

def cut_remainder_fft_size_multiple(F_partial,fft_size_multiple):
    """
    Function to migrate functionality from np arrays to list of arrays (for delay correction...).
    It divides an array into two, one of them with rows of length fft_size_multiple, the other with the remainder.
    
    Parameters
    ----------
     F_partial : list of numpy arrays
         samples in the time domain, with no rotation applied yet, and wherein each of its elements may have a different length.
     fft_size_multiple : int
         maximum integer multiple of fft_size that is lesser than the length of the shortest element of F_partial.
    
    Returns
    -------
     F_partial_out
         F_partial (input) truncated to the ff_size_multiple first elements.
     F_partial_rem
         Remainder of removing F_partial_out from F_partial.
    """
    F_partial_rem=[]
    F_partial_out=[]
    for i in range(len(F_partial)):
        F_partial_rem.append(np.delete(F_partial[i],np.s_[:fft_size_multiple]))
        F_partial_out.append(np.delete(F_partial[i],np.s_[fft_size_multiple:]))
    return([F_partial_out,F_partial_rem])


def compute_fx_for_all(F1_partial,F_ind_partial,F1,fft_size,windowing,acc_mat,count_acc,normalize_after_compute=False,F_ind=None,\
                       last_F_ind=None,n_sp=0,failed_acc_count=0,dismissed_acc_count=0,scaling_pair="A.A",dtype_complex=complex,\
                       acc_pcal=None,pre_pcal=None,n_bins_pcal=0,count_acc_pcal=0,phase_calibration=None,\
                       bypass_fx=0,F_delays=[],F_rates=[],F_fs=[],freq_channel=0.0,F_first_sample=[],F_first_sample_partial=[],\
                       F_frac=[],block_time=0.0,F_adj_shift_partial=[],F_stack_shift=[],F_adj_shift_pcal=[],F_stack_shift_pcal=[],\
                       F_pcal_fix=[],F_side=[],F_lti=[]):
    """
    Fringe rotation, FFTs, fractional sample correction for all station-polarizations, and 
         cross multiplication and accumulation for all baseline (all-baselines-per-task-mode)
    
    Parameters
    ----------
     F1_partial
         list of previously stored samples.
     F_ind_partial
         station-polarization identifiers for the elements of F1_partial.
     F1
         new samples.
     fft_size
         number of coefficients fo the FFT.
     windowing
         shape of window prior to FFT (square by default).
     acc_mat
         accumulation matrix with provisional results (see compute_x_all for acc_mat description).
     count_acc
         number of accumulations performed so far.
     normalize_after_compute
         whether or not to normalize the results (to be activated in the last call in the integration period).
     F_ind
         station-polarization identifiers for the elements of F1.
     last_F_ind
         saved value of F_ind in the previous iteration.
     n_sp
         number of station-polarizations.
     failed_acc_count
         counter with number of failed accumulations. 
     dismissed_acc_count
         counter with number of dismissed accumulations.
     scaling_pair
         mode of operation: "A.A" for all-baselines-per-task (default).
     dtype_complex
         complex type used for initialization of numpy arrays.
     acc_pcal
         provisional results for accumulated phase calibration.
     pre_pcal
         stored samples to be used in accumulation of phase calibration.
     n_bins_pcal
         number of samples for the windows to be accumulated for the pcal signal.
     count_acc
         number of accumulations performed previously during this integration period.
     phase_calibration
         whether or not to extract phase calibration tones.
     bypass_fx
         if 1, it will not do computations for cross-multiplication and accumulation.
     F_delays
         [unused] absolute delays for each of the streams in F1.
     F_rates
         delay information for each of the streams in F1.
     F_fs
         sampling frequency for each of the streams in F1.
     freq_channel
         sky frequency.
     F_first_sample
         first sample for each of the streams in F1.
     F_first_sample_partial
         first sample for each of the streams in the stored samples.
     F_frac
         fractional and integer sample delay for each of the streams in F1.
     block_time
         time for the current accumulation period.
     F_adj_shift_partial
         [unused] previously used to keep track of the samples to add/drop due to fractional sample 
             correction overflow (now using F_frac_over).
     F_stack_shift
         [unused] previously used to keep track of the samples added/droped due to fractional sample 
              correction (now using F_frac_over).
     F_adj_shift_pcal
         [unused] previously used to keep track of the samples to roll in the pcal signal.
     F_stack_shift_pcal
         [unused] equivalent to F_stack_shift for pcal.
     F_pcal_fix
         number of samples to roll in the pcal accumuled signal.
     F_side
         single side band side corresponding to the streams in F1.
     F_lti
         list of last, total, invalid samples for each stream
     
    Returns
    -------
     All output variables are udpated versions of those use as input, see procedure below for details.
     | acc_mat
     | count_acc
     | count_sub_acc
     | n_sp
     | last_F_ind
     | failed_acc_count
     | dismissed_acc_count
     | F1_partial
     | F_ind_partial
     | acc_pcal
     | pre_pcal
     | count_acc_pcal
     | F_first_sample_partial
     | F_adj_shift_partial
     | F_stack_shift
     | F_adj_shift_pcal
     | F_stack_shift_pcal
     | F_pcal_fix_out
     | F_lti
    
    Notes
    -----
    |
    | **Procedure:**
    |
    | All-stations-per-tasks:
    |  1. Concatenate samples (list of np arrays) for phase calibration computations.
    |  2. Check for overflow in fractional sample correction in new samples.
    |  3. Correct overflow in fractional sample correction (adding/dropping the required samples) in new samples.
    |  4. Concatenate new samples into stored samples.
    |  5. If possible separate the stored samples into two parts: the first part with a number of samples that is a multiple
    |        of the fft size, and the remained in the second part.
    |  6.  Correct fringe rotation, FFT, correct fractional sample delay and cross-multiply, results go to accumulation matrix.
    |  7. Leave unprocessed samples in F1_partial (stored samples).
    |
    |
    | **Conventions:**
    |
    | failed accumulation: an accumulation is considered failed if the elements for the new samples do not match those stored.
    | dismissed accumulation: an accumulation is considered dismissed if it is not computed becaused there are not enough samples.
    | variables ending in *_partial correspond to stored samples.
    | F_refs is used to point to new samples when iterating through stored samples. Note that F1, F_frac, so e.g. 
    |        F_ind[F_refs[i]]==F_ind_partial[i] for i in range(F_ind_partial).
    |
    |
    | **Configuration for trade-off performance vs. memory requirements:**
    |
    |  Use the variable COMPUTE_FOR_SUB_ACC_PERIOD in const_mapred.py to configure the number of times the new samples are simply
    |     stored without further computation. E.g. if =100, computations will be by-passed 99 out 100 times before the end 
    |     of the integration period. This allows to take advantage of increased efficiency with long numpy arrays, and also
    |     to avoid some repeated computations in fringe_rotation() and compute_f_all().
    | 
    |
    | **Limitations:**
    |
    |  Curently number of bins for pcal has to be the same for all stations.
    |
    |
    | **TO DO:**
    |
    |  Add support for different numbers of bins?
    |  Check  if not(not_enough_data):, may be problematic with samples left out.
    """
    
    #if phase_calibration==0:
    #    phase_calibration=None
    
    F_pcal_fix_out=[]
    #if F_first_sample_partial==[]:
    #    F_first_sample_partial=F_first_sample[:]
    
    #new_records=len(F_first_sample)-len(F_first_sample_partial)
    #if new_records>0:
    #    for i in range(new_records
        

    # TO DO: this assumes that if more records are received, then none processing has been done, and therefore the first sample is still zero for all streams 
    # TO DO: Check this
    if len(F_first_sample_partial)<len(F_first_sample):
        #F_first_sample_partial=[0]*len(F_first_sample)
        F_first_sample_partial=np.copy(F_first_sample)
    


    if DEBUG_DELAYS or DEBUG_LIB_DELAY: # or DEBUG_FRAC_OVER :
        if F1!=[]:
            n_max=np.max(map(len,F1))
            n_min=np.min(map(len,F1))
            n_delta=n_max-n_min
            print("zR"+KEY_SEP+"pre-hsp: stpols "+str(F_ind_partial)+" with "+\
              ','.join(map(str,map(len,F1)))+" samples, delta_n is  "+str(n_delta))

    count_sub_acc=0
    
    # will reset F1 and F_ind at the end
    reset_inputs=0
    reset_pcal=0
    F1_partial_rem=None
    pre_pcal_rem=None

    not_enough_data=0

    if last_F_ind is not None:
        if len(last_F_ind)>len(F_ind):
            # TO DO: check if it's possible to still do computations for stored samples
            # Data not available for all stations (probably due to these being the last samples after alignment, will discard them)
            dismissed_acc_count+=1
            not_enough_data=1

    # TO DO: allow to pass for last iteration, but do not include new samples?

    if not(not_enough_data):


        # Stack samples from (F1_partial and F1)
        #if phase_calibration is not None:
        if phase_calibration>0:
            # Do not drop/add samples for phase calibration
            # TO DO: currently updated based on clock model (?)
            [pre_pcal,F_ind_unused,F_refs,F_stack_shift_pcal,F_lti_unused,F_unused]=hstack_new_samples(pre_pcal,F_ind_partial,\
                                                    F_ind,F1,F_adj_shift_pcal,F_stack_shift_pcal,F_lti,F_first_sample,"pcal",[])
        
        # Note:
        #  F_first_sample is for F1 (new samples)
        #  F_first_sample for F1_partial (stored samples)
        # Get locations of samples to be added/dropped (updates in fractional sample correction)
        F_frac_over_ind=get_frac_over_ind(F_first_sample,F1,F_rates,F_fs,F_ind,F_side)
        
        # Apply changes to new data
        F1=fix_frac_over(F1,F_frac_over_ind,F_ind,F_first_sample)
        
        # Store new data on previous structures, relocating based on previous ordering
        [F1_partial,F_ind_partial,F_refs,F_stack_shift,F_lti,F1]=hstack_new_samples(F1_partial,F_ind_partial,F_ind,F1,\
                                                F_adj_shift_partial,F_stack_shift,F_lti,F_first_sample,"f",F_frac_over_ind)
        
 
        index_scaling_pair=-1
        if scaling_pair!="A.A":
            # Linear scaling stations
            index_scaling_pair=F_ind.index(scaling_pair)
        
        #if (last_F_ind is None)or(last_F_ind==F_ind):
        if (last_F_ind is None)or(last_F_ind.sort()==F_ind_partial.sort()):
            #last_F_ind=F_ind
            last_F_ind=F_ind_partial[:]
            
            
            # pcal first
            if bypass_fx==0:
                #if phase_calibration is not None:
                if phase_calibration>0:
                    
                    [shortest_row_pre_pcal,longest_row_pre_pcal] = shortest_row_F(pre_pcal)
                    if shortest_row_pre_pcal>=n_bins_pcal:
                        if longest_row_pre_pcal>n_bins_pcal:
                            pcal_size_multiple = int((shortest_row_pre_pcal//n_bins_pcal)*n_bins_pcal)
                            [pre_pcal,pre_pcal_rem]=cut_remainder_fft_size_multiple(pre_pcal,pcal_size_multiple)
                        else:
                            pre_pcal_rem=None
                        count_acc_pcal+=int(shortest_row_pre_pcal//n_bins_pcal)
                        acc_pcal = accumulate_pcal_all(acc_pcal,pre_pcal,n_bins_pcal)
                        reset_pcal=1
             
            if bypass_fx==0:
                
                
                if DEBUG_DELAYS  or DEBUG_LIB_DELAY: #or DEBUG_FRAC_OVER:
                    n_max=np.max(map(len,F1_partial))
                    n_min=np.min(map(len,F1_partial))
                    n_delta=n_max-n_min
                    print("zR"+KEY_SEP+"hsp: stpols "+str(F_ind_partial)+" with "+\
                      ','.join(map(str,map(len,F1_partial)))+" samples, delta_n is  "+str(n_delta))
                
                [shortest_row_F1_partial,longest_row_F1_partial] = shortest_row_F(F1_partial)
                if shortest_row_F1_partial>=fft_size:
                    # Enough samples to do the processing            
                    fft_size_multiple=1
                    if longest_row_F1_partial>fft_size:
                        # Too many samples, save for next acc block
                        fft_size_multiple=int((shortest_row_F1_partial//fft_size)*fft_size)
                        [F1_partial,F1_partial_rem]=cut_remainder_fft_size_multiple(F1_partial,fft_size_multiple)
                    else:
                        F1_partial_rem=None
                    
                    if fft_size_multiple>0:
                        
                        [F1_partial,F_first_sample_partial] = fringe_rotation(F1_partial,F_first_sample_partial,\
                                                       F_rates,freq_channel,F_fs,F_delays,F_refs,block_time,F_frac,\
                                                       F_adj_shift_partial,F_side,F_ind_partial,F_lti)

                        # This includes corrections in frequency domain (fractional sample...)
                        [F1_fft,F2_fft,F_adj_shift_partial,F_adj_shift_pcal,\
                                    F_pcal_fix_out,F_first_sample_partial] = compute_f_all(F1_partial,fft_size,windowing,\
                                                                         dtype_complex,F_frac,F_fs,\
                                                                      F_refs,freq_channel,F_first_sample_partial,F_rates,\
                                                                      F_pcal_fix,F_side,F_ind_partial,F_lti)
                        
                        [acc_mat,count_acc,count_sub_acc,n_sp] = compute_x_all(F1_fft,F2_fft,count_acc,acc_mat,\
                                                                    index_scaling_pair,dtype_complex)

                        reset_inputs=1

                        #    #TO DO?: resort if necessary based on F_ind and F_ind_partial...
                
        else:
            failed_acc_count+=1
        
        #if normalize_after_compute:
        #    acc_mat = normalize_mat(acc_mat,count_acc)
            
        if reset_pcal:
            if pre_pcal_rem is None:
                pre_pcal=np.array([])
            else:
                pre_pcal=np.copy(pre_pcal_rem)
        
        if reset_inputs:
            if F1_partial_rem is None:
                F1_partial=np.array([])
                #F_ind_partial=[]
            else:
                F1_partial=np.copy(F1_partial_rem)

    if normalize_after_compute:
        if acc_mat is not None:
            acc_mat = normalize_mat(acc_mat,count_acc)
        else:
            failed_acc_count+=1


    if (len(F_pcal_fix)>=len(F_pcal_fix_out)):
        F_pcal_fix_out=F_pcal_fix #[:]

    return([acc_mat,count_acc,count_sub_acc,n_sp,last_F_ind,failed_acc_count,dismissed_acc_count,F1_partial,F_ind_partial,acc_pcal,pre_pcal,count_acc_pcal,F_first_sample_partial,F_adj_shift_partial,F_stack_shift,F_adj_shift_pcal,F_stack_shift_pcal,F_pcal_fix_out,F_lti])
                    
    
    

# <codecell>


