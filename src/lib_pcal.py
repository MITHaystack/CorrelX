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
#File: lib_pcal.py.
#Author: A.J. Vazquez Alvarez (ajvazquez@haystack.mit.edu)
#Description: 
"""
Phase calibration routines.

"""
#History:
#initial version: 2016.10 ajva
#MIT Haystack Observatory

from __future__ import print_function
import fractions
import numpy as np
import imp



# Map routines.

def fr_gcd(a,b): return(fractions.gcd(a,b))


def get_pcal_ind(freqs,sidebands,fs,f_pcal,o_pcal):
    """
    Compute window length for phase calibration and get tone seperation.
    
    Parameters
    ----------
     freqs : list of float
         start frequency for the bands [Hz].
     sidebands : list of str
         sideband identifier (L/U).
     fs : int
         sampling frequency [Hz].
     f_pcal : int
         frequency separation for phase calibration tones [Hz].
     o_pcal : int
         frequency offset for phase calibration tones [Hz].
     
    Returns
    -------
     n_bins_pcal
         number of samples in pcal window.
     f_pcal
         pcal tone separation [Hz].
    
    Notes
    -----
    |
    | **Limitations / TO DO:**
    |
    |  Currently only LSB, add USB.
    |  Change int for float.
    |  Offset untested.
    """
        
    # Currently only LSB
    sideband_sign = -1
        

    freqs_fs = [fs]*len(freqs)
    freqs_bw = [fs/2]*len(freqs)
    freqs_offsets = np.subtract(freqs,freqs_bw)
    fsplit_v = []
    for i in range(len(freqs_fs)):
        fsplit_v.append(int(fr_gcd(freqs_fs[i],fr_gcd(freqs_offsets[i],f_pcal))))

    n_bins_pcal = np.divide((fs),fsplit_v)
    
    # TO DO: complex
    #n_bins_pcal=n_bins_pcal//2
    
    return([n_bins_pcal,f_pcal])


# Reduce routines.

def normalize_pcal(pcal_fft,count_acc_pcal):
    """
    Normalize the FFTs coefficients containing the phase calibration accumulations.
    
    Notes
    -----
    |
    | **TO DO:**
    |
    |  This needs to be corrected.
    """
    # TO DO: wrong, fix
    #return(pcal_fft/len(pcal_fft))
    return(pcal_fft/(len(pcal_fft)*count_acc_pcal))





def reshape_pcal(v1_pcal,n_bins_pcal):
    """
    [Used in one-baseline-per-task mode.]
    
    Notes
    -----
    |
    | **TO DO:
    |
    |  Remove and merge implementation with all-baslines-per-task.
    """
    # TO DO: check why fft is only in one of the conditionals below...
    num_chunks1=len(v1_pcal)//n_bins_pcal
    if num_chunks1==1:
        v1pcal=np.array([v1_pcal])
    else:
        # TO DO: add rfft for real data (?)
        v1pcal=scfft.fft(np.reshape(v1_pcal,(-1,n_bins_pcal)))
    return(v1pcal)

def accumulate_pcal(acc_pcal,v1pcal,count_acc_pcal):
    """
    [Used in one-baseline-per-task mode.] 
    """
    count_acc_pcal+=len(v1pcal)
    if len(acc_pcal)==0:
        if len(v1pcal)==1:
            acc_pcal=v1pcal[0]
        else:
            acc_pcal=np.sum(v1pcal,axis=0)
    else:
        if len(v1pcal)==1:
            acc_pcal+=v1pcal[0]
        else:
            acc_pcal+=np.sum(v1pcal,axis=0)
    return([acc_pcal,count_acc_pcal])




def accumulate_pcal_all(acc_pcal,pre_pcal,n_bins_pcal,dtype_complex=complex):
    """
    Accumulate pcal window (all-baselines-per-task mode).
    """
    #n_sp=pre_pcal.shape[0]
    n_sp=len(pre_pcal)
    reshaped_pre_pcal = np.reshape(pre_pcal,(n_sp,-1,n_bins_pcal))
    if (len(acc_pcal)==0)or(acc_pcal.shape[0]==0):
        acc_pcal=np.zeros([n_sp,n_bins_pcal],dtype=dtype_complex)
    acc_pcal+=np.sum(reshaped_pre_pcal,axis=1)
    return(acc_pcal)


def adjust_shift_acc_pcal(acc_pcal,F_pcal_fix,bypass_adjust_pcal=0,v=0):
    """
    Remove delay shift due to delay model.
    
    Parameters
    ----------
     acc_pcal
         accumulation matrix for the phase calibration signals.
     F_pcal_fix
         number of samples to roll each of the arrays in acc_pcal.
     bypass_adjust_pcal
         0 by default.
     v
         verbose if 1.
    
    Returns
    -------
     acc_pcal_out
         acc_pcal with the rolls (circular shift) applied as defined in F_pcal_fix.
    
    Notes
    -----
    |
    | **TO DO:**
    |
    |  Asses error raised from this correction. It should be inside a sample in phase.
    |  Check that corrections are applied to the proper row (F_refs...).
    """
    if bypass_adjust_pcal==0:
        n_bins=acc_pcal.shape[1]
        acc_pcal_out=np.zeros(acc_pcal.shape,dtype=complex)
        R_pcal=[]
        if F_pcal_fix!=[]:
            for i in range(acc_pcal.shape[0]):
                if i<len(F_pcal_fix):
                    # TO DO: check
                    #
                    #roll_pcal=int(n_bins-(F_pcal_fix[i]%n_bins))
                    roll_pcal=int((F_pcal_fix[i]%n_bins))
                    if i==0:
                        R_pcal=[roll_pcal]
                    else:
                        R_pcal+=[roll_pcal]
            max_roll=np.max(R_pcal)
            #R_pcal=np.subtract(max_roll,R_pcal)
            for i in range(acc_pcal.shape[0]):
                if i<len(F_pcal_fix):
                    if v==1:
                        print("zR\troll_pcal "+str(i)+": "+str(R_pcal[i]))
                    acc_pcal_out[i] = np.roll(acc_pcal[i],R_pcal[i])[:]
        else:
            acc_pcal_out=acc_pcal
    else:
        acc_pcal_out=acc_pcal
    return(acc_pcal_out)


