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
#File: lib_quant.py.
#Author: A.J. Vazquez Alvarez (ajvazquez@haystack.mit.edu)
#Description: 
"""
Module with basic quantizer and dequantizer. 

"""
#History:
#initial version: 2015.12 ajva
#MIT Haystack Observatory

from __future__ import print_function
import numpy as np
import imp

# Quantization levels
import const_quant
imp.reload(const_quant)
from const_quant import *


def compute_range_bits(bits_per_sample):
    """
    Compute list of values for unpacking samples.
    [Used by decode_samples_red()]
    
    Notes
    -----
    |
    | **Example:**
    |
    |  >>bits_per_sample=2
    |  >>list(compute_range_bits(bits_per_sample))
    |   [6, 4, 2, 0]
    """
    return(reversed(range(0,8,bits_per_sample)))

def decode_samples_red(words,bits_per_sample):
    """
    Unpack samples given a binary vector with the packed samples and the number of bits per sample.
    [Used by sub_unpack_samples()]
    
    Parameters
    ----------
     words
     bits_per_sample : int
         number of bits per sample.
     
    Returns
    -------
     words_nostack : numpy 1D array
         unpacked samples.
    """
    
    range_offsets = compute_range_bits(bits_per_sample)
    mask_bits = (1<<bits_per_sample)-1
    words_extended = [((words)>>i) & (mask_bits) for i in range_offsets]
    #words_extended = [np.bitwise_and(np.right_shift(words,i),mask_bits) for i in range_offsets]
    words_extended2 = np.concatenate(words_extended)
    words_nostack = np.reshape(words_extended2,(-1,len(words))).transpose(1,0).flatten()
    return(words_nostack)


def np_take_samples(v_2bps,unpacked_samples):
    """
    Translate vector with sampled values into dequantized values.
    [Used by sub_unpack_samples()]
    
    Parameters
    ----------
     v_2bps : list
         quantization values.
     unpacked_samples
         quantized samples.
     
    Returns
    -------
     out : numpy 1D array
         dequantized sample components.
    """
    return(np.take(v_2bps,unpacked_samples))


def sub_unpack_samples(samples_quant,bits_per_sample,current_data_type,v_2bps_complex,v_2bps_real):
    """
    Unpack samples packed in a binary vector into dequantized values.
    
    Parameters
    ----------
     See get_samples().
     
    Returns
    -------
     out : numpy 1D array of complex
         dequantized samples.
    """
    unpacked_samples = decode_samples_red(samples_quant,bits_per_sample)
    if current_data_type=='c':
        samples_out = np_take_samples(v_2bps_complex,unpacked_samples)
    else:
        samples_out = np_take_samples(v_2bps_real,unpacked_samples)

    return(samples_out)


def sub_pack_complex_samples(samples,bits_per_sample):
    """
    Group dequantized values into complex values (pair where first one is real part, second one imaginary part).
    
    Parameters
    ----------
     samples : numpy 1D array
         samples components (complex): [real_part_0, imag_part_0, real_part_1, ... ].
     bits_per_sample : int
         number of bits per sample.
    
    Returns
    -------
     complex_samples : numpy 1D array
         complex samples: [complex_0, complex_1, ...]
     
    Notes
    -----
    |
    | **TO DO:**
    |
    |  Currently only 2 bits per sample.
    """
    if bits_per_sample==1:
        # TO DO: This branch is untested...
        samples[samples==0]=-1
        complex_samples = samples[::2]+1j*samples[1::2]
    else:
        samples[1::2] *= 1j 
        samples_reshape=samples.reshape(-1,2)
        complex_samples = np.sum(samples_reshape,axis=1)
      
    return(complex_samples)


def get_samples(samples_quant,bits_per_sample,current_data_type,num_samples=-1,single_precision=0):
    """
    Get dequantized samples from samples in binary format.
    
    Parameters
    ----------
     samples_quant : numpy 1D array
         samples (components if complex), packed in binary format (uint8).
     bits_per_sample : int
         number of bits per sample.
     current_data_type : char {'c','r'}
         'c' for complex, 'r' for real data.
     num_samples : int
         number of samples to be returned (first num_samples). This is used to avoid errors if the number
                           samples is smaller than the number of samples fitting a uint8.
     v_2bps_complex : list
         quantization levels for 2 bit complex.
     v_2bps_real : list
         quantization levels for 2 bit real.
     
    Returns
    -------
     result : numpy 1D array
         complex with dequantized samples.
     
    Notes
    -----
    |
    | **TO DO:**
    | 
    |  Currently only 2 bits per sample, generalize for different numbers of bits per sample.
    """
    
    #v_2bps_complex=np.array(QUANT_LEVELS_2BIT,dtype=np.complex64) if SINGLE_PRECISION else \
    #                   np.array(QUANT_LEVELS_2BIT,dtype=complex)
    v_2bps_complex=np.array(QUANT_LEVELS_2BIT,dtype=np.complex128) if single_precision else \
                        np.array(QUANT_LEVELS_2BIT,dtype=np.complex128)
    #v_2bps_real=np.array(QUANT_LEVELS_2BIT,dtype=np.float32) if SINGLE_PRECISION else \
    #                    np.array(QUANT_LEVELS_2BIT,dtype=float)
    v_2bps_real=np.array(QUANT_LEVELS_2BIT,dtype=np.complex128) if single_precision else \
                        np.array(QUANT_LEVELS_2BIT,dtype=np.complex128)
    
    
    # Unpack and dequantize
    v_all = sub_unpack_samples(samples_quant,bits_per_sample,current_data_type,v_2bps_complex,v_2bps_real)
    if current_data_type=='c':
        result = sub_pack_complex_samples(v_all,bits_per_sample)
    else:
        result = v_all
    if num_samples>-1:
        #if current_data_type=='c':
        #    result=result[:(num_samples//2)]
        #else:
        result=result[:num_samples]
    return(result)






# -----------------------------------------------------------------------------
# -                  First implementation, use only for testing.              -
# -                  Used by signal generation libraries...
dictc01={"0":-1,\
          "1":1}
dict1={0:-1,\
          1:1}
dictc01bit={0:-1,\
          1:1}
dict2bps={0:-3.336,\
          1:-1.0,\
          2:1.0,\
          3:3.336}


def simple_quantizer(samples,bits_quant=1,signal_limits=[-1,+1],force_limits=0):
    """
    Basic quantizer: it takes as input a list of samples and returns a list of the same size
        with integers from 0 to 2**bits_quant - 1, representing the quantization thressholds,
        distributed at equal intervals between teh minimum and the maximum of the signal.
            
        Example:
            signal = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
            bits_quant = 2
            output=simple_quantizer(signal,bits_quant)
            output = [0, 0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 3]
            
        To be done:
           -Introduce other quantization strategies
           
    (!) Use only for testing.
    (!) TO DO: migrate code using this function to newer implementation.
    """

    

    #bits_quant=1

    levels=2**bits_quant
    ths=levels-1

    if force_limits==0:
        max_s = max(samples)
        min_s = min(samples)
    else:
        min_s = signal_limits[0]
        max_s = signal_limits[1]

    
    values_v=np.linspace(min_s,max_s,levels)[:]
    ths_v_plus=[]
    for i in range(levels)[1:]:
        ths_v_plus.append(0.5*(values_v[i]+values_v[i-1]))
    ths_v_plus.append(values_v[-1])
    ths=ths_v_plus

    q_samples = []
    for i in samples:
        for j in range(levels):
            #If smaller than threshold or last one (will be last if outside of range)
            if (i<=ths_v_plus[j])or(j==levels-1):
                #q_samples.append(values_v[j])
                q_samples.append(j)
                break
    return(q_samples)


def simple_dequantizer(signal=[],bits_quant=1,limits=[-1,1]):
    """
    Basic dequantizer: it takes as input a list of quantized samples (output of simple_quantizer())
        and returns a list of the same size with float values from limits[0] to limits[1], representing 
        the quantization thressholds, distributed at equal intervals between these limits
    
        Example:
            signal = [0, 0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 3]
            bits_quant = 2
            output=simple_dequantizer(signal,bits_quant)
            output = [-0.75, -0.75, -0.75, -0.75, -0.25, -0.25, -0.25, 0.25, 0.25, 0.25, 0.75, 0.75, 0.75]
            
    (!) Use only for testing.
    (!) TO DO: migrate code using this function to newer implementation.
    """
    

    if bits_quant==1:
        return([dict1[x] for x in signal])
    
    
    levels = 2**bits_quant
    
    max_s=limits[1]
    min_s=limits[0]
    #values_v=(np.linspace(min_s,max_s,levels+1))[1:]-((max_s-min_s)/(2*levels))
    
    values_v=np.linspace(min_s,max_s,levels)
    

    # Don't convert to list, array is user later, so the line below has better performance
    return(values_v[signal])

def group_pairs_complex(samples):
    """
    From a given list of samples, group into pairs of 2 (real and imaginary).
    """
    samples_pairs=list(zip(samples[0::2],samples[1::2]))
    samples_out=[scp[0]+np.complex("j")*scp[1] for scp in samples_pairs]
    return(samples_out)

# <codecell>


