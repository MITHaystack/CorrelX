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
#File: lib_vdif.py.
#Author: A.J. Vazquez Alvarez (ajvazquez@haystack.mit.edu)
#Description: 
"""
Module with basic functions to read and write VDIF headers and data.

| -- VDIF Specification version [VD01]--
|  VLBI Data Interchange Format (VDIF) Specification
|    Release 1.0 (w/24 August 2009 corrections/clarifications highlighted)
|    Ratified 26 June 2009
|

Notes
------
| -bitarray library still used for VDIF file generation.
|  Updated 2015.10.15:
|  -frame length (//8 when writing header, *8 when reading header)
|  -changed endiannes to be correct
|  2015.12.03: changed bits2int(bitsin)
|  2015.12.03: read_samples is now more efficient, but may not work with numbers of bits per sample that do not divide 32.
|  TO DO: implement in "read_samples" the functionality to read bits per sample that do not divide 32.
|  TO DO: testing on epoch and seconds conversions.
|  2016.09.21: bitarray reader implementation removed in favor of raw which is faster.

"""
#History:
#initial version: 2015.12 ajva
#MIT Haystack Observatory

from __future__ import print_function
import select
import array
import os
import sys

USE_BITARRAY=0
if USE_BITARRAY:
    from bitarray import bitarray                   # Enable for VDIF creation functions (testing)
import struct
import numpy as np
from datetime import date,datetime,timedelta

# Constants for VDIF reader
TYPE_WORD=np.uint32                              # Data type for binary file reader.
WORD_SIZE=32                                     # 32 bits per word. This is tied to TYPE_WORD.
WORD_SIZE_BYTES=WORD_SIZE//8                     # 4 bytes per word.
HEADER_VDIF_WORDS=8                              # 8 words (VDIF header).
HEADER_BYTES=WORD_SIZE_BYTES*HEADER_VDIF_WORDS   # 32 bytes (VDIF header).

# Constants for bitarray implementation (used in frame writer)
ENDIAN_STRUCT_READING = ">I"                     # Struct endian.
ENDIAN_STRUCT = ">I"                             # < for little endian, > for big endian, I for unsigned int (4 bytes).
ENDIAN_BITARRAY = 'big'                          # Bitarray endian.

# Low level configuration for bitarray implementation (used in frame writer)
#  Use 'L' (min size 4 bytes) or 'I' (min size 2 bytes) for integer
#  https://docs.python.org/2/library/array.html
#  With 'L' it works in Python3, but in Python2 the words seem to be extended to 64 bytes, padded with zeros.
#  'I' seems to works well in both Python2 and Python3
#LOW_LEVEL_WORD='L'
LOW_LEVEL_WORD='I'

REF_YEAR=2000
#REF_J2000=51544.5
REF_J2000=51544
SIX_MONTH_DAYS=365/2
SECONDS_DAY=24*60*60

# Masks for read_header_vdif_from_raw
MASK_1  = 1
MASK_3  = ((1<<3)-1)
MASK_5  = ((1<<5)-1)
MASK_6  = ((1<<6)-1)
MASK_10 = ((1<<10)-1)
MASK_16 = ((1<<16)-1)
MASK_24 = ((1<<24)-1)
MASK_30 = ((1<<30)-1)






# Reading routines  --------------------------


def print_header_vdif(seconds_fr=17, invalid=False, legacy=True, 
               ref_epoch=15, frame_num=1, 
               vdif_version=1, log_2_channels=1, frame_length = 4*5000, 
               data_type=0, bits_per_sample=2, thread_id=1, station_id=1):
    """
    Show header of vdif frame, for logging.
    
    Parameters
    ----------
     See read_header_vdif_from_raw() output.
    """
    print("Seconds frame: \t", seconds_fr)
    print("Invalid: \t", invalid)
    print("Legacy: \t", legacy)
    print("Ref. epoch: \t", ref_epoch)    
    print("Frame number: \t", frame_num)
    print("VDIF version: \t", vdif_version)
    print("Log 2 num channels: \t", log_2_channels)
    print("Frame Length: \t", frame_length, "B", frame_length // 1024, "kB")
    print("Data type: \t", data_type)
    print("Bits per sample: \t", bits_per_sample)
    print("Thread ID: \t", thread_id)
    print("Station ID: \t", station_id)
    

def print_header_vdif_info(brief):
    """
    Show stats results header.
    """
    
    print("VDIF file stats:")
    
    if brief==0:
        print("count:     Counter (based on skip and limit)")
        print("st:        Station ID numeric str")
        print("I:         Invalid")
        print("leg:       Legacy")
        print("vv:        VDIF version")
        print("epoch:     Ref. epoch: \t") 
        print("[s]:       Seconds frame")
        print("num:       Frame number")
        print("thread:    Thread ID")
        print("log2C:     Log2 of the number of channels per frame")
        print("len[B]:    Frame Length in bytes")
        print("len[kB]:   Frame Length in kilobytes")
        print("R/C:       Data type")
        print("bpsamp:    Bits per sample")

    
    print("")
    print("count".rjust(6)+\
          "st".rjust(8)+\
          "I".rjust(3)+\
          "leg".rjust(5)+\
          "vv".rjust(5)+\
          "epoch".rjust(8)+\
          "[s]".rjust(10)+\
          "num".rjust(8)+\
          "thread".rjust(8)+\
          "log2C".rjust(8)+\
          "len[B]".rjust(8)+\
          "len[kB]".rjust(8)+\
          "R/C".rjust(4)+\
          "bpsamp".rjust(8))
          

def print_header_vdif_row(id=0,seconds_fr=17, invalid=False, legacy=True, 
               ref_epoch=15, frame_num=1, 
               vdif_version=1, log_2_channels=1, frame_length = 4*5000, 
               data_type=0, bits_per_sample=2, thread_id=1, station_id=1):
    dt=['R','C']
    print(str(id).rjust(6)+\
          str(station_id).rjust(8)+\
          str(int(invalid)).rjust(3)+\
          str(int(legacy)).rjust(5)+\
          str(vdif_version).rjust(5)+\
          str(ref_epoch).rjust(8)+\
          str(seconds_fr).rjust(10)+\
          str(frame_num).rjust(8)+\
          str(thread_id).rjust(8)+\
          str(log_2_channels).rjust(8)+\
          str(frame_length).rjust(8)+\
          str(frame_length//1024).rjust(8)+\
          dt[data_type].rjust(4)+\
          str(bits_per_sample).rjust(8))


def vdif_epoch_seconds_to_epoch_seconds_datetime(epoch_six,tot_seconds):
    """
    Compute reference epoch and seconds from VDIF standard to MJD and seconds.
    Using datetime.
    
    Parameters
    ----------
     epoch_six : int
         epoch directly from VDIF header.
     tot_seconds : int
         seconds directly from VDIF header.
     
    Returns
    -------
     epoch
         epoch MJD.
     seconds
         epoch seconds.
    """
    
    # Seconds -> number of days, seconds
    seconds=tot_seconds%SECONDS_DAY
    days_from_six=int(tot_seconds//SECONDS_DAY)
    
    # Six-month period -> years, six-month period
    years_num=epoch_six//2
    part_num=epoch_six%2

    # Get days from ref epoch
    year_mod=REF_YEAR+years_num
    base_date = date(REF_YEAR,1,1)
    if part_num==0:
        compare_date = date(year_mod,1,1)
    else:
        compare_date = date(year_mod,7,1)
    diff_date = (compare_date-base_date).days

    epoch=REF_J2000+diff_date+days_from_six

    return([epoch,seconds])



def date_to_vdif(year,month,day,hour,minute,second,offset_seconds=0):
    """
    Compute VDIF fields for epoch and seconds from date.
    """
    base_date = datetime(REF_YEAR,1,1)
    base_date_current = datetime(year,7,1,0,0,0)
    compare_date = datetime(year,month,day,hour,minute,second)
    compare_date += timedelta(0,offset_seconds)
    second_part = compare_date>base_date_current
    if not second_part:
        base_date_current = datetime(year,1,1,0,0,0)
    diff_seconds = (compare_date-base_date_current).seconds
    diff_days = (compare_date-base_date_current).days
    vdif_seconds=diff_seconds+diff_days*SECONDS_DAY
    
    diff_years = year-REF_YEAR
    vdif_epoch = 2*diff_years+int(second_part)

    return([vdif_epoch,vdif_seconds])




def read_header_vdif_from_raw(header):
    """
    Reader vdif header.
    
    Parameters
    ----------
     header : numpy array with four np.unit32 words.
     
    Returns
    -------
     [seconds_fr,invalid,legacy,ref_epoch,frame_num,vdif_version,log_2_channels,
            frame_length,data_type,bits_per_sample,thread_id,station_id]:
            where:
             |   seconds_fr:        VDIF frame seconds (integer).
             |   invalid:           VDIF invalid bit.
             |   legacy:            VDIF legacy bit.
             |   ref_epoch:         VDIF frame epoch (float with MJD (TBC)).
             |   frame_num:         VDIF frame number (integer).
             |   vdif_version:      VDIF frame version (integer).
             |   log_2_channels:    VDIF logarithm in base 2 of the number of channels in the frame.
             |   frame_length:      VDIF frame length (integer with number of bytes).
             |   data_type:         VDIF frame data type bit.
             |   bits_per_sample:   number of bits per sample.
             |   thread_id:         VDIF thread identifier field (integer).
             |   station_id:        VDIF station id field (integer).
    """
    header = header.tolist()
    # Word 0
    invalid =            header[0] >> 31
    legacy =            (header[0] >> 30) & MASK_1
    seconds_fr =         header[0]        & MASK_30
    # Word 1
    ref_epoch =         (header[1] >> 24) & MASK_6 
    frame_num =          header[1]        & MASK_24
    # Word 2
    vdif_version =      (header[2] >> 29) & MASK_3 
    log_2_channels =    (header[2] >> 24) & MASK_5 
    frame_length =       header[2]        & MASK_24 
    frame_length = 8 * frame_length  #Stored and read as a multiple of 8, see [VD01].
    # Word 3
    data_type =          header[3] >> 31
    bits_per_sample =   (header[3] >> 26) & MASK_5 
    bits_per_sample +=1
    thread_id =         (header[3] >> 16) & MASK_10 
    station_id =         header[3]        & MASK_16 
    
    # Adjust epoch and seconds according to VDIF standard (6 month periods...)
    [ref_epoch,seconds_fr]=vdif_epoch_seconds_to_epoch_seconds_datetime(ref_epoch,seconds_fr)
    
    return([seconds_fr,invalid,legacy,ref_epoch,frame_num,vdif_version,log_2_channels,
            frame_length,data_type,bits_per_sample,thread_id,station_id])



def compute_range(word_size,bits_per_sample):
    """
    Get starting positions in word for groups of bits_per_sample bits.
    
    Notes
    -----
    |
    | **Example:**
    |
    |  word_size=32
    |  bits_per_sample=4
    |  list(compute_range(word_size,bits_per_sample))
    |  >>> [0, 4, 8, 12, 16, 20, 24, 28]
    """
    return(range(0,word_size,bits_per_sample))

def read_samples_from_raw(words,bits_per_sample,word_size=32):
    """
    Extract samples from unit32 words into numpy array of integers.
    
    Parameters
    ----------
     words : numpy array of np.unit32.
     bits_per_sample : int
     word_size : int
     
    Notes
    -----
    |
    | **Limitations:**
    |  bits per sample should be a divisor of word_size.
    """
    
    range_offsets = compute_range(word_size,bits_per_sample)
    mask_bits = (1<<bits_per_sample)-1
    words_extended = [((words)>>i) & (mask_bits) for i in range_offsets]
    words_extended2 =np.concatenate(words_extended)
    words_nostack=np.reshape(words_extended2,(-1,len(words))).transpose(1,0).flatten()
    return(words_nostack)


def read_bytes_offset_file(f,n_bytes,v=0):
    """
    Used to skip some offset when reading a binary file.
    
    Parameters
    ----------
     f : file handler
         (sys.stdin).
     n_words : int
         number of words of type TYPE_WORD.
     v : int
         [0 by default] verbose mode if 1.
    """
    words_array = []
    try:
        words_array = np.fromfile(file = f,dtype=np.uint8, count=n_bytes)
        if v==1:
            print("vdif - Read "+str(n_bytes))
    except EOFError:
        if v==1:
            print("vdif - Tried to read "+str(n_bytes))
        return([])
    return([])


def read_words_from_file_to_raw(f,n_words,v=0):
    """
    Read binary file directly into numpy array.
    
    Parameters
    ----------
     f : file handler
         (sys.stdin).
     n_words : int
         number of words of type TYPE_WORD.
     v : int
         [0 by default] verbose mode if 1.
     
    Returns
    -------
     words_array :  numpy array with number->n_words type->TYPE_WORD words.
     
    Notes
    -----
    |
    | **Configuration:**
    |
    |  TYPE_WORD:   [np.uint32 by default] type of words to read.
    """
    words_array = []
    try:
        words_array = np.fromfile(file = f,dtype=TYPE_WORD, count=n_words)
        if v==1:
            print("vdif - Read "+str(n_words))
    except EOFError:
        if v==1:
            print("vdif - Tried to read "+str(n_words))
        return([])   
    return(words_array)


def read_vdif_frame(f,show_errors=0,forced_frame_length=0,offset_bytes=0,v=0,return_raw_header=0):
    """
    Main routine for reading one VDIF frame.
    
    Parameters
    ----------
     f : file handler
         input file handler (typically sys.stdin).
     show_errors : int
         [0 by default] display information on errors if 1.
     forced_frame_length : int
         [0 by default] number of bytes to read including header, if 0 will take value from header (recommended).
     offset_bytes : int
         [0 by default] number of bytes to skip before reading header.
     v : int
         [0 by default] verbosed mode if 1.
     return_raw_header : int
         [0 by default] if 0 returns a list of integers with the header fields, if 1 returns bytes.
     
    Returns
    -------
     header : list of int
         list of integers with the fields in the VDIF header:
            |                   [seconds_fr,invalid,legacy,ref_epoch,frame_num,vdif_version,log_2_channels,\
            |                     frame_length,data_type,bits_per_sample,thread_id,station_id]. 
            |              See read_header_vdif_from_raw() for details.
            |             If errors, header is None.
     samples : 1D numpy array of int 
         sample components. E.g. for VDIF complex frame, [I0, Q0, I1, Q1, ...]
            |              if errors, samples is None.
     check_size_samples : int
         1 if read as many samples as expected from the frame length (and rest of metadata), 0 otherwise.
    
    Notes
    -----
    |
    | **Configuration:**
    |
    |  HEADER_VDIF_WORDS
    |  HEADER_BYTES
    |  WORD_SIZE_BYTES
    | 
    | **Procedure:**
    |
    |  1. Read VDIF header.
    |  2. Read rest of the frame based on frame length information in header.
    """

    words_samples=[]
    failed_read=0
    get_raw=1

    # 0 if less samples than reported in header
    check_size_samples=1

    if offset_bytes>0:
        read_bytes_offset_file(f,offset_bytes)


    words_header = read_words_from_file_to_raw(f,HEADER_VDIF_WORDS,v)
    
    if (words_header == [])or(len(words_header)==0):
        failed_read = 1
        if show_errors:
            print("z-"  + "-Failed to read samples")
        


    if failed_read==0:
        header = read_header_vdif_from_raw(words_header) #[:HEADER_VDIF_WORDS])   
        if v==1:
            [seconds_fr,invalid,legacy,ref_epoch,frame_num,vdif_version,log_2_channels,\
                               frame_length,data_type,bits_per_sample,thread_id,station_id] = header
            print_header_vdif(seconds_fr, invalid, legacy,ref_epoch, frame_num,vdif_version, log_2_channels,\
                               frame_length,data_type, bits_per_sample, thread_id, station_id)
        if show_errors:
            [seconds_fr,invalid,legacy,ref_epoch,frame_num,vdif_version,log_2_channels,\
                             frame_length,data_type,bits_per_sample,thread_id,station_id] = header
        else:
            frame_length=header[7]
            bits_per_sample=header[9]
        
        # Force frame length
        if forced_frame_length>0:
            frame_length=forced_frame_length
                
        #if show_errors:
        #    #print("z-" + str(time.time()) + "- bpsamp: " + str(bits_per_sample)+ "- length: " + str(frame_length))
        #    print("z-" + "- bpsamp: " + str(bits_per_sample)+ "- length: " + str(frame_length))

        n_words_samples = (frame_length-HEADER_BYTES)//(WORD_SIZE_BYTES)
        
        words_samples = read_words_from_file_to_raw(f,n_words_samples,v)#,ENDIAN_BITARRAY,ENDIAN_STRUCT_READING)
        if words_samples == []:
            failed_read = 1
            if show_errors:
                print("z-"  + "-Failed to read samples")
    
        #print_header_vdif(seconds_fr,invalid,legacy,ref_epoch,frame_num,vdif_version,log_2_channels,
        #      frame_length,data_type,bits_per_sample,thread_id,station_id)
    
        if failed_read==0:
            if bits_per_sample>0:
                samples = read_samples_from_raw(words=words_samples,bits_per_sample=bits_per_sample,word_size=WORD_SIZE)
                expected_frame_size = int((len(samples)*bits_per_sample)//8)
                if expected_frame_size < (frame_length-HEADER_BYTES):
                    check_size_samples=0
            else:
                header = None
                samples = None
        else:
            header = None
            samples = None
    else:
        header = None
        samples = None
    
    
    
    if return_raw_header>0:
        if header is not None:
            header = words_header

    return([header,samples,check_size_samples])




def reshape_samples(allsamples,data_type,samples_in_frame,num_channels):
    """
    Corner-turning. 
    
    Parameters
    ----------
     allsamples : 1D array
         with all samples read from VDIF file.
     data_type : int
         0 for real, 1 for complex.
     samples_in_frame : int
         number of samples components per channel.
     num_channels : int
         number of channels.
     
    Returns
    -------
     samples : 2D array with num_channels rows, with each row containing only its associated samples.
    
    Notes
    -----
    |
    | **Procedure:**
    |
    |    # example
    |    aa=list(range(0,20))
    |    bb=np.reshape(aa,(-1,5,2))
    |    cc=np.transpose(bb,(1,0,2))
    |    dd=np.reshape(cc,(5,-1))
    |    dd
          
    """
    
    # If complex data need to group differently
    if data_type==0:
        # real data
        samples = allsamples.reshape((samples_in_frame//num_channels,num_channels)).T
    else:   
        # complex data
        
        
        # np.column_stack is less efficient
        bb=np.reshape(allsamples,(-1,num_channels,2))
        cc=np.transpose(bb,(1,0,2))
        samples=np.reshape(cc,(num_channels,-1))

    return(samples)








# Untested  ---------------------

def read_header_mark5(header):
    """
    [DELETE]
    
    Untested. Consider deleting.
    """
    # Note inverse ordering...
    
    # Word 0
    sync_word = bits2int(header[0][:])
    
    # Word 1
    years = bits2int(header[1][-32:-28])
    user_data = bits2int(header[1][-28:-16])
    bit_t = bits2int(header[1][-16:-15])    
    frame_num = bits2int(header[1][-15:]) 
    
    # Word 2
    time_code_w1 = []
    for i in range(8):
        time_code_w1.append(bits2int(header[2][-32+4*i:-32+4*(i+1)]))
    
    # Word 3
    time_code_w2 = []
    for i in range(4):
        time_code_w2.append(bits2int(header[3][-32+4*i:-32+4*(i+1)]))
    crcc = bitarray(endian=ENDIAN_BITARRAY)
    crcc = header[2][-17:]
    
    
    return([sync_word, years, user_data, bit_t, frame_num, time_code_w1, time_code_w2, crcc])
    
def print_header_mark5(sync_word, years, user_data, bit_t, frame_num, time_code_w1, time_code_w2, crcc):
    """
    [DELETE]
    
    Untested. Consider deleting.
    """
    print("Sync word: \t", sync_word)
    print("Years: \t", years)
    print("User Data: \t", user_data)
    print("Bit T: \t", bit_t)    
    print("Frame number: \t", frame_num)
    print("Time code word 1: \t", time_code_w1)
    print("Time code word 2: \t", time_code_w2)
    print("CRCC: \t", crcc)





###########################################################
#                Writing libraries
###########################################################
# TO DO: needs further testing

def int2bits(value):
    value_B = struct.pack(ENDIAN_STRUCT, value)
    value_b = bitarray(endian=ENDIAN_BITARRAY)
    value_b.frombytes(value_B)
    return(value_b)


def strbitarray2int(a):
    return(int(a,2))

def bits2int(bitsin):
    return(int(bitsin.to01(),2))


    
def create_header_vdif(seconds_fr=17, invalid=False, legacy=True, 
               ref_epoch=15, frame_num=1, 
               vdif_version=1, log_2_channels=1, frame_length = 4*5000, 
               data_type=0, bits_per_sample=2, thread_id=1, station_id=1):
    """
    TO DO: check epoch...
    """

    #DATA TYPE 0 FOR REAL, 1 FOR COMPLEX
    # TO DO: issues with epoch...

    # Computation for ref_epoch and seconds
    #[ref_epoch,seconds_fr]=epoch_seconds_to_vdif_epoch_seconds(ref_epoch,seconds_fr)
    # TO DO: check this...
    #[ref_epoch,seconds_fr]=vdif_epoch_seconds_to_epoch_seconds_datetime(ref_epoch,seconds_fr)
    #[ref_epoch,seconds_fr]=date_to_vdif(ref_epoch,seconds_fr)

    
    # Word 0
    sec_b = int2bits(seconds_fr)

    # Word structure: invalid (31), legacy (30), seconds from reference (29-0) 
    word0 = bitarray(endian=ENDIAN_BITARRAY)
    word0.append(invalid)
    word0.append(legacy)
    word0.extend(sec_b[-30:])


    # Word 1

    ref_epoch_b = int2bits(ref_epoch)
    #print(ref_epoch_b)
    frame_num_b = int2bits(frame_num)

    # Word structure: unassigned (31-30), ref epoch (29-24), frame number (23-0)
    word1 = bitarray(endian=ENDIAN_BITARRAY)
    word1.append(0)
    word1.append(0)
    word1.extend(ref_epoch_b[-6:])
    word1.extend(frame_num_b[-24:])


    # Word 2

    vdif_version_b = int2bits(vdif_version)
    log_2_channels_b = int2bits(log_2_channels)
    frame_length_b = int2bits(frame_length//8) #Stored and read as a multiple of 8!!!!

    # Word structure: vdif version (31-29), log2 of num channels (28-24), data frame number within second (23-0)
    word2 = bitarray(endian=ENDIAN_BITARRAY)
    word2.extend(vdif_version_b[-3:])
    word2.extend(log_2_channels_b[-5:])
    word2.extend(frame_length_b[-24:])


    # Word 3
    d_type = int2bits(data_type) # 0 for real, 1 for complex
    bits_per_sample-=1
    bits_per_sample_b = int2bits(bits_per_sample)
    thread_id_b = int2bits(thread_id)
    station_id_b = int2bits(station_id)

    # Word structure: data type (31), bits/sample (30-26), thread id (25-16), station id (15-0)
    word3 = bitarray(endian=ENDIAN_BITARRAY)
    word3.extend(d_type[-1:])
    word3.extend(bits_per_sample_b[-5:])
    word3.extend(thread_id_b[-10:])
    word3.extend(station_id_b[-16:])

    # Word 4 --EMPTY--
    word4 = bitarray(endian=ENDIAN_BITARRAY)
    word4.extend(int2bits(0))

    # Word 5 --EMPTY--
    word5 = bitarray(endian=ENDIAN_BITARRAY)
    word5.extend(int2bits(0))
    
    # Word 6 --EMPTY--
    word6 = bitarray(endian=ENDIAN_BITARRAY)
    word6.extend(int2bits(0))
    
    # Word 7 --EMPTY--
    word7 = bitarray(endian=ENDIAN_BITARRAY)
    word7.extend(int2bits(0))

    header = [word0,word1,word2,word3,word4,word5,word6,word7]
    return(header)




def write_samples(samples,bits_per_sample,word_size=32):
    """
    Write vdif samples. Used by vdif signal generator.
    """
    samples_word = word_size // bits_per_sample
    offset = word_size - (samples_word * bits_per_sample)
    
    # Zero padding word size
    excess_samples = len(samples) % samples_word
    if excess_samples>0:
        samples.extend([0] * (samples_word - excess_samples))
    
    tot_words = len(samples) // samples_word
    
    words = []
    for i in range(tot_words):
        bitbase = bitarray(endian=ENDIAN_BITARRAY)
        if offset>0:
            bitbase.extend(int2bits(0)[-offset:])
        for j in reversed(range(samples_word)):
            bitbase.extend(int2bits(samples[i*samples_word+j])[-bits_per_sample:])
        words.append(bitbase)
    return(words)


def write_samples_raw(samples,bits_per_sample,word_size=32):
    """
    Write vdif samples.
    
    TO DO:
    ------
    Untested.
    Limited to bits_per_samples that are divisor of 8.
    """
    samples_word = word_size // bits_per_sample
    offset = word_size - (samples_word * bits_per_sample)

    # Zero padding word size
    excess_samples = len(samples) % samples_word
    if excess_samples>0:
        #samples.extend([0] * (samples_word - excess_samples))
        words=[]
    else:
    
        tot_words = len(samples) // samples_word
        
        range_offsets = range(bits_per_sample-1,-1,-1)
        
    
        # !! Limited to bits_per_sample divisors of 8!
        samples_reshape = np.reshape(samples,(-1,8//bits_per_sample))
    
        samples_reshape = np.fliplr(samples_reshape)
    
        samples_reshape = np.reshape(samples_reshape,(1,-1))
        #bits_offset = [((samples)>>i) & (1) for i in range_offsets]
        bits_offset = [((samples_reshape)>>i) & (1) for i in range_offsets]
        bits_extended = np.concatenate(bits_offset)
        bits_trans = np.transpose(np.reshape(bits_extended,(bits_per_sample,-1)),(1,0))
        words = np.packbits(bits_trans.reshape(1,-1))
    return(words)



def write_words_to_file(f,words):
    words_int=[bits2int(i) for i in words]
    words_array = array.array(LOW_LEVEL_WORD,words_int)
    words_array.tofile(f)


def write_words_to_file_raw(f,words):
    #words_int=[bits2int(i) for i in words]
    #words_array = array.array(LOW_LEVEL_WORD,words)
    words.tofile(f)


###########################################################
#                      Utilities
###########################################################



def disp_list(a):
    return(','.join(map(str,a)))


def get_vdif_stats(filename,packet_limit=-1,forced_packet_size=0,offset_bytes=0,only_offset_once=1,v=1,short_output=0):
    """
    Get information from vdif file by processing headers. As many headers as packet_limit are processed.
    Each of the output parameters are vectors with all the possible values found.
    
    Parameters
    ----------
     filename : str
         path to VDIF file.
     packet_limit : int
         [default -1] maximum number of packets, if -1 no limit.
     forced_packet_size : int
         [default 0] if >0 will read this number of bytes instead those in the VDIF header.
     offset_bytes : int
         [default 0] skip this number of bytes before reading the first VDIF header.
     only_offset_once : int
         [default 1] if ==0 will skip offset_bytes before every frame to be read.
     v : int
         [default 1] verbose mode if 1.
     short_output : int
         [default 0] group very long vector (frames) if 1.
    
    Returns
    -------
     v_stations : list
         list with unique ids found for stations in all frames read.
     v_seconds : list
         list with unique seconds found for all frames read.
     v_frames : list
         list with unique frame numbers for all frames read.
     v_sizes : list
         list with unique frame sizes for all frames read.
     total_size : int          total size for all frames read.
    """
    file_size=os.path.getsize(filename)
    f_read=open(filename,'rb')
    keep_reading=1
    SHOW_ERRORS = 0
    ljv=20
    
    packet_count = 0
    reader = f_read #sys.stdin

    v_epoch=[]
    v_seconds=[]
    v_frames=[]
    v_data_type=[]
    v_sizes=[]
    v_stations=[]
    v_numchannels=[]
    v_bpsamp=[]
    v_threads=[]

    total_file_size=0 # Including headers
    total_size=0      # Only data

    v_file=np.arange(file_size*0.1,file_size*1.1,file_size/10) # File size comparison thresholds
    v_perc=np.arange(10,110,10)                                # Percentages

    if v==1:
        print("Reading VDIF file...   0%",end="")
        sys.stdout.flush()
    
    while keep_reading==1:
        if v==1:
            for i in range(len(v_file)):
                if i>=len(v_file):
                    break
                if total_file_size>=v_file[i]:
                    print("..."+str(v_perc[i])+"%",end="")
                    sys.stdout.flush()
                    v_file=v_file[i+1:]
                    v_perc=v_perc[i+1:]
                else:
                    break

        packet_count += 1
        if packet_limit != -1:
            if packet_count == packet_limit:
                keep_reading = 0
       
        
        # Get header and samples from VDIF packet
        [header,samples,check_size_samples] = read_vdif_frame(reader,SHOW_ERRORS,forced_packet_size,offset_bytes)
        
        if only_offset_once==1:
            offset_bytes=0

        if not(header == None):

            # Decode header
            [seconds_fr,invalid,legacy,ref_epoch,frame_num,vdif_version,log_2_channels,
                frame_length,data_type,bits_per_sample,thread_id,station_id] = header
            
            if ref_epoch not in v_epoch:
                v_epoch+=[ref_epoch]
            if seconds_fr not in v_seconds:
                v_seconds+=[seconds_fr]
            if frame_num not in v_frames:
                v_frames+=[frame_num]
            if frame_length not in v_sizes:
                v_sizes+=[frame_length]
            if data_type not in v_data_type:
                v_data_type+=[data_type]    
            if station_id not in v_stations:
                v_stations+=[station_id]
            num_channels=2**log_2_channels
            if num_channels not in v_numchannels:
                v_numchannels+=[num_channels]
            if bits_per_sample not in v_bpsamp:
                v_bpsamp+=[bits_per_sample]
            if thread_id not in v_threads:
                v_threads+=[thread_id]
            total_size+=frame_length
            total_file_size+=frame_length
            total_file_size+=(HEADER_VDIF_WORDS*WORD_SIZE_BYTES)


        else:
            keep_reading=0

    f_read.close()
    
    if v==1:
        print("")
        print("File information:")
        if packet_limit>0:
            print(" (!) Packet limit: ".ljust(ljv) + str(packet_limit))
        print(" File name: ".ljust(ljv) + str(filename))
        print(" Data size: ".ljust(ljv) + str(total_size) + " B" + " (~"+str(total_size/(1024*1024))+" MB)")
        print(" Stations: ".ljust(ljv) + str(v_stations))
        print(" Epochs: ".ljust(ljv) + disp_list(v_epoch))
        print(" Seconds: ".ljust(ljv) + disp_list(v_seconds))
        if short_output:
            v=[]
            pre_i=-1
            for i in v_frames:
                if pre_i>=0:
                    if i!=pre_i+1:
                        pre_i=-1
                        v.append([start,i])
                    else:
                        pre_i=i
                else:
                    start=i
                    pre_i=i
            if i==v_frames[-1]:
                v.append([start,i])
            v=str(v).replace(' ','').replace(',',':').replace('L','')
            
            print(" Frames: ".ljust(ljv)+v)
        else:
            print(" Frames: ".ljust(ljv) + disp_list(v_frames))
        print(" Sizes: ".ljust(ljv) + disp_list(v_sizes))
        print(" Data type: ".ljust(ljv) + disp_list(v_data_type))
        print(" Num.channels: ".ljust(ljv) + disp_list(v_numchannels))
        print(" Num.threads: ".ljust(ljv) + disp_list(v_threads))
        print(" Bits per sample: ".ljust(ljv) + disp_list(v_bpsamp))
        
    return([v_stations,v_seconds,v_frames,v_sizes,total_size])






def get_vdif_num_frames(filename,packet_limit=-1,forced_packet_size=0,offset_bytes=0,\
                        only_offset_once=1,v=1):
    """
    Get information from vdif file by processing headers. As many headers as packet_limit are processed.
    Each of the output parameters are vectors with all the possible values found.
    
    Parameters
    ----------
     See get_vdif_stats().
     
    Returns
    -------
     num_frames : int
         number of frames read.
    
    Notes
    -----
    |
    | **TO DO:**
    |
    |  This is a modified version of get_vdif_stats(), merge with it.
    """
    f_read=open(filename,'rb')
    keep_reading=1
    SHOW_ERRORS = 0

    
    packet_count = 0
    reader = f_read #sys.stdin

    v_epoch=[]
    v_seconds=[]
    v_frames=[]
    v_data_type=[]
    v_sizes=[]
    v_stations=[]
    v_numchannels=[]
    v_bpsamp=[]
    
    num_frames=0

    total_size=0

    while keep_reading==1:
        packet_count += 1
        if packet_limit != -1:
            if packet_count == packet_limit:
                keep_reading = 0
       
        
        # Get header and samples from VDIF packet
        [header,samples,check_size_samples] = read_vdif_frame(reader,SHOW_ERRORS,forced_packet_size,offset_bytes)
        
        if only_offset_once==1:
            offset_bytes=0

        if not(header == None):

            # Decode header
            [seconds_fr,invalid,legacy,ref_epoch,frame_num,vdif_version,log_2_channels,
                frame_length,data_type,bits_per_sample,thread_id,station_id] = header
            
            if ref_epoch not in v_epoch:
                v_epoch+=[ref_epoch]
            if seconds_fr not in v_seconds:
                v_seconds+=[seconds_fr]
            if frame_num not in v_frames:
                v_frames+=[frame_num]
            if frame_length not in v_sizes:
                v_sizes+=[frame_length]
            if data_type not in v_data_type:
                v_data_type+=[data_type]    
            if station_id not in v_stations:
                v_stations+=[station_id]
            num_channels=2**log_2_channels
            if num_channels not in v_numchannels:
                v_numchannels+=[num_channels]
            if bits_per_sample not in v_bpsamp:
                v_bpsamp+=[bits_per_sample]
            total_size+=frame_length
            
            
            if len(v_seconds)==2:
                keep_reading=0
                

        else:
            keep_reading=0

    f_read.close()
    
    num_frames = max(v_frames)+1
        
    return(num_frames)


def show_headers_vdif(filename,limit_frames=-1,skip_frames=-1,brief=0):
    """
    Get tabulated information from vdif file headers. The first "limit_frames" after "skip_frames" are read.
    
    Parameters
    ----------
     filename : str
         path to file.
     limit_frames : int
         maximum number of lines to display (-1 for no limit).
     skip_frames : int
         number of frames to skip before starting displaying frames (default <=0).
     brief : int
         1 to avoid displaying descriptions for columns.
     
    Returns
    -------
     N/A
    """
    f_read=open(filename,'rb')

    SHOW_ERRORS=0
    counter_frames=-1
    
    keep_reading=1
    
    
    print_header_vdif_info(brief)
    
    while keep_reading==1:
        
        
        if limit_frames>0 and skip_frames<=0:
            counter_frames+=1
            if counter_frames==(limit_frames-1):
                keep_reading=0
        elif (limit_frames<0 and limit_frames<-0):
            counter_frames+=1
        reader = f_read
        
        # Get header and samples from VDIF packet
        [header,samples,check_size_samples] = read_vdif_frame(reader,SHOW_ERRORS)
        

        if not(header == None):
           
            
            # Decode header
            [seconds_fr,invalid,legacy,ref_epoch,frame_num,vdif_version,log_2_channels,
                frame_length,data_type,bits_per_sample,thread_id,station_id] = header

            if skip_frames>0:
                skip_frames-=1
                continue
            print_header_vdif_row(counter_frames,seconds_fr,invalid,legacy,ref_epoch,frame_num,vdif_version,log_2_channels,
                frame_length,data_type,bits_per_sample,thread_id,station_id)
            #print("--------------------")

        else:
            keep_reading = 0

    f_read.close()

# <codecell>


