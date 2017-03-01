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
#File: vdif_generator.py.
#Author: A.J. Vazquez Alvarez (ajvazquez@haystack.mit.edu)
#Description: 
"""
Module for generating test input files (VDIF). 

(!) IMPORTANT:
--------------
 This is not a VLBI signal generator, use is simply as a tool to generate VDIF test files.
 Use only for debugging!

(!) Known issues / TO DO:
-------------------------
 Requires further testing, included only as a template to be extended.
 Update implementation to not using the bitarray module.

"""
#History:
#initial version: 2015.12 ajva
#MIT Haystack Observatory

from __future__ import print_function
import sys
import numpy as np
import matplotlib.pyplot as plt
from scipy.fftpack import fft,ifft
from scipy import signal
from datetime import date,datetime

from bitarray import bitarray

import imp

import lib_vdif
imp.reload(lib_vdif)
from lib_vdif import *

import lib_channelized_signals
imp.reload(lib_channelized_signals)
from lib_channelized_signals import *

from lib_quant import *

  
    
def generate_multi_sine_wave(N,fs,fv,x0,ampv,noise_amp,file_log=sys.stdout,v=1,v_debug=1):
    """
    Generate test signal composed of multiple sine waves and noise.
    
    Parameters
    ----------
     N
         number of samples.
     fs
         sampling frequency.
     fv
         list with frequencies for the sines.
     x0
         number of offset samples.
     ampv
         list of amplitudes for the sines.
     noise_amp
         noise amplitude.
     file_log
         logging file (deafult sys.stdout).
     v
         verbose if 1.
     v_debug
         displays debugging information if 1.
      
    Returns
    -------
     y
         generated signal.
    """
    
    fsh = fs/2
    T=1/fs
    if v==1:
        if v_debug==1:
            print("x0: "+str(x0), end=" ",file=file_log)
            print("N: "+str(N),file=file_log)
    x = np.array(range(x0,x0+N))
    x_times = x*T
    
    ssines = np.zeros(N)
    
    for (samp,sf) in zip(ampv,fv):
        ssines += samp*np.sin(sf * 2.0*np.pi*x_times) 
    
    noise = np.random.normal(0.0,noise_amp,len(x_times))

    y =  noise
    y += ssines
    return(y)


def filter_signals_fir(y,numtaps,num_channels,channel_mapping=[],file_log=sys.stdout,v=1,v_debug=1):
    """
    Given a signal y, this function returns num_channel signals that correspond to the signal passed through 
    a filterbank of num_channels channels, which filters have numtaps elements.
    
    Parameters
    ----------
     y
         signal to be processed.
     numtaps
         number of coefficients in the FIR filter.
     num_channels
         number of channels to generate.
     channel_mapping
         see notes below.
     file_log
         logging file (default sys.stdout)
     v
         verbose if 1.
     v_debug
         debugging information if 1.
    
    Returns
    -------
     filtered_signals_out
         list of filtered signals for each channel.
     filterbank
         list of filter responses for each channel.
    
    Notes
    -----
     Channel mapping allows to customize the order and number of bands for the filtering.
     default for nuchannels=4 (if channel_mapping==[]) is equivalent to channel_mapping=[0,1,2,3]
     e.g. channel_mapping=[0,1,0,1] # channel_mapping=[0,1,2,3]
    """
    
    if channel_mapping!=[]:
        num_channels=max(channel_mapping)+1
    
    N = len(y)
    if numtaps % 2 ==0:
        numtaps +=2
        if v==1:
            if v_debug==1:
                print("(numtaps changed to be odd)", end="",file=file_log)
                print("[N="+str(N)+"]", end="",file=file_log)
    #Filter bank specification: vector of frequencies [f0, f1, ..., fM] -> Bands: 0..f0, f0..f1, ... , fM..fs/2
    #ff = np.dot([0.5],0.5)

    if num_channels>1:
        ff = np.linspace(0.0,0.99,num_channels+1)[1:]
    else:
        ff = [0.99]

    # Filterbank
    filterbank = [] 
    filtered_signals = []
    maxiff=len(ff)
    if maxiff==0:
        # Single channel, do nothing
        filterbank += [signal.firwin(numtaps, ff[0])]
        filtered_signals += [y]
    else:
        # multiple channel
        for i in range(len(ff)):
            #print(ff[i])
            if i==0:
                # Low pass
                filterbank += [signal.firwin(numtaps, ff[i])]
            elif i==maxiff:  
                # High pass
                filterbank += [signal.firwin(numtaps, ff[i-1],pass_zero=False)]
            else:
                # Band pass
                filterbank += [signal.firwin(numtaps, [ff[i-1],ff[i]],pass_zero=False)]
            filtered = signal.convolve(y,filterbank[-1],'same')
            downconv = np.real(np.multiply(filtered,np.exp(1j*np.pi*(float(i)/float(num_channels))*np.arange(len(y)))))
            #if i>0:
            #    downconv*=2
            resampled = signal.resample(downconv,len(y)/num_channels)
            filtered_signals += [resampled]
            #filtered_signals += [signal.convolve(y,filterbank[-1],'same')]
            
    if channel_mapping==[]:
        # Simply output filtered signals
        filtered_signals_out = filtered_signals
    else:
        # Select signals based on order specified in channel_mapping
        filtered_signals_out=[]
        for i in channel_mapping:
            filtered_signals_out+=[filtered_signals[i]]
    
    if v==1:
        if v_debug==1:
            print("[filts="+str(len(filtered_signals_out[0]))+"]x"+str(len(filtered_signals_out)), end="",file=file_log)
    return([filtered_signals_out,filterbank])


def get_filename_vg(prefix,station,ext=".vt"):
    return(prefix+"-"+str(station)+ext)
    
def generate_vdif(tot_stations,bw_in,bytes_payload_per_frame,bits_quant,snr_in,sines_f_in,sines_amp_in,\
                  prefix,signal_limits,log_2_channels,num_threads,threaded_channels,num_taps_filterbank,\
                  date_vector,seconds_duration,simple_file="file_test_simple",unquant_file="file_test_unquant",\
                  v=0,file_log=sys.stdout,data_dir="./",write_raw=0,channel_mapping=[],mode_in="sines",v_debug=0):
    """
    Generator for VDIF test (.vt) data. Use for debugging only.
    
    IMPORTANT: This is not a VLBI signal generator, use is simply as a tool to generate VDIF test files.
    
    Parameters
    ----------
     tot_stations
         number of station (one file per station).
     bw_in
         bandwidth for the complete signal.
     bytes_payload_per_frame
         approximate size for the payload section of the frames.
     bits_quant
         number of bits per sample.
     snr_in
         signal to noise ratio (in natural units).
     sines_f_in
         list with frequencies for the tones in Hz (each must be < bw_in)..
     sines_amp_in
         list with relative amplitudes for the tones. 
     prefix
         prefix for the output files.
     signal_limits
         list with [minimum,maximum] values for the linear quantizer.
     log_2_channels
         logarithm in base 2 of the number of VDIF channels.
     num_threads
         number of VDIF threads.
     threaded_channels
         1 for channelizing the initial bandwdith into the VDIF threads.
     num_taps_filterbank
         number of taps for the FIR filter used to channelized the initial bandwidth.
     date_vector
         list of integers with [year,month,day,hour,minute,second] for the first sample.
     seconds_duration
         integer with signal duration.
     simple_file
         filename for debugging file with quantized signal.
     unquant_file
         filenaem for debugging file with unquantized signal.
     v
         verbose if 1.
     file_log
         logging file (deafult is sys.stdout)
     data_dir
         path for output files (default is current path).
     write_raw
         generates debugging files if 1.
     channel_mapping
         see filter_signals_fir() (deafult is []).
     mode_in
         type of generated signal (currently only sines).
     v_debug
         shows debugging information if 1.
    
    Returns
    -------
     N/A. It generates one file for each station.
    
    Notes
    -----
    |
    | **Approximations:**
    |
    |  Using linear quantizer.
    |  Always sampling.
    |  Only mode currently implemented: tones + noise.
    |  Either multiple threads or multiple channels.
    |  Data is duplicated for all stations
    |
    |
    | **TO DO:**
    |
    |  WARNING: use only for debugging!!
    |  This requires further work.
    """


    # For seconds iterations
    seconds = range(seconds_duration)
    
    num_channels = 2**log_2_channels    
    # Sampling frequency (should be 2*BW)
    fs = 2*bw_in
    if num_threads>1 and threaded_channels:
        num_bands=num_threads
    else:
        num_bands=num_channels

    # Check that payload size is multiple of 8
    check_bytes = bytes_payload_per_frame / 8 - bytes_payload_per_frame // 8
    if check_bytes != 0:
        if v==1:
            print("Frame size is not a multiple of 8, correcting...",file=file_log)
            bytes_payload_per_frame = (bytes_payload_per_frame//8)*8
    
    samples_per_frame = int(bytes_payload_per_frame * 8 // bits_quant)
    diff_bytes = samples_per_frame - bytes_payload_per_frame * 8 / bits_quant
    if diff_bytes != 0:
        if v==1:
            print("Frame size is not a multiple of bits/sample",file=file_log)
    
    frames_per_second = int(np.ceil(num_channels * fs / samples_per_frame))
    
    diff_samples = float(frames_per_second) - (num_channels*fs) / samples_per_frame
    
    
    estimated_size = frames_per_second * (bytes_payload_per_frame+(HEADER_VDIF_WORDS*WORD_SIZE/4)) * seconds_duration

    # Data will be channelized with same length as input. Reduce number of samples to fit payload size.
    tot_samples = frames_per_second*samples_per_frame #// num_channels
    
    frame_length=0
    if write_raw==1:
        f_simple_out=open(data_dir+simple_file,'w')  
        f_unquant_out=open(data_dir+unquant_file,'w')  
    

    # Phases for the different stations
    #phases (e.g. equispaced values from zero to pi/5)
    #phi_value=0.2
    phi_value=0
    phi=np.linspace(0.0, 2.0*np.pi*phi_value, tot_stations)
    
    # Number of samples
    N = int(tot_samples)
   
    #Noise amplitude
    noise_amp = 1.0
    
    #Test tone amplitude
    signal_amp = noise_amp*np.sqrt(snr_in)#0.001
    
    

    num_taps = num_taps_filterbank


    
    sines_f = sines_f_in #np.multiply(sines_f_in,fs/2)
    sines_amp = np.multiply(sines_amp_in,noise_amp*np.sqrt(snr_in))
    
    print("VDIF frames generator:")
    print("")
    print(" Data type: "+str(bits_quant)+"-bit real (linearly quantized)")
    print(" Total BW            = "+str(fs/2)+" Hz")
    print(" SNR                 = "+str(10*np.log10(snr_in))+" dB")
    print(" Tones:")
    for f,a,i in zip(sines_f,sines_amp,list(range(len(sines_f)))):
        print("  "+str(i).ljust(4)+" "+"f = "+str(f)+" Hz,   a = "+str(a))
    print(" Data channelization:")
    print("  Number of bands:     "+str(num_bands))
    print("  Band BW:             "+str(bw_in/num_bands)+" Hz")
    print("  Filterbank FIR taps: "+str(num_taps_filterbank))
    print(" VDIF channels/threads:")
    print("  Number of channels:  "+str(num_channels))
    print("  Number of threads:   "+str(num_threads)+" ",end="")
    if num_threads==0:
        print("")
    else:
        if threaded_channels:
            print("(one band per thread)")
        else:
            print("(threads are duplicated bands)")
    print(" Signal time info:")
    print("  Date:            "+str(datetime(*date_vector)))
    [check_m,check_s] =  vdif_epoch_seconds_to_epoch_seconds_datetime(*date_to_vdif(*date_vector))
    print("   MJD:            "+str(check_m))
    print("   Seconds:        "+str(check_s))
    print("  Signal duration: "+str(seconds_duration)+" s")
    print("Output:")
    print(" Output folder:       " + data_dir)
    print(" VDIF file(s):        " + ','.join([get_filename_vg(prefix,station) for station in range(tot_stations)]))
    print(" Samples per frame:   " + str(samples_per_frame))
    print(" Frames per second:   " + str(frames_per_second))
    print(" Estimated file size: " + str(frames_per_second) + " fps * "+\
              str(bytes_payload_per_frame+(HEADER_VDIF_WORDS*WORD_SIZE/4))\
              + " B/f * " +  str(seconds_duration) + " s = " + str(estimated_size) + " B (" + str(estimated_size/1024) +" kB)")
    
    if diff_samples != 0:
        if v==1:
            print(" Not enough samples for last packet: "+str((num_channels*fs) / samples_per_frame) + " -> " + str(frames_per_second) + " fps",file=file_log)
    

    # Split into tot_steps iterations to avoid memory issues
    #tot_steps=2**3
    tot_steps=1
    #tot_steps=2
    tot_steps=frames_per_second
    N_partial = int(N/tot_steps)
    
    all_frame_ids=[]
    [year,month,day,hour,minute,second]=date_vector
    for station in range(tot_stations):
        total_frames=0
        # Prepare vdif file
        filename=get_filename_vg(prefix,station)
        f_out=open(data_dir+filename,'wb')
        
        
        first_sample=0
        x0=0
        
        for second_offset in seconds:
            frame_id_adjusted = -1
            
            for small_step in range(tot_steps):
            
                # Time offset for generated data
                x0_adj = x0 + small_step * N_partial

                if v==1:
                    if v_debug==1:
                        print(" Station ",station,": s ",second, " :",end=" ",file=file_log)
        
                # Generate multi-sine wave
                ##if mode_in=="sines":
                ymulti = generate_multi_sine_wave(N=N_partial,fs=fs,x0=x0_adj,fv=sines_f,ampv=sines_amp,noise_amp=noise_amp,file_log=file_log,v=v,v_debug=v_debug)
                ##elif mode_in==#[...]
                ##    #[...]
        
                if write_raw==1:
                    # Signals for logging (no channelization nor interleaving, only quantization)
                    ymulti_quant = simple_quantizer(ymulti,bits_quant,signal_limits,force_limits=1)
        

                if threaded_channels == 1:
                    # Threads also have channels:
                    tot_channels = num_threads*num_channels
                else:
                    # Threads will not have channels
                    tot_channels = num_channels
                # Get channelized signals
                [filtered_signals,filterbank] = filter_signals_fir(ymulti,num_taps_filterbank,tot_channels,channel_mapping,file_log,v,v_debug)     
        

                multi_channel_threads = []
                multi_channel_threads_quantized = []
                for tid in range(0,num_threads):#tot_channels,num_channels
                    # For each thread:
                        sidv =[]
                        for cid in range(0,num_channels):
                        # For each sub-channel inside the thread
                            # Interleave sub-channel's samples
                            # Index of filtered signal
                    
                            if threaded_channels == 1:
                                # composite index
                                sid = tid*num_channels+cid
                            else:
                                # just duplicate corresponding channel
                                sid = cid
                            #filtered_signals
                            sidv += [sid]
                        # Interleave signals corresponding to same thread
                        multi_channel_signal = [i for ilist in zip(*[filtered_signals[sidv_id] for sidv_id in sidv]) for i in ilist]
                        if v==1:
                            if v_debug==1:
                                print("  Thread " + str(tid) + ", bands: " +' '.join(map(str,sidv)), end=" ",file=file_log)
                        multi_channel_threads += [multi_channel_signal]
                        multi_channel_threads_quantized += [simple_quantizer(multi_channel_signal,bits_quant,signal_limits,force_limits=1)]

    
                samplv = []
    
               
                if small_step<(tot_steps-1):
                    frames_per_second_adjusted = int(frames_per_second//tot_steps)
                else:
                    frames_per_second_adjusted = int(frames_per_second%(frames_per_second//tot_steps))
                    frames_per_second_adjusted =int(np.ceil(frames_per_second/tot_steps))
                    
                for frame_id in range(frames_per_second_adjusted): 
                    frame_id_adjusted+=1
                    all_frame_ids += [frame_id_adjusted]
        
                    # For each thread:
                    for tid in range(0,num_threads):
            
        
                        #Create header
                        [epoch_fr,seconds_fr] = date_to_vdif(year,month,day,hour,minute,second,second_offset)
                        invalid=False
                        legacy=False 
                        # Adjusted frame number to be written in frame (absolute for second), but relative number for calculation below
                        frame_num=frame_id_adjusted
                        vdif_version=7
                        log_2_channels= log_2_channels #0
                        data_type = 0
                        bits_per_sample=bits_quant
                        thread_id=tid
                        station_id=station
        
                        # Take signal, quantize,             
                        y_quant = multi_channel_threads_quantized[tid]
                        y_quant_start_index = frame_id*samples_per_frame
                        y_quant_end_index = (frame_id+1)*min(len(y_quant),samples_per_frame)
                
                        samples_words = write_samples(y_quant[y_quant_start_index:y_quant_end_index],bits_quant,word_size=WORD_SIZE)
                
                        frame_length = (len(samples_words)+HEADER_VDIF_WORDS)*WORD_SIZE//8
    
                        samplv += [frame_length]

                        if v==1:
                            if v_debug==1:
                                print(frame_length,file=file_log)
                        total_frames+=1
            
                        header1 = create_header_vdif(seconds_fr=seconds_fr, invalid=invalid, legacy=legacy, \
                               ref_epoch=epoch_fr, frame_num=frame_num, \
                               vdif_version=vdif_version, log_2_channels=log_2_channels, frame_length=frame_length, \
                               data_type=data_type, bits_per_sample=bits_per_sample, thread_id=thread_id, station_id=station_id)

                        write_words_to_file(f_out,header1)
                        write_words_to_file(f_out,samples_words)

            
                if write_raw==1:
                    #Write signal to file
                    f_simple_out.write(str(station)+' '+str(fs)+' '+str(second)+'.'+str(first_sample)+' '+' '.join(map(str, ymulti_quant))+'\n')#,file=f_out) 
                    f_unquant_out.write(str(station)+' '+str(fs)+' '+str(second)+'.'+str(first_sample)+' '+' '.join(map(str, ymulti))+'\n')
    
        # Close vdif file (single station)
        f_out.close()

        if v==1:
            print("  "+str(total_frames)+"x",end=" ",file=file_log)
            print(set(samplv),end=" ",file=file_log)
            print("(l="+str(len(samplv))+")",end=" ",file=file_log)
            print("-> ",filename,end="\n",file=file_log)


    if write_raw==1:
        # Close basic format file (all station)
        f_simple_out.close()
        f_unquant_out.close()
   
    if v==1:
        print(" Simple file: \t\t\t",simple_file,end="\n",file=file_log)
        print(" Unquantized signal file: \t",unquant_file,end="\n",file=file_log)
    
    if v_debug:
        print(all_frame_ids)
    return(frame_length)



  
def plot_fft_set(yv,fs,intitle,showlegend=1,overlay=1):
    """
    Plot abs. of spectrum of list of signals.
    
    Parameters
    ----------
     yv
         list of signals (one signal per channel).
     fs
         sampling frequency.
     intitle
         title for the plot.
     showlegend
         displays legend if 1.
     overlay
         displays spectra in the same axis, sorted by position in list.
     
    Returns
    -------
     N/A
     
    Notes
    -----
    |
    | **Assumptions:**
    |
    |  Assuming real signal and sampling frequency = 2 * bw.
    """
    fig = plt.figure()
    legv = []
    count = 0
    max_count=len(yv)
    for y in yv:
        N = len(y)
        yf=fft([complex(i) for i in y])
        yf=yf[:(N//2)]
        freq_sample=fs
        if overlay:
            xf = [int(i) for i in np.linspace(count*freq_sample/(2.0), 1.0*(count+1)*freq_sample/(2.0), N/2)]
        else:
            xf = [int(i) for i in np.linspace(0.0, 1.0*freq_sample/(2.0), N/2)]
        plt.plot(xf[:], (2.0/N)*np.abs(yf[:]),'o-')
        legv += ["Channel " +str(count)]
        count+=1
    if showlegend==1:
        if overlay:
            plt.legend(legv,fontsize=8, loc='center left', bbox_to_anchor=(1, 0.5))
        else:
            plt.legend(legv, loc='best')
    fig.suptitle(intitle, fontsize=12)
    plt.show()
    fig.savefig("fig_multichannel.png")#, fontproperties=font)
    
    

def plot_signal_from_packets(filename,bw,only_station=-1,packet_limit=-1,same_figure=1):
    """
    Plot spectra of bands from a VDIF test file. Use for debugging only.
    
    Parameters
    ----------
     filename
         path to VDIF test file.
     bw
         bandwidth of each channel.
     only_station
         filter frames corresponding to this station (-1 for no filtering).
     packet_limit
         maximum number of frames to read (specify at least as many as threads).
     same_figure
         one channel per figure if 0, all channels into one figure if 2.
    
    Returns
    -------
     N/A
     
    Notes
    -----
    |
    | **Assumptions:**
    |
    |  Assuming real data.
    |  Using linear dequantizer.
    """
    f_read=open(filename,'rb')
    keep_reading=1
    SHOW_ERRORS = 0

    freq_sample = 2*bw
    packet_count = 0
    reader = f_read #sys.stdin

    list_threads = []
    list_signals = []
    while keep_reading==1:
        packet_count += 1
        if packet_limit != -1:
            if packet_count == packet_limit:
                keep_reading = 0
       
        # For reading bytes in python3, but hadoop works with python2
        #reader = reader.detach()
        

        [header,allsamples,check_size_samples] = read_vdif_frame(reader,SHOW_ERRORS)
        
        

        
        if not(header == None):
            
            # Decode header
            [seconds_fr,invalid,legacy,ref_epoch,frame_num,vdif_version,log_2_channels,
                frame_length,data_type,bits_per_sample,thread_id,station_id] = header
            
            #print(header)
            #print(station_id)
            #print(only_station)
            
            # Only for this station
            if station_id == only_station or only_station==-1:

                num_channels = 2**log_2_channels
                
                if thread_id not in list_threads:
                    list_threads += [thread_id]
                    #list_threads.index(thread_id)
                    list_signals += [None] * num_channels
                    
                for channel in range(num_channels):
                    # Take samples corresponding to applicable channel
                    # Starting at [channel], step of [num_channels]

                    index_signal = list_threads.index(thread_id)*num_channels+channel
                    if list_signals[index_signal] == None:
                        list_signals[index_signal] = allsamples[channel:][::num_channels]
                    else:
                        list_signals[index_signal] += allsamples[channel:][::num_channels]
                    
                    
        else:
            keep_reading=0

    f_read.close()  
    
    if list_threads!=[]:
        print(list_threads)
        print(len(list_signals))
        tot_channels = (max(list_threads)+1)*num_channels
                    
        print(len(list_signals[0]))
        dequant_signals = []
        for signal_in in list_signals:
            dequant_signals += [simple_dequantizer(signal_in,bits_per_sample)]
    
    
        if same_figure ==1:
            plot_fft_set(dequant_signals,freq_sample,"Signal channels (nchannels = " + str(tot_channels) + ") (FFT)",overlay=0)
        elif same_figure ==0:
            count_channel = -1
            for tid in range(max(list_threads)+1):
                for cid in range(num_channels):
                    count_channel += 1
                    plot_fft_set([dequant_signals[count_channel]],freq_sample,\
                                 "Signal channel (thread = " + str(tid) + ", channel = " + str(cid) + ") (FFT)",overlay=0)
        else:
            plot_fft_set(dequant_signals,freq_sample,"Signal channels (nchannels = " + str(tot_channels) + ") (FFT)",overlay=1)
    






    
#if __name__ == '__main__':
#   main()    

# <codecell>


