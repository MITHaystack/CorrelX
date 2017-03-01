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
#File: lib_ini_exper.py.
#Author: A.J. Vazquez Alvarez (ajvazquez@haystack.mit.edu)
#Description: 
"""
Routines for processing the experiment configuration.

"""
#History:
#initial version: 2016.10 ajva
#MIT Haystack Observatory

from __future__ import print_function

import imp
import sys
import os


import lib_ini_files
imp.reload(lib_ini_files)
from lib_ini_files import *

import lib_delay_model
imp.reload(lib_delay_model)

import lib_vdif
imp.reload(lib_vdif)

import lib_acc_comp
imp.reload(lib_acc_comp)


##################################################################
#
#                      .ini file processing
#
##################################################################





def process_ini_correlation(params_array_corr,file_log,v=1):
    """
    Process correlation.ini file.
    
    Parameters
    ----------
     params_array_corr
         list with correlation configuration.
     file_log
         handler for log file.
     v
         verbose if 1.
     
    Returns
    -------
     stations : int
         number of stations.
     fft_size : int
         number of coefficients in DFT.
     accumulation_time : str(float)
         accumulation time in seconds.
     windowing : str
         window type to be applied before DFT.
     mjd_start : int
         MJD for the start of the scan.
     seconds_start : int
         number of seconds in MJD for the start of the scan.
     seconds_duration : int
         duration of the scan in seconds.
     phase_calibration : int
         1 if phase calibration tones will be extracted, 0 otherwise.
     auto_stations : int
         1 if compute autocorrelations, 0 otherwise.
     auto_pols : int
         1 if crosspolarizations (see msvf.py).
     first_frame_num : int
         [testing only] start reading after this frame id (-1 to read from first frame).
     num_frames : int
         [testing only] read this number of frames per second (-1 to read all frames)
     num_pols : int
         [testing only] default -1.
    """
    
    if v==1:
        print(" Processing (correlation).ini file",file=file_log) 
        
    stations = int(get_param_serial(params_array_corr,C_INI_CR_S_ELEMENTS,C_INI_CR_STATIONS))
    fft_size = int(get_param_serial(params_array_corr,C_INI_CR_S_COMP,C_INI_CR_FFT))
    accumulation_time = get_param_serial(params_array_corr,C_INI_CR_S_COMP,C_INI_CR_ACC)
    windowing = get_param_serial(params_array_corr,C_INI_CR_S_COMP,C_INI_CR_WINDOW)
    mjd_start = int(get_param_serial(params_array_corr,C_INI_CR_S_TIMES,C_INI_CR_MJD))
    seconds_start = int(get_param_serial(params_array_corr,C_INI_CR_S_TIMES,C_INI_CR_START))
    seconds_duration = float(get_param_serial(params_array_corr,C_INI_CR_S_TIMES,C_INI_CR_DURATION))
     
    phase_cal_str=get_param_serial(params_array_corr,C_INI_CR_S_COMP,C_INI_CR_PC)
    if phase_cal_str=="yes":
        phase_calibration=1
    else:
        phase_calibration=0
    
    # Autocorrelation same station
    str_autocorr=get_param_serial(params_array_corr,C_INI_CR_S_ELEMENTS,C_INI_CR_AUTO_ST)
    if str_autocorr=="no":
        auto_stations=0
    else:
        auto_stations=1
        
    # Correlation for different polarizations
    str_crosspol=get_param_serial(params_array_corr,C_INI_CR_S_ELEMENTS,C_INI_CR_CROSS_POL)
    if str_crosspol=="no":
        auto_pols=2
    else:    
        auto_pols=1
    
    # Use only for testing...
    first_frame_num = -1       # If >-1 do not read frames with smaller id than this (for every second)
    num_frames = -1            # If >-1 do not read more than this number of frames (for every second)
    num_pols = -1
    
    
    if v==1:
        print(" FFT size: \t\t\t" + str(fft_size),file=file_log)
        print(" Accumulation time [s]: \t\t" + str(accumulation_time),file=file_log)
    

    return([stations,fft_size,accumulation_time,windowing,mjd_start,seconds_start,seconds_duration,phase_calibration,\
            auto_stations,auto_pols,\
            first_frame_num,num_frames,num_pols])

            
def process_ini_media(params_array_media,data_dir,v,file_log):
    """
    Process media.ini file.
    
    Parameters
    ----------
     params_array_corr : list
         list with media configuration.
     data_dir : str
         folder with media files.
     v : int
         verbose if 1.
     file_log : file handler
         handler for log file.
     
    Returns
    -------
     v_stations : list of str
         all stations ids corresponding to all the media files listed.
     max_num_channels : int
         maximum number of channels per media file.
     max_num_pols : int
         maximum number of polarizations per media file.
     input_files : list of str
         filenames for media files. 
     total_frames : int
         total frames considering all media files.
     max_packet_size : int
         maximum frame size per media file.
     error_str_v :  list of str
         errors on missing media files ([] if no errors).
    
    Notes
    -----
    |
    | **Considerations:**
    |
    |  Currently assuming that all the media files have frames with the same size.
    |  Currently assuming that all the files listed are processed, and that there is a section for each
    |    of the listed files.
    """
    v_stations = get_all_values_serial(params_array_media,C_INI_MEDIA_STATION)
    
    total_frames=0
    input_files=get_val_vector(params_array_media,C_INI_MEDIA_S_FILES,C_INI_MEDIA_LIST)
    max_packet_size=0
    
    if v==1:
        print(" Checking maximum packet size for the hdfs block size...",file=file_log)
    
    
    # Compute number of partitions
    
    max_num_channels=0
    max_num_pols=0
    error_str_v=[]
    total_partitions=-1
    for fi in input_files:
        
        if not os.path.isfile(data_dir+fi):
            error_str_v+=["ERROR: Media file "+data_dir+fi+" does not exist!"]
        else:
        
            vdif_stats=lib_vdif.get_vdif_stats(data_dir+fi,packet_limit=1,offset_bytes=0,only_offset_once=0,v=0)
            packet_size=vdif_stats[4]
            if packet_size>max_packet_size:
                max_packet_size=packet_size
        
            channels_asoc_vector=[int(val) for val in get_param_eq_vector(params_array_media,fi,C_INI_MEDIA_CHANNELS)]
            num_channels=len(set(channels_asoc_vector))
            num_channels_frame=len(channels_asoc_vector)
            if num_channels>max_num_channels:
                max_num_channels=num_channels
            pols_asoc_vector=[int(val) for val in get_param_eq_vector(params_array_media,fi,C_INI_MEDIA_POLARIZATIONS)]
            num_pols=len(set(pols_asoc_vector))
            if num_pols>max_num_pols:
                max_num_pols=num_pols
    
            total_frames += os.path.getsize(data_dir+fi)//packet_size
        
    
        
    if v==1:
        print(" Max packet size: "+str(max_packet_size),file=file_log)
        print(" Max num channels: "+str(max_num_channels),file=file_log)
        print(" Max num polarizations: "+str(max_num_pols),file=file_log)
        print(" Total frames: "+str(total_frames),file=file_log)
        print(" Input files: \t" + str(input_files),file=file_log)
    
    
    return([v_stations,max_num_channels,max_num_pols,\
             input_files,total_frames,max_packet_size,error_str_v])  



def process_ini_stations(params_array_stations,v_stations):
    """
    Process stations.ini file.
    
    Parameters
    ----------
     params_array_corr
         list with stations configuration.
     v_stations
         list of str with all stations ids corresponding to all the media files listed (from media).
     
    Returns
    -------
     v_id_stations
         list of int with the ids corresponding to v_stations.
    """
    v_id_stations = []
    for i_st in v_stations:
        v_id_stations += [str(get_param_serial(params_array_stations,i_st,C_INI_ST_ID))]
    return(v_id_stations)


def get_num_partitions_red(one_baseline_per_task,accumulation_time,signal_duration,stations,auto_stations,\
                           max_num_channels,max_num_pols,v,file_log):
    """
    Get number of reducers based on the experiment configuration.
    
    Parameters
    ----------
     one_baseline_per_task : int
         [default 0] 0 for all baselines per task, 1 for single baseline per task.
     accumulation_time : str(float)
         accumulation period in seconds.
     signal_duration : str(float)
         signal duration in seconds.
     stations : int
         number of stations.
     auto_stations : int
         1 if compute autocorrelations, 0 otherwise.
     max_num_channels : int
         maximum number of channels per media file.
     max_num_pol : int
         maximum number of polarizations per media file.
     v : int
         verbose if 1.
     file_log : file handler
         handler to log file.
     
    Returns
    -------
     total_partitions : int
         number of reducers.
     
    Notes
    -----
     This function is critical (together with the custom partitioner) to parallelize the 
          reduce phase and  keep a well balanced load among the reducers.
    """
    num_accs = lib_acc_comp.get_tot_acc_blocks(accumulation_time,signal_duration)
    if one_baseline_per_task:
        num_baselines=max(1,stations*(stations-1+auto_stations)/2)
        total_partitions = num_baselines*max_num_channels*max_num_pols*num_accs
    else:
        #total_partitions = num_channels*num_accs
        total_partitions = max_num_channels*num_accs
    
    if v==1:
        print(" Acc. periods: "+str(num_accs),file=file_log)
    
    return(total_partitions)




def process_ini_files(data_dir, ini_stations, ini_sources, ini_delay_model, ini_delays, ini_media, ini_correlation,\
                      one_baseline_per_task,v=1,file_log=sys.stdout):
    """
    Process .ini files.
    
    Parameters
    ----------
     data_dir : str
         path to folder with media files.
     ini_stations : str
         path to stations.ini.
     ini_sources : str
         path to sources.ini.
     ini_delay_model : str
         path to delay_model.ini.
     ini_delays : str
         path to delays.ini.
     ini_media : str
         path to media.ini.
     ini_correlation : str
         path to correlation.ini.
     one_baseline_per_task : int
         default 0.
     v : int
         verbose if 1.
     file_log : file handler
         handler for log file.
     
    Returns
    -------
     stations_serial_str : str
         serialized version of stations.ini.
     media_serial_str : str
         serialized version of media.ini.
     correlation_serial_str : str
         serialized version of correlation.ini.
     delays_serial_str : str
         serialized version of delays.ini.
     auto_stations : int
         1 if compute autocorrelations, 0 otherwise.
     auto_pols : int
         1 if compute cross-polarization correlations, 2 if only same-polarization correlations (see msvf.py).
     fft_size : int
         requested number of coefficients in the FFT.
     accumulation_time : str
         integration period duration [s].
     stations : int
         number of stations (it will be used to compute number of mappers...).
     ref_epoch : int
         MJD for the start of the scan.
     signal_start : int
         seconds after MJD for the start of the scan.
     signal_duration : float
         duration of the scan [s].
     input_files : list of str
         filenames for media files.
     first_frame_num : int
         [default -1] This can be used to force the mapper to start reading frames after this id.
     num_frames : int
         [default -1] This can be used to force the mapper to read only this number of frames for each second.
     codecs_serial : str
         [default "0"] Serialized version of the codebook if using compression.  
     max_packet_size : int
         number of bytes in the largest frame of the media files.
     total_frames : int
         number of frames in all the media files.
     total_partitions : int
         number of reducer tasks.
     windowing : str
         windowing as defined in correlation.ini.
     phase_calibration : int
         1 if extract phase calibration tones, 0 otherwise.
     delay_error : None or str
         None if no errors during delay computations, str with error otherwise. 
     error_str_v : 
         list of str with errors for media files not existing.
     num_pols : int
         -1
    
    Notes
    -----
    |
    | **TO DO:**
    |
    |  Remove num_pols.
    |  Remove vq.
    """
    
    if v==1:
        print("\nProcessing *.ini files",file=file_log) 
    

    # media.ini
    media_serial_str = serialize_config(ini_media)
    params_array_media = serial_params_to_array(media_serial_str)
    [v_stations,max_num_channels,max_num_pols,\
             input_files,total_frames,max_packet_size,error_str_v] = process_ini_media(params_array_media,data_dir,v,file_log)
    
    
    # stations.ini
    stations_serial_str=serialize_config(ini_stations)
    params_array_st=serial_params_to_array(stations_serial_str)
    v_id_stations = process_ini_stations(params_array_st,v_stations)
    
    # correlation.ini
    correlation_serial_str = serialize_config(ini_correlation)
    params_array_corr = serial_params_to_array(correlation_serial_str)
    [stations,fft_size,accumulation_time,windowing,mjd_start,seconds_start,seconds_duration,phase_calibration,\
            auto_stations,auto_pols,\
            first_frame_num,num_frames,num_pols] = process_ini_correlation(params_array_corr,file_log,v)
    
    # TO DO: merge these three pairs into three variables
    ref_epoch = mjd_start
    signal_start = seconds_start
    signal_duration = seconds_duration
    
    
    # Read ini file (media.ini) and serialize)

    sources_serial_str=serialize_config(ini_sources)
    delay_model_serial_str=serialize_config(ini_delay_model)
    
    params_array_so=serial_params_to_array(sources_serial_str)
    params_array_delay_model=serial_params_to_array(delay_model_serial_str)

    
    #model_type = get_param_serial(params_array_corr,C_INI_CR_S_DELAYS,C_INI_CR_MODEL)
    model_type = C_INI_CR_M_FILE


    seconds_per_step = float(accumulation_time)
    print(" Delay computations for every acc period!")
    tot_steps=int(np.ceil(seconds_duration/seconds_per_step))
    #correlation.ini
    delay_error=None
        
        
   
            
    if v==1:
        print("\n Recomputing delays:",file=file_log) 
        print("  MJD: "+str(mjd_start),file=file_log)  
        print("  seconds start: "+str(seconds_start),file=file_log)  
        print("  tot steps: "+str(tot_steps),file=file_log) 
        print("  seconds per step: "+str(seconds_per_step),file=file_log) 
        print("  output file: "+ini_delays,file=file_log)
        print("  (Silent output for delay libs)",file=file_log) 
        print(" ",file=file_log) 
    
    
    # TO DO: remove?
    if model_type==C_INI_CR_M_SIMPLE or model_type=="":
        # Use simple model
        if v==1:
             print("  Using simple model",file=file_log) 
        gen_delays_ini_file(file_stations_ini=ini_stations,params_array_stations=params_array_st,file_sources_ini=ini_sources,\
                                params_array_sources=params_array_so,\
                                file_delays_ini=ini_delays,params_array_delay_model=params_array_delay_model,\
                                mjd_start=mjd_start,\
                                tot_steps=tot_steps,step_seconds=seconds_per_step, v=0,seconds_offset=seconds_start)   
        
    else:
        # Use model from file
        if v==1:
             print("  Using model from file",file=file_log) 
    
        delay_error=gen_delays_ini_file(file_stations_ini=ini_stations,params_array_stations=params_array_st,\
                                file_sources_ini=ini_sources,\
                                params_array_sources=params_array_so,\
                                file_delay_model_ini=ini_delay_model,params_array_delay_model=params_array_delay_model,\
                                file_delays_ini=ini_delays,mjd_start=mjd_start,\
                                seconds_ref=seconds_start,tot_steps=tot_steps,step_seconds=seconds_per_step, v=0)
        
        

        
    if delay_error is not None:
        delays_serial_str=serialize_config(ini_delays) 
    else:
        delays_serial_str=""
        
    
 
    total_partitions = get_num_partitions_red(one_baseline_per_task,accumulation_time,signal_duration,stations,auto_stations,\
                                              max_num_channels,max_num_pols,v,file_log)
        
    
    


    
    # VQ
    # Codecs for compression
    # Generate serialized version of station names and corresponding codecs (codebooks)
    
    
    
    # 2016.08.22: disabled compression
    #v_bits_sample = get_all_values_serial(params_array_media,C_INI_MEDIA_BITS_SAMPLE)
    #v_compression = get_all_values_serial(params_array_media,C_INI_MEDIA_COMPRESSION)
    #v_codecs = get_all_values_serial(params_array_media,C_INI_MEDIA_C_CODECS)
    v_compression=["no"]
    
    if "no" in v_compression:
        codecs_serial = "0"
    else:
        codecs_serial = get_group_of_serialized_codebooks_noencoding(v_stations,v_id_stations,v_codecs,v_bits_sample)
    
    

    
    return([stations_serial_str,media_serial_str,correlation_serial_str,delays_serial_str,\
            auto_stations, auto_pols, fft_size, accumulation_time, stations, ref_epoch, signal_start, signal_duration,input_files,\
            first_frame_num,num_frames,codecs_serial,max_packet_size,total_frames,total_partitions,windowing,\
            phase_calibration,delay_error,error_str_v,num_pols])



def check_errors_ini_exper(data_dir,ini_folder,ini_stations,ini_sources,ini_delay_model,ini_media):
    """
    Check experiment configuration for errors. Currently simply checking existence of paths and files.
    
    Parameters
    ----------
     data_dir : str
         path to media folder.
     ini_folder : str
         path to experiment folder.
     ini_stations : str
         path to stations.ini (absolute).
     ini_sources : str
         path to sources.ini (absolute).
     ini_delay_model : str
         path to delay_model.ini (absolute).
     ini_media : str
         path to media.ini (absolute)
     
    Returns
    -------
     init_success : int
         0 if errors, 1 otherwise.
     
    Notes
    -----
    |
    | **TO DO:**
    |
    |  Add more checks. Consider checking ini files syntax, range of values, etc.
    """
    init_success=1
    # Check if input folder exists
    if not os.path.isdir(data_dir):
        init_success=0
        print("\nERROR: Input data dir does not exist! ("+data_dir+")")
    
    # Check if experiment folder exists
    elif not os.path.isdir(ini_folder):
        init_success=0
        print("\nERROR: Experiment dir does not exist! ("+ini_folder+")")
    
    # Check if initialization files for experiment exist
    else:
        for ini_file,t_file in zip([ini_stations,ini_sources,ini_delay_model,ini_media],\
                                   ["stations","sources","delay model","media"]):
            if not os.path.isfile(ini_file):
                init_success=0
                print("ERROR: Initialization file for "+t_file+" does not exist! ("+ini_file+")")

    return(init_success)



def gen_delays_ini_file(file_stations_ini,params_array_stations,file_sources_ini,params_array_sources,\
                        file_delay_model_ini,params_array_delay_model,file_delays_ini,mjd_start,seconds_ref,\
                        tot_steps,step_seconds=1,seconds_offset=0, v=1):
    """
    Generate file with delay polynomials from delay model.

    Parameters
    ----------
     file_stations_ini : str
         path to stations.ini.
     params_array_stations : list
         configuration of stations.ini.
     file_sources_ini : str
         path to sources.ini.
     params_array_sources : list
         configuration of sources.ini.
     file_delay_model_ini : str
         path to delay_model.ini.
     params_array_delay_model : list
         configuration of delay_mode.ini.
     file_delays_ini : str
         path to delays.ini [will write].
     mjd_start : int
         MJD for the start of the scan.
     seconds_ref : int
         seconds in MJD for the start of the scan.
     tot_steps : int
         number of accumulation periods in the scan.
     step_seconds : str(float)
         accumulation period [s].
     seconds_offset
         0
     v : int
         verbose if 1.
     
    Returns
    -------
     s_delay
         None if error, ini configuration to write if successful.
    """
    
    # Config parsers
    s_st = configparser.ConfigParser()
    s_st.optionxform=str
    s_st.read(file_stations_ini) #('stations.ini')
    s_so = configparser.ConfigParser()
    s_so.optionxform=str
    s_so.read(file_sources_ini) #('sources.ini')
    s_delay = configparser.ConfigParser()
    s_delay.optionxform=str


    seconds_offset=0
    if v==1:
        print("offset:"+str(seconds_offset))

    s_delay=lib_delay_model.compute_initial_delays(params_array_delay_model,params_array_stations,s_st,s_so,s_delay,\
                                                mjd_start,seconds_ref,tot_steps,step_seconds,seconds_offset=0,v=v,\
                                                file_ini=file_delays_ini)
    
    if s_delay is not None:
        # Write configuration to file
        with open(file_delays_ini, 'w') as configfile:
            s_delay.write(configfile)
    
        if v==1:
            print("Created delays file: "+file_delays_ini)
    return(s_delay)

# <codecell>


