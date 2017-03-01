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
#File: lib_mapredcorr.py.
#Author: A.J. Vazquez Alvarez (ajvazquez@haystack.mit.edu)
#Description: 
"""
Main functions for performing the mapreduce correlation through Hadoop (and pipeline).

"""
#History:
#initial version: 2015.12 ajva
#MIT Haystack Observatory

from __future__ import print_function
import sys
import time
import os
import imp

# Constants for mapper and reducer
import const_mapred
imp.reload(const_mapred)
from const_mapred import *

import const_hadoop
imp.reload(const_hadoop)
from const_hadoop import *

import lib_profiling
imp.reload(lib_profiling)



##################################################################
#
#                  Map and reduce caller scripts
#
##################################################################



def get_mapper_params_str(stations,num_pols,fft_size,accumulation_time,signal_start,signal_duration,\
                          first_frame_num,num_frames,codecs_serial,auto_stations,\
                          auto_pols,ini_stations,ini_media,ini_delays,fft_at_mapper,\
                          internal_log_mapper,ffts_per_chunk,windowing,\
                          one_baseline_per_task,phase_calibration,min_mapper_chunk,max_mapper_chunk,\
                          task_scaling_stations,single_precision):
    """
    Returns string with all the parameters to call the mapper.
    
    Parameters
    ----------
     stations
         number of stations.
     num_pols
         [not used in all-baselines-per-task mode] number of polarizations.
     fft_size
         [not used if fft in reducer?] fft length from the configuration.
     accumulation_time
         duration of the accumulation period (float) [s] .
     signal_start
         start time for the experiment (float) [s] .
     signal_duration
         duration of the experiment (float) [s] .
     first_frame_num
         [only testing] -1 by default. Discard frames with id lesser than this value.
     num_frames
         [only testing] -1 by default. If >0 discard frames with id greater than this value. 
                              If both first_frame_num and num_frames are <0, all frames are processed.
     codecs_serial
         [only testing] "" by default. Serialized version of codecs used for comrpession.
     auto_stations
         [not used in all-baselines-per-task mode] controls pairs generation (see msvf.calculate_corr_pairs())
     auto_pols
         [not used in all-baselines-per-task mode] controls pairs generation (see msvf.calculate_corr_pairs())
     ini_stations
         string with stations ini file name.
     ini_media
         string with media ini file name.
     ini_delays
         string with delays ini file name.
     fft_at_mapper
         [0 by default]. Initially devised to allow configuration of FFT in mapper or reducer.
     internal_log_mapper
         [unused]
     ffts_per_chunk
         [-1 by default]. If -1, all the samples in the channel go into the same line. Other values allow to 
                            control the number of samples that go into the same line, but this feature is discontinued.
     windowing : str
         window before FFT, default value is "square".
     one_baseline_per_task: bool
         boolean to activate one-baseline-per-task mode.
     phase_calibration
         [unused].
     min_mapper_chunk
         [-1 by default]
     max_mapper_chunk
         [-1 by default]
     task_scaling_stations
         0 for all-baselines-per-task mode, 1 to activate linear scaling with number of stations.
     single_precision
         [unused]
    
    Returns
    -------
     mapper_params_str : str
         parameters to call mapper.
    
    Notes:
    ------
    |
    | **TO DO:**
    |
    |  Automate finding max number of polarizations.
    |  Remove all unused arguments.
    |  Consider adding paths for long ini files, currently this is done only for delays.ini.
    |  Consider removing fft_size, fft always in reducer.
    """
    mapper_params_str = str(stations) + " " +\
                        str(num_pols) + " " +\
                        str(fft_size) + " " +\
                        str(accumulation_time) + " " +\
                        str(signal_start)+  " " +\
                        str(signal_duration)+  " " +\
                        str(first_frame_num)+  " " +\
                        str(num_frames)+  " " +\
                        str(auto_stations) + " " +\
                        str(auto_pols)+ " " +\
                        "1" + " " +\
                        "'"+ini_stations+"'"+" " +\
                        "'"+ini_media+"'"+ " " +\
                        "'"+ini_delays+"'"+ " "+\
                        "'"+codecs_serial +"'" + " " +\
                        str(int(fft_at_mapper))+ " " + \
                        str(internal_log_mapper)+ " " + \
                        str(ffts_per_chunk)+ " " + \
                        "'"+windowing+"'"+ " " + \
                        str(int(one_baseline_per_task))+ " " + \
                        str(int(phase_calibration))+ " " + \
                        str(min_mapper_chunk)+ " " + \
                        str(max_mapper_chunk)+ " " + \
                        str(int(task_scaling_stations))+ " " + \
                        str(int(single_precision))
    return(mapper_params_str)


def get_reducer_params_str(codecs_serial,fft_at_mapper,internal_log_reducer,fft_size,windowing,phase_calibration,single_precision):
    """
    Returns string with all the parameters to call the reducer.
    
    Parameters
    ----------
     codecs_serial
         [only testing] "" by default. Serialized version of codecs used for comrpession.
     fft_at_mapper
         same variable used to call get_mapper_params_str(), inverted here.
     internal_log_reducer
         [unused]
     fft_size
         fft length from the configuration.
     windowing
         window type before FFT, "square" by default.
     phase_calibration
         if 1 phase calibration tones will be extracted.
     single_precision
         boolean to control data types for unpacked samples.
     
    Returns
    -------
     reducer_params_str : str
         parameters to call mapper.
    
    Notes
    -----
    |
    | **TO DO:
    |
    |  Remove unused parameters
    """
    reducer_params_str = "'"+codecs_serial +"'"+ " " + \
                        str(int(not(fft_at_mapper)))+ " " + \
                        str(internal_log_reducer)+ " " + \
                        str(fft_size)+ " " + \
                        "'"+windowing+"'"+ " " + \
                        str(int(phase_calibration))+ " " + \
                        str(int(single_precision))
                        
    return(reducer_params_str)




def create_inter_sh(filename,python_x,command,temp_log,v=0,file_log=sys.stdout):
    """
    Create script with python call for mapper/reducer.
    Devised to avoid issues with arguments.
    Currently used for creating the mapper and reducer scripts passed to hadoop.
    
    Parameters
    ----------
     filename
         filename for resulting bash script with python call.
     python_x
         python executable.
     command
         python script plus arguments.
     temp_log
         path to temporary (buffer) file for system calls.
     v
         verbose if 1.
     file_log
         handler for log file.
     
    Returns
    -------
     N/A
    """
    os.system("echo $LD_LIBRARY_PATH > " + temp_log)
        
    line = "\""
    with open(temp_log, 'r') as f_log:
        for line_in in f_log:
            line+=line_in.strip()
    line+="\""
 

    os.system("echo $PYTHONPATH > " + temp_log)
    line2 = "\""
    with open(temp_log, 'r') as f_log:
        for line_in in f_log:
            line2+=line_in.strip()
    line2+="\""


    with open(filename, 'w') as f:
        print("#!/bin/bash",file=f)
        print("export LD_LIBRARY_PATH=" + line,file=f)
        print("export PYTHONPATH=" + line2,file=f)
        #print(python_x+" --version")
        print(python_x + " " + command,file=f)

    if v==1:
        print("\nCreating script file:",file=file_log)
            
        with open(filename, 'r') as f:
            for line in f:
                print(" "+line.strip(),file=file_log)






def get_mr_command(app_dir,script,params):
    """
    Script for creating line with call to mapper/reducer with parameters.
    """
    command =  app_dir + script + " " + params 
    return(command)




##################################################################
#
#                      Pipeline
#
##################################################################



def pipeline_app(python_x,stations,input_files,app_dir,mapper,reducer,fft_size,fft_at_mapper,accumulation_time,\
                 signal_start,signal_duration,first_frame_num,num_frames,\
                 data_dir,output_dir,output_sym,auto_stations,auto_pols,num_pols,\
                 codecs_serial,v=0,file_log=sys.stdout,\
                 file_out="vt4.txt",ini_stations="none",\
                 ini_media="none",ini_delays="none",internal_log_mapper=1,internal_log_reducer=1,ffts_per_chunk=1,\
                 windowing="square",one_baseline_per_task=True,phase_calibration=0,min_mapper_chunk=-1,\
                 max_mapper_chunk=-1,task_scaling_stations=0,sort_output=1,single_precision=0,profile_map=0,profile_red=0,timestamp_str=""):
    """
    Perform correlation through pipeline execution (that is, without hadoop). All the data is passed through the mapper, 
    then the results are sorted and passed through the reducer.
                                                                                           
    Parameters
    ----------
     See table below.
    
    Returns
    -------
     list with start time, end time and duration of the execution in seconds.     
     
    Notes
    -----
     Note that the environment variable map_input_file is modified for each processed file to emulate the hadoop behavior 
        (and thus provide access to the mapper to the name of the file currently being processed.
    
    +-------------------------+----------------------------+---------------------------+
    |                         |   get_mapper_params_str()  |  get_reducer_params_str() |
    +=========================+============================+===========================+
    |  stations               |        x                   |                           |
    +-------------------------+----------------------------+---------------------------+
    |  fft_size               |        x                   |      x                    |
    +-------------------------+----------------------------+---------------------------+
    |  fft_at_mapper          |        x                   |      x                    |
    +-------------------------+----------------------------+---------------------------+    
    |  accumulation_time      |        x                   |                           |                        
    +-------------------------+----------------------------+---------------------------+
    |  signal_start           |        x                   |                           |
    +-------------------------+----------------------------+---------------------------+
    |  signal_duration        |        x                   |                           |
    +-------------------------+----------------------------+---------------------------+
    |  first_frame_num        |        x                   |                           |
    +-------------------------+----------------------------+---------------------------+
    |  num_frames             |        x                   |                           |
    +-------------------------+----------------------------+---------------------------+
    |  auto_stations          |        x                   |                           |
    +-------------------------+----------------------------+---------------------------+
    |  auto_pols              |        x                   |                           |
    +-------------------------+----------------------------+---------------------------+
    |  num_pols               |        x                   |                           |
    +-------------------------+----------------------------+---------------------------+
    |  codecs_serial          |        x                   |       x                   |
    +-------------------------+----------------------------+---------------------------+
    |  ini_stations           |        x                   |                           |
    +-------------------------+----------------------------+---------------------------+
    |  ini_media              |        x                   |                           |
    +-------------------------+----------------------------+---------------------------+
    |  ini_delays:            |        x                   |                           |
    +-------------------------+----------------------------+---------------------------+
    |  internal_log_mapper:   |        x                   |                           |
    +-------------------------+----------------------------+---------------------------+
    |  internal_log_reducer:  |                            |       x                   |
    +-------------------------+----------------------------+---------------------------+
    |  ffts_per_chunk:        |        x                   |                           |
    +-------------------------+----------------------------+---------------------------+
    |  windowing:             |        x                   |       x                   |
    +-------------------------+----------------------------+---------------------------+
    |  one_baseline_per_task: |        x                   |                           |
    +-------------------------+----------------------------+---------------------------+
    |  phase_calibration:     |        x                   |       x                   |
    +-------------------------+----------------------------+---------------------------+
    |  min_mapper_chunk:      |        x                   |                           |
    +-------------------------+----------------------------+---------------------------+
    |  max_mapper_chunk:      |        x                   |                           |
    +-------------------------+----------------------------+---------------------------+
    |  task_scaling_stations: |        x                   |                           |
    +-------------------------+----------------------------+---------------------------+
    |  single_precision:      |        x                   |       x                   |
    +-------------------------+----------------------------+---------------------------+
    |  python_x:              |  str with python executable.                           |
    +-------------------------+----------------------------+---------------------------+
    |  input_files:           | list with filenames for the media.                     |
    +-------------------------+----------------------------+---------------------------+
    |  app_dir:               | path with the location of the .py files for mapper,    |
    |                         |     reducer and all their dependencies.                |
    +-------------------------+----------------------------+---------------------------+
    |  mapper:                | mapper .py filename.                                   |
    +-------------------------+----------------------------+---------------------------+
    |  reducer:               | reducer .py filename.                                  |
    +-------------------------+----------------------------+---------------------------+
    |  data_dir:              | path with the location for the media.                  |
    +-------------------------+----------------------------+---------------------------+
    |  output_dir:            | path for the intermediate and output files.            |
    +-------------------------+----------------------------+---------------------------+
    |  output_sym:            | path for the symbolic link to the output file          |
    |                         |      (typically sub-path in experiment folder).        |
    +-------------------------+----------------------------+---------------------------+
    |  v:                     | boolean to activate verbose mode.                      |
    +-------------------------+----------------------------+---------------------------+
    |  file_log:              | file for logging.                                      |
    +-------------------------+----------------------------+---------------------------+
    |  file_out:              | output file.                                           |
    +-------------------------+----------------------------+---------------------------+
    |  sort_output:           | [0 by default] boolean to activate the sorting of the  |
    |                         |      output.                                           |
    +-------------------------+----------------------------+---------------------------+
    |  profile_map:           | [0 by default] 1 to profile mapper using pycallgraph,  |
    |                         |                2 to profile using cProfile.            |
    +-------------------------+----------------------------+---------------------------+
    |  profile_red:           | [0 by default] 1 to profile reducer using pycallgraph, |
    |                         |                2 to profile using cProfile.            |
    +-------------------------+----------------------------+---------------------------+
     
    
    """
    if v==1:
        print("\nPipeline execution...",file=file_log)

    files_str=""
    files_out_str=""
    command=""
    str_cprof_conv=" "
    # Map for every file, setting the environment variable map_input_file (to access the filename). This environment variable is 
    #  generated by hadoop, so we manually create it in the pipeline execution.
    i=0
    for input_file in input_files: #range(stations):
        i+=1
        file_str =  data_dir + input_file #prefix_files + "-" + str(i) + ".vt"
        file_out_str = output_dir + file_out + "part" + str(i)
        files_str += " " + file_str
        command += "export "+C_H_ENV_MAP_INPUT_FILE+"="+ file_str + " && "
        command += "cat " + file_str + "|"
        if profile_map==1:
            i_args = lib_profiling.get_include_functions(str(app_dir+mapper),profile_memory=0)
            command += "PYTHONPATH="+app_dir+":$PYTHONPATH "+lib_profiling.get_pycallgraph_str(i_args,file_out_str+"_"+input_file+"_map_prof_"+timestamp_str+".png ")
        elif profile_map==2:
            str_map_cprof = file_out_str+"_"+input_file+"_map_prof_"+timestamp_str+".cprof"
            str_map_ctxt = file_out_str+"_"+input_file+"_map_prof_"+timestamp_str+".txt"
            command += python_x+" "+lib_profiling.C_PROFILE_OPTS+" -o "+str_map_cprof+" "
            str_cprof_conv += "&& "+python_x+" "+app_dir+lib_profiling.C_PROFILE_CONVERT_CMD+str_map_cprof+" > "+str_map_ctxt+" "
        else: # 0
            command += python_x+" "
        command += str(app_dir+mapper)
        command += " " + get_mapper_params_str(stations,num_pols,fft_size,accumulation_time,signal_start,signal_duration,\
                            first_frame_num,num_frames,codecs_serial,\
                            auto_stations,auto_pols,ini_stations,ini_media,ini_delays,fft_at_mapper,\
                            internal_log_mapper,ffts_per_chunk,windowing,\
                            one_baseline_per_task,phase_calibration,min_mapper_chunk,max_mapper_chunk,\
                            task_scaling_stations,single_precision)
        command+=" > " + file_out_str + " && " 
        command+="unset "+C_H_ENV_MAP_INPUT_FILE+" && "
        files_out_str += " " + file_out_str
    
    # Reduce (includes sorting and reducing)
    command+= " cat " + files_out_str
    #command+= "|sort -t"+FIELD_SEP+" -k1 -k2 -k3 -k4 -k5 -k6 -k7 -k8 -k9 >" + output_dir + file_out + "_tmp"
    command+= "|sort -t"+FIELD_SEP+" "+COMMON_SORT_ALL_BASELINES_PER_TASK_STR+" >" + output_dir + file_out + "_tmp"
    command += " && cat "+ output_dir + file_out +"_tmp|"
    if profile_red==1:
        i_args = lib_profiling.get_include_functions(str(app_dir+reducer))
        command += "PYTHONPATH="+app_dir+":$PYTHONPATH "+lib_profiling.get_pycallgraph_str(i_args,file_out_str+"_red_prof"+timestamp_str+".png")
    elif profile_red==2:
        str_red_cprof = file_out_str+"_red_prof_"+timestamp_str+".cprof"
        str_red_ctxt = file_out_str+"_red_prof_"+timestamp_str+".txt"
        command += python_x+" "+lib_profiling.C_PROFILE_OPTS+" -o "+str_red_cprof
        str_cprof_conv += "&& "+python_x+" "+app_dir+lib_profiling.C_PROFILE_CONVERT_CMD+str_red_cprof+" > "+str_red_ctxt+" "
    else:
        command += python_x
    command += " " + str(app_dir+reducer) 
    command += " " + get_reducer_params_str(codecs_serial,fft_at_mapper,internal_log_reducer,fft_size,windowing,\
                                           phase_calibration,single_precision) + " "
    if sort_output:
        command+= "|sort > " + output_dir + file_out
    else:
        command+= " > " + output_dir + file_out

    command+=str_cprof_conv
    
    if v==1:
        print("",command,file=file_log)

        
    # Execution times
    start_time = time.time()
    os.system(command)
    end_time = time.time()
    elapsed_time = end_time - start_time

    command_mk = "mkdir "+output_sym
    command_ln = "ln -s "+output_dir+file_out+" "+output_sym+file_out
    os.system(command_mk)
    os.system(command_ln)

    if v==1:
        print(" Elapsed time = "+ str(elapsed_time)+ " s",file=file_log)





    return([start_time,end_time,elapsed_time])








##################################################################
#
#             In-line job submission java options
#
##################################################################


def d_opt(param,value,extra_q=0):
    """
    Create string "-D param=value"
    """
    sym_q=""
    if extra_q==1:
        sym_q="\""
    return("-D "+param+"="+sym_q+value+sym_q+" ")


def get_options_partitioning(field_sep,key_fields,key_field_sep,part_opts,comp_opts):
    """
    Get partitioning options.
    
    Parameters
    ----------
     field_sep
         field separator.
     key_fields
         number of key fields.
     key_field_sep
         key field separator.
     part_opts
         partitioning options.
     comp_opts
         comparator options.
    
    Returns
    -------
     option1 : str
         java options (relative to partitioning) for job submission.
    """

    options_partitioner = ""
    options_partitioner += d_opt(C_H_INLINE_FIELD_SEP,field_sep)
    options_partitioner += d_opt(C_H_INLINE_KEY_FIELDS,str(key_fields))
    options_partitioner += d_opt(C_H_INLINE_KEY_FIELD_SEP,key_field_sep)
    options_partitioner += d_opt(C_H_INLINE_PARTITIONER_OPTS,part_opts,extra_q=1)
    options_partitioner += d_opt(C_H_INLINE_COMPARATOR_OPTS,comp_opts,extra_q=1)
    return(options_partitioner)

def get_options_custom_partitioner(use_nohash_partitioner=1):
    """
    Options for custom partitioner.
    """
    if use_nohash_partitioner==1:
        options_custom_partitioner = " -partitioner "+C_H_INLINE_NOHASH_PARITIONER
    else:
        options_custom_partitioner = " -partitioner "+C_H_INLINE_DEFAULT_PARITIONER
    return(options_custom_partitioner)


def get_options_lustre():
    """
    Options for lustre filesystem.
    """
    options_lustre = ""
    options_lustre += " "+d_opt(C_H_INLINE_LUSTRE_FS_ABS_PARAM,C_H_INLINE_LUSTRE_FS_ABS_VAL)
    options_lustre += " "+d_opt(C_H_INLINE_LUSTRE_FS_PARAM,C_H_INLINE_LUSTRE_FS_VAL)
    options_lustre += " "+d_opt(C_H_INLINE_SHUFFLE_PARAM,C_H_INLINE_SHUFFLE_VAL)
    return(options_lustre)

def get_options_logging(log_properties):
    """
    Options for log4j logging.
    
    Parameters
    ----------
     log_properties
         path to log4j.properties file.
     
    Notes
    -----
    |
    | **TO DO:**
    |
    |  Currently not working, and thus "log_properties" is copied to a specific folder (see un_mapreduce_sh()).
    """
    options_logging = d_opt(C_H_INLINE_LOG_CONF,log_properties)
    options_logging += " "
    return(options_logging)

def get_options_text_delimiter(hadoop_text_delimiter):
    """
    Options for text delimiter.
    """
    options_text_delimiter = " "
    options_text_delimiter += d_opt(C_H_INLINE_TEXT_DELIMITER,hadoop_text_delimiter)
    options_text_delimiter += " "
    return(options_text_delimiter)

def get_options_num_maps(num_maps):
    """
    Total number of mappers for this job.
    """
    return(d_opt(C_H_INLINE_NUM_MAPS,str(num_maps)))
    
def get_options_num_reduces(num_reduces):
    """
    Total number of reducers for this job.
    """
    return(d_opt(C_H_INLINE_NUM_REDUCES,str(num_reduces)))

def get_options_fixed_length_records(record_size):
    """
    Options for fixed length records as input, instead of text.
    
    Notes
    -----
    | 
    | **TO DO:**
    |
    |  This needs work.
    """
    option_fixed_1 = d_opt(C_H_INLINE_FIXED_LENGTH,str(record_size))+" "
    option_fixed_2 = " -inputformat "+C_H_INLINE_FIXED_FORMAT
    return([option_fixed_1,option_fixed_2])



##################################################################
#
#                      Job submission
#
##################################################################



def run_mapreduce_sh(record_size,jobsh,mappersh,reducersh,app_dir,hadoop_dir,hadoop_conf_dir,folder_deps,files_deps,add_deps,\
                  mapper,reducer,hdfs_data_dir,hdfs_output_file,output_hadoop,text_mode,hadoop_text_delimiter,output_dir,output_sym,\
                  temp_log,packets_per_hdfs_block,total_frames,total_partitions,adjust_mappers,adjust_reducers,\
                  use_nohash_partitioner=1,use_lustre_plugin=0,lustre_user_dir="r/ajva/",num_slaves=1,num_vcores=1,\
                  one_baseline_per_task=True,sort_output=1,bm_delete_output=0,bypass_reduce=0,v=0,file_log=sys.stdout):
                  #use_lustre_plugin=0,lustre_user_dir="r/ajva/",num_slaves=1,num_vcores=1,v=0,file_log=sys.stdout):

    """
    Perform correlation through hadoop. Requires hadoop started (see lib_hadoop_hdfs.py).
    
    Parameters
    ----------
     record_size
         [0 by default] If 1 will use fixed length records in Hadoop, testing only.
     jobsh
         bash file to write complete call to hadoop for job submission.
     mappersh
         bash file with complete call to the mapper.
     reducersh
         bash file with complete call to the reducer.
     app_dir
         path to mapper and reducer .py files.
     hadoop_dir
         base path of the hadoop installation.
     hadoop_conf_dir
         path with hadoop configuration files.
     folder_deps
         path to the dependencies for the mapper and the reducer. 
     files_deps
         list of strings with the filenames of the dependencies (.py files).
     add_deps
         list of strings with paths for additional dependencies (.ini files if applicable)
     mapper
         filename of the mapper (.py).
     reducer
         filename of the reducer (.py).
     hdfs_data_dir
         working path in HDFS or Lustre.
     hdfs_output_file
         filename of the output file in the working path (HDFS or Lustre).
     output_hadoop
         filename of the output file in a local folder (output_dir).
     text_mode
         [1 by default] 0 for binary, only testing.
     hadoop_text_delimiter
         text delimiter for input data at mapper, see notes below.
     output_dir
         output folder (local).
     output_sym
         path (local) for the symbolic link to the output file (typically sub-path in experiment folder).
     temp_log
         filename for temporary logging.
     packets_per_hdfs_block
         number of frames per mapper.
     total_frames
         total number of frames in all the VDIF files to be processed.
     total_partitions
         maximum number of reducers.
     adjust_mappers
         force the calculated number of mappers to be multiplied by this factor.
     adjust_reducers
         force the calculated number of reducers to be multiplied by this factor.
     use_nohash_partitioner
         0 for default partitioner, 1 for nohash partitioner (better load balancing).
     use_lustre_plugin
         boolean to allow Hadoop to work directly in Lustre instead of HDFS.
     lustre_user_dir
         absolute path for the Lustre working path.
     num_slaves
         [unused] included to have the option to control the number of mappers reducers based on this.
     num_vcores
         [unused] included to have the option to control the number of mappers reducers based on this
     one_baseline_per_task [0 by default]
     sort_output
         [0 by default]
     bm_delete_output
         delete ouput file if 1 (only for benchmarking).
     bypass_reduce
         do not run reduce phase if 1 (so that output is directly that of the mappers), use only for debugging.
     v
         0 by default, 1 for verbose mode.
     file_log
         logging file.
                  
    Returns
    -------
     start_time
         number of seconds when the job is launched.
     end_time
         number of seconds when the job finishes.
     elapsed_time
         duration of the execution of the job [s].
     ret_start_time
         number of seconds when the output file is requested to the working filesystem (HDFS/Lustre).
     ret_end_time
         number of seconds when the output file gets to the local output folder.
     ret_elapsed_time
         duration of the retrieval of the output file from HDFS/Lustre to the local output folder [s].
     sort_start_time
         number of seconds when the output file sort starts.
     sort_end_time
         number of seconds when the output file sort starts.
     sort_elapsed_time
         duration of the output file sort [s].
    
    Notes
    -----
    |
    | **Configuration:**
    |
    |  -Controling the number of mappers and reducers:
    |    packets_per_hdfs_block and total frames control the number of mappers.
    |    total_partitions controls the number of reducers.
    |    adjust_mappers and adjust_reducers allow to modify the number or mappers/reducers.
    |    *The number of reducers allows finer tunning:
    |     -if adjust_reducers==0, the reducer phase is bypassed.
    |     -if adjust_reducers<0, the number of reducers is fixed to the absolute value of the integer given.
    |  -Configuration of the input reader:
    |    Hadoop is currently used in text mode to process binary data. Until an implementation with full binary support,
    |      the text mode is used. In this mode Hadoop splits the input blocks if it finds this delimiter, so it has to 
    |      be configured to minimize its probability. Need to find a more elegant solution.
    |  -Configuration files:
    |     Use a different hadoop_conf_dir for each node, otherwise there will be conflicts in clusters with shared filesystems.
    |  -Filesystem:
    |     Use Lustre if possible.
    |
    |
    | **Dependencies:**
    |
    |  -Lustre:
    |    Lustre support based on the pluging in https://github.com/Seagate/lustrefs.
    |  -Partitioner:
    |    The custom partitioner KeyFieldBasedPartitionerNH (NH for no hash) has to be used to avoid unbalanced loads on the reducers,
    |       due to the default behavior by hashing the keys.
    |
    |
    | **Notes:**
    |
    |  Note that in this case if  adjust_mappers and adjust_reducers are -1 that does not mean that are computed
    |   automatically (this is the usual convention in the code), but however that only 1 mapper or 1 reducer are used 
    |   (see "Configuration" for more details).
    |
    |
    | **TO DO:**
    |
    | Implement native binary support, instead of using text mode.
    | Document sorting.
    """

    
    if v==1:
        print("\nRunning mapreduce...",file=file_log)  
        print(" (HDFS) Input file dir: ".ljust(24) + hdfs_data_dir,file=file_log)  
        print(" (HDFS) Output file: ".ljust(24) + hdfs_output_file,file=file_log)  
        print(" Deleting HDFS output file...",file=file_log)
        print(" Text delimiter: ".ljust(24)+hadoop_text_delimiter,file=file_log)
        print(" Record size: ".ljust(24)+str(record_size),file=file_log)
        print(" Text mode: ".ljust(24)+str(text_mode),file=file_log)
    
    #TO DO: pass this as an argument to hadoop (not working well currently, and this may cause reducers to fail)
    if v==1:
        print("\nCopying log4j configuration to classpath...",file=file_log) 
    os.system("cp "+hadoop_conf_dir+"/log4j.properties "+hadoop_dir+"/share/hadoop/mapreduce")
    
    # Remove output file from HDFS if it exists
    os.system(hadoop_dir+"bin/hdfs dfs -rm -r -f " + hdfs_output_file)

    num_maps=max(1,total_frames//packets_per_hdfs_block)
    num_reduces=max(1,total_partitions)
    
    # Adjust values for number of mappers and reducers
    num_maps=max(1,int((num_maps*adjust_mappers)//1))
    num_reduces=max(1,int((num_reduces*adjust_reducers)//1))
    if adjust_mappers<0:
        num_maps=int(-adjust_mappers//1)
    if adjust_reducers<0:
        num_reduces=int(-adjust_reducers//1)
    
    if bypass_reduce or adjust_reducers==0:
        num_reduces=0 


    
    if v==1:
        if text_mode==1:
            print("  Forcing mappers: " + str(num_maps)) 
        print("  Forcing reducers: " + str(num_reduces)) 

    # TO DO: outputdelimiter: this needs to be done in a different way (more efficient and correct)
    #     Workaround to avoid spliting packets, default delimiter is custom. Need to find option for fixed text length

    # Group options with -D into option1
    # Group options with other names (-partitioner, -inputformat, ...) into option2
    
    option1=""
    if use_lustre_plugin==1:
        option1 += get_options_lustre()
 
    if text_mode==1:
        option1 += get_options_text_delimiter(hadoop_text_delimiter)
        option1 += get_options_num_maps(num_maps)
    else:
        option1 += " "
    
    # Separator and sorting.
    # TODO: Take this to the constants file (const_mapred.py) (?). Currently in configuration file.

    option1 += get_options_logging(hadoop_conf_dir+"/log4j.properties")
    
    # all baselines
    if one_baseline_per_task:
        option1 += get_options_partitioning(FIELD_SEP,\
                                            HADOOP_NUM_KEY_FIELDS,\
                                            FIELD_SEP,\
                                            HADOOP_PARTITION_ONE_BASELINE_PER_TASK_STR,\
                                            COMMON_SORT_ONE_BASELINE_PER_TASK_STR)
    else:
        option1 += get_options_partitioning(FIELD_SEP,\
                                            HADOOP_NUM_KEY_FIELDS,\
                                            FIELD_SEP,\
                                            HADOOP_PARTITION_ALL_BASELINES_PER_TASK_STR,\
                                            COMMON_SORT_ALL_BASELINES_PER_TASK_STR)
    
    option1 += get_options_num_reduces(num_reduces)
    
    
    #option2 =  " -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner"
    # No hash - custom partitioner (specify reducer id as number from 0 to num_reducers)
    #option2 =  " -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitionerNH"
    
    option2 = get_options_custom_partitioner(use_nohash_partitioner)
    if text_mode==0:
        [options_fixed_1,options_fixed_2] = get_options_fixed_length_records(record_size)
        option1 += options_fixed_1
        option2 += options_fixed_2
    else:
        hdfs_data_dir += "/*"
   
    # Files
    files_extra= " -file \"" + mappersh + "\"" 
    files_extra+=" -file \"" + reducersh + "\"" 
    files_extra+=" -file \"" + app_dir + mapper + "\"" 
    files_extra+=" -file \"" + app_dir + reducer + "\"" 
        
    # Application dependencies
    for f in files_deps:
        files_extra+=" -file \"" + folder_deps + f + "\"" 
    
    # Configuration dependencies (if applicable)
    for f_add in add_deps:
        files_extra+=" -file \"" + f_add + "\"" 
    
    
    # TO DO: specify log4j file...
    #option2 += " -D log4j.configuation="+hadoop_conf_dir+"/log4j.properties"
    
    str_command = hadoop_dir + "bin/hadoop --config " + hadoop_conf_dir + " jar " + hadoop_dir + "share/hadoop/tools/lib/"+HADOOP_STREAMING_JAR+" " + option1 + "-input " + hdfs_data_dir  + " -output " + hdfs_output_file + " -mapper " + "\""  + mappersh + "\"" + " -reducer " + "\"" + reducersh + "\"" + files_extra + option2
    
    if v==1:
        print(" " + str_command,file=file_log)
       


    
    
    # Prepare jobsh bash file for job submission
    os.system("echo $LD_LIBRARY_PATH > " + temp_log)

    line = "\""
    with open(temp_log, 'r') as f_log:
        for line_in in f_log:
            line+=line_in.strip()
    line+="\""


    with open(jobsh, 'w') as f:
        print("#!/bin/bash",file=f)
        print("export LD_LIBRARY_PATH=" + line,file=f)
        print(str_command,file=f)

    os.system("chmod +x "+jobsh)
    
    if "/home" not in jobsh:
        jobsh="./"+jobsh
        
    start_time = time.time() #                --------------- Execution time start
    os.system(jobsh)
    end_time = time.time() #                  --------------- Execution time stop
    
    elapsed_time = end_time - start_time

   
    
    # Retrive file (or delete if running in benchmarking mode)
    ret_start_time = time.time()
    unsorted_suffix =  "_unsorted"
    if use_lustre_plugin==0:
        os.system(hadoop_dir + "bin/hdfs --config " + hadoop_conf_dir + " dfs -getmerge " + hdfs_output_file + " " +\
                  output_dir + output_hadoop + unsorted_suffix)
    else:
        if bm_delete_output==0:
            os.system(hadoop_dir + "bin/hadoop --config " + hadoop_conf_dir + " fs -getmerge " +\
                      lustre_user_dir + "/" + hdfs_output_file + " " + output_dir + output_hadoop + unsorted_suffix)
            #os.system("cp " + hdfs_output_file + " " + output_dir + output_hadoop + unsorted_suffix)
        else:
            if v==1:
                print(" (!!) Benchmarking mode, will not retrieve output)",file=file_log)
                
                os.system("ls -l `ls -td -- "+lustre_user_dir + "/" + hdfs_output_file +"` > " + temp_log)
                print(" Output files:",end="\n",file=file_log)
                with open(temp_log, 'r') as f_tmp:
                    for line in f_tmp:
                        print("  "+line.strip(),end="\n",file=file_log)
                print(" (!!) Benchmarking mode, deleting part* files...)",file=file_log)
            os.system("mv "+lustre_user_dir + "/" + hdfs_output_file+"/part-00000 "+lustre_user_dir + "/" + hdfs_output_file+"/only-saved-part-00000")
            os.system("rm "+lustre_user_dir + "/" + hdfs_output_file+"/part*")
                
    ret_end_time = time.time()
    ret_elapsed_time = ret_end_time - ret_start_time
    
    
    sort_start_time = time.time()
    # Show output files and delete them if benchmarking
    if bm_delete_output==1:
        if use_lustre_plugin:
            if v==1:
                print(" (!!) Benchmarking mode, output not retrieved)",file=file_log)
    else:
        if sort_output:
            if v==1:
                print(" Re-sorting output file (may be unsorted if using multiple reducers)",file=file_log) 
            os.system("sort -k1,1 " + output_dir + output_hadoop + unsorted_suffix + ">" + output_dir + output_hadoop)
            os.system("rm " + output_dir + output_hadoop + unsorted_suffix)
        else:
            if v==1:
                print(" No sorting of output!",file=file_log) 
            os.system("cp " + output_dir + output_hadoop + unsorted_suffix + " " + output_dir + output_hadoop)
            os.system("rm " + output_dir + output_hadoop + unsorted_suffix)
        
        command_mk = "mkdir "+output_sym
        command_ln = "ln -s "+output_dir+output_hadoop+" "+output_sym+output_hadoop
        os.system(command_mk)
        os.system(command_ln)
        
    sort_end_time = time.time()
    sort_elapsed_time = sort_end_time - sort_start_time

    
    if v==1:
        print(" Output file: ".ljust(24) + output_dir + output_hadoop,file=file_log)  
        print(" Elapsed time = "+ str(elapsed_time)+ " s",file=file_log)
        
    # Job id and status
    os.system(hadoop_dir + "bin/hadoop job -list all > " + temp_log)
    max_lines=3
    line_count=0
    line_with_job_id=3
    job_id=""
    if v==1:
        print(" Job summary:",end="\n",file=file_log)
        with open(temp_log, 'r') as f_tmp:
            for line in f_tmp:
                line_count+=1
                if line_count<=max_lines:
                    print("  "+line.strip(),end="\n",file=file_log)

    # Check nodes (in case some where unavailable)
    if v==1:
        print(" Checking nodes after job...",end="\n",file=file_log)
        os.system(hadoop_dir + "bin/yarn --config "+ hadoop_conf_dir +" node -list | grep Total > " + temp_log)
        with open(temp_log, 'r') as f_tmp:
            for line in f_tmp:
                print("  "+line.strip(),end="\n",file=file_log)

    return([start_time,end_time,elapsed_time,ret_start_time,ret_end_time,ret_elapsed_time,sort_start_time,sort_end_time,sort_elapsed_time])


# <codecell>


