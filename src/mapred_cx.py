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
#File: mapred_cx.py.
#Author: A.J. Vazquez Alvarez (ajvazquez@haystack.mit.edu)
#Description: 
"""
Main script to run the CorrelX correlator.

Parameters
----------
|  [-c configuration_file] (optional): configuration file with the configuration for the correlation.
|  [-s log_output_folder] (optional): folder for logs.
|  [-f forced_parameters] (optional): comma sepparated assignments for overriding parameters (for test batching).
|  [--help-parameters] (optional): show a list of all parameters available to override the configuration file.

Returns
-------
|  Correlation results: "Output directory" in configuration file. 
|                       Symbolic link to output file added to experiment folder.
|  Log files:           Folder specified in -s option.

Notes
-----
|
| **Example:**
|
|  python mapred_cx -n 10.0.2.4 -c basic_files_conf/configh.conf -s exp3 -f exper=/home/hduser/basic_files_data/ini_files_eht_two,fftr=1
|
|
| **TO DO:**
|
|  More detailed documentation.

"""
#History:
#initial version: 2016.11 ajva
#MIT Haystack Observatory

from __future__ import print_function
import sys
import os
import time
import imp
import argparse
import numpy as np

import const_config
imp.reload(const_config)
from const_config import *

import const_hadoop
imp.reload(const_hadoop)
from const_hadoop import *

import lib_config
imp.reload(lib_config)
from lib_config import *

import lib_ini_exper
imp.reload(lib_ini_exper)
from lib_ini_exper import *

import lib_mapredcorr
imp.reload(lib_mapredcorr)
from lib_mapredcorr import *

import lib_hadoop_hdfs
imp.reload(lib_hadoop_hdfs)
from lib_hadoop_hdfs import *

import lib_net_stats
imp.reload(lib_net_stats)
from lib_net_stats import *

# Vector quantization                           # VQ disabled
#import lib_vq
#imp.reload(lib_vq)
#from lib_vq import *


    


def print_execution_times(exec_times,io_times,bypass_print=0,v=1,file_log=sys.stdout):
    """
    Print execution times.
    
    Parameters
    ----------
     exec_times : list of [str_id , num_slaves, num_vcores, hadoop_t_s,hadoop_t_e,hadoop_d] elements where:
                     |   str_id:       string with identifier for this run ("pipeline" or "hadoop*").
                     |   num_slaves:   number of worker nodes (requested).
                     |   num_vcores:   number of virtual CPU cores per node.
                     |   hadoop_t_s:   timing start time in seconds.
                     |   hadoop_t_e:   timing end time in seconds.
                     |   hadoop_d:     timing duration in seconds.
     io_times : same format as exect times (but str_id changes for "put" [local -> distributed filesystem], 
                        "get" [distributed filesystem -> local], "sort" [output sort].
     bypass_print
         currently used to indicate that profiling is used. 
     v
         verbose if 1.
     file_log
         handler to log file.
     
    Returns
    -------
     None
    
    Notes
    ------
     Devised to show speedup initially, but now running single iteration by default.
     Note that the number of nodes is as requested, not the actual number of healthy nodes.
     
     (!) Note that the execution times are increased when profiling mapper and/or reducer (!).
    """
    
    if bypass_print==1:
        add_txt=" [Profiling active, increased execution times!]"
    else:
        add_txt=""
    
    if v==1:
        #if bypass_print==0:
        if len(exec_times)>0:
            if exec_times[0][1]==0:
                print("\nNote: 0 nodes and 0 vcores correspond to pipeline execution",file=file_log) 
        
            print("\nShowing results...",file=file_log) 
        
            print("\nFile IO approximate times",file=file_log)  
            print(" " + "Type".ljust(20) + "File IO. time [s]",file=file_log) 
            for i in io_times:
                print(" " + i[0].ljust(20) + str(i[5]) + add_txt,file=file_log)  
            
            print("\nExecution times",file=file_log)  
            print(" " + "Type".ljust(20) + "Exec. time [s]",file=file_log) 
            for i in exec_times:
                print(" " + i[0].ljust(20) + str(i[5]) + add_txt,file=file_log)  
            
            #print("\nSpeedup",file=file_log)  
            #print(" " + "Nodes".ljust(10)  + "V. Cores".ljust(10) + "Speedup",file=file_log) 
            #
            #base_time = exec_times[0][5]
            #for i in exec_times:
            #    print(" " + str(i[1]).ljust(10) + str(i[2]).ljust(10)  + str(base_time/max(i[5],1)) + add_txt,file=file_log) 



def print_header(header="Header",v=0,file_log=sys.stdout):
    """
    Print header in logging file.
    """
    if v==1:
        line_l = "#-----------------------------------------------------"
        line_e = "#    "
        print("\n\n" + line_l ,file=file_log)   
        print(line_e + header,file=file_log)
        print( line_l ,file=file_log)   





if __name__ == '__main__':
    
    # Network stats
    NETWORK_STATS=0
    # Verbose
    v=1
    
    
    
    # Default values for configuration:
    config_file = "configh.conf"
    config_suffix = "_mod"
    output_log_folder = time.strftime("e%Y%m%d_%H%M%S")
    forced_params =""
    nodes_list=os.uname()[1] # Default first node only #""

    # Configuration for parameter help
    const_config_file=os.path.dirname(sys.argv[0])+"/const_config.py"    # File with configuration constants
    str_help_param="C_ARG_"                                              # Prefix for constants to be displayed
    len_str_help_param=len(str_help_param)

    timestamp_str = time.strftime("%Y%m%d_%H%M%S")




    # Argument parser configuration
    cparser = argparse.ArgumentParser(description='CorrelX')
    
    # TO DO: change option -s for -o
    # Configuration file
    cparser.add_argument('-c', action="store",\
                         dest="configuration_file",default=config_file,\
                         help="Specify a configuration file.")
     
    cparser.add_argument('-n', action="store",\
                         dest="nodes_list",default=nodes_list,\
                         help="Specify a comma-separated list of nodes.")
    
    # Suffix folder (for having log and output files of different simulations in different folders)
    cparser.add_argument('-s', action="store",\
                         dest="output_log_folder",default=output_log_folder,\
                         help="Specify a folder to store the output log files.")
    
    # Forced parameters (for overriding configuration from the configuration file)
    cparser.add_argument('-f', action="store",\
                         dest="forced_params",default=forced_params,\
                         help="Specify a comma-separated list of parameter=value to override "+\
                         "the configuration file(see --help-parameters).")
    
    cparser.add_argument('--help-parameters',action="store_true",\
                         dest="help_parameters",default=False,\
                         help="Show all parameters for option -f.")
    
    
    # Get arguments
    args = cparser.parse_args()   
    config_file = args.configuration_file
    output_log_folder = args.output_log_folder
    forced_params = args.forced_params

    nodes_list = args.nodes_list
    NODES_LIST=nodes_list.split(',')


    # Help for parameter overrides
    if args.help_parameters:
        # Show help and exit
        print("")
        print("Showing available configuration parameters:")
        print("")
        with open(const_config_file, 'r') as f_const:
            for line in f_const:
                if line[:len_str_help_param]==str_help_param:
                    line_split=line.strip().split('\"')
                    if line_split[3]=="display_in_help":
                        print(line_split[1]+line_split[2])
        print("")
        print("Example: python mapred_cx.py -f ppb=5000,slowstart=0.95")
        print("")
    else:
        
    
        # Timing results
        exec_times = []             #  Execution times
        output_files_list = []      #  Output files
        io_times = []               #  Times to copy files local <-> HDFS (these are estimated times!)
        
        # Network stats
        [v_str_hadoop,v_stats_param,v_stats_values_s,v_stats_values_e,v_stats_ping,v_stats_ping] = init_net_stats()
        
        # Prepare suffix for output and log files
        suffix_host=""
        with os.popen("hostname",'r',1) as f_out:
            for line in f_out:
                suffix_host=line.strip()
                break
        suffix_log="_"+time.strftime("%Y%m%d_%H%M%S")+"_"+suffix_host
        config_suffix+=suffix_log
        
        
        
        # Initilialize logging
        # Read only log file from configuration file     
        [FILE_LOG,TEMP_LOG] = get_log_file(config_file=config_file,suffix=suffix_log,output_log_folder=output_log_folder)
        
        
        print_header(header="Configuration",v=v,file_log=FILE_LOG)
        
        # Get files to get list of nodes
        #NODES_EXEC_FILE = get_nodes_file(config_file)+"_"+suffix_host
        
        if NODES_LIST==[]:
            if v==1:
                #print("\n[!] Error with hosts file! " + NODES_EXEC_FILE + "\n     Try running: ' ./get_nodelist.sh > " + NODES_EXEC_FILE + " ' before launching srun.",file=FILE_LOG)
                print("\n[!] Error with nodes list.",file=FILE_LOG)
        else:
            
            master_reduced=NODES_LIST[0]
    
            # Check if this node is master
            [is_master,my_name,my_ip] = is_this_node_master(master=master_reduced,temp_log=TEMP_LOG,v=1,file_log=FILE_LOG)
        
            # Substitute localhost with master ip/name (and update config_file to avoid overwriting template)
            config_file = get_config_mod_for_this_master(config_file,config_suffix,NODES_LIST[0],sys.argv[0])
            
            # Override configuration parameters
            override_configuration_parameters(forced_configuration_string=forced_params,config_file=config_file,\
                                              v=v,file_log=FILE_LOG)
        
            if v==1:
                print("\nConfig file updated: " + config_file,file=FILE_LOG)  
        
            
            
        
            # Read constants from configuration file
            [MAPPER, REDUCER, DEPENDENCIES, PACKETS_PER_HDFS_BLOCK,CHECKSUM_SIZE,\
                SRC_DIR,APP_DIR, CONF_DIR, TEMPLATES_CONF_DIR, TEMPLATES_ENV_DIR, HADOOP_DIR,HADOOP_CONF_DIR,NODES, \
                MAPPERSH,REDUCERSH,JOBSH,PYTHON_X,\
                USERNAME_MACHINES,MAX_SLAVES,SLAVES,MASTERS,MASTER_IS_SLAVE,HADOOP_TEMP_DIR,DATA_DIR,DATA_DIR_TMP,HDFS_DATA_DIR,HADOOP_START_DELAY,HADOOP_STOP_DELAY,\
                PREFIX_OUTPUT,HADOOP_TEXT_DELIMITER,OUTPUT_DIR,OUTPUT_SYM,RUN_PIPELINE,RUN_HADOOP,MAX_CPU_VCORES,\
                HDFS_REPLICATION,OVER_SLURM,HDFS_COPY_DELAY,\
                FFT_AT_MAPPER,INI_FOLDER,\
                INI_STATIONS, INI_SOURCES, INI_DELAY_MODEL, INI_DELAYS, INI_MEDIA, INI_CORRELATION,\
                INTERNAL_LOG_MAPPER,INTERNAL_LOG_REDUCER,ADJUST_MAPPERS,ADJUST_REDUCERS,FFTS_PER_CHUNK,TEXT_MODE,\
                USE_NOHASH_PARTITIONER,USE_LUSTRE_PLUGIN,LUSTRE_USER_DIR,LUSTRE_PREFIX,ONE_BASELINE_PER_TASK,\
                MIN_MAPPER_CHUNK,MAX_MAPPER_CHUNK,TASK_SCALING_STATIONS,SORT_OUTPUT,BM_AVOID_COPY,\
                BM_DELETE_OUTPUT,TIMEOUT_STOP,SINGLE_PRECISION,PROFILE_MAP,PROFILE_RED] = \
                    get_configuration(v=v,config_file=config_file,timestamp_str=timestamp_str,file_log=FILE_LOG)
    
            # Check errors in experiment .ini files
            init_success = check_errors_ini_exper(DATA_DIR,INI_FOLDER,INI_STATIONS,INI_SOURCES,INI_DELAY_MODEL,INI_MEDIA)
    
    
    
            if init_success==0:
                print("Failed initialization, exiting!")
    
            else:
    
                # Get configuration and output folders (specific for this master)
                [APP_DIR,CONF_DIR,HADOOP_CONF_DIR,HADOOP_DEFAULT_CONF_DIR,OUTPUT_DIR] = get_conf_out_dirs(master_name=my_name,\
                                                                                                hadoop_dir=HADOOP_DIR,\
                                                                                                app_dir=APP_DIR,\
                                                                                                conf_dir=CONF_DIR,\
                                                                                                suffix_conf=suffix_host,\
                                                                                                output_dir=OUTPUT_DIR,\
                                                                                                suffix_out=output_log_folder,\
                                                                                                v=v,file_log=FILE_LOG)
                                                                                                
    
                num_slaves = MAX_SLAVES
                num_vcores = MAX_CPU_VCORES        
            
            
                # Reduce node list if required by configuration
                [num_slaves,NODES_LIST] = reduce_list_nodes(num_slaves=num_slaves,nodes_list=NODES_LIST,v=v,file_log=FILE_LOG)
                
                # Overwrite nodes file
                overwrite_nodes_file(nodes_list=NODES_LIST,nodes_file=CONF_DIR+NODES,v=v,file_log=FILE_LOG)   
                    
                              
                 
                # Distribute Hadoop configuration files
                # This is required for all nodes to have the proper configuration files. 
                # This includes all .xml files but also
                #   .sh scripts with environment setup.
                # Note that HADOOP_CONF_DIR should be a different folder for every deployment i.e. for every different master,
                #   and thus name should depend on master name.
                distribute_files(simply_copy_local=OVER_SLURM,file_group="Hadoop config files",v=v,exec_permission=0,file_log=FILE_LOG,\
                                 files=["*"],source_dir=TEMPLATES_ENV_DIR,conf_dir=CONF_DIR,\
                                 destination_dir=HADOOP_CONF_DIR,nodes=NODES,username=USERNAME_MACHINES,temp_log=TEMP_LOG,\
                                 force_node=','.join(NODES_LIST))
            
            
                
                # Process .ini files
                [stations_serial_str,media_serial_str,\
                 correlation_serial_str,delays_serial_str,\
                 AUTO_STATIONS, AUTO_POLS, FFT_SIZE, \
                 ACCUMULATION_TIME, STATIONS, \
                 REF_EPOCH, SIGNAL_START, \
                 SIGNAL_DURATION,INPUT_FILES,\
                 FIRST_FRAME_NUM,NUM_FRAMES,CODECS_SERIAL,\
                 max_packet_size,total_frames,\
                 total_partitions,windowing,\
                 PHASE_CALIBRATION,delay_error,error_str_v,NUM_POLS] = process_ini_files(DATA_DIR,\
                                                                INI_STATIONS,\
                                                                INI_SOURCES,\
                                                                INI_DELAY_MODEL,\
                                                                INI_DELAYS,\
                                                                INI_MEDIA,\
                                                                INI_CORRELATION, ONE_BASELINE_PER_TASK)
                # TO DO: write a library for error checking.
                if delay_error is None:
                    init_success=0
                    print("ERROR: Incomplete data in delay model file! Exiting...")
                
                
                elif error_str_v!=[]:
                    init_success=0
                    print("")
                    for i in error_str_v:
                        print(i)
                        
                
                if init_success==1:
                    
                    # Pipeline mode
    
                    print_header(header="Pipeline execution",v=v,file_log=FILE_LOG)
                    if (is_master) and (RUN_PIPELINE):
                        # Pipeline application execution
                        pipeline_output_file=PREFIX_OUTPUT + "_s" + str(0) + "_v" + str(0) + ".out"
                        [pipeline_t_s,pipeline_t_e,pipeline_d] = pipeline_app(python_x=PYTHON_X,\
                                                                     stations=STATIONS,\
                                                                     input_files=INPUT_FILES,\
                                                                     app_dir=SRC_DIR,\
                                                                     mapper=MAPPER,\
                                                                     reducer=REDUCER,\
                                                                     fft_size=FFT_SIZE,\
                                                                     fft_at_mapper=FFT_AT_MAPPER,\
                                                                     accumulation_time=ACCUMULATION_TIME,\
                                                                     signal_start=SIGNAL_START,\
                                                                     signal_duration=SIGNAL_DURATION,\
                                                                     first_frame_num=FIRST_FRAME_NUM,\
                                                                     num_frames=NUM_FRAMES,\
                                                                     data_dir=DATA_DIR,\
                                                                     output_dir=OUTPUT_DIR,\
                                                                     output_sym=OUTPUT_SYM,\
                                                                     auto_stations=AUTO_STATIONS,\
                                                                     auto_pols=AUTO_POLS,\
                                                                     num_pols=NUM_POLS,\
                                                                     codecs_serial=CODECS_SERIAL,\
                                                                     v=v,\
                                                                     file_log=FILE_LOG,\
                                                                     file_out=pipeline_output_file,\
                                                                     ini_stations=INI_STATIONS,\
                                                                     ini_media=INI_MEDIA,\
                                                                     ini_delays=INI_DELAYS,\
                                                                     internal_log_mapper=INTERNAL_LOG_MAPPER,\
                                                                     internal_log_reducer=INTERNAL_LOG_REDUCER,\
                                                                     ffts_per_chunk=FFTS_PER_CHUNK,\
                                                                     windowing=windowing,\
                                                                     one_baseline_per_task=ONE_BASELINE_PER_TASK,\
                                                                     phase_calibration=PHASE_CALIBRATION,\
                                                                     min_mapper_chunk=MIN_MAPPER_CHUNK,\
                                                                     max_mapper_chunk=MAX_MAPPER_CHUNK,\
                                                                     task_scaling_stations=TASK_SCALING_STATIONS,\
                                                                     sort_output=SORT_OUTPUT,\
                                                                     single_precision=SINGLE_PRECISION,\
                                                                     profile_map=PROFILE_MAP,\
                                                                     profile_red=PROFILE_RED,\
                                                                     timestamp_str=timestamp_str)
                        
            
                        
                        exec_times+=[["Pipeline", 0, 0, pipeline_t_s, pipeline_t_e, pipeline_d]]
                        output_files_list+=[pipeline_output_file]
                    
                    
                    else:
                        exec_times+=[["Pipeline", 0, 0, 0, 0, 0]]
                        if v==1:
                            print("\nNot executed based on configuration.",file=FILE_LOG)
                
                    
                    
                    
                    #Hadoop execution
                    
                    
                    
                    print_header(header="MapReduce",v=v,file_log=FILE_LOG)

                    if RUN_HADOOP:   
                        # Process configuration files
                        [list_configurations,pairs_config] = get_list_configuration_files(config_file)
                        configuration_files = process_hadoop_config_files(list_configurations,pairs_config,\
                                                                          templates_dir=TEMPLATES_CONF_DIR,\
                                                                          conf_dir=CONF_DIR,v=v,\
                                                                          file_log=FILE_LOG)
                    
    
                        # Distribute configuration files
                    
                        # Copy configuration files to all nodes
                        files = configuration_files
                        # This node
                        distribute_files(simply_copy_local=1,file_group="Conf - first node",v=v,exec_permission=0,file_log=FILE_LOG,\
                                     files=files,source_dir=CONF_DIR,conf_dir=CONF_DIR,destination_dir=HADOOP_CONF_DIR,\
                                     nodes=NODES,username=USERNAME_MACHINES,temp_log=TEMP_LOG,force_node=','.join(NODES_LIST))
                        # Other nodes
                        distribute_files(simply_copy_local=OVER_SLURM,file_group="Conf",v=v,exec_permission=0,file_log=FILE_LOG,\
                                     files=files,source_dir=CONF_DIR,conf_dir=CONF_DIR,destination_dir=HADOOP_CONF_DIR,\
                                     nodes=NODES,username=USERNAME_MACHINES,temp_log=TEMP_LOG,force_node=','.join(NODES_LIST))
            
            
            
                        # Configure application and distribute it to nodes
            
            
            
            
                        # Interm. sh files for recovering LD_LIBRARY_PATH and calling the mapper and reducer
                        # Write these to CONF_DIR, otherwise they may be overwritten (by other tasks running in other nodes)!
                        
                        

                        # Additional dependencies
                        add_deps=[INI_DELAYS,INI_MEDIA,INI_STATIONS]
                        ini_delays_dep = INI_DELAYS.split("/")[-1]
                        ini_media_dep = INI_MEDIA.split("/")[-1]
                        ini_stations_dep = INI_STATIONS.split("/")[-1]                        
                        print("Additional dependencies:")
                        print(" "+','.join(add_deps))
            
            
            
                        # Create script for mapper
                        params_mapper=get_mapper_params_str(STATIONS,NUM_POLS,FFT_SIZE,ACCUMULATION_TIME,SIGNAL_START,SIGNAL_DURATION,\
                                                   FIRST_FRAME_NUM,NUM_FRAMES,CODECS_SERIAL,\
                                                   AUTO_STATIONS,AUTO_POLS,ini_stations_dep,ini_media_dep,\
                                                   ini_delays_dep,FFT_AT_MAPPER,INTERNAL_LOG_MAPPER,FFTS_PER_CHUNK,\
                                                   windowing,\
                                                   one_baseline_per_task=ONE_BASELINE_PER_TASK,\
                                                   phase_calibration=PHASE_CALIBRATION,min_mapper_chunk=MIN_MAPPER_CHUNK,
                                                   max_mapper_chunk=MAX_MAPPER_CHUNK,task_scaling_stations=TASK_SCALING_STATIONS,\
                                                   single_precision=SINGLE_PRECISION)
                        command_map = get_mr_command(app_dir=APP_DIR,script=MAPPER,params=params_mapper)
                        create_inter_sh(CONF_DIR+MAPPERSH,PYTHON_X,command_map,temp_log=TEMP_LOG,v=v,file_log=FILE_LOG)
                        
                        
                        
                        
                        # Get script for reducer
                        params_reducer=get_reducer_params_str(CODECS_SERIAL,FFT_AT_MAPPER,INTERNAL_LOG_REDUCER,FFT_SIZE,windowing,\
                                                              PHASE_CALIBRATION,SINGLE_PRECISION)
                        command_red = get_mr_command(app_dir=APP_DIR,script=REDUCER,params=params_reducer)
                        create_inter_sh(CONF_DIR+REDUCERSH,PYTHON_X,command_red,temp_log=TEMP_LOG,v=v,file_log=FILE_LOG)
            
            
            
            
            
                        # Copy application files to all nodes (master-associated folder)
                        # TO DO: add add_deps too
                        files = DEPENDENCIES
                        files.extend([MAPPER])
                        files.extend([REDUCER])
                        distribute_files(simply_copy_local=OVER_SLURM,file_group="App",v=v,exec_permission=1,file_log=FILE_LOG,\
                                     files=files,source_dir=SRC_DIR,conf_dir=CONF_DIR,\
                                     destination_dir=APP_DIR,nodes=NODES,username=USERNAME_MACHINES,temp_log=TEMP_LOG,\
                                     force_node=','.join(NODES_LIST))
                        files_sh = [MAPPERSH, REDUCERSH]
                        distribute_files(simply_copy_local=OVER_SLURM,file_group="App-sh",v=v,exec_permission=1,file_log=FILE_LOG,\
                                     files=files_sh,source_dir=CONF_DIR,conf_dir=CONF_DIR,\
                                     destination_dir=APP_DIR,nodes=NODES,username=USERNAME_MACHINES,temp_log=TEMP_LOG,\
                                     force_node=','.join(NODES_LIST))
            
            
            
                        # Stop Hadoop in case it's running
    
                        nodes_to_slaves_masters(conf_dir=CONF_DIR,hadoop_conf_dir=HADOOP_CONF_DIR,file_nodes=NODES,file_slaves=SLAVES,\
                                        file_masters=MASTERS,max_slaves=MAX_SLAVES,master_is_slave=MASTER_IS_SLAVE,v=v,file_log=FILE_LOG)
                        
                        cluster_stop(wait_time=HADOOP_STOP_DELAY,hadoop_dir=HADOOP_DIR,hadoop_conf_dir=HADOOP_CONF_DIR,\
                                               timeout=TIMEOUT_STOP,v=v,file_log=FILE_LOG,temp_log=TEMP_LOG)
                        
                        # Shut down other hadoop processes (this needs to be done in another way)
                        if v==1:
                            print("\nForcing shutdown of hadoop processes (in case they were not shut down properly)...",file=FILE_LOG)
                            # TO DO?: comment line in start-dfs.sh which starts the secondary namenode 
                            
                        os.system("kill `ps -axu|grep hadoop|grep " + USERNAME_MACHINES + "|awk '{print $2}'` > " + TEMP_LOG)
                        if v==1:    
                            with open(TEMP_LOG, 'r') as f_log:
                                for line in f_log:
                                    print(" "+line.strip(),file=file_log)
                        
                    
                        # Iterate on number of slaves
                        hadoop_failed=0
                        
                        #process nodes file, for creating slaves file
                        nodes_to_slaves_masters(conf_dir=CONF_DIR,hadoop_conf_dir=HADOOP_CONF_DIR,file_nodes=NODES,file_slaves=SLAVES,\
                                    file_masters=MASTERS,max_slaves=num_slaves,master_is_slave=MASTER_IS_SLAVE,v=v,file_log=FILE_LOG)
                    
                    
                    
                    
                        # Update replication (check that it is <= than the number of nodes
                        replication_forced = HDFS_REPLICATION
                        if num_slaves<=HDFS_REPLICATION:
                            replication_forced = num_slaves
                            replication_file = 'hdfs-site.xml'
                            if v==1:
                                print("\nUpdating " + replication_file + "...",file=FILE_LOG)
        
                            replication_config = [('Configuration file',replication_file),(C_H_HDFS_REPLICATION,str(num_slaves))]
                            processed_file = process_hcfile(v=v,file_log=FILE_LOG,pairs=replication_config,conf_dir=CONF_DIR,\
                                                    templates_dir=CONF_DIR)
        
                            files=[replication_file]
                            # This node
                            distribute_files(simply_copy_local=1,file_group="Replication - first node",v=v,exec_permission=0,file_log=FILE_LOG,files=files,source_dir=CONF_DIR,conf_dir=CONF_DIR,\
                                 destination_dir=HADOOP_CONF_DIR,nodes=NODES,username=USERNAME_MACHINES,temp_log=TEMP_LOG,\
                                 force_node=','.join(NODES_LIST))
                            # Other nodes
                            distribute_files(simply_copy_local=OVER_SLURM,file_group="Replication",v=v,exec_permission=0,file_log=FILE_LOG,files=files,source_dir=CONF_DIR,conf_dir=CONF_DIR,\
                                 destination_dir=HADOOP_CONF_DIR,nodes=NODES,username=USERNAME_MACHINES,temp_log=TEMP_LOG,\
                                 force_node=','.join(NODES_LIST))
                
                
                            
                      
                    
    
                        # Start Hadoop
                        if hadoop_failed==0:
                            t_nodes = cluster_start(wait_time=HADOOP_START_DELAY,hadoop_conf_dir=HADOOP_CONF_DIR,hadoop_dir=HADOOP_DIR,file_slaves=SLAVES,\
                                  temp_dir=HADOOP_TEMP_DIR,username=USERNAME_MACHINES,single_node=OVER_SLURM,\
                                  use_lustre_plugin=USE_LUSTRE_PLUGIN,v=v,file_log=FILE_LOG,temp_log=TEMP_LOG)
                    
    
                        if t_nodes>0:
          
            
            
                            # Copy media files into distributed filesystem (HDFS or Lustre)
    
                            # (!)Time measurements are approximate!! print and wait statements in function!!
                            [put_t_s,put_t_e,put_d] = copy_files_to_hdfs(replication=HDFS_REPLICATION,\
                                    input_files=INPUT_FILES,data_dir=DATA_DIR,data_dir_tmp=DATA_DIR_TMP,hadoop_dir=HADOOP_DIR,\
                                    hadoop_conf_dir=HADOOP_CONF_DIR,hdfs_data_dir=HDFS_DATA_DIR,\
                                    packets_per_hdfs_block=PACKETS_PER_HDFS_BLOCK,\
                                    temp_log=TEMP_LOG,copy_delay=HDFS_COPY_DELAY,checksum_size=CHECKSUM_SIZE,text_mode=TEXT_MODE,\
                                    use_lustre_plugin=USE_LUSTRE_PLUGIN,lustre_prefix=LUSTRE_PREFIX,bm_avoid_copy=BM_AVOID_COPY,\
                                    v=v,file_log=FILE_LOG) 
                            io_times+=[["HDFS-put " + str(num_slaves) + "s-" + str(num_vcores)+ "v" , num_slaves, num_vcores, put_t_s,put_t_e,put_d]]
    
    
    
    
            
                            
                            hdfs_output_file = PREFIX_OUTPUT + "_s" + str(num_slaves) + "_v" + str(num_vcores)+ suffix_log +".out"
                            if NETWORK_STATS==1:
                                [stats_params_s,stats_values_s,stats_ping_s] = get_network_stats(nodes_list=NODES_LIST,over_slurm=OVER_SLURM,v=v,file_log=FILE_LOG)
                            
                            
                            
                            
                            
                            
                            # Run mapreduce
                            [hadoop_t_s,hadoop_t_e,hadoop_d,get_t_s,get_t_e,get_d,sort_t_s,sort_t_e,sort_d] = \
                              run_mapreduce_sh(record_size=max_packet_size,\
                                                jobsh=CONF_DIR+JOBSH,\
                                                mappersh=CONF_DIR+MAPPERSH,\
                                                reducersh=CONF_DIR+REDUCERSH,\
                                                app_dir=APP_DIR,\
                                                hadoop_dir=HADOOP_DIR,\
                                                hadoop_conf_dir=HADOOP_CONF_DIR,\
                                                folder_deps=APP_DIR,\
                                                files_deps=DEPENDENCIES,\
                                                add_deps=add_deps,\
                                                mapper=MAPPER,\
                                                reducer=REDUCER,\
                                                hdfs_data_dir=HDFS_DATA_DIR,\
                                                hdfs_output_file=hdfs_output_file,\
                                                output_hadoop=hdfs_output_file,\
                                                text_mode=TEXT_MODE,\
                                                hadoop_text_delimiter=HADOOP_TEXT_DELIMITER,\
                                                temp_log=TEMP_LOG,\
                                                packets_per_hdfs_block=PACKETS_PER_HDFS_BLOCK,\
                                                total_frames=total_frames,\
                                                total_partitions=total_partitions,\
                                                adjust_mappers=ADJUST_MAPPERS,\
                                                adjust_reducers=ADJUST_REDUCERS,\
                                                use_nohash_partitioner=USE_NOHASH_PARTITIONER,\
                                                use_lustre_plugin=USE_LUSTRE_PLUGIN,\
                                                lustre_user_dir=LUSTRE_USER_DIR,\
                                                num_slaves=num_slaves,\
                                                num_vcores=num_vcores,\
                                                one_baseline_per_task=ONE_BASELINE_PER_TASK,\
                                                sort_output=SORT_OUTPUT,\
                                                bm_delete_output=BM_DELETE_OUTPUT,\
                                                bypass_reduce=0,\
                                                v=v,\
                                                file_log=FILE_LOG,\
                                                output_dir=OUTPUT_DIR,\
                                                output_sym=OUTPUT_SYM)
                                 
                            if NETWORK_STATS==1:
                                [stats_params_e,stats_values_e,stats_ping_e] = get_network_stats(nodes_list=NODES_LIST,over_slurm=OVER_SLURM,v=v,file_log=FILE_LOG)
                                delta_stats = compute_txrx_bytes(stats_values_s,stats_values_e,v=v,file_log=FILE_LOG)
                            
                            # Logging results
                            str_hadoop = "Hadoop " + str(num_slaves) + "s-" + str(num_vcores)+ "v" + " "
                            exec_times+=[[str_hadoop , num_slaves, num_vcores, hadoop_t_s,hadoop_t_e,hadoop_d]]
                            output_files_list+=[hdfs_output_file]
                            str_hdfs_get = "HDFS-get " + str(num_slaves) + "s-" + str(num_vcores)+ "v" + " "
                            io_times+=[[str_hdfs_get , num_slaves, num_vcores, get_t_s,get_t_e,get_d]]
    
                            str_file_sort = "File-sort " + str(num_slaves) + "s-" + str(num_vcores)+ "v" + " "
                            io_times+=[[str_file_sort , num_slaves, num_vcores, sort_t_s,sort_t_e,sort_d]]
    
                            v_str_hadoop += [str_hadoop]
                            
                            if NETWORK_STATS==1:
                                v_stats_param += [stats_params_s]
                                v_stats_values_s += [stats_values_s]
                                v_stats_values_e += [stats_values_e]
                                #v_stats_ping += [stats_ping_s]
                            
                                # Get only network status after mr
                                v_stats_ping += [stats_ping_e]
    
                        else:
                            hadoop_failed=1
                            if v==1:
                                print("\nHadoop initialization failed!",file=FILE_LOG)
                                print(" Check http://localhost:8088/cluster/nodes/unhealthy",file=FILE_LOG)
                                print(" Check available storage",file=FILE_LOG)
                                print(" Check available ports",file=FILE_LOG)
                        
                        # Cluster is stopped for each iteration
                        cluster_stop(wait_time=HADOOP_STOP_DELAY,hadoop_dir=HADOOP_DIR,hadoop_conf_dir=HADOOP_CONF_DIR,\
                                               timeout=TIMEOUT_STOP,v=v,file_log=FILE_LOG,temp_log=TEMP_LOG)
        
                
                    # Show results
                    print_header(header="Results",v=v,file_log=FILE_LOG)
                    
                
                    # Print execution times to log file
                    if PROFILE_MAP:
                        if v==1:
                            print("Profiled mapper, see output folder for results.",file=FILE_LOG)
                    if PROFILE_RED:
                        if v==1:
                            print("Profiled reducer, see output folder for results.",file=FILE_LOG)
                            
                    print_execution_times(exec_times=exec_times,io_times=io_times,bypass_print=PROFILE_MAP or PROFILE_RED,v=v,file_log=FILE_LOG)
                    if NETWORK_STATS==1:
                        print_network_totals(NODES_LIST,v_str_hadoop,v_stats_param,v_stats_values_s,v_stats_values_e,v_stats_ping,v=v,file_log=FILE_LOG)
            
        # Close log file
        if FILE_LOG!=sys.stdout:
            FILE_LOG.close()
    
    
        
        # Delete temporary files
        os.system("rm " + TEMP_LOG)

# <codecell>


