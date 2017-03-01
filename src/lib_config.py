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
#File: lib_config.py.
#Author: A.J. Vazquez Alvarez (ajvazquez@haystack.mit.edu)
#Description: 
"""
Management of CorrelX configuration files.

"""
#History:
#initial version: 2016.10 ajva
#MIT Haystack Observatory

from __future__ import print_function

import imp
import sys
import os

import const_config
imp.reload(const_config)
from const_config import *

import const_hadoop
imp.reload(const_hadoop)
from const_hadoop import *


try:
    import configparser
except ImportError:
    import ConfigParser as configparser
    

    
    
    
def get_log_file(config_file,suffix="",output_log_folder="e"):
    """
    Get logging files.
    
    Parameters
    ----------
     config_file : str
         path to CorrelX configuration file.
     suffix : str
         suffix (with timestamp) to be added to log filename.
     output_log_folder : str
         suffix to be added to log file path.
     
    Returns
    -------
     file_log : handler to file
         handler to log file.
     temp_log : str
         path to temporary (buffer) file for system calls.
    """
    
    config = configparser.ConfigParser()
    config.optionxform = str
    config.read(config_file)
    
    temp_log=config.get(C_CONF_FILES,C_CONF_FILES_TMP_LOG)
    temp_log+=suffix
    file_log_input=config.get(C_CONF_GEN,C_CONF_GEN_LOG_FILE)
    if file_log_input=="sys.stdout":
        file_log = sys.stdout
    else:
        os.system("mkdir -p "+output_log_folder)
        file_log_input=output_log_folder+"/log_"+suffix+"_ext"
        file_log = open(file_log_input,'w')
        
    return([file_log,temp_log])




def get_nodes_file(config_file):
    """
    Get name of file with list of nodes (from config file).
    
    Parameters
    ----------
     config_file : str
         path to CorrelX configuration file.
     
    Returns
    -------
     file_read_nodes : str
         path to hosts file.
    """
    config = configparser.ConfigParser()
    config.optionxform = str
    config.read(config_file)
    file_read_nodes=config.get(C_CONF_GEN,C_CONF_GEN_HOSTS)
    return(file_read_nodes)
    
    

def get_config_mod_for_this_master(config_file,config_suffix,master_node,script_arg_zero):
    """
    Overwrite all instances of "localhost" in configuration file with master node name 
     into new configuration file (original configuration file is used as a template), 
     "~" with home folder for the current user, "localuser" with the current user,
     and "localpath" with the path of the script_arg_zero (mapred_cx.py).
    
    Parameters
    ----------
     config_file : str
         path to CorrelX configuration file.
     config_suffix : str
         suffix to be added to resulting configuration file.
     master_node : str
         master node name.
     script_arg_zero : str
         path given for the main script (mapred_cx.py).
    
    Returns
    -------
     new_config_file : str
         configuration file plus suffix.
     
    Notes
    -----
    |
    | **TO DO:**
    |
    |  Move new configuration file into folder with logs for this job.
    """
    new_config_file = config_file + config_suffix
    abs_local_path = os.path.dirname(os.path.abspath(script_arg_zero))
    abs_local_path = os.path.abspath(abs_local_path+C_CONF_RELATIVE_PATH_LOCALPATH)

    os.system("sed -e 's/"+C_CONF_RES_LOCALHOST+"/" + master_node + "/' " + config_file +" > " + new_config_file)
    os.system("sed -i \"s@~@$HOME@g\" "  + new_config_file)
    os.system("sed -i \"s@"+C_CONF_RES_LOCALUSER+"@$USER@g\" "  + new_config_file)
    os.system("sed -i \"s@"+C_CONF_RES_LOCALPATH+"@" + abs_local_path + "@g\" "  + new_config_file)
    return(new_config_file)


def get_configuration(file_log,config_file,timestamp_str,v=0):
    """
    Read parameters from configuration file "configh.conf".
    
    Parameters
    ----------
     file_log : handler to file
         handler to log file.
     config_file : str
         path to CorrelX configuration file.
     timestamp_str : str
         suffix to be added to temporary data folder (hwere media will be split).
     v : int
         verbose if 1.
     
    Returns
    -------
     MAPPER : str
         Python mapper (.py).
     REDUCER : str
         Python reducer (.py).
     DEPENDENCIES : str
         Comma separated list of Python files required for mapper and reducer (1.py,2.py,etc).
     PACKETS_PER_HDFS_BLOCK : int
         Number of VDIF frames per file split.
     CHECKSUM_SIZE : int
         Number of bytes for checksum.
     SRC_DIR : str
         Folder with Python sources for mapper, reducer and dependencies.
     APP_DIR : str
         Folder to place mapper, reducer and dependencies in all nodes (in master-associated folder).
     CONF_DIR : str
         Base working folder for configuration files (to be updated later for this master).
     TEMPLATES_CONF_DIR : str
         Folder with templates for core-site.xml,yarn-site.xml,mapred-site.xml,hdfs-site.xml.
     TEMPLATES_ENV_DIR : str
         Folder with templates for hadoop-env.sh, etc.
     HADOOP_DIR : str
         Path to Hadoop home folder.
     HADOOP_CONF_DIR : str
         Path to Hadoop configuration folder (to be updated later for this master).
     NODES : str
         File to write list of nodes to host the cluster (one node per line).
     MAPPERSH : str
         File to write bash script for mapper (call to python script with all arguments).
     REDUCERSH : str
         File to write bash script for reducer (call to python script with all arguments).
     JOBSH : str
         File to write bash script for job request for Hadoop (call to python script with all arguments).
     PYTHON_X : str
         Path to Python executable.
     USERNAME_MACHINES : str
         Username for ssh into the cluster machines.
     MAX_SLAVES : int
         Maximum number of worker nodes (-1 no maximum).
     SLAVES : str
         Filename for Hadoop slaves file.
     MASTERS : str            Filename for Hadoop masters file.
     MASTER_IS_SLAVE : bool
         Boolean, if 1 master is also launching a nodemanager (doing mapreduce).
     HADOOP_TEMP_DIR : str
         Folder for Hadoop temporary folders.
     DATA_DIR : str
         Path with media input files.
     DATA_DIR_TMP : str
         Path to folder to place splits of input file before moving them to the distributed filesystem.
     HDFS_DATA_DIR : str
         Path in the HDFS distributed filesystem to move input splits.
     HADOOP_START_DELAY : str
         Number of seconds to wait after every interaction with Hadoop during the cluster initialization.
     HADOOP_STOP_DELAY : str
         Number of seconds to wait after every interaction with Hadoop during the cluster termination.
     PREFIX_OUTPUT : str
         Prefix for output file.
     HADOOP_TEXT_DELIMITER : str
         Text delimiter for input splits (lib_mapredcorr.run_mapreduce_sh).
     OUTPUT_DIR : str
         Folder in local filesystem to place output file.
     OUTPUT_SYM : str
         Folder within experiment configuration folders to place symbolic link to output file.
     RUN_PIPELINE : bool
         Boolean, if 1 will run in pipeline mode.
     RUN_HADOOP : bool
         Boolean, if 1 will run Hadoop.
     MAX_CPU_VCORES : int
         Maximum number of virtual CPU cores.
     HDFS_REPLICATION : int
         Number of copies of each input split in HDFS.
     OVER_SLURM : bool
         Boolean, 1 to run in a cluster where the local filesystem is NFS (or synchronized among all nodes).
     HDFS_COPY_DELAY : int
         Number of seconds to wait after every interaction with Hadoop during file distribution to HDFS.
     FFT_AT_MAPPER : bool
         Boolean, if 0 FFT is done at reducer (default).
     INI_FOLDER : str
         Folder with experiment .ini files.
     INI_STATIONS : str
         Stations ini file name.
     INI_SOURCES : str
         Sources ini file name.
     INI_DELAY_MODEL : str
         Delay model ini file name.
     INI_DELAYS : str
         Delay polynomials ini file name.
     INI_MEDIA : str
         Media ini file name.
     INI_CORRELATION : str
         Correlation ini file name.
     INTERNAL_LOG_MAPPER
         [remove] currently default 0.
     INTERNAL_LOG_REDUCER
         [remove] currenlty default 0.
     ADJUST_MAPPERS : float
         Force number of mappers computed automatically to be multiplied by this number.
     ADJUST_REDUCERS : float
         Force number of reducers computed automatically to be multiplied by this number.
     FFTS_PER_CHUNK
        [Remove] Number of DFT windows per mapper output, -1 by default (whole frame)
     TEXT_MODE : bool
        True by default.
     USE_NOHASH_PARTITIONER : bool
        True to use NoHash partitioner.
     USE_LUSTRE_PLUGIN : bool
        True to use Lustre plugin for Hadoop.
     LUSTRE_USER_DIR : str
        Absolute path for the Lustre working path (used in mapreduce job).
     LUSTRE_PREFIX : str
        Path in Lustre to preceed HDFS_DATA_DIR if using Lustre.
     ONE_BASELINE_PER_TASK : int
        0 by default (if 1, old implementation allowed scaling with one baseline per task in the reducers).
     MIN_MAPPER_CHUNK
        [Remove] Chunk constraints for mapper.
     MAX_MAPPER_CHUNK
        [Remove] Chunk constraints for mapper.
     TASK_SCALING_STATIONS: int
        0 by default (if 1, old implementation allowed linear scaling per task in the reducers).
     SORT_OUTPUT : bool
        If 1 will sort lines in output file.
     BM_AVOID_COPY : bool
        If 1 will not split and copy input files if this has already been done previously (for benchmarking).
     BM_DELETE_OUTPUT : bool
        If 1 will not retrieve output file from distributed filesystem (for benchmarking).
     TIMEOUT_STOP : int
        Number of seconds to  wait before terminating nodes during cluster stop routine.
     SINGLE_PRECISION : bool
        If 1 computations will be done in single precision.
     PROFILE_MAP: int
        | if 1 will generate call graphs with timing information for mapper (requires Python Call Graph package),
        | if 2 will use cProfile.
     PROFILE_RED : int
        | if 1 will generate call graphs with timing information for reducer (requires Python Call Graph package),
        | if 2 will use cProfile.
    
    Notes
    -----
    |
    | **Configuration:**
    |
    |  All constants taken from const_config.py and const_hadoop.py.
    |
    |
    | **TO DO:**
    |
    |  OVER_SLURM: explain better, and check assumptions.
    |  Remove INTERNAL_LOG_MAPPER and INTERNAL_LOG_REDUCER.
    |  Remove FFTS_PER_CHUNK,MIN_MAPPER_CHUNK and MAX_MAPPER_CHUNK.
    |  Check that SINGLE_PRECISION is followed in mapper and reducer.
    """
    
   
    #config_file="configh.conf"
    config = configparser.ConfigParser()
    config.optionxform = str
    config.read(config_file)

    if v==1:
        print("\nReading configuration file...",file=file_log)    

        
    # General
    RUN_PIPELINE =           config.getboolean( C_CONF_GEN, C_CONF_GEN_RUN_PIPE)  
    SORT_OUTPUT =            config.getboolean( C_CONF_GEN, C_CONF_GEN_SORT_OUTPUT)
    RUN_HADOOP =             config.getboolean( C_CONF_GEN, C_CONF_GEN_RUN_HADOOP) 
    OVER_SLURM =             config.getboolean( C_CONF_GEN, C_CONF_GEN_OVER_SLURM) 
    USE_NOHASH_PARTITIONER = config.getboolean(C_CONF_GEN, C_CONF_GEN_USE_NOHASH)
    USE_LUSTRE_PLUGIN =      config.getboolean( C_CONF_GEN, C_CONF_GEN_USE_LUSTRE)
    LUSTRE_USER_DIR =        config.get(        C_CONF_GEN, C_CONF_GEN_LUSTRE_USER_FOLDER)
    LUSTRE_PREFIX =          config.get(        C_CONF_GEN, C_CONF_GEN_LUSTRE_PREFIX)     
    INTERNAL_LOG_MAPPER =    0                                                                # TO DO: remove
    INTERNAL_LOG_REDUCER =   0                                                                # TO DO: remove
        
        
        
    # Benchmarking
    BM_AVOID_COPY =     config.getboolean( C_CONF_BENCHMARKING, C_CONF_BENCHMARKING_AVOID_COPY)
    BM_DELETE_OUTPUT =  config.getboolean( C_CONF_BENCHMARKING, C_CONF_BENCHMARKING_DELETE_OUTPUT)

    
    
    # Profiling
    PROFILE_MAP =       config.getboolean( C_CONF_PROFILING, C_CONF_PROFILING_MAP)
    PROFILE_RED =       config.getboolean( C_CONF_PROFILING, C_CONF_PROFILING_RED)
    use_pycallgraph =   config.getboolean( C_CONF_PROFILING, C_CONF_PROFILING_PYCALLGRAPH)
    if not(use_pycallgraph):
        PROFILE_MAP = 2*int(PROFILE_MAP)
        PROFILE_RED = 2*int(PROFILE_RED)
    
    
    # Files
    MAPPER =             config.get(       C_CONF_FILES, C_CONF_FILES_MAP)
    REDUCER =            config.get(       C_CONF_FILES, C_CONF_FILES_RED)
    DEPENDENCIES =       config.get(       C_CONF_FILES, C_CONF_FILES_DEP).split(',')
    MAPPERSH =           config.get(       C_CONF_FILES, C_CONF_FILES_MAP_BASH)
    REDUCERSH =          config.get(       C_CONF_FILES, C_CONF_FILES_RED_BASH)
    JOBSH =              config.get(       C_CONF_FILES, C_CONF_FILES_JOB_BASH)
    PYTHON_X =           config.get(       C_CONF_FILES, C_CONF_FILES_PYTHON)
    NODES =              config.get(       C_CONF_FILES, C_CONF_FILES_NODES)
    SRC_DIR =            config.get(       C_CONF_FILES, C_CONF_FILES_SRC_DIR)
    if SRC_DIR[-1]!="/":
        SRC_DIR+=("/")
    APP_DIR =            config.get(       C_CONF_FILES, C_CONF_FILES_APP_DIR)
    if APP_DIR[-1]!="/":
        APP_DIR+=("/")
    CONF_DIR =           config.get(       C_CONF_FILES, C_CONF_FILES_CONF_DIR)
    if CONF_DIR[-1]!="/":
        CONF_DIR+=("/")
    TEMPLATES_CONF_DIR = config.get(       C_CONF_FILES, C_CONF_FILES_CONF_TEMPLATES)
    if TEMPLATES_CONF_DIR[-1]!="/":
        TEMPLATES_CONF_DIR+=("/")
    TEMPLATES_ENV_DIR = config.get(       C_CONF_FILES, C_CONF_FILES_ENV_TEMPLATES)
    if TEMPLATES_ENV_DIR[-1]!="/":
        TEMPLATES_ENV_DIR+=("/")
    HADOOP_DIR =         config.get(       C_CONF_FILES, C_CONF_FILES_HADOOP_DIR)
    if HADOOP_DIR[-1]!="/":
        HADOOP_DIR+=("/")
    HADOOP_CONF_DIR = HADOOP_DIR+HADOOP_REL_ETC                                   # (!) This is overriten later at get_conf_out_dirs()
    HADOOP_TEMP_DIR =    config.get(       C_CONF_FILES, C_CONF_FILES_TMP_DIR)
    if HADOOP_TEMP_DIR[-1]!="/":
        HADOOP_TEMP_DIR+=("/")
    DATA_DIR_TMP =       config.get(       C_CONF_FILES, C_CONF_FILES_TMP_DATA_DIR)
    if DATA_DIR_TMP[-1]!="/":
        DATA_DIR_TMP+=("/")
    OUTPUT_DIR =         config.get(       C_CONF_FILES, C_CONF_FILES_OUT_DIR)
    if OUTPUT_DIR[-1]!="/":
        OUTPUT_DIR+=("/")  
    USERNAME_MACHINES =  config.get(       C_CONF_FILES, C_CONF_FILES_USERNAME_MACHINES)
    PREFIX_OUTPUT =      config.get(       C_CONF_FILES, C_CONF_FILES_PREFIX_OUTPUT)
    
    
    
    # HDFS
    HDFS_DATA_DIR =          config.get(    C_CONF_HDFS, C_CONF_HDFS_INPUT_DATA_DIR)
    HDFS_DATA_DIR_SUFFIX =   config.get(    C_CONF_HDFS, C_CONF_HDFS_DIR_SUFFIX)
    HDFS_DATA_DIR+=HDFS_DATA_DIR_SUFFIX
    if HDFS_DATA_DIR[-1]!="/":
        HDFS_DATA_DIR+=("/")
    PACKETS_PER_HDFS_BLOCK = config.getint( C_CONF_HDFS, C_CONF_HDFS_PACKETS_PER_BLOCK)
    CHECKSUM_SIZE =          config.getint( C_CONF_HDFS, C_CONF_HDFS_CHECKSUM_SIZE) # Has to divide the block size (which is a multiple of 32)
    # PACKET SIZE is generated with "generate_vdif", next cell.    
    
    
    
    # Slaves
    SLAVES =                 config.get(    C_CONF_HSLAVES, C_CONF_H_ALL_CONFIG_FILE)
    MAX_SLAVES =             config.getint( C_CONF_HSLAVES, C_CONF_HSLAVES_MAX_SLAVES)
    
    
    
    # Masters
    MASTERS =           config.get(         C_CONF_HMASTERS, C_CONF_H_ALL_CONFIG_FILE)
    MASTER_IS_SLAVE =   config.getboolean(  C_CONF_HMASTERS, C_CONF_HMASTERS_MASTER_IS_SLAVE)



    # Hadoop-other
    HADOOP_START_DELAY =     config.getint(     C_CONF_OTHER, C_CONF_OTHER_START_DELAY)
    HADOOP_STOP_DELAY =      config.getint(     C_CONF_OTHER, C_CONF_OTHER_STOP_DELAY)
    HDFS_COPY_DELAY =        config.getint(     C_CONF_OTHER, C_CONF_OTHER_COPY_DELAY)
    TEXT_MODE =              config.getboolean( C_CONF_OTHER, C_CONF_OTHER_TEXT_MODE)
    HADOOP_TEXT_DELIMITER =  config.get(        C_CONF_OTHER, C_CONF_OTHER_TEXT_DELIMITER)
    MAX_CPU_VCORES =         config.getint(     C_CONF_OTHER, C_CONF_OTHER_MAX_CPU_VCORES)
    FFT_AT_MAPPER =          config.getboolean( C_CONF_OTHER, C_CONF_OTHER_FFT_MAP)
    ADJUST_MAPPERS =         config.getfloat(   C_CONF_OTHER, C_CONF_OTHER_ADJ_MAP)
    ADJUST_REDUCERS =        config.getfloat(   C_CONF_OTHER, C_CONF_OTHER_ADJ_RED)
    ONE_BASELINE_PER_TASK =  config.getboolean( C_CONF_OTHER, C_CONF_OTHER_ONE_BASELINE)
    TASK_SCALING_STATIONS =  config.getboolean( C_CONF_OTHER, C_CONF_OTHER_SCALING_STATIONS)
    TIMEOUT_STOP =           config.getint(     C_CONF_OTHER, C_CONF_OTHER_TIMEOUT_STOP_NODES)
    SINGLE_PRECISION =       config.getboolean( C_CONF_OTHER, C_CONF_OTHER_SINGLE_PRECISION)
    FFTS_PER_CHUNK =         -1                                                                # TO DO: remove
    MIN_MAPPER_CHUNK =       -1                                                                # TO DO: remove
    MAX_MAPPER_CHUNK =       -1                                                                # TO DO: remove
    
    
    
    # Experiment
    INI_FOLDER =                         config.get(   C_CONF_EXP, C_CONF_EXP_FOLDER)
    INI_STATIONS = INI_FOLDER + "/" +    config.get(   C_CONF_EXP, C_CONF_EXP_STATIONS)
    INI_SOURCES = INI_FOLDER + "/" +     config.get(   C_CONF_EXP, C_CONF_EXP_SOURCES)
    INI_DELAYS = INI_FOLDER + "/" +      config.get(   C_CONF_EXP, C_CONF_EXP_DELAYS)
    INI_DELAY_MODEL = INI_FOLDER + "/" + config.get(   C_CONF_EXP, C_CONF_EXP_DELAY_MODEL)
    INI_MEDIA = INI_FOLDER + "/" +       config.get(   C_CONF_EXP, C_CONF_EXP_MEDIA)
    INI_CORRELATION = INI_FOLDER + "/" + config.get(   C_CONF_EXP, C_CONF_EXP_CORRELATION)
    DATA_DIR = INI_FOLDER + "/" +        config.get(   C_CONF_EXP, C_CONF_EXP_MEDIA_SUB) + "/"
    OUT_DIR_PREFIX =                     config.get(   C_CONF_EXP, C_CONF_EXP_MEDIA_SUB_PREFIX)
    
     
     
    #TO DO: vcores value is forced from MAX_CPU_VCORES... maybe it has to be taken out from the configuration file section... fix this.
    
    HDFS_REPLICATION =                   config.getint( C_CONF_H_HDFS, C_H_HDFS_REPLICATION)

    ##AUTO_STATIONS = config.getint("Correlation",'Corr. same station')
    ##AUTO_THREADS = config.getint("Correlation",'Corr. same thread')
    
    OUTPUT_SYM=INI_FOLDER+"/"+OUT_DIR_PREFIX+"_"+timestamp_str+"/"


    if v==1:
        #if SAMPLES_PER_PACKET<FFT_SIZE:
        #    print(" (!) The packet has fewer samples than the size of the FFT",file=file_log)
   
        print(" FFT at mapper:\t\t\t" + str(int(FFT_AT_MAPPER)),file=file_log)
        print(" Hadoop delays: [initialization = " + str(HADOOP_START_DELAY) + " s], [termination = " + str(HADOOP_STOP_DELAY) + " s], [HDFS = " + str(HDFS_COPY_DELAY) + " s]",file=file_log)
        
        if MAX_SLAVES>0:
            print(" Iterating: NODES (max=" +str(MAX_SLAVES) + ")",file=file_log)
        else:
            print(" Single run max. number of available nodes",file=file_log)
        
        if OVER_SLURM:
            print(" ---> SLURM - Configured for running on SLURM",file=file_log)
        else:
            print(" ---> VBox - Configured for running on VBox",file=file_log)
    
        print(" Temp dir overriden (default: /tmp)",file=file_log)
    
    return([MAPPER, REDUCER, DEPENDENCIES, PACKETS_PER_HDFS_BLOCK,CHECKSUM_SIZE,\
            SRC_DIR,APP_DIR, CONF_DIR, TEMPLATES_CONF_DIR, TEMPLATES_ENV_DIR, HADOOP_DIR,HADOOP_CONF_DIR,NODES,
            MAPPERSH,REDUCERSH,JOBSH,PYTHON_X,\
            USERNAME_MACHINES,MAX_SLAVES,SLAVES,MASTERS,MASTER_IS_SLAVE,HADOOP_TEMP_DIR,DATA_DIR,DATA_DIR_TMP,HDFS_DATA_DIR,HADOOP_START_DELAY,HADOOP_STOP_DELAY,\
            PREFIX_OUTPUT,HADOOP_TEXT_DELIMITER,OUTPUT_DIR,OUTPUT_SYM,RUN_PIPELINE,RUN_HADOOP,MAX_CPU_VCORES,\
            HDFS_REPLICATION,OVER_SLURM,HDFS_COPY_DELAY,\
            FFT_AT_MAPPER,INI_FOLDER,\
            INI_STATIONS, INI_SOURCES, INI_DELAY_MODEL, INI_DELAYS, INI_MEDIA, INI_CORRELATION,\
            INTERNAL_LOG_MAPPER,INTERNAL_LOG_REDUCER,ADJUST_MAPPERS,ADJUST_REDUCERS,FFTS_PER_CHUNK,TEXT_MODE,\
            USE_NOHASH_PARTITIONER,USE_LUSTRE_PLUGIN,LUSTRE_USER_DIR,LUSTRE_PREFIX,ONE_BASELINE_PER_TASK,\
            MIN_MAPPER_CHUNK,MAX_MAPPER_CHUNK,TASK_SCALING_STATIONS,SORT_OUTPUT,BM_AVOID_COPY,\
            BM_DELETE_OUTPUT,TIMEOUT_STOP,SINGLE_PRECISION,PROFILE_MAP,PROFILE_RED])
    


def update_config_param(source_file,pairs,v=0,file_log=sys.stdout):
    """
    Updates a list of pairs [parameter,value] in a configuration file. Should be valid to any .ini file, but this is 
     used to override the configuration on the CorrelX configuration file.
    
    Parameters
    ----------
     source_file : str
         configuration file (.ini).
     pairs : list
         list of [parameter,value].
     v : int
         verbose if 1.
     file_log : file handler
         handler for log file.
     
    Returns
    -------
     N/A
     
    Notes
    -----
    |
    | **TO DO:**
    |
    |  Currently parameters that are not found are not added, this should be reported.
    """
    if v==1:
        print("\nOverriding configuration file!",file=file_log) 
    destination_file = source_file

    isamec = ""
    copy_again=1
    destination_file+=".tmp"
    isamec = " mv " + destination_file + " " + source_file
        

    command = ""
    iteration=0

    for pair in pairs:
        iteration+=1
        if iteration==1:
            #command+="sed -e 's/" + pair[0] + ".*/" + pair[0] + "\\t\\t" + pair[1] + "/g' " + source_file
            command+="sed -e 's;" + pair[0] + ".*;" + pair[0] + "\\t\\t" + pair[1] + ";g' " + source_file
        if iteration>0:
            #command+="|sed -e 's/" + pair[0] + ".*/" + pair[0] + "\\t\\t" + pair[1] + "/g' " 
            command+="|sed -e 's;" + pair[0] + ".*;" + pair[0] + "\\t\\t" + pair[1] + ";g' " 

    command+= "> " + destination_file 

    os.system(command)
    os.system(isamec)

    if v==1:
        for pair in pairs: #[1:]:
            print("  " + pair[0].ljust(49) + " " + pair[1],file=file_log) 
            
    # Now check if any parameter was not found, and thus was not updated
    # Checks lowercase!
    with open(source_file, 'r') as f:
        lines = f.read()
        for pair in pairs:
            count=1
            if pair[0].lower() in lines.lower():
                count=0
            if count==1:
                if v==1:
                    print("  (!) Parameter not found: " + pair[0].ljust(49),file=file_log) 
                    
                    
                    
def override_configuration_parameters(forced_configuration_string,config_file,v=1,file_log=sys.stdout):
    """
    This function takes as input the string with the parameters to the main script and overrides the corresponding parameters
    in the configuration files. This is to simplify batch testing.
    
    Parameters
    ----------
     forced_configuration_string : str
         Comma separated list of parameter0=value0,parameter1=value1,...
     config_file : str
         Path to CorrelX configuration file.
     v : int
         Verbose if 1.
     file_log : file handler
         Handler for log file.
    
    Output:
    -------
     N/A
    
    Notes
    -----
    |
    | **Assumptions:**
    |
    |  Assuming that C_H_MAPRED_RED_OPTS is higher than C_H_MAPRED_MAP_OPTS, so the first value is 
    |    taken for C_H_MAPRED_CHILD_OPTS.
    |
    |
    | **Notes:**
    |
    |  For new parameters in configh.conf:
    |  (1) Add constants for CLI in const_config.py.
    |  (2) Check/add constants for hadoop configuration files in const_hadoop.py (if applicable).
    |  (3) Add parameter reading in get_configuration().
    |  (4) Add option in if-structure below.
    """
    # Override configuration parameters
    # e.g.: -f stations=3,slowstart=0.4,ppb=1
    read_configuration_pairs=[i.split('=') for i in forced_configuration_string.split(',')]
    forced_configuration_pairs=[]
    unr_parameters=[]
    for pair in read_configuration_pairs:
        if len(pair)<2:
            unr_parameters+=[pair]
        else:
            [parameter,value] = pair


            if parameter==C_ARG_PPB:                                                             # Num. VDIF frames per block
                forced_configuration_pairs+=[[C_CONF_HDFS_PACKETS_PER_BLOCK+":",value]]
            
            elif parameter==C_ARG_SLOWSTART:                                                     # Mapreduce slowstart
                forced_configuration_pairs+=[[C_H_MAPRED_SLOWSTART+":",value]]
            
            elif parameter==C_ARG_REPLICATION:                                                   # HDFS replication
                forced_configuration_pairs+=[[C_H_HDFS_REPLICATION+":",value]] 
            
            elif parameter==C_ARG_FFTM:                                                          # FFT at mapper
                forced_configuration_pairs+=[[C_CONF_OTHER_FFT_MAP+":",value]]
            
            elif parameter==C_ARG_FFTR:                                                          # FFT at reducer
                forced_configuration_pairs+=[[C_CONF_OTHER_FFT_MAP+":",str(int(not(bool(value))))]]
                       
            elif parameter==C_ARG_ADJM:                                                          # Factor to multiply number of mappers   
                forced_configuration_pairs+=[[C_CONF_OTHER_ADJ_MAP+":",value]] 
            
            elif parameter==C_ARG_ADJR:                                                          # Factor to multiply number of reducers
                forced_configuration_pairs+=[[C_CONF_OTHER_ADJ_RED+":",value]]
            
            elif parameter==C_ARG_VCORES:                                                        # Number of cores per node
                forced_configuration_pairs+=[[C_H_YARN_MAX_VCORES+":",value]]
                forced_configuration_pairs+=[[C_H_YARN_RES_VCORES+":",value]]
                forced_configuration_pairs+=[[C_CONF_OTHER_MAX_CPU_VCORES+":",value]]
            
            elif parameter==C_ARG_VCORESPERMAP:                                                  # Number of cores per map task
                forced_configuration_pairs+=[[C_H_MAPRED_VCORES_MAP+":",value]]
            
            elif parameter==C_ARG_VCORESPERRED:                                                  # Number of cores per reduce task
                forced_configuration_pairs+=[[C_H_MAPRED_VCORES_RED+":",value]]
            
            elif parameter==C_ARG_MAPSPERNODE:                                                   # Max. maps per node
                forced_configuration_pairs+=[[C_H_MAPRED_MAP_MAX+":",value]]
            
            elif parameter==C_ARG_REDUCESPERNODE:                                                # Max. reduces per node
                forced_configuration_pairs+=[[C_H_MAPRED_RED_MAX+":",value]]
            
            elif parameter==C_ARG_NODEMEM:                                                       # Memory per node (maps+reduces)
                forced_configuration_pairs+=[[C_H_YARN_MAX_MEM_MB+":",value]]  
                forced_configuration_pairs+=[[C_H_YARN_RES_MEM_MB+":",value]]  
            
            elif parameter==C_ARG_CONTAINERMEM_MAP:                                              # Memory per container (mapper)
                forced_configuration_pairs+=[[C_H_YARN_MAP_MB+":",value]]
            
            elif parameter==C_ARG_CONTAINERMEM_RED:                                              # Memory per container (reducer)
                forced_configuration_pairs+=[[C_H_YARN_RED_MB+":",value]]
            
            elif parameter==C_ARG_CONTAINERMEM_AM:                                               # Memory per container (app manager)
                forced_configuration_pairs+=[[C_H_YARN_AM_RES_MB+":",value]]
            
            elif parameter==C_ARG_CONTAINERHEAP_MAP:                                             # Memory heap per container (mapper)  
                #forced_configuration_pairs+=[[C_H_MAPRED_MAP_OPTS+":",    "-Xms"+value+"m -Xmx"+value+"m"]] # init
                forced_configuration_pairs+=[[C_H_MAPRED_MAP_OPTS+":", "-Xmx"+value+"m"]]                    # no init
                
            elif parameter==C_ARG_CONTAINERHEAP_RED:                                             # Memory heap per container (mapper)  
                #forced_configuration_pairs+=[[C_H_MAPRED_RED_OPTS+":",    "-Xms"+value+"m -Xmx"+value+"m"]] # init
                #forced_configuration_pairs+=[[C_H_MAPRED_CHILD_OPTS+":", "-Xms"+value+"m -Xmx"+value+"m"]] # init
                forced_configuration_pairs+=[[C_H_MAPRED_RED_OPTS+":", "-Xmx"+value+"m"]]                    # no init
                forced_configuration_pairs+=[[C_H_MAPRED_CHILD_OPTS+":", "-Xmx"+value+"m"]]                 # no init
            
            elif parameter==C_ARG_CONTAINERHEAP_AM:                                             # Memory heap per container (mapper)  
                #forced_configuration_pairs+=[[C_H_MAPRED_AM_OPTS+":", "-Xms"+value+"m -Xmx"+value+"m"]] # init
                forced_configuration_pairs+=[[C_H_MAPRED_AM_OPTS+":", "-Xmx"+value+"m"]]                    # no init


                
                
                forced_configuration_pairs+=[[C_H_MAPRED_MAP_OPTS+":", "-Xmx"+value+"m"]]                   # no init
                forced_configuration_pairs+=[[C_H_MAPRED_RED_OPTS+":", "-Xmx"+value+"m"]]                   # no init

            elif parameter==C_ARG_SORTMEM:                                                       # Sort memory (after map)
                forced_configuration_pairs+=[[C_H_MAPRED_SORT_MB+":",value]]
            
            elif parameter==C_ARG_BLOCKSIZE:                                                     # HDFS block size
                forced_configuration_pairs+=[[C_H_HDFS_BLOCKSIZE+":",value]]
            
            elif parameter==C_ARG_MASTERWORKS:                                                   # Master node is also slave
                forced_configuration_pairs+=[[C_CONF_HMASTERS_MASTER_IS_SLAVE+":",value]]
            
            elif parameter==C_ARG_TASKSPERJVM:                                                   # Tasks before restaring JVM
                forced_configuration_pairs+=[[C_H_MAPRED_JVM_NUMTASKS+":",value]]
            
            elif parameter==C_ARG_AVOIDCOPY:                                                     # Do not re-copy input files
                forced_configuration_pairs+=[[C_CONF_BENCHMARKING_AVOID_COPY+":",value]]
            
            elif parameter==C_ARG_DELETEOUTPUT:                                                  # Delete output file (for benchmarking)
                forced_configuration_pairs+=[[C_CONF_BENCHMARKING_DELETE_OUTPUT+":",value]]
            
            elif parameter==C_ARG_SCALEST:                                                       # Linear scaling stations
                forced_configuration_pairs+=[[C_CONF_OTHER_SCALING_STATIONS+":",value]]
            
            elif parameter==C_ARG_MEDIASUFFIX:                                                   # Media suffix folder
                forced_configuration_pairs+=[[C_CONF_HDFS_DIR_SUFFIX+":",value]]
            
            elif parameter==C_ARG_SINGLEPRECISION:                                               # Single precision in computations
                forced_configuration_pairs+=[[C_CONF_OTHER_SINGLE_PRECISION+":",value]]
            
            #elif parameter==C_ARG_DATA:                                                          # Data directory
            #    list_value=value.split("/")
            #    value='\/'.join(map(str, list_value))
            #    forced_configuration_pairs+=[["Data directory:",value]]
            
            elif parameter==C_ARG_EXPER:                                                          # Experiment folder
                forced_configuration_pairs+=[[C_CONF_EXP_FOLDER+":",value]]
                
            elif parameter==C_ARG_OUT:                                                            # Output folder
                forced_configuration_pairs+=[[C_CONF_FILES_OUT_DIR+":",value]]
            
            elif parameter==C_ARG_APP:                                                            # Application folder
                forced_configuration_pairs+=[[C_CONF_FILES_APP_DIR+":",value]]
               
            elif parameter==C_ARG_SERIAL:                                                         # Run pipeline mode
                forced_configuration_pairs+=[[C_CONF_GEN_RUN_PIPE+":",value]]
            
            elif parameter==C_ARG_PARALLEL:                                                       # Run Hadoop
                forced_configuration_pairs+=[[C_CONF_GEN_RUN_HADOOP+":",value]]
            
            elif parameter==C_ARG_NM_LOC_PORT:                                                    # Nodemanager ports
                forced_configuration_pairs+=[[C_H_YARN_NM_LOCALIZER_ADDRESS+":",HOSTNAME_HADOOP+":"+value]]
            elif parameter==C_ARG_NM_WEB_PORT:                                                    
                forced_configuration_pairs+=[[C_H_YARN_NM_WEBAPP_ADDRESS+":",HOSTNAME_HADOOP+":"+value]]
            elif parameter==C_ARG_SHUFFLE_PORT:                                                    
                forced_configuration_pairs+=[[C_H_MAPRED_SHUFFLE_PORT+":",value]]
            
            
            else:                                                                                 # Unknown parameter, simply report it
                unr_parameters+=[parameter]
                
    update_config_param(source_file=config_file,pairs=forced_configuration_pairs,v=v,file_log=file_log)
    
    if v==1:
        if unr_parameters!=[]:
            print("\n Unrecognized parameters:",file=file_log)
            for i in unr_parameters:
                print("  "+''.join(map(str,i)),file=file_log)




def get_list_configuration_files(config_file):
    """
    Get list of Hadoop configuration files.
    
    Parameters
    ----------
     config_file : str
         Path to CorrelX configuration file.
     
    Returns
    -------
     list_configurations : list
         List of sections from configuration file associated to lists in "pairs_config" below.
     pairs_config : list
         List of pairs [[param0,value0],[param1,value1]...]...,[[...]...,[...]]] to update
                                 Hadoop config files later.
    """
    config = configparser.ConfigParser()
    config.optionxform = str
    config.read(config_file)
    
    list_configurations=[i for i in config.sections() if (C_CONF_PREH in i)and(C_CONF_SUF_SLAVES not in i)and(C_CONF_SUF_OTHER not in i)]
    
    pairs_config = [[list(j) for j in config.items(i)] for i in list_configurations  ]

    
    return([list_configurations,pairs_config])



        
    
def is_this_node_master(master,temp_log,v=0,file_log=sys.stdout):
    """
    Devised in case the script was run in parallel at many nodes.
    Currenlty simply used to enforce that only one node is running as master.
    
    Parameters
    ----------
     master : str
         master node name.
     temp_log : str
         path to temporary file for system calls (buffer).
     v : int
         verbose if 1.
     file_log : file handler
         handler for log file.
    
    Returns
    -------
     this_is_master : int
         1 if current node is the master, 0 otherwise.
     my_name : str
         current node name.
     my_ip : str
         current node IP address.
    
    Notes
    -----
    |
    | **TO DO:**
    |
    |  Simplify this.
    """
    os.system("hostname > " + temp_log) 
    os.system("hostname -I >> " + temp_log) 
    this_is_master = 0
    local_info=[]
    thiss=" not "
    with open(temp_log, 'r') as f_tmp:
        for line in f_tmp:
            local_info += line.strip().split(",")
            if master in line:
                this_is_master = 1
                thiss=" "
    
    my_name = local_info[0]
    my_ip = local_info[1]
    if v==1:
        print("\nChecking if this node is master:",file=file_log)   
        print(" Node " + my_name + " (" + my_ip + ") is" + thiss + "master",file=file_log)
    
    return([this_is_master,my_name,my_ip])




def overwrite_nodes_file(nodes_list,nodes_file,v=0,file_log=sys.stdout):
    """
    Overwrite nodes file (in case less nodes are requested than are available).
    
    Parameters
    ----------
     nodes_list : list of str
         names of the nodes in the allocation.
     nodes_file : str
         path to nodes file.
     v : int
         verbose if 1.
     file_log : file handler
         handler for log file.
    """
    if v==1:
        print("\nCreating nodes file:",file=file_log)   
        print(" " + nodes_file,file=file_log)

    with open(nodes_file, 'w') as f:
        for i in nodes_list:
            print(i,file=f)
            if v==1:
                print("  " + i,file=file_log)
    if v==1:
        print(" Nodes file overwritten!",file=file_log)      



def create_directories(directories,v=0,file_log=sys.stdout):
    """
    Create directories from a list of str with their paths.
    """
    if v==1:
        print("\nCreating directories:",file=file_log)    
    for i in directories:
        if not os.path.isdir(i):
            os.system("mkdir -p " + i)
        if v==1:
            print(" "+i,file=file_log)    
            
            

def get_conf_out_dirs(master_name,hadoop_dir,app_dir,conf_dir,suffix_conf,output_dir,suffix_out,v=1,file_log=sys.stdout):
    """
    Get paths for configuration and output folders.
    App and Conf directories are modified with master's name.
    
    Parameters
    ----------
     master_name : str
         master node hostname.
     hadoop_dir : str
         hadoop base folder.
     app_dir : str
         base app folder.
     conf_dir : str
         base configuration folder.
     suffix_conf : str
         suffix for configuration folder.
     output_dir : str
         base output folder.
     suffix_out : str
         suffix for output folder (only the string after the last "/" is taken).
     v : int
         verbose if 1.
     file_log : file handler
         handler for log file.
     
    Returns
    -------
     app_dir : str
         path to app folder (modified for this master).
     conf_dir : str
         path to configuration folder (for modified config file and bash scripts).
     hadoop_conf_dir : str
         path to hadoop configuration folder for master node.
     hadoop_default_conf_dir : str
         path to hadoop default configuration folder (to be used at slaves nodes).
     output_dir : str
         path in local filesystem for output file.
     
    Notes
    -----
     Having a different folder associated to the master node allows to run multiple deployments in the same cluster if
      the local filesystem is NFS.
    """

    if "/" in suffix_out:
        suffix_out=suffix_out.split('/')[-1]

    
    conf_dir+=master_name + "/"                                        # Create conf dir for each node (still shared...)
    app_dir+=master_name + "/"                                         # Create app dir for each node (still shared...)
    hadoop_conf_dir = conf_dir + "etc_hadoop_" + suffix_conf +"/"      # Hadoop conf dir for this host
    output_dir = output_dir + suffix_out +"/"                          # Create more folders for different experiments

    create_directories(directories=[conf_dir,hadoop_conf_dir,output_dir],v=v,file_log=file_log)

    # This is for slaves, for distributing files
    hadoop_default_conf_dir = hadoop_dir+HADOOP_REL_ETC
    
    
    return([app_dir,conf_dir,hadoop_conf_dir,hadoop_default_conf_dir,output_dir])



def reduce_list_nodes(num_slaves,nodes_list,v=1,file_log=sys.stdout):
    """
    Reduce list of nodes given a maximum number of nodes.
    
    Parameters
    ----------
     num_slaves : int
         maximum number of slaves (-1 for no maximum).
     nodes_list : list of str
         names of nodes.
     
    Output:
    -------
     num_slaves:  number of nodes in updated list.
     nodes_list:  updated nodes_list.
    """
    
    if num_slaves>0 and num_slaves<len(nodes_list):
        nodes_list=nodes_list[:num_slaves]
        if v==1:
            print("\nForcing number of nodes: ".ljust(24) + str(num_slaves),file=file_log) 
    else:
        num_slaves=len(nodes_list)
    
    return([num_slaves,nodes_list])



