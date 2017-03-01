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
#File: const_config.py.
#Author: A.J. Vazquez Alvarez (ajvazquez@haystack.mit.edu)
#Description:
"""
  Constants for CorrelX configuration:
  -Configuration file (configh.conf).
  -Arguments for command line interface.

Notes
-----
  -Follow constant convention as defined in each section.
  
"""
#History:
#initial version: 2016.11 ajva
#MIT Haystack Observatory


# ------------ Configuration file (configh.conf) ------------ 
#
# Structure:
#
#  C_CONF_<header>='header as in config file'
#  C_CONF_<header>_<field>='field as in config file'
#

C_CONF_GEN =                     "General"
C_CONF_GEN_LOG_FILE =            'Log file'
C_CONF_GEN_HOSTS =               'Hosts list file'
C_CONF_GEN_RUN_PIPE =            'Run pipeline'
C_CONF_GEN_SORT_OUTPUT =         'Sort output'
C_CONF_GEN_RUN_HADOOP =          'Run hadoop'
C_CONF_GEN_OVER_SLURM =          'Over SLURM'
C_CONF_GEN_USE_NOHASH =          'Use NoHash partitioner'
C_CONF_GEN_USE_LUSTRE =          'Use Lustre plugin'
C_CONF_GEN_LUSTRE_USER_FOLDER =  'Lustre user folder'
C_CONF_GEN_LUSTRE_PREFIX =       'Lustre prefix'


C_CONF_BENCHMARKING =            "Benchmarking"
C_CONF_BENCHMARKING_AVOID_COPY = 'Avoid copying input files (lustre)'
C_CONF_BENCHMARKING_DELETE_OUTPUT ='Delete output files (lustre)'


C_CONF_PROFILING =              "Profiling"
C_CONF_PROFILING_MAP =          "Profile mapper (pipeline)"
C_CONF_PROFILING_RED =          "Profile reducer (pipeline)"
C_CONF_PROFILING_PYCALLGRAPH =  "Use PyCallGraph"


C_CONF_FILES =                  "Files"
C_CONF_FILES_MAP =              'Mapper'
C_CONF_FILES_RED =              'Reducer'
C_CONF_FILES_DEP =              'Dependencies'
C_CONF_FILES_MAP_BASH =         'Mapper bash'
C_CONF_FILES_RED_BASH =         'Reducer bash'
C_CONF_FILES_JOB_BASH =         'Job bash'
C_CONF_FILES_PYTHON =           'Python executable'
C_CONF_FILES_NODES =            'Nodes'
C_CONF_FILES_SRC_DIR =          'Src directory'
C_CONF_FILES_APP_DIR =          'App directory'
C_CONF_FILES_CONF_DIR =         'Conf directory'
C_CONF_FILES_CONF_TEMPLATES =   'Conf templates'
C_CONF_FILES_ENV_TEMPLATES =    'Env templates'
C_CONF_FILES_HADOOP_DIR =       'Hadoop directory'
C_CONF_FILES_TMP_DIR =          'Temp directory'
C_CONF_FILES_TMP_DATA_DIR =     'Temporary data directory'
C_CONF_FILES_TMP_LOG =          'Temp log'
C_CONF_FILES_OUT_DIR =          'Output directory'
C_CONF_FILES_USERNAME_MACHINES ='Username machines'
C_CONF_FILES_PREFIX_OUTPUT =    'Prefix for output'


C_CONF_HDFS =                   "HDFS"
C_CONF_HDFS_INPUT_DATA_DIR =    'Input data directory'
C_CONF_HDFS_DIR_SUFFIX =        'Input directory suffix'
C_CONF_HDFS_PACKETS_PER_BLOCK = 'Packets per HDFS block'
C_CONF_HDFS_CHECKSUM_SIZE =     'Checksum size'




C_CONF_PREH =                   "Hadoop-"    # To be used with Hadoop 
                                             # configuration files (e.g. Hadoop-core, Hadoop-hdfs, etc.)
C_CONF_SUF_SLAVES =             "slaves"
C_CONF_SUF_MASTERS =            "masters"
C_CONF_SUF_OTHER =              "other"
C_CONF_SUF_HDFS =               "hdfs"
C_CONF_H_HDFS =                 C_CONF_PREH+C_CONF_SUF_HDFS
C_CONF_H_ALL_CONFIG_FILE =      'Configuration file'  # Hadoop configuration file filename (hdfs-site.xml...)


C_CONF_HSLAVES =                C_CONF_PREH+C_CONF_SUF_SLAVES
C_CONF_HSLAVES_MAX_SLAVES =     'Max number of slaves'


C_CONF_HMASTERS =               C_CONF_PREH+C_CONF_SUF_MASTERS
C_CONF_HMASTERS_MASTER_IS_SLAVE ='Master is slave'


C_CONF_OTHER =                  C_CONF_PREH+C_CONF_SUF_OTHER
C_CONF_OTHER_START_DELAY =      'Start time delay [s]'
C_CONF_OTHER_STOP_DELAY =       'Stop time delay [s]'
C_CONF_OTHER_COPY_DELAY =       'Copy files delay [s]'
C_CONF_OTHER_TEXT_MODE =        'Text mode'
C_CONF_OTHER_TEXT_DELIMITER =   'Text delimiter'
C_CONF_OTHER_MAX_CPU_VCORES =   'Max cpu vcores'
C_CONF_OTHER_FFT_MAP =          'FFT at mapper'
C_CONF_OTHER_ADJ_MAP =          'Adjust mappers'
C_CONF_OTHER_ADJ_RED =          'Adjust reducers'
C_CONF_OTHER_ONE_BASELINE =     'One baseline per task'
C_CONF_OTHER_SCALING_STATIONS = 'Task scaling stations'
C_CONF_OTHER_TIMEOUT_STOP_NODES ='Timeout stop nodes [s]'
C_CONF_OTHER_SINGLE_PRECISION = 'Single precision'


C_CONF_EXP =                    "Experiment"
C_CONF_EXP_FOLDER =             'Experiment folder'
C_CONF_EXP_STATIONS =           'Stations file'
C_CONF_EXP_SOURCES =            'Sources file'
C_CONF_EXP_EOP =                'EOP file'
C_CONF_EXP_DELAYS =             'Delays file'
C_CONF_EXP_DELAY_MODEL =        'Delay model file'
C_CONF_EXP_MEDIA =              'Media file'
C_CONF_EXP_CORRELATION =        'Correlation file'
C_CONF_EXP_MEDIA_SUB =          'Media sub-folder'
C_CONF_EXP_MEDIA_SUB_PREFIX =   'Output sub-folder prefix'



# Reserved strings
C_CONF_RES_LOCALHOST =          "localhost"
C_CONF_RES_LOCALPATH =          "localpath"
C_CONF_RES_LOCALUSER =          "localuser"

# Relative path to Correlx main folder from location of main script (src/mapred_cx.py)
C_CONF_RELATIVE_PATH_LOCALPATH = "/.."


# ------------ Command line arguments ------------ 
#
# Structure:
#
#  C_ARG_<arg>='argument'
#

# ******************
# (!) The following lines (starting with C_ARG_ and including "display_in_help) are also displayed in the help of the program (!)
# ******************

C_ARG_HEADER_SHOW =             "[Argmument]"        # [Comments]                                 [Type]  [Example]    "display_in_help
C_ARG_PPB =                     "ppb"                # Number of frames per split                 int     5000         "display_in_help
C_ARG_SLOWSTART =               "slowstart"          # Initialize reduce after map completion     float   0.95         "display_in_help
C_ARG_REPLICATION =             "replication"        # Number of copies of input splits in HDFS   int     2            "display_in_help
C_ARG_FFTM =                    "fftm"               # FFT in mapper                              int     0            "display_in_help
C_ARG_FFTR =                    "fftr"               # FFT in reducer                             int     1            "display_in_help
C_ARG_ADJM =                    "adjm"               # Adjust number of mappers                   float   1            "display_in_help
C_ARG_ADJR =                    "adjr"               # Adjust number of reducers                  float   1            "display_in_help
C_ARG_VCORES =                  "vcores"             # Number of cores per node                   int     14           "display_in_help
C_ARG_MAPSPERNODE =             "mapspernode"        # Simultaneous maps                          int     8            "display_in_help
C_ARG_REDUCESPERNODE =          "reducespernode"     # Simultaneous reduces                       int     8            "display_in_help
C_ARG_VCORESPERMAP =            "vcorespermap"       # Number of virtual cores per map task       int     1            "display_in_help
C_ARG_VCORESPERRED =            "vcoresperred"       # Number of virtual cores per reduce task    int     1            "display_in_help
C_ARG_NODEMEM =                 "nodemem"            # Memory per node [MB]                       int     59000        "display_in_help
C_ARG_CONTAINERMEM_MAP =        "containermemmap"    # Memory per container [MB]                  int     2048         "display_in_help
C_ARG_CONTAINERHEAP_MAP =       "containerheapmap"   # Memory heap per container (map) [MB]       int     1800         "display_in_help
C_ARG_CONTAINERMEM_RED =        "containermemred"    # Memory per container (map) [MB]            int     4096         "display_in_help
C_ARG_CONTAINERHEAP_RED =       "containerheapred"   # Memory heap per container (reducer) [MB]   int     3800         "display_in_help
C_ARG_CONTAINERMEM_AM =         "containermemam"     # Memory per container (app manager) [MB]    int     2048         "display_in_help
C_ARG_CONTAINERHEAP_AM =        "containerheapam"    # Memory heap per container (app mgr) [MB]   int     1800         "display_in_help
C_ARG_SORTMEM =                 "sortmem"            # Sort memory for shuffle [MB]               int     800          "display_in_help
C_ARG_BLOCKSIZE =               "blocksize"          # Block size in distributed filesystem [MB]  int     1640000000   "display_in_help
C_ARG_MASTERWORKS =             "masterworks"        # Master also doing computations             int     1            "display_in_help
C_ARG_TASKSPERJVM =             "tasksperjvm"        # Tasks per JVM before reinit                int     -1           "display_in_help
C_ARG_AVOIDCOPY =               "avoidcopy"          # Avoid copying input files if existing      int     1            "display_in_help
C_ARG_DELETEOUTPUT =            "deleteoutput"       # Delete output file (benchmarking only)     int     0            "display_in_help
C_ARG_SCALEST =                 "scalest"            # Linear scaling stations                    (N/A)                "display_in_help
C_ARG_MEDIASUFFIX =             "mediasuffix"        # Suffix for media folder                    str     _16st        "display_in_help
C_ARG_SINGLEPRECISION =         "singleprecision"    # Single precision in computations           int     0            "display_in_help
C_ARG_EXPER =                   "exper"              # Experiment folder                          str     ./ini_vgos_4st "display_in_help
C_ARG_OUT =                     "out"                # Output folder                              str     ./cx_out     "display_in_help
C_ARG_APP =                     "app"                # Application sources folder                 str     ./correlx/src "display_in_help
C_ARG_SERIAL =                  "serial"             # Run pipeline mode                          int     0            "display_in_help
C_ARG_PARALLEL =                "parallel"           # Run Hadoop                                 int     1            "display_in_help
C_ARG_NM_LOC_PORT =             "nmlocport"          # Nodemanager localizer port                 int     20000        "display_in_help
C_ARG_NM_WEB_PORT =             "nmwebport"          # Nodemanager web port                       int     20001        "display_in_help
C_ARG_SHUFFLE_PORT =            "shuffleport"        # Nodemanager shuffle port                   int     20002        "display_in_help


#C_ARG_DATA =                    "data"

#
# ------------ Other conventions ------------ 
#
#  Use localhost in configuration file to be replaced during execution by master name [see lib_config.get_config_mod_for_this_master()].

