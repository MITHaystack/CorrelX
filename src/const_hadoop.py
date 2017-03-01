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
#File: const_hadoop.py.
#Author: A.J. Vazquez Alvarez (ajvazquez@haystack.mit.edu)
#Description:
"""
 Constants for Hadoop configuration files.
 Current configuration for Hadoop 2.7.3. In case of update, replace the parameters herein as applicable.

TO DO
-----
 Add relative folders /etc/hadoop ....

"""
#History:
#initial version: 2016.10 ajva
#MIT Haystack Observatory



# General (to be used in configuration file, this will be replace by hadoop with localhost)
HOSTNAME_HADOOP =            '${host.name}'

# Relative path to etc/hadoop in Hadoop 2.7.3 (.environment set, templates, etc)
HADOOP_REL_ETC =             "etc/hadoop/"

# Folders in tmp
HADOOP_DIR_DONE_INTER =      "done_intermediate"
HADOOP_DIR_DONE =            "done"


#
# ------------  hdfs-site.xml ------------ 
#

# Replication
C_H_HDFS_REPLICATION =       "dfs.replication"

# Block settings
C_H_HDFS_BLOCKSIZE =         "dfs.blocksize"
C_H_HDFS_CHECKSUM =          "dfs.bytes-per-checksum"



#
# ------------  mapred-site.xml ------------ 
#

# Slowstart
C_H_MAPRED_SLOWSTART =        "mapreduce.job.reduce.slowstart.completedmaps"

# CPU cores per task
C_H_MAPRED_VCORES_MAP =       "mapreduce.map.cpu.vcores"
C_H_MAPRED_VCORES_RED =       "mapreduce.reduce.cpu.vcores"

# Max. tasks
C_H_MAPRED_MAP_MAX =          "mapreduce.tasktracker.map.tasks.maximum"
C_H_MAPRED_RED_MAX =          "mapreduce.tasktracker.reduce.tasks.maximum"

# Java heap per container
C_H_MAPRED_AM_OPTS =          "yarn.app.mapreduce.am.command-opts"
C_H_MAPRED_CHILD_OPTS =       "mapred.child.java.opts"
C_H_MAPRED_MAP_OPTS =         "mapreduce.map.java.opts"
C_H_MAPRED_RED_OPTS =         "mapreduce.reduce.java.opts"

# Tasks per JVM
C_H_MAPRED_JVM_NUMTASKS =     "mapreduce.job.jvm.numtasks"

# Sort
C_H_MAPRED_SORT_MB =          "mapreduce.task.io.sort.mb"

# Shuffle
C_H_MAPRED_SHUFFLE_PORT =     "mapreduce.shuffle.port"


#
# ------------  yarn-site.xml ------------ 
#

# CPU cores
C_H_YARN_MAX_VCORES =         "yarn.scheduler.maximum-allocation-vcores"
C_H_YARN_RES_VCORES =         "yarn.nodemanager.resource.cpu-vcores"

# Memory per node
C_H_YARN_MAX_MEM_MB =         "yarn.scheduler.maximum-allocation-mb"
C_H_YARN_RES_MEM_MB =         "yarn.nodemanager.resource.memory-mb"

# Memory per container
C_H_YARN_AM_RES_MB =          "yarn.app.mapreduce.am.resource.mb"
C_H_YARN_MAP_MB =             "mapreduce.map.memory.mb"
C_H_YARN_RED_MB =             "mapreduce.reduce.memory.mb"

#Nodemanager ports
C_H_YARN_NM_LOCALIZER_ADDRESS = "yarn.nodemanager.localizer.address"
C_H_YARN_NM_WEBAPP_ADDRESS =   "yarn.nodemanager.webapp.address"


#
# ------------ Environment variables ------------ 
#

# Filename for the split currently processed at the mapper
#  If changed, need to change also const_mapred.MAP_INPUT_FILE
C_H_ENV_MAP_INPUT_FILE =      "map_input_file"


#
# ------------ Inline parameters for job submission ------------ 
#

# Hadoop jar  
HADOOP_STREAMING_JAR =           "hadoop-streaming-2.7.3.jar"

# Partitioning
C_H_INLINE_FIELD_SEP =           "stream.map.output.field.separator"
C_H_INLINE_KEY_FIELDS =          "stream.num.map.output.key.fields"
C_H_INLINE_KEY_FIELD_SEP =       "mapreduce.map.output.key.field.separator"
C_H_INLINE_PARTITIONER_OPTS =    "mapreduce.partition.keypartitioner.options"
C_H_INLINE_COMPARATOR_OPTS =     "mapreduce.partition.keycomparator.options"

# NoHash partitioner
C_H_INLINE_NOHASH_PARITIONER =   "org.apache.hadoop.mapred.lib.KeyFieldBasedPartitionerNH"
C_H_INLINE_DEFAULT_PARITIONER =  "org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner"

# Lustre
C_H_INLINE_LUSTRE_FS_ABS_PARAM = "fs.AbstractFileSystem.lustrefs.impl"
C_H_INLINE_LUSTRE_FS_ABS_VAL =   "org.apache.hadoop.fs.local.LustreFs"
C_H_INLINE_LUSTRE_FS_PARAM =     "fs.lustrefs.impl"
C_H_INLINE_LUSTRE_FS_VAL =       "org.apache.hadoop.fs.lustrefs.LustreFileSystem"
C_H_INLINE_SHUFFLE_PARAM =       "mapreduce.job.reduce.shuffle.consumer.plugin.class"
C_H_INLINE_SHUFFLE_VAL =         "org.apache.hadoop.mapreduce.task.reduce.Shuffle"

# Logging
C_H_INLINE_LOG_CONF =            "log4j.configuration"

# Text delimiter
C_H_INLINE_TEXT_DELIMITER =      "textinputformat.record.delimiter"

# Number of mappers and reducers
C_H_INLINE_NUM_MAPS =            "mapreduce.job.maps"
C_H_INLINE_NUM_REDUCES =         "mapreduce.job.reduces"

# Fixed length records
C_H_INLINE_FIXED_LENGTH =        "fixedlengthinputformat.record.length"
C_H_INLINE_FIXED_FORMAT =        "org.apache.hadoop.mapred.FixedLengthInputFormat"


#
# ------------ Other conventions currently in the library files ------------ 
#
#  Format followed in .xml files:                              lib_hadoop_hdfs.update_hcparam()
#
#  Relative paths and scripts calls for Hadoop deployment:     lib_hadoop_hdfs.cluster_start()
#                                                              lib_hadoop_hdfs.cluster_stop()
#                                                              lib_hadoop_hdfs.copy_files_to_hdfs()
#

# <codecell>


