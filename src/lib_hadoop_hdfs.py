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
#File: lib_hadoop_hdfs.py.
#Author: A.J. Vazquez Alvarez (ajvazquez@haystack.mit.edu)
#Description: 
"""
Functions for starting and stopping hadoop, and for sending files to HDFS (or copying files to Lustre).

"""
#History:
#initial version: 2015.12 ajva
#MIT Haystack Observatory

from __future__ import print_function
import os
import time
import sys
import imp
from lib_vdif import get_vdif_stats
import numpy as np

import const_hadoop
imp.reload(const_hadoop)
from const_hadoop import *




##################################################################
#
#                   Hadoop cluster start/stop
#
##################################################################




def cluster_start(wait_time,hadoop_conf_dir,hadoop_dir,file_slaves,temp_dir,username,temp_log,single_node=0,use_lustre_plugin=0,v=0,\
             file_log=sys.stdout):

    """
    Start hadoop. It returns the number of active nodes after initialization.
    
    Parameters
    ----------
     wait_time : int
         time [s] to wait after each command sent to Hadoop.
     hadoop_conf_dir : str
         Hadoop configuration folder path [hadoop_home/etc/hadoop].
     hadoop_dir : str
         Hadoop home folder path.
     file_slaves : str
         Path to Hadoop slaves file.
     temp_dir : str
         Path to temporary folder path
     username : str
         user name (to be used to login into slave nodes through ssh).
     temp_log : str
         Path to temporary log file.
     single_node : int
         If 1 will only delete temporary folders on current machine (master), if 0 on all (master and slaves)
     use_lustre_plugin : int
         1 for Lustre filesystem, 0 for HDFS.
     v : int
         1 for verbose.
     file_log : str
         Path to log file.
    
    Returns
    -------
     int_t_nodes : int
         (t_nodes) number of nodes up and running in the Hadoop cluster.
    
    Notes
    -----
    |
    | **Summary:**
    |
    |  1. Delete temporary files (otherwise there may be problems with previous process IDs...).
    |  2. Format HDFS (in the future the hadoop service may be running continuously to avoid initialization for every correlation...).
    |  3. Start HDFS.
    |  4. Start YARN.
    |  5. Check running processes.
    """

    #forced_temp_folder = "/tmp"
    #temp_files =  forced_temp_folder + "/*" + username + "*"
    forced_temp_folder = temp_dir
    temp_files =  forced_temp_folder + "/*"
    
    command_delete_basic = "rm -Rf --verbose " + temp_files + " > " + temp_log
    #command_delete_owned = "find /tmp/ -user " + username + " -exec rm -fr {} \;"
        
    if v==1:
        print("\nStarting Hadoop...",file=file_log)   
        print("\n (!) Deleting temporary files (forced dir: " + forced_temp_folder + " for user " + username + ") ",file=file_log)   
        print("  Single node: ".ljust(10),end="",file=file_log)
        if single_node==1:
            print("yes",file=file_log)
        else:
            print("no",file=file_log)
  

    # Delete temporary files and reformat HDFS
    if v==1:
        print(" Deleting temporary files (on all nodes) and reformatting HDFS...",file=file_log)
    if single_node:
        os.system(command_delete_basic)
        #os.system(command_delete_owned)
    else:
        os.system("pdsh -d -R ssh -l " + username + " -w `cat " + \
                hadoop_conf_dir + file_slaves + "|tr '\n' ','|rev|cut -c2-|rev` " + 
                command_delete_basic)
        #os.system("pdsh -d -R ssh -l " + username + " -w ^" + hadoop_conf_dir + file_slaves + " " + command_delete_owned)

        
    # Count number of removed files.
    count_nodes=0
    filtered_list=[]
    list_nodes=[]
    with open(hadoop_conf_dir + file_slaves, 'r') as f_nodes:
        for line in f_nodes:
            list_nodes.append(line.strip())
            count_nodes+=1
    with open(temp_log, 'r') as f_tmp:
        for line in f_tmp:
            [ip_s,file_s]=line.split("removed")
            filtered_list+=[ip_s]
        reduced_list = set(filtered_list)
        if v==1:
            for i in reduced_list:
                print("  " + i.ljust(18) + " removed files: ".ljust(32) + str(filtered_list.count(i)),file=file_log)
    
    
    # Format HDFS
    if v==1:
        print(" Reformatting HDFS...",file=file_log)
    os.system(hadoop_dir + "bin/hdfs --config "+hadoop_conf_dir+" namenode -format -force")
    #os.system(hadoop_dir + "bin/hdfs dfsadmin -safemode leave")

    # Wait for initialization
    if v==1:
        print(" Waiting for Hadoop initialization (1) ("+str(wait_time)+" s)...",file=file_log)   
        file_log.flush()
    if wait_time>0:
        time.sleep(wait_time)
    
    # Start HDFS
    if v==1:
        print(" Starting HDFS...",file=file_log)
    
    if use_lustre_plugin==0:
        os.system(hadoop_dir + "sbin/start-dfs.sh --config "+hadoop_conf_dir) # + new_inter)
    
    # Wait for initialization
    if v==1:
        print(" Waiting for Hadoop initialization (2) ("+str(wait_time)+" s)...",file=file_log)   
        file_log.flush()
    if wait_time>0:
        time.sleep(wait_time)

    # Start YARN
    if v==1:
        print(" Starting YARN...",file=file_log)
    os.system(hadoop_dir + "sbin/start-yarn.sh --config "+hadoop_conf_dir) # + new_inter)

    # Check open ports
    #os.system("netstat -nlp|grep java" + plus_out)
    #show_file(file_out)
    
    
    # Wait for initialization
    if v==1:
        print(" Waiting for Hadoop initialization (3) ("+str(wait_time)+" s)...",file=file_log)   
        file_log.flush()
    if wait_time>0:
        time.sleep(wait_time)

    
    # Start History server
    if v==1:
        print(" Starting History server...",file=file_log)
    os.system("mkdir -p " + temp_dir + "/"+HADOOP_DIR_DONE_INTER)
    os.system("mkdir -p " + temp_dir + "/"+HADOOP_DIR_DONE)
    if use_lustre_plugin==0:
        os.system(hadoop_dir + "sbin/mr-jobhistory-daemon.sh --config "+hadoop_conf_dir+" start historyserver") # + new_inter)
    else:
        os.system(hadoop_dir + "sbin/mr-jobhistory-daemon.sh --config "+hadoop_conf_dir +\
             " start historyserver -D "+C_H_INLINE_LUSTRE_FS_ABS_PARAM+"="+C_H_INLINE_LUSTRE_FS_ABS_VAL) # + new_inter)

    if v==1:
        print(" Waiting for Hadoop initialization (and 4) ("+str(wait_time)+" s)...",file=file_log)
        file_log.flush()
    if wait_time>0:
        time.sleep(wait_time)
    
    # Check running processes  
    if v==1:
        os.system("jps" + " > " + temp_log)
        print(" Launched processes:",file=file_log)   
        with open(temp_log, 'r') as f_log:
             for line in f_log:
                 print("  "+line.strip(),file=file_log)
    if v==1:
        print(" List of nodes:",file=file_log)   
    os.system(hadoop_dir + "bin/yarn --config "+ hadoop_conf_dir +" node -list > " + temp_log)#temp_dir + ) # + plus_out)
    
    t_nodes=0
    
    for iter_refresh in range(1):
        with open(temp_log, 'r') as f_tmp:
            for line in f_tmp:
                
                if "RUNNING" in line:
                    for i in list_nodes:
                        if i in line:
                            list_nodes.remove(i)
                
                if v==1:
                    print("   "+line.strip(),end="\n",file=file_log)
                if "Nodes:" in line:
                    [stotal,t_nodes]=line.split(":")
            
        if (iter_refresh==0)and(int(t_nodes)>count_nodes):
            if v==1:
                print("    Too many nodes initiated!",file=file_log)
    if v==1:
        print("Nodes off: "+str(list_nodes),file=file_log)
                #os.system(hadoop_dir + "bin/hadoop dfsadmin -refreshNodes")
    return(int(t_nodes)) #number of nodes active




def cluster_stop(wait_time,hadoop_dir,hadoop_conf_dir,temp_log,timeout=-1,v=0,file_log=sys.stdout):
    """
    Stop hadoop. 
    
    Parameters
    ----------
     wait_time : int
         Time [s] to wait after each command sent to Hadoop.
     hadoop_dir : str
         Hadoop home folder path.
     hadoop_conf_dir : str
         Hadoop configuration folder path [hadoop_home/etc/hadoop].
     temp_log : str
         Path to temporary log file.
     timeout : int
         If >0 will terminate Hadoop stop command after "timeout" seconds.
     v : int
         1 for verbose.
     file_log : str
         Path to log file.
     
    Returns
    -------
     N/A
    
    Notes
    -----
    |
    | **Summary:**
    |
    |  -Stop YARN.
    |  -Stop DFS.
    |  -Wait "wait_time" seconds for termination.
    """
    
    use_lustre_plugin = 0
        
    if v==1:
        print("\nStopping Hadoop...",file=file_log)   

    
     # Stop YARN
    if v==1:
        print(" Stopping YARN...",file=file_log)   
    if timeout<0:
        os.system(hadoop_dir + "sbin/stop-yarn.sh --config "+hadoop_conf_dir) # + new_inter)
    else:
        os.system("timeout "+str(timeout)+" "+hadoop_dir + "sbin/stop-yarn.sh --config "+hadoop_conf_dir) # + new_inter)


    # Stop DFS
    if v==1:
        print(" Stopping DFS...",file=file_log)   
        #print(" Stopping ALL...",file=file_log)   
    
    if use_lustre_plugin==0:
        os.system(hadoop_dir + "sbin/stop-dfs.sh --config "+hadoop_conf_dir) # + new_inter)
    #os.system(hadoop_dir + "sbin/stop-all.sh")
    
    
    # Stop history server
    os.system(hadoop_dir + "sbin/mr-jobhistory-daemon.sh --config "+hadoop_conf_dir+" stop historyserver")
    
    # Wait for termination
    if v==1:
        print(" Waiting for Hadoop termination ("+str(wait_time)+" s)...",file=file_log)   
        #file_log.flush()
    if wait_time>0:    
        time.sleep(wait_time)
        
        
        
        
        
        

##################################################################
#
#          Move media files to distributed filesystem
#
##################################################################



def copy_files_to_hdfs(replication,input_files,data_dir,data_dir_tmp,hadoop_dir,hadoop_conf_dir,hdfs_data_dir,\
                       packets_per_hdfs_block,temp_log,copy_delay=0,checksum_size=100,text_mode=1,\
                       use_lustre_plugin=0,lustre_prefix="/nobackup1/ajva/hadoop",bm_avoid_copy=0,v=0,file_log=sys.stdout):        
    """
    Copy files from local directories to HDFS. It returns the elapsed time for moving the files (including the applied delay).
    
    Parameters
    ----------
     replication : int
         Number of copies of the same file block in the HDFS system.
     input_files : list of str
         names of the files to be moved into HDFS/LustreFS.
     data_dir : str
         Path (in local filesystem) to the folder hosting the files to be moved into HDFS/LustreFS.
     data_dir_tmp : str
         Path (in local filesystem) to the folder for copy split input data before being moved into HDFS/LustreFS.
     hadoop_dir : str
         Hadoop home folder path (local filesystem).
     hadoop_conf_dir : str
         Hadoop configuration folder path (etc/hadoop).
     hdfs_data_dir : str
         Path (in HDFS/LustreFS), thus relative to "lustre_prefix", to host input files.
     packets_per_hdfs_block : int
         Number of VDIF frames per file split.
     temp_log : str
         Path to temporary log file.
     copy_delay : int
         Time [s] to wait after each command sent to Hadoop.
     checksum_size : int
         Number of bytes for checksum (for each split) [this overrides automatic calculation].
     text_mode : int
         [default 1] If 1 use checksum_size to override value computed automatically.
     use_lustre_plugin : int
         If 1 use Lustre filesysm.
     lustre_prefix : str
         Path in Lustre to preceed "hdfs_data_dir" if using Lustre.
     bm_avoid_copy : int
        [default 0] If 1 it will not split input files if "lustre_prefix"+"hdfs_data_dir" has already the data
                        for the file in "input_files" from a previous execution. See notes below.
     v : int
         1 for verbose.
     file_log : str
         Path to log file.
    
    Returns
    -------
     put_t_s : float
         timestamp with start time for loop copying files.
     put_t_e : float
         timestamp with stop time for loop copying files.
     put_d : float
         total execution time for loop copying files.
    
    Notes
    -----
    |
    | **Summary:**
    |
    |  -Delete existing files in HDFS (to avoid errors on existing files)
    |  TODO: consider overwritting files.
    |  -Wait for delay if applicable.
    |  -Move files to HDFS (with specified block size)
    |  2015.12.1. packet_size is read from the first frame of the file.
    |
    |
    | **Notes:**
    |
    |  -Regarding filesystems:
    |    HDFS: Path inside Hadoop distributed filesystem (accessible from Hadoop).
    |    LustreFS: Path relative to Hadoop Lustre home folder (accessible from Hadoop).
    |    Local filesystem: Path accesible from this command, it can be local, NFS, Lustre, etc.
    |  
    |  -Regarding "bm_avoid_copy":
    |    Always 0 by default.
    |    After each execution, for each processed file there will be a folder in "lustre_prefix"+"hdfs_data_dir"+"file_name"+... with
    |      the splits for that file. Setting this to 1 will avoid to re-split the file if it was already used previously. Use only 
    |      for repeated benchmarking.
    """
    
    # Split file manually into the size of the hdfs blocks
    # Use 1 if processing input as text
    #split_file_sequentially=0
    split_file_sequentially=text_mode
    
    
    

    if v==1:
        print("\nCopying data files to HDFS...",file=file_log)   
        print(data_dir)
        print(input_files)
    
    safe_status = "OFF"
    if use_lustre_plugin==0:
        os.system(hadoop_dir + "bin/hdfs --config "+hadoop_conf_dir+" dfsadmin -report|grep Safe > " + temp_log)
        with open(temp_log, 'r') as f_tmp:
            for line in f_tmp:
                safe_status = line.strip()


    if safe_status == "OFF":
        if v==1:
            #print("  Safe status:     " + safe_status,file=file_log)
            print("  Checking HDFS: OK",file=file_log)
    else:
        #os.system(hadoop_dir+"bin/hdfs dfsadmin -safemode leave")
        if v==1:
            #print(" Forcing out of safe mode...",file=file_log)  
            print(" ERROR!: Namenode is in safe mode!...",file=file_log)  
            # TODO: Propagate the error...

    dest_dir = hdfs_data_dir
    
    


    # Delete all existing files in HDFS and create directory.
    if use_lustre_plugin==0:
        os.system(hadoop_dir+"bin/hdfs --config "+hadoop_conf_dir+" dfs -rm -r -f "+ dest_dir)
        os.system(hadoop_dir+"bin/hdfs --config "+hadoop_conf_dir+" dfs -mkdir " + dest_dir)
    else:
        if bm_avoid_copy==0:
            os.system("rm -r -f "+lustre_prefix+ dest_dir)
        os.system("mkdir " + lustre_prefix + dest_dir)
    
    #if v==1:
    #    print(" Forcing out of safe mode...",file=file_log)  
    #os.system(hadoop_dir+"bin/hdfs dfsadmin -safemode leave")
    
    
    if copy_delay>0:
        if v==1:
            print(" Waiting for HDFS interaction (" + str(copy_delay) + " s)...",file=file_log) 
        time.sleep(copy_delay)
    
    
    if v==1:
        print(" Will wait for " + str(len(input_files)) + " HDFS interaction(s) (" + str(copy_delay) + " s)...",file=file_log) 
    
    
    
    
    if split_file_sequentially==0:
        put_t_s = time.time()
        for filename in input_files:
            vdif_stats=get_vdif_stats(data_dir+filename,packet_limit=1,offset_bytes=0,only_offset_once=0,v=0)
            packet_size=vdif_stats[4]
        
            #blocksize = min(packet_size*packets_per_hdfs_block,os.path.getsize(data_dir+filename))
            blocksize = packet_size*packets_per_hdfs_block
            
            command_hdfs=hadoop_dir + "bin/hdfs --config "+hadoop_conf_dir+" dfs"+\
                      " -D "+C_H_HDFS_CHECKSUM+    "="+str(checksum_size)+\
                      " -D "+C_H_HDFS_BLOCKSIZE+   "="+str(blocksize)+\
                      " -D "+C_H_HDFS_REPLICATION+ "="+str(replication)+\
                      " -put -f " + data_dir + filename + " " + dest_dir
            os.system(command_hdfs)  
            if copy_delay>0:
                time.sleep(copy_delay)
        put_t_e = time.time()
    else:
        put_t_s = time.time()
        for filename in input_files:
            vdif_stats=get_vdif_stats(data_dir+filename,packet_limit=1,offset_bytes=0,only_offset_once=0,v=0)
            packet_size=vdif_stats[4]
            checksum_size=packets_per_hdfs_block
    

            blocksize = packet_size*packets_per_hdfs_block
            suffix_size=int(1+np.ceil(np.log10(1+os.path.getsize(data_dir+filename)//blocksize)))     
            command_hdfs="No command executed"
            send_tmp_folder = data_dir_tmp+filename+"_tmp_folder"

            test_file_exists = lustre_prefix + dest_dir+"/0"+"/"+filename
            if bm_avoid_copy:
                if os.path.isfile(test_file_exists):
                    if v==1:
                        print("(!!) Avoiding copy of file "+filename+", already found in "+lustre_prefix + dest_dir,file=file_log)
                    continue
                
            os.system("rm -r "+send_tmp_folder)
            os.system("mkdir -p "+send_tmp_folder)
            os.system("split --bytes="+str(blocksize)+" -d -a " + str(suffix_size) + " " +data_dir+filename+" "+send_tmp_folder+"/tmp_")
            
            files_to_process = os.listdir(send_tmp_folder)
            num_iters=len(files_to_process)
            
            if v==1:
                print(" Sending "+str(num_iters)+" block(s):",file=file_log) 
                print(" "+','.join(files_to_process),file=file_log) 
            

            iteri=-1
            
            check_value=0
            for fi in files_to_process:
                iteri+=1
                
                if v==1:
                    if (iteri/num_iters)>(check_value/100):
                        print(str(check_value)+"%",file=file_log) 
                        check_value+=10
                

                if use_lustre_plugin==0:
                    os.system(hadoop_dir + "bin/hdfs --config "+hadoop_conf_dir+" dfs -mkdir -p " + dest_dir+"/"+str(iteri)+"/")
                    os.system(hadoop_dir + "bin/hdfs --config "+hadoop_conf_dir+" dfs -ls " + dest_dir + " >> " + temp_log)
                else:
                    os.system("mkdir "+lustre_prefix+dest_dir+"/"+str(iteri)+"/"+" &> /dev/null")
                    
                
                if use_lustre_plugin==0:
                    command_hdfs=hadoop_dir + "bin/hdfs --config "+hadoop_conf_dir+" dfs"+\
                         " -D "+C_H_HDFS_CHECKSUM+    "="+str(checksum_size)+\
                         " -D "+C_H_HDFS_BLOCKSIZE+   "="+str(blocksize)+\
                         " -D "+C_H_HDFS_REPLICATION+ "="+str(replication)+\
                         " -put -f " + send_tmp_folder + "/" + fi + " " + dest_dir+str(iteri)+"/"+filename
                    os.system(command_hdfs)
                else:
                    command_hdfs="mv "+ send_tmp_folder + "/" + fi + " "+ lustre_prefix + dest_dir+str(iteri)+"/"+filename
                    os.system(command_hdfs)
                    
                    
                
            if copy_delay>0:
                time.sleep(copy_delay)
            os.system("rm -r "+send_tmp_folder)
            if v==1:
                print("100%, deleting temp files",file=file_log) 
        put_t_e = time.time()
    put_d = put_t_e - put_t_s
    
    if use_lustre_plugin==0:
        os.system(hadoop_dir + "bin/hdfs --config "+hadoop_conf_dir+" dfs -ls " + dest_dir + " >> " + temp_log)
    if copy_delay>0:
        if v==1:
            print(" Last command: "+command_hdfs)
            print(" Waiting for HDFS interaction (" + str(copy_delay) + " s)...",file=file_log) 
        time.sleep(copy_delay)
    os.system("cd " + hadoop_dir)
    
    if v==1:
        if use_lustre_plugin==0:
            os.system(hadoop_dir + "bin/hdfs --config "+hadoop_conf_dir+" fsck " + dest_dir + " -files -blocks >> " + temp_log)
        if copy_delay>0:
            if v==1:
                print(" Waiting for HDFS interaction (" + str(copy_delay) + " s)...",file=file_log)   
            time.sleep(copy_delay)
        with open(temp_log, 'r') as f_tmp:
            for line in f_tmp:
                print("  "+line.strip(),file=file_log)
    
    
    return([put_t_s,put_t_e,put_d])




##################################################################
#
#              Hadoop master and slaves files
#
##################################################################



def nodes_to_slaves_masters(conf_dir,hadoop_conf_dir,file_nodes,file_slaves,file_masters,max_slaves=0,master_is_slave=0,v=0,file_log=sys.stdout):
    """
    Convert list of nodes (obtained by slurm or by local script) into hadoop slaves file.
    
    Parameters
    ----------
     conf_dir
         path to folder where master and slaves files are copied to.
     hadoop_conf_dir
         path to folder where masters and slaves files are created.
     file_nodes
         filename for file with all nodes in the allocation (master+slaves), one node per line.
     file_slaves
         filename for Hadoop slave nodes file.
     file_masters
         filename for Hadoop master node file.
     max_slaves
         maximum number of slave nodes.
     master_is_slave
         if 1 include master node in list of slave nodes.
     v
         verbose if 1.
     file_log
         handler for log file.

    Returns
    -------
     N/A

    Notes
    -----
    |
    | **TO DO:**
    |
    |  Remove hadoop_conf_dir (check correct folder).
    """
    if max_slaves<1:
        max_slaves=1

    if v==1:
        print("\nCreating slaves files...",file=file_log)   
        print(" Slaves:",file=file_log)

        i=0
        with open(conf_dir + file_nodes, 'r') as f_nodes:
            with open(hadoop_conf_dir + file_slaves, 'w') as f_slaves:
                with open(hadoop_conf_dir + file_masters, 'w') as f_masters:
                    if 0==1: #max_slaves==1:
                        print("localhost",file=f_slaves)
                        print("localhost",file=f_slaves)
                        if v==1:
                            print("  Forcing slaves files to show only localhost",file=file_log)
                    else:
                        for line in f_nodes:
                            i+=1
                            if i==1:
                                print(line.strip(),file=f_masters)
                                if master_is_slave==1:
                                    print(line.strip(),file=f_slaves)
                            elif i<=max_slaves:
                                print(line.strip(),file=f_slaves)
                                if v==1:
                                    print("  "+line.strip().ljust(20),file=file_log)

        
        os.system("cp " + hadoop_conf_dir + file_slaves + " " + conf_dir + file_slaves)
        os.system("cp " + hadoop_conf_dir + file_masters + " " + conf_dir + file_masters)




##################################################################
#
#       Hadoop configuration files (Update configuration)
#
##################################################################





def update_hcparam(source_file,destination_file,pairs,v=0,file_log=sys.stdout):
    """
    Update parameter for hadoop configuration file.
    
    Parameters
    ----------
     source_file
         Template for Hadoop xml configuration file.
     destination_file
         Final Hadoop xml configuration file (template with mods applied).
     pairs
         list of pairs [parameter, value]. See notes below.
     v
         verbose if 1.
     file_log
         handler for log file.
    
    Returns
    -------
     N/A
    
    Notes
    ------
    | If the parameter already exists in the file, its value is overriden.
    | If the parameter does not exist, it is added following the required format (Hadoop xml).
    | That is, given a list of pairs [PARAMETER,VALUE]
    | 
    | <configuration>
    |     <property>
    |             <name>PARAMETER</name>
    |             <value>VALUE</value>
    |     </property>
    | ...
    | </configuration>
    """

    isamec = ""
    copy_again=0
    input_destination_file=destination_file
    if source_file==destination_file:
        copy_again=1
        destination_file+=".tmp"
        isamec = " mv " + destination_file + " " + source_file
        

    command = ""
    iteration=0
    updated_params=[]
    for pair in pairs:
        iteration+=1
        if iteration==1:
            command+="sed -e '/" + pair[0] + "/I!b;n;c\\\\t\\t<value>" + pair[1] + "</value>' "+source_file
        if iteration>0:
            command+="|sed -e '/" + pair[0] + "/I!b;n;c\\\\t\\t<value>" + pair[1] + "</value>' "
    command+= "> " + destination_file 
    os.system(command)
    if copy_again==1:
        os.system(isamec)

    if v==1:
        for pair in pairs: #[1:]:
            print("  " + pair[0].ljust(49) + " " + pair[1],file=file_log) 
            
    # Now check if any parameter was not found, and thus was not updated
    # Checks lowercase!
    add_params=[]
    with open(source_file, 'r') as f:
        lines = f.read()
        for pair in pairs:
            count=1
            if pair[0].lower() in lines.lower():
                updated_params+=[pair[0]]
                count=0
            if count==1:
                if v==1:
                    print("  (!) Parameter not found: " + pair[0].ljust(49),file=file_log) 
                    if pair[0] not in updated_params:
                        add_params+=[[pair[0],pair[1]]]
     

        
    if add_params!=[] and ".xml" in source_file:
        with open(input_destination_file, 'r') as f:
            lines_content = f.readlines()
        with open(input_destination_file, 'w') as f:
            for line in lines_content[:-1]:
                #if "</configuration>" not in line:
                    print(line,end="",file=f) 
                #else:
            for pair in add_params:
                print("        <property>",file=f) 
                print("                <name>"+pair[0]+"</name>",file=f)
                print("                <value>"+pair[1]+"</value>",file=f) 
                print("        </property>",file=f) 
                     
                if v==1:
                    print("  (!) Added parameter: " + pair[0].ljust(49)+ " = "+pair[1],file=file_log) 
                    
            print("</configuration>",file=f) 
    

def process_hcfile(pairs,conf_dir,templates_dir,v=0,file_log=sys.stdout):
    """
    Process hadoop configuration file.
    
    Parameters
    ----------
     pairs
         list (by file) of lists (param,value) for overriding Hadoop configuration files.
     conf_dir
         path to folder for placing modified Hadoop configuration files.
     templates_dir
         path to folder with templates for Hadoop configuration files.
     v
         verbose if 1.
     file_log
         handler for log file.
    
    Returns
    -------
     processed_filename
         filename of the processed file.
    
    Notes
    -----
    |
    | **TO DO**
    |
    | Currently assuming that the filename is the first value in the list (i.e. [0][1]),
    |   need to use C_CONF_H_ALL_CONFIG_FILE instead
    """
    
    if v==1:
        print(pairs[0][1])
        print(" Processing " + pairs[0][1] + "...",file=file_log)   

    source_file = templates_dir+pairs[0][1]
    destination_file = conf_dir+pairs[0][1]
    

    update_hcparam(source_file=source_file,destination_file=destination_file,pairs=pairs[1:],v=v,file_log=file_log) 
    processed_filename = pairs[0][1]

    return(processed_filename)




def process_hadoop_config_files(list_configurations,pairs_config,templates_dir,conf_dir,v=0,file_log=sys.stdout):
    """
    Process a set of Hadoop configuration files (core-site.xml, etc).
    
    Parameters
    ----------
     list_configurations
         list of headers in configuration file associated to "pairs_config" below.
     pairs_config
         list of lists of pairs [[(param0,value0),(param1,value1),...]] to update xml files.
     templates_dir
         path to folder with Hadoop .xml file templates.
     v
         verbose if 1.
     file_log
         handler for log file.
     
    Returns
    -------
     configuration_files
         list of str with filenames of processed configuration files.
    """

    if v==1:
        print("\nProcessing Hadoop configuration files...",file=file_log)  
        print(" Reading from: " + templates_dir,file=file_log)  
        print(" Writing to: " + conf_dir,file=file_log) 
    
    configuration_files=[]

    for (configuration,pair_config) in zip(list_configurations,pairs_config):
        processed_file = process_hcfile(v=v,file_log=file_log,pairs=pair_config,conf_dir=conf_dir,templates_dir=templates_dir)
        configuration_files+=[processed_file]
    

    return(configuration_files)








##################################################################
#
#    Hadoop application and configuration files distribution
#
##################################################################


def fix_file_header(filename="x.py"):

    """ 
    Remove the first line which the iPython notebook adds to the .py files, since
             files that do not begin with #!/usr/bin/env python may raise errors in hadoop.

    Parameters
    ----------
     filename
         path to python script.
            
    Returns
    -------
     N/A
    """

    string_test="python"
    string_result="\#\!\/usr\/bin\/python"
    command="sed -i -e 's/^.*" + string_test + "/" + string_result + "/' " + filename
    os.system(command)
    command="sed -i -n '/python/,$p' " + filename
    os.system(command)



def distribute_files(simply_copy_local,file_group,files,source_dir,conf_dir,destination_dir,\
            nodes,temp_log,v=0,exec_permission=0,file_log=sys.stdout,username="hduser",force_node=""):
    """
    Copy configuration and application files to nodes.
    
    Parameters
    ----------
     simply_copy_local : int
         | If 1 it will only copy app and config files to the specified folder for the current machine.
         | If 0, it will distribute the app and config files to the rest of the nodes via ssh.
     file_group : str
         identifier for this batch of files, only for reporting.
     source_dir
         path to folder with the files that will be distributed.
     files
         list of files (relative to the "source_dir" folder").
     destination_dir
         path to destination folder in the remote machines (thise in "nodes").
     conf_dir
         path to folder that includes the file "nodes".
     nodes
         filename (relative to "conf_dir") with one machine per line.
     temp_log
         handler for intermediate file (buffer) for system calls.
     v
         verbose if 1.
     exec_permission
         if 1 it will give execution permissions to the distributed files.
     file_log
         handler for log file.
     username : str
         user name (to be used to login into slave nodes through ssh).
     force_node : str
         if not "", it will only send the files to this node ("force_node").
    
    Returns
    -------
     0
    
    Notes
    -----
    |
    | **TO DO:**
    |
    |  (!) It will delete ipython-notebook lines. Need to add this as an option, or add another configuration option for third-party sources.
    """

    #show_errors=""
    show_errors=" 2>> " + temp_log
    
    count_nodes=0
    if v==1:
        print("\nCopying " + file_group + " files to nodes...",file=file_log)   
        print(" Nodes:",end="",file=file_log)
        with open(conf_dir + nodes, 'r') as f_nodes:
            for line in f_nodes:
                count_nodes+=1
                print("\t\t\t"+line.strip(),file=file_log)
        print(" Username: \t\t" + username,file=file_log)
        print(" " + file_group + " source dir: \t"+source_dir,file=file_log)
        print(" " + file_group + " files: \t\t"+" ".join(files),file=file_log)  
        print(" " + file_group + " destination dir: \t"+destination_dir,file=file_log)


    fixed_headers = 0
    for f in files:
        
        #Remove ipython-notebook lines from source files (.py)
        filename=(source_dir + f)
        #Only for third party dependencies, need to add this intoa new line in config file.
        #TO DO: Quick fix for third party library (hardcoded). Add option for third party dependencies.
        #if (filename[-3:]==".py")and("six" not in filename):
        if filename[-3:]==".py":
            fixed_headers+=1
            fix_file_header(filename)
        
        if simply_copy_local:
            os.system("mkdir -p " + destination_dir)
            os.system("cp " + source_dir + f + " " + destination_dir + show_errors)
        else:
            #os.system("pdsh -d -R ssh -l " + username + " -w ^" + conf_dir + nodes + " mkdir -p " + destination_dir + " 2>> " + temp_log)
            #os.system("pdcp -d -R ssh -l " + username + " -w ^" + conf_dir + nodes + " " + source_dir + f + " " + destination_dir + " 2> " + temp_log)
            if force_node=="":
                os.system("pdsh -d -R ssh -l " + username + " -w `cat" + conf_dir + nodes + \
                          "|tr '\n' ','|rev|cut -c2-|rev` mkdir -p " + destination_dir + show_errors)
                os.system("pdcp -d -R ssh -l " + username + " -w `cat" + conf_dir + nodes + \
                          "|tr '\n' ','|rev|cut -c2-|rev` " + source_dir + f + " " + destination_dir + show_errors)
            else:
                os.system("pdsh -d -R ssh -l " + username + " -w " + force_node + " mkdir -p " + destination_dir + show_errors)
                os.system("pdcp -d -R ssh -l " + username + " -w " + force_node + " " + source_dir + f + " " + destination_dir + show_errors)
        
        if exec_permission==1:
            if simply_copy_local:
                os.system("chmod a+x " + destination_dir + f + show_errors)
            else:
                os.system("pdsh -d -R ssh -l " + username + " -w ^" + conf_dir + nodes + " chmod a+x " + destination_dir + f + show_errors)
        if v==1:
            with open(temp_log, 'r') as f_tmp:
                lines = set(f_tmp.read())
                for line in lines:
                    if "Failures" in line:
                        print("  "+f.ljust(24)+ " -> " ,end="",file=file_log)
                        print(" "+line.strip(),end="",file=file_log)
                        print(" for "+str(count_nodes)+" nodes.",end="\n",file=file_log)

    if v==1:
        print(" Fixed .py files: \t" + str(fixed_headers),file=file_log)
        print(" Execution permission:\t", end="",file=file_log)   
        if exec_permission:
            print("+x",file=file_log)   
        else:
            print("N/A",file=file_log)   

    return(0)

# <codecell>


