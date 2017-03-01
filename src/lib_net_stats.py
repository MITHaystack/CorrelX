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
#File: lib_net_stats.py.
#Author: A.J. Vazquez Alvarez (ajvazquez@haystack.mit.edu)
#Description: 
"""
Routines for network statistics.

"""
#History:
#initial version: 2016.11 ajva
#MIT Haystack Observatory

from __future__ import print_function,division
import sys





def print_network_stats(nodes_list,stats_params,stats_values=[],stats_ping=[],title_stats="",v=0,file_log=sys.stdout):
    """
    Write output for logging of network statistics.
    
    Parameters
    ----------
     nodes_list
     stats_params
         list of paramters for each node.
     stats_values
         list of lists with values associted to stats_params.
     stats_ping
         lists of lists with ping results among all pairs of nodes.
     title_stats
         str to be used in suffix of the title of the statistics.
     v
         verbose if 1.
     file_log
         handler to log file.
    """
    if v==1:
        print("\nNetwork Stats -- " + title_stats,file=file_log)
        print(" TX/RX counters [bytes]",file=file_log)
        if stats_values==[]:
            print("  No information available.",file=file_log)
        else:
            print("  Node\t"+'\t'.join([i for i in stats_params]),file=file_log)
            for node,stats_line in zip(nodes_list,stats_values):
                print("  " + node + '\t'+'\t'.join([str(i) for i in stats_line]),file=file_log)
            
        print(" Ping times [ms]",file=file_log)
        if stats_ping==[]:
            print("  No information available.",file=file_log)
        else:
            print("  From(r)\\To(c)\t"+'\t'.join([i for i in nodes_list]),file=file_log)
            for node,stats_line in zip(nodes_list,stats_ping):
                print("  " + node + '\t'+'\t'.join([str(i) for i in stats_line]),file=file_log)
        

def compute_txrx_bytes(stats_values_init,stats_values_end,v=0,file_log=sys.stdout):
    """
    Computing total traffic for ogging of network statistics.
    
    Parameters
    ----------
     stats_values_init : list
         start times.
     stats_values_end : list 
         end times.
     v
         verbose if 1 [unused]
     file_log
         handler for log file [unused].
     
    Returns
    -------
     delta_stats
         stats_values_end - stats_values_start
    """
    if stats_values_end!=[]:
        if len(stats_values_end)==len(stats_values_init):
            print(stats_values_end)
            print(stats_values_init)
            delta_stats = list(np.subtract(np.array(stats_values_end),np.array(stats_values_init)))
        else:
            delta_stats =[]
    return(delta_stats)



def print_network_totals(nodes_list,v_str_hadoop,v_stats_param,v_stats_values_s,v_stats_values_e,v_stats_ping,v=0,file_log=sys.stdout):
    """
    Print network stats. 
    
    It calls print_network_stats() all tuples from the input.
    """
    for str_hadoop,params,stats_s,stats_e,ping_stats in zip(v_str_hadoop,v_stats_param,v_stats_values_s,v_stats_values_e,v_stats_ping):
        delta_stats = compute_txrx_bytes(stats_s,stats_e,v=v,file_log=FILE_LOG)
        print_network_stats(nodes_list,params,delta_stats,stats_ping=ping_stats,title_stats=str_hadoop,v=v,file_log=FILE_LOG)
        


def get_network_stats(nodes_list,over_slurm=0,v=0,file_log=sys.stdout):
    """
    Obtain network statistics from nodes.
    
    Parameters
    ----------
     nodes_list : list of str
         nodes names.
     over_slurm : int
         used to select network interface.
     v : int
         verbose if 1.
     file_log : file handler
         handler to log file.
     
    Returns
    -------
     stats_params
         list of paramters for each node.
     stats_values
         list of lists with values associted to stats_params.
     ping_times
         lists of lists with ping results among all pairs of nodes.
     
    Notes
    -----
    |
    | **TO DO:**
    |
    |  Change over_slurm for explicit selection of the interface.
    """
    if v==1:
        print("\nGetting network statistics...",file=file_log) 
     
    stats_params = [ "TX bytes", "RX bytes"]
    stats_values = []
    stats_values_node =[]
    
    if over_slurm==0:
        interface="eth0"
    else:
        interface="ib0"
    
    # TX and RX bytes
    for node in nodes_list:

        #txrx="TX" # txrx="RX"
        print(" "+node + " " + interface)
        command_ssh="ssh "+node+" ifconfig "+interface #+"|grep \""+txrx +"bytes\"|cut -d\"(\" -f1|cut -d: -f2"
        #print(command_ssh)
        with os.popen(command_ssh,'r',10) as f_out:
            # -1 if failed
            value_read="-1"
            for line in f_out:
                for match in stats_params:
                    if match in line:
                        value_read=line.split(match+":",1)[1].split(' ')[0]
                        stats_values_node+=[int(value_read)]
        if stats_values_node!=[]:
            stats_values+=[stats_values_node]
        else:
            stats_values+=[[0,0]]
        stats_values_node = []

    # ping (average for x times) [ms]
    do_ping=5
    ping_times_node=[]
    ping_times=[]
    for node_from in nodes_list:
        for node_to in nodes_list:
            command_ssh="ssh "+node_from+" ping "+node_to + " -c " + str(do_ping)
            with os.popen(command_ssh,'r',10) as f_out:
                for line in f_out:
                    if "rtt " in line:
                        ping_times_node+=[float(line.split('/')[4])]
        ping_times+=[ping_times_node]
        ping_times_node=[]

    return([stats_params,stats_values,ping_times])


def init_net_stats():
    """
    Initializate structures for network statistics.
    """
    v_str_hadoop = []
    v_stats_param = []
    v_stats_values_s = []
    v_stats_values_e = []
    v_stats_ping = []
    v_stats_ping = []
    return([v_str_hadoop,v_stats_param,v_stats_values_s,v_stats_values_e,v_stats_ping,v_stats_ping])

