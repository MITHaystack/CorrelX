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
#File: get_params_ports.py.
#Author: A.J. Vazquez Alvarez (ajvazquez@haystack.mit.edu)
#Description: 
"""
    Script for getting list with parameters for ports to be used in CorrelX (see const_config.py for details).

    TO DO: 
    ------
     -Create constants for default ports (currently hardcoded).
     -Create constants for parameters (currently hardcoded).

"""
#History:
#initial version: 2016.10 ajva
#MIT Haystack Observatory

import sys


def find_unused_ports_from_netstat():
    """
    Get a list of ports that are not already taken.
    
    Parameters
    ----------
    input_file
        input file based on netstat output generated e.g.: 
              netstat -nlp|grep LISTEN|grep : > used_ports_$1_`hostname`.txt
    v
        verbose if 1.
    
    Notes:
    ------
    | The input file may be the concatenation of files for multiple nodes to 
    | find common nodes available on all nodes.
    | 
    |  This is to overcome the issue of some ports held by previous Hadoop processes.
    |
    |
    | **TO DO:**
    |
    |  -Add default ports, checks, etc.
    """

    start_port_range=20000
    end_port_range=32000
    num_ports=3
    #default_ports=[20000,20001,20002]
    
    input_file=sys.argv[1]
    v=1
    
    with open(input_file,'r') as f:
        ports_v=[]
        for line in f:
            line_short=' '.join(line.strip().split())
            line_split=line_short.split()
            port=line_split[3].split(':')[-1]
            ports_v.append(port)
        
        found_ports=[]
        id_port=0
        for test_port in range(start_port_range,end_port_range):
            if str(test_port) not in ports_v:
                found_ports.append(test_port)
                id_port+=1
                if id_port==num_ports:
                    break
    #if v==1:    
    #    print("Used ports: "+','.join(map(str,ports_v)))
    #    print("Available ports: "+','.join(map(str,found_ports)))
    
    str_out=",nmlocport="+str(found_ports[0])+",nmwebport="+str(found_ports[1])+",shuffleport="+str(found_ports[2])
    print(str_out)
    return(str_out)


if __name__ == '__main__':
    find_unused_ports_from_netstat()




