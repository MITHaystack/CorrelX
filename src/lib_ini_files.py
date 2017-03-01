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
#File: lib_ini_files.py.
#Author: A.J. Vazquez Alvarez (ajvazquez@haystack.mit.edu)
#Description: 
"""
Routines for generating/accessing the .ini files with the configuration of the scenario for the correlation.

Notes
-----
 Parameters and values are case sensitive!

| Experiment files:
| Every experiment requires the following files:
|    correation.ini:             configuration of the correlation (start and end time, FFT length, etc.)
|    stations.ini:               information about stations clocks (polynomials).
|    sources.ini:                information about sources.
|    media.ini:                  information about media (input file names, formats, etc)
|    delay_model.ini:            information aobut delay model (polynomials).
| 
| Additionally, the following file is generated during initialization:
|    delays.ini:                 re-processed polynomials for delay model.

Regarding initialization files
------------------------------
 Initialization files follow this format:
 | 
 | [section]
 | param: value

"""
#History:
#initial version: 2015.12 ajva
#MIT Haystack Observatory

from __future__ import print_function

import imp
import sys
import os
import numpy as np

import const_ini_files
imp.reload(const_ini_files)
from const_ini_files import *


try:
    import configparser
except ImportError:
    import ConfigParser as configparser




def serialize_config(sources_file='media.ini'):
    """
    Converts configuration file into string.
    
    Parameters
    ----------
     sources_file : str
         name of initialization [.ini] file (e.g.: media.ini, stations.ini, delays.ini, etc.)
     
    Returns
    -------
     serial_str : str
         serialized contents of initialization file.
    
    Notes
    -----
    | **Format configuration:**
    |
    |  See const_ini_files.py:
    |   SEPARATOR_ELEMENTS
    |   SEPARATOR_VECTOR
    |   SEP_VALUES
    |  
    |
    | **Notes:
    |
    |  Avoid use of reserved separators from list of file if existing
    |   E.g.: list of files separated with commas
    |         file1,file2 -> file1:file2
    |
    |
    | **Example:**
    |
    |  >>> serial_str=serialize_config()
    |  >>> print(serial_str)
    |   VF-0.vt,polarizations/L:R:L:R,station/At,channels/0:0:1:1;VF-1.vt,polarizations/L:R:L:R,station/At,channels/0:0:1:1
    """
    
    s = configparser.ConfigParser()
    s.optionxform=str
    s.read(sources_file)
    serial_str=""
    for section in s.sections():
        serial_str+=section
        for (each_key, each_val) in s.items(section):
            each_val_mod=""
            for i in each_val:
                if (i==SEPARATOR_ELEMENTS)or(i==SEPARATOR_VECTOR):
                    each_val_mod+=SEP_VALUES
                else:
                    each_val_mod+=i
            
            serial_str+= SEPARATOR_ELEMENTS + each_key + SEPARATOR_PARAM_VAL + each_val_mod
        serial_str+= SEPARATOR_VECTOR
    serial_str=serial_str[:-1]
    
    return(serial_str)

def serial_params_to_array(read_str=""):
    """
    Converts string with serialized configuration file into array.
    
    Parameters
    ----------
     read_str : str
         serialized configuration [created with serialize_config()].
     
    Returns
    -------
     files_param : list of lists
         list of lists based on serialized configuration.
     
    Notes
    -----
    |
    | **Example:**
    |
    |  >>> params_array=serial_params_to_array(serial_str)
    |  >>> print(params_array)
    |   [['VF-0.vt', 'polarizations/L:R:L:R', 'station/At', 'channels/0:0:1:1'], ['VF-1.vt', 'polarizations/L:R:L:R', 'station/At', 'channels/0:0:1:1']]
    """
    
    read_split = read_str.split(SEPARATOR_VECTOR)
    files_param =[]
    for i in read_split:
        files_param += [i.split(SEPARATOR_ELEMENTS)]
    
    return(files_param)

def get_param_serial(params_array,section,param):
    """
    Retrieves value given an array with parameters, the filename and the parameter.
    
    Parameters
    ----------
     params_array : list
         configuration [created with serial_params_to_array()].
     section : str
         section to be looked up.
     param : str
         parameter to be looked up.
    
    Returns
    -------
     value : str
         value corresponding to requested section and param.
     
    Notes
    -----
    |
    | **Example:**
    |
    |  >>> value = get_param_serial(params_array,'VF-0.vt','channels')
    |  >>> print(value)
    |   0:0:1:1
    """
    if 1==1:
        will_break=0
        value=""
        for vector in params_array:
            if will_break==1:
                break
            if vector[0]==section:
                for i in vector[1:]:
                    if will_break==1:
                        break
                    if param in i:
                        value=i.split(SEPARATOR_PARAM_VAL)[1]
                        will_break=1
    else:
        value=""
        id_section = list(zip(*params_array))[0].index(section)
        for i in params_array[id_section][1:]:
            if param in i:
                value=i.split(SEPARATOR_PARAM_VAL)[1]
    return(value)


def get_param_total(params_array,section,param,separator_values=SEP_VALUES):
    """
    Returns the number of different values for a parameter.
    This is e.g. for getting the number of polarizations in a media.ini file for a station.
    
    Parameters
    ----------
     params_array : list
         configuration [created with serial_params_to_array()].
     section : str
         section to be looked up.
     param : str
         parameter to be looked up.
     separator_values
         separator for values [SEP_VALUES from const_ini_files.py by default].

    Returns
    -------
     total : int
            number of different values separated by separator_vaules.
    
    Notes
    -----
    |
    | **Example:**
    |
    |  >>> tot = get_param_total(params_array,'VF-0.vt','Channels')
    |  >>> print(tot)
    |   2
    """

    value = get_param_serial(params_array,section,param)
    total = len(set(value.split(SEP_VALUES)))
    return(total)

def get_param_eq_vector(params_array,section,param,separator_values=SEP_VALUES,modein="int"):
    """
    Returns the vector with the mapped values for the specified parameters.
    
    Parameters
    ----------
     params_array : list
         configuration [created with serial_params_to_array()].
     section : str
         section to be looked up.
     param : str
         parameter to be looked up.
     separator_values
         separator for values [SEP_VALUES from const_ini_files.py by default].
     modein : str
         selector of format for output:
         |    "int" : convert values in output list to integer.
         |    else:   return output list as is (strings).
    
    Returns
    -------
     eq_vector
         equivalent vector with indices reported at the first sections of the ini file.
     
    Notes
    -----
    |
    | **Example:**
    |
    | Given the file media.ini:
    |   [channels]
    |   CH2 = 1
    |   CH1 = 0
    |   [polarizations]
    |   L = 0
    |   R = 1
    |   [VDF-0.vt]
    |   polarizations = L:R:L:R
    |   channels = CH1:CH1:CH2:CH2
    |   station = At
    |   ...
    |  >>>params_array=serial_params_to_array(serialize_config(sources_file='media.ini'))    
    |  >>>eq_vector=get_param_eq_vector(params_array,'VDF-0.vt','polarizations')
    |  >>>print(eq_vector)
    |   [0, 1, 0, 1]
    """
    value = get_param_serial(params_array,section,param)
    values = value.split(SEP_VALUES)
    eq_vector=[]
    if modein=="int":
        for i in values:
            eq_vector+=[int(get_param_serial(params_array,param,i))]
    else:
        for i in values:
            eq_vector+=[get_param_serial(params_array,param,i)]
    return(eq_vector)
    
    
    
#def get_param_eq_vector_list(params_array,section,param,separator_values=SEP_VALUES):
#    """
#    Returns the vector with the mapped values for the specified parameters for separated elements.
#    
#    Example:
#    --------
#     Given the file media.ini:
#      [channels]
#      P15 = 4,5
#
#            
#    """
#    value = get_param_serial(params_array,section,param)
#    values = value.split(SEP_VALUES)
#    eq_vector=[]
#    for i in values:
#        eq_vector+=[list(map(int,get_val_vector(params_array,param,i)))]
#    return(eq_vector)
    
def get_val_vector(params_array,section,param):
    """
    Returns vector with list of values.
    
    Parameters
    ----------
     params_array : list
         configuration [created with serial_params_to_array()].
     section : str
         string with section to be looked up.
     param : str
         parameter to be looked up.
    
    Notes
    -----
    |
    | **Example:**
    |  Given the file media.ini:
    |   ...
    |   [file1]
    |   sidebands = U:U:U:U
    |   ...
    |  >>>>sidebands=get_val_vector(params_media,"file1",C_INI_MEDIA_SIDEBANDS)
    |  >>>>print(sidebands)
    |   ['U', 'U', 'U', 'U']
    """
    value = get_param_serial(params_array,section,param)
    values = value.split(SEP_VALUES)
    return(values)
    
def get_all_params_serial(params_array,section):
    """
    Retrieves list with all the parameters for a section.

    Parameters
    ----------
     params_array : list
         list with configuration [created with serial_params_to_array()].
     section : str
         section to be looked up.

    Returns
    -------
     values : str
         lists of strings corresponding to the parameters in the section.
    """

    values=[]
    for vector in params_array:
        if vector[0]==section:
            for i in vector[1:]:
                values+=[i.split(SEPARATOR_PARAM_VAL)[0]]
    return(values)

def get_all_values_serial(params_array,param):
    """
    Retrieves list with all the values of a paramter for all the section.

    Parameters
    ----------
     params_array : list
            configuration [created with serial_params_to_array()].
     param : str
            parameter to be looked up through all sections.
                
    Returns
    -------
     values
        list with strings with all the values corresponding to the requested paramter.
    """

    values=[]
    for vector in params_array:
        for i in vector[1:]:
            if param in i:
                values+=[i.split(SEPARATOR_PARAM_VAL)[1]]
    return(values)



def get_all_sections(params_array):
    """
    Retrieves list with all the section names.
    
    Parameters
    ----------
     params_array
         list with configuration [created with serial_params_to_array()].
     
    Returns
    -------
     sections
         list of strings with the names of all the sections in the ini file.
    """
    sections=[]
    for vector in params_array:
        sections.append(vector[0])
    return(sections)



##################################################################
#
#                      Specific routines
#
##################################################################

# Specific for delay model




def get_pair_st_so(st_id,so_id):
    """
    Get string st<st_id>-so<so_id> (e.g. st0-so0).
    
    Parameters
    ----------
     st_id : int
         station id.
     so_id : int
         source id.
     
    Returns
    -------
     pair_st_so : str
         output.
    """
    return("st"+str(st_id)+"-"+"so"+str(so_id))
    


def get_section_delay_model(params_array,mjd_in,seconds,source_id_in,station_id_in,v=0):
    """
    Get section from delay model ini file.
    
    Parameters
    ----------
     params_array : list
         information from delay model ini file (see lib_ini_files.py).
     mjd_in
         MJD for the start of the experiment.
     seconds
         number of seconds for the start of the experiment.
     source_id_in : int
         source identifier in sources ini file.
     station_id_in : int
         stations identifier in stations ini file.
     v : int
         0 by default, 1 for verbose mode.
     
    Returns
    -------
     section_found : str
         section found in the delay model ini file, otherwise "".
     start_s : int
         seconds for this section.
    
    Notes
    -----
    |
    | **TO DO:**
    |
    |  Move to lib_ini_files.py
    """
    section_found = ""
    sections = get_all_sections(params_array)
    start_s=0
    
    for i in sections:
        if v==1:
            print(i)
        [mjd_str,start_str,end_str,so_str,st_str]=i.split('-')
        mjd=int(mjd_str)
        start_s=int(start_str)
        end_s=int(end_str)
        so_id=int(so_str[2:])
        st_id=int(st_str[2:])
        if (mjd_in==mjd)and(start_s<=seconds)and(seconds<end_s)and(source_id_in==so_id)and(station_id_in==st_id):
            section_found=i
            break
    return([section_found,start_s])



def get_vector_delay_ref(vector_params_delay):
    """
    Get seconds corresponding to elements in delays.ini.
        
    Parameters
    ----------
     vector_params_delay : str
         serialized representation of the delay model ini file.
     
    Returns
    -------
     sv : list of floats
         seconds for delay information (start time polynomials).
    """
    sv=[]
    for i in vector_params_delay:
        if DELAY_MODEL_REL_MARKER in i[:len(DELAY_MODEL_REL_MARKER)]:
            #sv+=[int(i.split(DELAY_MODEL_REL_MARKER)[1])]
            sv+=[float(i.split(DELAY_MODEL_REL_MARKER)[1])]

    sv=np.array(sv)
    return(sv)


def find_nearest_seconds(vector_seconds_ref,seconds_fr):
    """
    Find second which is nearest to seconds_fr in delay param vector. In other words, find the timestamp
     (start time) of the accumulation period to which the timestamp of the frame corresponds.
    
    Parameters
    ----------
     vector_seconds_ref : list of floats
         seconds for delay information (start time polynomials).
     seconds_fr
         seconds to be searched for in the previous item.
     
    Returns
    -------
     seconds_out
         highest value lesser than the input number of seconds, or -1 if not found
    """

    
    sv = vector_seconds_ref # get_vector_delay_ref(vector_params_delay)
    sv_sub=np.array([seconds_fr]*len(sv))




    difference=np.subtract(sv,sv_sub)

    
    if np.abs(difference) is None or np.abs(difference).size==0:
        # TO DO: internal log? if so argument to function
        #if INTERNAL_LOG==1:
        #print("zM"+KEY_SEP+"Failed find_nearest="+str(seconds_fr)+ " in ("+','.join(vector_params_delay)+")")
        if DEBUG_GENERAL_M:
            print("zM\tFailed find_nearest="+str(seconds_fr)+ " in ("+','.join(vector_params_delay)+")")
        seconds_out = -1
    else:
        seconds_out = sv[np.argmin(np.abs(difference))]

    return(seconds_out)



#def get_rates_delays(seconds_fr_nearest,pair_st_so,params_delays,cache_rates=[]):
def get_rates_cache(seconds_fr_nearest,pair_st_so,params_delays,cache_rates=[]):
    """
    Get rates from delays.ini. It allows to keep a cache (currently only one element) to 
     reduce the number of look-ups.
    """
    
    found=None
    if cache_rates!=[]:
        #i = cache_rates[0]
        if seconds_fr_nearest==cache_rates[0] and pair_st_so==cache_rates[1]:
            #found=i[2][0]
            found=0
    #found = check_delay_cache(seconds_fr_nearest,pair_st_so,cache_rates)

    if found is not None:
        [rate_delay,ref_delay,abs_delay,delay]= cache_rates[2][0]#found_rates
        
    else:
        
        #if VERBOSE_INI_DELAYS:
        rr0_epoch=DELAY_MODEL_RR0_MARKER+str(seconds_fr_nearest)
        rr1_epoch=DELAY_MODEL_RR1_MARKER+str(seconds_fr_nearest)
        rr2_epoch=DELAY_MODEL_RR2_MARKER+str(seconds_fr_nearest)
        rrr_epoch=DELAY_MODEL_RRR_MARKER+str(seconds_fr_nearest)
        rc0_epoch=DELAY_MODEL_RC0_MARKER+str(seconds_fr_nearest)
        rc1_epoch=DELAY_MODEL_RC1_MARKER+str(seconds_fr_nearest)
        zc0_epoch=DELAY_MODEL_ZC0_MARKER+str(seconds_fr_nearest)
        zc1_epoch=DELAY_MODEL_ZC1_MARKER+str(seconds_fr_nearest)
        rcr_epoch=DELAY_MODEL_RCR_MARKER+str(seconds_fr_nearest)
        rcm_epoch=DELAY_MODEL_RCM_MARKER+str(seconds_fr_nearest)
        rcc_epoch=DELAY_MODEL_RCC_MARKER+str(seconds_fr_nearest)
        ddd_epoch=DELAY_MODEL_DDD_MARKER+str(seconds_fr_nearest)
        rate_delay = [float(get_param_serial(params_delays,pair_st_so,rr0_epoch)),\
                  float(get_param_serial(params_delays,pair_st_so,rr1_epoch)),\
                  float(get_param_serial(params_delays,pair_st_so,rr2_epoch)),\
                  float(get_param_serial(params_delays,pair_st_so,rrr_epoch)),\
                  float(get_param_serial(params_delays,pair_st_so,rc0_epoch)),\
                  float(get_param_serial(params_delays,pair_st_so,rc1_epoch)),\
                  float(get_param_serial(params_delays,pair_st_so,zc0_epoch)),\
                  float(get_param_serial(params_delays,pair_st_so,zc1_epoch)),\
                  float(get_param_serial(params_delays,pair_st_so,rcr_epoch)),\
                  float(get_param_serial(params_delays,pair_st_so,rcm_epoch)),\
                  float(get_param_serial(params_delays,pair_st_so,rcc_epoch)),\
                  float(get_param_serial(params_delays,pair_st_so,ddd_epoch))]
        
        rel_epoch=DELAY_MODEL_REL_MARKER+str(seconds_fr_nearest)
        delay = float(get_param_serial(params_delays,pair_st_so,rel_epoch))
        
        f_epoch=DELAY_MODEL_REF_MARKER+str(seconds_fr_nearest)
        ref_delay=float(get_param_serial(params_delays,pair_st_so,f_epoch))
        
        abs_epoch=DELAY_MODEL_ABS_MARKER+str(seconds_fr_nearest)
        abs_delay = float(get_param_serial(params_delays,pair_st_so,abs_epoch))
        
        #else:
        #    di_epoch=DELAY_MODEL_DI_MARKER+str(seconds_fr_nearest)
        #    rate_delay=map(float,get_param_serial(params_delays,pair_st_so,di_epoch).split(SEP_VALUES))
        #if rate_delay!=rate_delay2:
        #    print("Diff")
        
        new_rates = [rate_delay,ref_delay,abs_delay,delay]
        
        # Simply store last element
        #cache_rates=[]
        #cache_rates.append([seconds_fr_nearest,pair_st_so,[new_rates]])
        cache_rates = [seconds_fr_nearest,pair_st_so,[new_rates]]
    
    rates_out = [rate_delay,ref_delay,abs_delay,cache_rates]
    #delays_out = [delay,cache_rates]
    #return([rates_out,delays_out])
    return(rates_out)



#    [rates_out,delays_out] = get_rates_delays(seconds_fr_nearest,pair_st_so,params_delays,cache_rates)
#    return(rates_out)

def get_delay_cache(seconds_fr_nearest,pair_st_so,params_delays,cache_delays=[]):
    """
    Get the delays from the cache.
    """
    #print("get: "+str(seconds_fr_nearest))
    # First try to find in delay cache, then complete cache, then fetch from ini info.
    found = None
    #if 1==0: #cache_delays!=[]:
    if cache_delays!=[]:
        #i = cache_rates[0]
        if seconds_fr_nearest==cache_delays[0] and pair_st_so==cache_delays[1]:
            delay_new=cache_delays[2]
            found=1
    if found is None:
        #[rates_out,delays_out] = get_rates_delays(seconds_fr_nearest,pair_st_so,params_delays,cache_rates)
        
        rel_epoch=DELAY_MODEL_REL_MARKER+str(seconds_fr_nearest)
        delay = float(get_param_serial(params_delays,pair_st_so,rel_epoch))
        

        cache_delays = [seconds_fr_nearest,pair_st_so,delay]
        delays_out= [delay,cache_delays]
    else:
        #cache_delays = [seconds_fr_nearest,pair_st_so,delay_new]
        delays_out=[delay_new,cache_delays]
    
    #print(delays_out)
    return(delays_out)

# <codecell>


