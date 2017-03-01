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
#File: lib_delay_model.py.
#Author: A.J. Vazquez Alvarez (ajvazquez@haystack.mit.edu)
#Description: 
"""
Library for calculating delays using polynomial models.

Known issues
------------
     (!) Delays currently computed w.r.t. the first station, they should computed w.r.t. the center of the Earth instead.
     
TO DO
-----
     Compute absolute delays instead of relative to reference station.

"""
#History:
#initial version: 2016.06 ajva
#MIT Haystack Observatory

from __future__ import print_function,division
import imp

#from datetime import date

import lib_ini_files
imp.reload(lib_ini_files)
from lib_ini_files import *

try:
    import configparser
except ImportError:
    import ConfigParser as configparser

import numpy as np
    
 
    
###########################################
#         Debugging
###########################################
    
# Basic delay information for testing
SHOW_DELAY_INFO=0
#SHOW_DELAY_INFO=0

# Show information on computation of delays
DEBUG_GET_DELAY=0
#DEBUG_GET_DELAY=1         
    
# Show information on computation of the delays, to be used with the option for debugging in rsvf (DEBUG_LIB_DELAY)
DEBUG_LIB_DELAY_ONLY_HERE=0
#DEBUG_LIB_DELAY_ONLY_HERE=1 
 
# Show debug info for delay computation
DEBUG_FINAL_DELAYS=0
#DEBUG_FINAL_DELAYS=1        

###########################################
#         Thresholds
###########################################
    
# Threshold for warning about too many seconds between referent time (experiment start time) and clock epoch
#  (Value taken from fourfit)
# TO DO: If this threshold is passed, there may be precision issues with the delay computation, work on this.
TH_WARNING_DIFF_CLOCK=3.0e5
        
# Threshold for considering null a relative delay
TH_REL_DELAY_ZERO=1e-17


###########################################
#         Configuration
###########################################

# ---- Absolute/differential polynomials (during correlation) ----
# default DIFF_POLY=1
DIFF_POLY=1  # (Default) Will use differential polynomials for delay computations
#DIFF_POLY=0 # This needs debugging

# ---- Offset at calculation of first delay
# deafult DIFF_OFFSET=1
DIFF_OFFSET=1   # (Deafult) Will apply offset at calculation of first delay
#DIFF_OFFSET=0  # Untested

# ---- Delay model and clock into same polynomial
# default SUM_CLK_DIFF=1
SUM_CLK_DIFF=1 # Default (add clock coefficients to delay polynomial)
#SUM_CLK_DIFF=0 # Untested

# --- Order of polynomials used for computations
# default LIMIT_POLY_EVAL=5
LIMIT_POLY_EVAL=5 # (read from delay_model.ini file)
LIMIT_POLY_FOUND=2 # (computations) 2 for quadratic, so that it will take the first three elements of the polynomial

# ---- Type for the coefficients of the polynomials read from the ini files
TYPE_COEFF_DELAY = float
#TYPE_COEFF_DELAY = np.float64
#TYPE_COEFF_DELAY = np.longdouble
    
# ---- Number of decimal positions in coefficients of polynomials to be written as strings
# default TOT_DEC_POS=16
TOT_DEC_POS=16
    
    



###########################################
#         Shift and fractional delay
###########################################

    
    
def get_delay_shift_frac(delay,fs,data_type=0):
    """
    Compute integer and fractional shift.
    
    Parameters
    ----------
     delay : float
         value for the delay [s].
     fs : float
         sampling frequency [Hz].
     data_type : int
         0 for real, 1 for complex.
     
    Returns
    -------
     shift_int : int
         offset in number of samples.
     fractional_sample_delay : float
         fractional sample.
    
    Notes
    -----
     Data type is for being used with complex samples unpacked as a list of integers (in msvf),
      otherwise it should be left as zero.
    """
    shift = delay * fs

    fractional_sample_delay = shift%1
    shift_int = int(shift//1)
    if data_type==1:
        shift_int*=2
    
    
    if fractional_sample_delay>0.5:
        fractional_sample_delay-=1
        shift_int+=1
        if data_type==1:
            shift_int+=1

    
    
    return([shift_int,fractional_sample_delay])
    


def get_full_frac_val(r_recalc,fs,diff_frac=0,bypass_correction=0):
    """
    Compute total offset in number of samples, and also fractional sample correction.
    
    Parameters
    ----------
     r_recalc : float
         delay.
     fs : float
         sampling frequency.
     diff_frac : 0
         [unused] 0 by default.
     bypass_correction : int
         | if 0: corrects the fractional sample correction to be between -0.5 and +0.5.
         | if 1: returns the fractional sample correction between 0 and 1.
     
    Returns
    -------
     full_fractional_recalc : float
         total offset in number of samples, including fractional part.
     fractional_recalc : float
         fractional sample correction.
    
    Notes
    -----
     Bypass correction used in get_frac_over for simplicity.
    """

    fractional_recalc=((r_recalc)*fs)
    full_fractional_recalc=fractional_recalc
    fractional_recalc=np.mod(fractional_recalc,1)

    if bypass_correction==0:
        fractional_recalc_out=fractional_recalc-(fractional_recalc>0.5).astype(np.float64)
    else:
        fractional_recalc_out=fractional_recalc
    return([full_fractional_recalc,fractional_recalc_out])



###########################################
#         Polynomials
###########################################


def get_poly_list(params_array,section_str):
    """
    Get list with poly from params array and section string.
    
    Parameters
    ----------
     params_array : list
         information from delay model ini file (see lib_ini_files.py).
     section_str : str
         section of the ini file associated with a certain station, source and time (see const_ini_files.py)
    
    Returns
    -------
     poly_model : list of float
         list of [n-th coeff] with polynomial coefficients for the delay model with n from zero to max_order, in seconds.

    Notes
    -----
    |
    | **Configuration (constants):**
    |
    |  LIMIT_POLY_EVAL:  [2 by default] maximum order coefficient of the polynomial.
    |
    |
    | **Notes:**
    |
    |  Coefficients in the delay model ini file are in microseconds, output polynomials in seconds.
    """
    poly_model_delay = list(map(TYPE_COEFF_DELAY,get_val_vector(params_array,section_str,C_INI_MODEL_DELAY)))
    

    poly_model=poly_model_delay
    
    
    # microseconds to seconds
    poly_model=np.multiply(poly_model,1e-6)
    
    max_order=min(len(poly_model),LIMIT_POLY_EVAL+1)
    poly_model=poly_model[:max_order]
    
    return(poly_model)


def get_poly_clock(params_array,section_str):
    """
    Get list with poly from params array and section string.
    Clock correction.
    
    Parameters
    ----------
     params_array : list
         information from delay model ini file (see lib_ini_files.py).
     section_str : str
         section of the ini file associated with a certain station, source and time (see const_ini_files.py)
    
    Returns
    -------
     poly_model : list of float
         list of [m-th coeff] with polynomial coefficients for the station clock model with m from zero to max_order-1, in seconds.
    """
    poly_model = list(map(TYPE_COEFF_DELAY,get_val_vector(params_array,section_str,C_INI_ST_CLOCK_POLY)))
    
    # microseconds to seconds
    poly_model=np.multiply(poly_model,1e-6)
    return(poly_model)



def apply_offset_coefficients_poly(poly_found,seconds_from_ref):
    """
    Apply offset to polynomial, i.e. given a list of polynomial coefficients [p0,p1,p2,...] 
     from low to high order for f(x), obtain coefficientes for f(x+s) where s is an offset:
    
      q(x)=p(x+s)=p0+p1(x+s)+p2(x+s)^2+p3(x+s)^3+...
    
    The functions returns the list of polynomial coefficients [q0,q1,q2,...] for q(x).
    
    Parameters
    ----------
     poly_found : list of float
         list of polynomial coefficients [p0,p1,p2,...] (LIMIT_POLY_EVAL coefficients).
     seconds_from_ref : float
         offset in seconds.
     
    Returns
    -------
     poly_found : list of int
         list of polynomial coefficients [q0,q1,p2,...] (LIMIT_POLY_FOUND coefficients).
     seconds_from_ref : 0 
         (since this offset has been applied to the polynomial).
    
    Notes
    -----
    |
    | **Configuration:**
    |
    |  LIMIT_POLY_EVAL:  maximum order coefficient in input. (e.g.: 5).
    |  LIMIT_POLY_FOUND: maximum order coefficient in output (e.g.: 2).
    """

    # Note that order of operations is important, first low order, then high order
    poly_found[0]+=poly_found[1]*seconds_from_ref
    poly_found[0]+=poly_found[2]*np.power(seconds_from_ref,2)
    poly_found[0]+=poly_found[3]*np.power(seconds_from_ref,3)
    poly_found[0]+=poly_found[4]*np.power(seconds_from_ref,4)
    poly_found[0]+=poly_found[5]*np.power(seconds_from_ref,5)
    
    poly_found[1]+=poly_found[2]*2.0*seconds_from_ref
    poly_found[1]+=poly_found[3]*3.0*np.power(seconds_from_ref,2)
    poly_found[1]+=poly_found[4]*4.0*np.power(seconds_from_ref,3)
    poly_found[1]+=poly_found[5]*5.0*np.power(seconds_from_ref,4)
    
    poly_found[2]+=poly_found[3]*3.0*seconds_from_ref
    poly_found[2]+=poly_found[4]*6.0*np.power(seconds_from_ref,2)
    poly_found[2]+=poly_found[5]*10.0*np.power(seconds_from_ref,3) # check
    
    poly_found=poly_found[:LIMIT_POLY_FOUND+1]
    
    seconds_from_ref=0.0
    
    return([poly_found,seconds_from_ref])

def get_polynomials_interval(params_array_delay_model,params_array_stations,mjd,seconds_ref,seconds_from_ref,source_id,station_id,v,current_offset=0):
    """
    Get the polynomials for the delay and clock models for a given station, source and time:
      -day: MJD.
      -seconds: seconds_ref+seconds_from_ref.
    
    Parameters
    ----------
     params_array_delay_model : list
         information from delay model ini file (see lib_ini_files.py).
     params_array_stations : list
         information from stations ini file (see lib_ini_files.py).
     mjd : int
         MJD for the start of the scan.
     seconds_ref : int or float
         seconds for the start of the scan.
     seconds_from_ref : int or float
         offset seconds from the start of the scan.
     source_id : int
         source id.
     station_id : int
         station id.
     v : int
         verbose if 1.
    
    Returns
    -------
     result : float
         delay [s].
     poly_found
         polynomial (see get_poly_list() for format).
     seconds_from_ref
         [check this]
     poly_station_clock
         polynomial for the station clock.
     seconds_diff_clock
         seconds between the epoch and the start of the clock.
     result_dr
         geometric delay [s].
     result_cr
         station clock delay [s].
     error_model : { 0, None}
         0 if success, None if error.
     section_in_model : str
         section in the delay model .ini file (for debugging).
     no_offset : int
         0 if no offset applied from .ini file [default], 1 otherwise.
    
    
    Notes
    -----
    |
    | **Limitations:**
    |
    |  Currently single source source_id=0.
    |
    |
    | **Notes:**
    |
    |  It returns delay from model ini file.
    |  If no model is available returns None.
    |  Clock polynomial always zero offset with respect to start time, thus no need to offset (unless delta from start).
    |
    |
    | **TO DO:**
    |
    |  Organize.
    |  Further testing on precision.
    """
    result=None
    poly_found=None
    error_model=0    

    
    pre_current_offset=current_offset

    
    # Find section in ini file for this pair station-source and this time.
    [section_in_model,seconds_ref_poly]=get_section_delay_model(params_array_delay_model,mjd,seconds_ref+seconds_from_ref,source_id,station_id,v)
    station_str=get_all_sections(params_array_stations)[station_id]
    
    # Clock epoch and polynomial
    poly_station_clock = get_poly_clock(params_array_stations,station_str)
    poly_ref_clock = float(get_val_vector(params_array_stations,station_str,C_INI_ST_CLOCK_REF)[0])
    no_offset=0
    
    # Consider deleting this, no longer used
    try:
        poly_offset_clock = float(get_val_vector(params_array_stations,station_str,C_INI_ST_CLOCK_OFFSET)[0])*1e-6
        no_offset=1
        print(" Clock offset: "+str(poly_offset_clock)+" offset for station "+station_str)
    except ValueError:
        poly_offset_clock = 0.0
        #print("Zero offset for station "+station_str)
    
    # Reference time
    seconds_ref_frac=float(seconds_ref)/(24.0*60.0*60.0)
    mjd_and_frac = float(mjd)+seconds_ref_frac
    

    # Seconds from epoch for clock
    seconds_diff_clock = (mjd_and_frac-poly_ref_clock)*24*60*60
    seconds_diff_clock+=seconds_from_ref
    
    
    if seconds_diff_clock>TH_WARNING_DIFF_CLOCK:
        if SHOW_DELAY_INFO:
            print("Warning: seconds between clock epoch and experiment start time is: "+str(seconds_diff_clock))
    
    # Clock model  
    result=np_polyval(poly_station_clock,seconds_diff_clock)
    
    # Move reference to start time
    offset_at_start=np_polyval(poly_station_clock,seconds_diff_clock-current_offset+pre_current_offset)

    # Offset of clock polynomial coefficients
    #if CLK_OFFSET:
    poly_station_clock[0]=offset_at_start
    poly_station_clock[0]+=poly_offset_clock

    seconds_diff_clock=0
    
    if DEBUG_GET_DELAY:
        print("gdm "+        str( mjd                ).rjust(8)+\
                             str( seconds_ref        ).rjust(8)+\
                             str( seconds_from_ref   ).rjust(8)+\
                "{0:.4f}".format( mjd_and_frac       ).rjust(13)+\
                             str( source_id          ).rjust(4)+\
                             str( station_id         ).rjust(4)+\
                "{0:.4f}".format( poly_ref_clock     ).rjust(13)+\
                             str( seconds_diff_clock ).rjust(13)+\
                    " "+str(list( poly_station_clock )))
                #,end="")
    
    result_cr=result
    seconds_from_ref+=seconds_ref-seconds_ref_poly

    #seconds_from_ref-=current_offset
    
    # Delay model
    if section_in_model != "":
        poly_found = get_poly_list(params_array_delay_model,section_in_model)

        if DEBUG_GET_DELAY:
            print("gdm     "+str(seconds_from_ref).rjust(10)+" "+str(list(poly_found)[:4]))
        
        result_dr=np_polyval(poly_found,seconds_from_ref)
        result+=result_dr
    else:
        error_model=None
        result=None
        result_dr=None
        result_cr=None
        if v==1:
            print("not found")
    
    # Apply offset to polynomial and zero offset
    [poly_found,seconds_from_ref] = apply_offset_coefficients_poly(poly_found,seconds_from_ref)

    if DEBUG_GET_DELAY:
        print("gdmr ".ljust(10)+\
                "sfr="+str(seconds_from_ref)+\
                ", mop="+str(len(poly_found)-1)+\
                ", sdc="+str(seconds_diff_clock)+\
                ", moc="+str(len(poly_station_clock)-1))
        print("---------")

    
    return([result,poly_found,seconds_from_ref,poly_station_clock,seconds_diff_clock,\
               result_dr,result_cr,error_model,section_in_model,no_offset])
    



def np_polyval(pol,x):
    """
    Evaluate a polynomial at x. Numpy polyval, changing order of polynomial (numpy is decreasing order).
    
    Parameters
    ----------
     pol : list of floats
         polynomial, leftmost term is zero order.
     x : float
         value to be evaluated.
    
    Returns
    -------
     out : float
         result.
    
    Notes
    -----
    | -For numpy polyval the rightmost term is zero order.
    | (!) --Excerpt from the reference-- "Notes:Horner's scheme [R65] is used to evaluate the polynomial. 
    |    Even so, for polynomials of high degree the values may be inaccurate due to rounding errors. 
    |    Use carefully."
    """
    return(np.polyval(pol[::-1],x))

def np_polyder(pol,x,o):
    """
    Get derivative of order o at x. Numpy polyval, changing order of polynomial (numpy is decreasing order).
    
    See np_polyval() for more info.
    """
    return(np.polyval(np.polyder(pol[::-1],o),x))


def np_roots(pol):
    """
    Get roots of a polynomial. Numpy roots, changing order.
    
    Parameters
    ----------
     pol : list of floats
         polynomial, leftmost term is zero order.
     
    Returns
    -------
     out : list of complex
         roots of the polynomial.
    """
    return(np.roots(pol[::-1]))


def filter_roots(roots):
    """
    Return the real positive smallest delay from a list of complex. 
    To be called after np_roots(pol).
    
    Parameters
    ----------
     roots : list of complex
         roots of a delay polynomial.
     
    Returns
    -------
     min_val : float
         result.
    """
    filt_roots=[]
    for i in roots:
        if np.imag(i)==0:
            val_r = i.astype(np.float64)
            filt_roots.append(val_r)       
    min_val=filt_roots[0]
    for i in filt_roots:
        if np.abs(i)<np.abs(min_val):
            min_val=i
    return(min_val)



def get_all_polynomials(params_array_delay_model,params_array_stations,s_st,s_so,mjd_start,seconds_ref,\
                        seconds_offset,tot_steps,step_seconds,v=0):
    """
    Read and pre-process the delay polynomials from the models in the ini files.
    
    Parameters
    ----------
     params_array_delay_model : list
         list with configuration of delay_mode.ini.
     params_array_stations : list
         list with configuration of stations.ini.
     s_st : configparser handler
         configparser handler for the station ini file.
     s_so : configparser handler
         configparser handler for the sources ini file.
     mjd_start
         MJD for the start of the scan.
     seconds_ref
         seconds for the start of the scan.
     seconds_offset
         offset seconds from the start of the scan.
     tot_steps
         number of accumulation periods.
     step_seconds
         accumulation period duration.
     v
         verbose if 1.
    
    Returns
    -------
     seconds_inc_v : 3D list (tot_steps x sources x stations) 
         offset in seconds since scan start.
     max_saved : 2D list (tot_steps x sources)
         maximum delay for all stations.
     min_saved : 2D list (tot_steps x sources) 
         minimum delay for all stations.
     v_delays : 3D list (tot_steps x sources x stations)
         delays in seconds.
     v_delay_rates : 3D list (tot_steps x sources x stations) 
         lists with [poly_delay,seconds_from_ref,poly_station_clock,\
                    seconds_diff_clock,result_dr,result_cr,section_in_model] from get_polynomials_interval().
     no_offset_total : int
         number of times that manual offsets have been applied (based on .ini).
    
    Notes
    -----
    |
    | **TO DO:**
    |
    |  Check seconds_offset.
    """
    v_delays = []
    v_delay_rates=[] # delay rates linear for further use (only component 1 of the polynomial)
    #max_saved = -1.0
    
    max_saved=[]
    min_saved=[]
    
    no_offset_total=0
    
    seconds_inc_v=[]
    
    
    for i in range(tot_steps):

        seconds_inc=i*step_seconds+seconds_offset
        
        #print("Computing delay at delta_t="+str(seconds_inc))
        seconds_inc_v.append(seconds_inc)
        
        i_max_saved=[]
        i_min_saved=[]
        for soj in s_so.sections():
    
            # Read source data from file
            so_id=s_so.getint(soj,'id')

            e_delays = []
            e_delay_rates = []
            for sti in s_st.sections():
        
                st_id=s_st.getint(sti,'id')
                if v==1:
                    print(st_id)
    

                if v==1:
                    print(seconds_inc)
                    print(mjd_start)
                    print(seconds_ref)
                    print(seconds_inc)
                    print(so_id)
                    print(st_id)
                [delay,poly_delay,seconds_from_ref,
                            poly_station_clock,seconds_diff_clock,\
                            result_dr,result_cr,error_model,section_in_model,no_offset] = get_polynomials_interval(params_array_delay_model,\
                                                                        params_array_stations,mjd_start,\
                                                                        seconds_ref,seconds_inc,so_id,st_id,v,\
                                                                        current_offset=i*step_seconds)
                
                no_offset_total+=no_offset
                # TO DO: if and exit later
                if error_model is None:
                    return(None)
                
                
                e_delays+=[delay]
                e_delay_rates+=[[poly_delay,seconds_from_ref,poly_station_clock,seconds_diff_clock,result_dr,result_cr,section_in_model]]
                
                # Compute maximum with new computed delays
            i_max_saved.append(max(e_delays))
            i_min_saved.append(min(e_delays))
        
        v_delays += [e_delays]
        v_delay_rates += [e_delay_rates] # only linear component
        max_saved.append(i_max_saved)
        min_saved.append(i_min_saved)

    return([seconds_inc_v,max_saved,min_saved,v_delays,v_delay_rates,no_offset_total])




###########################################
#         Display
###########################################


def print_delays_header(v=1):
    """
    Print header for summary of delay information.
    
    Parameters
    ----------
     v : int
         verbose mode if 1.
    
    Returns
    -------
     str_out : str
         line with header.
    """
    str_out=("D "+"st".rjust(3)+ "t0 [s]".rjust(14)+"total delay [us]".rjust(20)+\
          "clock [us]".rjust(20)+\
          "clock rate [us/s]".rjust(20)+\
          "total rate [us/s]".rjust(20)+\
          "total accel [us/s/s]".rjust(25))
    if v==1:
        print(str_out)
    return(str_out)


def print_delays(sti,poly_total,poly_clock,t0,t_display=None,v=1):
    """
    Print delay information in [us] for debugging.
    
    Parameters
    ----------
     sti : int
         station id.
     poly_total : list of floats
         polynomial for total delay (leftmost is zero order).
     poly_clock : list of floats
         polynomial for clock (leftmost is zero order).
     t0 : float
         time to evaluate polynomials at.
     t_display : float
         time to be displayed.
     v : int
         verbose mode if 1.
     
    Returns
    -------
     str_out : str
         line with delay information.
    """
    if t_display is None:
        t_display=t0
    r_td = np_polyval(poly_total,t0)*1e6
    r_cd = np_polyval(poly_clock,t0)*1e6
    r_cr = np_polyder(poly_clock,t0,1)*1e6
    r_tr = np_polyder(poly_total,t0,1)*1e6
    r_ta = np_polyder(poly_total,t0,2)*1e6
    
    
    str_out=("D "+str(sti).rjust(3)+\
              str(t_display).rjust(14)+\
              get_str_scf(r_td).rjust(20)+\
              get_str_scf(r_cd).rjust(20)+\
              get_str_scf(r_cr).rjust(20)+\
              get_str_scf(r_tr).rjust(20)+\
              get_str_scf(r_ta).rjust(25))
    
    if v==1:
        print(str_out)
    return(str_out)



def get_str_scf(val,tot_dec_pos=TOT_DEC_POS):
    """
    Get string representation of float, used for normalization.
    """
    str_format_out=str(val)
    return(str_format_out)



def set_config_delay(s_delay,st_so,seconds_i,a_delay,r_delay,f_delay,poly_diff,clock_diff,clock_abs,\
                     seconds_from_ref,seconds_diff_clock,m_delay,c_delay,delta_reference_delay,\
                     section_in_model):
                                     
    """
    Save configuration parameters to delay ini file handler.
    
    Parameters
    ----------
     s_delay : configparser handler
         configparser handler for the delays ini file (to be written).
     st_so : str
         station - source.
     seconds_i : int or float
         seconds for the start of this interval since the start of the scan.
     a_delay : float
         absolute delay [s].
     r_delay : float
         relative delay (w.r.t. reference station) [s]
     f_delay : float
         [unused?]
     poly_diff : list of float
         polynomial for geometric delay.
     clock_diff : list of float
         polynomial for station clock delay.
     clock_abs : float
         [unused?] initially for ref station clock.
     seconds_from_ref : float
         [same as seconds_i ?]
     seconds_diff_clock : float
         seconds from epoch for the clock (zero if offset).
     m_delay : float
        geometric-model-only delay.
     c_delay : float
         clock-only delay.
     delta_reference_delay
         [unused?]
     section_in_model : str
         section in delay model (for debugging).
     
    Returns
    -------
     s_delay : configparser handler
         updated version of input with added information.
    """
    # Store same polynomials as used previously, otherwise there will be offsets
    
    s_delay.set(st_so, DELAY_MODEL_ABS_MARKER+str(seconds_i),          str(a_delay))
    s_delay.set(st_so, DELAY_MODEL_REL_MARKER+str(seconds_i),  get_str_scf(r_delay))
    s_delay.set(st_so, DELAY_MODEL_REF_MARKER+str(seconds_i),  get_str_scf(f_delay))
    
    # See const_ini_files.py for descriptions
    s_delay.set(st_so, DELAY_MODEL_RR0_MARKER+str(seconds_i), get_str_scf(poly_diff[0]))
    s_delay.set(st_so, DELAY_MODEL_RR1_MARKER+str(seconds_i), get_str_scf(poly_diff[1]))
    s_delay.set(st_so, DELAY_MODEL_RR2_MARKER+str(seconds_i), get_str_scf(poly_diff[2]))
    
    s_delay.set(st_so, DELAY_MODEL_RC0_MARKER+str(seconds_i),         str(clock_diff[0]))
    s_delay.set(st_so, DELAY_MODEL_RC1_MARKER+str(seconds_i),         str(clock_diff[1]))
    
    s_delay.set(st_so, DELAY_MODEL_ZC0_MARKER+str(seconds_i),         str(clock_abs[0]))
    s_delay.set(st_so, DELAY_MODEL_ZC1_MARKER+str(seconds_i),         str(clock_abs[1]))
    
    s_delay.set(st_so, DELAY_MODEL_RRR_MARKER+str(seconds_i),         str(seconds_from_ref))

    s_delay.set(st_so, DELAY_MODEL_RCR_MARKER+str(seconds_i),         str(seconds_diff_clock))
    s_delay.set(st_so, DELAY_MODEL_RCM_MARKER+str(seconds_i),         str(m_delay))
    s_delay.set(st_so, DELAY_MODEL_RCC_MARKER+str(seconds_i),         str(c_delay))
    
    s_delay.set(st_so, DELAY_MODEL_DDD_MARKER+str(seconds_i),         str(delta_reference_delay))
    
    # For debugging:
    s_delay.set(st_so, DELAY_MODEL_SIM_MARKER+str(seconds_i),         str(section_in_model))
    
    return(s_delay)


def create_sections_config_delays(s_delay,s_st,s_so):
    """
    Add sections to the delay ini file handler.
    
    Parameters
    ----------
     s_delay
         configparser handler for the delays ini file.
     s_st
         configparser handler for the station ini file.
     s_so
         configparser handler for the sources ini file.
     
    Returns
    -------
     s_delay : configparserhandler
         updated version of input.
     pairs_vv : list of str
         list with headers, to be accessed using the station and source ids.
    """
    pairs_vv = []
    
    # Create section for configuration file
    for sti in s_st.sections():  
        pairs_i = []
        
        for soj in s_so.sections():  
            #st_so = "st"+s_st.get(sti,'id')+"-"+"so"+s_so.get(soj,'id')
            st_so = get_pair_st_so(s_st.get(sti,'id'),s_so.get(soj,'id'))
            s_delay.add_section(st_so)
            pairs_i.append([st_so])
        pairs_vv.append(pairs_i)
        
    # print(pairs_vv[0][0])
    # print(pairs_vv[1][0])
            
    return([s_delay,pairs_vv])



###########################################
#         Delay computations
###########################################


def get_delay_val(clock_diff,poly_diff,seconds_ref_clock,seconds_ref_poly,seconds,seconds_offset,v=0,diff_pol=1):
    """
    Compute delays at seconds_offset+seconds, considering the offsets for each polynomial (seconds_ref_clock and seconds_ref_poly).
    
    Parameters
    ----------
     clock_diff : list of float
         polynomial for station clock delay.
     poly_diff : list of float
         polynomial for geometric model delay.
     seconds_ref_clock : float
        offset for the station clock polynomial.
     seconds_ref_poly : float
         offset for the geometric model polynomial.
     seconds : float 1D np.array
         seconds (for evaluating polys).
     seconds_offset : float
         offset to be applied to seconds.
     v : int
         verbose if 1.
     diff_pol : int
         1 if using differential polynomials.
    
    Returns
    -------
     r_recalc : float 1D np.array
         delays in seconds (model+clock)
     m_delay : float 1D np.array
         delays in seconds (model)
     c_delay : float 1D np.array
         delays in seconds (clock)
     rate_mc : list of int
         [0]
     acce_mc : list of int
         [0]
    
    Notes
    -----
    |
    | **Assumptions:**
    |
    |  If the four first elements of poly_diff are zero the resulting model delay is zero to avoid calling polyval.
    |
    |
    | **Notes:**
    |
    | simple offset reduces the computation of the offset to only the first value in the timescale provided in seconds.
    | No correction for retarded baselines.
    |
    |
    | **TO DO:**
    |
    |  This needs further work.
    |  Remove rate_mc and acce_mc, no longer used.
    |  Check diff_pol, different behavior ini_exper and lib_fx...
    """


    
    zero_r_recalc=0
    if (len(seconds)>1):
        if 0==poly_diff[0]==poly_diff[1]==poly_diff[2]:
            zero_r_recalc=1
            m_delay=np.zeros(seconds.shape)
    if zero_r_recalc==0:
        seconds_np=seconds+seconds_offset+seconds_ref_poly
        m_delay=np_polyval(poly_diff,seconds_np)
    if diff_pol==0:
        #c_delay=np_polyval(clock_diff,seconds_np)
        #m_delay=np_polyval(poly_diff,seconds_np-m_delay)
        c_delay=np_polyval(clock_diff,seconds_np)
    else:
        c_delay=0

    r_recalc=m_delay+c_delay
    if diff_pol==0:
        r_recalc=-r_recalc
    rate_mc=[0]
    acce_mc=[0]

    return([r_recalc,m_delay,c_delay,rate_mc,acce_mc])


  


def get_initial_abe(poly_delay,seconds_ref_poly,seconds_offset=0):
    """
    Compute delay due to aberration.
    """

    poly_st_B = np.multiply(1,np.copy(poly_delay))
    t0 = seconds_ref_poly+seconds_offset
    
    # intersection of wavefront propagation with position of current station

    mix_pol=np.copy(poly_st_B)
    mix_pol=mix_pol.astype(np.float64)
    
    
    mix_pol[1]-=1


    
    # find roots and delete complex and out of window results
    # TO DO: check thresholds
    roots_pol = np_roots(mix_pol)

    roots_pf=filter_roots(roots_pol)
    
    return(roots_pf)




###########################################
#         Generation of delays.ini
###########################################


def compute_initial_delays(params_array_delay_model,params_array_stations,s_st,s_so,s_delay,mjd_start,seconds_ref_in,\
                           tot_steps,step_seconds,seconds_offset=0,v=1,file_ini=""):
    """
    Main script for computing the initial delays and delay polynomials.
    
    Parameters
    ----------
     params_array_delay_model : list
         information from delay model ini file (see lib_ini_files.py).
     params_array_stations : list
         information from stations ini file (see lib_ini_files.py).
     s_st
         configparser handler for stations ini.
     s_so
         configparser handler for sources ini.
     s_delay
         configparser handler for delays ini.
     mjd_start
         MJD for the start of the scan.
     seconds_ref_in
         seconds for the start of the scan.
     tot_steps
         number of accumulation periods.
     step_seconds
         accumulation period duration [s].
     seconds_offset
         [should be 0, consider removing]
     v
         verbose if 1.
     file_ini
         delays.ini output filename.
     
    Returns
    -------
     s_delay
         configparser handler to delays.ini output filename.
     
    Notes
    -----
    |
    | **TO DO:**
    |
    |  Check seconds_offset, remove.
    """


    [s_delay,pairs_vv] = create_sections_config_delays(s_delay,s_st,s_so)
   


    if DEBUG_GET_DELAY:
        print("gdm "+"mjd_ref".rjust(8)+\
                "s_ref".rjust(8)+\
                "offset".rjust(8)+\
                "mjd[|4]".rjust(13)+\
                "so".rjust(4)+\
                "st".rjust(4)+\
                "clk_ref[|4]".rjust(13)+" "+\
                "clk_ref_d".rjust(13)+\
                "   clk_poly_s")
        print("gdm ".ljust(90)+"d_poly_s".rjust(8))#,end="")


    # Vectors for storing pairs station-source and delays
    v_st_so = []
    
    
    

    # Get EOP values
    mjd_str=str(int(np.round(mjd_start)))
    
    #seconds_ref=int(((mjd_start%1)*(24*60*60))//1)
    seconds_ref_in+=seconds_offset
    
    if v==1:
        print("seconds ref: "+str(seconds_ref_in))


    seconds_ref=seconds_ref_in
    info_polynomials = get_all_polynomials(params_array_delay_model,params_array_stations,s_st,s_so,mjd_start,\
                                                seconds_ref,seconds_offset,tot_steps,step_seconds,v)
    if info_polynomials is None:
        return(None)

    [seconds_inc_v,max_saved,min_saved,v_delays,v_delay_rates,no_offset_total] = info_polynomials

    print("")
    if SHOW_DELAY_INFO:
        print("Delay model:")
        print(" Found delay models for t = "+str(seconds_ref_in)+" + ["+', '.join(map(str,seconds_inc_v))+"] s")
        if no_offset_total>0:
            print(" Offsets applied!")
        #else:
        #    print(" No offsets applied.")
        print("")
        print("Delay summary:")
    

    
    with open(file_ini+"_debug", 'w') as debugfile:
                
        
        dh = print_delays_header(v=SHOW_DELAY_INFO)
        print(dh,file=debugfile)
    
        if v==1:
            print("max delay: "+str(max_saved))
            
        j=-1
        tot_st=len(s_st.sections())
        
    
        seconds_iter=-1
        seconds_ref=seconds_ref_in-1
        for j in range(len(v_delays)):
            #j+=1      
            
            seconds_iter+=1
            
            seconds_ref+=1
            #seconds_inc=i*step_seconds+seconds_offset
            
            seconds_i=seconds_ref_in+j*step_seconds
        
            so_id=-1
            for soj in s_so.sections():  
            
                so_id+=1
                i=-1
                
                for sti in s_st.sections():
                    i+=1
                    
                    if max_saved[j][so_id]==v_delays[j][i]:
                        [poly_delay_max,seconds_from_ref_max,poly_station_clock_max,seconds_diff_clock_max,result_dr_max,result_cr_max,section_in_model_max] = v_delay_rates[j][i]
                        st_max_delay=i
                        
                        
                delta_reference_delay=0
                
                if DEBUG_FINAL_DELAYS:
                    print("Station with max delay is i="+str(st_max_delay))

                
                i=-1
                for sti in s_st.sections():       
                    i+=1
                    #Moved to "compute_delay"
                    #### Create section for configuration file
                    #st_so = get_pair_st_so(s_st.get(sti,'id'),s_so.get(soj,'id'))
                    st_so = pairs_vv[s_st.getint(sti,'id')][s_so.getint(soj,'id')][0]    # See create_sections_config_delays()
                    
                    
                    if v==1:
                        print(sti, " - ", soj)
                        print(st_so)

                    
                    # Correct delay with maximum and add to configuration
                    a_delay = v_delays[j][i]
                    r_delay = max_saved[j][so_id]-v_delays[j][i]
                    
                    
                    d_delay = v_delay_rates[j][i] 
                    [poly_delay,seconds_from_ref,poly_station_clock,seconds_diff_clock,result_dr,result_cr,section_in_model] = d_delay[:]
                    poly_diff = [poly_delay[0]-poly_delay_max[0],\
                                 poly_delay[1]-poly_delay_max[1],\
                                 poly_delay[2]-poly_delay_max[2]]
                    
                    clock_diff = [poly_station_clock[0]-poly_station_clock_max[0],\
                                  poly_station_clock[1]-poly_station_clock_max[1]]
                    


                    seconds_v=np.array([0])
                    seconds_offset = 0
            


                    offset_clock=np.polyval(poly_station_clock_max,seconds_from_ref)
                    offset_clock=0

                    
                    
                    clock_diff_pre=np.multiply(clock_diff[:],-1)
        
                    if SUM_CLK_DIFF:
                        poly_diff[0]+=clock_diff[0]
                        poly_diff[1]+=clock_diff[1]
                   
                    clock_diff=[0,0]
                    clock_abs=[0,0]
                    seconds_diff_clock=0
                    
                    poly_diff=np.multiply(poly_diff,-1)
                

                    initial_offset=0
                    if DIFF_OFFSET:
                        poly_ref_offset = np.copy(poly_delay_max)
                        poly_ref_offset[0] += poly_station_clock_max[0]
                        poly_ref_offset[1] += poly_station_clock_max[1]

                        initial_offset=get_initial_abe(poly_ref_offset,seconds_from_ref,seconds_offset=0)

                    f_delay=initial_offset
                    
                
                   
                
                    # Relative
                    [r_recalc,m_delay,c_delay,rate_mc,acce_mc]=get_delay_val(clock_diff=[0],\
                                       poly_diff=poly_diff,\
                                       seconds_ref_clock=0,\
                                       seconds_ref_poly=seconds_from_ref+initial_offset,\
                                       seconds=seconds_v,\
                                       seconds_offset=offset_clock,\
                                       v=DEBUG_LIB_DELAY_ONLY_HERE)
                    

                    r_delay=r_recalc[0]

                
                    # Display information
                    if i!=st_max_delay:
                        # Change sign for getting a simple line
                        if i==0:
                            dh_mod = print_delays(seconds_i,np.multiply(poly_diff,-1),\
                                          np.multiply(clock_diff_pre,-1),\
                                          seconds_from_ref,seconds_i,v=SHOW_DELAY_INFO)
                        else:
                            dh_mod = print_delays(i,poly_diff,\
                                          clock_diff_pre,\
                                          seconds_from_ref,seconds_i,v=SHOW_DELAY_INFO)
                        print(dh_mod,file=debugfile)
                  

                    #m_delay = -m_delay
                    m_delay = 0.0
                    c_delay = 0.0

                  
                    # Absolute
                    [r_recalc1,m_delay1,c_delay1,rate_mc_u1,acce_mc_u1]=get_delay_val(clock_diff=poly_station_clock,\
                                           poly_diff=poly_delay,\
                                           seconds_ref_clock=0,\
                                           seconds_ref_poly=seconds_from_ref,\
                                           seconds=seconds_v,\
                                           seconds_offset=0,\
                                           v=DEBUG_LIB_DELAY_ONLY_HERE,\
                                           diff_pol=0)



                    # (Reference station absolute)
                    #[r_recalc_max2,m_delay_u2,c_delay_u2,rate_mc_u2,acce_mc_u2]=get_delay_val(\
                    #                       clock_diff=poly_station_clock_max,\
                    #                       poly_diff=poly_delay_max,\
                    #                       seconds_ref_clock=0,\
                    #                      seconds_ref_poly=seconds_from_ref_max,\
                    #                       seconds=seconds_v,\
                    #                       seconds_offset=0,\
                    #                       v=DEBUG_LIB_DELAY_ONLY_HERE,\
                    #                       diff_pol=0)

                    a_delay = -r_recalc1[0]
                              
                    seconds_from_ref_out=seconds_from_ref
                    if DIFF_POLY!=0:
                        seconds_from_ref_out=seconds_from_ref
                    else:
                        poly_diff=np.multiply(poly_delay,1)
                        clock_diff=np.multiply(poly_station_clock,1)
    

                    s_delay = set_config_delay(s_delay,st_so,seconds_i,a_delay,r_delay,f_delay,\
                                     poly_diff,clock_diff,clock_abs,seconds_from_ref_out,\
                                     seconds_diff_clock,m_delay,c_delay,delta_reference_delay,\
                                     section_in_model)
                    

    return(s_delay)       
    




# <codecell>


