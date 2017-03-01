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
#File: lib_debug.py.
#Author: A.J. Vazquez Alvarez (ajvazquez@haystack.mit.edu)
#Description: 
"""
Routines for displaying debug information.

"""
#History:
#initial version: 2016.10 ajva
#MIT Haystack Observatory

from __future__ import print_function
import imp

import const_debug
imp.reload(const_debug)
from const_debug import *


    

###########################################
#           Routines Mapper
###########################################   

# DEBUG_ALIGN

def print_debug_m_align_header():
    print("Debugging alignment computations msvf.")
    print("")
    print(" st:    Station")
    print(" id:    Station id (stations.ini)")
    print(" tsf:   Total sample components per channel in frame)")
    print(" t:     Data type: 0 for real, 1 for complex")
    print(" tsff:  Total samples per channel in frame")
    print(" s_fr:  Seconds of frame in VDIF header")
    print(" n_fr:  Number of frame in VDIF header")
    print(" s_adj: Seconds corresponding to the first sample in this frame (based on VDIF header info)")
    print(" r:     Delay corresponding to this frame")
    print(" s_adj_r: Seconds corresponding to the first sample in this frame adjusted with the delay")
    print(" rel_pol: Relative position for this frame into the accumulation period")
    print(" ai:    Accumulation block index (i_front)")
    print(" acc:   Accumulation block index (accu_block)")
    print(" n_fr_ne: Number of frame adjusted with offset due to delay (may be negative)")
    print(" f_fr_adj: Number of frame adjusted with offset due to delay (can not be negative)")
    print(" sh:    Integer shift to be applied in this accumulation period")
    print(" sh_fr: Integer shift to be applied to the first frame of the accumulation period")
    print(" [frac|4]: Fractional sample correction for this accumulation block (showing only 4 decimal digits)")
    print(" pf:    Process frame, 1 if frame will be processed")
    print(" af:    Aligned frame, 0 if it corresponds to previous accumulation block (which has different delay parameters)")
    print(" pp:    Previously printed, number lines printed in previous round")
    print(" [N]:   Number of sample components to be potentially printed in this round")
    print(" [s]:   Accumulation period corresponding to the generated line")
    print(" [ofi]: Offset iterator, offset to the sample components to be taken from the frame")
    print(" [ofs]: Offset signal, offset to the first sample number to be displayed in the geenrated line")
    print(" [cs]:  Chunk size (by default same as N)")
    print(" [acc]: Accumulation period (-1 if samples will be discarded)")
    print(" [sf]: Superframe mode (0 deafult, 1 if grouping samples for multiple frames into one line")
    print(" [sf_id]: Superframe id +1 (incremented before reporting)")
    print("")
    print("zM"+KEY_SEP+"a".ljust(1)+"st".rjust(5)+"id".rjust(5)+\
          "tsf".rjust(7)+\
          "t".rjust(3)+\
          "tsff".rjust(7)+\
          "s_fr".rjust(10)+"n_fr".rjust(6)+\
          "s_adj".rjust(15)+\
          "r".rjust(20)+\
          "s_adj_r".rjust(17)+\
          "rel_pos".rjust(10)+\
          "ai".rjust(5)+\
          "acc".rjust(10)+\
          "n_fr_ne".rjust(10)+\
          "n_fr_adj".rjust(10)+\
          "sh".rjust(10)+"sh_fr".rjust(6)+\
          "[fra|4]".rjust(10)+\
          "pf".rjust(3)+\
          "af".rjust(3)+\
          "pp".rjust(3)+\
          "[N]".rjust(12)+\
          "[s]".rjust(8)+\
          "[ofi]".rjust(8)+\
          "[ofs]".rjust(8)+\
          "[cs]".rjust(8)+\
          "[acc]".rjust(8)+\
          "[sf]".rjust(6)+\
          "[sf_id]".rjust(6))
    # pp is for previous prints


def print_debug_m_align_no_end(station_name,station_id,tot_samples_per_channel_and_frame,data_type,tot_samples_per_channel_and_frame_full,\
                             seconds_fr,frame_num,adjusted_frame_time,delay,actual_frame_time,rel_pos_frame,i_front,accu_block,\
                             frame_num_adjusted_neg,frame_num_adjusted,shift_int,adjusted_shift_inside_frame,fractional_sample_delay,\
                             process_frame,aligned_frame,count_print):
                                                 
                                                 
    print("zM"+KEY_SEP+"a".ljust(1)+station_name.rjust(5)+str(station_id).rjust(5)+\
          str(tot_samples_per_channel_and_frame).rjust(7)+\
          str(data_type).rjust(3)+\
          str(tot_samples_per_channel_and_frame_full).rjust(7)+\
          str(seconds_fr).rjust(10)+str(frame_num).rjust(6)+\
          str("{0:.7f}".format(adjusted_frame_time)).rjust(15)+\
          str(delay).rjust(20)+\
          str("{0:.7f}".format(actual_frame_time)).rjust(17)+\
          str(rel_pos_frame).rjust(10)+\
          str(i_front).rjust(5)+\
          str(accu_block).rjust(10)+\
          str(frame_num_adjusted_neg).rjust(10)+\
          str(frame_num_adjusted).rjust(10)+\
          str(shift_int).rjust(10)+str(adjusted_shift_inside_frame).rjust(6)+\
          str("{0:.4f}".format(fractional_sample_delay)).rjust(10)+\
          str(process_frame).rjust(3)+\
          str(aligned_frame).rjust(3)+\
          str(count_print).rjust(4),end="")

def print_debug_m_align_last(tot_samples_v,seconds_v,offset_first_sample_iterator_v,offset_first_sample_signal_v,\
                                              chunk_size_v,acc_v,do_sup_frame,sup_frame_id):
                
    print(str(tot_samples_v).rjust(12)+str(seconds_v).rjust(8)+\
          str(offset_first_sample_iterator_v).rjust(8)+\
          str(offset_first_sample_signal_v).rjust(8)+\
          str(chunk_size_v).rjust(8)+str(acc_v).rjust(8)+\
          str(do_sup_frame).rjust(6)+\
          str(sup_frame_id).rjust(6))

def print_sup_frame(sup_frame_id):
    print("".rjust(58)+str(sup_frame_id).rjust(6))

###########################################
#           Routines Reducer
###########################################   

# DEBUG_DELAYS

def print_debug_r_delays_header():
    print(" Debugging delay computations rsvf.")
    print("")
    print(" d/f:     Fringe rotation / fractional sample correction")
    print(" i:       Index to F1 (new samples)")
    print(" ref:     Index to F1_partial (stored samples)")
    print(" sp:      Station.polarization")
    print(" s_id:    First sample number")
    print(" N:       Number of samples")
    print(" n:       Length of timescale for calling compute_delay")
    print(" 0:       First element of the timescale for calling compute_delay [s]")
    print(" -1:      First element of the timescale for calling compute_delay [s]")
    print(" s_off:   Time corresponding to the first sample [s]")
    print(" r[0][s]: Delay for first sample")
    print(" r[-1][s]:Delay for last sample")
    print(" delta_r: Rate for this interval (linear)")
    print(" fra_ini: Fractional sample correction (first one should match that in delays.ini)")
    print(" fra_now_full: Full fractional sample correction (including shift if applicable)")
    print(" fra_now: Fractional sample correction (between -0.5 and +0.5)")
    print(" diff:    Differential delay between acc periods")
    print(" C:       Computed (will be zero if taken rotation vectors from previous iteration)")
    print(" R:       Rotated (will be zero if no rotation has been applied)")
    print("")
    print("zR"+KEY_SEP+"d/f"+"i".rjust(5)+"ref".rjust(8)+"sp".rjust(8)+"s_id".rjust(8)+"N".rjust(10)+"n".rjust(10)+"0".rjust(16)+\
          "-1".rjust(10)+"s_off".rjust(20)+str("r[0] [s]").rjust(20)+str("r[-1] [s]").rjust(20)+\
          str("delta_r").rjust(20)+str("fra_ini").rjust(20)+str("fra_now_full").rjust(20)+str("fra_now").rjust(20)+str("diff").rjust(15)+str("C").rjust(3)+str("R").rjust(3))


def print_debug_r_delays_d(i,F_refs_row,F_ind_row,first_sample,n_samples,timescale,timescale_0,timescale_last,\
                                     seconds_offset,first_delay,last_delay,rate_interval,fractional_sample_correction,\
                                     diff_frac,computed,nr):
    """
    Fringe rotation delay computation debugging information.
    """
    print("zR"+KEY_SEP+"d  "+str(i).rjust(5)+str(F_refs_row).rjust(8)+F_ind_row.rjust(8)+\
          str(first_sample).rjust(10)+str(n_samples).rjust(10)+str(len(timescale)).rjust(10)+str(timescale_0).rjust(16)+\
          str(timescale_last).rjust(10)+\
          str(seconds_offset).rjust(20)+\
          str(first_delay).rjust(20)+\
          str(last_delay).rjust(20)+\
          str(rate_interval).rjust(20)+\
          str(fractional_sample_correction).rjust(20)+"".rjust(40)+\
          str(diff_frac).rjust(15)+str(computed).rjust(3)+str(int(not(nr))).rjust(3))

def print_debug_r_delays_f(stpol,F_refs_row,F_ind_row,fsr,num_samples,len_timescale,total_timescale_row,timescale_empty,\
                                     total_seconds_offset,r_recalc_row,r_unused,a_unused,fractional_sample_correction,\
                                     full_fractional_recalc_row,fractional_recalc_row,diff_frac):
    """
    Fractional sample correction delay computation debugging information.
    """
    print("zR"+KEY_SEP+"f  "+str(stpol).rjust(5)+str(F_refs_row).rjust(8)+F_ind_row.rjust(8)+\
          str(fsr).rjust(10)+str(num_samples).rjust(10)+str(len_timescale).rjust(10)+\
          str(total_timescale_row).rjust(16)+\
          str(timescale_empty).rjust(10)+\
          str(total_seconds_offset).rjust(20)+\
          str(r_recalc_row).rjust(20)+\
          str(r_unused).rjust(20)+\
          str(a_unused).rjust(20)+str(fractional_sample_correction).rjust(20)+\
          str(full_fractional_recalc_row).rjust(20)+\
          str(fractional_recalc_row).rjust(20)+\
          str(diff_frac).rjust(15))






# DEBUG_HSTACK

def print_debug_r_hstack_header():
    print(" Debugging hstack rsvf.")
    print("")
    print(" pc/f:         Phase calibration / FX processing")
    print(" ip:           Index to F1_partial (stored samples)")
    print(" Fip[ip]:      Station.polarization in F1_partial, input of the function")
    print(" Fipo[ip]:     Station.polarization in F1_partial, output of the function")
    print(" i:            Index to F_ind")
    print(" Fi[i]:        Station.polarization in F_ind")
    print(" hstack line:  number of samples in F1_partial joined to number of samples in F1, totals and stats where")
    print("                F_lti: list with last sample (l), total number of samples processed (t), invalid samples (i), and")
    print("                adjuted samples for each stream.")
    print("")
    print("zR"+KEY_SEP+"pc/f".ljust(5)+"ip".rjust(10)+\
          "Fip[ip]".rjust(10)+\
          "Fipo[ip]".rjust(10)+\
          "i".rjust(10)+"Fi[i]".rjust(10))

def print_debug_r_hstack(mode_str,idx_p,F_ind_partial_row,F_ind_partial_out_row,i,F_ind_row_i):
    if F_ind_partial_row is None:
        F_ind_partial_row="N/A"
    print("zR"+KEY_SEP+mode_str.ljust(5)+str(idx_p).rjust(10)+F_ind_partial_row.rjust(10)+str(F_ind_partial_out_row).rjust(10)+\
                      str(i).rjust(10)+str(F_ind_row_i).rjust(10))

def print_debug_r_hstack_arrow(F1_partial,F1,F1_partial_out,F_lti_out):
    print("zR"+KEY_SEP+"hstack: "+\
              " F1p ("+','.join(map(str,[len(i) for i in F1_partial]))+") U F1 ("+\
              ','.join(map(str,[len(i) for i in F1]))+") -> "+
              ','.join(map(str,[len(i) for i in F1_partial_out]))+" with F_lti="+str(map(list,F_lti_out)))
    
def print_debug_r_hstack_separator():
    print("zR"+KEY_SEP+"------------------------------")



#DEBUG_FRAC_OVER

def print_debug_r_frac_over_header():
    print("zR"+KEY_SEP+"o/a/j".ljust(5)+"l_sta".rjust(10)+"l_adj".rjust(10)+"ref".rjust(15)+"sp".rjust(8)+"s_id".rjust(8)+"N".rjust(10)+\
          "n".rjust(10)+"ff".rjust(70)+"fi".rjust(15))
    
    
    
    
def print_key(key_pair_accu):
    """
    Print key for the line processed for logging.
    """
    print("zR"+KEY_SEP)
    print("zR"+KEY_SEP+">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    print("zR"+KEY_SEP+"         "+key_pair_accu+"         ")
    print("zR"+KEY_SEP+">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    print("zR"+KEY_SEP)

