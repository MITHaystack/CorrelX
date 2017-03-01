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
#File: const_ini_files.py.
#Author: A.J. Vazquez Alvarez (ajvazquez@haystack.mit.edu)
#Description:
"""
 Constants with parameter names for configuration (.ini) files.

"""
#History:
#initial version: 2015.12 ajva
#MIT Haystack Observatory


# Separators for the serialization
SEPARATOR_VECTOR=";"
SEPARATOR_ELEMENTS=","
SEPARATOR_PARAM_VAL="="
SEP_VALUES=":"
# E.g.: 
#channels,CH2=1,CH1=0;polarizations,L=0,R=1;VDF-0.vt,polarizations=L:R:L:R,station=At,channels=CH1:CH1:CH2:CH2;VDF-1.vt,polarizations=L:R:L:R,station=Ho,channels=CH1:CH1:CH2:CH2;VDF-2.vt,polarizations=L:R:L:R,station=Mp,channels=CH1:CH1:CH2:CH2
#[['channels', 'CH2=1', 'CH1=0'], ['polarizations', 'L=0', 'R=1'], ['VDF-0.vt', 'polarizations=L:R:L:R', 'station=At', 'channels=CH1:CH1:CH2:CH2'], ['VDF-1.vt', 'polarizations=L:R:L:R', 'station=Ho', 'channels=CH1:CH1:CH2:CH2'], ['VDF-2.vt', 'polarizations=L:R:L:R', 'station=Mp', 'channels=CH1:CH1:CH2:CH2']]


# Separators in ini file
INI_SEP=" = "    # parameter-value separator
INI_HF="["       # first char in .ini header
INI_HL="]"       # last char in .ini header
INI_SUB="-"      # Separator between subfields for delay_model header
INI_VEC=":"      # Separator vector in .ini


#-----------------------------------
# Stations (station.ini)
#
#    [At]
#    id = 0
#    x = -4752008.07205
#    y = 2791332.20178
#    z = -3200197.64436
#
#  Identifier (integer starting in 0). Note that in the correlation configuration file a maximum number of stations will be
#  specified, so that stations from 0 to that number minus one will be involved in the correlation.
C_INI_ST_ID = 'id'
# Position (x,y,z). e.g. for 'x': -3950237.16050
C_INI_ST_X = 'x'
C_INI_ST_Y = 'y'
C_INI_ST_Z = 'z'
C_INI_ST_CLOCK_REF = 'clock_ref'
C_INI_ST_CLOCK_POLY = 'clock_poly_us'
C_INI_ST_CLOCK_OFFSET = 'clock_offset_us'

#-----------------------------------
# EOP (eop.ini)
# Times
C_INI_EOP_TAI = 'TAI_UTC'
C_INI_EOP_UT1 = 'UT1_UTC'


#-----------------------------------
# Correlation (correlation.ini)
#
#    [elements]
#    autocorr_station = no
#    cross_polarization = no
#    stations = 2
#    
#    [computation]
#    FFT = 256
#    accumulation = 2
#    
#    [times]
#    mjd_start = 54626
#    seconds_start = 5280
#    seconds_duration = 3
#
#    [delays]
#    recompute = yes
#    seconds_step = 1
#

C_INI_CR_S_ELEMENTS = 'elements'                #   Elements in the correlation
C_INI_CR_STATIONS = 'stations'                  #   Stations (number of stations). If =x, then stations [0,1,...,x-2,x-1] 
#                                               #      is the set of stations for the correlation.
C_INI_CR_AUTO_ST = 'autocorr_station'           #   Autocorrelations for same stations (no/yes)
C_INI_CR_CROSS_POL = 'cross_polarization'       #   Correlate different polarizations for same band (no/yes)
C_INI_CR_S_COMP ='computation'                  #  Parameters affecting the computation
C_INI_CR_FFT = 'FFT'                            #  FFT size, e.g. 128, 40960 ...
C_INI_CR_ACC = 'accumulation'                   #  Accumulation time [s], e.g. 2
C_INI_CR_WINDOW = 'window'                      #  Windowing (square, hanning, ...)
C_INI_CR_WINDOW_SQUARE = 'square'
C_INI_CR_WINDOW_HANNING = 'hanning'
C_INI_CR_PC = 'phase_calibration'               #  Phase calibration (yes or no)
C_INI_CR_S_TIMES = 'times'                      #  Parameters for start and end times [s]
C_INI_CR_MJD = 'mjd_start'
C_INI_CR_START = 'seconds_start'
C_INI_CR_DURATION = 'seconds_duration'
C_INI_CR_FIRST_FRAME = 'first_frame'            #  First frame number to be processed [default -1 so no limit]
C_INI_CR_NUM_FRAMES = 'num_frames'              #  Number of frames for same second [default -1 so no limit]
C_INI_CR_ITER_ACC_GROUPS = 'iter_acc_groups'    #  TO DO: delete, no longer used
C_INI_CR_S_DELAYS = 'delays'                    #  Parameters for delay computation
C_INI_CR_MODEL = 'model'                        #  TO DO: delete, no longer used
C_INI_CR_M_SIMPLE = 'simple'                    #  TO DO: delete, no longer used
C_INI_CR_M_FILE = 'file'                        #  TO DO: delete, no longer used
C_INI_CR_RECOMPUTE = 'recompute'                #  TO DO: delete, no longer used
C_INI_CR_STEP = 'seconds_step'                  #  TO DO: delete, no longer used



#-----------------------------------
# Sources (sources.ini)
#   Example of sources.ini file:
#
#    [0208-512]
#    id = 0
#    dec = -51:1:1.891780
#    ra = 2:10:46.2004270
#
C_INI_SRC_ID = 'id'
# e.g.: 2:10:46.2004270
C_INI_SRC_RA = 'ra'
C_INI_SRC_DEC = 'dec'

    
#-----------------------------------
# Media (media.ini)
#   Example of media.ini file:
#
#    [channels]
#    CH2 = 1
#    CH1 = 0
#
#    [polarizations]
#    L = 0
#    R = 1
#
#    [VDF-0.vt]
#    polarizations = L:R:L:R
#    channels = CH1:CH1:CH2:CH2
#    station = At
#    format = VDIF
#    version = custom
#    framebytes = 0
#
#

C_INI_MEDIA_S_CHANNELS = 'channels'           #  Mapping between channels and integers 0, 1, 2, ...
C_INI_MEDIA_S_POLARIZATIONS = 'polarizations' #  Mapping between polarizations and integers 0, 1, 2, ...
C_INI_MEDIA_S_FILES = 'files'                 #  Files to be included in the correlation (if all, all are gathered)
C_INI_MEDIA_LIST = 'list'
#C_INI_MEDIA_LIST_VAL_ALL = 'all'
C_INI_MEDIA_STATION = 'station'
C_INI_MEDIA_CHANNELS = 'channels'             #  Channels in the frame. E.g.: 'CH1:CH1:CH2:CH2' 
C_INI_MEDIA_CH = 'CH'                         #  Channel str
C_INI_MEDIA_ZF = 'ZF'                         #  Zoom str
C_INI_MEDIA_ZB = 'ZB'                         #  Zoom bw str
C_INI_MEDIA_POLARIZATIONS = 'polarizations'   #  Polarizations in the frame. E.g.: 'L:R:L:R'
C_INI_MEDIA_PCALS = 'i_pcal'                  #  Phase calibration indices in frames. E.g.: 'P14:05' that 
#                                             #    correspond to 1 and 4; and to 0 and 5
C_INI_MEDIA_FRAMEBYTES = 'framebytes'         #  Bytes per frame (this is to force frame readers to take a fixed 
#                                             #    frame length). If zero, this value is read from the frame header.
C_INI_MEDIA_FREQ_SAMPLE = 'f_sample'          # Sampling frequency (for calculating shifts based on delays)
C_INI_MEDIA_FORMAT = 'format'                 #  Format of the frames
C_INI_MEDIA_F_VDIF = 'VDIF'
C_INI_MEDIA_VERSION = 'version'               #  Version of the format of the frames
C_INI_MEDIA_V_CUSTOM = 'custom'
C_INI_MEDIA_COMPRESSION = 'compression'       #  Compression scheme
C_INI_MEDIA_C_VQ = 'vq'
C_INI_MEDIA_C_NO = 'no'
C_INI_MEDIA_C_CODECS = 'codecs'
C_INI_MEDIA_BITS_SAMPLE = 'bits_sample'
C_INI_MEDIA_F_PCAL = 'f_pcal'                 #  Frequency for phase calibration [Hz]
C_INI_MEDIA_O_PCAL = 'o_pcal'                 #  Offset for phase calibration [Hz]
C_INI_MEDIA_FREQUENCIES = 'frequencies'       #  Frequency of the edge of the band (upper edge if LSB, lower edge if USB)
C_INI_MEDIA_BANDWIDTHS = 'bandwidths'
C_INI_MEDIA_SIDEBANDS = 'sidebands'           #  L (for LSB) or U (for USB)
C_INI_MEDIA_ZOOM_FREQ = 'zoom_freq'           #  Zoom frequencies post-processing
C_INI_MEDIA_ZOOM_POST = 'zoom_post'
C_INI_MEDIA_ZP_FREQ = 'zoom_freq'
C_INI_MEDIA_ZP_BW = 'zoom_bw'




#-----------------------------------
# Dela model (delay_model.ini)
#   Example of delay_model.ini file:
#
###[mjd,seconds_start_interval,seconds_end_interval,source_id,station_id]
#[57350-68160-68279-so0-st0]
#delay_us = 1.4462836897700863e+04:-1.9265480065495619e-01:-4.1002719262484822e-05:1.7074065069883762e-10:1.8126332624884168e-14:1.0089475440522877e-19
#dry_us = 1.1214812228789390e-02:1.4866345298454206e-07:3.3605540194332499e-11:7.3080821025852112e-16:8.8428017539174689e-20:2.6832876710214920e-24
#wet_us = 5.4786130476330787e-04:7.2828971090686533e-09:1.6467123068458460e-12:3.5986390836540993e-17:4.3580776480308735e-21:1.3230351086598679e-25
#

C_INI_MODEL_DELAY = 'delay_us'  # polynomial for total delay [microseconds]
C_INI_MODEL_DRY = 'dry_us'      # polynomial for dry atmosphere delay [microseconds]
C_INI_MODEL_WET = 'wet_us'      # polynomial for wet atmosphere delay [microseconds]


# Markers for delays.ini [generated automatically]

DELAY_MODEL_ABS_MARKER="a."     # absolute delay (model+clock)
DELAY_MODEL_REL_MARKER="r."     # relative delay to reference station (model+clock)
DELAY_MODEL_REF_MARKER="f."     # reference delay
DELAY_MODEL_RR0_MARKER="d0."    # delay poly model 0 order
DELAY_MODEL_RR1_MARKER="d1."    # delay poly model 1 order
DELAY_MODEL_RR2_MARKER="d2."    # delay poly model 2 order
DELAY_MODEL_RRR_MARKER="dr."    # time from reference time for the delay model poly
DELAY_MODEL_RC0_MARKER="c0."    # clock poly 0 order
DELAY_MODEL_RC1_MARKER="c1."    # clock poly 1 order
DELAY_MODEL_RCR_MARKER="cr."    # time from reference time for the clock poly
DELAY_MODEL_RCM_MARKER="m."     # model only delay for the first sample 
DELAY_MODEL_RCC_MARKER="c."     # clock only delay for the first sample
DELAY_MODEL_ZC0_MARKER="z0."    # reference clock 0 order
DELAY_MODEL_ZC1_MARKER="z1."    # reference clock 0 order
DELAY_MODEL_DDD_MARKER="dd."    # delta reference delay for ref station
DELAY_MODEL_DI_MARKER="di."     # TO DO: DELETE? delay info (aiming to avoid overhead)
DELAY_MODEL_SIM_MARKER="si."    # header in delay model (for debugging)


