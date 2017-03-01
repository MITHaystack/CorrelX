#!/bin/bash
#
# The MIT CorrelX Correlator
#
# https://github.com/MITHaystack/CorrelX
# Contact: correlX@haystack.mit.edu
# Project leads: Victor Pankratius, Pedro Elosegui Project developer: A.J. Vazquez Alvarez
#
# Copyright 2017 MIT Haystack Observatory
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
#
#----------------------
#----------------------
# Project: CorrelX.
# File: rel_paths.sh.
# Author: A.J. Vazquez Alvarez (ajvazquez@haystack.mit.edu)
# Description:
#
#    Replace absolute paths in .calc and .input by relative path to current folder
#
#    Arguments:
#    ----------
#     $1:  experiment id (e.g. h_1000 for processing files h_1000.*)
#
#History:
#initial version: 2017.02 ajva
#MIT Haystack Observatory
#
# new path
NEWPATH="./"

# .calc
CALCF=$1".calc"
VEXSTR="VEX FILE:           "
IMSTR="IM FILENAME:        "
FLAGSTR="FLAG FILENAME:      "

# .input
INPUTF=$1".input"
CALCSTR="CALC FILENAME:      "
CORESTR="CORE CONF FILENAME: "
OUTPUTSTR="OUTPUT FILENAME:    "


replace_field(){
  #echo "$*"
  MODF=`echo "$*"|cut -d% -f1`
  MODSTR=`echo "$*"|cut -d% -f2`
  FIELDL=`cat $MODF|grep "$MODSTR"`
  FIELDF=$NEWPATH`echo $FIELDL|rev|cut -d\/ -f1|rev`
  sed -i "s|$MODSTR.*|$MODSTR$FIELDF|" $MODF
  #echo " $FIELDL    -->    $FIELDF"
  echo " `cat $MODF|grep "$MODSTR"`"
}


if [ "$#" -ne 1 ]; then
    echo "Usage: ./rel_path.sh <experiment_id>"
else

echo ""
echo "Paths found:"
echo ""
echo $CALCF
echo " `cat $CALCF|grep "$VEXSTR"`"
echo " `cat $CALCF|grep "$IMSTR"`"
echo " `cat $CALCF|grep "$FLAGSTR"`"
echo $INPUTF
echo " `cat $INPUTF|grep "$CALCSTR"`"
echo " `cat $INPUTF|grep "$CORESTR"`"
echo " `cat $INPUTF|grep "$OUTPUTSTR"`"
echo ""
echo "New path:"$NEWPATH
read -n1 -r -p "Press any key to continue... (Ctrl+C to cancel)" key

echo ""
echo "Updating "$CALCF
replace_field "$CALCF%$VEXSTR%"
replace_field "$CALCF%$IMSTR%"
replace_field "$CALCF%$FLAGSTR%"
echo "Updating "$INPUTF
replace_field "$INPUTF%$CALCSTR%"
replace_field "$INPUTF%$CORESTR%"
replace_field "$INPUTF%$OUTPUTSTR%"
echo ""
echo "Unchanged paths: in $INPUTF"
cat $INPUTF|grep "FILE "
echo ""
echo "Done."
fi
