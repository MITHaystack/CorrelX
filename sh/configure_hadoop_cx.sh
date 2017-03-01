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
# File: configure_hadoop_cx.sh.
# Author: A.J. Vazquez Alvarez (ajvazquez@haystack.mit.edu)
# Description:
#
#    Script for configuration Hadoop initialization scripts and updating paths in CorrelX configuration file.
#    The script is launched with no arguments, it will ask for confirmation on the found paths and apply the configuration.
#
#    Procedure:
#    ----------
#     1. Replace Hadoop and Java home variables in hadoop-env.sh and yarn-env.sh.
#     2. Modify scripts hadoop-daemon.sh, yarn-daemon.sh, mr-jobhistory-daemon.sh (terminate previous instances).
#     3. Copy Hadoop configuration files to CorrelX templates folder.
#     4. Update Hadoop path in correlx.ini.
#
#    Assumptions:
#    ------------
#     This script is intended to be use with Hadoop 2.7.3, it will need to be checked for other versions.
#
#History:
#initial version: 2016.12 ajva
#MIT Haystack Observatory
#
echo "Hadoop 2.7.3 for CorrelX: configuration"

# Replace Hadoop and Java home variables in hadoop-env.sh

FILE=`find ~ -name "lib_mapredcorr.py"|head -n1`
DIR=`readlink -f $FILE`
DIR=`dirname $DIR`
CDIR=`readlink -f $DIR/..`
echo ""
echo "CorrelX folder found:" $CDIR
echo "Enter to confirm, new path to override: [Enter]"
read TIN
if [ -n "$TIN" ]; then
 CDIR=$TIN
fi
echo "Using Correlx dir: "$CDIR
echo ""

# Hadoop folder
FILE=`find ~ -name "hadoop-mapreduce-examples*"|head -n1`
DIR=`readlink -f $FILE`
DIR=`dirname $DIR`
HDIR=`readlink -f $DIR/../../..`
echo ""
echo "Hadoop folder found:" $HDIR
echo "Enter to confirm, new path to override: [Enter]"
read TIN
if [ -n "$TIN" ]; then
 HDIR=$TIN
fi
echo "Using Hadoop dir: "$HDIR
echo ""

# Java folder
JDIR=`dirname $(dirname $(readlink -f $(which javac)))`
echo ""
echo "Java folder found:" $JDIR
echo "Enter to confirm, new path to override: [Enter]"
read TIN
if [ -n "$TIN" ]; then
 JDIR=$TIN
fi
echo "Using Java dir: "$JDIR
echo ""

echo "Configuring environment scripts..."
# hadoop-env.sh
HLINE="export HADOOP_HOME="$HDIR
JLINE="export JAVA_HOME="$JDIR
CLINE="export HADOOP_CONF_DIR"
sed -i "/export JAVA_HOME=/d" $HDIR/etc/hadoop/hadoop-env.sh
sed -i "/export HADOOP_HOME=/d" $HDIR/etc/hadoop/hadoop-env.sh
sed -i "/export HADOOP_CONF_DIR=/d" $HDIR/etc/hadoop/hadoop-env.sh
sed -i "/java implementation to use/a $CLINE" $HDIR/etc/hadoop/hadoop-env.sh
sed -i "/java implementation to use/a $HLINE" $HDIR/etc/hadoop/hadoop-env.sh
sed -i "/java implementation to use/a $JLINE" $HDIR/etc/hadoop/hadoop-env.sh


# Add host.name variable in yarn-env.sh
# yarn-env.sh
SLINE="HOSTNAME_VAR=\`hostname -s\`"
NLINE="YARN_OPTS=\"\$YARN_OPTS -Dhost.name=\$HOSTNAME_VAR\""
sed -i "/$SLINE/d" $HDIR/etc/hadoop/yarn-env.sh
sed -i "/$NLINE/d" $HDIR/etc/hadoop/yarn-env.sh
sed -i "/YARN_OPTS -Dhadoop.log.dir=/i $SLINE" $HDIR/etc/hadoop/yarn-env.sh
sed -i "/YARN_OPTS -Dhadoop.log.dir=/i $NLINE" $HDIR/etc/hadoop/yarn-env.sh

echo "Configuring initialization scripts..."
# Terminate previous instances of Hadop entities
# sbin
OLINEA="        echo Overriding: forcing termination and continuing execution! \#mod_inst"
OLINEB="        kill -9 \`cat \$pid\` \#mod_inst"
for FILE in "hadoop-daemon" "yarn-daemon" "mr-jobhistory-daemon"; do
 sed -i "/\#mod_inst/d" $HDIR/sbin/$FILE.sh
 sed -i '/Stop it first/,+1 s/^/#/' $HDIR/sbin/$FILE.sh
 sed -i "/Stop it first/i $OLINEA"  $HDIR/sbin/$FILE.sh
 sed -i "/Stop it first/i $OLINEB"  $HDIR/sbin/$FILE.sh
done

# Configuration files (templates)
echo "Copying Hadoop configuration files to templates folder: correlx/templates/hadoop_config_files"
mkdir -p $CDIR/templates/hadoop_config_files/
cp $HDIR/etc/hadoop/* $CDIR/templates/hadoop_config_files/

# Configuration file (correlx.ini)
echo "Updating Hadoop path in CorrelX configuration..."
HLINE="Hadoop directory:               "$HDIR
sed -i "s|Hadoop directory:.*|$HLINE|" $CDIR/conf/correlx.ini

echo "Done"
