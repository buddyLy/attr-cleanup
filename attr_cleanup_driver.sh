#!/bin/ksh

#=============================================================================================================#
#   DESCRIPTION:This is the main script for the attribute cleanup component 
#   ARGUMENTS: Config file path (Eg: script.sh <config path>
#   OUTPUT: Attribute cleanup data are created in hive tables. 
#           Hive mapping entry created in mapping file for every category 
#   DESCRIPTION OF RETURN CODE: None                                                                          #
#   USAGE: sh attr_cleanup_driver.sh /path/to/config/attr_cleanup_finalization.cfg
#=============================================================================================================#
basedir=/u/users/$USER/attr-cleanup
logdir=${basedir}/logs
bindir=${basedir}/bin
cfgdir=${basedir}/config

#source main program with configuration file and output all error message to error log
source ${bindir}/attr_cleanup_finalization.sh "${cfgdir}/attr_cleanup_finalization.cfg" 2>>${logdir}/error_out.log

#main
startProgram 2>>${logdir}/error_out.log
