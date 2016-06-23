#!/bin/ksh

#=============================================================================================================#
#   DESCRIPTION:This is the main script for the attribute cleanup component 
#   ARGUMENTS: Config file path (Eg: script.sh <config path>
#   OUTPUT: Attribute cleanup data are created in hive tables. 
#           Hive mapping entry created in mapping file for every category 
#   DESCRIPTION OF RETURN CODE: None                                                                          #
#   USAGE: sh attr_cleanup_main.sh /path/to/config/attr_cleanup.cfg
#=============================================================================================================#

#check all the configuration variables, exit it any of the required variables is missing
function check_config_variable
{
	#-----------------------------directory structure--------------------
	[[ -n "${base_dir_local}" ]] || error_exit "$LINENO: Config variable not set. rc $?"
	[[ -n "${base_dir_hdfs}" ]] || error_exit "$LINENO: Config variable not set. rc $?"
	[[ -n "${bin_dir}" ]] || error_exit "$LINENO: Config variable not set. rc $?"
	[[ -n "${data_dir}" ]] || error_exit "$LINENO: Config variable not set. rc $?"
	[[ -n "${data_dir_hdfs_input}" ]] || error_exit "$LINENO: Config variable not set. rc $?"
	[[ -n "${data_dir_hdfs_output}" ]] || error_exit "$LINENO: Config variable not set. rc $?"
	[[ -n "${logdir}" ]] || error_exit "$LINENO: Config variable not set. rc $?"

	#----------------------------program files------------------
	[[ -n "${hdfs_output_file}" ]] || error_exit "$LINENO: Config variable not set. rc $?"
	[[ -n "${output_file_from_hdfs}" ]] || error_exit "$LINENO: Config variable not set. rc $?"
	[[ -n "${transpose_input}" ]] || error_exit "$LINENO: Config variable not set. rc $?"
	[[ -n "${transpose_output}" ]] || error_exit "$LINENO: Config variable not set. rc $?"
	[[ -n "${attr_cleansed_file}" ]] || error_exit "$LINENO: Config variable not set. rc $?"
	[[ -n "${attr_cleansed_file_wo_header}" ]] || error_exit "$LINENO: Config variable not set. rc $?"
	[[ -n "${attr_summ_file}" ]] || error_exit "$LINENO: Config variable not set. rc $?"
	[[ -n "${attr_header_file}" ]] || error_exit "$LINENO: Config variable not set. rc $?"
	[[ -n "${attr_mapping_file}" ]] || error_exit "$LINENO: Config variable not set. rc $?"
	[[ -n "${log_file}" ]] || error_exit "$LINENO: Config variable not set. rc $?"

	#----------------------------db variables------------------
	[[ -n "${dtm_schema}" ]] || error_exit "$LINENO: Config variable not set. rc $?"
	[[ -n "${dtm_table_base}" ]] || error_exit "$LINENO: Config variable not set. rc $?"

	#----program constants--------------
	[[ -n "${TRUE}" ]] || error_exit "$LINENO: Config variable not set. rc $?"
	[[ -n "${FALSE}" ]] || error_exit "$LINENO: Config variable not set. rc $?"

	#----------------------------program variables------------------
	[[ -n "${retail_channel_code}" ]] || error_exit "$LINENO: Config variable not set. rc $?"
	[[ -n "${category_nbr}" ]] || error_exit "$LINENO: Config variable not set. rc $?"
	[[ -n "${dept_nbr}" ]] || error_exit "$LINENO: Config variable not set. rc $?"
	[[ -n "${week_id}" ]] || error_exit "$LINENO: Config variable not set. rc $?"
	[[ -n "${mapred_timeout}" ]] || error_exit "$LINENO: Config variable not set. rc $?"
	[[ -n "${log_retention}" ]] || error_exit "$LINENO: Config variable not set. rc $?"
	[[ -n "${support_email}" ]] || error_exit "$LINENO: Config variable not set. rc $?"
	[[ -n "${debug_mode}" ]] || error_exit "$LINENO: Config variable not set. rc $?"
	[[ -n "${trace_mode}" ]] || error_exit "$LINENO: Config variable not set. rc $?"

	#----------------------------program executables------------------
	[[ -n "${hadoop_stream_jar}" ]] || error_exit "$LINENO: Config variable not set. rc $?"
	[[ -n "${atribute_summary_jar}" ]] || error_exit "$LINENO: Config variable not set. rc $?"
	[[ -n "${attr_cleanup_mapper}" ]] || error_exit "$LINENO: Config variable not set. rc $?"
	[[ -n "${attr_cleanup_reducer}" ]] || error_exit "$LINENO: Config variable not set. rc $?"
	[[ -n "${generate_map_file}" ]] || error_exit "$LINENO: Config variable not set. rc $?"
	[[ -n "${columns_to_rows}" ]] || error_exit "$LINENO: Config variable not set. rc $?"
	[[ -n "${load_attribute_cleanup}" ]] || error_exit "$LINENO: Config variable not set. rc $?"
	[[ -n "${load_map_file}" ]] || error_exit "$LINENO: Config variable not set. rc $?"
}

#get the current date
function get_current_date
{
	mydate=$(date +"%Y-%m-%d %H:%M:%S")	
}

function alert_me
{
	#msg=$1
	servername=$(hostname)
	echo "$*" | mailx -s "${progname}: ${servername}: Error in attribute cleanup main script" ${support_email}
}

function log_me
{
	get_current_date
	msg=$1
	logmode=$2 #0=log everything, 1=debug statements only 
		
	#if log msg is for debugging and debug mode is turned on then log, else log everything else
	if [[ $logmode -eq 1 ]]; then
		if [[ $debug_mode -eq 1 ]]; then
			echo "${mydate}: $$: ${msg}" >> ${logdir}/${log_file}
		fi
	else
		echo "${mydate}: $$: ${msg}" >> ${logdir}/${log_file}
	fi
}

#error exit due to fatal error
function error_exit
{
	#this use parameter expansion, if $1 is Unknown error is substituted
	#echo "${progname}: ${1:-"Unknown Error"}" 1>&2
	echo "${1:-"Unknown Error"}" 2>&1
	#echo "${progname}: ${1:-"Unknown Error"}" >> ${logdir}/${log_file} 2>&1
	log_me "${1:-"Unknown Error"}" "0"
	alert_me "$progname: ${1:-"Unknown Error"}" 
	exit 1
	#usage #cd /error_dir || error_exit "$LINENO: An error has occurred."
}

#check return code
function check_return_code
{
	return_code=$1
	msg=$2
	if [[ $return_code -ne 0 ]];then
		alert_me $msg
	fi
}

# keep the last x number of log statements
function retain_lastx_log_size
{
    tail -${log_retention} ${logdir}/${log_file} > ${logdir}/${log_file}.tmp
    cp ${logdir}/${log_file}.tmp ${logdir}/${log_file}
    rm ${logdir}/${log_file}.tmp
}

function cleanup
{
	retain_lastx_log_size
}

#check to see if required free disk space is sufficient 
function has_enough_space
{
	required_space=$1
	shift
	the_mount=$1
	
	if [[ $required_space == "" || $the_mount == "" ]];then
		log_me "$LINENO:Incorrect amount of parameters passed in" "0"
		return ${FALSE}
	else
		log_me "$LINENO:Checking required space: $required_space on mount: $the_mount" "1"
	fi
	
	len=${#required_space}
	one=1 #space of the unit
	lminus=$((len - one))
	
	#get the unit and number
	inputunit=${required_space:$lminus:1}
	inputnum=${required_space:0:$lminus}
	
	mult=1000
	
	#figure out the unit and get the byte size
	if [[ $inputunit == "T" || $inputunit == "G" ]] || [[ $inputunit == "M" ]] || [[ $inputunit == "K" ]]; then
	
		if [ $inputunit == "G" ] ; then #if space is in gigabytes
	        	sq=$((mult*mult*mult))
	        	fspace=$(echo "$inputnum * $sq" | bc)
		elif [ $inputunit == "K" ] ; then #if space is in kilobytes
	        	fspace=$(echo "$inputnum * $mult" | bc)
		elif [ $inputunit == "M" ] ; then #if space is in megabytes
	        	sq=$((mult*mult))
	        	fspace=$(echo "$inputnum * $sq" | bc)
		else  #if space is in terabytes
	        	sq=$((mult*mult*mult*mult))
	        	fspace=$(echo "$inputnum * $sq" | bc)
		fi
	
	elif [[ $inputunit == "0" ]] || [[ $inputunit == "1" ]] || [[ $inputunit == "2" ]] || [[ $inputunit == "3" ]] || [[ $inputunit == "4" ]] || [[ $inputunit == "5" ]] || [[ $inputunit == "6" ]] || [[ $inputunit == "7" ]] || [[ $inputunit == "8" ]] || [[ $inputunit == "9" ]] ; then
		fspace=$required_space
	else
		log_me "$LINENO:The input for required space is not valid. Input is $required_space" "0"
		return ${FALSE}
	fi
	
	avail_space=$(df -Ph $the_mount | tail -1 | awk '{print $4}') || error_exit "Error while getting mount info. [rc=$?]"
	 
	length=${#avail_space} #measures the length of the given string
	dminus=$((length-one)) #space of the number
	
	unit=${avail_space:$dminus:1} #locates the location of the unit based on the space of the number
	number=${avail_space:0:$dminus} #locates the number
	
	#series of if statements to determine the size of the file based on the Units K, M, G, T, or a normal byte
	#outputs the calculated size in bytes
	
	if [ $unit == "G" ] ; then #if space is in gigabytes
		sq=$((mult*mult*mult))
		space=$(echo "$number * $sq" | bc)
	elif [ $unit == "K" ] ; then #if space is in kilobytes
		space=$(echo "$number * $mult" | bc)
	elif [ $unit == "M" ] ; then #if space is in megabytes
		sq=$((mult*mult))
		space=$(echo "$number * $sq" | bc)
	elif [ $unit == "T" ] ; then #if space is in terabytes
		sq=$((mult*mult*mult*mult))
		space=$(echo "$number * $sq" | bc)
	else #if space is in bytes
		factor=1
		space=$(echo "$required_space * $factor" | bc)
	fi
	
	if [ $(echo "$fspace > $space" | bc) -ne 0  ] ; then
		log_me "$LINENO:Location does not have enough space. Need: $fspace Available: $space" "0"
		return ${FALSE}
	else
		log_me "$LINENO:You have enough space. Need: $fspace Available: $space" "1"
		return ${TRUE}
	fi	
}

#does destination have enough space to receive the files
function check_local_space
{
	if [[ $check_local_space -eq 1 ]];then
		log_me "$LINENO:Checking to see if local server has enough space" "1"
		has_enough_space ${local_space_needed} ${local_mount_location}
		rc=$?
		if (( rc ));then
			log_me "$LINENO:Enough space is available: Need space: $local_space_needed Avail space: $avail_space" "1"
		else
			alert_me "Not enough space is available: Need space: $fspace ($local_space_needed) Avail space: $space ($avail_space)" 
			log_me "$LINENO:Not enough space is available: Need space: $local_space_needed Avail space: $avail_space" "0"
			exit 1
		fi
	else
		log_me "$LINENO:Skipping checking for available space" "1"
	fi
}

function create_folder_structure
{
	mkdir -p ${logdir} || error_exit "$LINENO: Error creating log directory ${logdir}: [rc=$?]"
	mkdir -p ${data_dir} || error_exit "$LINENO: Error creating data directory ${data_dir}: [rc=$?]"
}

function init_cfg
{	
	echo "Initialize attribute cleanup script before run"
	progname=$(basename $0)
	
	#set pipefail to return the first error code in piped commands
	set -o pipefail

	#source the cfg that was passed in, else, source the default cfg
	if [[ ${cfg_file} != "" ]];then
		source $cfg_file || error_exit "$LINENO: Error sourcing config file. [rc=$?]"
		echo "$$: Sourcing config file: $cfg_file" >> $logdir/$log_file
	else
		currentpath=$(pwd)
		
		ls ${currentpath}/dmt_model_main.cfg
		if [[ $? -eq 0 ]];then
			default_cfg=${currentpath}/dmt_model_main.cfg
		else
			#look in config folder
			ls ${currentpath}/config/dmt_model_main.cfg
			if [[ $? -eq 0 ]];then
				default_cfg=${currentpath}/config/dmt_model_main.cfg
			else
				#if script is in bin, then check one level below in config
				cd ${currentpath}; cd ..;
				ls ./config/dmt_model_main.cfg
				if [[ $? -eq 0 ]];then
					default_cfg="./config/dmt_model_main.cfg"	
				else
					log_me "$LINENO:Config file not found...exiting" "1"
					alert_me "Config file not found."
					exit 1
				fi	
			fi
		fi
		
		source ${default_cfg}
		echo "$$: Sourcing default configuration file: ${default_cfg}" >> ${logdir}/${log_file}
	fi
}

#initialize program before running
function init_attr_cleanup
{
	#turning on tracing mode
	[[ $trace_mode -eq 1 ]] && set -x

	#measure start time
	start_ts=$(date +"%Y-%m-%d %H:%M:%S")
	start_time=$SECONDS

	#check variables in configuration file
	check_config_variable

	#create necessary folders
	create_folder_structure

	#make sure there's enough space for job to run
	check_local_space
}

#get the time elapsed in minutes
function get_time_elapsed
{
	end_time=$SECONDS
	time_elapsed_in_sec=$(echo "($end_time-$start_time)"|bc -l)	
	time_elapsed_in_min=$(echo "($end_time-$start_time)/60"|bc -l|xargs printf "%.2f")
	time_elapsed_in_min_round_up=$(echo "($end_time-$start_time)/60"|bc -l|xargs printf "%1.f")	
}

function transpose_csv_file
{
	log_me "$LINENO: Entering ${FUNCNAME[0]}" "1"
	rm ${transpose_output} 2>/dev/null

	#if the input file is missing, don't start tranposed module
	ls ${transpose_input} || error_exit "$LINENO: Missing input file for tranposed module. [rc=$?]"

	log_me "$LINENO: Executing: python ${columns_to_rows} ${config_dir} ${transpose_input} ${transpose_output}"
	python ${columns_to_rows} ${config_dir} ${transpose_input} ${transpose_output}
	rc=$?
	if [[ $rc != 0 ]];then	
		log_me "$LINENO: Error executing python to transpose csv file. Check python log for details" "0"	
		alert_me "Error executing python to transpose csv file. Check log for details"
	fi
	log_me "$LINENO: Exit ${FUNCNAME[0]}" "1"
}

function move_to_hdfs
{
	log_me "$LINENO: Entering ${FUNCNAME[0]}" "1"
	
	#remove what's already there
	hdfs dfs -rm -R ${data_dir_hdfs_input} 2>/dev/null	#send all errors to bit bucket
	hdfs dfs -mkdir ${data_dir_hdfs_input} || error_exit "$LINENO: Unable to make hdfs data dir [${data_dir_hdfs_input}]. [rc=$?]"
	
	#if output of transpose is missing, exit
	ls ${transpose_output} || error_exit "$LINENO: Missing output file of the tranposed module. [rc=$?]"
	
	log_me "$LINENO: Executing cmd: hdfs dfs -copyFromLocal -f ${transpose_output} ${data_dir_hdfs_input}" "1"
	hdfs dfs -copyFromLocal -f ${transpose_output} ${data_dir_hdfs_input} || error_exit "$LINENO: Error copying output transposed to hdfs. [rc=$?]"
	log_me "$LINENO: Exiting ${FUNCNAME[0]}" "1"
}

function get_from_hdfs
{
	log_me "$LINENO: Entering ${FUNCNAME[0]}" "1"
	
	#remove old out files
	rm ${output_file_from_hdfs} 2>/dev/null

	#make sure input file exists prior to running
	hdfs dfs -ls ${hdfs_output_file} || error_exit "$LINENO: Missing string match hdfs output file. [rc=$?]"

	log_me "$LINENO: Executing cmd: hdfs dfs -copyToLocal ${hdfs_output_file} ${output_file_from_hdfs}" "1"
	hdfs dfs -copyToLocal ${hdfs_output_file} ${output_file_from_hdfs} || error_exit "Unable to get string match output. [rc=$?]"
	log_me "$LINENO: Exiting ${FUNCNAME[0]}" "1"
}

function run_string_match
{
	log_me "$LINENO: Entering ${FUNCNAME[0]}" "1"
	
	#remove existing output
	hdfs dfs -rm -r -skipTrash ${data_dir_hdfs_output}

	#make sure input file exists prior to running
	hdfs dfs -ls ${data_dir_hdfs_input} || error_exit "$LINENO: Missing hdfs input file to run string match. [rc=$?]"
	
	log_me "$LINENO: Executing cmd: hadoop jar ${hadoop_stream_jar} -D mapred.task.timeout=${mapred_timeout} -file ${attr_cleanup_mapper} -mapper ${attr_cleanup_mapper} -file ${attr_cleanup_reducer} -reducer ${attr_cleanup_reducer} -input ${data_dir_hdfs_input} -output ${data_dir_hdfs_output}" "1"
	hadoop jar ${hadoop_stream_jar} -D mapred.task.timeout=${mapred_timeout} -file ${attr_cleanup_mapper} -mapper ${attr_cleanup_mapper} -file ${attr_cleanup_reducer} -reducer ${attr_cleanup_reducer} -input ${data_dir_hdfs_input} -output ${data_dir_hdfs_output}
	log_me "$LINENO: Exiting ${FUNCNAME[0]}" "1"
}

#generate cleansed excel sheet and summary sheet
function generate_summary_excel
{
	log_me "$LINENO: Entering ${FUNCNAME[0]}" "1"
	
	#remove old files
	rm ${attr_cleansed_file} 2>/dev/null
	rm ${attr_summ_file} 2>/dev/null

	#if output of string match is missing, exit
	ls ${output_file_from_hdfs} || error_exit "$LINENO: Missing output from string match. [rc=$?]"

	log_me "$LINENO: Executing cmd: java -jar ${atribute_summary_jar} ${output_file_from_hdfs} ${attr_cleansed_file} ${attr_summ_file} ${retail_channel_code} ${category_nbr} ${dept_nbr}" "1"
	#arguments are input file from mapred program, name of the cleanses excel, name of the summary file, retail channel code, category nbr, dept nbr
	java -jar ${atribute_summary_jar} ${output_file_from_hdfs} ${attr_cleansed_file} ${attr_summ_file} ${retail_channel_code} "${category_nbr}" "${dept_nbr}" "${acctg_dept_desc}" "${dept_category_desc}"
	log_me "$LINENO: Exiting ${FUNCNAME[0]}" "1"
}

#generate map file from cleansed excel
function create_map_file
{
	log_me "$LINENO: Entering ${FUNCNAME[0]}" "1"

	#remove old files
	rm ${attr_mapping_file} 2>/dev/null
	rm ${attr_header_file} 2>/dev/null
	
	#if output of summary generation moduleh is missing, exit
	ls ${attr_cleansed_file} || error_exit "$LINENO: Missing output from generation summary process. [rc=$?]"
	
	#arguments are the cleansed excel sheet 
	log_me "$LINENO: executing cmd: python ${generate_map_file} ${attr_cleansed_file} ${attr_mapping_file} ${attr_header_file} ${dept_nbr} ${retail_channel_code}" "1"
	python ${generate_map_file} ${config_dir} ${attr_cleansed_file} ${attr_mapping_file} ${attr_header_file} ${dept_nbr} ${retail_channel_code}
	rc=$?
	if [[ $rc != 0 ]];then	
		log_me "$LINENO: Error executing python to generate map file. Check python log for details" "0"	
		alert_me "Error executing python to generate map file. Check log for details"
	fi
	log_me "$LINENO: Exiting ${FUNCNAME[0]}" "1"
}

#email summary page to customer
function emailSummary
{
	echo "email summary page"
	echo "email summary" | mailx -s "summary page" -a ${attr_summ_file} ${support_email}
	echo "clean summary" | mailx -s "clean page" -a ${attr_cleansed_file} $support_email}
}

function cleanAttributes
{
	#Todo: compare csv summary page to clean csv file, remove not needed column
	echo "clean attributes"
}

function setPermission
{
	mypath="/user/hive/warehouse/dtm_attribute_cleanup.db"
	permission=777
	hdfs dfs -chmod -R ${permission} ${mypath}
}

#load attribute cleanup file to hive
function load_hive_cleansedfile
{
	log_me "$LINENO: Entering ${FUNCNAME[0]}" "1"

	#make sure files are present before using
	ls ${attr_header_file} || error_exit "$LINENO: Missing header file. [rc=$?]"
	ls ${attr_cleansed_file} || error_exit "$LINENO: Missing cleansed excel file file. [rc=$?]"

	header_info=`cat ${attr_header_file} | head -n +1`
	cat ${attr_cleansed_file} | tail -n +2 > ${attr_cleansed_file_wo_header} 

	log_me "$LINENO: Executing cmd: hive -f ${bin_dir}/${load_attribute_cleanup} --hivevar table_def=${header_info} --hivevar dtm_schema=${dtm_schema} --hivevar dtm_table=${dtm_table} --hivevar inputfile=${attr_cleansed_file_wo_header}" "1"
	hive -f ${bin_dir}/${load_attribute_cleanup} --hivevar table_def="${header_info}" --hivevar dtm_schema=${dtm_schema} --hivevar dtm_table=${dtm_table} --hivevar inputfile=${attr_cleansed_file_wo_header} || error_exit "$LINENO: Load cleansed excel to hive failed. [rc=$?]"

	log_me "$LINENO: Exiting ${FUNCNAME[0]}" "1"
}

function load_hive_mapfile
{
	log_me "$LINENO: Entering ${FUNCNAME[0]}" "1"

	#file exists before running
	ls ${attr_mapping_file} || error_exit "$LINENO: Missing mapping file. [rc=$?]"
	
	log_me "hive -f ${bin_dir}/${load_map_file}  --hivevar map_file=${attr_mapping_file} --hivevar dtm_schema=${dtm_schema}" "1"
	hive -f ${bin_dir}/${load_map_file}  --hivevar map_file="${attr_mapping_file}" --hivevar dtm_schema=${dtm_schema} || error_exit "$LINENO: Load map file to hive failed. [rc=$?]" 
	log_me "$LINENO: Exiting ${FUNCNAME[0]}" "1"
}

function callWTE
{
	echo "calling WTE"
	echo "category number: ${mapFileArray[1]}"
	echo "timestamp: ${mapFileArray[2]}"
	echo "calling WTE with these arguments retail channel code ($retail_channel_code), category nbr ($category_nbr), timestamp ($runTimestamp), wk id range ($week_id)"
}

function obtain_map_var
{
	log_me "$LINENO: Entering ${FUNCNAME[0]}" "1"

	ls ${attr_mapping_file} || error_exit "$LINENO: Missing mapping file. [rc=$?]"
	
	mapFile=$(cat ${attr_mapping_file})
	IFS='|' read -a mapFileArray <<< "$mapFile"
	#mapFileArray=(${mapFile//|/ })

	#exit if the number of columns generated for mapping hive table isn't enough
	if [[ ${#mapFileArray[@]} -ne ${nbr_map_columns} ]];then
		error_exit "$LINENO: Nbr of columns isn't enough. Expected=${nbr_map_columns}. Found=${#mapFileArray[@]}"
	fi

	category_nbr=${mapFileArray[2]}
	runTimestamp=${mapFileArray[3]}
	attributes=${mapFileArray[5]}
	
	#get the size from the number of attributes
	IFS=',' read -a attributesArray <<< "$attributes"
   
	noOfAttr="${#attributesArray[@]}"   #get total number of attributes
	log_me "$LINENO: complete table name is [dtm_table=${dtm_table_base}_${category_nbr}_A${noOfAttr}_${runTimestamp}]" "1"
	dtm_table=${dtm_table_base}_${category_nbr}_A${noOfAttr}_${runTimestamp}
	log_me "$LINENO: Exiting ${FUNCNAME[0]}" "1"
}

function startProgram
{
	init_cfg

	log_me "$LINENO: --------------------STARTING main program ----------------------------" "0"
   	
	#do program pre-process prior to execution
	init_attr_cleanup	
	
	#transpose csv file form rows to columns
	transpose_csv_file	
	
	#move file to hdfs to prepare for string match
	move_to_hdfs
	
	#run string match to generate necessary data for summary page and cleaned page
	run_string_match
	
	#get files to local after string matching completes
	get_from_hdfs
	
	#generate summary and clean page
	generate_summary_excel
	
	#map file from cleansed excel
	create_map_file
	
	#load to hdfs
	load_hive_mapfile
	
	#get variable required for hive
	obtain_map_var
	
	#cleanAttributes
	load_hive_cleansedfile
	
	#call wte
	#callWTE

	#set permission 
	#setPermission	

	#email summary
	#emailSummary

	cleanup
	log_me "$LINENO: --------------------END main program ----------------------------" "0"
}
cfg_file="$1"
