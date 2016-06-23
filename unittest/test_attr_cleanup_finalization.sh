#/bin/ksh

######################################################
#Unit Testing starts here
#This unit testing will not execute if sourced or get kicked off by another program.
#Unit test cases will execute if run this shell script by itself
######################################################
(
	#bash_source at 0 holds the actual name, if kicked off from another program, base_source is not itself, then exit
	#since we wrapped this in a subshell "( )", then it will only exit the subshell not the actual program
	[[ "${BASH_SOURCE[0]}" == "${0}" ]] || exit 0
	mycfgfile="/u/users/lcle/attr-cleanup/config/attr_cleanup_finalization.cfg"
	myexecfile="/u/users/lcle/attr-cleanup/bin/attr_cleanup_finalization.sh"
	
	unittestfile="./unitTestResult.txt"

	function assertEquals
	{
		msg=$1; shift
		expected=$1; shift
		actual=$1; shift
		/bin/echo -n "$msg: " >> ${unittestfile}
		if [ "$expected" != "$actual" ]; then
			echo "FAILED: EXPECTED=$expected ACTUAL=$actual" >> ${unittestfile}
		else
			echo "PASSED" >> ${unittestfile}
		fi
	}

	function check_config_variable_fail_test
	{
		source ${mycfgfile}
		source ${myexecfile} ${mycfgfile}
		data_dir=""	#blanked out value for test
		#replace datadir with blank
		(check_config_variable )
		assertEquals ">>>TEST check config variable not set" ${TRUE} $? 
	}

	function check_config_variable_test
	{
		source ${mycfgfile}
		source ${myexecfile} ${mycfgfile}
		#replace datadir with blank
		(check_config_variable )
		assertEquals ">>>TEST check config variable set" 0 $?
	}

	function local_space_not_enough_test
	{
		source ${mycfgfile}
		source ${myexecfile} ${mycfgfile}
		has_enough_space "1T" "/u"
		assertEquals ">>>TEST local doesn't have enough space" ${FALSE} $?
	}
	
	function log_file_size_cleanup_test
	{
		source ${mycfgfile}
		source ${myexecfile} ${mycfgfile}
		log_retention=2
		log_file="test.log"
		echo "log msg 1" > ${logdir}/${log_file}
		echo "log msg 2" >> ${logdir}/${log_file}
		echo "log msg 3" >> ${logdir}/${log_file}
		retain_lastx_log_size
		log_size=$(cat ${logdir}/${log_file} | wc -l)
		assertEquals ">>>TEST retain log file size" 2 ${log_size}
	}
	
	function transpose_csv_file_missing_file_test
	{
		source ${mycfgfile}
		source ${myexecfile} ${mycfgfile}
		transpose_input="./3186_Yogurt5.csv" 	#this file does not exist
		(transpose_csv_file)
		assertEquals ">>>TEST transpose csv file doesn't exist" ${TRUE} $?
	}

	function move_to_hdfs_missing_file_test
	{
		source ${mycfgfile}
		source ${myexecfile} ${mycfgfile}
		transpose_output="./3186YogurtTransposed5.csv" 	#this file does not exist
		(move_to_hdfs)
		assertEquals ">>>TEST move to hdfs file doesn't exist" ${TRUE} $?
	}
	
	function move_to_hdfs_missing_file_test
	{
		source ${mycfgfile}
		source ${myexecfile} ${mycfgfile}
		transpose_output="./3186YogurtTransposed5.csv" 	#this file does not exist
		(move_to_hdfs)
		assertEquals ">>>TEST move to hdfs file doesn't exist" ${TRUE} $?
	}


	function run_string_match_missing_file_test
	{
		source ${mycfgfile}
		source ${myexecfile} ${mycfgfile}
		data_dir_hdfs_input="/not/found/directory" 	#this file does not exist
		(run_string_match)
		assertEquals ">>>TEST run string match file doesn't exist" ${TRUE} $?
	}

	function get_from_hdfs_missing_file_test
	{
		source ${mycfgfile}
		source ${myexecfile} ${mycfgfile}
		hdfs_output_file="/not/found/directory/file" 	#this file does not exist
		(move_to_hdfs)
		assertEquals ">>>TEST get from hdfs file doesn't exist" ${TRUE} $?
	}


	function generate_summary_excel_missing_file_test
	{
		source ${mycfgfile}
		source ${myexecfile} ${mycfgfile}
		output_file_from_hdfs="/not/found/directory/file" 	#this file does not exist
		(move_to_hdfs)
		assertEquals ">>>TEST generate summary excel file doesn't exist" ${TRUE} $?
	}

	function create_map_file_missing_file_test
	{
		source ${mycfgfile}
		source ${myexecfile} ${mycfgfile}
		attr_cleansed_file="/not/found/directory/file" 	#this file does not exist
		(create_map_file)
		assertEquals ">>>TEST generate map file doesn't exist" ${TRUE} $?
	}

	function load_hive_mapfile_missing_file_test
	{
		source ${mycfgfile}
		source ${myexecfile} ${mycfgfile}
		attr_mapping_file="/not/found/directory/file" 	#this file does not exist
		(load_hive_mapfile)
		assertEquals ">>>TEST load map file to hive file doesn't exist" ${TRUE} $?
	}

	function load_hive_mapfile_load_hive_failed_test
	{
		source ${mycfgfile}
		source ${myexecfile} ${mycfgfile}
		load_map_file="/not/found/directory/file" 	#this file does not exist
		(load_hive_mapfile)
		assertEquals ">>>TEST load map file to hive hive load statement failed" ${TRUE} $?
	}

	function obtain_map_var_missing_file_test
	{
		source ${mycfgfile}
		source ${myexecfile} ${mycfgfile}
		attr_mapping_file="/not/found/directory/file" 	#this file does not exist
		(obtain_map_var)
		assertEquals ">>>TEST get mapping variable file doesn't exist" ${TRUE} $?
	}

	function obtain_map_var_not_enough_attr_test
	{
		source ${mycfgfile}
		source ${myexecfile} ${mycfgfile}
		nbr_map_columns=2	#expected 6, but set to 2 to fail
		(obtain_map_var)
		assertEquals ">>>TEST get mapping variable not enough attribute" ${TRUE} $?
	}

	function load_hive_cleansedfile_missing_header_file_test
	{
		source ${mycfgfile}
		source ${myexecfile} ${mycfgfile}
		attr_header_file="/not/found/directory/file" 	#this file does not exist
		(load_hive_cleansedfile)
		assertEquals ">>>TEST load cleansed excel file to hive header file doesn't exist" ${TRUE} $?
	}

	function load_hive_cleansedfile_missing_cleansed_file_test
	{
		source ${mycfgfile}
		source ${myexecfile} ${mycfgfile}
		attr_cleansed_file="/not/found/directory/file" 	#this file does not exist
		(load_hive_cleansedfile)
		assertEquals ">>>TEST load cleansed excel file to hive cleansed file doesn't exist" ${TRUE} $?
	}

	function load_hive_cleansedfile_hive_load_failed_test
	{
		source ${mycfgfile}
		source ${myexecfile} ${mycfgfile}
		load_attribute_cleanup="/not/found/directory/file" 	#this file does not exist
		(load_hive_cleansedfile)
		assertEquals ">>>TEST load cleansed excel file to hive load hive failed" ${TRUE} $?
	}

	function monitor_long_run_process_test
	{
		source ${mycfgfile}
		monitor_long_process=1
		process_max_time=1
		check_wait_time=1
		processtomonitor="longruntest.sh"
		mylogfile="longruntest.log"
		
		echo "echo \"testing long running process. sleeping at..\"" > ${processtomonitor}
		echo "date" >> ${processtomonitor}
		echo "sleep 125" >> ${processtomonitor}
		echo "echo \"waking up at...\"" >> ${processtomonitor}
		echo "date" >> ${processtomonitor}
		
		if [[ $monitor_long_process -eq 1 ]];then
			log_me "Monitoring long running process" "1"
			nohup sh ${processtomonitor} &
			nohup sh mon_process_time.sh "$processtomonitor" "$process_max_time" "${check_wait_time}" "$support_email" "${logdir}/$mylogfile" &
			
			while [[ curr_count=$(ps -ef | grep $processtomonitor | grep -v 'grep\|view\|vi\|mon_process_time.sh' | grep -c $processtomonitor) -ne 0 ]]
			do
				echo "Waiting for the last process to finish"
				sleep $sleeptime			
			done
		fi
		
		runlongcount=$(grep "running long" ${mylogfile} | wc -l)
		
		assertEquals ">>>TEST monitor long running process" 1 ${runlongcount}
		rm ${logdir}/${mylogfile}
		rm ${processtomonitor}
	}	
	
	#Test calls
	check_config_variable_test
	check_config_variable_fail_test
	local_space_not_enough_test
	log_file_size_cleanup_test
	transpose_csv_file_missing_file_test
	move_to_hdfs_missing_file_test
	move_to_hdfs_missing_file_test
	run_string_match_missing_file_test
	get_from_hdfs_missing_file_test
	generate_summary_excel_missing_file_test
	create_map_file_missing_file_test
	load_hive_mapfile_missing_file_test
	load_hive_mapfile_load_hive_failed_test
	obtain_map_var_missing_file_test
	obtain_map_var_not_enough_attr_test
	load_hive_cleansedfile_missing_header_file_test
	load_hive_cleansedfile_missing_cleansed_file_test
	load_hive_cleansedfile_hive_load_failed_test
	#monitor_long_run_process_test
)
