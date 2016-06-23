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
	mycfgfile="/Users/lcle/git/attr-cleanup/test/dmt_model_main.cfg"
	myexecfile="/Users/lcle/git/attr-cleanup/dmt_model_main.sh"
	source ${myexecfile}
	
	function assertEquals
	{
		msg=$1; shift
		expected=$1; shift
		actual=$1; shift
		/bin/echo -n "$msg: "
		if [ "$expected" != "$actual" ]; then
			echo "FAILED: EXPECTED=$expected ACTUAL=$actual"
		else
			echo "PASSED"
		fi
	}

	function check_config_variable_test
	{
		cp ${mycfgfile}	${mycfgfile}.bak
		source ${mycfgfile}	
		#replace datadir with blank
		/bin/sed -i "s|data_dir=.*|data_dir=\"\"|g" ${mycfgfile}
		check_config_variable 
		assertEquals ">>>TEST check config variable not set" 1 $?
	}

	function local_space_not_enough_test
	{
		source ${mycfgfile}
		has_enough_space "1T" "/u" "1"
		assertEquals ">>>TEST local doesn't have enough space" ${FALSE} $?
	}
	
	function dest_space_is_not_enough_test
	{
		source ${mycfgfile}
		has_enough_space "1T" "/u" "0"
		assertEquals ">>>TEST destination server does have enough space" ${FALSE} $?
	}
	
	function each_file_transmit_test
	{
		source ${mycfgfile}
		datadir="/user/lcle/test"
		destdir="/u/users/lcle/test"
		
		testfile="test.txt"
		echo "abcdefg" > ${testfile}
		
		#hdfs dfs -touchz ${datadir}/${testfile}
		hdfs dfs -copyFromLocal ${testfile} ${datadir}/${testfile}
		command="touch ${destdir}/${testfile}; echo abcdefg > ${destdir}/${testfile}"
		exec_remote_cmd "${command}" "0"
		
		validate_each_file_transmit "${testfile}"
		assertEquals ">>>TEST validate each file transmit" ${TRUE} $?
		
		#clean up files created
		rm ${testfile}
		hdfs dfs -rm ${datadir}/${testfile}
		command="rm ${destdir}/${testfile}"
		exec_remote_cmd "${command}" "0"
	}
	
	function total_transfer_size_validation_test
	{
		source ${mycfgfile}
		datadir="/user/lcle/test"
		destdir="/u/users/lcle/test"
		
		testfile="test.txt"
		echo "abcdefg" > ${testfile}
		
		#hdfs dfs -touchz ${datadir}/${testfile}
		hdfs dfs -copyFromLocal ${testfile} ${datadir}/${testfile}
		command="touch ${destdir}/${testfile}; echo abcdefg > ${destdir}/${testfile}"
		exec_remote_cmd "${command}" "0"
		
		
		validate_total_size
		assertEquals ">>>TEST transfer total size validation" ${TRUE} $?
		
		#clean up files created
		rm ${testfile}
		hdfs dfs -rm ${datadir}/${testfile}
		command="rm ${destdir}/${testfile}"
		exec_remote_cmd "${command}" "0"
		
	}
	
	function total_count_validation_test
	{
		source ${mycfgfile}
		datadir="/user/lcle/test"
		destdir="/u/users/lcle/test"
		
		testfile="test.txt"
		hdfs dfs -touchz ${datadir}/${testfile}
		command="rm ${destdir}/${testfile}; touch ${destdir}/${testfile}"
		exec_remote_cmd "${command}" "0"
		time_transfer_in_min=1
		
		validate_total_count "${time_transfer_in_min}"
		assertEquals ">>>TEST validate total count" ${TRUE} $?
		
		#clean up
		hdfs dfs -rm ${datadir}/${testfile}
		command="rm ${destdir}/${testfile}"
		exec_remote_cmd "${command}" "0"
	}
	
	function destination_server_setup_test
	{
		source ${mycfgfile}
		clean_dest_dir=1 #this should clean up the destination directory
		destdir="/u/users/lcle/test"
		setup_dest_server
		command="ls $destdir | wc -l"
		exec_remote_cmd "${command}" "0"
		assertEquals ">>>TEST set up destination server" ${return_value} 0 
	}
	
	function max_error_count_test
	{
		source ${mycfgfile}
		echo 6 > ${logdir}/$error_count_file
		max_error=5
		check_error_exceeds_max
		assertEquals ">>>TEST error count exceeds max allowed" ${TRUE} $?
	}
	
	function log_file_size_cleanup_test
	{
		source ${mycfgfile}
		log_retention=2
		log_file="test.log"
		echo "log msg 1" > ${logdir}/${log_file}
		echo "log msg 2" >> ${logdir}/${log_file}
		echo "log msg 3" >> ${logdir}/${log_file}
		retain_lastx_log_size
		log_size=$(cat ${logdir}/${log_file} | wc -l)
		assertEquals ">>>TEST retain log file size" 2 ${log_size}
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
	
	function initialize_loggers_test
	{
		source ${mycfgfile}
		initialize_loggers
		success=1
		[[ -e ${passdir}/${passfile} ]] || success=0
		[[ -e ${logdir}/${log_file} ]] || success=0
		[[ -e ${logdir}/${status_file} ]] || success=0
		[[ -e ${logdir}/${total_sent_status} ]] || success=0
		[[ -e ${logdir}/${error_count_file} ]] || success=0
		
		assertEquals ">>>TEST initialize loggers" 1 $success
	}
	
	#Test calls
	#initialize_loggers_test
	check_config_variable_test
	#local_space_not_enough_test
	#dest_space_is_not_enough_test
	#log_file_size_cleanup_test
	#total_transfer_size_validation_test
	#total_count_validation_test
	#each_file_transmit_test
	#destination_server_setup_test
	#max_error_count_test
	#monitor_long_run_process_test
)
