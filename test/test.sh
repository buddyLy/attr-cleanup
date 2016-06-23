function foo1
{
#df -Ph /Users | tail -1 | awk '{print }' 
set -o pipefail
#df -Ph /u | tail -1 | awk '{print }'
the_mount="/u"
avail_space=$(df -Ph $the_mount | tail -1 | awk '{print $4}' 2>&1)
echo "avail_space=\"$error_value\" ~ rc=[$?]"
}

#rm dsba.txt 2> /dev/null

#foo1
#error_value=$(foo1 2>&1)
#echo "avail_space=\"$error_value\" ~ rc=[$?]"

function callpy
{
	python ./test.py
	#return 1
}

function sayhelloworld
{
	echo "hello world from shell" 
	return 5
}

rc1=$(callpy)
returncode1=$?
rc2=$(sayhelloworld)
returncode2=$?

echo "rc1=$rc1"
echo "rc2=$rc2"
echo "returncode1=$returncode1"
echo "returencode2=$returncode2"
