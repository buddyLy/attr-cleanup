#/bin/ksh
echo "Running test cases"

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
    #source ${myexecfile}

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
)
