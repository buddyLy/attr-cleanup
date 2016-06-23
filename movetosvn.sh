IFS="." read -a inputname <<< "$1"

echo "${inputname[1]}"

if [[ ${inputname[1]} == "py" ]]; then
	echo "moving to /Users/lcle/Documents/workspace/ciq-demand-transference-model/src/main/scripts/python"
	cp $1 /Users/lcle/Documents/workspace/ciq-demand-transference-model/src/main/scripts/python/
elif [[ ${inputname[1]} == "sh" ]]; then
	echo "moving to /Users/lcle/Documents/workspace/ciq-demand-transference-model/src/main/scripts/shell/attribute_cleanup"
	cp $1 /Users/lcle/Documents/workspace/ciq-demand-transference-model/src/main/scripts/shell/attribute_cleanup
else
	echo "unrecognize input"
fi

