#!/bin/bash

# run the neo4j ingest script
# <rajibc[at]iitg[dot]ac[dot]in>

# setup values
SCRIPT_NAME="import_csv_to_neo_202-sleep.py"
STOP_FILE_NAME="__stop_neo4j_import__"

# start execution
#-----------------------------------------------------------------------------
# if the script is already running, do not run it again
pid_num=$(pgrep -f "$SCRIPT_NAME")
if [ ! -z "$pid_num" ]; then
	echo "Script ($SCRIPT_NAME) is already running (PID: $pid_num)...not running it again."
	exit 1
fi

# else, run script
while :
do
	# if stop file found, stop
	[ -f "$STOP_FILE_NAME" ] && break

	# run script
	/usr/bin/python  "$SCRIPT_NAME"

	# if the python script is killed, start it again
	# but to avoid busy-wait condition, use a small delay
	sleep 5s
done

# should do
exit 0

