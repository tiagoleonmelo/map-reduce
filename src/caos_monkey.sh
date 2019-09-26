#!/bin/bash


while :
do
	workers=`pgrep -f worker.py | xargs`
	coordenators=`pgrep -f coordinator.py | xargs`
	procs="$workers $coordenators"

	for pid in $procs; do
		kill_probability=$((RANDOM % 10))
		echo "$kill_probability ? $pid"
		if [ "$kill_probability" -lt 1 ]; then
			kill -HUP $pid
			echo "Killed pid $pid, giving you 10 sec to recover"
			sleep 10 
		fi
	done
	sleep 1
done
