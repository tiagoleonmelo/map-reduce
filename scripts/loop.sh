#!/bin/bash
if [ $# -lt 1 ]; then
	echo "Usage: loop.sh process"
	echo "Example: loop.sh coordinator -f lusiadas.txt"
	exit 0
fi

proc="$1"
shift
args="$@"

while :
do
	python3 $proc.py $args
	echo "Press <CTRL+C> to exit."
	sleep 1
done

