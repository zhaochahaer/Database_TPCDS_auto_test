#!/bin/bash
set -e
PWD=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

GPFDIST_PORT=$1
GEN_DATA_PATH=$2

qbfdist -p $GPFDIST_PORT -d $GEN_DATA_PATH > qbfdist.$GPFDIST_PORT.log 2>&1 < qbfdist.$GPFDIST_PORT.log &
pid=$!

if [ "$pid" -ne "0" ]; then
	sleep .4
	count=$(ps -ef 2> /dev/null | grep -v grep | awk -F ' ' '{print $2}' | grep $pid | wc -l)
	if [ "$count" -eq "1" ]; then
		echo "Started qbfdist on port $GPFDIST_PORT"
	else
		echo "Unable to start qbfdist on port $GPFDIST_PORT"
		exit 1
	fi
else
	echo "Unable to start background process for qbfdist on port $GPFDIST_PORT"
	exit 1
fi

