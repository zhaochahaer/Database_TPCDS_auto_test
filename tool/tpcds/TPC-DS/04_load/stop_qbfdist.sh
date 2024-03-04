#!/bin/bash
set -e

for i in $(ps -ef | grep qbfdist |  grep -v grep | grep -v stop_qbfdist | awk -F ' ' '{print $2}'); do
        echo "killing $i"
        kill $i
done
