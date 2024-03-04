#!/bin/bash
set -e

PWD=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $PWD/../functions.sh
source_bashrc
step="multi_user_reports"

init_log $step

get_version
if [[ "$VERSION" == *"gpdb"* ]]; then
	filter="gpdb"
elif [ "$VERSION" == "postgresql" ]; then
	filter="postgresql"
else
	echo "ERROR: Unsupported VERSION!"
	exit 1
fi

for i in $(ls $PWD/*.$filter.*.sql); do
	echo "qsql -v ON_ERROR_STOP=1 -a -f $i"
	qsql -v ON_ERROR_STOP=1 -a -f $i
	echo ""
done

filename=$(ls $PWD/*.copy.*.sql)

for i in $(ls $PWD/../log/rollout_testing_*); do
	logfile="'""$i""'"
	
        echo "qsql -v ON_ERROR_STOP=1 -a -f $filename -v LOGFILE=\"$logfile\""
        qsql -v ON_ERROR_STOP=1 -a -f $filename -v LOGFILE="$logfile"
done

qsql -v ON_ERROR_STOP=1 -t -A -c "select 'analyze ' || n.nspname || '.' || c.relname || ';' from qb_class c join qb_namespace n on n.oid = c.relnamespace and n.nspname = 'tpcds_testing'" | qsql -v ON_ERROR_STOP=1 -t -A -e

qsql -v ON_ERROR_STOP=1 -F $'\t' -A -P pager=off -f $PWD/detailed_report.sql
echo ""

end_step $step
