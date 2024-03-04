#!/bin/bash
set -e

PWD=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $PWD/../functions.sh
source_bashrc
step=single_user_reports

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

for i in $(ls $PWD/*.copy.*.sql); do
	logstep=$(echo $i | awk -F 'copy.' '{print $2}' | awk -F '.' '{print $1}')
	logfile="$PWD""/../log/rollout_""$logstep"".log"
	logfile="'""$logfile""'"
	echo "qsql -v ON_ERROR_STOP=1 -a -f $i -v LOGFILE=\"$logfile\""
	qsql -v ON_ERROR_STOP=1 -a -f $i -v LOGFILE="$logfile"
	echo ""
done

qsql -v ON_ERROR_STOP=1 -q -t -A -c "select 'analyze ' || n.nspname || '.' || c.relname || ';' from qb_class c join qb_namespace n on n.oid = c.relnamespace and n.nspname = 'tpcds_reports'" | qsql -v ON_ERROR_STOP=1 -t -A -e

echo "********************************************************************************"
echo "Generate Data"
echo "********************************************************************************"
qsql -F $'\t' -A -v ON_ERROR_STOP=1 -P pager=off -f $PWD/gen_data_report.sql
echo ""
echo "********************************************************************************"
echo "Data Loads"
echo "********************************************************************************"
qsql -F $'\t' -A -v ON_ERROR_STOP=1 -P pager=off -f $PWD/loads_report.sql
echo ""
echo "********************************************************************************"
echo "Analyze"
echo "********************************************************************************"
qsql -F $'\t' -A -v ON_ERROR_STOP=1 -P pager=off -f $PWD/analyze_report.sql
echo ""
echo ""
echo "********************************************************************************"
echo "Queries"
echo "********************************************************************************"
qsql -F $'\t' -A -v ON_ERROR_STOP=1 -P pager=off -f $PWD/queries_report.sql
echo ""
end_step $step
