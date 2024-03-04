#!/bin/bash
set -e

PWD=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $PWD/../functions.sh
source_bashrc

BigTableLoad=$6

step=load
init_log $step

ADMIN_HOME=$(eval echo ~$ADMIN_USER)

get_version
if [[ "$VERSION" == *"gpdb"* ]]; then
	filter="gpdb"
elif [ "$VERSION" == "postgresql" ]; then
	filter="postgresql"
else
	echo "ERROR: Unsupported VERSION $VERSION!"
	exit 1
fi

copy_script()
{
	echo "copy the start and stop scripts to the hosts in the cluster"
	for i in $(cat $PWD/../segment_hosts.txt); do
		echo "scp start_qbfdist.sh stop_qbfdist.sh $ADMIN_USER@$i:$ADMIN_HOME/"
		scp $PWD/start_qbfdist.sh $PWD/stop_qbfdist.sh $ADMIN_USER@$i:$ADMIN_HOME/
	done
}
stop_qbfdist()
{
	echo "stop qbfdist on all ports"
	for i in $(cat $PWD/../segment_hosts.txt); do
		ssh -n -f $i "bash -c 'cd ~/; ./stop_qbfdist.sh'"
	done
}
start_qbfdist()
{
	stop_qbfdist
	sleep 1
	get_qbfdist_port
	
	if [ "$VERSION" == "gpdb_6" ]; then
		for i in $(qsql -v ON_ERROR_STOP=1 -q -A -t -c "select rank() over(partition by g.hostname order by g.datadir), g.hostname, g.datadir from gp_segment_configuration g where g.content >= 0 and g.role = 'p' order by g.hostname"); do
			CHILD=$(echo $i | awk -F '|' '{print $1}')
			EXT_HOST=$(echo $i | awk -F '|' '{print $2}')
			GEN_DATA_PATH=$(echo $i | awk -F '|' '{print $3}')
			GEN_DATA_PATH=$GEN_DATA_PATH/pivotalguru
			PORT=$(($GPFDIST_PORT + $CHILD))
			echo "executing on $EXT_HOST ./start_qbfdist.sh $PORT $GEN_DATA_PATH"
			ssh -n -f $EXT_HOST "bash -c 'cd ~/; ./start_qbfdist.sh $PORT $GEN_DATA_PATH'"
			sleep 1
		done
	else
		for i in $(qsql -v ON_ERROR_STOP=1 -q -A -t -c "select rank() over (partition by g.hostname order by p.fselocation), g.hostname, p.fselocation as path from gp_segment_configuration g join qb_filespace_entry p on g.dbid = p.fsedbid join qb_tablespace t on t.spcfsoid = p.fsefsoid where g.content >= 0 and g.role = 'p' and t.spcname = 'qb_default' order by g.hostname"); do
			CHILD=$(echo $i | awk -F '|' '{print $1}')
			EXT_HOST=$(echo $i | awk -F '|' '{print $2}')
			GEN_DATA_PATH=$(echo $i | awk -F '|' '{print $3}')
			GEN_DATA_PATH=$GEN_DATA_PATH/pivotalguru
			PORT=$(($GPFDIST_PORT + $CHILD))
			echo "executing on $EXT_HOST ./start_qbfdist.sh $PORT $GEN_DATA_PATH"
			ssh -n -f $EXT_HOST "bash -c 'cd ~/; ./start_qbfdist.sh $PORT $GEN_DATA_PATH'"
			sleep 1
		done
	fi
}

#Concurrent import of external tables: creating an installer statement
make_inster_table()
{
	rm -rf $PWD/gpdb.store_sales/*
	rm -rf $PWD/gpdb.catalog_sales/*
	rm -rf $PWD/gpdb.web_sales/*
	for j in $(cat $PWD/../segment_hosts.txt); do
		n1=$(echo $j | awk -F '.' '{print $1}')
		# echo ">>>>>>>>>>>>>>>>>>>$n1"
		echo "INSERT INTO tpcds.store_sales SELECT * FROM ext_tpcds.store_sales_$n1;" > $PWD/gpdb.store_sales/023.gpdb.store_sales_$n1.sql
		echo "INSERT INTO tpcds.catalog_sales SELECT * FROM ext_tpcds.catalog_sales_$n1;" > $PWD/gpdb.catalog_sales/021.gpdb.catalog_sales_$n1.sql
		echo "INSERT INTO tpcds.web_sales SELECT * FROM ext_tpcds.web_sales_$n1;" >  $PWD/gpdb.web_sales/019.gpdb.web_sales_$n1.sql
done
}


#Concurrent import of external tables
load_date_table()
{
	if [[ "$VERSION" == *"gpdb"* ]]; then
	copy_script	
	start_qbfdist
	for i in $(ls $PWD/*.$filter.*.sql); do
		start_log

		id=$(echo $i | awk -F '.' '{print $1}')
		schema_name=$(echo $i | awk -F '.' '{print $2}')
		table_name=$(echo $i | awk -F '.' '{print $3}')
		
		if [ "$table_name" == "store_sales" ] || [ "$table_name" == "catalog_sales" ] || [ "$table_name" == "web_sales" ]; then
			for j in $(ls $PWD/gpdb.${table_name}/*.$filter.*.sql); do
				echo "qsql -v ON_ERROR_STOP=1 -f $j | grep INSERT | awk -F ' ' '{print \$3}'"
				qsql -v ON_ERROR_STOP=1 -f $j | grep INSERT | awk -F ' ' '{print $3}' >> tuples_file &
			done
			wait
			tuples=$(awk '{sum += $1} END {print sum}' tuples_file)
			rm tuples_file
			log $tuples 

		else
			echo "qsql -v ON_ERROR_STOP=1 -f $i | grep INSERT | awk -F ' ' '{print \$3}'"
			tuples=$(qsql -v ON_ERROR_STOP=1 -f $i | grep INSERT | awk -F ' ' '{print $3}'; exit ${PIPESTATUS[0]})

			log $tuples
		fi
	done
	stop_qbfdist

	fi

}

#Concurrent import of external tables: switch judgment
if [ "$BigTableLoad" == "true" ];then
	echo "loading Concurrent Import"
	make_inster_table
	load_date_table
else
	if [[ "$VERSION" == *"gpdb"* ]]; then
		copy_script
		start_qbfdist

		for i in $(ls $PWD/*.$filter.*.sql); do
			start_log

			id=$(echo $i | awk -F '.' '{print $1}')
			schema_name=$(echo $i | awk -F '.' '{print $2}')
			table_name=$(echo $i | awk -F '.' '{print $3}')

			echo "qsql -v ON_ERROR_STOP=1 -f $i | grep INSERT | awk -F ' ' '{print \$3}'"
			tuples=$(qsql -v ON_ERROR_STOP=1 -f $i | grep INSERT | awk -F ' ' '{print $3}'; exit ${PIPESTATUS[0]})

			log $tuples
		done
		stop_qbfdist
	else
		if [ "$PGDATA" == "" ]; then
			echo "ERROR: Unable to determine PGDATA environment variable.  Be sure to have this set for the admin user."
			exit 1
		fi

		PARALLEL=$(lscpu --parse=cpu | grep -v "#" | wc -l)
		echo "parallel: $PARALLEL"

		for i in $(ls $PWD/*.$filter.*.sql); do
			short_i=$(basename $i)
			id=$(echo $short_i | awk -F '.' '{print $1}')
			schema_name=$(echo $short_i | awk -F '.' '{print $2}')
			table_name=$(echo $short_i | awk -F '.' '{print $3}')
			for p in $(seq 1 $PARALLEL); do
				filename=$(echo $PGDATA/pivotalguru_$p/"$table_name"_"$p"_"$PARALLEL".dat)
				if [[ -f $filename && -s $filename ]]; then
					start_log
					filename="'""$filename""'"
					echo "qsql -v ON_ERROR_STOP=1 -f $i -v filename=\"$filename\" | grep COPY | awk -F ' ' '{print \$2}'"
					tuples=$(qsql -v ON_ERROR_STOP=1 -f $i -v filename="$filename" | grep COPY | awk -F ' ' '{print $2}'; exit ${PIPESTATUS[0]})
					log $tuples
				fi
			done
		done
	fi

fi

max_id=$(ls $PWD/*.sql | tail -1)
i=$(basename $max_id | awk -F '.' '{print $1}' | sed 's/^0*//')

if [[ "$VERSION" == *"gpdb"* ]]; then
	dbname="$PGDATABASE"
	if [ "$dbname" == "" ]; then
		dbname="$ADMIN_USER"
	fi

	if [ "$PGPORT" == "" ]; then
		export PGPORT=5432
	fi
fi


if [[ "$VERSION" == *"gpdb"* ]]; then
	schema_name="tpcds"
	table_name="tpcds"

	start_log
	#Analyze schema using qanalyzedb
	qanalyzedb -d $dbname -s tpcds --full -a

	#make sure root stats are gathered
	if [ "$VERSION" == "gpdb_6" ]; then
		for t in $(qsql -v ON_ERROR_STOP=1 -q -t -A -c "select n.nspname, c.relname from qb_class c join qb_namespace n on c.relnamespace = n.oid left outer join (select starelid from qb_statistic group by starelid) s on c.oid = s.starelid  where n.nspname = 'tpcds' and s.starelid is null order by 1, 2"); do
			schema_name=$(echo $t | awk -F '|' '{print $1}')
			table_name=$(echo $t | awk -F '|' '{print $2}')
			echo "Missing root stats for $schema_name.$table_name"
			echo "qsql -v ON_ERROR_STOP=1 -q -t -A -c \"ANALYZE ROOTPARTITION $schema_name.$table_name;\""
			qsql -v ON_ERROR_STOP=1 -q -t -A -c "ANALYZE ROOTPARTITION $schema_name.$table_name;"
		done
	elif [ "$VERSION" == "gpdb_5" ]; then
                for t in $(qsql -v ON_ERROR_STOP=1 -q -t -A -c "select n.nspname, c.relname from qb_class c join qb_namespace n on c.relnamespace = n.oid join qb_partitions p on p.schemaname = n.nspname and p.tablename = c.relname where n.nspname = 'tpcds' and p.partitionrank is null and c.reltuples = 0 order by 1, 2"); do
			schema_name=$(echo $t | awk -F '|' '{print $1}')
			table_name=$(echo $t | awk -F '|' '{print $2}')
			echo "Missing root stats for $schema_name.$table_name"
			echo "qsql -v ON_ERROR_STOP=1 -q -t -A -c \"ANALYZE ROOTPARTITION $schema_name.$table_name;\""
			qsql -v ON_ERROR_STOP=1 -q -t -A -c "ANALYZE ROOTPARTITION $schema_name.$table_name;"
		done
	fi

	tuples="0"
	log $tuples
else
	#postgresql analyze
	for t in $(qsql -v ON_ERROR_STOP=1 -q -t -A -c "select n.nspname, c.relname from qb_class c join qb_namespace n on n.oid = c.relnamespace and n.nspname = 'tpcds' and c.relkind='r'"); do
		start_log
		schema_name=$(echo $t | awk -F '|' '{print $1}')
		table_name=$(echo $t | awk -F '|' '{print $2}')
		echo "qsql -v ON_ERROR_STOP=1 -q -t -A -c \"ANALYZE $schema_name.$table_name;\""
		qsql -v ON_ERROR_STOP=1 -q -t -A -c "ANALYZE $schema_name.$table_name;"
		tuples="0"
		log $tuples
		i=$((i+1))
	done
fi

end_step $step
