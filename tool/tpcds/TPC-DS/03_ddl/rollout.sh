#!/bin/bash
set -e

PWD=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $PWD/../functions.sh
source_bashrc

GEN_DATA_SCALE=$1
EXPLAIN_ANALYZE=$2
RANDOM_DISTRIBUTION=$3
MULTI_USER_COUNT=$4
SINGLE_USER_ITERATIONS=$5
BigTableLoad=$6

if [[ "$GEN_DATA_SCALE" == "" || "$EXPLAIN_ANALYZE" == "" || "$RANDOM_DISTRIBUTION" == "" || "$MULTI_USER_COUNT" == "" || "$SINGLE_USER_ITERATIONS" == "" ]]; then
	echo "You must provide the scale as a parameter in terms of Gigabytes, true/false to run queries with EXPLAIN ANALYZE option, true/false to use random distrbution, multi-user count, and the number of sql iterations."
	echo "Example: ./rollout.sh 100 false false 5 1"
	exit 1
fi

step=ddl
init_log $step
get_version

if [[ "$VERSION" == *"gpdb"* ]]; then
	filter="gpdb"
elif [ "$VERSION" == "postgresql" ]; then
	filter="postgresql"
else
	echo "ERROR: Unsupported VERSION $VERSION!"
	exit 1
fi

#Create tables
for i in $(ls $PWD/*.$filter.*.sql); do
	id=$(echo $i | awk -F '.' '{print $1}')
	schema_name=$(echo $i | awk -F '.' '{print $2}')
	table_name=$(echo $i | awk -F '.' '{print $3}')
	start_log

	if [ "$filter" == "gpdb" ]; then
		if [ "$RANDOM_DISTRIBUTION" == "true" ]; then
			DISTRIBUTED_BY="DISTRIBUTED RANDOMLY"
		else
			for z in $(cat $PWD/distribution.txt); do
				table_name2=$(echo $z | awk -F '|' '{print $2}')
				if [ "$table_name2" == "$table_name" ]; then
					distribution=$(echo $z | awk -F '|' '{print $3}')
				fi
			done
			DISTRIBUTED_BY="DISTRIBUTED BY (""$distribution"")"
		fi
	else
		DISTRIBUTED_BY=""
	fi

	echo "qsql -v ON_ERROR_STOP=1 -q -a -P pager=off -f $i -v SMALL_STORAGE=\"$SMALL_STORAGE\" -v MEDIUM_STORAGE=\"$MEDIUM_STORAGE\" -v LARGE_STORAGE=\"$LARGE_STORAGE\" -v DISTRIBUTED_BY=\"$DISTRIBUTED_BY\""
	qsql -v ON_ERROR_STOP=1 -q -a -P pager=off -f $i -v SMALL_STORAGE="$SMALL_STORAGE" -v MEDIUM_STORAGE="$MEDIUM_STORAGE" -v LARGE_STORAGE="$LARGE_STORAGE" -v DISTRIBUTED_BY="$DISTRIBUTED_BY"

	log
done

#Concurrent import of external tables: building appearances
make_ext_big()
{
	rm -rf $PWD/ext_tpcds.store_sales/*
	rm -rf $PWD/ext_tpcds.catalog_sales/*
	rm -rf $PWD/ext_tpcds.web_sales/*
	for j in $(cat $PWD/../segment_hosts.txt); do
		n1=$(echo $j | awk -F '.' '{print $1}')
		# echo ">>>>>>>>>>>>>>>>>>>$n1"
		echo "CREATE EXTERNAL TABLE ext_tpcds.store_sales_$n1
	(
		ss_sold_date_sk integer,
		ss_sold_time_sk integer,
		ss_item_sk int,
		ss_customer_sk integer,
		ss_cdemo_sk integer,
		ss_hdemo_sk integer,
		ss_addr_sk integer,
		ss_store_sk integer,
		ss_promo_sk integer,
		ss_ticket_number bigint,
		ss_quantity integer,
		ss_wholesale_cost numeric(7,2),
		ss_list_price numeric(7,2),
		ss_sales_price numeric(7,2),
		ss_ext_discount_amt numeric(7,2),
		ss_ext_sales_price numeric(7,2),
		ss_ext_wholesale_cost numeric(7,2),
		ss_ext_list_price numeric(7,2),
		ss_ext_tax numeric(7,2),
		ss_coupon_amt numeric(7,2),
		ss_net_paid numeric(7,2),
		ss_net_paid_inc_tax numeric(7,2),
		ss_net_profit numeric(7,2)
	)
	LOCATION (:LOCATION)
	FORMAT 'TEXT' (DELIMITER '|' NULL AS '' ESCAPE AS E'\\\');
	" > $PWD/ext_tpcds.store_sales/044.ext_tpcds.store_sales_$n1.sql

	echo "CREATE EXTERNAL TABLE ext_tpcds.catalog_sales_$n1 (like tpcds.catalog_sales)
LOCATION (:LOCATION)
FORMAT 'TEXT' (DELIMITER '|' NULL AS '' ESCAPE AS E'\\\');
" > $PWD/ext_tpcds.catalog_sales/029.ext_tpcds.catalog_sales_$n1.sql

	echo "CREATE EXTERNAL TABLE ext_tpcds.web_sales_$n1 (like tpcds.web_sales)
LOCATION (:LOCATION)
FORMAT 'TEXT' (DELIMITER '|' NULL AS '' ESCAPE AS E'\\\');
" > $PWD/ext_tpcds.web_sales/049.ext_tpcds.web_sales_$n1.sql

done
}

#Concurrent import of external tables: creating appearances
create_ext_tables()
{
    if [ "$filter" == "gpdb" ]; then
        get_qbfdist_port
        for i in $(ls $PWD/*.ext_tpcds.*.sql); do
            start_log

            id=$(echo $i | awk -F '.' '{print $1}')
            schema_name=$(echo $i | awk -F '.' '{print $2}')
            table_name=$(echo $i | awk -F '.' '{print $3}')

            counter=0
            #need parallel load table:store_sales/catalog_sales/web_sales
            if [ "$table_name" == "store_sales" ] || [ "$table_name" == "catalog_sales" ] || [ "$table_name" == "web_sales" ]; then
                for j in $(ls $PWD/ext_tpcds.${table_name}/*.ext_tpcds.*.sql); do
                    
                    s=$(echo $j | awk -F '.' '{print $(NF-1)}' | awk -F '_' '{print $NF}')
                    
                    counter1=0
                    
                        for x in $(qsql -v ON_ERROR_STOP=1 -q -A -t -c "select rank() over(partition by g.hostname order by g.datadir), g.hostname from gp_segment_configuration g where g.content >= 0 and g.role = 'p' order by g.hostname"); do
                        CHILD=$(echo $x | awk -F '|' '{print $1}')
                        EXT_HOST=$(echo $x | awk -F '|' '{print $2}')
                        EXT_HOST1=$(echo $x | awk -F '|' '{print $2}'| awk -F '.' '{print $1}')
                        
                        PORT=$(($GPFDIST_PORT + $CHILD))    
                        
                            if [ "$s" == "$EXT_HOST1" ];then
                                    if [ "$counter1" -eq "0" ]; then
                                        LOCATION="'"
                                    else
                                        LOCATION+="', '"
                                    fi

                                    LOCATION+="qbfdist://$EXT_HOST:$PORT/"$table_name"_[0-9]*_[0-9]*.dat"

                                    counter1=$(($counter1 + 1))
                            fi
                        done

                    LOCATION+="'"
                    echo "qsql -v ON_ERROR_STOP=1 -q -a -P pager=off -f $j -v LOCATION=\"$LOCATION\""
                    qsql -v ON_ERROR_STOP=1 -q -a -P pager=off -f $j -v LOCATION="$LOCATION"
                done
			else
                for x in $(qsql -v ON_ERROR_STOP=1 -q -A -t -c "select rank() over(partition by g.hostname order by g.datadir), g.hostname from gp_segment_configuration g where g.content >= 0 and g.role = 'p' order by g.hostname"); do
                    CHILD=$(echo $x | awk -F '|' '{print $1}')
                    EXT_HOST=$(echo $x | awk -F '|' '{print $2}')
                    PORT=$(($GPFDIST_PORT + $CHILD))

                    if [ "$counter" -eq "0" ]; then
                        LOCATION="'"
                    else
                        LOCATION+="', '"
                    fi

                    LOCATION+="qbfdist://$EXT_HOST:$PORT/"$table_name"_[0-9]*_[0-9]*.dat"

                    counter=$(($counter + 1))
                done
                LOCATION+="'"
                echo "qsql -v ON_ERROR_STOP=1 -q -a -P pager=off -f $i -v LOCATION=\"$LOCATION\""
                qsql -v ON_ERROR_STOP=1 -q -a -P pager=off -f $i -v LOCATION="$LOCATION"
            fi
        done
    fi
}

#Concurrent import of external tables: switch judgment
if [ "$BigTableLoad" == "true" ];then
	echo "Concurrent import of external tables"
	make_ext_big
	create_ext_tables

else
	#external tables are the same for all gpdb
	if [ "$filter" == "gpdb" ]; then

	get_qbfdist_port
	
	for i in $(ls $PWD/*.ext_tpcds.*.sql); do
		start_log

		id=$(echo $i | awk -F '.' '{print $1}')
		schema_name=$(echo $i | awk -F '.' '{print $2}')
		table_name=$(echo $i | awk -F '.' '{print $3}')

		counter=0

		if [ "$VERSION" == "gpdb_6" ]; then
			for x in $(qsql -v ON_ERROR_STOP=1 -q -A -t -c "select rank() over(partition by g.hostname order by g.datadir), g.hostname from gp_segment_configuration g where g.content >= 0 and g.role = 'p' order by g.hostname"); do
				CHILD=$(echo $x | awk -F '|' '{print $1}')
				EXT_HOST=$(echo $x | awk -F '|' '{print $2}')
				PORT=$(($GPFDIST_PORT + $CHILD))

				if [ "$counter" -eq "0" ]; then
					LOCATION="'"
				else
					LOCATION+="', '"
				fi

				LOCATION+="qbfdist://$EXT_HOST:$PORT/"$table_name"_[0-9]*_[0-9]*.dat"

				counter=$(($counter + 1))
			done
		else
			for x in $(qsql -v ON_ERROR_STOP=1 -q -A -t -c "select rank() over (partition by g.hostname order by p.fselocation), g.hostname from gp_segment_configuration g join pg_filespace_entry p on g.dbid = p.fsedbid join pg_tablespace t on t.spcfsoid = p.fsefsoid where g.content >= 0 and g.role = 'p' and t.spcname = 'pg_default' order by g.hostname"); do
				CHILD=$(echo $x | awk -F '|' '{print $1}')
				EXT_HOST=$(echo $x | awk -F '|' '{print $2}')
				PORT=$(($GPFDIST_PORT + $CHILD))

				if [ "$counter" -eq "0" ]; then
					LOCATION="'"
				else
					LOCATION+="', '"
				fi
				LOCATION+="qbfdist://$EXT_HOST:$PORT/"$table_name"_[0-9]*_[0-9]*.dat"

				counter=$(($counter + 1))
			done
		fi
		LOCATION+="'"

		echo "qsql -v ON_ERROR_STOP=1 -q -a -P pager=off -f $i -v LOCATION=\"$LOCATION\""
		qsql -v ON_ERROR_STOP=1 -q -a -P pager=off -f $i -v LOCATION="$LOCATION" 

		log
	done
fi

fi

end_step $step
