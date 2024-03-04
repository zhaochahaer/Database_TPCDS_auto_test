########################################################################################
TPC-DS benchmark scripts for QianBaseMPP database.
########################################################################################
Supported versions:
QianBaseMPP 6* 7*
Beta: PostgreSQL 10.*

########################################################################################
script
1.tpcds_run.sh,
1.1.function tpcds_run, This function is used to automatically execute tpcds multiple times, and the results will be collected at the specified path
1.2.function copy_data, This function is used to migrate the tpcds generated data under other data directories to the target database data directory; It is mainly used for the same tpcds data to test the import and query performance comparison between different database versions; At present, this function needs to be copied to each segment node for execution;

2.tpcds_mpp7/tpcds_mpp6/tpcds_gp7,These shell scripts are used to execute tpcds of different database versions. However, due to the replacement of keywords used internally, if the tpcds tool is executed in mpp6 and then in mpp7, you need to re-download the tpcds tool and execute tpcds_ Mpp7, which is replaced according to the mpp7 keyword adaptation rule;


########################################################################################


########################################################################################
functions:
1.add external tables parallel load function on big tables: 
1.1.add parallel load as follow tables: catalog_sales/store_sales/web_sales;
1.2.Currently, the number of external tables corresponding to each large table has been modified from 1 to the number of primary segment nodes, which is used to achieve parallel import of multiple external tables
1.3.Add switch:BigTableLoad
1.3.1. change files:
tpcds_variables.sh : BigTableLoad="true"
tpcds_mpp7.sh
tpcds/TPC-DS/rollout.sh
tpcds/TPC-DS/03_ddl/rollout.sh
tpcds/TPC-DS/04_load/rollout.sh

Concurrent import of large tables
1.4. add files:
03_ddl/ext_tpcds.catalog_sales
03_ddl/ext_tpcds.store_sales
03_ddl/ext_tpcds.web_sales
04_load/gpdb.catalog_sales
04_load/gpdb.store_sales
04_load/gpdb.web_sales
1.5. add function
03_dll/rollou.sh:
make_ext_big(): Concurrent import of external tables: building appearances
create_ext_tables():Concurrent import of external tables: creating appearances
04_load/rollou.sh:
make_inster_table():Concurrent import of external tables: creating an installer statement
load_date_table():Concurrent import of external tables

########################################################################################


########################################################################################
comments:
1.Many modifications have been made based on mpp, mainly keyword replacement
2.all replace in tpcds.sh,and operation is irreversible,for example, change the tpcds tool from mpp6 to mpp7,you need to download the tpcds tool from git,do not use the tpcds tool which have run on mpp6;
3.if you want to run the Specified steps,you can modify the "$step -ge 0":
vi TPC-DS/rollout.sh
#only run 05_sql,don't run
step=0

for i in $(ls -d $PWD/0*); do
#only run 05_sql and later procedure
#       if [ $step -eq 4 ]; then
#       if [ $step -ge 5 ]; then
        if [ $step -ge 0 ]; then
                echo "$i/rollout.sh"
                $i/rollout.sh $GEN_DATA_SCALE $EXPLAIN_ANALYZE $RANDOM_DISTRIBUTION $MULTI_USER_COUNT $SINGLE_USER_ITERATIONS
        fi
        step=$(($step+1))
done


########################################################################################

