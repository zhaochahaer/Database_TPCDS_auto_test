#!/bin/bash
num=$1
# result=/opt/perftest/diff_mpp7_gp7/same_data_20230213/mpp7_samedata
result=$2

# db_version=20230213
db_version=$3

passwd=$4
seg_num=$5
#db=gp7
#segment_flag=gpseg
data_diri_source=/data1/greenplum7/primary
data_diri_target=/data1/qianbasempp7_v20230213/primary

#create database dbadmin in first run tpcds on this db
#qcreatedb qbadmin

#now must run on every segment node,will only run on master node by sshpass later
function copy_data
(
  for segment_id in `ls -d ${data_diri_source}/gpseg*|awk -F'gpseg' '{print $2}'`
  do
    cp -r ${data_diri_source}/gpseg${segment_id}/pivotalguru ${data_diri_target}/qbseg${segment_id}/
  done
)

function tpcds_run
(
  echo "" > ${result}/log_tpcds_list
  local rollout_file="./TPC-DS/rollout.sh"
  local initial_step_value=0

  if [ ! -f "${rollout_file}.bak" ]; then
    cp "${rollout_file}" "${rollout_file}.bak"
  else
    cp "${rollout_file}.bak" "${rollout_file}"
  fi

  for((id=1;id<=$num;id++))
  do
    current_date=$(date +%Y%m%d%H%M%S)
    mkdir ${result}/${current_date}
    
    if [ $id -eq 2 ]; then
      sed -i "s/if \[ \$step -ge $initial_step_value \]; then/if \[ \$step -ge 2 \]; then/g" "${rollout_file}"
    fi

    echo "$passwd" |./tpcds_mpp7.sh 2>&1 |tee -a ${result}/${current_date}/tpcds_1t_mpp7_v${db_version}_${seg_num}seg_zlib4_${current_date}.log
    cp -r ./TPC-DS/log ${result}/${current_date}/
    echo "${result}/${current_date}/tpcds_1t_mpp7_v${db_version}_${seg_num}seg_zlib4_${current_date}.log" >> ${result}/log_tpcds_list
    echo "End of the $id !"
    sleep 1800
  done
)

#copy_data;
tpcds_run;
