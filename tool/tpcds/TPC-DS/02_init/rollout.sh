#!/bin/bash
set -e

PWD=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $PWD/../functions.sh
source_bashrc

step=init
init_log $step
start_log
schema_name="tpcds"
table_name="init"

set_segment_bashrc()
{
	#this is only needed if the segment hosts don't have the bashrc file created
	echo "if [ -f /etc/bashrc ]; then" > $PWD/segment_bashrc
	echo "	. /etc/bashrc" >> $PWD/segment_bashrc
	echo "fi" >> $PWD/segment_bashrc
	echo "source /usr/local/QianBaseMPP/qianbasempp_path.sh" >> $PWD/segment_bashrc
	echo "export LD_PRELOAD=/lib64/libz.so.1" >> $PWD/segment_bashrc
	chmod 755 $PWD/segment_bashrc

	#copy generate_data.sh to ~/
	for ext_host in $(cat $PWD/../segment_hosts.txt); do
		# don't overwrite the master.  Only needed on single node installs
		shortname=$(echo $ext_host | awk -F '.' '{print $1}')
		if [ "$MASTER_HOST" != "$shortname" ]; then
			bashrc_exists=$(ssh $ext_host "ls ~/.bashrc" 2> /dev/null | wc -l)
			if [ "$bashrc_exists" -eq "0" ]; then
				echo "copy new .bashrc to $ext_host:$ADMIN_HOME"
				scp $PWD/segment_bashrc $ext_host:$ADMIN_HOME/.bashrc
			else
				count=$(ssh $ext_host "grep qianbasempp_path ~/.bashrc" 2> /dev/null | wc -l)
				if [ "$count" -eq "0" ]; then
					echo "Adding qianbasempp_path to $ext_host .bashrc"
					ssh $ext_host "echo \"source /usr/local/QianBaseMPP/qianbasempp_path.sh\" >> ~/.bashrc"
				fi
				count=$(ssh $ext_host "grep LD_PRELOAD ~/.bashrc" 2> /dev/null | wc -l)
				if [ "$count" -eq "0" ]; then
					echo "Adding LD_PRELOAD to $ext_host .bashrc"
					ssh $ext_host "echo \"export LD_PRELOAD=/lib64/libz.so.1\" >> ~/.bashrc"
				fi
			fi
		fi
	done
}
check_gucs()
{
	update_config="0"

	if [ "$VERSION" == "gpdb_5" ]; then
		counter=$(qsql -v ON_ERROR_STOP=1 -q -t -A -c "show optimizer_join_arity_for_associativity_commutativity" | grep -i "18" | wc -l; exit ${PIPESTATUS[0]})
		if [ "$counter" -eq "0" ]; then
			echo "setting optimizer_join_arity_for_associativity_commutativity"
			qbmconfig -c optimizer_join_arity_for_associativity_commutativity -v 18 --skipvalidation
			update_config="1"
		fi
	fi

	echo "check optimizer"
	counter=$(qsql -v ON_ERROR_STOP=1 -q -t -A -c "show optimizer" | grep -i "on" | wc -l; exit ${PIPESTATUS[0]})

	if [ "$counter" -eq "0" ]; then
		echo "enabling optimizer"
		qbmconfig -c optimizer -v on --masteronly
		update_config="1"
	fi

	echo "check analyze_root_partition"
	counter=$(qsql -v ON_ERROR_STOP=1 -q -t -A -c "show optimizer_analyze_root_partition" | grep -i "on" | wc -l; exit ${PIPESTATUS[0]})
	if [ "$counter" -eq "0" ]; then
		echo "enabling analyze_root_partition"
		qbmconfig -c optimizer_analyze_root_partition -v on --masteronly
		update_config="1"
	fi

	echo "check qb_autostats_mode"
	counter=$(qsql -v ON_ERROR_STOP=1 -q -t -A -c "show qb_autostats_mode" | grep -i "none" | wc -l; exit ${PIPESTATUS[0]})
	if [ "$counter" -eq "0" ]; then
		echo "changing qb_autostats_mode to none"
		qbmconfig -c qb_autostats_mode -v none --masteronly
		update_config="1"
	fi

	echo "check default_statistics_target"
	counter=$(qsql -v ON_ERROR_STOP=1 -q -t -A -c "show default_statistics_target" | grep "100" | wc -l; exit ${PIPESTATUS[0]})
	if [ "$counter" -eq "0" ]; then
		echo "changing default_statistics_target to 100"
		qbmconfig -c default_statistics_target -v 100
		update_config="1"
	fi

	if [ "$update_config" -eq "1" ]; then
		echo "update cluster because of config changes"
		qstop -u
	fi
}
copy_config()
{
	echo "copy config files"
	if [ "$MASTER_DATA_DIRECTORY" != "" ]; then
		cp $MASTER_DATA_DIRECTORY/qb_hba.conf $PWD/../log/
		cp $MASTER_DATA_DIRECTORY/qianbasetp.conf $PWD/../log/
	fi
	#gp_segment_configuration
	qsql -v ON_ERROR_STOP=1 -q -A -t -c "SELECT * FROM gp_segment_configuration" -o $PWD/../log/gp_segment_configuration.txt
}
set_search_path()
{
	echo "qsql -v ON_ERROR_STOP=1 -q -A -t -c \"ALTER USER $USER SET search_path=$schema_name,public;\""
	qsql -v ON_ERROR_STOP=1 -q -A -t -c "ALTER USER $USER SET search_path=$schema_name,public;"
}

get_version
if [[ "$VERSION" == *"gpdb"* ]]; then
	set_segment_bashrc
	check_gucs
	copy_config
fi
set_search_path

log

end_step $step
