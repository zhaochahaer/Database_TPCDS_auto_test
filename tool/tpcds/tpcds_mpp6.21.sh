#!/bin/bash
set -e

PWD=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

MYCMD="tpcds.sh"
MYVAR="tpcds_variables.sh"
##################################################################################################################################################
# Functions
##################################################################################################################################################
check_variables()
{
	new_variable="0"

	### Make sure variables file is available
	if [ ! -f "$PWD/$MYVAR" ]; then
		touch $PWD/$MYVAR
		new_variable=$(($new_variable + 1))
	fi
	local count=$(grep "REPO=" $MYVAR | wc -l)
	if [ "$count" -eq "0" ]; then
		echo "REPO=\"TPC-DS\"" >> $MYVAR
		new_variable=$(($new_variable + 1))
	fi
	local count=$(grep "REPO_URL=" $MYVAR | wc -l)
	if [ "$count" -eq "0" ]; then
		echo "REPO_URL=\"https://github.com/pivotalguru/TPC-DS\"" >> $MYVAR
		new_variable=$(($new_variable + 1))
	fi
	local count=$(grep "ADMIN_USER=" $MYVAR | wc -l)
	if [ "$count" -eq "0" ]; then
		echo "ADMIN_USER=\"gpadmin\"" >> $MYVAR
		new_variable=$(($new_variable + 1))
	fi
	local count=$(grep "INSTALL_DIR=" $MYVAR | wc -l)
	if [ "$count" -eq "0" ]; then
		echo "INSTALL_DIR=\"/pivotalguru\"" >> $MYVAR
		new_variable=$(($new_variable + 1))
	fi
	local count=$(grep "EXPLAIN_ANALYZE=" $MYVAR | wc -l)
	if [ "$count" -eq "0" ]; then
		echo "EXPLAIN_ANALYZE=\"false\"" >> $MYVAR
		new_variable=$(($new_variable + 1))
	fi
	local count=$(grep "RANDOM_DISTRIBUTION=" $MYVAR | wc -l)
	if [ "$count" -eq "0" ]; then
		echo "RANDOM_DISTRIBUTION=\"false\"" >> $MYVAR
		new_variable=$(($new_variable + 1))
	fi
	local count=$(grep "MULTI_USER_COUNT" $MYVAR | wc -l)
	if [ "$count" -eq "0" ]; then
		echo "MULTI_USER_COUNT=\"5\"" >> $MYVAR
		new_variable=$(($new_variable + 1))
	fi
	local count=$(grep "GEN_DATA_SCALE" $MYVAR | wc -l)
	if [ "$count" -eq "0" ]; then
		echo "GEN_DATA_SCALE=\"3000\"" >> $MYVAR
		new_variable=$(($new_variable + 1))
	fi
	local count=$(grep "SINGLE_USER_ITERATIONS" $MYVAR | wc -l)
	if [ "$count" -eq "0" ]; then
		echo "SINGLE_USER_ITERATIONS=\"1\"" >> $MYVAR
		new_variable=$(($new_variable + 1))
	fi
	#00
	local count=$(grep "RUN_COMPILE_TPCDS" $MYVAR | wc -l)
	if [ "$count" -eq "0" ]; then
		echo "RUN_COMPILE_TPCDS=\"false\"" >> $MYVAR
		new_variable=$(($new_variable + 1))
	fi
	#01
	local count=$(grep "RUN_GEN_DATA" $MYVAR | wc -l)
	if [ "$count" -eq "0" ]; then
		echo "RUN_GEN_DATA=\"false\"" >> $MYVAR
		new_variable=$(($new_variable + 1))
	fi
	#02
	local count=$(grep "RUN_INIT" $MYVAR | wc -l)
	if [ "$count" -eq "0" ]; then
		echo "RUN_INIT=\"true\"" >> $MYVAR
		new_variable=$(($new_variable + 1))
	fi
	#03
	local count=$(grep "RUN_DDL" $MYVAR | wc -l)
	if [ "$count" -eq "0" ]; then
		echo "RUN_DDL=\"true\"" >> $MYVAR
		new_variable=$(($new_variable + 1))
	fi
	#04
	local count=$(grep "RUN_LOAD" $MYVAR | wc -l)
	if [ "$count" -eq "0" ]; then
		echo "RUN_LOAD=\"true\"" >> $MYVAR
		new_variable=$(($new_variable + 1))
	fi
	#05
	local count=$(grep "RUN_SQL" $MYVAR | wc -l)
	if [ "$count" -eq "0" ]; then
		echo "RUN_SQL=\"true\"" >> $MYVAR
		new_variable=$(($new_variable + 1))
	fi
	#06
	local count=$(grep "RUN_SINGLE_USER_REPORT" $MYVAR | wc -l)
	if [ "$count" -eq "0" ]; then
		echo "RUN_SINGLE_USER_REPORT=\"true\"" >> $MYVAR
		new_variable=$(($new_variable + 1))
	fi
	#07
	local count=$(grep "RUN_MULTI_USER" $MYVAR | wc -l)
	if [ "$count" -eq "0" ]; then
		echo "RUN_MULTI_USER=\"true\"" >> $MYVAR
		new_variable=$(($new_variable + 1))
	fi
	#08
	local count=$(grep "RUN_MULTI_USER_REPORT" $MYVAR | wc -l)
	if [ "$count" -eq "0" ]; then
		echo "RUN_MULTI_USER_REPORT=\"true\"" >> $MYVAR
		new_variable=$(($new_variable + 1))
	fi
	#09
	local count=$(grep "RUN_SCORE" $MYVAR | wc -l)
	if [ "$count" -eq "0" ]; then
		echo "RUN_SCORE=\"true\"" >> $MYVAR
		new_variable=$(($new_variable + 1))
	fi

        # add parameter of multi external table parallel load
        local count=$(grep "BigTableLoad" $MYVAR | wc -l)
        if [ "$count" -eq "0" ]; then
                echo "BigTableLoad=\"true\"" >> $MYVAR
                new_variable=$(($new_variable + 1))
        fi

	if [ "$new_variable" -gt "0" ]; then
		echo "There are new variables in the tpcds_variables.sh file.  Please review to ensure the values are correct and then re-run this script."
		exit 1
	fi
	echo "############################################################################"
	echo "Sourcing $MYVAR"
	echo "############################################################################"
	echo ""
	source $MYVAR
}

check_user()
{
	### Make sure root is executing the script. ###
	echo "############################################################################"
	echo "Make sure root is executing this script."
	echo "############################################################################"
	echo ""
	local WHOAMI=`whoami`
	if [ "$WHOAMI" != "root" ]; then
		echo "Script must be executed as root!"
		exit 1
	fi
}

yum_installs()
{
	### Install and Update Demos ###
	echo "############################################################################"
	echo "Install git, gcc, and bc with yum."
	echo "############################################################################"
	echo ""
	# Install git and gcc if not found
	local YUM_INSTALLED=$(yum --help 2> /dev/null | wc -l)
	local CURL_INSTALLED=$(gcc --help 2> /dev/null | wc -l)
	local GIT_INSTALLED=$(git --help 2> /dev/null | wc -l)
	local BC_INSTALLED=$(bc --help 2> /dev/null | wc -l)

	if [ "$YUM_INSTALLED" -gt "0" ]; then
		if [ "$CURL_INSTALLED" -eq "0" ]; then
			yum -y install gcc
		fi
		if [ "$GIT_INSTALLED" -eq "0" ]; then
			yum -y install git
		fi
		if [ "$BC_INSTALLED" -eq "0" ]; then
			yum -y install bc
		fi
	else
		if [ "$CURL_INSTALLED" -eq "0" ]; then
			echo "gcc not installed and yum not found to install it."
			echo "Please install gcc and try again."
			exit 1
		fi
		if [ "$GIT_INSTALLED" -eq "0" ]; then
			echo "git not installed and yum not found to install it."
			echo "Please install git and try again."
			exit 1
		fi
		if [ "$BC_INSTALLED" -eq "0" ]; then
			echo "bc not installed and yum not found to install it."
			echo "Please install bc and try again."
			exit 1
		fi
	fi
	echo ""
}

repo_init()
{
	### Install repo ###
	echo "############################################################################"
	echo "Install the github repository."
	echo "############################################################################"
	echo ""

	internet_down="0"
	for j in $(curl google.com 2>&1 | grep "Couldn't resolve host"); do
		internet_down="1"
	done

	if [ ! -d $INSTALL_DIR ]; then
		if [ "$internet_down" -eq "1" ]; then
			echo "Unable to continue because repo hasn't been downloaded and Internet is not available."
			exit 1
		else
			echo ""
			echo "Creating install dir"
			echo "-------------------------------------------------------------------------"
			mkdir $INSTALL_DIR
			chown $ADMIN_USER $INSTALL_DIR
		fi
	fi

	if [ ! -d $INSTALL_DIR/$REPO ]; then
		if [ "$internet_down" -eq "1" ]; then
			echo "Unable to continue because repo hasn't been downloaded and Internet is not available."
			exit 1
		else
			echo ""
			echo "Creating $REPO directory"
			echo "-------------------------------------------------------------------------"
			mkdir $INSTALL_DIR/$REPO
			chown $ADMIN_USER $INSTALL_DIR/$REPO
			su -c "cd $INSTALL_DIR; GIT_SSL_NO_VERIFY=true; git clone --depth=1 $REPO_URL" $ADMIN_USER
		fi
	else
		if [ "$internet_down" -eq "0" ]; then
			git config --global user.email "$ADMIN_USER@$HOSTNAME"
			git config --global user.name "$ADMIN_USER"
			su -c "cd $INSTALL_DIR/$REPO; GIT_SSL_NO_VERIFY=true; git fetch --all; git reset --hard origin/master" $ADMIN_USER
		fi
	fi
}

script_check()
{
	### Make sure the repo doesn't have a newer version of this script. ###
	echo "############################################################################"
	echo "Make sure this script is up to date."
	echo "############################################################################"
	echo ""
	# Must be executed after the repo has been pulled
	local d=`diff $PWD/$MYCMD $INSTALL_DIR/$REPO/$MYCMD | wc -l`

	if [ "$d" -eq "0" ]; then
		echo "$MYCMD script is up to date so continuing to TPC-DS."
		echo ""
	else
		echo "$MYCMD script is NOT up to date."
		echo ""
		cp $INSTALL_DIR/$REPO/$MYCMD $PWD/$MYCMD
		echo "After this script completes, restart the $MYCMD with this command:"
		echo "./$MYCMD"
		exit 1
	fi

}

echo_variables()
{
	echo "############################################################################"
	echo "REPO: $REPO"
	echo "REPO_URL: $REPO_URL"
	echo "ADMIN_USER: $ADMIN_USER"
	echo "INSTALL_DIR: $INSTALL_DIR"
	echo "MULTI_USER_COUNT: $MULTI_USER_COUNT"
	echo "############################################################################"
	echo ""
}

##################################################################################################################################################
# Body
##################################################################################################################################################

#check_user
check_variables
#yum_installs
#repo_init
#script_check
echo_variables

echo "update database version info:"
#Adapt to qianbasempp6.21

#change: database message in function.sh
sed -i "s/greenplum_path/qianbasempp_path/g" $INSTALL_DIR/$REPO/functions.sh
sed -i "s/WHEN POSITION ('Greenplum Database 6' IN version) > 0 THEN 'gpdb_6'/WHEN POSITION ('QianBaseMPP Database 6' IN version) > 0 THEN 'gpdb_6'/g" $INSTALL_DIR/$REPO/functions.sh
sed -i "s/get_gpfdist_port/get_qbfdist_port/g" $INSTALL_DIR/$REPO/functions.sh
sed -i "s/hostname -s/hostname/g" $INSTALL_DIR/$REPO/functions.sh

#change: function.sh,psql to qsql
if [ `qsql -v ON_ERROR_STOP=1 -t -A -c "SELECT version();"|grep "QianBaseMPP Database 6"|wc -l` -eq 1 ];then
        sed -i "s/psql /qsql /g" $INSTALL_DIR/$REPO/functions.sh
fi

##change: function.sh, change tablename pg_compression to qb_compression
if [ `qsql -v ON_ERROR_STOP=1 -t -A -c "SELECT version();"|grep "QianBaseMPP Database 6"|wc -l` -eq 1 ];then
        sed -i "s/pg_compression/qb_compression/g" $INSTALL_DIR/$REPO/functions.sh
fi

#change:gpstate in 01_gen_data/rollout.sh
sed -i "s/gpstate/qstate/g" $INSTALL_DIR/$REPO/01_gen_data/rollout.sh
#change:01_gen_data/rollout.sh,only for kylin environment,check get_count_generate_data function in cycle all the time in arm kylin os,replce ssh returned "Authorized users only. All activities may be monitored and reported."
if [ `uname -a|grep aarch64 |wc -l` -eq 1 ];then
  sed -i "s/\"bash -c 'ps -ef | grep generate_data.sh | grep -v grep | wc -l'\"/\"bash -c 'ps -ef | grep generate_data.sh | grep -v grep'\" | sed \"s\/Authorized users only. All activities may be monitored and reported.\/\/g\"|sed \"s\/\\\n\/\/g\" |wc -l/g" $INSTALL_DIR/$REPO/01_gen_data/rollout.sh
fi

#change:01_gen_data/rollout.sh,replace psql to qsql
if [ `qsql -v ON_ERROR_STOP=1 -t -A -c "SELECT version();"|grep "QianBaseMPP Database 6"|wc -l` -eq 1 ];then
         sed -i "s/psql/qsql/g" $INSTALL_DIR/$REPO/01_gen_data/rollout.sh
fi

#change:02_init/rollout.sh
sed -i "s/greenplum-db\/greenplum_path.sh/QianBaseMPP\/qianbasempp_path.sh/g" $INSTALL_DIR/$REPO/02_init/rollout.sh
sed -i "s/greenplum_path/qianbasempp_path/g" $INSTALL_DIR/$REPO/02_init/rollout.sh
sed -i "s/\$GREENPLUM_PATH/\/usr\/local\/QianBaseMPP\/qianbasempp_path.sh/g" $INSTALL_DIR/$REPO/02_init/rollout.sh
sed -i "s/gp_autostats_mode/qb_autostats_mode/g" $INSTALL_DIR/$REPO/02_init/rollout.sh
sed -i "s/gpconfig/qbmconfig/g" $INSTALL_DIR/$REPO/02_init/rollout.sh
sed -i "s/gpstop/qstop/g" $INSTALL_DIR/$REPO/02_init/rollout.sh
sed -i "s/pg_hba.conf/qb_hba.conf/g" $INSTALL_DIR/$REPO/02_init/rollout.sh
sed -i "s/postgresql.conf/qianbasetp.conf/g" $INSTALL_DIR/$REPO/02_init/rollout.sh
sed -i "s/LD_PRELOAD=\/lib64\/libz.so.1 ps/LD_PRELOAD=\/lib64\/libz.so.1/g" $INSTALL_DIR/$REPO/02_init/rollout.sh

#change:02_init/rollout.sh,replace psql to qsql
if [ `qsql -v ON_ERROR_STOP=1 -t -A -c "SELECT version();"|grep "QianBaseMPP Database 6"|wc -l` -eq 1 ];then
         sed -i "s/psql/qsql/g" $INSTALL_DIR/$REPO/02_init/rollout.sh
         sed -i "s/pg_hba/qb_hba/g" $INSTALL_DIR/$REPO/02_init/rollout.sh
         sed -i "s/qianbase.conf/qianbasetp.conf/g" $INSTALL_DIR/$REPO/02_init/rollout.sh
fi

#change: gpfdist in 03_ddl/rollout.sh
sed -i "s/gpfdist/qbfdist/g" $INSTALL_DIR/$REPO/03_ddl/rollout.sh
if [ `qsql -v ON_ERROR_STOP=1 -t -A -c "SELECT version();"|grep "QianBaseMPP Database 6"|wc -l` -eq 1 ];then
         sed -i "s/psql/qsql/g" $INSTALL_DIR/$REPO/03_ddl/rollout.sh
fi

#change:if current database version is mpp7,need to change as follow
if [ `qsql -v ON_ERROR_STOP=1 -t -A -c "SELECT version();"|grep "QianBaseMPP Database 6"|wc -l` -eq 1 ];then
	sed -i "s/join (select tablename from pg_partitions group by tablename) p on p.tablename = c.relname//g" $INSTALL_DIR/$REPO/04_load/rollout.sh
fi

#change: gpfdsit in 04_load/rollout.sh
sed -i "s/gpfdist/qbfdist/g" $INSTALL_DIR/$REPO/04_load/rollout.sh
if [ `qsql -v ON_ERROR_STOP=1 -t -A -c "SELECT version();"|grep "QianBaseMPP Database 6"|wc -l` -eq 1 ];then
         sed -i "s/psql/qsql/g" $INSTALL_DIR/$REPO/04_load/rollout.sh
         sed -i "s/pg_/qb_/g" $INSTALL_DIR/$REPO/04_load/rollout.sh
fi

#change: 04_load/rollout.sh,replace analyzedb to qanalyzedb
if [ `qsql -v ON_ERROR_STOP=1 -t -A -c "SELECT version();"|grep "QianBaseMPP Database 6"|wc -l` -eq 1 ];then
        sed -i "s/\<analyzedb/qanalyzedb/g" $INSTALL_DIR/$REPO/04_load/rollout.sh
	sed -i "s/\<analyzedb/qanalyzedb/g" $INSTALL_DIR/$REPO/04_load/analyze.sh
fi

#change:gpfdist in 04_load/stop_gpfdist.sh
if [ -f $INSTALL_DIR/$REPO/04_load/stop_gpfdist.sh ];then
   sed -i "s/gpfdist/qbfdist/g" $INSTALL_DIR/$REPO/04_load/stop_gpfdist.sh
   mv $INSTALL_DIR/$REPO/04_load/stop_gpfdist.sh $INSTALL_DIR/$REPO/04_load/stop_qbfdist.sh
fi

#change:gpfdist in 04_load/start_gpfdist.sh
if [ -f $INSTALL_DIR/$REPO/04_load/start_gpfdist.sh ];then
   sed -i "s/gpfdist/qbfdist/g" $INSTALL_DIR/$REPO/04_load/start_gpfdist.sh
   mv $INSTALL_DIR/$REPO/04_load/start_gpfdist.sh $INSTALL_DIR/$REPO/04_load/start_qbfdist.sh
fi

#change: 05_sql/rollout.sh
if [ `qsql -v ON_ERROR_STOP=1 -t -A -c "SELECT version();"|grep "QianBaseMPP Database 6"|wc -l` -eq 1 ];then
         sed -i "s/psql/qsql/g" $INSTALL_DIR/$REPO/05_sql/rollout.sh
fi

#change: 06_single_user_reports/rollout.sh
if [ `qsql -v ON_ERROR_STOP=1 -t -A -c "SELECT version();"|grep "QianBaseMPP Database 6"|wc -l` -eq 1 ];then
        sed -i "s/psql/qsql/g" $INSTALL_DIR/$REPO/06_single_user_reports/rollout.sh
	sed -i "s/pg_class/qb_class/g" $INSTALL_DIR/$REPO/06_single_user_reports/rollout.sh
	sed -i "s/pg_namespace/qb_namespace/g" $INSTALL_DIR/$REPO/06_single_user_reports/rollout.sh
fi

#change:07_multi_user/rollout.sh,replace "grep psql" to "grep qsql"
if [ `qsql -v ON_ERROR_STOP=1 -t -A -c "SELECT version();"|grep "QianBaseMPP Database 6"|wc -l` -eq 1 ];then
        sed -i "s/grep psql/grep qsql/g" $INSTALL_DIR/$REPO/07_multi_user/rollout.sh
fi

#change:07_multi_user/test.sh,replace psql to qsql
if [ `qsql -v ON_ERROR_STOP=1 -t -A -c "SELECT version();"|grep "QianBaseMPP Database 6"|wc -l` -eq 1 ];then
        sed -i "s/psql/qsql/g" $INSTALL_DIR/$REPO/07_multi_user/test.sh
fi

#change: 08_multi_user_reports/rollout.sh
if [ `qsql -v ON_ERROR_STOP=1 -t -A -c "SELECT version();"|grep "QianBaseMPP Database 6"|wc -l` -eq 1 ];then
        sed -i "s/psql/qsql/g" $INSTALL_DIR/$REPO/08_multi_user_reports/rollout.sh
	sed -i "s/pg_class/qb_class/g" $INSTALL_DIR/$REPO/08_multi_user_reports/rollout.sh
        sed -i "s/pg_namespace/qb_namespace/g" $INSTALL_DIR/$REPO/08_multi_user_reports/rollout.sh
fi


#change: 09_score/rollout.sh
if [ `qsql -v ON_ERROR_STOP=1 -t -A -c "SELECT version();"|grep "QianBaseMPP Database 6"|wc -l` -eq 1 ];then
         sed -i "s/psql/qsql/g" $INSTALL_DIR/$REPO/09_score/rollout.sh
fi

#r892 issue,table gp_segment_configuration does not exist,only qb_segment_configuration exists
if [ `qsql -v ON_ERROR_STOP=1 -t -A -c "SELECT version();"|grep "QianBaseMPP Database 6"|wc -l` -eq 1 ];then
         sed -i "s/gp_segment_configuration/qb_segment_configuration/g" $INSTALL_DIR/$REPO/functions.sh
         sed -i "s/gp_segment_configuration/qb_segment_configuration/g" $INSTALL_DIR/$REPO/01_gen_data/rollout.sh
         sed -i "s/gp_segment_configuration/qb_segment_configuration/g" $INSTALL_DIR/$REPO/02_init/rollout.sh
         sed -i "s/gp_segment_configuration/qb_segment_configuration/g" $INSTALL_DIR/$REPO/03_ddl/rollout.sh
         sed -i "s/gp_segment_configuration/qb_segment_configuration/g" $INSTALL_DIR/$REPO/04_load/rollout.sh
fi

su -l $ADMIN_USER -c "cd \"$INSTALL_DIR/$REPO\"; ./rollout.sh $GEN_DATA_SCALE $EXPLAIN_ANALYZE $RANDOM_DISTRIBUTION $MULTI_USER_COUNT $RUN_COMPILE_TPCDS $RUN_GEN_DATA $RUN_INIT $RUN_DDL $RUN_LOAD $RUN_SQL $RUN_SINGLE_USER_REPORT $RUN_MULTI_USER $RUN_MULTI_USER_REPORT $RUN_SCORE $SINGLE_USER_ITERATIONS $BigTableLoad"
