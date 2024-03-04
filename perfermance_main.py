#!/usr/bin/env python
# -*- coding:UTF-8 -*-
'''
@File    :   pro_mian.py
@Time    :   2023/05/05
@Author  :   xin
@Version :   1.0
@Contact :   xin.zhao@esgyn.cn
'''

from lib.Logger import logger
from lib.mppInfo import *
from lib.sshClient import *
from lib.downLoad import download
from lib.installPackage import *
from lib.mppQbmconfig import *
from datetime import datetime
from lib.resultProcess import *
from lib.sendMail import *
import os
import schedule


class Test:
    def __init__(self,ssh_all):
        self.ssh=ssh_all
        self.tpcds_path = os.path.join(os.getcwd(), 'tool')
        self.date = datetime.now().strftime('%Y%m%d')
        self.num=Userinof.tpcds_num
        self.tpcds_passwd=Userinof.qbadmin_passwd
        self.seg_num=Userinof.segment_num
        self.tpcds_result_path=os.path.join(os.getcwd(), 'result', 'tpcds')

    def chown_tpcds(self):
        cmd_chmod = f"chmod 755 {self.tpcds_path} -R"
        cmd_chown = f"chown -R qbadmin:qbadmin {self.tpcds_path} {self.tpcds_path}/../result -R"
        
        # Execute the chmod command
        ret_chmod = os.system(cmd_chmod)
        if ret_chmod != 0:
            # handle the error
            logger.error(f"Failed to execute command: {cmd_chmod}")
            return False
        # Execute the chown command
        ret_chown = os.system(cmd_chown)
        if ret_chown != 0:
            # handle the error
            logger.error(f"Failed to execute command: {cmd_chown}")
            return False
        # Both commands executed successfully
        logger.info("Permissions updated successfully")
        return True


    #run 运行TPC-DS：
    def run_tpcds(self):
        run_path = os.path.join(self.tpcds_path, 'tpcds')

        if not self.chown_tpcds():
            logger.error("Failed to set permissions for tpcds folder and files")
            return False
        
        logger.info("Running tpcds...")
        cmd = f'su - qbadmin -c "cd {run_path} && nohup ./tpcds_run.sh {self.num} {self.tpcds_result_path} {self.date} {self.tpcds_passwd} {self.seg_num}"'
        logger.info(f"Command: {cmd}")

        stdin, stdout, stderr = self.ssh[0].exec_command(cmd)
        channel = stdout.channel
        while not channel.exit_status_ready():
            # Only print data when there is data to print
            if channel.recv_ready():
                data = channel.recv(1024)
                logger.info(data.decode('utf-8'), extra={'end': ''})

        stdout.close()
        stderr.close()
        logger.info("TPC-DS finished running")
        return True


    #预留tpch的接口：
    def run_tpch(self,ssh):
        pass


def test_tpcds_main():
    logger.info("Starting performance testing...")
    try:
        ssh = sshclient.conn(Userinof.mpp_master_ip,Userinof.ssh_name,Userinof.ssh_passwd,Userinof.ssh_port)
        ssh_segments = []
        for ip in Userinof.segment_list:
            ssh_segments.append(sshclient.conn(ip,Userinof.ssh_name,Userinof.ssh_passwd,Userinof.ssh_port))

        if Userinof.mpp_master_ip in Userinof.segment_list :
            ssh_all=ssh_segments
        else:
            ssh_all=[ssh] + ssh_segments

        #下载rpm安装包并同步到每个节点：
        if download.scp_packages(ssh_all):
            logger.info("下载同步完成")
        else:
            logger.error("同步失败!!!")
            assert download.scp_packages(ssh_all), "同步失败!!!"

        #卸载与安装rpm:
        install=InstallPackage(ssh_all,ssh_segments,Userinof.master_path,Userinof.primary_path)
        if install.install_database() :
            logger.info("install Complete!")
        else:
            logger.error("install false!!!")
            assert False, "install false!!!"

        #qbmconfig配置数据库参数
        qbmconfig = MppQbmconfig(ssh_all,Userinof.master_path)
        if qbmconfig.database_config() :
            logger.info("qbmconfig配置完成!")
        else:
            logger.error("qbmconfig配置失败")
            assert False, "qbmconfig配置失败"

        #运行tpcds测试
        start_test_time=datetime.now()
        logger.info(f"TPC-DS start_test_time:{start_test_time}")
        tool=Test(ssh_all)     
        if tool.run_tpcds() :
            logger.info("run_tpcds Complete!")
        else:
            logger.error("run_tpcds false!")
            assert False, "run_tpcds false!"

        end_test_time=datetime.now()
        logger.info(f"TPC-DS end_test_time:{end_test_time}")
        
        #结果处理
        resultProcess=ResultProcess()
        list_logs=resultProcess.read_tpcds_log_list()
        excel_file=resultProcess.performance_tpcds_excel(list_logs)
        excel_file_all=resultProcess.performance_tpcds_excel_all(start_test_time,excel_file)

        #send mail
        mail.send(excel_file, excel_file_all)
        logger.info("Code executed successfully")
    except Exception as e:
        logger.error("An error occurred: {}".format(str(e)))
        mail.send_error_mail(logger.out)
        logger.error("Code execution failed") 


if __name__ == '__main__':
    #设置定时任务
    schedule.every().day.at("03:00").do(test_tpcds_main)

    while True:
        schedule.run_pending()
        time.sleep(1)

    # test_tpcds_main()