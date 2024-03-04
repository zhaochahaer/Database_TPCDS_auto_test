
from time import time
from lib.Logger import logger
from lib.dataBaseOption import *
import os
import time

class MppQbmconfig:
    def __init__(self,ssh_all,master_path):
        self.ssh = ssh_all
        self.path = master_path
        self.data_config_path=os.path.join(os.getcwd(), 'config')

    def getConfig(self,path_config):
        with open(path_config,'r+') as f:
            return f.read()

    def create_database(self):
        logger.info("Creating database qbadmin")
        cmd_check = 'su - qbadmin -c "qsql -d qianbase -c \\"select datname from qb_database;\\""'
        logger.info(cmd_check)
        stdin, stdout, stderr = self.ssh[0].exec_command(cmd_check)
        if "qbadmin" in stdout.read().decode():
            logger.warning("Database qbadmin already exists")
            return True
        else:
            cmd_create = 'su - qbadmin -c "qcreatedb qbadmin"'
            stdin, stdout, stderr = self.ssh[0].exec_command(cmd_create)
            for line in stderr.readlines():
                logger.error("Failed to create database qbadmin: " + line)
                assert "error" not in line
        return True

    
    def database_qbmconfig(self):
        data_config_name = os.path.join(self.data_config_path, 'data_config')
        data_config = self.getConfig(data_config_name)
        for i in data_config.rstrip().split('\n'):
            cmd = "su - qbadmin -c '" + i +"'"
            logger.info(cmd)
            stdin, stdout, stderr = self.ssh[0].exec_command(cmd)
            if "successfull" not in stdout.read().decode():
                logger.error(stdout.read().decode())
                assert "error" not in stdout.read().decode()
                return False
        return True

    def append_qb_hba_conf(self):
        logger.info("append_qb_hba_conf:host all all 0/0 trust")
        hba_path = os.path.join(self.path, 'qbseg-1', 'qb_hba.conf')
        with open(hba_path, 'a') as f:
            f.write('host     all         all             0/0            trust\n')

    
    def database_config(self):
        logger.info("database_qbmconfig...")
        dboption=DataBaseOption(self.ssh)
        if self.create_database():
            self.append_qb_hba_conf()
            if self.database_qbmconfig():
                #qstop -a -M immediate强制停止
                if dboption.database_qboption('qstop -a -M immediate'):
                    time.sleep(2)
                    if dboption.database_qboption('qstart'):
                        time.sleep(2)
                        logger.info("QianBaseMPP database qbmconfig Complete!")
                        return True
            



    