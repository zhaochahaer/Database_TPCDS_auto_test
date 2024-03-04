
from lib.mppInfo import *
from lib.Logger import logger
from lib.downLoad import download
import os
import socket

class InstallPackage:
    def __init__(self,ssh_all,ssh_segments,master_path,primary_path):
        self.ssh_all = ssh_all
        self.master_path = master_path
        self.primary_path = primary_path
        self.config_dir = '/home/qbadmin'
        self.ssh_segment=ssh_segments

    def check_datebase(self):
        logger.info("check packages")
        cmd = 'rpm -qa | grep QianBaseMPP'
        for ssh in self.ssh_all:
            stdin, stdout, stderr = ssh.exec_command('rpm -qa | grep QianBaseMPP')
            for line in stdout.readlines():
                if 'QianBaseMPP' in line:
                    logger.info("uninstall: "+'echo y | rpm -e {}'.format(line))
                    stdin, stdout, stderr = ssh.exec_command('echo y | rpm -e {}'.format(line))
                    for line in stderr.readlines():
                        logger.error("load error:" + line)
                        assert "error" not in line
            logger.info(f"{ssh.get_transport().getpeername()[0]} uninstall Complete!")
        return True

    def install_package(self):
        logger.info("install package:")
        package_dir = os.path.join(os.getcwd(), 'packages')
        package_name = os.path.basename(os.listdir(package_dir)[0])
        cmd = f'echo y | yum install {os.path.join(package_dir, package_name)}'
        if self.check_datebase():
            for ssh in self.ssh_all:
                logger.info(cmd)
                stdin, stdout, stderr = ssh.exec_command(cmd)
                if "Complete!" not in stdout.read().decode():
                    logger.error(stdout.read().decode())
                    return False
                logger.info(f"{ssh.get_transport().getpeername()[0]} install package Complete!")
        return True

    def cleanup_datebase(self):
        for i, ssh in enumerate(self.ssh_all):
            if i == 0:
                #存在一个bug需要修复--标记一下
                cmd = f'mkdir -p {self.master_path} && mkdir -p {self.primary_path} && chown -R qbadmin:qbadmin {self.master_path} {self.primary_path} && rm -rf {self.primary_path}/* && rm -rf {self.master_path}/* && rm -rf /tmp/.s.QBSQL.*.lock'
            else:
                cmd = f'mkdir -p {self.primary_path} && chown -R qbadmin:qbadmin {self.primary_path} && rm -rf {self.primary_path}/* && rm -rf /tmp/.s.QBSQL.*.lock'
            stdin, stdout, stderr = ssh.exec_command(cmd)
            exit_status = stdout.channel.recv_exit_status()
            if exit_status == 0:
                logger.info(f"Cleanup database on {ssh.get_transport().getpeername()[0]} succeeded.")
            else:
                logger.error(f"Cleanup database on {ssh.get_transport().getpeername()[0]} failed with exit code {exit_status}.")
                return False
        return True

    
    def write_hostfile(self):
        logger.info("write_hostfile")
        hostfile_path=os.path.join(self.config_dir, 'hostfile_qbinitsystem')

        with open(hostfile_path, 'w') as f:
            for ssh in self.ssh_segment:
                transport = ssh.get_transport()
                remote_ip = transport.getpeername()[0]
                remote_hostname = socket.gethostbyaddr(remote_ip)[0]
                logger.info(remote_hostname)
                f.write(f'{remote_hostname}\n')

        os.system('chown qbadmin:qbadmin {}'.format(hostfile_path))

    def write_qbinitsystem_config(self):
        logger.info("write_qbinitsystem_config")
        ssh_master=self.ssh_all[0]
        primary_path_str=self.primary_path
        qbinitsystem_config_path=os.path.join(self.config_dir, 'qbinitsystem_config')
        # Generate DATA_DIRECTORY
        data_directory_str = f"declare -a DATA_DIRECTORY=({primary_path_str}"
        data_directory_str += f" {primary_path_str}" * (int(Userinof.segment_num) - 1) + ")\n"
        logger.info(data_directory_str)
        # Generate qbinitsystem_config content
        content = f'''ARRAY_NAME="qianbase data Platform"
SEG_PREFIX=qbseg
PORT_BASE=4000
{data_directory_str}
MASTER_HOSTNAME={socket.gethostbyaddr(ssh_master.get_transport().getpeername()[0])[0]}
MASTER_DIRECTORY={self.master_path}
MASTER_PORT=5432
TRUSTED_SHELL=ssh
ENCODING=UNICODE
'''
        # Write qbinitsystem_config file
        with open(qbinitsystem_config_path, 'w') as f:
            f.write(content)

        os.system('chown qbadmin:qbadmin {}'.format(qbinitsystem_config_path))

    def write_bashrc(self):
        logger.info("write_bashrc")
        bashrc_path = os.path.join(self.config_dir, '.bashrc')

         # Generate .bashrc content
        content = f'''# .bashrc
# Source global definitions
if [ -f /etc/bashrc ]; then
        . /etc/bashrc
fi
PATH="$HOME/.local/bin:$HOME/bin:$PATH"
export PATH
# User specific aliases and functions
if [ -e /usr/local/QianBaseMPP ];then
  source {Userinof.mpp_path}
fi
export COORDINATOR_DATA_DIRECTORY={self.master_path}/qbseg-1
export QBPORT=5432
export LD_PRELOAD=/lib64/libz.so.1 ps
export QBUSER=qbadmin
         '''
        with open(bashrc_path, 'w') as f:
            f.write(content)

        os.system('chown qbadmin:qbadmin {}'.format(bashrc_path))


    def install_database(self):
        logger.info("install database...")
        hostfile_path=os.path.join(self.config_dir, 'hostfile_qbinitsystem')
        qbinitsystem_config_path=os.path.join(self.config_dir, 'qbinitsystem_config')
        if self.install_package() and self.cleanup_datebase():
            self.write_hostfile() 
            self.write_bashrc()
            self.write_qbinitsystem_config()
            cmd = "su - qbadmin --session-command 'qinitsystem -a -c {} -h {}'".format(qbinitsystem_config_path,hostfile_path)
            logger.info(cmd)
            stdin, stdout, stderr = self.ssh_all[0].exec_command(cmd)
            for line in stdout.readlines():
                logger.info(line)
                if "QianBaseMPP Database instance successfully created" in line:
                    logger.info("数据库安装成功")
                    return True
            logger.error("数据库初始化失败？？？")
            return False
            
