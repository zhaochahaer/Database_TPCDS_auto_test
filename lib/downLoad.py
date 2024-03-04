#!/usr/bin/env python
# -*- coding:UTF-8 -*-
'''
@File    :   pro_mian.py
@Time    :   2023/05/08
@Author  :   xin
@Version :   1.0
@Contact :   xin.zhao@esgyn.cn
'''

import os
import hashlib
from datetime import datetime
import shutil
import wget
from lib.Logger import logger
import socket

class DownLoad:
    def __init__(self):
        self.package_dir = os.path.join(os.getcwd(), 'packages')
        self.package_url = 'http://10.14.40.24/qianbasempp/daily/{}/7/x86_64/'
        self.package_name = 'QianBaseMPP-7.0.0-1.el7.x86_64.{}.rpm'
        self.package_md5_name = 'QianBaseMPP-7.0.0-1.el7.x86_64.{}.rpm.md5'


    def download_daily_package(self):
        date = datetime.now().strftime('%Y%m%d')
        package_url = self.package_url.format(date)
        package_name = self.package_name.format(date)
        package_md5_name = self.package_md5_name.format(date)
        package_path = os.path.join(self.package_dir, package_name)
        package_md5_path = os.path.join(self.package_dir, package_md5_name)

        if os.path.exists(self.package_dir):
            shutil.rmtree(self.package_dir)
        os.makedirs(self.package_dir)

        if not os.path.exists(package_path):
            url = package_url + package_name
            wget.download(url, package_path)
            logger.info(f"\nDownloaded package: {package_name}")

        if not os.path.exists(package_md5_path):
            url_md5 = package_url + package_md5_name
            wget.download(url_md5, package_md5_path)
            logger.info('Downloaded package md5: {}'.format(package_md5_name))

        if self.verify_package_md5(package_path, package_md5_path):
            logger.info("Package verification passed.")
            return True
        else:
            logger.error("Package verification failed.")
            return False

    def verify_package_md5(self, package_path, package_md5_path):
        with open(package_md5_path, 'r') as f:
            md5sum, _ = f.read().split('  ')
        md5 = hashlib.md5(open(package_path, 'rb').read()).hexdigest()
        return md5 == md5sum

    def create_remote_dir(self, ssh, path):
        mkdir_cmd = 'mkdir -p {}'.format(path)
        logger.info('Creating remote directory: {}'.format(path))
        stdin, stdout, stderr = ssh.exec_command(mkdir_cmd)
        for line in stderr.readlines():
            logger.error('Error creating remote directory: {}'.format(line))
            return False
        return True

    def scp_package(self, ssh, package_path):
        sftp = ssh.open_sftp()
        remote_path = os.path.join(self.package_dir, os.path.basename(package_path))
        logger.info('Transferring package to remote host: {}'.format(remote_path))
        try:
            sftp.put(package_path, remote_path)
        except Exception as e:
            logger.error('Error transferring package to remote host: {}'.format(e))
            return False
        finally:
            sftp.close()
        return True

    def scp_packages(self, ssh_list):
        if not os.path.exists(self.package_dir):
            os.makedirs(self.package_dir)
        package_path = os.path.join(self.package_dir, self.package_name.format(datetime.now().strftime('%Y%m%d')))
        if not os.path.exists(package_path):
            self.download_daily_package()

        success_count = 0
        local_ip = socket.gethostbyname(socket.gethostname())
        for ssh in ssh_list:
            remote_ip = ssh.get_transport().getpeername()[0]
            if remote_ip == local_ip:
                success_count += 1
                continue
            ssh.exec_command('rm -rf {}/*'.format(self.package_dir))
            if self.create_remote_dir(ssh, self.package_dir) and self.scp_package(ssh, package_path):
                success_count += 1

        return success_count == len(ssh_list)


download=DownLoad()