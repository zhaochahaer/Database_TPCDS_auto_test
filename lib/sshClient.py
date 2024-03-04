#!/usr/bin/env python
# -*- coding:UTF-8 -*-
'''
@File    :   pro_mian.py
@Time    :   2022/10/31
@Author  :   xin
@Version :   1.0
@Contact :   xin.zhao@esgyn.cn
'''
import paramiko

class sshClient:

    def conn(self,ip,username,password,port):
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(hostname=ip, port=port, username=username, password=password)
            return ssh
        except:
            print("ssh链接失败！！！")
            return False

    def close(self):
        if self.ssh :
            self.ssh.close()

sshclient = sshClient()