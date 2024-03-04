import configparser

class mppInfo:
    mpp_master_ip=None
    segment_list=None
    mpp_port=None
    perfermance_path=None
    ssh_name = None
    ssh_passwd=None
    ssh_port=None
    segment_num=None
    qbadmin_passwd=None
    tpcds_num=None
    mail_list=None

    def __init__(self):
        sys_config_path = "config/sys_config.ini"
        config = configparser.RawConfigParser()
        config.read(sys_config_path)
        self.perfermance_path=config.get('perfoermance','perfoermance_path')
        self.mpp_path=config.get('perfoermance','mpp_path')
        self.master_path=config.get('perfoermance','master_path')
        self.primary_path=config.get('perfoermance','primary_path')
        self.mpp_master_ip=config.get('sshConnection','master')
        self.segment_list=config.get('sshConnection','segment_list').split(',')
        self.ssh_name=config.get('sshConnection','ssh_name')
        self.ssh_passwd=config.get('sshConnection','ssh_passwd')
        self.ssh_port=config.get('sshConnection','port')
        self.segment_num=config.get('database','segment_num')
        self.qbadmin_passwd=config.get('database','qbadmin_passwd')
        self.tpcds_num=config.get('database','tpcds_num')
        self.mail_list=config.get('database','mail_list').split(',')
        
Userinof = mppInfo()