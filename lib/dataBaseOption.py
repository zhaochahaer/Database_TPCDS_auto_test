
from lib.Logger import logger

class DataBaseOption:
    def __init__(self,ssh_all):
        self.ssh = ssh_all


    def database_qboption(self,option):
        logger.info("{} database mpp...".format(option))
        qboption = "su - qbadmin -c 'echo y|" + option + "'"
        logger.info(qboption)
        if "qstart" in option:
            stdin, stdout, stderr = self.ssh[0].exec_command(qboption)
            for line in stdout.readlines():
                logger.info(line)
                if "Database successfully started" in line:
                    logger.info("QianBaseMPP Database successfully started Complete!")
                    return True
        if "qstop" in option:
            stdin, stdout, stderr = self.ssh[0].exec_command(qboption)
            for line in stdout.readlines():
                logger.info(line)
                if "Database successfully shutdown" in line :
                     logger.info("QianBaseMPP Database successfully shutdown Complete!")
                     return True
        logger.error("{}database mpp fail? ".format(option))
        assert "error" not in line
        return False