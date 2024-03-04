import smtplib
import os
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from lib.Logger import logger
from lib.mppInfo import *
from datetime import datetime
import pandas as pd

class SendMail:
    smtpsrvr = "mail.esgyn.cn"  # SMTP server address
    smtpport = 587  # SMTP server port
    username = "publicuser@esgyn.cn"  # SMTP username
    password = "D85vR42tt2"  # SMTP password
    sender = "publicuser@esgyn.cn"  # Sender email address

    # 读取Excel文件
    def read_excel_file(cls):
        logger.info("read_excel_file")
        excel_file_all=os.path.join(os.getcwd(),'html','tpcds',"performance_tpcds_all.xlsx")
        df = pd.read_excel(excel_file_all, sheet_name="TotalTime")
        max_row = df.shape[0]
        # first_row = df.iloc[0, :].tolist()
        if max_row <= 5:
            html_content = df.to_html(index=False)
            return html_content
        else:
            last_five_rows = df.iloc[-5:].to_html(index=False)
            # first_row_html = "<tr>" + "".join([f"<th>{i}</th>" for i in first_row]) + "</tr>"
            return last_five_rows

    #今天耗时对比：
    def today_comparison(cls):
        excel_file_all = os.path.join(os.getcwd(), 'html', 'tpcds', 'performance_tpcds_all.xlsx')
        df = pd.read_excel(excel_file_all, sheet_name="TotalTime")
        
        if len(df) <= 1:
            result = [0, 0, 0]
            return result
        else:
            result = []
            
            last_row_col4 = df.iloc[-1, 3]
            second_last_row_col4 = df.iloc[-2, 3]
            col4_percentage = round(((second_last_row_col4 - last_row_col4) / second_last_row_col4) * 100,2)
            result.append(col4_percentage)
            
            last_row_col5 = df.iloc[-1, 4]
            second_last_row_col5 = df.iloc[-2, 4]
            col5_percentage = round(((second_last_row_col5 - last_row_col5) / second_last_row_col5) * 100,2)
            result.append(col5_percentage)
            
            last_row_col6 = df.iloc[-1, 5]
            second_last_row_col6 = df.iloc[-2, 5]
            col6_percentage = round(((second_last_row_col6 - last_row_col6) / second_last_row_col6) * 100,2)
            result.append(col6_percentage)
            
            return result

    #读取config\data_config
    def read_data_config(cls):
        logger.info("read_data_config")
        lines=''
        data_config=os.path.join(os.getcwd(),'config',"data_config")
        with open(data_config,'r+') as f:
            for line in f.readlines():
                lines=lines + line + '<br>'
            return lines

    # 构建邮件内容
    def make_html_content(cls,excel_file,excel_file1):
        logger.info("make_html_content")
        result = cls.today_comparison()
        html_content = f"""
        <html>
        <body>
            Hi all:<br>
            &emsp;TPC-DS 日常性能基准测试报告如下：<br><br>
            <strong>1.  硬件环境：</strong><br>
            &emsp;a) CPU型号: Intel(R) Xeon(R) CPU E5-2630 v4 @ 2.20GHz<br>
            &emsp;b) CPU数量：48<br>
            &emsp;c) 内存: 256G<br>
            &emsp;d) 网络: Speed: 10000Mb/s 万兆网卡<br>
            &emsp;e) 磁盘: 6*1.8T sata机械盘*raid0:/data <br>
            &emsp;f) ip地址：{Userinof.mpp_master_ip},{','.join(Userinof.segment_list)}<br>
            <strong>2.  软件环境：</strong><br>
            &emsp;a)  数据库daily：{datetime.now().strftime('%Y%m%d')}<br>
            &emsp;b)  tpcds版本：1.0.0<br>
            &emsp;c)  QianBaseMPP7 master节点：{Userinof.mpp_master_ip}<br>
            &emsp;d)  QianBaseMPP7 数据节点：{','.join(Userinof.segment_list)}<br>
            &emsp;e)  primary:{int(Userinof.segment_num) * len(Userinof.segment_list)} 个<br>
            &emsp;f)  开启最大性能模式<br>
            <strong>3.  数据库配置：</strong><br>{cls.read_data_config()}
            <strong>4.  测试结果：</strong><br>
            &emsp;<strong>a) 近五天测试结果汇总：</strong><br>
            {cls.read_excel_file()}<br>
            
            &emsp;今日tpcds导入耗时，比较昨天tpcds导入耗时, 百分比为：{result[0]}%<br>
            &emsp;今日tpcds查询耗时，比较昨天tpcds查询耗时,百分比为：{result[1]}%<br>
            &emsp;今日tpcds总耗时，比较昨天tpcds总好耗时,百分比为：{result[2]}%<br>
            &emsp;<strong>b) 今日daily详细结果：</strong><br>
            &emsp;&emsp;请查看附件：{os.path.basename(excel_file)}<br>
            &emsp;<strong>c) 当日99条query与前一日的百分比对比:</strong><br>
            &emsp;&emsp;请查看附件：{os.path.basename(excel_file1)}<br>
            &emsp;<strong>d) 历史数据结果，请查看：</strong><br>
            &emsp;&emsp;链接：<a href="http://10.15.40.23/tpcds">http://10.15.40.23/tpcds</a><br><br>
            Best Regards,<br>
            赵鑫<br>
        </body>
        </html>
    """
        return html_content

    def send(cls,excel_file,excel_file1):
        message=cls.make_html_content(excel_file,excel_file1)
        recipient_list=Userinof.mail_list
        subject = f"[{datetime.now().strftime('%Y%m%d')}] Test Daily Performance Benchmark of TPC-DS"
        try:
            server = smtplib.SMTP(cls.smtpsrvr, cls.smtpport)  # Connect to the SMTP server
            server.starttls()  # Start TLS encryption
            server.login(cls.username, cls.password)  # Login to the SMTP server

            msg = MIMEMultipart()  # Create a new email message

            msg["From"] = cls.sender  # Set the sender of the email
            msg["To"] = ", ".join(recipient_list)  # Set the recipients of the email
            msg["Subject"] = subject  # Set the subject of the email

            msg.attach(MIMEText(message, "html"))  # Add the plain text message to the email

            # 添加附件
            with open(excel_file, 'rb') as f:
                attachment = MIMEApplication(f.read(), 'xlsx')
                attachment.add_header('Content-Disposition', 'attachment', filename=os.path.basename(excel_file))
                msg.attach(attachment)

            # 添加附件1
            with open(excel_file1, 'rb') as f:
                attachment = MIMEApplication(f.read(), 'xlsx')
                attachment.add_header('Content-Disposition', 'attachment', filename=os.path.basename(excel_file1))
                msg.attach(attachment)

            server.send_message(msg)  # Send the email
            server.quit()  # Disconnect from the SMTP server
            logger.info("Email sent successfully!")
            return True
        except smtplib.SMTPException as e:
            logger.error("An error occurred while sending the email:", e)
            return False

    def send_error_mail(cls,filename):
        # 构建邮件内容
        html_content = f"""
            <html>
            <body>
                Hi all:<br>
                &emsp;<strong style="color:red;">日常基准测试失败！请查看附件日志</strong><br>
                Best Regards,<br>
                赵鑫<br>
            </body>
            </html>
        """
        recipient_list=Userinof.mail_list
        subject = f"[{datetime.now().strftime('%Y%m%d')}] Test Daily Performance Fail!!!"
        try:
            server = smtplib.SMTP(cls.smtpsrvr, cls.smtpport)  # Connect to the SMTP server
            server.starttls()  # Start TLS encryption
            server.login(cls.username, cls.password)  # Login to the SMTP server

            msg = MIMEMultipart()  # Create a new email message

            msg["From"] = cls.sender  # Set the sender of the email
            msg["To"] = ", ".join(recipient_list)  # Set the recipients of the email
            msg["Subject"] = subject  # Set the subject of the email

            msg.attach(MIMEText(html_content, "html"))  # Add the plain text message to the email

            # 添加附件
            with open(filename, 'rb') as f:
                attachment = MIMEText(f.read(), 'base64', 'utf-8')
                attachment["Content-Type"] = "application/octet-stream"
                attachment["Content-Disposition"] = f'attachment; filename="{filename}"'
                msg.attach(attachment)

            server.send_message(msg)  # Send the email
            server.quit()  # Disconnect from the SMTP server
            logger.info("Email sent successfully!")
            return True
        except smtplib.SMTPException as e:
            logger.error("An error occurred while sending the email:", e)
            return False

mail=SendMail()