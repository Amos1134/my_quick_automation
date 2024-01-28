print("Running Wallet Balance  on Portal Vend and First Platform")


#LTD Connection
import os
from dotenv import load_dotenv
load_dotenv()

username = os.getenv("db_user")
password = os.getenv("ltd_db_pass")
host = os.getenv("ltd_db_host")
port = 3306
database = os.getenv("database")

# Vend Connection
username2 = os.getenv("db_user")
password2 = os.getenv("vend_db_pass")
host2 = os.getenv("vend_db_host")
port2 = 3306
database2 = os.getenv("database")


# Portal Connection
username3 = os.getenv("db_user")
password3 = os.getenv("portal_db_pass")
host3 = os.getenv("portal_db_host")
port3 = 3306
database3 = os.getenv("database")


import datetime
import time
import calendar

yesterday_start_time = datetime.datetime.combine((datetime.date.today() - datetime.timedelta(days=1)), datetime.time(00, 00, 00)).strftime('%Y-%m-%d %H:%M:%S')
yesterday_end_time = datetime.datetime.combine((datetime.date.today() - datetime.timedelta(days=1)), datetime.time(23, 59, 59)).strftime('%Y-%m-%d %H:%M:%S')

date_time1 = yesterday_start_time
pattern1 = '%Y-%m-%d %H:%M:%S'

date_time2 = yesterday_end_time
pattern2 = '%Y-%m-%d %H:%M:%S'


start_time_epoch1 = int(time.mktime(time.strptime(date_time1, pattern1)))
end_time_epoch1 = int(time.mktime(time.strptime(date_time2, pattern2)))
print(start_time_epoch1)
print(end_time_epoch1)

back_slash = os.getenv("back_slash")
print(back_slash)

# folder_name = f"C:{back_slash}Users{back_slash}Amos{back_slash}Documents{back_slash}Credit Switch{back_slash}Merchant Record{back_slash}2022{back_slash}Wallet Balance Notification"
# print(folder_name)

# os.makedirs(folder_name, exist_ok=True) 
# print(folder_name)



import mysql.connector as mariadb
# import mariadb

import sys

# import sys
# sys.setrecursionlimit(10**5)
import numpy as np
import pandas as pd
# import schedule
import datetime as datetime
import time
from matplotlib.backends.backend_pdf import PdfPages
import statistics
from scipy.stats import pearsonr
from datetime import datetime
from datetime import timedelta
import pytz
from tzlocal import get_localzone
local_tz = get_localzone()
print(local_tz)
print(pytz.country_timezones["ng"])
# %matplotlib notebook
import matplotlib.pyplot as plt

pd.set_option('display.max_rows', 2000)
pd.set_option('display.max_columns', 70)
pd.set_option('display.width', 100)

print("Connecting to MariaDB...")
# Connect to MariaDB Platform
print("Connecting to MariaDB Platform...")
try:
    conn = mariadb.connect(
        user=username,
        password=password,
        host=host,
        port=port,
        database=database
    )
except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)

# Get Cursor
cur = conn.cursor()

print("Connected to MariaDB Successfull!!!")
print(" ")
print("Extracting report from Account Debits...")
process_start_time1 = datetime.now()
# select_ST = 'SELECT * from accountdebits where merchant = "315518" and dates between {} and {}'.format(start_time_epoch1, end_time_epoch1)
# select_ST = 'SELECT * from accountdebits where merchant = "315518"'

daily_utilization = f'select merchant, count(*) AS number_of_sales, SUM(amount) AS Total_face_value, SUM(netValue) AS Total_Net_value FROM accountdebits where tranx_status = "Successfull" and dates between "{start_time_epoch1}"and "{end_time_epoch1}" group by merchant ORDER BY number_of_sales'
merchant_balance = 'select id, merchant, balance FROM merchants ORDER BY balance'


# select_all = 'SELECT * from activity_log where requestId = "230921154031254836933294"'

cur.execute(daily_utilization)
daily_utilization_result1 = cur.fetchall()
cur.execute(merchant_balance)
merchant_balance1 = cur.fetchall()

# print(ST_result)

process_end_time1 = datetime.now()
print("Extraction time---", process_end_time1 - process_start_time1)# "Extraction time---", end_time - start_time
daily_utilization_result_final = pd.DataFrame(daily_utilization_result1)
merchant_balance_final = pd.DataFrame(merchant_balance1)
# print(ST_dataset)

daily_utilization_result_final.columns = ["Merchant", "Successful Count", "Successful FaceValue", "Successful Net Value"]
merchant_balance_final.columns = ["ID", "Merchant", "Wallet Balance"]
merchant_balance_final["Platform"] = "First Platform"

try:

    print("Connecting to VEND MariaDB Platform...")
    try:
        conn = mariadb.connect(
            user=username2,
            password=password2,
            host=host2,
            port=port2,
            database=database2
        )
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)

    # Get Cursor
    cur = conn.cursor()

    print("Connected to MariaDB Successfull!!!")
    print(" ")
    print("Extracting report from Account Debits...")
    process_start_time1 = datetime.now()
    # select_ST = 'SELECT * from accountdebits where merchant = "315518" and dates between {} and {}'.format(start_time_epoch1, end_time_epoch1)
    # select_ST = 'SELECT * from accountdebits where merchant = "315518"'

    vend_daily_utilization = f'select merchant, count(*) AS number_of_sales, SUM(amount) AS Total_face_value, SUM(netValue) AS Total_Net_value FROM accountdebits where tranx_status = "Successfull" and dates between "{start_time_epoch1}"and "{end_time_epoch1}" group by merchant ORDER BY number_of_sales'
    vend_merchant_balance = 'select id, merchant, balance FROM merchants ORDER BY balance'


    # select_all = 'SELECT * from activity_log where requestId = "230921154031254836933294"'

    cur.execute(vend_daily_utilization)
    vend_daily_utilization_result1 = cur.fetchall()
    cur.execute(vend_merchant_balance)
    vend_merchant_balance1 = cur.fetchall()

    # print(ST_result)

    process_end_time1 = datetime.now()
    print("Extraction time---", process_end_time1 - process_start_time1)# "Extraction time---", end_time - start_time
    vend_daily_utilization_result = pd.DataFrame(vend_daily_utilization_result1)
    vend_merchant_balance = pd.DataFrame(vend_merchant_balance1)


    vend_daily_utilization_result.columns = ["Merchant", "Successful Count", "Successful FaceValue", "Successful Net Value"]
    vend_merchant_balance.columns = ["ID", "Merchant", "Wallet Balance"]
    vend_merchant_balance["Platform"] = "Second Platform"
except:
    details = {
    'ID' : [0],
    'Merchant' : ["None"],
    'Wallet Balance' : [0.0]}
    df = pd.DataFrame(details)
    vend_merchant_balance = df
    vend_merchant_balance["Platform"] = "Second Platform"
    
    details2 = {
    'Merchant' : ["None"],
    'Successful Count' : [0],
    'Successful FaceValue' : [0.0],
    'Successful FaceValue':[0.0]}
    df2 = pd.DataFrame(details2)
    vend_daily_utilization_result = df2

print(vend_merchant_balance)

print("Connecting to Portal MariaDB Platform...")
try:
    conn = mariadb.connect(
        user=username3,
        password=password3,
        host=host3,
        port=port3,
        database=database3
    )
except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)

# Get Cursor
cur = conn.cursor()

print("Connected to MariaDB Successfull!!!")
print(" ")
print("Extracting report from Account Debits...")
process_start_time1 = datetime.now()
# select_ST = 'SELECT * from accountdebits where merchant = "315518" and dates between {} and {}'.format(start_time_epoch1, end_time_epoch1)
# select_ST = 'SELECT * from accountdebits where merchant = "315518"'

portal_daily_utilization = f'select merchant, count(*) AS number_of_sales, SUM(amount) AS Total_face_value, SUM(netValue) AS Total_Net_value FROM accountdebits where tranx_status = "Successfull" and dates between "{start_time_epoch1}"and "{end_time_epoch1}" group by merchant ORDER BY number_of_sales'
portal_merchant_balance = 'select id, merchant, balance FROM merchants ORDER BY balance'


# select_all = 'SELECT * from activity_log where requestId = "230921154031254836933294"'

cur.execute(portal_daily_utilization)
portal_daily_utilization_result1 = cur.fetchall()
cur.execute(portal_merchant_balance)
portal_merchant_balance1 = cur.fetchall()

# print(ST_result)

process_end_time1 = datetime.now()
print("Extraction time---", process_end_time1 - process_start_time1)# "Extraction time---", end_time - start_time
portal_daily_utilization_result = pd.DataFrame(portal_daily_utilization_result1)
portal_merchant_balance = pd.DataFrame(portal_merchant_balance1)


portal_daily_utilization_result.columns = ["Merchant", "Successful Count", "Successful FaceValue", "Successful Net Value"]
portal_merchant_balance.columns = ["ID", "Merchant", "Wallet Balance"]
portal_merchant_balance["Platform"] = "Third Platform"




tranx_date1 = process_end_time1
tranx_date = datetime.strftime(tranx_date1, '%Y-%m-%d %H-%M-%S')
tranx_date2 = datetime.strftime(tranx_date1, '%Y-%m-%d %H:%M:%S')
# tranx_date
daily_utilization_result_final["Platform"] = "First Platform"
vend_daily_utilization_result["Platform"] = "Second Platform"
portal_daily_utilization_result["Platform"] = "Third Platform"


daily_utilization_result = pd.concat([daily_utilization_result_final, vend_daily_utilization_result, portal_daily_utilization_result], ignore_index = True)
merchant_balance = pd.concat([merchant_balance_final, vend_merchant_balance, portal_merchant_balance], ignore_index = True)

utilization_summary = pd.merge(daily_utilization_result, merchant_balance, 
                               left_on = ["Merchant", "Platform"], 
                               right_on = ["ID", "Platform"], 
                               how = "right")

utilization_summary


utilization_summary.columns = ['Merchant_x', 'Successful Count', 'Successful FaceValue (Yesterday)', 
       'Successful Net Value (Yesterday)', "Platform", 'ID', 'Merchant', 'Current Wallet Balance']

utilization_summary_final = utilization_summary[['Merchant','Successful FaceValue (Yesterday)', 'Successful Net Value (Yesterday)',
                                                 'Current Wallet Balance', "Platform"]]
utilization_summary_final = utilization_summary_final.fillna(0)


# utilization_summary_final["Successful FaceValue (Yesterday)"] = utilization_summary_final["Successful FaceValue (Yesterday)"].apply(lambda x : '{0:,.2f}'.format(x))
utilization_summary_final["Current Wallet Balance"] = utilization_summary_final["Current Wallet Balance"].astype(float)
utilization_summary_final["Successful Net Value (Yesterday)"] = utilization_summary_final["Successful Net Value (Yesterday)"].astype(float)

utilization_summary_final["Estimated Utilization Day"] = round(utilization_summary_final["Current Wallet Balance"]/utilization_summary_final["Successful Net Value (Yesterday)"])
utilization_summary_final["Estimated Utilization Day"] = utilization_summary_final["Estimated Utilization Day"].fillna(0)

utilization_summary_final["Estimated Utilization Day"] = utilization_summary_final["Estimated Utilization Day"].apply(lambda x : '{0:,.0f}'.format(x)) + " Day(s)"
# utilization_summary_final["Estimated Utilization Day"] = utilization_summary_final["Estimated_day"]
# utilization_summary_final = utilization_summary_final.drop(["Estimated_day"], axis = 1)
utilization_summary_final["Successful Net Value (Yesterday)"] = utilization_summary_final["Successful Net Value (Yesterday)"].apply(lambda x : '{0:,.2f}'.format(x))
utilization_summary_final["Current Wallet Balance"] = utilization_summary_final["Current Wallet Balance"].apply(lambda x : '{0:,.2f}'.format(x))


list_of_merchant = ["EcoInstitution", "GLL Institution", "Gomoney", "MYD (Mo)", "MUN Innovations Ltd PostPaid", "PL Institution",
                    "PL Digital Institution","ST Institution Plc", "UB Institution for Africa Plc", "WM Institution", "WM Institution Al", 
                    "Access Institution Plc", "Lotus Institution Limited", "9 Payment Service Institution", "Payv Postpaid Limited ", 
                    "Payv Postpaid Limited", "Pr TRUST Institution", "OPTIMUS Institution LIMITED", "FIBO FINTECH LIMITED", 
                    "FL TECHNOLOGY SOLUTIONS LIMITED", "931mobileUSSDweb", "UNN Institution", "ST IBTC FINANCIAL SERVICES  LIMITED",
                    "RI Telll LTD"]

boolean_series = utilization_summary_final.Merchant.isin(list_of_merchant)
filtered_df = utilization_summary_final[boolean_series]
boolean_series
filtered_df = filtered_df.sort_values(["Successful FaceValue (Yesterday)"], ascending = False)
# filtered_df["Estimated utilization Day"]
filtered_df["Successful FaceValue (Yesterday)"] = filtered_df["Successful FaceValue (Yesterday)"].apply(lambda x : '{0:,.0f}'.format(x))


filtered_df


from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.message import EmailMessage
from email import encoders
import smtplib, ssl
# import pyodbc
from pretty_html_table import build_table
import re
import sys, ast
import subprocess


from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from subprocess import Popen, PIPE
msg = MIMEMultipart('alternative')

Email_Address = os.getenv("main_sender")
# Email_Address = os.getenv("bk_mail")

email_password2 =  os.getenv("sender_pass")
# email_password2 =  os.getenv("bk_apppass")

receiver_email = [Email_Address]
# copy_email = [Email_Address]
copy_email = [Email_Address]
# 
subject =f"check Wallet Balance Report for {tranx_date2}"

def send_balance_mail(body):
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = Email_Address
    message["To"] = receiver_email
    message["cc"] = copy_email

    
    body_content = body
    message.add_alternative(body_content, subtype="html")
        
    context = ssl.create_default_context()
    smtp = smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context)
    smtp.login(Email_Address, email_password2)
    smtp.send_message(message)
    #
    smtp.quit()
    
output1 = build_table(filtered_df[filtered_df["Platform"] == "First Platform"], "yellow_dark", text_align = "center")
output2 = build_table(filtered_df[filtered_df["Platform"] == "Second Platform"], "green_dark", text_align = "center")
output3 = build_table(filtered_df[filtered_df["Platform"] == "Third Platform"], "blue_dark", text_align = "center")


salutation1 = f"<br><strong>Hello Admin, <br> <br> Kindly find attached Wallet Balance report as at {tranx_date2}.</strong></br>"
check_ltd_platform = f"<br><strong>WALLET BALANCE AMONG POSTPAID MERCHANTS ON First Platform AS AT {tranx_date2}</strong></br>"
check_vend_platform = f"<br><strong>WALLET BALANCE AMONG POSTPAID MERCHANTS ON Second Platform AS AT {tranx_date2}</strong></br>"
check_portal_platform = f"<br><strong>WALLET BALANCE AMONG MERCHANTS ON Third Platform AS AT {tranx_date2}</strong></br>"

closing_remark = "<br><strong>Thank you</strong>"

output = salutation1 + "<br>" "<br>"+ check_ltd_platform + output1 + "<br>" "<br>" + check_vend_platform + output2 + "<br>" "<br>" + check_portal_platform + output3 + "<br>" "<br>" + closing_remark

send_balance_mail(output)

print('Email sent successfully')
