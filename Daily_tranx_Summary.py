print("Daily Transactions Performance review and Overall monthly Update Feedback")

print("Extracting Daily Transactions Report")
print("Running Previous day Transactions")

from login import databasYX_connection as dBHH
from login import databasRV_connection as db2
from login import databasRV_connection as db3
import pandas as pd


from datetime import datetime
start_timer = datetime.now()
now_now =datetime.now()
from dotenv import load_dotenv
load_dotenv()

#LTD Connection
import os
back_slash = os.getenv("back_slash")
print(back_slash)

import datetime
import time
import calendar

day_to_extract = 1
month_at_hand = datetime.datetime.combine((datetime.date.today() - datetime.timedelta(days=day_to_extract)), datetime.time(23, 59, 59)).strftime('%Y-%m-%B')
month_at_hand2 = datetime.datetime.combine((datetime.date.today() - datetime.timedelta(days=day_to_extract)), datetime.time(23, 59, 59)).strftime('%Y_%m_%B')

print(month_at_hand)

folder_namYM = f".{back_slash}Raw_filYX"
folder_name = f".{back_slash}Raw_filYX{back_slash}Main_{month_at_hand2}"
folder_namRV = f".{back_slash}Daily_Performance_Review"
folder_name3 = f".{back_slash}CSW_DB_Tables"

os.makedirs(folder_namYM, exist_ok=True)
os.makedirs(folder_name, exist_ok=True)
os.makedirs(folder_namRV, exist_ok=True) 
os.makedirs(folder_name3, exist_ok=True)


print(folder_name)

# yesterday_start_time = datetime.datetime.combine((datetime.date.today()), datetime.time(00, 00, 00)).strftime('%Y-%m-%d %H:%M:%S')
# yesterday_end_time = datetime.datetime.combine((datetime.date.today()), datetime.time(23, 59, 59)).strftime('%Y-%m-%d %H:%M:%S')
# yesterday = datetime.datetime.combine((datetime.date.today()), datetime.time(23, 59, 59)).strftime('%Y-%m-%d')

yesterday_start_time = datetime.datetime.combine((datetime.date.today() - datetime.timedelta(days=day_to_extract)), datetime.time(00, 00, 00)).strftime('%Y-%m-%d %H:%M:%S')
yesterday_end_time = datetime.datetime.combine((datetime.date.today() - datetime.timedelta(days=day_to_extract)), datetime.time(23, 59, 59)).strftime('%Y-%m-%d %H:%M:%S')
yesterday = datetime.datetime.combine((datetime.date.today() - datetime.timedelta(days=day_to_extract)), datetime.time(23, 59, 59)).strftime('%Y-%m-%d')
full_month = datetime.datetime.combine((datetime.date.today() - datetime.timedelta(days=day_to_extract)), datetime.time(23, 59, 59)).strftime('%B')

print(full_month)
print(yesterday)
# print(folder_month)
print(folder_name)
# yesterday_start_time = '2023-06-11 00:00:00'
# yesterday_end_time = '2023-06-11 06:59:59'
date_timYX = yesterday_start_time
pattern1 = '%Y-%m-%d %H:%M:%S'

date_timRV = yesterday_end_time
pattern2 = '%Y-%m-%d %H:%M:%S'


start_time_epoch1 = int(time.mktime(time.strptime(date_timYX, pattern1)))
end_time_epoch1 = int(time.mktime(time.strptime(date_timRV, pattern2)))
print(start_time_epoch1)
print(end_time_epoch1)
print(date_timYX)
print(date_timRV)


import mysql.connector as mariadb
import sys
import numpy as np
import pandas as pd
import datetime as datetime
import time
from matplotlib.backends.backend_pdf import PdfPages
from datetime import datetime
from datetime import timedelta
import pytz
from tzlocal import get_localzone
local_tz = get_localzone()
print(local_tz)
print(pytz.country_timezones["ng"])
# %matplotlib notebook
import matplotlib.pyplot as plt


from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.message import EmailMessage
from email import encoders
import smtplib, ssl
# import pyodbc
from pretty_html_table import build_table

email_from = os.getenv("main_sender")
email_password =  os.getenv("sender_pass")
email_to = [email_from] 

pd.set_option('display.max_rows', 2000)
pd.set_option('display.max_columns', 70)
pd.set_option('display.width', 100)

table11 = os.getenv("table_name1")
table12 = os.getenv("table_name2")
table13 = os.getenv("table_name3")
table14 = os.getenv("table_name4")
table15 = os.getenv("table_name5")

DBNAME = os.getenv("database")



# select_all = f'select accdbts.merchant, accdbts.mert_RefId, accdbts.recipient, accdbts.amount, accdbts.service_code, accdbts.dates, accdbts.confirmCode, accdbts.request_id, accdbts.tranx_status, accdbts.netValue, accdbts.discount, accdbts.discountAmt, trax_exch.updated_at, trax_exch.batch_id, trax_exch.product_id, trax_exch.processing_server, FROM creditsw_Creditswitchmerh.accdbts inner join creditsw_Creditswitchmerh.trax_exch on accdbts.request_id = trax_exch.request_id where (accdbts.dates between "{start_time_epoch1}" and "{end_time_epoch1}")'
select_all = f'''select merh.merchant, accdbts.merchant, accdbts.mert_RefId, accdbts.recipient, accdbts.amount, 
accdbts.service_code, accdbts.dates, accdbts.confirmCode, accdbts.request_id, cast(accdbts.auditNo AS CHAR), 
accdbts.tranx_status, accdbts.netValue, accdbts.discount, accdbts.discountAmt, trax_exch.updated_at, 
trax_exch.batch_id, trax_exch.product_id, trax_exch.processing_server, provider.id, provider.network, 
provider.discount,  provider.provider, servp.service, accdbts.product 
FROM {DBNAME}.{table11} as accdbtes inner join {DBNAME}.{table12} as trax_exch
on accdbts.request_id = trax_exch.request_id left join "{DBNAME}".{table13} as merh
on accdbts.merchant = merh.id left join {DBNAME}.{table14} as servp 
on accdbts.service_code = servp.serviceId left join {DBNAME}.{table15} as provider 
on trax_exch.batch_id = provider.id where (accdbts.dates between "{start_time_epoch1}" and "{end_time_epoch1}")'''
print("Select command---", select_all)

columns_needed = ["Merchant", "Merchant ID", "M Ref", "Recipient", "FaceValue", "service_code", 
                  "dates", "Confirm Code", "Tranx ID", "Audit No","Status", "Net Val", 
                  "Discount(%)", "Discount(NGN)", "Updated Time", "Batch_ID", "Bundle", 
                  "Processing Server", "provider Id", "Network", "Provider Discount(%)", 
                  "Provider", "servp", "product2"]

dataset1 = dBHH(select_all, columns_needed)
dataset2 = db2(select_all, columns_needed)
dataset3 = db3(select_all, columns_needed)


# Joining all the Platform Datasets
all_dataset = pd.concat([dataset1, dataset2, dataset3], axis = 0)

print(all_dataset.head(4))

print("Count of all Transaction for the Day", len(all_dataset))
print(all_dataset["Network"].value_counts())
all_dataset["Actual_Network"] = all_dataset["servp"].str.split(" ",expand=True,).iloc[: ,:1]
all_dataset["Actual_Network"].value_counts()

# dummy network check
def dummy_check(row):
    if ((row["Network"] == "Dummy")):
        return row["Actual_Network"] + " " + row["Network"]
                 
    else:
        return row["Network"]

all_dataset = all_dataset.assign(dummy_checking=all_dataset.apply(dummy_check, axis=1))

all_dataset["Provider Discount(%)"] = pd.to_numeric(all_dataset["Provider Discount(%)"], errors='coerce')
all_dataset["FaceValue"] = pd.to_numeric(all_dataset["FaceValue"], errors='coerce')

all_dataset["Provider Discount(NGN)"] = round(((all_dataset["Provider Discount(%)"]/100) * all_dataset["FaceValue"]),2)
def set_column_type(row):
    if ((row["service_code"] == "BX4E") or (row["service_code"] == "BX1E") or\
        (row["service_code"] == "BX3E") or (row["service_code"] == "BX2E") or\
        (row["service_code"] == "BX6E")):
        return "Airtime"
      
    elif ((row["service_code"] == "DX4D") or (row["service_code"] == "DX2D") or\
          (row["service_code"] == "DX3D") or (row["service_code"] == "DX1D") or\
          (row["service_code"] == "DX5D")):
        return "Data"
              
    elif ((row["service_code"] == "XX4N") or (row["service_code"] == "XX2N") or\
          (row["service_code"] == "XX3N") or (row["service_code"] == "XX1N") or\
          (row["service_code"] == "XX5N") or (row["service_code"] == "XX6N") or\
          (row["service_code"] == "XX7N") or (row["service_code"] == "XX8N") or\
          (row["service_code"] == "XX9N") or (row["service_code"] == "P10N")):
        return "Pin"
              
    elif ((row["service_code"] == "V6TV") or (row["service_code"] == "V8TV") or\
          (row["service_code"] == "V7TV")):
        return "Cable Tv"
        
    elif ((row["service_code"] == "N01Q") or (row["service_code"] == "N01Q") or\
          (row["service_code"] == "N01Q")):
        return "ANQ"
          
    elif ((row["service_code"] == "YM5E") or (row["service_code"] == "YM1E") or\
          (row["service_code"] == "YM2E") or (row["service_code"] == "YM3E") or\
          (row["service_code"] == "YM4E") or (row["service_code"] == "YM6E") or\
          (row["service_code"] == "YM6E") or (row["service_code"] == "YM7E") or\
          (row["service_code"] == "YM8E") or (row["service_code"] == "YM9E") or\
          (row["service_code"] == "YX0E") or (row["service_code"] == "YX1E") or\
          (row["service_code"] == "YX2E") or (row["service_code"] == "YX3E") or\
          (row["service_code"] == "YX4E") or (row["service_code"] == "YX5E") or\
          (row["service_code"] == "YX6E") or (row["service_code"] == "YX7E") or\
          (row["service_code"] == "YX8E") or (row["service_code"] == "YX9E") or\
          (row["service_code"] == "RV0E") or (row["service_code"] == "RV1E") or\
          (row["service_code"] == "RV2E")):
        return "Electricity"

    
    elif ((row["service_code"] == "SL4M") or (row["service_code"] == "SMSK") or\
          (row["service_code"] == "SL2M") or (row["service_code"] == "SL3M") or\
          (row["service_code"] == "SL1M") or (row["service_code"] == "SO2M") or\
          (row["service_code"] == "SL5M") or (row["service_code"] == "SL6M") or\
          (row["service_code"] == "SL7M") or (row["service_code"] == "SO8M") or\
          (row["service_code"] == "SL9M") or (row["service_code"] == "SK0M") or\
          (row["service_code"] == "SK1M") or (row["service_code"] == "SK2M")):
        return "SMS"


    elif (row["service_code"] == "MJY1M"):
        return "Wallet to Wallet"
    elif (row["service_code"] == "MJY2M"):
        return "Wallet to Institute"
    elif (row["service_code"] == "1000"):
        return "Dummy"
    
    elif (row["service_code"] == "T01G"):
        return "Toll"
    
    elif ((row["service_code"] == "BHT1T") or (row["service_code"] == "BHT3T") or\
         (row["service_code"] == "BHT4T") or (row["service_code"] == "BHT5T") or\
         (row["service_code"] == "BHT6T") or (row["service_code"] == "BHT7T") or\
         (row["service_code"] == "BHT8T") or (row["service_code"] == "BHT9T") or\
         (row["service_code"] == "BHH0T") or (row["service_code"] == "BHH1T") or\
         (row["service_code"] == "BHH2T") or (row["service_code"] == "BHH3T") or\
         (row["service_code"] == "BHH4T")):
        return "Betting"
    
    else:
        return 
all_dataset = all_dataset.assign(Type=all_dataset.apply(set_column_type, axis=1))
all_dataset["Type"] = all_dataset["Type"].fillna(all_dataset["product2"])

all_dataset["Network"] = all_dataset['Network'].replace(["Mobile 2", "Mobile 2", "Mobile 2", "Mobile 2"], "Mobile 2")
all_dataset["Network"] = all_dataset['Network'].replace(["Mobile 1", "Mobile 1", "Mobile 1"], 'Mobile 1')
all_dataset["Status"] = all_dataset['Status'].replace(["Successfull", "successful", "success", "Success"], 'Successful')
all_dataset["Date"] = pd.to_datetime(pd.to_datetime(all_dataset['dates'], unit='s').dt.tz_localize("Africa/Lagos") + timedelta(hours=1)).dt.strftime("%Y-%m-%d %H:%M:%S")

all_dataset.columns = ['Merchant', 'Merchant ID', 'M Ref', 'Recipient', 'FaceValue', 'service_code', 'dates',
       'Confirm Code', 'Tranx ID', 'Audit No', 'Status', 'Net Val', 'Discount(%)', 'Discount(NGN)',
       'Updated Time', 'Batch_ID', 'Bundle', 'Processing Server', 'provider Id', 'Network',
       'Provider Discount(%)', 'Provider', 'servp', "product2", 'Platform', 'Network_actual',
       'Dummy Network', 'Provider Discount(NGN)', 'Type', 'Date']

all_dataset = all_dataset[['Merchant', 'Merchant ID', 'M Ref', 'Recipient', 'FaceValue', 'service_code', 'dates',
       'Confirm Code', 'Tranx ID', 'Audit No', 'Status', 'Net Val', 'Discount(%)', 'Discount(NGN)',
       'Updated Time', 'Batch_ID', 'Bundle', 'Processing Server', 'provider Id', 'Network',
       'Provider Discount(%)', 'Provider', 'servp', 'Platform', 'Network_actual',
       'Dummy Network', 'Provider Discount(NGN)', 'Type', 'Date']]

all_dataset_final = all_dataset[['Merchant', 'Recipient', 'Network', 'Provider', 'Type', 'FaceValue',  'Date', 'Status',
        'Discount(%)', 'Discount(NGN)', 'Net Val', 'Provider Discount(%)', "Provider Discount(NGN)", 'M Ref', 
        'Confirm Code', 'Tranx ID', 'Audit No', 'Processing Server', 'Updated Time', "Platform", "Dummy Network"]]
print(all_dataset_final)
print(all_dataset_final["Type"].value_counts())
print(all_dataset_final["Network"].value_counts())

daily_vtu_report = all_dataset_final[(all_dataset_final["Type"] == "Airtime") | (all_dataset_final["Type"] == "Data") | (all_dataset_final["Type"] == "Pin")]
daily_vtu_report
#Exporting file to folders created
daily_vtu_report.to_csv(fr"{folder_name}{back_slash}DWReport_{yesterday}.csv", index= None)
all_dataset_final.to_csv(fr"{folder_name3}{back_slash}CSW Overall_{yesterday}.csv", index= None)

print("VTU Daily Report successfully Exported!!!")
print(pd.concat([daily_vtu_report["Platform"].value_counts(), daily_vtu_report.groupby(["Platform"])["FaceValue"].sum()], axis = 1))


# Calculating Daily Summary Report

start_point = datetime.now()
all_failed_tranx = daily_vtu_report
# print(all_failed_tranx.columns)

all_failed_tranx.columns = ['Merchant', 'Recipient', 'Network', 'Provider', 'Type', 'FaceValue', 'Date', 'Status',
       'Discount(%)', 'Discount(NGN)', 'Net Val', 'Provider Discount(%)', 'Provider Discount(NGN)',
       'M Ref', 'Confirm Code', 'Tranx ID', 'Audit No', 'Processing Server', 'Updated Time', 'Platform', "Dummy Network"]
all_failed_tranx = all_failed_tranx.reset_index()

daily_error = all_failed_tranx#response_daily_review[response_daily_review["Status"] != "Successful"]
daily_error

start_time = datetime.now()
error_split2 = daily_error["Status"].str.split(" ",expand=True,)
error_split2
error_split12 = error_split2[0] + " " + error_split2[1] + " " + error_split2[2] #+ " " + error_split[3] + " " + error_split[4] + " " + error_split[5] + " " + error_split[6]
end_time = datetime.now()
print("processing time---", end_time - start_time)
error_split12


start_time = datetime.now()
# print(error_split12.value_counts())
daily_error.columns

daily_error["Error RGP"] = error_split12

daily_error["Error RGP"] = daily_error["Error RGP"].fillna(daily_error["Status"])
# print(daily_error["Error RGP"].value_counts())
end_time = datetime.now()
print("processing time---", end_time - start_time)


def set_column25(row):
    if row["Error RGP"] == "Limit on multiple":
        return "Limit on Multiple Recharges"
    elif row["Error RGP"] == "Your request cannot":
        return "Your request cannot be processed at this time, please try again later"
    elif row["Error RGP"]=="The previous request":
        return "The previous request of the recipient is under process, please try again later"
    elif row["Error RGP"]=="The mobile number":
        return "The mobile number cannot use this service within 0.00 minutes of last successful transaction"  
    elif row["Error RGP"]=="An error occurred":
        return "An error occurred while processing your request. We will be solving it shortly. Please try again later"
    elif row["Error RGP"] == "Your current request":
        return "Your current request to transfer cannot be processed as you do not have enough credit"
    elif row["Error RGP"] == "Your request to":
        return "Your request to transf eTopUP cant be processed as your current bal is low" 
    elif row["Error RGP"] == "Your account has":
        return "Your account has been credited back for failed Txn Id"
    else:
        return 
daily_error = daily_error.assign(Final_response=daily_error.apply(set_column25, axis=1))

daily_error["Final_response"] = daily_error["Final_response"].fillna(daily_error["Status"])
end_time = datetime.now()
print("processing time---", end_time - start_time)
# daily_error.head(2)

def set_columnba(row):
    if row["Merchant"] == "WM Institute" or row["Merchant"] == "WM AUTO REFUND":
        return "WM Institute"
    
    elif row["Merchant"] == "EcoInstitute" or row["Merchant"] == "EcoInstitute Auto Refund":
        return "Eco Institute"
    
    elif row["Merchant"] == "Access Institute Plc" or row["Merchant"] == "Access Institute":
        return "Access Institute Plc"
      
    elif row["Merchant"] == "United Institute for Africa Plc":
        return "United Institute for Africa Plc"
    elif row["Merchant"] == "ST Institute Plc" or row["Merchant"] == "ST Institute":
        return "ST Institute Plc"
    elif row["Merchant"] == "Munfat Innovations Limited" or row["Merchant"]== "Munfat Innovations Ltd PostPaid" or\
                            row["Merchant"] == "Munfat Auto Refund":
        return "Munfat Innovations Ltd PostPaid"
        
    elif row["Merchant"] == "Payvantage Postpaid Limited " or row["Merchant"]== "Payvantage Postpaid Limited":
        return "Payvantage Postpaid Limited"
    
    elif row["Merchant"] == "9 Payment Service Institute" :
        return "9 Payment Service Institute"
        
    elif row["Merchant"] == "Mobile 3 YDFS (MOW)":
        return "Mobile 3 YDFS (MOW)"
    elif row["Merchant"] == "Mobile 4bus Institute":
        return "Mobile 4bus Institute"
        
    elif row["Merchant"] == "FLT TECHNOLOGY SOLUTIONS LIMITED":
        return "FLT TECHNOLOGY SOLUTIONS LIMITED"

    elif row["Merchant"] == "Gom":
        return "Gom"
       
    elif row["Merchant"] == "54gene" or row["Merchant"] == "Fela MarketPlace" or\
         row["Merchant"] == "Greystone Partners Limited" or row["Merchant"] == "Roger Tomlinson" or\
         row["Merchant"] == "TM30 Mobile 4bal Limited" or row["Merchant"] == "iTechngdotnet Enterprises" or\
         row["Merchant"] == "730VIEWS" or row["Merchant"] == "Qualiquant servp Limited":
        return "Other API/Web"
#         return "Other API/Web"

    elif row["Merchant"] == "Postpaid Promo" or row["Merchant"] == "Polaris Promo" or\
         row["Merchant"] == "EcoInstitute Promo" or row["Merchant"] == "WM Alat Promo":
        return "Promo Account"
#         return "Promo"
    elif row["Merchant"] == "Csw Mobile 4 Switch" or row["Merchant"] == "Csw Mobile 4 Refunds":
        return "Mobile 4 Switching"

    elif row["Merchant"] == "Csw Test" or row["Merchant"] == "CSW Brand Test":
        return "Test account"
    
    elif row["Merchant"] == "931mobileUSSDweb":
        return "931 Customers"

    elif row["Merchant"] == "Mobile 3 MOW Refund" or row["Merchant"] == "WM Refunds" or row["Merchant"] == "Polaris Refund" or\
         row["Merchant"] == "WM Alat Refund" or row["Merchant"] == "Mobile 4 Switching Refund" or row["Merchant"]== "Polaris Refund" or\
         row["Merchant"] == "Polaris Digital Refund" or row["Merchant"] == "UBA Refund" or row["Merchant"] == "MOW REVERSALS" or\
         row["Merchant"] == "EcoInstitute Refund" or row["Merchant"] == "ST Refund" or row["Merchant"] == "Mobile 4bus Refund":
        return "Refund Account"
    else:
        return 

    
daily_error = daily_error.assign(Merchant_Name=daily_error.apply(set_columnba, axis=1))
daily_error["Merchant_Name"] = daily_error["Merchant_Name"].fillna(daily_error["Merchant"])
print("Merchant_Rep Completed...")

daily_error.columns = ['index', 'Merchant', 'Recipient', 'Network', 'Provider', 'Type', 'FaceValue', 'Date',
       'Status', 'Discount(%)', 'Discount(NGN)', 'Net Val', 'Provider Discount(%)',
       'Provider Discount(NGN)', 'M Ref', 'Confirm Code', 'Tranx ID', 'Audit No',
       'Processing Server', 'Updated Time', 'Platform', "Dummy Network", 'Error RGP', 'Current Status',
       'Merchant Name']


# print(daily_error["Network"].value_counts())

daily_error["Actual Network"] = daily_error['Dummy Network'].replace(["Mobile 2", "Mobile 2", "Mobile 2", "Mobile 2"], 'Mobile 2')
daily_error["Actual Network"] = daily_error['Dummy Network'].replace(["Mobile 1", "Mobile 1", "Mobile 1"], 'Mobile 1')
daily_error["Actual Network"] = daily_error['Actual Network'].replace(["Mobile 4", "Mobile 4"], 'Mobile 4')

daily_error["Actual Network"] = daily_error["Actual Network"].fillna(daily_error["Network"])
print(daily_error['Actual Network'].value_counts())
daily_error["Actual Network"] = daily_error['Actual Network'].replace(['Mobile 3100', 'Mobile 3200', 'Mobile 3500', 'Mobile 31000', 'Mobile 31500', "Mobile 3400", "Mobile 3750"], 'Mobile 3')
daily_error["Actual Network"] = daily_error['Actual Network'].replace(["Mobile 4", "Mobile 4"], 'Mobile 4')
daily_error["Actual Network"] = daily_error['Actual Network'].replace(["Mobile 1", "Mobile 1", "Mobile 1"], 'Mobile 1')
daily_error["Actual Network"] = daily_error['Actual Network'].replace([ "Mobile 2", "Mobile 2", '9mobilRV00', "9mobilYX00", "Mobile 2", "Mobile 2500", "9mobilYX000"], 'Mobile 2')

# print(daily_error['Actual Network'].value_counts())

daily_error["Duration"] = pd.to_datetime(daily_error["Date"]).dt.hour + 1

def set_column18(row):
    if row["Duration"] == 1:
        return "00:00 am - 00:59 am"
    elif row["Duration"] == 2:
        return "01:00 am - 01:59 am"
    elif row["Duration"] == 3:
        return "02:00 am - 02:59 am"
    elif row["Duration"] == 4:
        return "03:00 am - 03:59 am"
    elif row["Duration"] == 5:
        return "04:00 am - 04:59 am"
    elif row["Duration"] == 6:
        return "05:00 am - 05:59 am"
    elif row["Duration"] == 7:
        return "06:00 am - 06:59 am"
    elif row["Duration"] == 8:
        return "07:00 am - 07:59 am"
    elif row["Duration"] == 9:
        return "08:00 am - 08:59 am"
    elif row["Duration"] == 10:
        return "09:00 am - 09:59 am"
    elif row["Duration"] == 11:
        return "10:00 am - 10:59 am"
    elif row["Duration"] == 12:
        return "11:00 am - 11:59 am"
    elif row["Duration"] == 13:
        return "12:00 pm - 12:59 pm"
    elif row["Duration"] == 14:
        return "13:00 pm - 13:59 pm"
    elif row["Duration"] == 15:
        return "14:00 pm - 14:59 pm"
    elif row["Duration"] == 16:
        return "15:00 pm - 15:59 pm"
    elif row["Duration"] == 17:
        return "16:00 pm - 16:59 pm"
    elif row["Duration"] == 18:
        return "17:00 pm - 17:59 pm"
    elif row["Duration"] == 19:
        return "18:00 pm - 18:59 pm"
    elif row["Duration"] == 20:
        return "19:00 pm - 19:59 pm"
    elif row["Duration"] == 21:
        return "20:00 pm - 20:59 pm"
    elif row["Duration"] == 22:
        return "21:00 pm - 21:59 pm"
    elif row["Duration"] == 23:
        return "22:00 pm - 22:59 pm"
    elif row["Duration"] == 24:
        return "23:00 pm - 23:59 pm"
    else:
        return "Not found"
daily_error = daily_error.assign(Hour=daily_error.apply(set_column18, axis=1))
print("Duration1 Computation Completed...")


daily_tranx_final = daily_error
all_failed_tranx = daily_tranx_final[daily_tranx_final["Current Status"] != "Successful"]
# print(all_failed_tranx["Actual Network"].value_counts())
all_successful_tranx = daily_tranx_final[daily_tranx_final["Current Status"] == "Successful"]

daily_tranx_final["servp"] = daily_tranx_final["Actual Network"] + " " + daily_tranx_final["Type"]


service_Mobile 3_airtime = daily_tranx_final[(daily_tranx_final["Current Status"] == "Successful") & (daily_tranx_final["servp"] == "Mobile 3 Airtime")].groupby(["Merchant Name"])["FaceValue"].sum()
service_Mobile 3_airtime
service_Mobile 3_data = daily_tranx_final[(daily_tranx_final["Current Status"] == "Successful") & (daily_tranx_final["servp"] == "Mobile 3 Data")].groupby(["Merchant Name"])["FaceValue"].sum()

service_Mobile 1_airtime = daily_tranx_final[(daily_tranx_final["Current Status"] == "Successful") & (daily_tranx_final["servp"] == "Mobile 1 Airtime")].groupby(["Merchant Name"])["FaceValue"].sum()
# service_Mobile 3_airtime
service_Mobile 1_data = daily_tranx_final[(daily_tranx_final["Current Status"] == "Successful") & (daily_tranx_final["servp"] == "Mobile 1 Data")].groupby(["Merchant Name"])["FaceValue"].sum()

service_Mobile 4_airtime = daily_tranx_final[(daily_tranx_final["Current Status"] == "Successful") & (daily_tranx_final["servp"] == "Mobile 4 Airtime")].groupby(["Merchant Name"])["FaceValue"].sum()
service_Mobile 4_airtime
service_Mobile 4_data = daily_tranx_final[(daily_tranx_final["Current Status"] == "Successful") & (daily_tranx_final["servp"] == "Mobile 4 Data")].groupby(["Merchant Name"])["FaceValue"].sum()

service_9mo_airtime = daily_tranx_final[(daily_tranx_final["Current Status"] == "Successful") & ((daily_tranx_final["servp"] == "Mobile 2 Airtime") | (daily_tranx_final["servp"] == "Mobile 2 Airtime"))].groupby(["Merchant Name"])["FaceValue"].sum()
service_9mo_airtime
service_9mo_data = daily_tranx_final[(daily_tranx_final["Current Status"] == "Successful") & ((daily_tranx_final["servp"] == "Mobile 2 Data")| (daily_tranx_final["servp"] == "Mobile 2 Data"))].groupby(["Merchant Name"])["FaceValue"].sum()


service_9mo_pin = daily_tranx_final[(daily_tranx_final["Current Status"] == "Successful") & (daily_tranx_final["servp"] == "Mobile 2 Pin")].groupby(["Merchant Name"])["FaceValue"].sum()
service_Mobile 3_pin = daily_tranx_final[(daily_tranx_final["Current Status"] == "Successful") & (daily_tranx_final["servp"] == "Mobile 3 Pin")].groupby(["Merchant Name"])["FaceValue"].sum()
service_Mobile 4_pin = daily_tranx_final[(daily_tranx_final["Current Status"] == "Successful") & (daily_tranx_final["servp"] == "Mobile 4 Pin")].groupby(["Merchant Name"])["FaceValue"].sum()
service_Mobile 1_pin = daily_tranx_final[(daily_tranx_final["Current Status"] == "Successful") & (daily_tranx_final["servp"] == "Mobile 1 Pin")].groupby(["Merchant Name"])["FaceValue"].sum()


service_report = pd.concat([service_Mobile 3_airtime, service_Mobile 3_data, service_Mobile 1_airtime, service_Mobile 1_data,
                            service_Mobile 4_airtime, service_Mobile 4_data, service_9mo_airtime, service_9mo_data, 
                            service_Mobile 3_pin, service_Mobile 1_pin, service_Mobile 4_pin, service_9mo_pin], axis = 1)
service_report = service_report.fillna(0)
service_report.columns = ["Mobile 3 Airtime", "Mobile 3 Data", "Mobile 1 Airtime", "Mobile 1 Data",
                            "Mobile 4 Airtime", "Mobile 4 Data", "Mobile 2 Airtime", "Mobile 2 Data", 
                            "Mobile 3 Pin", "Mobile 1 Pin", "Mobile 4 Pin", "Mobile 2 Pin"]
service_report = service_report.sort_values(["Mobile 3 Airtime"], ascending = False)
service_report.loc['Grand Total',:]= service_report.sum(axis=0)
service_report

service_report["Mobile 3 Airtime Cont"] = round((service_report["Mobile 3 Airtime"] / service_report["Mobile 3 Airtime"].iloc[-1]) * 100, 1).astype(str) + "%"
service_report["Mobile 3 Data Cont"] = round((service_report["Mobile 3 Data"] / service_report["Mobile 3 Data"].iloc[-1]) * 100, 1).astype(str) + "%"

service_report["Mobile 1 Airtime Cont"] = round((service_report["Mobile 1 Airtime"] / service_report["Mobile 1 Airtime"].iloc[-1]) * 100, 1).astype(str) + "%"
service_report["Mobile 1 Data Cont"] = round((service_report["Mobile 1 Data"] / service_report["Mobile 1 Data"].iloc[-1]) * 100, 1).astype(str) + "%"

service_report["Mobile 4 Airtime Cont"] = round((service_report["Mobile 4 Airtime"] / service_report["Mobile 4 Airtime"].iloc[-1]) * 100, 1).astype(str) + "%"
service_report["Mobile 4 Data Cont"] = round((service_report["Mobile 4 Data"] / service_report["Mobile 4 Data"].iloc[-1]) * 100, 1).astype(str) + "%"

service_report["Mobile 2 Airtime Cont"] = round((service_report["Mobile 2 Airtime"] / service_report["Mobile 2 Airtime"].iloc[-1]) * 100, 1).astype(str) + "%"
service_report["Mobile 2 Data Cont"] = round((service_report["Mobile 2 Data"] / service_report["Mobile 2 Data"].iloc[-1]) * 100, 1).astype(str) + "%"
service_report

service_report['Mobile 3 Airtime'] = service_report['Mobile 3 Airtime'].apply(lambda x : '{0:,.0f}'.format(x))
service_report['Mobile 3 Data'] = service_report['Mobile 3 Data'].apply(lambda x : '{0:,.0f}'.format(x))

service_report['Mobile 1 Airtime'] = service_report['Mobile 1 Airtime'].apply(lambda x : '{0:,.0f}'.format(x))
service_report['Mobile 1 Data'] = service_report['Mobile 1 Data'].apply(lambda x : '{0:,.0f}'.format(x))

service_report['Mobile 4 Airtime'] = service_report['Mobile 4 Airtime'].apply(lambda x : '{0:,.0f}'.format(x))
service_report['Mobile 4 Data'] = service_report['Mobile 4 Data'].apply(lambda x : '{0:,.0f}'.format(x))

service_report['Mobile 2 Airtime'] = service_report['Mobile 2 Airtime'].apply(lambda x : '{0:,.0f}'.format(x))
service_report['Mobile 2 Data'] = service_report['Mobile 2 Data'].apply(lambda x : '{0:,.0f}'.format(x))


service_report['Mobile 3 Pin'] = service_report['Mobile 3 Pin'].apply(lambda x : '{0:,.0f}'.format(x))
service_report['Mobile 1 Pin'] = service_report['Mobile 1 Pin'].apply(lambda x : '{0:,.0f}'.format(x))
service_report['Mobile 4 Pin'] = service_report['Mobile 4 Pin'].apply(lambda x : '{0:,.0f}'.format(x))
service_report['Mobile 2 Pin'] = service_report['Mobile 2 Pin'].apply(lambda x : '{0:,.0f}'.format(x))

service_report


service_report["Mobile 3 Airtime"] = service_report["Mobile 3 Airtime"] + " (" + service_report["Mobile 3 Airtime Cont"] + ")"
service_report["Mobile 3 Data"] = service_report["Mobile 3 Data"] + " (" + service_report["Mobile 3 Data Cont"] + ")"

service_report["Mobile 1 Airtime"] = service_report["Mobile 1 Airtime"] + " (" + service_report["Mobile 1 Airtime Cont"] + ")"
service_report["Mobile 1 Data"] = service_report["Mobile 1 Data"] + " (" + service_report["Mobile 1 Data Cont"] + ")"

service_report["Mobile 4 Airtime"] = service_report["Mobile 4 Airtime"] + " (" + service_report["Mobile 4 Airtime Cont"] + ")"
service_report["Mobile 4 Data"] = service_report["Mobile 4 Data"] + " (" + service_report["Mobile 4 Data Cont"] + ")"

service_report["Mobile 2 Airtime"] = service_report["Mobile 2 Airtime"] + " (" + service_report["Mobile 2 Airtime Cont"] + ")"
service_report["Mobile 2 Data"] = service_report["Mobile 2 Data"] + " (" + service_report["Mobile 2 Data Cont"] + ")"
service_report_final = service_report[['Mobile 3 Airtime', 'Mobile 3 Data', 'Mobile 1 Airtime', 'Mobile 1 Data', 
                                       'Mobile 4 Airtime', 'Mobile 4 Data', 'Mobile 2 Airtime', 'Mobile 2 Data', 
                                       'Mobile 3 Pin', 'Mobile 1 Pin', 'Mobile 4 Pin', 'Mobile 2 Pin']].reset_index()
# print(service_report_final)
service_report = service_report.reset_index()
service_report

payv_data = service_report[(service_report["Merchant Name"] == "P Postpaid Limited")]["Mobile 3 Data Cont"].to_string(index=False)
payv_data

wem_data = service_report[(service_report["Merchant Name"] == "W Institute Alat")]["Mobile 3 Data Cont"].to_string(index=False)
wem_data

ste_data = service_report[(service_report["Merchant Name"] == "ST Institute Plc")]["Mobile 3 Data Cont"].to_string(index=False)
ste_data

pl_ussd_data = service_report[(service_report["Merchant Name"] == "PL Institute")]["Mobile 3 Data Cont"].to_string(index=False)
pl_ussd_data

pl_dig_data = service_report[(service_report["Merchant Name"] == "PLD Institute")]["Mobile 3 Data Cont"].to_string(index=False)
pl_dig_data

mun_data = service_report[(service_report["Merchant Name"] == "MU Innovations Ltd PostPaid")]["Mobile 3 Data Cont"].to_string(index=False)
mun_data

ub_data = service_report[(service_report["Merchant Name"] == "UNB Institute for Africa Plc")]["Mobile 3 Data Cont"].to_string(index=False)
ub_data

comment = f"""PayV contributed {payv_data} to our Mobile 3 data transactions, UB contibuted {ub_data} to our Mobile 3 data transactions, 
WEM contributed {wem_data} to our Mobile 3 data transactions, ST Institute contributed {ste_data} to our Mobile 3 data transactions, 
PL USSD contributed {pl_ussd_data} to our Mobile 3 data transactions, PL Dig contributed {pl_dig_data} to our Mobile 3 data transactions, MU contributed {mun_data} to our Mobile 3 data transactions"""
print(comment)


# Computing Merchant Performance Summary

total_merchant_count = daily_tranx_final.groupby(["Merchant Name"])["Status"].count()
total_merchant_value = daily_tranx_final.groupby(["Merchant Name"])["FaceValue"].sum()

Merchant_count = all_failed_tranx.groupby(["Merchant Name"])["Status"].count()
Merchant_value = all_failed_tranx.groupby(["Merchant Name"])["FaceValue"].sum()

successful_merchant_count = all_successful_tranx.groupby(["Merchant Name"])["Status"].count()
successful_merchant_value = all_successful_tranx.groupby(["Merchant Name"])["FaceValue"].sum()


Merchant_Mobile 3 = all_failed_tranx[all_failed_tranx["Actual Network"] == "Mobile 3"].groupby(["Merchant Name"])["Status"].count()
Merchant_Mobile 1 = all_failed_tranx[all_failed_tranx["Actual Network"] == "Mobile 1"].groupby(["Merchant Name"])["Status"].count()
Merchant_Mobile 4 = all_failed_tranx[all_failed_tranx["Actual Network"] == "Mobile 4"].groupby(["Merchant Name"])["Status"].count()
Merchant_Mobile 2 = all_failed_tranx[all_failed_tranx["Actual Network"] == "Mobile 2"].groupby(["Merchant Name"])["Status"].count()
Merchant_Mobile 3_dummy = all_failed_tranx[all_failed_tranx["Actual Network"] == "Mobile 3 Dummy"].groupby(["Merchant Name"])["Status"].count()
Merchant_Mobile 4_dummy = all_failed_tranx[(all_failed_tranx["Actual Network"] == "Mobile 4 Dummy") | (all_failed_tranx["Actual Network"] == "Mobile 4 Dummy")].groupby(["Merchant Name"])["Status"].count()
Merchant_Mobile 2_dummy = all_failed_tranx[(all_failed_tranx["Actual Network"] == "Mobile 2 Dummy") | (all_failed_tranx["Actual Network"] == "Mobile 2 Dummy")].groupby(["Merchant Name"])["Status"].count()
Merchant_Mobile 1_dummy = all_failed_tranx[all_failed_tranx["Actual Network"] == "Mobile 1 Dummy"].groupby(["Merchant Name"])["Status"].count()

Merchant_Mobile 3_all = daily_tranx_final[daily_tranx_final["Actual Network"] == "Mobile 3"].groupby(["Merchant Name"])["Status"].count()
Merchant_Mobile 1_all = daily_tranx_final[daily_tranx_final["Actual Network"] == "Mobile 1"].groupby(["Merchant Name"])["Status"].count()
Merchant_Mobile 4_all = daily_tranx_final[daily_tranx_final["Actual Network"] == "Mobile 4"].groupby(["Merchant Name"])["Status"].count()
Merchant_Mobile 2_all = daily_tranx_final[daily_tranx_final["Actual Network"] == "Mobile 2"].groupby(["Merchant Name"])["Status"].count()
Merchant_Mobile 3_dummy_all = daily_tranx_final[daily_tranx_final["Actual Network"] == "Mobile 3 Dummy"].groupby(["Merchant Name"])["Status"].count()
Merchant_Mobile 4_dummy_all = daily_tranx_final[(daily_tranx_final["Actual Network"] == "Mobile 4 Dummy") | (daily_tranx_final["Actual Network"] == "Mobile 4 Dummy")].groupby(["Merchant Name"])["Status"].count()
Merchant_Mobile 2_dummy_all = daily_tranx_final[(daily_tranx_final["Actual Network"] == "Mobile 2 Dummy") | (daily_tranx_final["Actual Network"] == "Mobile 2 Dummy")].groupby(["Merchant Name"])["Status"].count()
Merchant_Mobile 1_dummy_all = daily_tranx_final[daily_tranx_final["Actual Network"] == "Mobile 1 Dummy"].groupby(["Merchant Name"])["Status"].count()



merchant_failed_summary = pd.concat([total_merchant_count, successful_merchant_count, Merchant_count, successful_merchant_value, Merchant_value, 
                                     Merchant_Mobile 3, Merchant_Mobile 1, Merchant_Mobile 4, Merchant_Mobile 2, 
                                     Merchant_Mobile 3_dummy, Merchant_Mobile 4_dummy, Merchant_Mobile 2_dummy, Merchant_Mobile 1_dummy, 
                                     Merchant_Mobile 3_all, Merchant_Mobile 1_all, Merchant_Mobile 4_all, Merchant_Mobile 2_all, 
                                     Merchant_Mobile 3_dummy_all, Merchant_Mobile 4_dummy_all, Merchant_Mobile 2_dummy_all, Merchant_Mobile 1_dummy_all], axis = 1)
merchant_failed_summary.columns = ["Total Count", "Total Successful count", "Total Failed count", "Total Successful Value", 
                                   "Total Failed Value", "Mobile 3 Failed Count", "Mobile 1 Failed Count", "Mobile 4 Failed Count", 
                                   "Mobile 2 Failed Count", "Mobile 3 Dummy Failed Count", "Mobile 4 Dummy Failed Count", 
                                   "Mobile 2 Dummy Failed Count", "Mobile 1 Dummy Failed Count", "Mobile 3 Total Count", "Mobile 1 Total Count", "Mobile 4 Total Count", 
                                   "Mobile 2 Total Count", "Mobile 3 Dummy Total Count", "Mobile 4 Dummy Total Count", 
                                   "Mobile 2 Dummy Total Count", "Mobile 1 Dummy Total Count"]
merchant_failed_summary = merchant_failed_summary.fillna(0) 
merchant_failed_summary = merchant_failed_summary.sort_values(["Total Successful Value"], ascending = False)
merchant_failed_summary.loc['Grand Total',:]= merchant_failed_summary.sum(axis=0)
merchant_failed_summary["Failure Rate"] = round((merchant_failed_summary["Total Failed count"]/ merchant_failed_summary["Total Count"])*100,2)
merchant_failed_summary = merchant_failed_summary.fillna(0)
merchant_failed_summary["Failure Rate"] = merchant_failed_summary["Failure Rate"].astype(str) + "%"
merchant_failed_summary = merchant_failed_summary[['Failure Rate', 'Total Successful count', 'Total Failed count', 'Total Successful Value',
       'Total Failed Value', 'Mobile 3 Failed Count', 'Mobile 1 Failed Count', 'Mobile 4 Failed Count', 'Mobile 2 Failed Count', "Mobile 3 Dummy Failed Count", 
       "Mobile 4 Dummy Failed Count", "Mobile 2 Dummy Failed Count", "Mobile 1 Dummy Failed Count",
       "Mobile 3 Total Count", "Mobile 1 Total Count", "Mobile 4 Total Count", "Mobile 2 Total Count", "Mobile 3 Dummy Total Count", 
       "Mobile 4 Dummy Total Count", "Mobile 2 Dummy Total Count", "Mobile 1 Dummy Total Count"]]
merchant_failed_summary


merchant_failed_summary['Total Successful count'] = merchant_failed_summary['Total Successful count'].apply(lambda x : '{0:,.0f}'.format(x))
merchant_failed_summary['Total Failed count'] = merchant_failed_summary['Total Failed count'].apply(lambda x : '{0:,.0f}'.format(x))
merchant_failed_summary['Total Successful Value'] = merchant_failed_summary['Total Successful Value'].apply(lambda x : '{0:,.2f}'.format(x))
merchant_failed_summary['Total Failed Value'] = merchant_failed_summary['Total Failed Value'].apply(lambda x : '{0:,.0f}'.format(x))
merchant_failed_summary['Mobile 3 Failed Count'] = merchant_failed_summary['Mobile 3 Failed Count'].apply(lambda x : '{0:,.0f}'.format(x))
merchant_failed_summary['Mobile 1 Failed Count'] = merchant_failed_summary['Mobile 1 Failed Count'].apply(lambda x : '{0:,.0f}'.format(x))
merchant_failed_summary['Mobile 4 Failed Count'] = merchant_failed_summary['Mobile 4 Failed Count'].apply(lambda x : '{0:,.0f}'.format(x))
merchant_failed_summary['Mobile 2 Failed Count'] = merchant_failed_summary['Mobile 2 Failed Count'].apply(lambda x : '{0:,.0f}'.format(x))
merchant_failed_summary['Mobile 3 Dummy Failed Count'] = merchant_failed_summary['Mobile 3 Dummy Failed Count'].apply(lambda x : '{0:,.0f}'.format(x))
merchant_failed_summary['Mobile 4 Dummy Failed Count'] = merchant_failed_summary['Mobile 4 Dummy Failed Count'].apply(lambda x : '{0:,.0f}'.format(x))
merchant_failed_summary['Mobile 2 Dummy Failed Count'] = merchant_failed_summary['Mobile 2 Dummy Failed Count'].apply(lambda x : '{0:,.0f}'.format(x))
merchant_failed_summary['Mobile 1 Dummy Failed Count'] = merchant_failed_summary['Mobile 1 Dummy Failed Count'].apply(lambda x : '{0:,.0f}'.format(x))

merchant_failed_summary['Mobile 3 Total Count'] = merchant_failed_summary['Mobile 3 Total Count'].apply(lambda x : '{0:,.0f}'.format(x))
merchant_failed_summary['Mobile 1 Total Count'] = merchant_failed_summary['Mobile 1 Total Count'].apply(lambda x : '{0:,.0f}'.format(x))
merchant_failed_summary['Mobile 4 Total Count'] = merchant_failed_summary['Mobile 4 Total Count'].apply(lambda x : '{0:,.0f}'.format(x))
merchant_failed_summary['Mobile 2 Total Count'] = merchant_failed_summary['Mobile 2 Total Count'].apply(lambda x : '{0:,.0f}'.format(x))
merchant_failed_summary['Mobile 3 Dummy Total Count'] = merchant_failed_summary['Mobile 3 Dummy Total Count'].apply(lambda x : '{0:,.0f}'.format(x))
merchant_failed_summary['Mobile 4 Dummy Total Count'] = merchant_failed_summary['Mobile 4 Dummy Total Count'].apply(lambda x : '{0:,.0f}'.format(x))
merchant_failed_summary['Mobile 2 Dummy Total Count'] = merchant_failed_summary['Mobile 2 Dummy Total Count'].apply(lambda x : '{0:,.0f}'.format(x))
merchant_failed_summary['Mobile 1 Dummy Total Count'] = merchant_failed_summary['Mobile 1 Dummy Total Count'].apply(lambda x : '{0:,.0f}'.format(x))


merchant_failed_summary['Mobile 3 Failed Count'] = merchant_failed_summary['Mobile 3 Failed Count'] + " / " + merchant_failed_summary['Mobile 3 Total Count'] 
merchant_failed_summary['Mobile 1 Failed Count'] = merchant_failed_summary['Mobile 1 Failed Count'] + " / " + merchant_failed_summary['Mobile 1 Total Count'] 
merchant_failed_summary['Mobile 4 Failed Count'] = merchant_failed_summary['Mobile 4 Failed Count'] + " / " + merchant_failed_summary['Mobile 4 Total Count'] 
merchant_failed_summary['Mobile 2 Failed Count'] = merchant_failed_summary['Mobile 2 Failed Count'] + " / " + merchant_failed_summary['Mobile 2 Total Count'] 
merchant_failed_summary['Mobile 3 Dummy Failed Count'] = merchant_failed_summary['Mobile 3 Dummy Failed Count'] + " / " + merchant_failed_summary['Mobile 3 Dummy Total Count'] 
merchant_failed_summary['Mobile 4 Dummy Failed Count'] = merchant_failed_summary['Mobile 4 Dummy Failed Count'] + " / " + merchant_failed_summary['Mobile 4 Dummy Total Count'] 
merchant_failed_summary['Mobile 2 Dummy Failed Count'] = merchant_failed_summary['Mobile 2 Dummy Failed Count'] + " / " + merchant_failed_summary['Mobile 2 Dummy Total Count'] 
merchant_failed_summary['Mobile 1 Dummy Failed Count'] = merchant_failed_summary['Mobile 1 Dummy Failed Count'] + " / " + merchant_failed_summary['Mobile 1 Dummy Total Count'] 

merchant_failed_summary_final = merchant_failed_summary[['Failure Rate', 'Total Successful count', 'Total Failed count', 'Total Successful Value',
       'Total Failed Value', 'Mobile 3 Failed Count', 'Mobile 1 Failed Count', 'Mobile 4 Failed Count', 'Mobile 2 Failed Count', 'Mobile 3 Dummy Failed Count',
       'Mobile 4 Dummy Failed Count', 'Mobile 2 Dummy Failed Count', 'Mobile 1 Dummy Failed Count']]
merchant_failed_summary_final

# Calculating Overall Error Analysis

overall_error_value = all_failed_tranx.groupby(["Current Status"])["FaceValue"].sum()#.apply(lambda x : '{0:,.0f}'.format(x))
overall_error_count = all_failed_tranx["Current Status"].value_counts()
overall_error_perc = round(all_failed_tranx["Current Status"].value_counts(normalize = True)*100, 2)
overall_error_analysis = pd.concat([overall_error_value, overall_error_count, overall_error_perc], axis = 1)
overall_error_analysis.columns = ['FaceValue', 'Count', 'Percentage']
overall_error_analysis = overall_error_analysis.reset_index()
overall_error_analysis.columns = ["Error Message", 'FaceValue', 'Count', 'Percentage']
overall_error_analysis = overall_error_analysis.sort_values(["Count"], ascending = False).set_index(["Error Message"])
overall_error_analysis.loc['Grand Total',:]= overall_error_analysis.sum(axis=0)
overall_error_analysis['FaceValue'] = overall_error_analysis['FaceValue'].apply(lambda x : '{0:,.2f}'.format(x))
overall_error_analysis['Count'] = overall_error_analysis['Count'].apply(lambda x : '{0:,.0f}'.format(x))
overall_error_analysis["Percentage"] = round(overall_error_analysis["Percentage"],1).astype(str) + "%"
overall_error_analysis


# Error Analysis By Network

failed_network_value = all_failed_tranx.groupby(["Actual Network", "Current Status"])["FaceValue"].sum()#.apply(lambda x : '{0:,.0f}'.format(x))
failed_network_count = all_failed_tranx.groupby(["Actual Network"])["Current Status"].value_counts()
failed_network_perc = round(all_failed_tranx.groupby(["Actual Network"])["Current Status"].value_counts(normalize = True)*100, 2)
failed_network_analysis = pd.concat([failed_network_value, failed_network_count, failed_network_perc], axis = 1)
failed_network_analysis.columns = ['FaceValue', 'Count', 'Percentage']
failed_network_analysis = failed_network_analysis.reset_index()
failed_network_analysis.columns = ['Actual Network', "Error Message", 'FaceValue', 'Count', 'Percentage']
# failed_network_analysis.sort_values(["Status"], ascending = False)
failed_network_analysis_Mobile 3 = failed_network_analysis[failed_network_analysis["Actual Network"] == "Mobile 3"].sort_values(["Count"], ascending = False).drop(["Actual Network"], axis = 1).set_index(["Error Message"])
failed_network_analysis_Mobile 1 = failed_network_analysis[failed_network_analysis["Actual Network"] == "Mobile 1"].sort_values(["Count"], ascending = False).drop(["Actual Network"], axis = 1).set_index(["Error Message"])
failed_network_analysis_Mobile 4 = failed_network_analysis[failed_network_analysis["Actual Network"] == "Mobile 4"].sort_values(["Count"], ascending = False).drop(["Actual Network"], axis = 1).set_index(["Error Message"])
failed_network_analysis_Mobile 2 = failed_network_analysis[failed_network_analysis["Actual Network"] == "Mobile 2"].sort_values(["Count"], ascending = False).drop(["Actual Network"], axis = 1).set_index(["Error Message"])

# failed_network_analysis_Mobile 1
failed_network_analysis_Mobile 1.loc['Grand Total',:]= failed_network_analysis_Mobile 1.sum(axis=0)
failed_network_analysis_Mobile 1['FaceValue'] = failed_network_analysis_Mobile 1['FaceValue'].apply(lambda x : '{0:,.2f}'.format(x))
failed_network_analysis_Mobile 1['Count'] = failed_network_analysis_Mobile 1['Count'].apply(lambda x : '{0:,.0f}'.format(x))
failed_network_analysis_Mobile 1['Percentage'] = failed_network_analysis_Mobile 1['Percentage'].astype(str) + "%"
failed_network_analysis_Mobile 1


failed_network_analysis_Mobile 3.loc['Grand Total',:]= failed_network_analysis_Mobile 3.sum(axis=0)
failed_network_analysis_Mobile 3['FaceValue'] = failed_network_analysis_Mobile 3['FaceValue'].apply(lambda x : '{0:,.2f}'.format(x))
failed_network_analysis_Mobile 3['Count'] = failed_network_analysis_Mobile 3['Count'].apply(lambda x : '{0:,.0f}'.format(x))
failed_network_analysis_Mobile 3['Percentage'] = round(failed_network_analysis_Mobile 3['Percentage'],1).astype(str) + "%"
failed_network_analysis_Mobile 3


failed_network_analysis_Mobile 4.loc['Grand Total',:]= failed_network_analysis_Mobile 4.sum(axis=0)
failed_network_analysis_Mobile 4['FaceValue'] = failed_network_analysis_Mobile 4['FaceValue'].apply(lambda x : '{0:,.2f}'.format(x))
failed_network_analysis_Mobile 4['Count'] = failed_network_analysis_Mobile 4['Count'].apply(lambda x : '{0:,.0f}'.format(x))
failed_network_analysis_Mobile 4['Percentage'] = round(failed_network_analysis_Mobile 4['Percentage']).astype(str) + "%"
failed_network_analysis_Mobile 4


failed_network_analysis_Mobile 2.loc['Grand Total',:]= failed_network_analysis_Mobile 2.sum(axis=0)
failed_network_analysis_Mobile 2['FaceValue'] = failed_network_analysis_Mobile 2['FaceValue'].apply(lambda x : '{0:,.2f}'.format(x))
failed_network_analysis_Mobile 2['Count'] = failed_network_analysis_Mobile 2['Count'].apply(lambda x : '{0:,.0f}'.format(x))
failed_network_analysis_Mobile 2['Percentage'] = round(failed_network_analysis_Mobile 2['Percentage']).astype(str) + "%"
failed_network_analysis_Mobile 2

# print(failed_network_analysis_Mobile 3)

# Server Performance Summary
total_server_count = daily_tranx_final.groupby(["Processing Server"])["Status"].count()
total_server_value = daily_tranx_final.groupby(["Processing Server"])["FaceValue"].sum()

server_count = all_failed_tranx.groupby(["Processing Server"])["Status"].count()
server_value = all_failed_tranx.groupby(["Processing Server"])["FaceValue"].sum()

successful_server_count = all_successful_tranx.groupby(["Processing Server"])["Status"].count()
successful_server_value = all_successful_tranx.groupby(["Processing Server"])["FaceValue"].sum()


server_Mobile 3 = all_failed_tranx[all_failed_tranx["Actual Network"] == "Mobile 3"].groupby(["Processing Server"])["Status"].count()
server_Mobile 1 = all_failed_tranx[all_failed_tranx["Actual Network"] == "Mobile 1"].groupby(["Processing Server"])["Status"].count()
server_Mobile 4 = all_failed_tranx[all_failed_tranx["Actual Network"] == "Mobile 4"].groupby(["Processing Server"])["Status"].count()
server_Mobile 2 = all_failed_tranx[all_failed_tranx["Actual Network"] == "Mobile 2"].groupby(["Processing Server"])["Status"].count()

server_failed_summary = pd.concat([total_server_count, successful_server_count, server_count, successful_server_value, server_value, 
                                     server_Mobile 3, server_Mobile 1, server_Mobile 4, server_Mobile 2], axis = 1)
server_failed_summary.columns = ["Total Count", "Total Successful count", "Total Failed count", "Total Successful Value", 
                                   "Total Failed Value", "Mobile 3 Failed Count", "Mobile 1 Failed Count", "Mobile 4 Failed Count", 
                                   "Mobile 2 Failed Count"]
server_failed_summary = server_failed_summary.fillna(0)
server_failed_summary = server_failed_summary.sort_values(["Total Successful Value"], ascending = False)
server_failed_summary.loc['Grand Total',:]= server_failed_summary.sum(axis=0)
server_failed_summary["Failure Rate"] = round((server_failed_summary["Total Failed count"]/ server_failed_summary["Total Count"])*100,2)
server_failed_summary = server_failed_summary.fillna(0)
server_failed_summary["Failure Rate"] = server_failed_summary["Failure Rate"].astype(str) + "%"
server_failed_summary = server_failed_summary[['Failure Rate', 'Total Successful count', 'Total Failed count', 'Total Successful Value',
       'Total Failed Value', 'Mobile 3 Failed Count', 'Mobile 1 Failed Count', 'Mobile 4 Failed Count', 'Mobile 2 Failed Count']]
# print(server_failed_summary)

server_failed_summary['Total Successful count'] = server_failed_summary['Total Successful count'].apply(lambda x : '{0:,.0f}'.format(x))
server_failed_summary['Total Failed count'] = server_failed_summary['Total Failed count'].apply(lambda x : '{0:,.0f}'.format(x))
server_failed_summary['Total Successful Value'] = server_failed_summary['Total Successful Value'].apply(lambda x : '{0:,.2f}'.format(x))
server_failed_summary['Total Failed Value'] = server_failed_summary['Total Failed Value'].apply(lambda x : '{0:,.0f}'.format(x))
server_failed_summary['Mobile 3 Failed Count'] = server_failed_summary['Mobile 3 Failed Count'].apply(lambda x : '{0:,.0f}'.format(x))
server_failed_summary['Mobile 1 Failed Count'] = server_failed_summary['Mobile 1 Failed Count'].apply(lambda x : '{0:,.0f}'.format(x))
server_failed_summary['Mobile 4 Failed Count'] = server_failed_summary['Mobile 4 Failed Count'].apply(lambda x : '{0:,.0f}'.format(x))
server_failed_summary['Mobile 2 Failed Count'] = server_failed_summary['Mobile 2 Failed Count'].apply(lambda x : '{0:,.0f}'.format(x))
# print(server_failed_summary)

server_failed_network_value = all_failed_tranx.groupby(["Processing Server", "Actual Network", "Current Status"])["FaceValue"].sum()#.apply(lambda x : '{0:,.0f}'.format(x))
server_failed_network_count = all_failed_tranx.groupby(["Processing Server", "Actual Network"])["Current Status"].value_counts()
server_failed_network_perc = round(all_failed_tranx.groupby(["Processing Server", "Actual Network"])["Current Status"].value_counts(normalize = True)*100, 2)
server_failed_network_analysis = pd.concat([server_failed_network_value, server_failed_network_count, server_failed_network_perc], axis = 1)
server_failed_network_analysis.columns = ['FaceValue', 'Count', 'Percentage']
# server_failed_network_analysis = server_failed_network_analysis.reset_index()
server_failed_network_analysis['FaceValue'] = server_failed_network_analysis['FaceValue'].apply(lambda x : '{0:,.2f}'.format(x))
server_failed_network_analysis['Count'] = server_failed_network_analysis['Count'].apply(lambda x : '{0:,.0f}'.format(x))
server_failed_network_analysis['Percentage'] = round(server_failed_network_analysis['Percentage'],1).astype(str) + "%"

# print(server_failed_network_analysis)
# server_failed_network_analysis.columns = ['Actual Network', "Error Message", 'FaceValue', 'Count', 'Percentage']
# failed_network_analysis.sort_values(["Status"], ascending = False)


failed_network_analysis_Mobile 3 = failed_network_analysis[failed_network_analysis["Actual Network"] == "Mobile 3"].sort_values(["Count"], ascending = False).drop(["Actual Network"], axis = 1).set_index(["Error Message"])
failed_network_analysis_Mobile 1 = failed_network_analysis[failed_network_analysis["Actual Network"] == "Mobile 1"].sort_values(["Count"], ascending = False).drop(["Actual Network"], axis = 1).set_index(["Error Message"])
failed_network_analysis_Mobile 4 = failed_network_analysis[failed_network_analysis["Actual Network"] == "Mobile 4"].sort_values(["Count"], ascending = False).drop(["Actual Network"], axis = 1).set_index(["Error Message"])
failed_network_analysis_Mobile 2 = failed_network_analysis[failed_network_analysis["Actual Network"] == "Mobile 2"].sort_values(["Count"], ascending = False).drop(["Actual Network"], axis = 1).set_index(["Error Message"])

# failed_network_analysis_Mobile 1
failed_network_analysis_Mobile 1.loc['Grand Total',:]= round(failed_network_analysis_Mobile 1.sum(axis=0),1)
failed_network_analysis_Mobile 1['FaceValue'] = failed_network_analysis_Mobile 1['FaceValue'].apply(lambda x : '{0:,.2f}'.format(x))
failed_network_analysis_Mobile 1['Count'] = failed_network_analysis_Mobile 1['Count'].apply(lambda x : '{0:,.0f}'.format(x))
failed_network_analysis_Mobile 1['Percentage'] = failed_network_analysis_Mobile 1['Percentage'].astype(str) + "%"
failed_network_analysis_Mobile 1


failed_network_analysis_Mobile 3.loc['Grand Total',:]= round(failed_network_analysis_Mobile 3.sum(axis=0),1)
failed_network_analysis_Mobile 3['FaceValue'] = failed_network_analysis_Mobile 3['FaceValue'].apply(lambda x : '{0:,.2f}'.format(x))
failed_network_analysis_Mobile 3['Count'] = failed_network_analysis_Mobile 3['Count'].apply(lambda x : '{0:,.0f}'.format(x))
failed_network_analysis_Mobile 3['Percentage'] = round(failed_network_analysis_Mobile 3['Percentage'],1).astype(str) + "%"
failed_network_analysis_Mobile 3


failed_network_analysis_Mobile 4.loc['Grand Total',:]= round(failed_network_analysis_Mobile 4.sum(axis=0),1)
failed_network_analysis_Mobile 4['FaceValue'] = failed_network_analysis_Mobile 4['FaceValue'].apply(lambda x : '{0:,.2f}'.format(x))
failed_network_analysis_Mobile 4['Count'] = failed_network_analysis_Mobile 4['Count'].apply(lambda x : '{0:,.0f}'.format(x))
failed_network_analysis_Mobile 4['Percentage'] = round(failed_network_analysis_Mobile 4['Percentage']).astype(str) + "%"
failed_network_analysis_Mobile 4


failed_network_analysis_Mobile 2.loc['Grand Total',:]= round(failed_network_analysis_Mobile 2.sum(axis=0))
failed_network_analysis_Mobile 2['FaceValue'] = failed_network_analysis_Mobile 2['FaceValue'].apply(lambda x : '{0:,.2f}'.format(x))
failed_network_analysis_Mobile 2['Count'] = failed_network_analysis_Mobile 2['Count'].apply(lambda x : '{0:,.0f}'.format(x))
failed_network_analysis_Mobile 2['Percentage'] = round(failed_network_analysis_Mobile 2['Percentage']).astype(str) + "%"
failed_network_analysis_Mobile 2

failed_network_analysis_Mobile 3


# Hourly Performance

daily_tranx_final

major_merchant_hourly = daily_tranx_final.groupby(["Duration", "Hour"])["Merchant"].count()
major_merchant_hourly_failed = daily_tranx_final[daily_tranx_final["Status"]!= "Successful"].groupby(["Duration", "Hour"])["Merchant"].count()
# major_merchant_hourly_failure_rate = round((major_merchant_hourly_failed / major_merchant_hourly) * 100 , 1).fillna(0.0).astype(str) + "%"
major_merchant_hourly_failed_uba = daily_tranx_final[(daily_tranx_final["Status"]!= "Successful") & (daily_tranx_final["Merchant Name"] == "United Institute for Africa Plc")].groupby(["Duration", "Hour"])["Merchant"].count()
major_merchant_hourly_failed_payv = daily_tranx_final[(daily_tranx_final["Status"]!= "Successful") & (daily_tranx_final["Merchant Name"] == "Payvantage Postpaid Limited")].groupby(["Duration", "Hour"])["Merchant"].count()
major_merchant_hourly_failed_WM = daily_tranx_final[(daily_tranx_final["Status"]!= "Successful") & (daily_tranx_final["Merchant Name"] == "WM Institute Alat")].groupby(["Duration", "Hour"])["Merchant"].count()
major_merchant_hourly_failed_polaris = daily_tranx_final[(daily_tranx_final["Status"]!= "Successful") & (daily_tranx_final["Merchant Name"] == "Polaris Institute")].groupby(["Duration", "Hour"])["Merchant"].count()
major_merchant_hourly_failed_ST = daily_tranx_final[(daily_tranx_final["Status"]!= "Successful") & (daily_tranx_final["Merchant Name"] == "ST Institute Plc")].groupby(["Duration", "Hour"])["Merchant"].count()
major_merchant_hourly_failed_MOW = daily_tranx_final[(daily_tranx_final["Status"]!= "Successful") & (daily_tranx_final["Merchant Name"] == "Mobile 3 YDFS (MOW)")].groupby(["Duration", "Hour"])["Merchant"].count()
major_merchant_hourly_failed_eco = daily_tranx_final[(daily_tranx_final["Status"]!= "Successful") & (daily_tranx_final["Merchant Name"] == "Eco Institute")].groupby(["Duration", "Hour"])["Merchant"].count()


major_merchant_hourly_uba = daily_tranx_final[(daily_tranx_final["Merchant Name"] == "United Institute for Africa Plc")].groupby(["Duration", "Hour"])["Merchant"].count()
major_merchant_hourly_payv = daily_tranx_final[(daily_tranx_final["Merchant Name"] == "Payvantage Postpaid Limited")].groupby(["Duration", "Hour"])["Merchant"].count()
major_merchant_hourly_WM = daily_tranx_final[(daily_tranx_final["Merchant Name"] == "WM Institute Alat")].groupby(["Duration", "Hour"])["Merchant"].count()
major_merchant_hourly_polaris = daily_tranx_final[(daily_tranx_final["Merchant Name"] == "Polaris Institute")].groupby(["Duration", "Hour"])["Merchant"].count()
major_merchant_hourly_ST = daily_tranx_final[(daily_tranx_final["Merchant Name"] == "ST Institute Plc")].groupby(["Duration", "Hour"])["Merchant"].count()
major_merchant_hourly_MOW = daily_tranx_final[(daily_tranx_final["Merchant Name"] == "Mobile 3 YDFS (MOW)")].groupby(["Duration", "Hour"])["Merchant"].count()
major_merchant_hourly_eco = daily_tranx_final[(daily_tranx_final["Merchant Name"] == "Eco Institute")].groupby(["Duration", "Hour"])["Merchant"].count()




major_merchant_hourly_analysis = pd.concat([major_merchant_hourly_failed, major_merchant_hourly, 
                    major_merchant_hourly_failed_payv, major_merchant_hourly_failed_uba, major_merchant_hourly_failed_WM, 
                    major_merchant_hourly_failed_polaris, major_merchant_hourly_failed_ST, major_merchant_hourly_failed_eco, 
                    major_merchant_hourly_failed_MOW, 
                    major_merchant_hourly_uba, major_merchant_hourly_payv, major_merchant_hourly_WM, 
                    major_merchant_hourly_polaris, major_merchant_hourly_ST, major_merchant_hourly_MOW, 
                    major_merchant_hourly_eco], axis = 1)
major_merchant_hourly_analysis.columns = ["Failed Count", "Total Count", "Payvantage Failed", "UBA Failed", 
                                          "WM Alat Failed", "Polaris Failed", "ST Failed", "EcoInstitute Failed", "MOW Failed", 
                                          "UBA", "Payvantage", "WM Alat", "Polaris", "ST", "MOW", "EcoInstitute"]
major_merchant_hourly_analysis = major_merchant_hourly_analysis.fillna(0)
major_merchant_hourly_analysis

major_merchant_hourly_analysis = major_merchant_hourly_analysis.reset_index()
major_merchant_hourly_analysis = major_merchant_hourly_analysis.set_index(["Hour"])
major_merchant_hourly_analysis = major_merchant_hourly_analysis.drop(["Duration"], axis = 1)
major_merchant_hourly_analysis.loc['Grand Total',:]= major_merchant_hourly_analysis.sum(axis=0)

# major_merchant_hourly_analysis.loc['Grand Total',:]= major_merchant_hourly_analysis.sum(axis=0)
# failed_network_analysis_Mobile 2['FaceValue'] = failed_network_analysis_Mobile 2['FaceValue'].apply(lambda x : '{0:,.2f}'.format(x))

major_merchant_hourly_analysis["Failure Rate"] = round((major_merchant_hourly_analysis["Failed Count"]/ major_merchant_hourly_analysis["Total Count"])*100 , 1).fillna(0.0).astype(str) + "%"
major_merchant_hourly_analysis
major_merchant_hourly_analysis['Failed Count'] = major_merchant_hourly_analysis['Failed Count'].apply(lambda x : '{0:,.0f}'.format(x))
major_merchant_hourly_analysis['Total Count'] = major_merchant_hourly_analysis['Total Count'].apply(lambda x : '{0:,.0f}'.format(x))
major_merchant_hourly_analysis['Payvantage Failed'] = major_merchant_hourly_analysis['Payvantage Failed'].apply(lambda x : '{0:,.0f}'.format(x))
major_merchant_hourly_analysis['UBA Failed'] = major_merchant_hourly_analysis['UBA Failed'].apply(lambda x : '{0:,.0f}'.format(x))
major_merchant_hourly_analysis['WM Alat Failed'] = major_merchant_hourly_analysis['WM Alat Failed'].apply(lambda x : '{0:,.0f}'.format(x))
major_merchant_hourly_analysis['Polaris Failed'] = major_merchant_hourly_analysis['Polaris Failed'].apply(lambda x : '{0:,.0f}'.format(x))
major_merchant_hourly_analysis['ST Failed'] = major_merchant_hourly_analysis['ST Failed'].apply(lambda x : '{0:,.0f}'.format(x))
major_merchant_hourly_analysis['EcoInstitute Failed'] = major_merchant_hourly_analysis['EcoInstitute Failed'].apply(lambda x : '{0:,.0f}'.format(x))
major_merchant_hourly_analysis['MOW Failed'] = major_merchant_hourly_analysis['MOW Failed'].apply(lambda x : '{0:,.0f}'.format(x))

major_merchant_hourly_analysis['Payvantage'] = major_merchant_hourly_analysis['Payvantage'].apply(lambda x : '{0:,.0f}'.format(x))
major_merchant_hourly_analysis['UBA'] = major_merchant_hourly_analysis['UBA'].apply(lambda x : '{0:,.0f}'.format(x))
major_merchant_hourly_analysis['WM Alat'] = major_merchant_hourly_analysis['WM Alat'].apply(lambda x : '{0:,.0f}'.format(x))
major_merchant_hourly_analysis['Polaris'] = major_merchant_hourly_analysis['Polaris'].apply(lambda x : '{0:,.0f}'.format(x))
major_merchant_hourly_analysis['ST'] = major_merchant_hourly_analysis['ST'].apply(lambda x : '{0:,.0f}'.format(x))
major_merchant_hourly_analysis['EcoInstitute'] = major_merchant_hourly_analysis['EcoInstitute'].apply(lambda x : '{0:,.0f}'.format(x))
major_merchant_hourly_analysis['MOW'] = major_merchant_hourly_analysis['MOW'].apply(lambda x : '{0:,.0f}'.format(x))


major_merchant_hourly_analysis['Payvantage Failed'] = major_merchant_hourly_analysis['Payvantage Failed'] + " / " + major_merchant_hourly_analysis['Payvantage']
major_merchant_hourly_analysis['UBA Failed'] = major_merchant_hourly_analysis['UBA Failed'] + " / " + major_merchant_hourly_analysis['UBA']
major_merchant_hourly_analysis['WM Alat Failed'] = major_merchant_hourly_analysis['WM Alat Failed'] + " / " + major_merchant_hourly_analysis['WM Alat']
major_merchant_hourly_analysis['Polaris Failed'] = major_merchant_hourly_analysis['Polaris Failed'] + " / " + major_merchant_hourly_analysis['Polaris']
major_merchant_hourly_analysis['ST Failed'] = major_merchant_hourly_analysis['ST Failed'] + " / " + major_merchant_hourly_analysis['ST']
major_merchant_hourly_analysis['EcoInstitute Failed'] = major_merchant_hourly_analysis['EcoInstitute Failed'] + " / " + major_merchant_hourly_analysis['EcoInstitute']
major_merchant_hourly_analysis['MOW Failed'] = major_merchant_hourly_analysis['MOW Failed'] + " / " + major_merchant_hourly_analysis['MOW']



major_merchant_hourly_analysis_final = major_merchant_hourly_analysis[['Failure Rate', 'Failed Count', 'Total Count', 'Payvantage Failed', 'UBA Failed', 'WM Alat Failed',
       'Polaris Failed', 'ST Failed', 'EcoInstitute Failed', 'MOW Failed']]
# major_merchant_hourly_analysis[""]
major_merchant_hourly_analysis.columns
major_merchant_hourly_analysis_final


# Provider Performance Review

daily_tranx_final["servp"] = daily_tranx_final["Provider"] + " " + daily_tranx_final["Actual Network"]
daily_tranx_final["servp"].value_counts()

provider_total = daily_tranx_final["servp"].value_counts()
provider_successful_value = daily_tranx_final[daily_tranx_final["Current Status"] == "Successful"].groupby(["servp"])["FaceValue"].sum()
provider_failed_value = daily_tranx_final[daily_tranx_final["Current Status"] != "Successful"].groupby(["servp"])["FaceValue"].sum()
provider_failed = daily_tranx_final[daily_tranx_final["Current Status"] != "Successful"]["servp"].value_counts()
provider_airtime_successful = daily_tranx_final[(daily_tranx_final["Current Status"] == "Successful") & (daily_tranx_final["Type"] == "Airtime")].groupby(["servp"])["FaceValue"].sum()
provider_airtime_failed = daily_tranx_final[(daily_tranx_final["Current Status"] != "Successful") & (daily_tranx_final["Type"] == "Airtime")].groupby(["servp"])["FaceValue"].sum()
provider_data_successful = daily_tranx_final[(daily_tranx_final["Current Status"] == "Successful") & (daily_tranx_final["Type"] == "Data")].groupby(["servp"])["FaceValue"].sum()
provider_data_failed = daily_tranx_final[(daily_tranx_final["Current Status"] != "Successful") & (daily_tranx_final["Type"] == "Data")].groupby(["servp"])["FaceValue"].sum()
provider_pin_successful = daily_tranx_final[(daily_tranx_final["Current Status"] == "Successful") & (daily_tranx_final["Type"] == "Pin")].groupby(["servp"])["FaceValue"].sum()
provider_pin_failed = daily_tranx_final[(daily_tranx_final["Current Status"] != "Successful") & (daily_tranx_final["Type"] == "Pin")].groupby(["servp"])["FaceValue"].sum()

provider_analysis = pd.concat([provider_total, provider_failed, provider_successful_value, provider_failed_value, 
                               provider_airtime_successful, provider_airtime_failed, 
                               provider_data_successful, provider_data_failed, 
                               provider_pin_successful, provider_pin_failed], axis = 1)
provider_analysis.columns = ["Total Count", "Failed Count", "Total Successful Value", "Total Failed Value", 
                             "Airtime Successful Value", "Airtime Failed Value", 
                             "Data Successful Value", "Data Failed Value", 
                             "PIN Successful Value", "PIN Failed Value"]
provider_analysis = provider_analysis.fillna(0)
provider_analysis.loc['Grand Total',:]= provider_analysis.sum(axis=0)

provider_analysis["Failure Rate"] = round((provider_analysis["Failed Count"] / provider_analysis["Total Count"])*100,1).astype(str) + "%"

provider_analysis['Total Count'] = provider_analysis['Total Count'].apply(lambda x : '{0:,.0f}'.format(x))
provider_analysis['Failed Count'] = provider_analysis['Failed Count'].apply(lambda x : '{0:,.0f}'.format(x))
provider_analysis['Total Successful Value'] = provider_analysis['Total Successful Value'].apply(lambda x : '{0:,.0f}'.format(x))
provider_analysis['Total Failed Value'] = provider_analysis['Total Failed Value'].apply(lambda x : '{0:,.0f}'.format(x))
provider_analysis['Airtime Successful Value'] = provider_analysis['Airtime Successful Value'].apply(lambda x : '{0:,.0f}'.format(x))
provider_analysis['Airtime Failed Value'] = provider_analysis['Airtime Failed Value'].apply(lambda x : '{0:,.0f}'.format(x))
provider_analysis['Data Successful Value'] = provider_analysis['Data Successful Value'].apply(lambda x : '{0:,.0f}'.format(x))
provider_analysis['Data Failed Value'] = provider_analysis['Data Failed Value'].apply(lambda x : '{0:,.0f}'.format(x))
provider_analysis['PIN Successful Value'] = provider_analysis['PIN Successful Value'].apply(lambda x : '{0:,.0f}'.format(x))
provider_analysis['PIN Failed Value'] = provider_analysis['PIN Failed Value'].apply(lambda x : '{0:,.0f}'.format(x))
# provider_analysis['Data Successful Value'] = provider_analysis['Data Successful Value'].apply(lambda x : '{0:,.0f}'.format(x))
provider_analysis['Failed Count / Total Count'] = provider_analysis['Failed Count'] + " / " + provider_analysis['Total Count']
provider_analysis = provider_analysis.reset_index()
provider_analysis.columns = ['Provider', 'Total Count', 'Failed Count', 'Total Successful Value', 'Total Failed Value',
       'Airtime Successful Value', 'Airtime Failed Value', 'Data Successful Value',
       'Data Failed Value', 'PIN Successful Value', 'PIN Failed Value', 'Failure Rate',
       'Failed Count / Total Count']
provider_analysis
provider_analysis_final = provider_analysis[["Provider", 'Failure Rate', 'Failed Count / Total Count', 'Total Successful Value', 
       'Total Failed Value', 'Airtime Successful Value', 'Airtime Failed Value', 'Data Successful Value',
       'Data Failed Value', 'PIN Successful Value', 'PIN Failed Value']]
# print(provider_analysis_final)

# Provider Service Analysis

daily_tranx_final["Service Type"] = daily_tranx_final["Provider"] + " " + daily_tranx_final["Actual Network"] + " " + daily_tranx_final["Type"]
daily_tranx_final
service_type_count = daily_tranx_final["Service Type"].value_counts()
service_type_count_failed = daily_tranx_final[daily_tranx_final["Current Status"] != "Successful"]["Service Type"].value_counts()
service_successful_value = daily_tranx_final[daily_tranx_final["Current Status"] == "Successful"].groupby(["Service Type"])["FaceValue"].sum()
service_failed_value = daily_tranx_final[daily_tranx_final["Current Status"] != "Successful"].groupby(["Service Type"])["FaceValue"].sum()
service_type_analysis = pd.concat([service_type_count, service_type_count_failed, service_successful_value, 
                                   service_failed_value], axis = 1)
service_type_analysis.columns = ["Total Count", "Failed Count", "Sucessful Value", "Failed Value"]
service_type_analysis = service_type_analysis.fillna(0)
service_type_analysis.loc['Grand Total',:]= service_type_analysis.sum(axis=0)
service_type_analysis["Failure Rate"] = round((service_type_analysis["Failed Count"] / service_type_analysis["Total Count"])*100,1).astype(str) + "%"
service_type_analysis['Total Count'] = service_type_analysis['Total Count'].apply(lambda x : '{0:,.0f}'.format(x))
service_type_analysis['Failed Count'] = service_type_analysis['Failed Count'].apply(lambda x : '{0:,.0f}'.format(x))
service_type_analysis['Sucessful Value'] = service_type_analysis['Sucessful Value'].apply(lambda x : '{0:,.0f}'.format(x))
service_type_analysis['Failed Value'] = service_type_analysis['Failed Value'].apply(lambda x : '{0:,.0f}'.format(x))
service_type_analysis = service_type_analysis.reset_index()
service_type_analysis.columns = ['Service', 'Total Count', 'Failed Count', 'Sucessful Value', 'Failed Value', 'Failure Rate']
# service_type_analysis["Failed"] = service_type_analysis['Failed Count'] + " / " + service_type_analysis['Total Count']


service_type_analysis_final = service_type_analysis[['Service', 'Failure Rate', 'Total Count', 'Failed Count', 'Sucessful Value', 'Failed Value']]
# print(service_type_analysis_final)


service_error_count = daily_tranx_final[daily_tranx_final["Current Status"] != "Successful"].groupby(["Service Type"])["Current Status"].value_counts()
service_error_perc = round(daily_tranx_final[daily_tranx_final["Current Status"] != "Successful"].groupby(["Service Type"])["Current Status"].value_counts(normalize = True) * 100 , 1).astype(str) + "%"
service_error_value = daily_tranx_final[daily_tranx_final["Current Status"] != "Successful"].groupby(["Service Type", "Current Status"])["FaceValue"].sum()
service_error_analysis = pd.concat([service_error_count, service_error_perc, service_error_value], axis = 1)
service_error_analysis.columns = ["Count", "% Contribution", "FaceValue"]
service_error_analysis['Count'] = service_error_analysis['Count'].apply(lambda x : '{0:,.0f}'.format(x))
service_error_analysis['FaceValue'] = service_error_analysis['FaceValue'].apply(lambda x : '{0:,.0f}'.format(x))

service_error_analysis



# Mobile 3_final
table_1 = pd.DataFrame([f"--Summary      Performance    Report   Table-- {yesterday}", " ** "])
table_2 = pd.DataFrame(["*", f"--Overall Error Report Table-- {yesterday}-"])
table_2_ = pd.DataFrame(["*", f"-- Successful Facevalue of Products by Merchant Table-- {yesterday}-"])
table_2_1 = pd.DataFrame(["Observations", f"{comment}-"])
table_2a = pd.DataFrame(["*", f"--Hourly Failure Report Among Major Merchant Table-- {yesterday}-"])
table_2b = pd.DataFrame(["*", f"--Provider Analysis Table-- {yesterday}-"])
table_2c = pd.DataFrame(["*", f"--Provider and Product Analysis Table-- {yesterday}-"])
table_3 = pd.DataFrame(["*", f"--Mobile 3 Error Report Table-- {yesterday}-"])
table_4 = pd.DataFrame(["*", "--Mobile 1 Error Report Table--"])
table_5 = pd.DataFrame(["*", "--Mobile 4 Error Report Table--"])
table_6 = pd.DataFrame(["*", "--Mobile 2 Failed Report Table--"])
table_6a = pd.DataFrame(["*", "--Product Type Failure Report Table--"])
table_7 = pd.DataFrame(["*", "--Overall Server Performance Report Table--"])
table_8 = pd.DataFrame(["*", "--Error Report Per Server and Network Table--"])
table_2_1


heading1 = f"<br><strong>--Summary Performance Report Table-- {yesterday}</strong></br>"
heading2 = f"<br><strong>-- Successful Facevalue of Products by Merchant Table-- {yesterday}-</strong></br>"
heading3 = f"<br><strong>Observations </strong></br><br><strong>{comment}-</strong></br>"
heading4 = f"<br><strong>--Hourly Failure Report Among Major Merchant Table-- {yesterday}-</strong></br>"
heading5 = f"<br><strong>--Provider Analysis Table-- {yesterday}-</strong></br>"
heading6 = f"<br><strong>--Provider and Product Analysis Table-- </strong></br>"
heading7 = f"<br><strong>--Overall Error Report Table-- {yesterday}-</strong></br>"
heading8 = f"<br><strong>--Mobile 3 Error Report Table-- {yesterday}-</strong></br>"
heading9 = f"<br><strong>--Mobile 1 Error Report Table-- {yesterday}-</strong></br>"
heading10 = f"<br><strong>--Mobile 4 Error Report Table-- {yesterday}-</strong></br>"
heading11 = f"<br><strong>--Mobile 2 Error Report Table-- {yesterday}-</strong></br>"
heading12 = f"<br><strong>--Product Type Failure Report Table-- {yesterday}-</strong></br>"
heading13 = f"<br><strong>--Overall Server Performance Report Table-- {yesterday}-</strong></br>"
heading14 = f"<br><strong>----Error Report Per Server and Network Table-- {yesterday}-</strong></br>"
closing_remark = "<br><strong>Thank you</strong>"

merchant_failed_summary_finalb = merchant_failed_summary_final.reset_index()
tBHH = build_table(merchant_failed_summary_finalb, "blue_dark", text_align = "center")
tb2 = build_table(service_report_final, "orange_dark", text_align = "center")
major_merchant_hourly_analysis_finalb = major_merchant_hourly_analysis_final.reset_index()
tb4 = build_table(major_merchant_hourly_analysis_finalb, "blue_dark", text_align = "center")
tb5 = build_table(provider_analysis_final, "grey_dark", text_align = "center")
tb6 = build_table(service_type_analysis_final, "grey_dark", text_align = "center")

overall_error_analysisb = overall_error_analysis.reset_index()
tb7 = build_table(overall_error_analysisb, "grey_dark", text_align = "center")

failed_network_analysis_Mobile 3b = failed_network_analysis_Mobile 3.reset_index()
tb8 = build_table(failed_network_analysis_Mobile 3b, "yellow_dark", text_align = "center")

failed_network_analysis_Mobile 1b = failed_network_analysis_Mobile 1.reset_index()
tb9 = build_table(failed_network_analysis_Mobile 1b, "red_dark", text_align = "center")

failed_network_analysis_Mobile 4b = failed_network_analysis_Mobile 4.reset_index()
tBHH0 = build_table(failed_network_analysis_Mobile 4b, "green_dark", text_align = "center")
failed_network_analysis_Mobile 2b = failed_network_analysis_Mobile 2.reset_index()
tBHH1 = build_table(failed_network_analysis_Mobile 2b, "red_light", text_align = "center")
service_error_analysisb = service_error_analysis.reset_index()
tBHH2 = build_table(service_error_analysisb, "green_light", text_align = "center")
server_failed_summaryb = server_failed_summary.reset_index()
tBHH3 = build_table(server_failed_summaryb, "grey_dark", text_align = "center")
server_failed_network_analysisb = server_failed_network_analysis.reset_index()
tBHH4 = build_table(server_failed_network_analysisb, "grey_dark", text_align = "center")


output_final = heading1 + '<br>' + tBHH + heading2 + '<br>' + tb2 + heading3 +  '<br>' + heading4 + '<br>' + tb4 + heading5 + '<br>' + tb5 + heading6 + '<br>' + tb6 + heading7 + '<br>' + tb7 + heading8 + '<br>' + tb8 + heading9 + '<br>' + tb9 + heading10 + '<br>' + tBHH0 + heading11 + '<br>' + tBHH1 + heading12 + '<br>' + tBHH2 + heading13 + '<br>' + tBHH3 + heading14 + '<br>' + tBHH4 +  '<br>' + '<br>' + closing_remark



with open(fr"{folder_name}{back_slash}Error Analysis for {yesterday}.html", 'w') as _file:
    _file.write(table_1.to_html(index = None, header = None, justify="justify-all").replace('<td>', '<td align="center">')+ 
                merchant_failed_summary_final.reset_index().to_html(index = None, justify="center").replace('<th>','<th style = "background-color: green">').replace('<td>', '<td align="center">') + "\n\n"
                + table_2_.to_html(index = None, header = None, justify="center").replace('<td>', '<td align="center">') + service_report_final.to_html(index = None, justify="center").replace('<th>','<th style = "background-color: blue">').replace('<td>', '<td align="center">') + "\n\n" 
                + table_2_1.to_html(index = None, header = None, justify="center").replace('<td>', '<td align="center">') + "\n\n" 
                + table_2a.to_html(index = None, header = None, justify="center").replace('<td>', '<td align="center">') + major_merchant_hourly_analysis_final.reset_index().to_html(index = None, justify="center").replace('<th>','<th style = "background-color: salmon">').replace('<td>', '<td align="center">') + "\n\n" 
                
                + table_2b.to_html(index = None, header = None, justify="center").replace('<td>', '<td align="center">') + provider_analysis_final.to_html(index = None, justify="center").replace('<th>','<th style = "background-color: violet">').replace('<td>', '<td align="center">') + "\n\n" 
                + table_2c.to_html(index = None, header = None, justify="center").replace('<td>', '<td align="center">') + service_type_analysis_final.to_html(index = None, justify="center").replace('<th>','<th style = "background-color: springgreen">').replace('<td>', '<td align="center">') + "\n\n" 
                + table_2.to_html(index = None, header = None, justify="center").replace('<td>', '<td align="center">') + overall_error_analysis.reset_index().to_html(index = None, justify="center").replace('<th>','<th style = "background-color: tomato">').replace('<td>', '<td align="center">') + "\n\n" 
                + table_3.to_html(index = None, header = None, justify="center").replace('<td>', '<td align="center">') + failed_network_analysis_Mobile 3.reset_index().to_html(index = None, justify="center").replace('<th>','<th style = "background-color: gold">').replace('<td>', '<td align="center">') + "\n\n" 
                + "\n\n" + table_4.to_html(index = None, header = None, justify="center").replace('<td>', '<td align="center">')+ " \n\n" + 
                failed_network_analysis_Mobile 1.reset_index().to_html(index = None, justify="center").replace('<th>','<th style = "background-color: red">').replace('<td>', '<td align="center">')
               + "\n\n" + table_5.to_html(index = None, header = None, justify="center").replace('<td>', '<td align="center">')+ " \n\n" + 
                failed_network_analysis_Mobile 4.reset_index().to_html(index = None, justify="center").replace('<th>','<th style = "background-color: green">').replace('<td>', '<td align="center">')
               + "\n\n" + table_6.to_html(index = None, header = None, justify="center").replace('<td>', '<td align="center">') + " \n\n" + 
                failed_network_analysis_Mobile 2.reset_index().to_html(index = None, justify="center").replace('<th>','<th style = "background-color: lime">').replace('<td>', '<td align="center">') 
                + "\n\n" + table_6a.to_html(index = None, header = None, justify="center").replace('<td>', '<td align="center">') + " \n\n" + 
                service_error_analysis.to_html(justify="center").replace('<td>', '<td align="center">') 
                +"\n\n" + table_7.to_html(index = None, header = None, justify="center").replace('<td>', '<td align="center">') + " \n\n" + 
                server_failed_summary.reset_index().to_html(index = None, justify="center").replace('<th>','<th style = "background-color: magenta">').replace('<td>', '<td align="center">')
                +"\n\n" + table_8.to_html(index = None, header = None, justify="center").replace('<td>', '<td align="center">') + " \n\n" + 
                server_failed_network_analysis.to_html( justify="center").replace('<td>', '<td align="center">') )
endpoint = datetime.now()
print("Error analysis Processing Time---", endpoint - start_point)

print("      --- END OF ALL DAILY EXTRACTION AND REVIEW PROCESSES---    ")


print("Examining Yesterday's Transaction")


shago_vtu_check = daily_vtu_report[daily_vtu_report["Merchant"] == "SHAGO Payments Limited"]#.to_excel("All Shago Transactions on January 6.xlsx")
# shago_vtu_check = (4,4)
try:
	# shago_vtu_check = df_d1[df_d1["Merchant"] == "SHAGO Payments Limited"]#.to_excel("All Shago Transactions on January 6.xlsx")
	# shago_vtu_check = (4,4)
	tranx_count1 = len(shago_vtu_check)
	shagovtu_value = shago_vtu_check["FaceValue"].sum()
	shago_successful_value = shago_vtu_check[shago_vtu_check["Status"] == "Successful"]["FaceValue"].sum()
	shago_successful_value = "₦" + '{0:,.2f}'.format(shago_successful_value)
	shagovtu_value = "₦" + '{0:,.2f}'.format(shagovtu_value)
	tranx_count = '{0:,.0f}'.format(tranx_count1)

	msisdn_value = pd.concat([shago_vtu_check[shago_vtu_check["Status"] == "Successful"].groupby(["Recipient"])["FaceValue"].sum(), 
	                          shago_vtu_check[shago_vtu_check["Status"] == "Successful"].groupby(["Recipient"])["Merchant"].count()], 
	                          axis = 1)
	msisdn_value.columns = ["FaceValue", "Attempts"]
	msisdn_value = msisdn_value.reset_index()
	msisdn_value_max = msisdn_value[msisdn_value["FaceValue"] == msisdn_value["FaceValue"].max()]

	def set_column18(row):
	    if row["Attempts"] == 1:
	        return " time"
	    elif row["Attempts"] > 1:
	        return " times"
	    else:
	        return " time(s)"
	msisdn_value_max = msisdn_value_max.assign(attempted=msisdn_value_max.apply(set_column18, axis=1))
	msisdn_value_max["MSISDN Statement"] = "This MSISDN " + msisdn_value_max["Recipient"] + " topup " + msisdn_value_max["Attempts"].apply(lambda x : '{0:,.0f}'.format(x)) + msisdn_value_max["attempted"] + " with cumulative amount of ₦" + msisdn_value_max["FaceValue"].apply(lambda x : '{0:,.2f}'.format(x)) + "\n"
	msisdn_value_max.loc['Overall',:]= msisdn_value_max.sum(axis=0)

	evaaa = np.array(msisdn_value_max["MSISDN Statement"].tail(1))
	email_satte = []
	for count in evaaa:
	    email_satte.append(count)

	msisdn_value_max

	maximum_mail_statement = email_satte[0]
	print(maximum_mail_statement)

except:
	tranx_count1 = len(shago_vtu_check)
	tranx_count = len(shago_vtu_check)



import os
import smtplib
import imghdr
from email.message import EmailMessage

Email_Address = os.getenv("main_sender")
email_password2 =  os.getenv("sender_pass")

receiver_email = [Email_Address]

try:
	if tranx_count1 >= 1:
#         issue = timestamp
	        statement = f"Please check Shago recorded {tranx_count} VTU transactions on CSW Platform yesterday {yesterday}. \nOverall Successful Value = {shago_successful_value}\n\nMSISDNs with Maximum Recharge Amount \n\n{maximum_mail_statement}"
	        print(statement)
	        msg = EmailMessage()
	        msg["Subject"] = "SUSPECTED FRAUDULENT VTU TRANSACTION ON SHAGO'S ACCOUNT"
	        msg["From"] = Email_Address
	        msg["To"] = receiver_email
	        msg.set_content(f"""Hello Admin, \n\n {statement}.
	                        \nCheck was done Daily on Previous Day Transactions.
	                        \nThanks""")
	        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
	                smtp.login(Email_Address, email_password2)
	                smtp.send_message(msg)
	        print('Email sent successfully')
	else:
	        statement = f"There was No VTU Transactions on Shago's account on {yesterday}..."
	        print(statement)
	        msg = EmailMessage()
	        msg["Subject"] = "NO VTU TRANSACTION ON SHAGO'S ACCOUNT"
	        msg["From"] = Email_Address
	        msg["To"] = receiver_email
	        msg.set_content(f"""Hello Admin, \n\n {statement}.
	                        \n\n Check was done Daily on Previous Day Transactions.
	                        \n\n Thanks""")
	        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
	                smtp.login(Email_Address, email_password2)
	                smtp.send_message(msg)
	        print('Email sent successfully')
	        
	        print("Everything is fine")
except:
	if tranx_count1 >= 1:
	#         issue = timestamp
	        statement = f"Please check Shago recorded {tranx_count} VTU transactions on CSW Platform yesterday {yesterday}..."
	        print(statement)
	        msg = EmailMessage()
	        msg["Subject"] = "SUSPECTED FRAUDULENT VTU TRANSACTION ON SHAGO'S ACCOUNT"
	        msg["From"] = Email_Address
	        msg["To"] = receiver_email
	        msg.set_content(f"""Hello Admin, \n\n {statement}.
	                        \n\n Check was done Daily on Previous Day Transactions.
	                        \n\n Thanks""")
	        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
	                smtp.login(Email_Address, email_password2)
	                smtp.send_message(msg)
	        print('Email sent successfully')
	else:
	        statement = f"There was No VTU Transactions on Shago's account on {yesterday}..."
	        print(statement)
	        msg = EmailMessage()
	        msg["Subject"] = "NO VTU TRANSACTION ON SHAGO'S ACCOUNT"
	        msg["From"] = Email_Address
	        msg["To"] = receiver_email
	        msg.set_content(f"""Hello Admin, \n\n {statement}.
	                        \n\n Check was done Daily on Previous Day Transactions.
	                        \n\n Thanks""")
	        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
	                smtp.login(Email_Address, email_password2)
	                smtp.send_message(msg)
	        print('Email sent successfully')
	        
	        print("Everything is fine")
print("Extracting Suspected FRAUDULENT Transactions")



email_to = [Email_Address]
copy_email = [Email_Address] 
# email_to = Email_Address
subject = f"Error Report for {yesterday}"


def send_mail3(body):
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = Email_Address
    message["To"] = email_to
    message["cc"] = copy_email
   
    body_content = body
    message.add_alternative(body_content, subtype="html")
    #msg_body = message.as_string()
    
    context = ssl.create_default_context()
    smtp = smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context)
    smtp.login(Email_Address, email_password2)
    smtp.send_message(message)
    #
    smtp.quit()
    
# output = build_table(df, "blue_light", text_align = "center")
send_mail3(output_final)
print("Email successfully sent to the recipients")


fraudulent_count_attempted = daily_vtu_report["Recipient"].value_counts()
fraudulent_count = daily_vtu_report[daily_vtu_report["Status"] == "Successful"]["Recipient"].value_counts()
fraudulent_value_attempted = daily_vtu_report.groupby(["Recipient"])["FaceValue"].sum()
fraudulent_value = daily_vtu_report[daily_vtu_report["Status"] == "Successful"].groupby(["Recipient"])["FaceValue"].sum()
fraudulent_min_value = daily_vtu_report[daily_vtu_report["Status"] == "Successful"].groupby(["Recipient"])["FaceValue"].min()
fraudulent_max_value = daily_vtu_report[daily_vtu_report["Status"] == "Successful"].groupby(["Recipient"])["FaceValue"].max()
fraudulent_tranx = pd.concat([fraudulent_count_attempted, fraudulent_count, 
							fraudulent_value_attempted, fraudulent_value, 
							fraudulent_min_value, fraudulent_max_value], 
                             axis = 1)
fraudulent_tranx.columns = ["Count of Tranx", "Count of Successful Tranx", "Sum of Value", 
							"Sum of Successful Value", "Minimum Value", "Maximum Value"]


fraudulent_count1_attempted = daily_vtu_report.groupby(["Recipient", "Merchant"])["Merchant"].count()
fraudulent_count1 = daily_vtu_report[daily_vtu_report["Status"] == "Successful"].groupby(["Recipient", "Merchant"])["Merchant"].count()
fraudulent_valuYX_attempted = daily_vtu_report.groupby(["Recipient", "Merchant"])["FaceValue"].sum()
fraudulent_valuYX = daily_vtu_report[daily_vtu_report["Status"] == "Successful"].groupby(["Recipient", "Merchant"])["FaceValue"].sum()
fraudulent_min_valuYX = daily_vtu_report[daily_vtu_report["Status"] == "Successful"].groupby(["Recipient", "Merchant"])["FaceValue"].min()
fraudulent_max_valuYX = daily_vtu_report[daily_vtu_report["Status"] == "Successful"].groupby(["Recipient","Merchant"])["FaceValue"].max()
fraudulent_tranx1 = pd.concat([fraudulent_count1_attempted, fraudulent_count1, 
							   fraudulent_valuYX_attempted, fraudulent_valuYX, 
							   fraudulent_min_valuYX, fraudulent_max_valuYX], 
                             axis = 1)
fraudulent_tranx1.columns = ["Count of Tranx", "Count of Sucessful Tranx", "Sum of Value", 
                             "Sum of Successful Value", "Minimum Value", "Maximum Value"]

combined_fraud = fraudulent_tranx[(fraudulent_tranx["Sum of Value"] > 10000) & (fraudulent_tranx["Count of Tranx"] > 1)].sort_values(["Sum of Value"], ascending = False)
merchant_grouped_fraud = fraudulent_tranx1[(fraudulent_tranx1["Sum of Value"] > 10000) & (fraudulent_tranx1["Count of Tranx"] >= 1)].sort_values(["Sum of Value"], ascending = False).head(100)#.to_excel("Top 10 Suspected Fraudulent Transactions Day 20.xlsx")

combined_fraud['Sum of Value'] = combined_fraud['Sum of Value'].apply(lambda x : '{0:,.0f}'.format(x))
combined_fraud['Sum of Successful Value'] = combined_fraud['Sum of Successful Value'].apply(lambda x : '{0:,.0f}'.format(x))
combined_fraud['Minimum Value'] = combined_fraud['Minimum Value'].apply(lambda x : '{0:,.0f}'.format(x))
combined_fraud['Maximum Value'] = combined_fraud['Maximum Value'].apply(lambda x : '{0:,.0f}'.format(x))

merchant_grouped_fraud['Sum of Value'] = merchant_grouped_fraud['Sum of Value'].apply(lambda x : '{0:,.0f}'.format(x))
merchant_grouped_fraud['Sum of Successful Value'] = merchant_grouped_fraud['Sum of Successful Value'].apply(lambda x : '{0:,.0f}'.format(x))
merchant_grouped_fraud['Minimum Value'] = merchant_grouped_fraud['Minimum Value'].apply(lambda x : '{0:,.0f}'.format(x))
merchant_grouped_fraud['Maximum Value'] = merchant_grouped_fraud['Maximum Value'].apply(lambda x : '{0:,.0f}'.format(x))

excel_writer = pd.ExcelWriter(fr"{folder_namRV}{back_slash}Suspected Fraudulent Transactions.xlsx", engine='xlsxwriter')
combined_fraud.to_excel(excel_writer, sheet_name = "Overall Suspected")
merchant_grouped_fraud.to_excel(excel_writer, sheet_name = "Top 100 Suspected by Merchant")
excel_writer.save()

time_framYX = date_timYX
time_framRV = date_timRV


msg = EmailMessage()
msg["Subject"] = f"Suspected Fraudulent Transactions on {yesterday}"
msg["From"] = Email_Address
msg["To"] = receiver_email
# msg["cc"] = copy_email
# msg["bcc"] = blind_copy
msg.set_content(f"""    Hello Admin,  \n \n    Kindly find attached Suspected Fraudulent Transactions for {yesterday}. \n   
                 Thanks \n \n \n    Regards.""")


files = [fr"{folder_namRV}{back_slash}Suspected Fraudulent Transactions.xlsx"]
file_name = 'Suspected Fraudulent Transactions.xlsx'
for file in files:
    with open(file, 'rb') as f:
        file_data = f.read()
        file_name = file_name

    msg.add_attachment(file_data, maintype = "application", 
                   subtype = "octet-stream", filename = file_name) 
with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
    smtp.login(Email_Address, email_password2)
    smtp.send_message(msg)

print('Email sent successfully')

print("     ")
print("     ")


print("Unauthorised MSISDN Daily Check")

list_of_merchant = ["0802950","0708628","0708628", "0803578", "0928749","0708628","081745","070628",
                    "07008509", "0901756", "08066450", "090604", "090107", "0902706", "080850", 
                    "0901165", "09026873319", "0700639"]

boolean_series = daily_vtu_report["Recipient"].isin(list_of_merchant)
filtered_df = daily_vtu_report[boolean_series]
boolean_series
# filtered_df.to_excel("Fraudulent on Day 1.xlsx")
filtered_df
# account_debit_merchant = filtered_df
if len(filtered_df)>= 1:
	filtered_df.to_excel(fr"{folder_namRV}{back_slash}Blacklisted MSISDN for {yesterday}.xlsx", index = None)
	msg = EmailMessage()
	msg["Subject"] = f"Blacklisted Transactions {yesterday}"
	msg["From"] = Email_Address
	msg["To"] = receiver_email
	# msg["cc"] = copy_email
	# msg["bcc"] = blind_copy
	msg.set_content(f"""    Hello Admin,  \n \n    Kindly find attached Transaction for Blaclisted MSISDN for {yesterday}. \n   
	                 Thanks \n \n \n    Regards.""")

	files = [fr"{folder_namRV}{back_slash}Blacklisted MSISDN for {yesterday}.xlsx"]
	file_name = f'Blacklisted MSISDN for {yesterday}.xlsx'
	for file in files:
	    with open(file, 'rb') as f:
	        file_data = f.read()
	        file_name = file_name

	    msg.add_attachment(file_data, maintype = "application", 
	                   subtype = "octet-stream", filename = file_name) 
	with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
	    smtp.login(Email_Address, email_password2)
	    smtp.send_message(msg)

	print('Email sent successfully')

else:
	statement = f"No Flagged MSISDN Transaction {yesterday}..."
	msg = EmailMessage()
	msg["Subject"] = f"Blacklisted Transactions {yesterday}"
	msg["From"] = Email_Address
	msg["To"] = receiver_email
	msg.set_content(f"""Hello Admin, \n\n {statement}.
                        \n\n Check was done Daily on Previous Day Transactions.
                        \n\n Thanks""")
	with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
		smtp.login(Email_Address, email_password2)
		smtp.send_message(msg)
		print('Email sent successfully')
        
print("Everything is fine")


#Reviewing Daily Performance among top Ten merh for All telcos
df_d1 = daily_vtu_report
# Mobile 3 Calculation
merchant_airtime = df_d1[(df_d1["Status"] == "Successful") & (df_d1["Type"] == "Airtime") & (df_d1["Network"] == "Mobile 3")].groupby(["Merchant"])["FaceValue"].sum()
merchant_data = df_d1[(df_d1["Status"] == "Successful") & (df_d1["Type"] == "Data") & (df_d1["Network"] == "Mobile 3")].groupby(["Merchant"])["FaceValue"].sum()

merchant_airtime_count = df_d1[(df_d1["Status"] == "Successful") & (df_d1["Type"] == "Airtime") & (df_d1["Network"] == "Mobile 3")].groupby(["Merchant"])["FaceValue"].count()
merchant_data_count = df_d1[(df_d1["Status"] == "Successful") & (df_d1["Type"] == "Data") & (df_d1["Network"] == "Mobile 3")].groupby(["Merchant"])["FaceValue"].count()

merchant_airtime_failed = df_d1[(df_d1["Status"] != "Successful") & (df_d1["Type"] == "Airtime") & (df_d1["Network"] == "Mobile 3")].groupby(["Merchant"])["FaceValue"].count()
merchant_data_failed = df_d1[(df_d1["Status"] != "Successful") & (df_d1["Type"] == "Data") & (df_d1["Network"] == "Mobile 3")].groupby(["Merchant"])["FaceValue"].count()

merchant_airtime_overall = df_d1[(df_d1["Type"] == "Airtime") & (df_d1["Network"] == "Mobile 3")].groupby(["Merchant"])["FaceValue"].count()
merchant_data_overall = df_d1[(df_d1["Type"] == "Data") & (df_d1["Network"] == "Mobile 3")].groupby(["Merchant"])["FaceValue"].count()

merchant_total = df_d1[(df_d1["Status"] == "Successful") & (df_d1["Network"] == "Mobile 3")].groupby(["Merchant"])["FaceValue"].sum()


merchant_all = pd.concat([merchant_airtime, merchant_data, merchant_airtime_count, merchant_data_count, 
                          merchant_airtime_failed, merchant_data_failed, merchant_airtime_overall, merchant_data_overall, 
                          merchant_total], axis = 1)
merchant_all.columns = ['Successful Mobile 3 Airtime FaceValue', 'Successful Mobile 3 Data FaceValue', 
                        'Successful Mobile 3 Airtime Count', 'Successful Mobile 3 Data Count', 
                        'Mobile 3 Airtime Failed Count', 'Mobile 3 Data Failed Count', 
                        'Mobile 3 Airtime Total FaceValue', 'Mobile 3 Data Total FaceValue', 
                        'Mobile 3 Overall Total FaceValue']

merchant_all = merchant_all.fillna(0)


# Mobile 4 Calculation

merchant_airtimYX = df_d1[(df_d1["Status"] == "Successful") & (df_d1["Type"] == "Airtime") &  ((df_d1["Network"] == "Mobile 4") | (df_d1["Network"] =="Mobile 4"))].groupby(["Merchant"])["FaceValue"].sum()
merchant_data1 = df_d1[(df_d1["Status"] == "Successful") & (df_d1["Type"] == "Data") &  ((df_d1["Network"] == "Mobile 4") | (df_d1["Network"] =="Mobile 4"))].groupby(["Merchant"])["FaceValue"].sum()

merchant_airtime_count1 = df_d1[(df_d1["Status"] == "Successful") & (df_d1["Type"] == "Airtime") &  ((df_d1["Network"] == "Mobile 4") | (df_d1["Network"] =="Mobile 4"))].groupby(["Merchant"])["FaceValue"].count()
merchant_data_count1 = df_d1[(df_d1["Status"] == "Successful") & (df_d1["Type"] == "Data") &  ((df_d1["Network"] == "Mobile 4") | (df_d1["Network"] =="Mobile 4"))].groupby(["Merchant"])["FaceValue"].count()

merchant_airtime_failed1 = df_d1[(df_d1["Status"] != "Successful") & (df_d1["Type"] == "Airtime") &  ((df_d1["Network"] == "Mobile 4") | (df_d1["Network"] =="Mobile 4"))].groupby(["Merchant"])["FaceValue"].count()
merchant_data_failed1 = df_d1[(df_d1["Status"] != "Successful") & (df_d1["Type"] == "Data") &  ((df_d1["Network"] == "Mobile 4") | (df_d1["Network"] =="Mobile 4"))].groupby(["Merchant"])["FaceValue"].count()

merchant_airtime_overall1 = df_d1[(df_d1["Type"] == "Airtime") &  ((df_d1["Network"] == "Mobile 4") | (df_d1["Network"] =="Mobile 4"))].groupby(["Merchant"])["FaceValue"].count()
merchant_data_overall1 = df_d1[(df_d1["Type"] == "Data") &  ((df_d1["Network"] == "Mobile 4") | (df_d1["Network"] =="Mobile 4"))].groupby(["Merchant"])["FaceValue"].count()

merchant_total1 = df_d1[(df_d1["Status"] == "Successful") &  ((df_d1["Network"] == "Mobile 4") | (df_d1["Network"] =="Mobile 4"))].groupby(["Merchant"])["FaceValue"].sum()


merchant_all1 = pd.concat([merchant_airtimYX, merchant_data1, merchant_airtime_count1, merchant_data_count1, 
                          merchant_airtime_failed1, merchant_data_failed1, merchant_airtime_overall1, merchant_data_overall1, 
                          merchant_total1], axis = 1)
merchant_all1.columns = ['Successful Mobile 4 Airtime FaceValue', 'Successful Mobile 4 Data FaceValue', 
                        'Successful Mobile 4 Airtime Count', 'Successful Mobile 4 Data Count', 
                        'Mobile 4 Airtime Failed Count', 'Mobile 4 Data Failed Count', 
                        'Mobile 4 Airtime Total FaceValue', 'Mobile 4 Data Total FaceValue', 
                        'Mobile 4 Overall Total FaceValue']

merchant_all1 = merchant_all1.fillna(0)




# Mobile 1 Calculation

merchant_airtimRV = df_d1[(df_d1["Status"] == "Successful") & (df_d1["Type"] == "Airtime") & (df_d1["Network"] == "Mobile 1")].groupby(["Merchant"])["FaceValue"].sum()
merchant_data2 = df_d1[(df_d1["Status"] == "Successful") & (df_d1["Type"] == "Data") & (df_d1["Network"] == "Mobile 1")].groupby(["Merchant"])["FaceValue"].sum()

merchant_airtime_count2 = df_d1[(df_d1["Status"] == "Successful") & (df_d1["Type"] == "Airtime") & (df_d1["Network"] == "Mobile 1")].groupby(["Merchant"])["FaceValue"].count()
merchant_data_count2 = df_d1[(df_d1["Status"] == "Successful") & (df_d1["Type"] == "Data") & (df_d1["Network"] == "Mobile 1")].groupby(["Merchant"])["FaceValue"].count()

merchant_airtime_failed2 = df_d1[(df_d1["Status"] != "Successful") & (df_d1["Type"] == "Airtime") & (df_d1["Network"] == "Mobile 1")].groupby(["Merchant"])["FaceValue"].count()
merchant_data_failed2 = df_d1[(df_d1["Status"] != "Successful") & (df_d1["Type"] == "Data") & (df_d1["Network"] == "Mobile 1")].groupby(["Merchant"])["FaceValue"].count()

merchant_airtime_overall2 = df_d1[(df_d1["Type"] == "Airtime") & (df_d1["Network"] == "Mobile 1")].groupby(["Merchant"])["FaceValue"].count()
merchant_data_overall2 = df_d1[(df_d1["Type"] == "Data") & (df_d1["Network"] == "Mobile 1")].groupby(["Merchant"])["FaceValue"].count()

merchant_total2 = df_d1[(df_d1["Status"] == "Successful") & (df_d1["Network"] == "Mobile 1")].groupby(["Merchant"])["FaceValue"].sum()


merchant_all2 = pd.concat([merchant_airtimRV, merchant_data2, merchant_airtime_count2, merchant_data_count2, 
                          merchant_airtime_failed2, merchant_data_failed2, merchant_airtime_overall2, merchant_data_overall2, 
                          merchant_total2], axis = 1)
merchant_all2.columns = ['Successful Mobile 1 Airtime FaceValue', 'Successful Mobile 1 Data FaceValue', 
                        'Successful Mobile 1 Airtime Count', 'Successful Mobile 1 Data Count', 
                        'Mobile 1 Airtime Failed Count', 'Mobile 1 Data Failed Count', 
                        'Mobile 1 Airtime Total FaceValue', 'Mobile 1 Data Total FaceValue', 
                        'Mobile 1 Overall Total FaceValue']

merchant_all2 = merchant_all2.fillna(0)



# Mobile 2 Calculation
merchant_airtime3 = df_d1[(df_d1["Status"] == "Successful") & (df_d1["Type"] == "Airtime") & (df_d1["Network"] == "Mobile 2")].groupby(["Merchant"])["FaceValue"].sum()
merchant_data3 = df_d1[(df_d1["Status"] == "Successful") & (df_d1["Type"] == "Data") & (df_d1["Network"] == "Mobile 2")].groupby(["Merchant"])["FaceValue"].sum()

merchant_airtime_count3 = df_d1[(df_d1["Status"] == "Successful") & (df_d1["Type"] == "Airtime") & (df_d1["Network"] == "Mobile 2")].groupby(["Merchant"])["FaceValue"].count()
merchant_data_count3 = df_d1[(df_d1["Status"] == "Successful") & (df_d1["Type"] == "Data") & (df_d1["Network"] == "Mobile 2")].groupby(["Merchant"])["FaceValue"].count()

merchant_airtime_failed3 = df_d1[(df_d1["Status"] != "Successful") & (df_d1["Type"] == "Airtime") & (df_d1["Network"] == "Mobile 2")].groupby(["Merchant"])["FaceValue"].count()
merchant_data_failed3 = df_d1[(df_d1["Status"] != "Successful") & (df_d1["Type"] == "Data") & (df_d1["Network"] == "Mobile 2")].groupby(["Merchant"])["FaceValue"].count()

merchant_airtime_overall3 = df_d1[(df_d1["Type"] == "Airtime") & (df_d1["Network"] == "Mobile 2")].groupby(["Merchant"])["FaceValue"].count()
merchant_data_overall3 = df_d1[(df_d1["Type"] == "Data") & (df_d1["Network"] == "Mobile 2")].groupby(["Merchant"])["FaceValue"].count()

merchant_total3 = df_d1[(df_d1["Status"] == "Successful") & (df_d1["Network"] == "Mobile 2")].groupby(["Merchant"])["FaceValue"].sum()


merchant_all3 = pd.concat([merchant_airtime3, merchant_data3, merchant_airtime_count3, merchant_data_count3, 
                          merchant_airtime_failed3, merchant_data_failed3, merchant_airtime_overall3, merchant_data_overall3, 
                          merchant_total3], axis = 1)
merchant_all3.columns = ['Successful Mobile 2 Airtime FaceValue', 'Successful Mobile 2 Data FaceValue', 
                        'Successful Mobile 2 Airtime Count', 'Successful Mobile 2 Data Count', 
                        'Mobile 2 Airtime Failed Count', 'Mobile 2 Data Failed Count', 
                        'Mobile 2 Airtime Total FaceValue', 'Mobile 2 Data Total FaceValue', 
                        'Mobile 2 Overall Total FaceValue']

merchant_all3 = merchant_all3.fillna(0)



merchant_all.loc['Overall',:]= merchant_all.sum(axis=0)
merchant_all1.loc['Overall',:]= merchant_all1.sum(axis=0)
merchant_all2.loc['Overall',:]= merchant_all2.sum(axis=0)
merchant_all3.loc['Overall',:]= merchant_all3.sum(axis=0)


merchant_all = merchant_all.sort_values(["Mobile 3 Overall Total FaceValue"], ascending = False)
merchant_all1 = merchant_all1.sort_values(["Mobile 4 Overall Total FaceValue"], ascending = False)
merchant_all2 = merchant_all2.sort_values(["Mobile 1 Overall Total FaceValue"], ascending = False)
merchant_all3 = merchant_all3.sort_values(["Mobile 2 Overall Total FaceValue"], ascending = False)


# Mobile 3 TOP TEN
merchant_all["Mobile 3 Airtime % Contribution"] = round((merchant_all["Successful Mobile 3 Airtime FaceValue"]/ merchant_all["Mobile 3 Overall Total FaceValue"])*100, 1).fillna(0)
merchant_all["Mobile 3 Data % Contribution"] = round((merchant_all["Successful Mobile 3 Data FaceValue"]/ merchant_all["Mobile 3 Overall Total FaceValue"])*100, 1).fillna(0)
merchant_all["Mobile 3 Airtime FaceValue (%Contribution)"] = merchant_all["Successful Mobile 3 Airtime FaceValue"].apply(lambda x : '{0:,.0f}'.format(x)) + " (" + merchant_all["Mobile 3 Airtime % Contribution"].astype(str) + "%)"
merchant_all["Mobile 3 Data FaceValue (%Contribution)"] = merchant_all["Successful Mobile 3 Data FaceValue"].apply(lambda x : '{0:,.0f}'.format(x)) + " (" + merchant_all["Mobile 3 Data % Contribution"].astype(str) + "%)"
merchant_all_final = merchant_all.head(11).reset_index()
merchant_all_final["Merchant"] = merchant_all_final["Merchant"].replace(["FLT TECHNOLOGY SOLUTIONS LIMITED"], ["FLT"])
merchant_all_final["Merchant"] = merchant_all_final["Merchant"] + " (" + merchant_all_final["Mobile 3 Overall Total FaceValue"].apply(lambda x : '{0:,.0f}'.format(x)) + ")"
merchant_all_final = merchant_all_final[["Merchant", "Mobile 3 Airtime FaceValue (%Contribution)", "Mobile 3 Data FaceValue (%Contribution)"]]


# Mobile 4 TOP TEN
merchant_all1["Mobile 4 Airtime % Contribution"] = round((merchant_all1["Successful Mobile 4 Airtime FaceValue"]/ merchant_all1["Mobile 4 Overall Total FaceValue"])*100, 1).fillna(0)
merchant_all1["Mobile 4 Data % Contribution"] = round((merchant_all1["Successful Mobile 4 Data FaceValue"]/ merchant_all1["Mobile 4 Overall Total FaceValue"])*100, 1).fillna(0)

merchant_all1["Mobile 4 Airtime FaceValue (%Contribution)"] = merchant_all1["Successful Mobile 4 Airtime FaceValue"].apply(lambda x : '{0:,.0f}'.format(x)) + " (" + merchant_all1["Mobile 4 Airtime % Contribution"].astype(str) + "%)"
merchant_all1["Mobile 4 Data FaceValue (%Contribution)"] = merchant_all1["Successful Mobile 4 Data FaceValue"].apply(lambda x : '{0:,.0f}'.format(x)) + " (" + merchant_all1["Mobile 4 Data % Contribution"].astype(str) + "%)"

merchant_all_final1 = merchant_all1.head(11).reset_index()
merchant_all_final1["Merchant"] = merchant_all_final1["Merchant"].replace(["FLT TECHNOLOGY SOLUTIONS LIMITED"], ["FLT"])
merchant_all_final1["Merchant"] = merchant_all_final1["Merchant"] + " (" + merchant_all_final1["Mobile 4 Overall Total FaceValue"].apply(lambda x : '{0:,.0f}'.format(x)) + ")"

merchant_all_final1 = merchant_all_final1[["Merchant", "Mobile 4 Airtime FaceValue (%Contribution)", "Mobile 4 Data FaceValue (%Contribution)"]]
merchant_all_final1["Merchant"] = merchant_all_final1["Merchant"].replace(["FLT TECHNOLOGY SOLUTIONS LIMITED"], ["FLT"])
merchant_all_final1


# Mobile 1 TOP TEN

merchant_all2["Mobile 1 Airtime % Contribution"] = round((merchant_all2["Successful Mobile 1 Airtime FaceValue"]/ merchant_all2["Mobile 1 Overall Total FaceValue"])*100, 1).fillna(0)
merchant_all2["Mobile 1 Data % Contribution"] = round((merchant_all2["Successful Mobile 1 Data FaceValue"]/ merchant_all2["Mobile 1 Overall Total FaceValue"])*100, 1).fillna(0)

merchant_all2["Mobile 1 Airtime FaceValue (%Contribution)"] = merchant_all2["Successful Mobile 1 Airtime FaceValue"].apply(lambda x : '{0:,.0f}'.format(x)) + " (" + merchant_all2["Mobile 1 Airtime % Contribution"].astype(str) + "%)"
merchant_all2["Mobile 1 Data FaceValue (%Contribution)"] = merchant_all2["Successful Mobile 1 Data FaceValue"].apply(lambda x : '{0:,.0f}'.format(x)) + " (" + merchant_all2["Mobile 1 Data % Contribution"].astype(str) + "%)"

merchant_all_final2 = merchant_all2.head(11).reset_index()
merchant_all_final2["Merchant"] = merchant_all_final2["Merchant"].replace(["FLT TECHNOLOGY SOLUTIONS LIMITED"], ["FLT"])
merchant_all_final2["Merchant"] = merchant_all_final2["Merchant"] + " (" + merchant_all_final2["Mobile 1 Overall Total FaceValue"].apply(lambda x : '{0:,.0f}'.format(x)) + ")"

merchant_all_final2 = merchant_all_final2[["Merchant", "Mobile 1 Airtime FaceValue (%Contribution)", "Mobile 1 Data FaceValue (%Contribution)"]]
merchant_all_final2["Merchant"] = merchant_all_final2["Merchant"].replace(["FLT TECHNOLOGY SOLUTIONS LIMITED"], ["FLT"])
merchant_all_final2


# Mobile 2 TOP TEN

merchant_all3["Mobile 2 Airtime % Contribution"] = round((merchant_all3["Successful Mobile 2 Airtime FaceValue"]/ merchant_all3["Mobile 2 Overall Total FaceValue"])*100, 1).fillna(0)
merchant_all3["Mobile 2 Data % Contribution"] = round((merchant_all3["Successful Mobile 2 Data FaceValue"]/ merchant_all3["Mobile 2 Overall Total FaceValue"])*100, 1).fillna(0)

merchant_all3["Mobile 2 Airtime FaceValue (%Contribution)"] = merchant_all3["Successful Mobile 2 Airtime FaceValue"].apply(lambda x : '{0:,.0f}'.format(x)) + " (" + merchant_all3["Mobile 2 Airtime % Contribution"].astype(str) + "%)"
merchant_all3["Mobile 2 Data FaceValue (%Contribution)"] = merchant_all3["Successful Mobile 2 Data FaceValue"].apply(lambda x : '{0:,.0f}'.format(x)) + " (" + merchant_all3["Mobile 2 Data % Contribution"].astype(str) + "%)"

merchant_all_final3 = merchant_all3.head(11).reset_index()
merchant_all_final3["Merchant"] = merchant_all_final3["Merchant"].replace(["FLT TECHNOLOGY SOLUTIONS LIMITED"], ["FLT"])
merchant_all_final3["Merchant"] = merchant_all_final3["Merchant"] + " (" + merchant_all_final3["Mobile 2 Overall Total FaceValue"].apply(lambda x : '{0:,.0f}'.format(x)) + ")"

merchant_all_final3 = merchant_all_final3[["Merchant", "Mobile 2 Airtime FaceValue (%Contribution)", "Mobile 2 Data FaceValue (%Contribution)"]]
merchant_all_final3["Merchant"] = merchant_all_final3["Merchant"].replace(["FLT TECHNOLOGY SOLUTIONS LIMITED"], ["FLT"])
merchant_all_final3



table_1 = pd.DataFrame([f"--Mobile 3    Performance    Analysis---  Top      Ten       Merchant     on {yesterday}", " ** "])
table_1.rename(columns=table_1.iloc[0], inplace = True)
table_1.drop([0,1], inplace = True)

table_2 = pd.DataFrame([" ** ", f"--Mobile 4    Performance    Analysis--  Top      Ten       Merchant     on {yesterday}"])
table_2.rename(columns=table_2.iloc[1], inplace = True)
table_2.drop([0,1], inplace = True)
table_2

table_3 = pd.DataFrame([" ** ", f"--Mobile 1    Performance    Analysis--  Top      Ten       Merchant     on {yesterday}"])
table_3.rename(columns=table_3.iloc[1], inplace = True)
table_3.drop([0,1], inplace = True)
table_3

table_4 = pd.DataFrame([" ** ", f"--Mobile 2    Performance    Analysis--  Top      Ten       Merchant     on {yesterday}"])
table_4.rename(columns=table_4.iloc[1], inplace = True)
table_4.drop([0,1], inplace = True)
table_4


# merchant_all_final.to_html(index = None, header = None, justify="justify-all").replace('<td>', '<td align="center">')

with open(fr"{folder_name}{back_slash}Performance Analysis for {yesterday} Updated2.html", 'w') as _file:
    _file.write(table_1.to_html(index = None, header = True, justify="justify-all").replace('<th>','<th style = "background-color: gold">').replace('<td>', '<td align="center">')+ 
                merchant_all_final.to_html(index = None, header = True, justify="justify-all").replace('<th>','<th style = "background-color: yellow">').replace('<td>', '<td align="center">') + "\n\n"
                + table_2.to_html(index = None, header = True, justify="center").replace('<th>','<th style = "background-color: darkgreen">').replace('<td>', '<td align="center">') 
                + merchant_all_final1.to_html(index = None, justify="center").replace('<th>','<th style = "background-color: lightgreen">').replace('<td>', '<td align="center">') 
                + table_3.to_html(index = None, header = True, justify="center").replace('<th>','<th style = "background-color: darkred">').replace('<td>', '<td align="center">') 
                + merchant_all_final2.to_html(index = None, justify="center").replace('<th>','<th style = "background-color: lightred">').replace('<td>', '<td align="center">')
                + table_4.to_html(index = None, header = True, justify="center").replace('<th>','<th style = "background-color: darkgreen">').replace('<td>', '<td align="center">') 
                + merchant_all_final3.to_html(index = None, justify="center").replace('<th>','<th style = "background-color: lightgreen">').replace('<td>', '<td align="center">') +"\n\n" )


from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.message import EmailMessage
from email import encoders
import smtplib, ssl
from pretty_html_table import build_table
import re
import sys, ast
import subprocess


from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from subprocess import Popen, PIPE
msg = MIMEMultipart('alternative')

email_from = os.getenv("main_sender")
email_password =  os.getenv("sender_pass")
email_to = [email_from] 

subject = f"Performance Analysis for {yesterday}"

def send_mail4(body):
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = email_from
    message["To"] = email_to
    
    body_content = body
    message.add_alternative(body_content, subtype="html")
    #msg_body = message.as_string()
    
    context = ssl.create_default_context()
    smtp = smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context)
    smtp.login(email_from, email_password)
    smtp.send_message(message)
    #
    smtp.quit()
    
output1 = build_table(merchant_all_final, "yellow_dark", text_align = "center")
output2 = build_table(merchant_all_final1, "green_dark", text_align = "center")
output3 = build_table(merchant_all_final2, "red_dark", text_align = "center")
output4 = build_table(merchant_all_final3, "green_light", text_align = "center")

salutation1 = f"<br><strong>Hello Admin, <br> <br> Kindly see below the Performance Analysis for {yesterday}</strong></br>"
Mobile 3_title = f"<br><strong>Mobile 3 PERFORMANCE AMONG TOP TEN merh FOR {yesterday}</strong></br>"
Mobile 4_title = f"<br><strong>Mobile 4 PERFORMANCE AMONG TOP TEN merh FOR {yesterday}</strong></br>"
Mobile 1_title = f"<br><strong>Mobile 1 PERFORMANCE AMONG TOP TEN merh FOR {yesterday}</strong></br>"
mobile_title = f"<br><strong>Mobile 2 PERFORMANCE AMONG TOP TEN merh FOR {yesterday}</strong></br>"

closing_remark = "<br><strong>Thank you</strong>"

output = salutation1 + "<br>" "<br>"+ Mobile 3_title + output1 + "<br>" "<br>" + Mobile 4_title + output2 + "<br>" "<br>" + Mobile 1_title + output3 + "<br>" "<br>" + mobile_title + output4 +  "<br>" "<br>" + closing_remark


send_mail4(output)