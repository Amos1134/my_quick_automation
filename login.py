
def database1_connection(query, column_name):
    
    from dotenv import load_dotenv
    load_dotenv()
    import os
    username = os.getenv("db_user")
    password = os.getenv("ltd_db_pass")
    host = os.getenv("ltd_db_host")
    port = 3306
    database = os.getenv("database1")
    import mysql.connector as mariadb
    import sys
    import datetime as datetime
    from datetime import datetime
    import pandas as pd

    print("Connecting to Database1...")
    connection_start_time_ltd = datetime.now()
    try:
        ltd_conn = mariadb.connect(
            user=username,
            password=password,
            host=host,
            port=port,
            database=database
        )
    except mariadb.Error as e:
        print(f"Error connecting to LTD Platform: {e}")
        sys.exit(1)
    connection_end_time_ltd = datetime.now()
    print("Connection Time to LTD Database---", connection_end_time_ltd - connection_start_time_ltd)# "Extraction time---", end_time - start_time

    # Get Cursor
    print("Extracting report From LTD Platform...")
    extraction_start_time_ltd = datetime.now()
    ltd_cur = ltd_conn.cursor()

    ltd_cur.execute(query)
    ltd_result = ltd_cur.fetchall()
    extraction_end_time_ltd = datetime.now()
    print("Extraction Time to LTD Database---", extraction_end_time_ltd - extraction_start_time_ltd)# "Extraction time---", end_time - start_time

    ltd_dataset = pd.DataFrame(ltd_result)
    ltd_dataset

    try:
        ltd_dataset.columns = column_name
    except:
        tranx_columns1 = column_name
        ltd_dataset = pd.DataFrame(columns = tranx_columns1)
    ltd_dataset["Platform"] = "LTD"
    print("Total Transactions Count on LTD---", len(ltd_dataset))
    return ltd_dataset


def database2_connection(query, column_name):
    print("Query to connect to Vend Platform...")
    from dotenv import load_dotenv
    load_dotenv()
    import os
    username2 = os.getenv("db_user")
    password2 = os.getenv("vend_db_pass")
    host2 = os.getenv("vend_db_host")
    port2 = 3306
    database2 = os.getenv("database1")
    import mysql.connector as mariadb
    import sys
    import datetime as datetime
    from datetime import datetime
    import pandas as pd

    print("Connecting to LTD Database...")
    connection_start_time_ltd = datetime.now()
    try:
        vend_conn = mariadb.connect(
            user=username2,
            password=password2,
            host=host2,
            port=port2,
            database=database2
        )
    except mariadb.Error as e:
        print(f"Error connecting to LTD Platform: {e}")
        sys.exit(1)
    connection_end_time_ltd = datetime.now()
    print("Connection Time to LTD Database---", connection_end_time_ltd - connection_start_time_ltd)# "Extraction time---", end_time - start_time


    # Get Cursor
    print("Extracting report From LTD Platform...")
    extraction_start_time_ltd = datetime.now()
    vend_cur = vend_conn.cursor()

    vend_cur.execute(query)
    vend_result = vend_cur.fetchall()
    extraction_end_time_ltd = datetime.now()
    print("Extraction Time to LTD Database---", extraction_end_time_ltd - extraction_start_time_ltd)# "Extraction time---", end_time - start_time

    vend_dataset = pd.DataFrame(vend_result)
    vend_dataset

    try:
        vend_dataset.columns = column_name
    except:
        tranx_columns1 = column_name
        vend_dataset = pd.DataFrame(columns = tranx_columns1)
    vend_dataset["Platform"] = "Vend"
    print("Total Transactions Count on LTD---", len(vend_dataset))
    return vend_dataset


def portal_connection(query, column_name):
    print("Query to connect to Vend Platform...")
    from dotenv import load_dotenv
    load_dotenv()
    import os
    # Portal Connection
    username3 = os.getenv("db_user")
    password3 = os.getenv("portal_db_pass")
    host3 = os.getenv("portal_db_host")
    port3 = 3306
    database3 = os.getenv("database1")
    import mysql.connector as mariadb
    import sys
    import datetime as datetime
    from datetime import datetime
    import pandas as pd

    print("Connecting to Database3...")
    connection_start_time_ltd = datetime.now()
    try:
        portal_conn = mariadb.connect(
            user=username3,
            password=password3,
            host=host3,
            port=port3,
            database=database3
        )
    except mariadb.Error as e:
        print(f"Error connecting to Portal Platform: {e}")
        sys.exit(1)
    connection_end_time_ltd = datetime.now()
    print("Connection Time to Portal Database---", connection_end_time_ltd - connection_start_time_ltd)# "Extraction time---", end_time - start_time


    # Get Cursor
    print("Extracting report From Portal Platform...")
    extraction_start_time_ltd = datetime.now()
    portal_cur = portal_conn.cursor()

    portal_cur.execute(query)
    portal_result = portal_cur.fetchall()
    extraction_end_time_ltd = datetime.now()
    print("Extraction Time to Portal Database---", extraction_end_time_ltd - extraction_start_time_ltd)# "Extraction time---", end_time - start_time

    portal_dataset = pd.DataFrame(portal_result)
    portal_dataset

    try:
        portal_dataset.columns = column_name
    except:
        tranx_columns1 = column_name
        portal_dataset = pd.DataFrame(columns = tranx_columns1)
    portal_dataset["Platform"] = "Portal"
    print("Total Transactions Count on Portal---", len(portal_dataset))
    return portal_dataset


def strabic_connection(query, column_name):
    print("Query to connect to Vend Platform...")
    from dotenv import load_dotenv
    load_dotenv()
    import os
    # Portal Connection
    username = os.getenv("3pp_db_user")
    password = os.getenv("3pp_db_pass")
    host = os.getenv("3pp_db_host")
    port = os.getenv("3pp_db_port")
    database = os.getenv("3pp_db_name")
    import mysql.connector as mariadb
    import sys
    import datetime as datetime
    from datetime import datetime
    import pandas as pd

    print("Connecting to 3PP DB Server...")
    try:
        conn = mariadb.connect(
            user=username,
            password=password,
            host=host,
            port=port,
            database=database
        )
    except mariadb.Error as e:
        print(f"Error connecting to 3PP DB Server: {e}")
        sys.exit(1)

    # Get Cursor
    cur = conn.cursor()

    print("Connected to 3PP DB Server Successfull!!!")
    print(" ")
    print("Extracting report from 3PP DB Tables...")
    process_start_time1 = datetime.now()

    cur.execute(query)
    stabic_result = cur.fetchall()
    process_end_time1 = datetime.now()

    print("Extraction time---", process_end_time1 - process_start_time1)# "Extraction time---", end_time - start_time
    stanbic_dataset = pd.DataFrame(stabic_result)

    print(" ")
    print("Report Extracted from Account Debits Successfully!!!")

    try: 
        stanbic_dataset.columns = column_name
                        
    except:
        tranx_columns1 = column_name
        stanbic_dataset = pd.DataFrame(columns = tranx_columns1)

    stanbic_dataset["Platform"] = "Stanbic 3PP"
    return stanbic_dataset


def downtime_email_config(Merchant_name, product_name, emailpassword, Sender, reciever, copied, Message_needed, product_failed_count, comments, error_data):
    import smtplib
    import imghdr
    from email.message import EmailMessage
    msg = EmailMessage()
    msg["Subject"] = f"{Merchant_name} Downtime Alert for {product_name}"
    msg["From"] = Sender
    msg["To"] = reciever
    msg["CC"] = copied
    msg.set_content(f"""Hello Admin, \n\n {Message_needed} \n\n In the last 100 transactions, {product_failed_count} Failed.
                    \n\n{comments} {error_data}.
                    \n\n Thanks""")
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(Sender, emailpassword)
            smtp.send_message(msg)
    print('Email sent successfully')




