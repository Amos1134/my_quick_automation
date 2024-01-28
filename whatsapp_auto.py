import keyboard as k   # for simulating ketboard keys
import pyautogui       # for automating mouse and keyboard actions
import time            # for adding delay in program execution
import pandas as pd    # for reading and manipulating Excel files
from urllib.parse import quote # for opening URLs in web browser
import webbrowser as web  # for URL encoding special characters

def send_whatsapp(data_file_excel, message_file_text, x_cord=733,y_cord=953):
    df = pd.read_excel(data_file_excel, dtype={"Recipient":str})
    name = df["Name"].values
    contact = df["Recipient"].values
    files = message_file_text
    
    with open (files) as f:
        filed_data = f.read()
    zipped = zip(name, contact)
    
    counter = 0
    
    for (a,b) in zipped:
        msg = filed_data.format(a)
        web.open(f"https://web.whatsapp.com/send?phone={b}&text={quote(msg)}")
        time.sleep(15)
        pyautogui.click(x_cord, y_cord)
        time.sleep(2)
        k.press_and_release('enter')
        time.sleep(2)
        k.press_and_release('ctrl+ w')
        time.sleep(1)
        counter +=1
        print(counter, "- Message Sent...!!")
        
    print("Done!")
    
excel_path = r"C:\Users\Amos\Documents\Credit Switch\Merchant Record\Data warehouse Script\excel_test.xlsx"
text_path = r"C:\Users\Amos\Documents\Credit Switch\Merchant Record\Data warehouse Script\message_to_send.txt"
send_whatsapp(excel_path, text_path)

