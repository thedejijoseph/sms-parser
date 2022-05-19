#!/usr/bin/env python
# coding: utf-8

# In[493]:


# import re

import sys
from datetime import datetime

import requests as r


# In[155]:


first_bank = "https://worker-service-prod.herokuapp.com/transactions/fetch/mystash/MSBgu2abmqk202203249586"
access_bank = "https://worker-service-prod.herokuapp.com/transactions/fetch/mystash/MSB2kug63rt202203814200"
gt_bank = "https://worker-service-prod.herokuapp.com/transactions/fetch/mystash/MSBido4xo6720220411202755"
keystone_bank_1 = "https://worker-service-prod.herokuapp.com/transactions/fetch/mystash/MSBqho5jf19202204617034"
keystone_bank_2 = "https://worker-service-prod.herokuapp.com/transactions/fetch/mystash/MSBx3zu9cil202205177018"
poliaris_1 = "https://worker-service-prod.herokuapp.com/transactions/fetch/mystash/MSB5re47yqf2022047231557"
poliaris_2 = "https://worker-service-prod.herokuapp.com/transactions/fetch/mystash/MSB7dxrbtnv20220410151115"
sterling = "https://worker-service-prod.herokuapp.com/transactions/fetch/mystash/MSBlsl4tvra2022041114058"
standard = "https://worker-service-prod.herokuapp.com/transactions/fetch/mystash/MSBmib4r9ss20220478215"
uba = "https://worker-service-prod.herokuapp.com/transactions/fetch/mystash/MSBrefruh08202204484746"
union_bank = "https://worker-service-prod.herokuapp.com/transactions/fetch/mystash/MSBkcboxo2i2022046231250"
wema_bank = "https://worker-service-prod.herokuapp.com/transactions/fetch/mystash/MSByooviikl2022032312748"
zenith = "https://worker-service-prod.herokuapp.com/transactions/fetch/mystash/MSBir4ngg1v2022032882648"
zenith_2 = "https://worker-service-prod.herokuapp.com/transactions/fetch/mystash/MSBqoixnexo2022041213740"

bank_urls = {
    "first_bank": "https://worker-service-prod.herokuapp.com/transactions/fetch/mystash/MSBgu2abmqk202203249586",
    "access_bank": "https://worker-service-prod.herokuapp.com/transactions/fetch/mystash/MSB2kug63rt202203814200",
    "gt_bank": "https://worker-service-prod.herokuapp.com/transactions/fetch/mystash/MSBido4xo6720220411202755",
    "keystone_bank_1": "https://worker-service-prod.herokuapp.com/transactions/fetch/mystash/MSBqho5jf19202204617034",
    "keystone_bank_2": "https://worker-service-prod.herokuapp.com/transactions/fetch/mystash/MSBx3zu9cil202205177018",
    "poliaris_1": "https://worker-service-prod.herokuapp.com/transactions/fetch/mystash/MSB5re47yqf2022047231557",
    "poliaris_2": "https://worker-service-prod.herokuapp.com/transactions/fetch/mystash/MSB7dxrbtnv20220410151115",
    "sterling": "https://worker-service-prod.herokuapp.com/transactions/fetch/mystash/MSBlsl4tvra2022041114058",
    "standard": "https://worker-service-prod.herokuapp.com/transactions/fetch/mystash/MSBmib4r9ss20220478215",
    "uba": "https://worker-service-prod.herokuapp.com/transactions/fetch/mystash/MSBrefruh08202204484746",
    "union_bank": "https://worker-service-prod.herokuapp.com/transactions/fetch/mystash/MSBkcboxo2i2022046231250",
    "wema_bank": "https://worker-service-prod.herokuapp.com/transactions/fetch/mystash/MSByooviikl2022032312748",
    "zenith": "https://worker-service-prod.herokuapp.com/transactions/fetch/mystash/MSBir4ngg1v2022032882648",
    "zenith_2": "https://worker-service-prod.herokuapp.com/transactions/fetch/mystash/MSBqoixnexo2022041213740"
}

messages = {}
for bank, url in bank_urls.items():
    resp = r.get(url)
    banks_messages = resp.json()
    messages[bank] = banks_messages
# In[488]:


def entry(payload):
    """
    send request to a handler
    
    assume request looks like this:
        {
            "uid": string,
            "sender": string,
            "message": string,
            "date": datetime
        }
    """
    selected_handler = handlers.get(payload['sender'], None)
    if selected_handler:
        handler = selected_handler(payload)
        return handler.process()
    else:
        return {
            "error": f"no handler for {payload['sender']}"
        }


# In[476]:


class Handler:
    def __init__(self, payload):
        assert type(payload) is dict
        self.payload = payload
        self.uid = self.payload.get('uid', None)
        self.sender = self.payload.get('sender', None)
        self.message = self.payload.get('message')
        self.date = self.payload.get('date', None)
        
        self.empty = {
            "bank": "bank",
            "tx_type": "",
            "amount": "",
            "currency": "",
            "account_number": "",
            "datetime": "",
            "description": "",
            "avail_bal": "",
            "message": self.message,
            "error": "could not parse 'message'"
        }


# In[477]:


class FirstBankHandler(Handler):
    def __init__(self, request):
        super().__init__(request)
    
    def process(self):
        results = self.string_processor()
        if results:
            return results
        else:
            return self.empty
    
    def string_processor(self):
        tx, acc_no, _, amt, _, date, time, _, *desc = self.message.split(" ")
        
        tx_type = tx[:-1].lower()
        self.tx_type_short = "dr" if tx_type == "debit" else "cr"
        
        self.account_number = acc_no
        
        amount = amt[3:].replace(',', '')
        self.amount = float(amount)
        self.currency = amt[:3]
        
        date_time = f"{date} {time}"
        self.datetime = datetime.strptime(date_time, '%d-%b-%Y %H:%M:%S')
        
        self.description = ' '.join(desc)
        
        result = {
            "bank": "firstbank",
            "tx_type": self.tx_type_short,
            "amount": self.amount,
            "currency": self.currency,
            "account_number": self.account_number,
            "datetime": self.datetime,
            "description": self.description
        }
        
        return result


# In[478]:


class AccessBankHandler(Handler):
    def __init__(self, request):
        super().__init__(request)
    
    def process(self):
        results = self.string_processor()
        if results:
            return results
        else:
            return self.empty
    
    def string_processor(self):
        tx, amt, acc_no, desc, date, avail_bal, total = self.message.split("\n")
        
        tx_type = tx.lower()
        self.tx_type_short = "dr" if tx_type == "debit" else "cr"
         
        amount = amt[7:].replace(',', '')
        self.amount = float(amount)
        self.currency = amt[4:7]
        
        self.account_number = acc_no[4:]
        
        self.description = desc[5:]
        
        date = date[5:]
        self.datetime = datetime.strptime(date, '%d/%m/%Y')
        
        avail_bal = avail_bal[13:].replace(',', '')
        self.avail_bal = float(avail_bal)
        
        total = total[9:].replace(',', '')
        self.total = float(total)
        
        
        result = {
            "bank": "accessbank",
            "tx_type": self.tx_type_short,
            "amount": self.amount,
            "currency": self.currency,
            "account_number": self.account_number,
            "datetime": self.datetime,
            "description": self.description,
            "avail_bal": self.avail_bal,
            "total": self.total
        }
        
        return result


# In[501]:


class GTBankHandler(Handler):
    def __init__(self, request):
        super().__init__(request)
    
    def process(self):
        results = self.string_processor()
        if results:
            return results
        else:
            return self.empty
    
    def string_processor(self):
        if not self.message.startswith("Acc"):
            return None
        if "\r" in self.message:
            acc_no, amt_tx, desc, bal, date_time = self.message.split("\r\n")
        else:
            acc_no, amt_tx, desc, bal, date_time = self.message.split("\n")
        
        self.account_number = acc_no[6:]
        
        amount = amt_tx[8:-3].replace(',', '')
        self.amount = float(amount)
        self.currency = amt_tx[5:8]
        
        tx_type = amt_tx[-2:].lower()
        self.tx_type_short = tx_type
        
        self.description = desc[6:]
        
        bal = bal[14:].replace(',', '')
        self.avail_bal = float(bal)
        
        date_time = date_time[6:].title().strip()
        self.datetime = datetime.strptime(date_time, '%d-%b-%Y %H:%M')
        
        result = {
            "bank": "gtbank",
            "tx_type": self.tx_type_short,
            "amount": self.amount,
            "currency": self.currency,
            "account_number": self.account_number,
            "datetime": self.datetime,
            "description": self.description,
            "avail_bal": self.avail_bal
        }
        
        return result


# In[479]:


class KeystoneBankHandler(Handler):
    def __init__(self, request):
        super().__init__(request)
    
    def process(self):
        results = self.string_processor()
        if results:
            return results
        else:
            return self.empty
    
    def string_processor(self):
        tx, acc_no, amt, desc, date_time, bal, _ = self.message.split("\n")
        
        tx_type = tx.lower()
        self.tx_type_short = "dr" if tx_type == "debit" else "cr"
        
        self.account_number = acc_no[5:]
        
        amount = amt[5:].replace(',', '')
        self.amount = float(amount)
        self.currency = "NGN" if amt[4:5] == "N" else None
        
        self.description = desc[5:]
        
        date_time = date_time[5:]
        self.datetime = datetime.strptime(date_time, '%d-%m-%Y %H:%M')
        
        bal = bal[7:].replace(',', '')
        self.avail_bal = float(bal)
        
        
        result = {
            "bank": "keystone",
            "tx_type": self.tx_type_short,
            "amount": self.amount,
            "currency": self.currency,
            "account_number": self.account_number,
            "datetime": self.datetime,
            "description": self.description,
            "avail_bal": self.avail_bal
        }
        
        return result


# In[480]:


class PolarisBankHandler(Handler):
    def __init__(self, request):
        super().__init__(request)
    
    def process(self):
        results = self.string_processor()
        if results:
            return results
        else:
            return self.empty
    
    def string_processor(self):
        tx, acc_no, amt, desc, bal, date_time, _ = self.message.split("\n")
        
        tx_type = tx.lower()
        self.tx_type_short = "dr" if tx_type == "debit" else "cr"
        
        self.account_number = acc_no[5:]
        
        amount = amt[5:].replace(',', '')
        self.amount = float(amount)
        self.currency = "NGN" if amt[4:5] == "N" else None
        
        self.description = desc[4:]
        
        bal = bal[5:].replace(',', '')
        self.avail_bal = float(bal)
        
        date_time = date_time
        self.datetime = datetime.strptime(date_time, '%d-%m-%Y%H:%M')
        
        
        result = {
            "bank": "polarisbank",
            "tx_type": self.tx_type_short,
            "amount": self.amount,
            "currency": self.currency,
            "account_number": self.account_number,
            "datetime": self.datetime,
            "description": self.description,
            "avail_bal": self.avail_bal
        }
        
        return result


# In[511]:


class SterlingBankHandler(Handler):
    """
    Examples:
    
    Money In Alert. \nNGN11,000.00 has just arrived from Paystack into **9032 at 7:15 AM 19-Apr-2022 \nYou now have: NGN ***."
    
    Money Out Alert!\nAcc#: **9032\nDesc: USSD/Airtime purchased of Amt 200.00 to Mobile 09126922679. RefID: 23\n09-May-2022 @ 5:32 PM\nAmt: NGN 200.00 \nAv.Bal: NGN ***"
    
    Debit Alert!\nAcc#: **9032\nDesc: USSD/Airtime purchased of Amt 200.00 to Mobile 09126308040. RefID: 221473\n22-Apr-2022 @ 7:52 PM\nAmt: NGN 200.00 \nAv.Bal: NGN ***"
    """
    def __init__(self, request):
        super().__init__(request)
    
    def process(self):
        results = self.string_processor()
        if results:
            return results
        else:
            return self.empty
    
    def string_processor(self):
        if self.message.startswith("Money"):
            if "Alert!" in self.message:
                tx, acc_no, desc, date_time, amt, bal, = self.message.split("\n")

                tx_type = tx.split(" ")[1].lower()
                self.tx_type_short = "dr" if tx_type == "out" else "cr"

                self.account_number = acc_no[6:]

                self.description = desc[6:]

                date_time = date_time
                self.datetime = datetime.strptime(date_time, '%d-%b-%Y @ %I:%M %p')

                amount = amt[9:].strip().replace(',', '')
                self.amount = float(amount)
                self.currency = "NGN" if amt[5:8] == "N" else None

                # 'Av.Bal: NGN ***.' is expected
                bal = bal[12:].replace(',', '')
                try:
                    self.avail_bal = float(bal)
                except:
                    self.avail_bal = None
            else:
                tx, detail, balance = self.message.split("\n")

                tx_type = tx.split(" ")[1].lower()
                self.tx_type_short = "dr" if tx_type == "out" else "cr"
                if tx_type == "in":
                    amt, _, _, _, *info = detail.strip().split(" ")
                    date_time = info[-3:]; info = info[:-3]
                    acc_no = info[-2]; info = info[:-3]
                    info = info

                    self.account_number = acc_no

                    amount = amt[3:].replace(',', '')
                    self.amount = float(amount)
                    self.currency = amt[:3]

                    self.description = " ".join(info)

                    date_time = "".join(date_time)
                    self.datetime = datetime.strptime(date_time, '%I:%M%p%d-%b-%Y')

                elif tx_type == "out":
                    _, _, _, amt, *info = detail.strip().split(" ")
                    date_time = info[-3:]; info = info[:-3]
                    acc_no = info[-2]; info = info[:-3]
                    info = info

                    self.account_number = acc_no

                    amount = amt[3:].replace(',', '')
                    self.amount = float(amount)
                    self.currency = amt[:3]

                    self.description = " ".join(info)

                    date_time = "".join(date_time)
                    self.datetime = datetime.strptime(date_time, '%I:%M%p%d-%b-%Y')

                # 'You now have NGN ***.' is expected
                *_, bal = balance.split(" ")
                bal = bal[:-1].replace(',', '')
                try:
                    self.avail_bal = float(bal)
                except:
                    self.avail_bal = None
        else:
            tx, acc_no, desc, date_time, amt, bal, = self.message.split("\n")
        
            tx_type = tx.split(" ")[0].lower()
            self.tx_type_short = "dr" if tx_type == "debit" else "cr"

            self.account_number = acc_no[6:]

            self.description = desc[6:]

            date_time = date_time
            self.datetime = datetime.strptime(date_time, '%d-%b-%Y @ %I:%M %p')

            amount = amt[9:].strip().replace(',', '')
            self.amount = float(amount)
            self.currency = "NGN" if amt[5:8] == "N" else None
            
            # 'Av.Bal: NGN ***.' is expected
            bal = bal[12:].replace(',', '')
            try:
                self.avail_bal = float(bal)
            except:
                self.avail_bal = None

        
        result = {
            "bank": "sterling",
            "tx_type": self.tx_type_short,
            "amount": self.amount,
            "currency": self.currency,
            "account_number": self.account_number,
            "datetime": self.datetime,
            "description": self.description,
            "avail_bal": self.avail_bal
        }
        
        return result


# In[482]:


class StandardCharteredHandler(Handler):
    def __init__(self, request):
        super().__init__(request)
    
    def process(self):
        results = self.string_processor()
        if results:
            return results
        else:
            return self.empty
    
    def string_processor(self):
        if not "Alert!" in self.message:
            return None
        
        tx_acc_no, amt, desc, date_time, bal = self.message.split(", ")
        
        tx, acc_no = tx_acc_no.split(" Alert! ")
        self.account_number = acc_no[5:]
        
        tx_type = tx.lower()
        self.tx_type_short = "dr" if tx_type == "debit" else "cr"
        
        amount = amt[7:].replace(',', '')
        self.amount = float(amount)
        self.currency = amt[4:7]
        
        self.description = desc[5:]
        
        date_time = date_time[5:]
        self.datetime = datetime.strptime(date_time, '%Y-%m-%d')
        
        bal = bal[7:].replace(',', '')
        self.avail_bal = float(bal)
        
        result = {
            "bank": "stanchart",
            "tx_type": self.tx_type_short,
            "amount": self.amount,
            "currency": self.currency,
            "account_number": self.account_number,
            "datetime": self.datetime,
            "description": self.description,
            "avail_bal": self.avail_bal
        }
        
        return result


# In[483]:


class UBAHandler(Handler):
    def __init__(self, request):
        super().__init__(request)
    
    def process(self):
        results = self.string_processor()
        if results:
            return results
        else:
            return self.empty
    
    def string_processor(self):
        if not self.message.startswith("Txn"):
            return None
        
        tx, acc_no, amt, desc, date_time, bal, _ = self.message.split("\n")
        
        tx_type = tx[5:].lower()
        self.tx_type_short = "dr" if tx_type == "debit" else "cr"
        
        self.account_number = acc_no[3:]
        
        amount = amt[8:].replace(',', '')
        self.amount = float(amount)
        self.currency = amt[4:7]
        
        self.description = desc[4:]
        
        date_time = date_time[5:]
        self.datetime = datetime.strptime(date_time, '%d-%b-%Y %H:%M')
        
        bal = bal[8:].replace(',', '')
        self.avail_bal = float(bal)
        
        
        result = {
            "bank": "uba",
            "tx_type": self.tx_type_short,
            "amount": self.amount,
            "currency": self.currency,
            "account_number": self.account_number,
            "datetime": self.datetime,
            "description": self.description,
            "avail_bal": self.avail_bal
        }
        
        return result


# In[484]:


class UnionBankHandler(Handler):
    def __init__(self, request):
        super().__init__(request)
    
    def process(self):
        results = self.string_processor()
        if results:
            return results
        else:
            return self.empty
    
    def string_processor(self):
        acc_no, amt, desc, date_time, bal = self.message.split("\n")
        
        self.account_number = acc_no[5:]
        
        amount = amt[7:].replace(',', '')
        self.amount = float(amount)
        self.currency = amt[4:7]
        
        tx_type = amt[:2].lower()
        self.tx_type_short = tx_type
        
        self.description = desc[6:]
        
        date_time = date_time[4:].title()
        self.datetime = datetime.strptime(date_time, '%d/%b/%Y %H:%M:%S')
        
        bal = bal[18:-2].replace(',', '')
        self.avail_bal = float(bal)
        
        
        result = {
            "bank": "unionbank",
            "tx_type": self.tx_type_short,
            "amount": self.amount,
            "currency": self.currency,
            "account_number": self.account_number,
            "datetime": self.datetime,
            "description": self.description,
            "avail_bal": self.avail_bal
        }
        
        return result


# In[507]:


text = "Acct: **5287\nDR: NGN5,500.00, COM+VAT:26.88\nDesc: MOBILE/UNION Transfer to Chisom Chisom Okpara - NA\nDT: 12/APR/2022 17:00:01\nAvailable Bal: NGN61,113.99CR"

text.split('\n')


# In[485]:


class WemaBankHandler(Handler):
    def __init__(self, request):
        super().__init__(request)
    
    def process(self):
        results = self.string_processor()
        if results:
            return results
        else:
            return self.empty
    
    def string_processor(self):
        tx, amt, acc_no, desc, bal, date_time = self.message.split("\r\n")
        
        tx_type = tx[5:].lower()
        self.tx_type_short = "dr" if tx_type == "debit" else "cr"
        
        amount = amt[3:].replace(',', '')
        self.amount = float(amount)
        self.currency = amt[:3]
        
        self.account_number = acc_no[8:]
        
        self.description = desc[7:]
        
        bal = bal[5:].replace(',', '')
        self.avail_bal = float(bal)
        
        date_time = date_time
        self.datetime = datetime.strptime(date_time, '%d-%m-%Y %H:%M')
        
        
        result = {
            "bank": "wemabank",
            "tx_type": self.tx_type_short,
            "amount": self.amount,
            "currency": self.currency,
            "account_number": self.account_number,
            "datetime": self.datetime,
            "description": self.description,
            "avail_bal": self.avail_bal
        }
        
        return result


# In[486]:


class ZenithBankHandler(Handler):
    def __init__(self, request):
        super().__init__(request)
    
    def process(self):
        results = self.string_processor()
        if results:
            return results
        else:
            return self.empty
    
    def string_processor(self):
        if not self.message.startswith("Acc"):
            return None
        
        acc_no, date_time, desc, tx_amt, bal, *_ = self.message.split("\n")
        
        self.account_number = acc_no[5:]
        
        date_time = date_time[3:]
        self.datetime = datetime.strptime(date_time, '%d/%m/%Y:%I:%M:%S%p')
        
        self.description = desc
        
        tx_type = tx_amt[2].lower()
        self.tx_type_short = tx_type
        
        amount = tx_amt[7:].replace(',', '')
        self.amount = float(amount)
        
        bal = bal[4:].replace(',', '')
        self.avail_bal = float(bal)
        
        result = {
            "bank": "zenithbank",
            "tx_type": self.tx_type_short,
            "amount": self.amount,
            "account_number": self.account_number,
            "datetime": self.datetime,
            "description": self.description,
            "avail_bal": self.avail_bal
        }
        
        return result


# In[487]:


handlers = {
    "FirstBank": FirstBankHandler,
    "AccessBank": AccessBankHandler,
    "GTBANK": GTBankHandler,
    "GTBank": GTBankHandler,
    "KEYSTONE": KeystoneBankHandler,
    "PolarisBank": PolarisBankHandler,
    "STERLING": SterlingBankHandler,
    "StanChart": StandardCharteredHandler,
    "UBA": UBAHandler,
    "UNIONBANK": UnionBankHandler,
    "WemaBank": WemaBankHandler,
    "ZENITHBANK": ZenithBankHandler
}


# In[299]:


def test(bank):
    for payload in messages[bank]:
        entry(payload)


# In[508]:


banks = [ 'zenithbank', 'wemabank', 'unionbank', 'uba',  'stanchart', 'sterling', 'polarisbank', 'keystone', 'gtbank', 'accessbank', 'firstbank']
results = {}
errors = []

def test_check():
    for bank in banks:
        print(f"processing {bank}")
        
        resp = r.get(f"https://worker-service-prod.herokuapp.com/transactions/test/processor?bank={bank}&limit=25")
        message_list = resp.json()
        
        parsed_list = []
        count = 1
        for payload in message_list:
            print(f"\t{count} of {len(message_list)}")
            
            try:
                parse_result = entry(payload)
                parsed_list.append(parse_result)
            except:
                print(f"error: {payload.get('message')[:40]}")
                e = sys.exc_info()[0]
                errors.append({
                    "bank": bank,
                    "error": e,
                    "input": payload
                })
            count += 1
        
        results[bank] = {
            "input": message_list,
            "output": parsed_list
        }

test_check()


# In[503]:


for bank in results:
    print(f"{bank}\n\t{len(results[bank]['output'])} of {len(results[bank]['input'])}")


# In[510]:


len(errors)


# In[509]:


errors


# In[ ]:




