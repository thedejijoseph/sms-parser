#!/usr/bin/env python
# coding: utf-8

from datetime import datetime


def parse(payload):
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
    assert type(payload) == dict, f"payload: type {type(payload)} is not dict"

    selected_handler = handlers.get(payload['sender'], None)
    if selected_handler:
        handler = selected_handler(payload)
        return handler.process()
    else:
        return {
            "error": f"no handler for {payload['sender']}"
        }


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
    def process(self):
        results = self.string_processor()
        if results:
            # change datetime to json-serializable format
            results['datetime'] = results['datetime'].isoformat()
            return results
        else:
            return self.empty

class FirstBankHandler(Handler):
    """Parse First Bank SMS
    """

    def __init__(self, request):
        super().__init__(request)
    
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

class AccessBankHandler(Handler):
    """Parse Access Bank SMS
    """

    def __init__(self, request):
        super().__init__(request)
    
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

class GTBankHandler(Handler):
    """Parse GT Bank SMS
    """

    def __init__(self, request):
        super().__init__(request)
    
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

class KeystoneBankHandler(Handler):
    """Parse Keystone Bank SMS
    """

    def __init__(self, request):
        super().__init__(request)
    
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

class PolarisBankHandler(Handler):
    """Parse Polaris Bank SMS
    """

    def __init__(self, request):
        super().__init__(request)
    
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

class SterlingBankHandler(Handler):
    """Parse Sterling Bank SMS
    """
    def __init__(self, request):
        super().__init__(request)
    
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
                self.currency = amt[5:8]

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

class StandardCharteredHandler(Handler):
    """Parse StandardChartered Bank SMS
    """

    def __init__(self, request):
        super().__init__(request)
    
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

class UBAHandler(Handler):
    """Parse UBA SMS
    """

    def __init__(self, request):
        super().__init__(request)
    
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

class UnionBankHandler(Handler):
    """Parse Union Bank SMS
    """

    def __init__(self, request):
        super().__init__(request)
    
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

class WemaBankHandler(Handler):
    """Parse Wema Bank SMS
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

class ZenithBankHandler(Handler):
    """Parse Zenith Bank SMS
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
