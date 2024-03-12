import pandas as pd
from DBConnection import *

class SaveData_DB:
    def __init__(self):
        
        self.objDB=ConnectDatabase()
        self.colNm=['id', 'Date','region', 'city','category','product','qty','unitprice','totalprice']
        self.dfDB=pd.DataFrame()

    def save_data(self):
        self.dfDB.columns=self.colNm
        self.objDB.execute_batch(self.dfDB)
        