import pandas as pd
import pathlib 

import os

class ReadSourceFile:
    def __init__(self):
        self.dfExcel=pd.DataFrame()
        self.curPath = pathlib.Path(__file__).parent.resolve()
        self.file_name=os.getenv("file_name")
        self.fullFileNm= pathlib.Path.joinpath(self.curPath,self.file_name)

    def read_source_excel(self):
        
        self.dfExcel= pd.read_excel(self.fullFileNm, sheet_name=os.getenv("sheet_name"),
                                    skiprows=[0,124,125,126])
        if not self.dfExcel.empty:
            self.dfExcel.fillna('',inplace=True)
        return self.dfExcel