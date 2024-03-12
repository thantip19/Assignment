from DBConnection import *
from datetime import datetime
from dotenv import load_dotenv
import logging
import pathlib
from read_source_file import *
from keep_to_DB import *

def main():
   
    path = pathlib.Path(__file__).parent.resolve()
    LOG_FILENAME = datetime.now().strftime(str(path)+ '/logfile_%Y_%m_%d_%H_%M_%S.log')
    
    try:
        print(f"running main method ")
        load_dotenv()
        
        readSrc= ReadSourceFile()
        dfDB= readSrc.read_source_excel()

        objKeep=SaveData_DB()
        objKeep.dfDB=dfDB
        objKeep.save_data()
        
        print(f"done")
    except:
        logging.basicConfig(format='%(asctime)s %(message)s',filename=LOG_FILENAME)
        logging.exception('')

   
if __name__ == "__main__":
    main()