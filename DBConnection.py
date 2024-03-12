import psycopg2
import sys
import pandas as pd
import psycopg2.extras as extras
import os
from dotenv import load_dotenv
import pathlib
import logging
from datetime import datetime

class ConnectDatabase:
   def __init__(self):
      
      self.TABLE_NAME=os.getenv("DB_tablename")
      self.lstData=[]
      self.page_size=100
      self.conn=None
      self.path = pathlib.Path(__file__).parent.resolve()
      self.LOG_FILENAME = datetime.now().strftime(str(self.path)+ '/logfile_%Y_%m_%d_%H_%M_%S.log')

   def connectDB(self):
      # load_dotenv()
      #conn = None
      try:
         
         self.conn = psycopg2.connect(
               host=os.getenv("DB_host"),
               database=os.getenv("DB_name"),
               user=os.getenv("DB_user"),
               password=os.getenv("DB_psw"),
               port=os.getenv("DB_port")
               )
         cur = self.conn.cursor()

         table_sql = (f"""CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
            id varchar(7) NOT NULL,
            "Date" date NULL,
            region varchar NULL,
            city varchar NULL,
            category varchar NULL,
            product varchar NULL,
            qty int4 NULL,
            unitprice float8 NULL,
            totalprice float8 NULL
         );
         CREATE UNIQUE INDEX IF NOT EXISTS {self.TABLE_NAME}_id_idx ON {self.TABLE_NAME} USING btree (id);
         """)
         cur.execute(table_sql)
         self.conn.commit()
         cur.close()

      except (Exception, psycopg2.DatabaseError) as error:
         logging.basicConfig(format='%(asctime)s %(message)s',filename=self.LOG_FILENAME)
         logging.exception('')
         sys.exit(1)
      
      return self.conn
      

 
   def execute_batch(self, df):
      self.conn=self.connectDB()
      # Create a list of tupples from the dataframe values
      tuples = [tuple(x) for x in df.to_numpy()]
      # Comma-separated dataframe columns
      cols = '","'.join(list(df.columns))
      cols='"'+cols+'"'
      # SQL quert to execute
      query  = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s)" % (self.TABLE_NAME, cols)
      cursor = self.conn.cursor()

      try:
            extras.execute_batch(cursor, query, tuples,self.page_size)
            self.conn.commit()
      except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            self.conn.rollback()
            cursor.close()
            return 1

      cursor.close()


   def execute_sql(self,sql):
   #Setting auto commit false
      self.conn=self.connectDB()
      self.conn.autocommit = True

      #Creating a cursor object using the cursor() method
      cursor = self.conn.cursor()
      #Updating the records
      cursor.execute(sql)
      self.conn.commit()
      self.conn.close()