import pandas as pd 
from .utils import read_sql
from skbox.connectors.bigquery import BigQuery
from skbox.connectors.teradata import Teradata
import os
import sys
import time
import logging as log

#PATH = ""
PATH = './data/'

SOURCE_DICT = { 
                'sales_11_14': PATH + 'load_sales.sql' ,
                'daily_sales': PATH + 'load_daily_sales.sql'
               }


class LoadSales():
    """
    """
    def __init__(self,data_source, option_source, year, week,store):
        if option_source == "teradata":
            self.data_source = data_source
            SALES = read_sql(SOURCE_DICT[self.data_source])
            SALES = SALES.replace('\n', '').replace('\r', '')
            sql_req = sql_req.replace('year', year).replace('week', week)
            teradata = Teradata()
            self.dataframe = teradata.select(SALES, chunksize=None)
        elif option_source == "bq":
            self.data_source = data_source
            sql_req = read_sql(SOURCE_DICT[self.data_source])
            sql_req = sql_req.replace('\n', ' ').replace('\r', ' ')
            sql_req = sql_req.replace('var_year', year).replace('var_week', week).replace('var_store',store)
            bq = BigQuery()
            df_BQ = bq.select(sql_req)
            self.dataframe = df_BQ
        else :
            print("wrong option")
