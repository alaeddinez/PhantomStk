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
                'sales_11_14': PATH + 'load_sales.sql',
                'daily_sales': PATH + 'load_daily_sales.sql',
                'day_sales': PATH + 'load_sales_day.sql',
                'cpq': PATH + 'load_cpq_temp.sql'
               }


class LoadSales():
    """
    """
    def __init__(self,data_source, option_source, date, store):
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
            sql_req = sql_req.replace('var_date', date).replace('var_store', store)
            bq = BigQuery()
            df_BQ = bq.select(sql_req)
            self.dataframe = df_BQ
        else :
            print("wrong option")

class LoadCPQ():
    """
    """
    def __init__(self,data_source):
            self.data_source = data_source
            sql_req = read_sql(SOURCE_DICT[self.data_source])
            sql_req = sql_req.replace('\n', ' ').replace('\r', ' ')
            bq = BigQuery()
            df_BQ = bq.select(sql_req)
            self.dataframe = df_BQ
