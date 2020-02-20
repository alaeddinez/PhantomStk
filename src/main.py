import pandas as pd
from data import LoadSales, LoadCPQ
from data import LoadInvent, LoadStkMag
from datetime import datetime
import numpy as np
import os
from data import storage_blob
from utils import *
import datetime
# TODO : intiate the loop with the first csv file (in this case 01/01/2020)
# loading CPQ
CPQ_df = LoadCPQ('cpq').dataframe
store = '14'
date_execution = "2020-01-03"
# ##### real sales ###########
SALES_df = LoadSales('day_sales', option_source="bq", store=store,
                     date=date_execution).dataframe
# ##### vekia prev ###########
data = storage_blob(bucket='big-data-dev-supply-sages',
                    blob='EXTRACTION_PV_' + store + '_'
                    + ''.join(e for e in date_execution if e.isalnum())
                    + '.csv').select_bucket(sep=";")
# #### inventory data ########
inv_data = LoadInvent('inventory', date=date_execution, store=store).dataframe
# #### stk mag data ##########
stk_data = LoadInvent('stk_mag', store=store, date=date_execution).dataframe

day_today = score_cum_day(real_sales=SALES_df, prev_sales=data,
                          cpq_table=CPQ_df, inv_table=inv_data,
                          stk_table=stk_data, date=date_execution,
                          store=store)


#TODO : get the day before automatically : j -1 of the date execution
#TODO the day_before DF should be loaded from a dataset in BQ per example and not calculated
#TODO for the moment is writed in the VM
date_execution_lag = datetime.datetime.strptime(date_execution, "%Y-%m-%d")
date_execution_lag = date_execution_lag - datetime.timedelta(days=1)
date_execution_lag = date_execution_lag.strftime("%Y-%m-%d")

try:
    day_before = pd.read_csv("../output/score_" +
                             ''.join(e for e in date_execution_lag if e.isalnum())
                             + ".csv", sep=";")
except:
    print("the corresponding file doesnt exist ==> initiating")
    day_before = pd.DataFrame(columns=day_today.columns.values)
    
day_before = day_before[["NUM_ART", "score_cum", "flag_inv"]]
day_before.rename(columns={'score_cum': 'score_cum_before'}, inplace=True)
day_before.rename(columns={'flag_inv': 'flag_inv_before'}, inplace=True)

day_today = day_today.merge(day_before, on=["NUM_ART"], how='left')
#TODO : create condition if score_cum_before is Nan
if day_today["score_cum_before"].notnull():
    day_today["score_cum"] = day_today["score_cum"] + day_today["score_cum_before"]
else:
    print("no score added")

day_today.to_csv("../output/score_" +
                 ''.join(e for e in date_execution if e.isalnum()) + ".csv",
                 sep=";")
