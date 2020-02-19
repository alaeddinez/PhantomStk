import pandas as pd
from data import LoadSales, LoadCPQ
#from data import LoadInvent, LoadStkMag
from datetime import datetime
import numpy as np
import os
from data import storage_blob
from utils import *
import datetime
# ##############################################################
#           calcul de score par mag/jour
# ##############################################################
# TODO :adding CPQ INFO
CPQ_df = LoadCPQ('cpq').dataframe
store = '14' 
start = datetime.datetime.strptime("2020-01-01", "%Y-%m-%d")
end = datetime.datetime.strptime("2020-01-10", "%Y-%m-%d")
date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end- start).days)]
list_date = list()
for date in date_generated:
    li = date.strftime("%Y-%m-%d")
    list_date.append(li)
data_score_days = pd.DataFrame()

for date_execution in list_date:
    print(date_execution)
    # ##### real sales ###########
    SALES_df = LoadSales('day_sales', option_source="bq",store = store ,date =date_execution).dataframe
    # ##### vekia prev ###########
    data = storage_blob(bucket='big-data-dev-supply-sages', blob='EXTRACTION_PV_' + store + '_'+ ''.join(e for e in date_execution if e.isalnum()) +'.csv').select_bucket(sep=";")
    data = prep_vekia(data, date_execution)
    data_score = calcul_score(data, SALES_df, CPQ_df)
    data_score_days = data_score_days.append(data_score)
    #sort by ref/day
    data_score_days = data_score_days.sort_values(by=['NUM_ART', 'variable'])

#############################################################
#           pousser les alertes
#############################################################
data_score_days["flag"] = 1
# TODO : utiliser les données des inventaires
# TODO : recuperer le flag si y avait inventaire ou pas le jour j - 1 ou j ?
inv_data = LoadInvent('inventory', store='14', date='2020-01-09').dataframe
merged = data_score_days.merge(inv_data, on=["NUM_ART"], how='left')
# TODO : utiliser les données stock magasin
stk_data = LoadInvent('stk_mag', store='14', date='2020-01-09').dataframe
merged = data_score_days.merge(stk_data, on=["NUM_ART"], how='left')

