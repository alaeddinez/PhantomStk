import pandas as pd
from data import LoadSales, LoadCPQ, LoadInvent3Weeks, LoadStkMagCPQ
from data import LoadInvent, LoadStkMag, LoadVekiaSuspect
from datetime import datetime
import numpy as np
import os
from data import storage_blob
from utils import *
import datetime
# TODO : intiate the loop with the first csv file (in this case 01/01/2020)
# loading CPQ
# TODO : cpq table is pointing on a temporal table(access is forbidden on origin table)
CPQ_df = LoadCPQ('cpq').dataframe
stores = ['3']
date_execution = "2020-02-16"
for store in stores:
    print(store)
    # ##### real sales ###########
    SALES_df = LoadSales('day_sales', option_source="bq", store=store,
                         date=date_execution).dataframe
    # ##### vekia prev ###########
    data = storage_blob(bucket='big-data-dev-supply-sages',
                        blob='EXTRACTION_PV_' + store + '_'
                        + ''.join(e for e in date_execution if e.isalnum())
                        + '.csv').select_bucket(sep=";")
    # #### inventory data ########
    inv_data = LoadInvent('inventory', date=date_execution,
                          store=store).dataframe
    # #### stk mag data ##########
    stk_data = LoadInvent('stk_mag', store=store,
                          date=date_execution).dataframe
    #
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
    # print day_before
    day_before = day_before[["NUM_ART", "score_cum", "flag_inv"]]
    day_before.rename(columns={'score_cum': 'score_cum_before'}, inplace=True)
    day_before.rename(columns={'flag_inv': 'flag_inv_before'}, inplace=True)
    # print day_today
    day_today = day_today.merge(day_before, on=["NUM_ART"], how='left')
    day_today.score_cum_before = day_today.score_cum_before.fillna(0)
    day_today["score_cum"] = day_today["score_cum"] + day_today["score_cum_before"]
    # load inv 3 weeks
    date_3weeks = datetime.datetime.strptime(date_execution, "%Y-%m-%d")
    date_3weeks = date_3weeks - datetime.timedelta(days=22)
    date_3weeks = date_3weeks.strftime("%Y-%m-%d")
    inv_3weeks = LoadInvent3Weeks('inv_3weeks', store=store,
                                  date_1=date_3weeks,
                                  date_2=date_execution).dataframe
    #TODO : ajouter une colonne flag 1 week ! (conseil greg)
    day_today = day_today.merge(inv_3weeks, on=["NUM_ART"], how='left')
    data_stk_cpq = LoadStkMagCPQ('stk_mag_cpq', store=store,
                                 date_1=date_execution_lag,
                                 date_2=date_execution, prop='2').dataframe
    day_today = day_today.merge(data_stk_cpq, on=["NUM_ART"], how='left')
    #
    day_today.flag_alerte[day_today.flag_inv_3weeks.notnull()] = 0
    day_today.flag_alerte[day_today.flag_stk_cpq.notnull()] = 0
    #
    day_today.to_csv("../output/score_" +
                     ''.join(e for e in date_execution if e.isalnum()) +
                     ".csv",
                     sep=";")
    # keep only suspicious sku
    susp_data = day_today
    susp_data = susp_data[susp_data.score_cum > 0]
    susp_data = susp_data[susp_data.flag_alerte == 1]
    susp_data = susp_data.sort_values(by=["score_cum"])
    # loading vekia suspect score to compare
    vekia_sus = LoadVekiaSuspect('vekia_sus', store=store,
                                 date=date_execution).dataframe
    # matcher avec les ref toped vekia
    vekia_sus["flag_match_vekia"] = 1
    susp_data = susp_data.merge(vekia_sus, on=["NUM_ART"], how='left')
    susp_data.to_csv("../output/suspect_example.csv", sep=";")
