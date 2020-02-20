import pandas as pd
from data import LoadSales, LoadCPQ
from data import LoadInvent, LoadStkMag
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
SALES_df = LoadSales('day_sales', option_source="bq",store = store ,date =date_execution).dataframe
def score_cum_day(date_execution, store):
    store = store
    # ##### real sales ###########
    
    # ##### vekia prev ###########
    data = storage_blob(bucket='big-data-dev-supply-sages', blob='EXTRACTION_PV_' + store + '_'+ ''.join(e for e in date_execution if e.isalnum()) +'.csv').select_bucket(sep=";")
    data = prep_vekia(data, date_execution)
    data_score_days = calcul_score(data, SALES_df, CPQ_df)
    #sort by ref/day
    data_score_days = data_score_days.sort_values(by=['NUM_ART', 'variable'])
    #############################################################
    #           pousser les alertes
    #############################################################
    data_score_days["flag_alerte"] = 1
    # TODO : utiliser les données des inventaires
    # TODO : recuperer le flag si y avait inventaire ou pas le jour j - 1 ou j ?
    inv_data = LoadInvent('inventory', date=date_execution, store=store).dataframe
    merged = data_score_days.merge(inv_data, on=["NUM_ART"], how='left')
    # TODO : utiliser les données stock magasin
    stk_data = LoadInvent('stk_mag', store=store, date=date_execution).dataframe
    merged = merged.merge(stk_data, on=["NUM_ART"], how='left')
    # ne pas pousser l'alerte lorsque le stock mag est dejà null le jour j
    merged.flag_alerte[merged.flag_stk.notnull()] = 0
    # TODO: ne pas pousser l'alerte lorsque un inventaire a été déjà fait le jour j ou j-1 ?
    # TODO : le score_cum tombe à 0 si un inventaire a déjà été fait !
    merged.flag_alerte[merged.flag_inv.notnull()] = 0
    merged.score_cum[merged.flag_inv.notnull()] = 0
    return(merged)
# ##############################################
#                      main
# ##############################################
day_before = score_cum_day("2020-01-09","14")
day_before = day_before[["NUM_ART","score_cum","flag_inv"]]
day_before.rename(columns={'score_cum': 'score_cum_before'}, inplace=True)
day_before.rename(columns={'flag_inv': 'flag_inv_before'}, inplace=True)

day_today = score_cum_day("2020-01-10")
day_today = day_today.merge(day_before, on=["NUM_ART"], how='left')
day_today["score_cum"] = day_today["score_cum"] + day_today["score_cum_before"]
