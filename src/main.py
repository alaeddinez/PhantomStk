import pandas as pd
from data import LoadSales
from datetime import datetime
import numpy as np
import os
from data import storage_blob
#executer le script mercredi pour récuperer toutes les ventes (jusqu'à mardi soir)
# loading data at 2020/02/11 (mardi)
date_execution = "20200212" # mercredi
store = '14'
# TODO : prev journaliere !!!  diviser pas 6 (vekia)
data = storage_blob(bucket='big-data-dev-supply-sages', blob='EXTRACTION_PV_' + store + '_'+ date_execution +'.csv').select_bucket(sep=";")
previous_monday_date = data.columns[3]
# on s'interesse aux semaine passée!  DU 2020/02/10   ==> 2020/02/17 pour matcher avec les ventes réelles
# cet intervalle correspond à la semaine 7 dans ce cas
#on recupere la prévision de cette semaine seulement
data = data.drop(["POS_ID", "ECART_TYPE"], axis=1)#56174895
data = pd.melt(data, id_vars=['RC_ID'])
data = data[data.variable == previous_monday_date]

def string_to_date(str_date):
    return(datetime.strptime(eval(str_date), '%d/%m/%Y'))

data.variable = data['variable'].apply(string_to_date)
# data["week_of_year"] = data["variable"].dt.week
# data["year_of_calendar"] = data["variable"].dt.year
data.rename(columns={'RC_ID': 'NUM_ART'}, inplace=True)
list_art_vekia = np.unique(data.NUM_ART)
# ##### real sales ###########
# TODO: un appel par magasin
SALES_df = LoadSales('daily_sales', option_source="bq", year = '2020',  week = '5',store = '14').dataframe
# TODO: sort by year and week to detect the recent !

# left join the vekia prev with actual values
merged = data.merge(SALES_df[["NUM_ART", "week_of_year", "year_of_calendar", "QTE_VTE"]], on=["NUM_ART"], how='left')
merged = merged.dropna()
merged.QTE_VTE = merged.QTE_VTE.astype("float")
# Load qté chantier

# merge
merged["coef"] = merged.value - merged.QTE_VTE
# TODO : utiliser les données des inventaires
df_counts = merged.groupby(['NUM_ART']).size().reset_index(name='counts')
df_counts.sort  
ex = merged[merged.NUM_ART == 953235]
