import pandas as pd
import numpy as np



def amplif_coeff(var):
    """[summary]
    
    Arguments:
        var {[type]} -- [description]
    """
    if var < 0:
        var = var * 5
    else:
        var
    return(var)

def string_to_date(str_date):
    """[summary]
    
    Arguments:
        str_date {[type]} -- [description]
    """
    return(datetime.strptime(eval(str_date), '%d/%m/%Y'))

def prep_vekia(df, date_execution):
    previous_monday_date = df.columns[3]
    df = df.drop(["POS_ID", "ECART_TYPE"], axis=1)
    df = pd.melt(df, id_vars=['RC_ID'])
    df = df[df.variable == previous_monday_date]
    df.variable = date_execution
    df.rename(columns={'RC_ID': 'NUM_ART'}, inplace=True)
    # estimate daily sales from weekly sales
    df.value = df.value/6
    return(df)

def calcul_score(df_vekia, df_sales, cpq_df):
    # left join the vekia prev with actual values
    merged = df_vekia.merge(df_sales[["NUM_ART", "QTE_VTE"]], on=["NUM_ART"], how='left')
    merged.QTE_VTE = merged.QTE_VTE.fillna('0')
    merged.QTE_VTE = merged.QTE_VTE.astype("float")
    merged = merged.merge(cpq_df, on=["NUM_ART"], how='left')
    merged.Standard_CPQ = merged.Standard_CPQ.fillna('1')
    merged.Standard_CPQ = merged.Standard_CPQ.astype("float")
    merged["score"] = (merged.value - merged.QTE_VTE) / (merged.Standard_CPQ)
    # TODO :définir une régle pour amplifier le score lorqu'il est négatif
    merged.score = merged['score'].apply(amplif_coeff)
    merged["score_cum"] = merged.score
    return merged
