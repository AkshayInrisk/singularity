import pandas as pd
import numpy as np
from math import inf
from datetime import datetime
import warnings
warnings.filterwarnings('ignore', category=RuntimeWarning)
import Cover_Definitions as cover_definitions

def backtest(RU_DF, Term_Sheet, xval):
    
    # Term_Sheet = risk_defn

    phase_len = 1
    Term_Sheet['Risk_Unit'] = [f'RU{i}' for i in range(1, len(Term_Sheet) + 1)]
    term_df = pd.DataFrame(columns=["year","parameter","Backtest_Payout","Cover","Data_Source"])

    Cover = Term_Sheet['Cover'][0]
    Data_Source = Term_Sheet['Data_Source'][0]

    if 'PCT' in Cover: 
        Strikes = []
    else:
        Strikes = Term_Sheet['Strikes'][0]
    
    Payouts = Term_Sheet['Payouts'][0]
    DIST_BC = Term_Sheet['DIST_BC'][0]
    Risk_Unit = Term_Sheet['Risk_Unit'][0]
    
    cover_type = Cover.split('-')[1]  
    val = next(map(lambda item: item[1], filter(lambda item: item[0] == Cover, xval)), None)


    cover_data, max_criteria, cover_name = getattr(cover_definitions, cover_type)(RU_DF, val)

    gt_type = Cover.split('-')[0]  
    conditions = []
    choices = []

    if 'PCT' in Cover:
        cover_data['Backtest_Payout'] = round(cover_data['parameter'] * Payouts, 2)

    elif 'VAR' in Cover:
        deductible = Strikes[0]
        num_levels = Strikes[1]
        level_size = Strikes[2] if len(Strikes) >= 3 else 1
        base_pay = Payouts[0]
        per_level_pay = Payouts[1]
        discrete_payout = Payouts[2]
        if gt_type=='LTE':
            if discrete_payout == 1:
                cover_data['Backtest_Payout'] = cover_data.apply(
                    lambda row: 0 if row['parameter'] > deductible else base_pay + (min(int((deductible - row['parameter'])/level_size), num_levels) * per_level_pay),
                    axis=1
                )
            elif discrete_payout == 0:
                cover_data['Backtest_Payout'] = cover_data.apply(
                    lambda row: 0 if row['parameter'] > deductible else base_pay + (min(float((deductible - row['parameter'])/level_size), num_levels) * per_level_pay),
                    axis=1
                )
        elif gt_type=='GTE':
            if discrete_payout == 1:
                cover_data['Backtest_Payout'] = cover_data.apply(
                    lambda row: 0 if row['parameter'] < deductible else base_pay + (min(int((row['parameter'] - deductible)/level_size), num_levels) * per_level_pay),
                    axis=1
                )
            elif discrete_payout == 0:
                cover_data['Backtest_Payout'] = cover_data.apply(
                    lambda row: 0 if row['parameter'] < deductible else base_pay + (min(float((row['parameter'] - deductible)/level_size), num_levels) * per_level_pay),
                    axis=1
                )
        
    else:
        for i in range(len(Strikes)):
            if i < len(Strikes) - 1:
                if gt_type == 'GTE':
                    condition = (cover_data['parameter'] >=Strikes[i]) & (cover_data['parameter'] <Strikes[i+1])
                    payout_actual = Payouts[i]
                else:
                    condition = (cover_data['parameter'] <=Strikes[i]) & (cover_data['parameter'] >Strikes[i+1])
                    payout_actual = Payouts[i]
            else:
                if gt_type == 'GTE':
                    condition = (cover_data['parameter'] >= Strikes[i])
                    payout_actual = Payouts[i]
                else:
                    condition = (cover_data['parameter'] <= Strikes[i])
                    payout_actual = Payouts[i]
    
            conditions.append(condition)
            choices.append(payout_actual)
        cover_data['Backtest_Payout'] = np.select(conditions, choices, default=0)

    cover_data['Cover'] = Cover
    cover_data['Data_Source'] = Data_Source
    cover_data['Risk_Unit'] = Risk_Unit
    cover_data_filtered = cover_data.dropna(axis=1, how='all')

    term_df = cover_data_filtered
        
    vintage = term_df['year'].nunique()
    term_df = term_df[term_df['Backtest_Payout']>=0]
    summary = pd.DataFrame(term_df.groupby('Risk_Unit')['Backtest_Payout'].sum()/vintage)
    summary.reset_index(inplace=True)
    summary = summary.merge(Term_Sheet[['Risk_Unit', 'DIST_BC', 'DIST_RP']], on='Risk_Unit', how='left')

    total_payout = term_df[term_df['Backtest_Payout'] > 0]
    total_payout = total_payout.groupby(['year'])['Backtest_Payout'].sum().reset_index()
    total_payout = total_payout.sort_values(by='year', ascending=False)

    return term_df, total_payout, summary