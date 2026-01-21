import pandas as pd
import numpy as np
from math import inf
import math
from datetime import datetime
import Cover_Definitions as cover_definitions
import scipy.stats as stats
import warnings
from Cover_Definitions import rf_ds_names, temp_ds_names, rh_ds_names, ws_ds_names, ss_ds_names, sm_ds_names

warnings.filterwarnings('ignore', category=RuntimeWarning)

def cover_defn(RU_DF, risk_defn, cover_wt, TLR, xval, bin_multipliers, strat_wt, min_return_period, cover_threshold_min, cover_threshold_max):
    phase_len = (pd.to_datetime(RU_DF[RU_DF["year"] == 2024]["time"]).max() - pd.to_datetime(RU_DF[RU_DF["year"] == 2024]["time"]).min()).days + 1
    subset_df = cover_wt.copy()
    SI = risk_defn.loc[0, 'Sum_Insured']
    PR = risk_defn.loc[0, 'Premium_Rate']

    subset_df.loc[:, 'RU_SI'] = subset_df['SI_Wt'] * SI
    subset_df.loc[:, 'BC'] = subset_df['Risk_Wt'] * SI * PR * TLR
    subset_df = subset_df.sort_values(by='SI_Wt', ascending=False)
    subset_df.loc[:, 'Rev_BC'] = subset_df['BC']*SI*(1/min_return_period)/subset_df['BC'].iloc[0]
    subset_df['Rev_BC'] = subset_df[['Rev_BC', 'BC']].max(axis=1)
    subset_df = subset_df.sort_values(by='Priority', ascending=True)
    subset_df.loc[:, 'cum_bc'] = subset_df['Rev_BC'].cumsum()
    max_bc = subset_df['BC'].sum()
    subset_df_2 = subset_df[subset_df['cum_bc'] <= max_bc]
    if subset_df_2.empty:
        subset_df = subset_df.iloc[:1]
    else:
        subset_df = subset_df_2
    subset_df.drop(columns=['cum_bc','BC'], inplace=True)
    subset_df.loc[:, 'Rev_BC'] = (max_bc/subset_df.loc[:, 'Rev_BC'].sum())* subset_df.loc[:, 'Rev_BC']
    subset_df.loc[:, 'Freq'] = subset_df['Rev_BC'] / subset_df['RU_SI']
    subset_df = subset_df.sort_values(by=['Cover','N_Strikes'], ascending=[True,False])
    subset_df['Cum_Freq'] = subset_df.groupby(['Cover'])['Freq'].cumsum()
    subset_df.drop(columns=['Freq'], inplace=True)
    subset_df = subset_df.rename(columns={'Cum_Freq': 'Freq'})

    term_df = pd.DataFrame(columns=["Cover", "Data_Source", "Strikes",  "Payouts", "Frequency","DIST_BC", "DIST_RP"])
    total_frequency = 0

    working_df = subset_df
    Payout = working_df['RU_SI'].tolist()
    Payout.sort()
    BC = working_df['Rev_BC'].sum()
    Frequency = working_df['Freq'].tolist()
    Frequency.sort(reverse=True)
    Priority = min(working_df['Priority'].tolist())
    filtered_DF = RU_DF.copy()
    cover = working_df['Cover'].unique()[0]
    Data_Source = working_df['Data_Source'].unique()[0]
    cover_type = cover.split('-')[1]
    val = str(next(map(lambda item: item[1], filter(lambda item: item[0] == cover, xval)), None))

    cover_data, max_criteria, cover_name = getattr(cover_definitions, cover_type)(filtered_DF, val)
    
    if max_criteria == "phase_len":
        max_limit = phase_len
    else:
        max_limit = inf
    
    cover_data = Import_ECDF(cover_data, max_limit, Priority, bin_multipliers, Data_Source, strat_wt)

    gt_type = cover.split('-')[0]


    if gt_type == 'GTE':
        strikes = tuple(min(max(float(np.interp(1 - freq, cover_data['ecdf'], cover_data['parameter'])), cover_threshold_min), 
                            cover_threshold_max) for freq in Frequency)
    elif gt_type == 'LTE':
        strikes = tuple(min(max(float(np.interp(freq, cover_data['ecdf'], cover_data['parameter'])), cover_threshold_min), cover_threshold_max) 
                        for freq in Frequency)

    if "Number" not in cover_name:
        if ((Data_Source.split('_')[0] + '_' + Data_Source.split('_')[1]) in ws_ds_names) or ((Data_Source.split('_')[0] 
                    + '_' + Data_Source.split('_')[1]) in ss_ds_names) or ((Data_Source.split('_')[0] 
                    + '_' + Data_Source.split('_')[1]) in rh_ds_names):
            strikes = tuple(round(x, 0) for x in strikes)
        elif (Data_Source.split('_')[0] + '_' + Data_Source.split('_')[1]) in rf_ds_names:
            if gt_type == 'GTE':
                strikes = tuple(math.ceil(x / 5) * 5 for x in strikes)
            elif gt_type == 'LTE':
                strikes = tuple(math.floor(x) for x in strikes)
        else:
            strikes = tuple(round(x, 1) for x in strikes)
    else:
        if gt_type == 'GTE':
            strikes = tuple(math.ceil(x) if x != 0 else 1 for x in strikes)
        elif gt_type == 'LTE':
            strikes = tuple(math.floor(x) if x > 1 else 1 for x in strikes)
    if gt_type == 'GTE':
        strikes = sorted(strikes)
    elif gt_type == 'LTE':
        strikes = sorted(strikes, reverse=True)


    freq_values = tuple(float(np.interp(strike, cover_data['parameter'], cover_data['ecdf'])) for strike in strikes)
    freq_values = pd.Series(freq_values)
    
    if gt_type == 'GTE': 
        freq_values = 1- freq_values

        differences = freq_values.diff(-1)
        differences.iloc[-1] = freq_values.iloc[-1]
        freq_values = tuple(differences.values)  


    RP = tuple(freq * payout for freq, payout in zip(freq_values, Payout))
    RP = sum(RP)

    total_frequency = max(Frequency)
    data = {"Cover": cover, "Data_Source": Data_Source, "Strikes": strikes, "Payouts": Payout, 
            "Frequency": total_frequency, "DIST_BC":BC,"DIST_RP":RP}
    term_df.loc[len(term_df)] = data
    
    return term_df, cover_name




def cover_rev_defn(RU_DF, risk_defn, xval, bin_multipliers, strat_wt, payout_type = "Single"):
    cover_data_list = []  # Create an empty list to store cover data
    term_df = pd.DataFrame(columns=["Phase","Dates", "Cover", "Strikes", "Payout"])

    dfs = [] 

    Phase = risk_defn['Phase'][0]
    cover = risk_defn['Cover'][0]
    cover_type = cover.split('-')[1]  
    data_source = risk_defn['Data_Source'][0]

    Strikes = risk_defn['Strikes'][0]
    Payouts =  risk_defn['Payouts'][0]

    filtered_DF = RU_DF.copy()
    val = next(map(lambda item: item[1], filter(lambda item: item[0] == cover, xval)), None)
    cover_data, max_criteria, cover_name = getattr(cover_definitions, cover_type)(filtered_DF, val)

    ecdf_data = Import_ECDF(cover_data, inf, 1, bin_multipliers, data_source, strat_wt)

    gt_type = cover.split('-')[0]

  
    if 'VAR' in cover:
        deductible = Strikes[0]
        num_levels = int(Strikes[1])
        level_size = Strikes[2] if len(Strikes) >= 3 else 1
        base_pay = Payouts[0]
        per_level_pay = Payouts[1]
        discrete_payout = Payouts[2]

        rp_data = ecdf_data.copy()

        if deductible < rp_data['parameter'].iloc[0] or deductible > rp_data['parameter'].iloc[-1]:
            pass
        else:
            for level in range(num_levels+1):
                level_threshold = deductible + level * level_size
                ecdf_value = np.interp(level_threshold, ecdf_data['parameter'], ecdf_data['ecdf'])
                
                rp_data.loc[len(rp_data)] = {
                    'parameter': level_threshold,
                    'ecdf': ecdf_value
                }
        rp_data = rp_data.sort_values(by=['parameter', 'ecdf'])

        rp_data['ecdf_diff'] = rp_data['ecdf'].diff().fillna(rp_data['ecdf'])
        
        if gt_type=='LTE':
            if discrete_payout == 1:
                rp_data['Payout'] = rp_data.apply(
                    lambda row: 0 if row['parameter'] > deductible else base_pay + (min(int((deductible - row['parameter'])/level_size), num_levels) * per_level_pay),
                    axis=1
                )
            elif discrete_payout == 0:
                rp_data['Payout'] = rp_data.apply(
                    lambda row: 0 if row['parameter'] > deductible else base_pay + (min(float((deductible - row['parameter'])/level_size), num_levels) * per_level_pay),
                    axis=1
                )
        elif gt_type=='GTE':
            if discrete_payout == 1:
                rp_data['Payout'] = rp_data.apply(
                    lambda row: 0 if row['parameter'] < deductible else base_pay + (min(int((row['parameter'] - deductible)/level_size), num_levels) * per_level_pay),
                    axis=1
                )
            if discrete_payout == 0:
                rp_data['Payout'] = rp_data.apply(
                    lambda row: 0 if row['parameter'] < deductible else base_pay + (min(float((row['parameter'] - deductible)/level_size), num_levels) * per_level_pay),
                    axis=1
                )
        
        rp_data['RP'] =  rp_data['Payout'] * rp_data['ecdf_diff']
        Payout_Freq = (rp_data['Payout'] > 0).sum() / len(rp_data)
        
        df = pd.DataFrame([
            (Phase, cover, sum(rp_data['RP']))
            ], columns = ["Phase", "Cover", "Total_RP"]
        )

    else:
        if gt_type=='LTE':
            ecdf_values = tuple(interpolate_ecdf(strike, ecdf_data) for strike in Strikes)
        elif gt_type=='GTE':
            ecdf_values = tuple(1 - interpolate_ecdf(strike, ecdf_data) for strike in Strikes)

        Payout_Freq = max(ecdf_values)

        ecdf_values = pd.Series(ecdf_values)
        differences = ecdf_values.diff(-1)
        differences.iloc[-1] = ecdf_values.iloc[-1]
        ecdf_values = tuple(differences.values)

        RP = tuple(ecdf * payout for ecdf, payout in zip(ecdf_values, Payouts))
        df = pd.DataFrame({'S{}'.format(i+1): [rp] for i, rp in enumerate(RP)})
        df['Phase'] = Phase
        df['Cover'] = cover
        df['Total_RP'] = sum(RP)  # Corrected to use the actual risk_unit value

    dfs.append(df)  # Append individual DataFrame to the list

    result_df = pd.concat(dfs, ignore_index=True, sort=False)  # Concatenate all DataFrames in the list
    
    return result_df, Payout_Freq, cover_name


def Import_ECDF(cover_data, max_limit, Priority, bin_multipliers, Data_Source, strat_wt, to_round="Yes", num_bins = 0):
    # max_limit = inf
    # Priority = 1

    np.random.seed(42)
    stratified_data = generate_stratified_sample(cover_data, strat_wt)
    stratified_data = sorted(stratified_data['parameter'].tolist())

    if num_bins == 0:
        num_bins = len(cover_data)

    hist, bin_edges = np.histogram(stratified_data, bins=num_bins)

    if ((Data_Source.split('_')[0] + '_' + Data_Source.split('_')[1]) in ws_ds_names) or ((Data_Source.split('_')[0] + 
            '_' + Data_Source.split('_')[1]) in rf_ds_names) or ((Data_Source.split('_')[0] + '_' + Data_Source.split('_')[1]) in rh_ds_names):
        if to_round == "Yes":
            bin_edges[0] = math.floor(bin_edges[0] * bin_multipliers[0])
            bin_edges[1] = math.floor(bin_edges[1] * bin_multipliers[1])
            bin_edges[num_bins-1] = math.ceil(bin_edges[num_bins-1] * bin_multipliers[2])
            bin_edges[num_bins] = math.ceil(bin_edges[num_bins] * bin_multipliers[3])
        else:
            bin_edges[0] = round(bin_edges[0] * bin_multipliers[0],5)
            bin_edges[1] = round(bin_edges[1] * bin_multipliers[1],5)
            bin_edges[num_bins-1] = round(bin_edges[num_bins-1] * bin_multipliers[2],5)
            bin_edges[num_bins] = round(bin_edges[num_bins] * bin_multipliers[3],5)
    else:
        bin_edges[0] = math.floor(bin_edges[0] * bin_multipliers[0] * 10)/10
        bin_edges[1] = math.floor(bin_edges[1] * bin_multipliers[1] * 10)/10
        bin_edges[num_bins-1] = math.ceil(bin_edges[num_bins-1] * bin_multipliers[2] * 10)/10
        bin_edges[num_bins] = math.ceil(bin_edges[num_bins] * bin_multipliers[3] * 10)/10

    hist *= 10
    generated_values = []
    for i in range(len(hist)):
        start_edge = bin_edges[i]
        end_edge = bin_edges[i + 1]
        count = hist[i]
        values = np.linspace(start_edge, end_edge, num=count, endpoint=True)
        generated_values.extend(values)

    generated_values = np.array(generated_values)
    ecdf_values = ecdf(generated_values)
    ecdf_values = pd.DataFrame({'parameter': ecdf_values.index, 'ecdf': ecdf_values.values})
    
    return ecdf_values

# Generate Stratified Sample
def generate_stratified_sample(cover_data, strat_wt):
    # strat_wt = 3
    # np.random.seed(55)
    actual_start_year = cover_data['year'].min()
    actual_end_year = cover_data['year'].max()
    time_span = actual_end_year - actual_start_year + 1
    weights = np.linspace(1, strat_wt, time_span)
    weights = np.round(weights, decimals=0)
    years_list = sorted(cover_data['year'].unique())

    if len(years_list) < len(weights):
        initial_years = np.arange(actual_start_year, actual_end_year + 1)
        dtype = [('year', int), ('weight', float)]
        x = np.array(list(zip(initial_years, weights)), dtype=dtype)
        missing_years = np.array([year for year in initial_years if year not in years_list])
        x_filtered = np.array([row for row in x if row['year'] not in missing_years], dtype=dtype)
        weights = x_filtered['weight']

    stratified_sample = np.repeat(years_list, weights.astype(int))
    stratified_sample = pd.DataFrame({'year': stratified_sample})
    merged_data = pd.merge(stratified_sample, cover_data, on='year', how='left')
    #merged_data['parameter'] += np.random.uniform(0, 0.1, size=len(merged_data))
    return merged_data

#ECDF
def ecdf(cover_data):
    if cover_data is None:
        return None
    sorted_data = np.sort(cover_data)
    n = len(cover_data)
    y_values = np.arange(1, n + 1) / n
    return pd.Series(y_values, index=sorted_data)

def interpolate_ecdf(strike, ecdf_data):
    if strike < ecdf_data['parameter'].iloc[0]:
        return 0.0
    else:
        return np.interp(strike, ecdf_data['parameter'], ecdf_data['ecdf'])

def interpolate_ecdf_mult(strike, ecdf_data):
    if strike < ecdf_data['parameter'].iloc[0]:
        return 0.0
    else:
        return np.interp(strike, ecdf_data['parameter'], ecdf_data['ecdf'])
