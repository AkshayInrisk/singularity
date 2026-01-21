import pandas as pd
import numpy as np

# Static Variables
rf_ds_names = ["IMD_RAIN", "ERA5_RAIN", "ERA5L_RAIN"]
temp_ds_names = ["IMD_TMAX", "IMD_TMIN", "IMD_TAVG", "ERA5_TMAX", "ERA5_TMIN", "ERA5_TAVG", "ERA5_HI", "ERA5L_TMAX", "ERA5L_TMIN", "ERA5L_TAVG", "ERA5L_HI"]
rh_ds_names = ["ERA5_RHAVG", "ERA5_RHMAX", "ERA5_RHMIN", "ERA5L_RHAVG", "ERA5L_RHMAX", "ERA5L_RHMIN"]
ws_ds_names = ["ERA5_WSAVG", "ERA5_WSMAX", "ERA5_WSMIN", "ERA5L_WSAVG", "ERA5L_WSMAX", "ERA5L_WSMIN"]
ss_ds_names = ["ERA5_SSRNET", "ERA5L_SSRNET"]
sm_ds_names = ["ERA5_SMAVG", "ERA5L_SMAVG"]

### Deficit Rainfall Starts ###

## Phase Cumulative Rainfall
def DR_PCRF(df, xval):
    val = float(xval)
    df = df.groupby('year')['parameter'].sum().reset_index()
    max_criteria = "limit"
    cover_name = "Deficit in Aggregate Rainfall"
    return df, max_criteria, cover_name

## N Cumulative Days Rainfall
def DR_NCRF(df, xval):
    val = float(xval)
    rolling_sum = df.groupby('year')['parameter'].rolling(window = int(val)).sum()
    df = rolling_sum.groupby('year').min().reset_index()
    max_criteria = "limit"
    cover_name = "Deficit in " + str(int(val)) + " Consecutive Day Cumulative Rainfall"
    return df, max_criteria, cover_name

## Phase Cumulative Rainfall - Variable Payout
def DR_PCRFVAR(df, xval):
    # val = float(xval)
    df = df.groupby('year')['parameter'].sum().reset_index()
    max_criteria = "limit"
    cover_name = "Variable Deficit in Aggregate Rainfall"
    return df, max_criteria, cover_name

## Number of Dry Days
def DR_NDD(df, xval):
    val = float(xval)
    df.loc[:, 'TARGET_DAY'] = np.where(df['parameter'] <= val, 1, 0)
    df = df.groupby('year')['TARGET_DAY'].sum().reset_index()
    df = df.rename(columns={'TARGET_DAY':'parameter'})
    max_criteria = "phase_len"
    cover_name = "Number of Dry Days (rainfall <= " + str(val) + " mm)"
    return df, max_criteria, cover_name

## Consecutive Dry Days
def DR_CDD(df, xval):
    val = float(xval)
    df.loc[:, 'TARGET_DAY'] = np.where(df['parameter'] <= val, 1, 0)
    df['time'] = pd.to_datetime(df['time'], format='%d-%m-%y')
    max_consecutive_days = {}
    for year, group in df.groupby(df['year']):
        max_consecutive_days[year] = 0
        current_consecutive_days = 0
        for tgt_day in group['TARGET_DAY']:
            if tgt_day == 1:
                current_consecutive_days += 1
                max_consecutive_days[year] = max(max_consecutive_days[year], current_consecutive_days)
            else:
                current_consecutive_days = 0

    df = pd.DataFrame(list(max_consecutive_days.items()), columns=['year', 'parameter'])
    
    max_criteria = "phase_len"
    cover_name = "Number of Consecutive Dry Days (rainfall <= " + str(val) + " mm)"
    return df, max_criteria, cover_name

### Deficit Rainfall Ends ###




### Excess Rainfall Starts ###

## N Cumulative Days Rainfall
def ER_NCRF(df, xval):
    val = float(xval)
    rolling_sum = df.groupby('year')['parameter'].rolling(window = int(val)).sum()
    df = rolling_sum.groupby('year').max().reset_index()

    max_criteria = "limit"
    cover_name = "Excess in " + str(int(val)) + " Consecutive Day Cumulative Rainfall "
    return df, max_criteria, cover_name

## N Cumulative Days Rainfall - Variable Payout
def ER_NCRFVAR(df, xval):
    val = float(xval)
    rolling_sum = df.groupby('year')['parameter'].rolling(window = int(val)).sum()
    df = rolling_sum.groupby('year').max().reset_index()
    max_criteria = "limit"
    cover_name = "Variable Excess in " + str(int(val)) + " Consecutive Day Cumulative Rainfall"
    return df, max_criteria, cover_name

## Phase Cumulative Rainfall
def ER_PCRF(df, xval):
    val = float(xval)
    df = df.groupby('year')['parameter'].sum().reset_index()
    max_criteria = "limit"
    cover_name = "Excess in Aggregate Rainfall"
    return df, max_criteria, cover_name

## Phase Cumulative Rainfall - Variable Payout
def ER_PCRFVAR(df, xval):
    val = float(xval)
    df = df.groupby('year')['parameter'].sum().reset_index()
    max_criteria = "limit"
    cover_name = "Variable Excess in Aggregate Rainfall"
    return df, max_criteria, cover_name

## Number of Wet Days
def ER_NWD(df, xval):
    val = float(xval)
    df.loc[:, 'TARGET_DAY'] = np.where(df['parameter'] >= val, 1, 0)
    df = df.groupby('year')['TARGET_DAY'].sum().reset_index()
    df = df.rename(columns={'TARGET_DAY':'parameter'})
    max_criteria = "phase_len"
    cover_name = "Number of Wet Days (rainfall >= " + str(val) + " mm)"
    return df, max_criteria, cover_name

## Consectuive Wet Days
def ER_CWD(df, xval):
    val = float(xval)
    df.loc[:, 'TARGET_DAY'] = np.where(df['parameter'] >= val, 1, 0)
    df['time'] = pd.to_datetime(df['time'], format='%d-%m-%y')
    max_consecutive_days = {}
    for year, group in df.groupby(df['year']):
        max_consecutive_days[year] = 0
        current_consecutive_days = 0
        for tgt_day in group['TARGET_DAY']:
            if tgt_day == 1:
                current_consecutive_days += 1
                max_consecutive_days[year] = max(max_consecutive_days[year], current_consecutive_days)
            else:
                current_consecutive_days = 0

    df = pd.DataFrame(list(max_consecutive_days.items()), columns=['year', 'parameter'])
    
    max_criteria = "phase_len"
    cover_name = "Number of Consecutive Wet Days (rainfall >= " + str(val) + " mm)"
    return df, max_criteria, cover_name

def ER_NWDVAR(df, xval):
    val = float(xval)
    df.loc[:, 'TARGET_DAY'] = np.where(df['parameter'] >= val, 1, 0)
    df = df.groupby('year')['TARGET_DAY'].sum().reset_index()
    df = df.rename(columns={'TARGET_DAY':'parameter'})
    
    max_criteria = "phase_len"
    cover_name = "Variable Number of Wet Days (rainfall >= " + str(val) + " mm)"
    return df, max_criteria, cover_name

def ER_NWXDVAR(df, xval):
    days = int(str(xval).strip('()').split(',')[0])
    val = float(str(xval).strip('()').split(',')[1])
    df['ROLLING_XDAY'] = df.groupby('year')['parameter'].transform(lambda x: x.rolling(days, min_periods=days).sum())
    # Count how many times X-day rainfall exceeds threshold
    df['TARGET_DAY'] = np.where(df['ROLLING_XDAY'] >= val, 1, 0)
    df = df.drop(columns=['ROLLING_XDAY'])
    # Group by year and sum exceedances
    df = df.groupby('year')['TARGET_DAY'].sum().reset_index()
    df = df.rename(columns={'TARGET_DAY': 'parameter'})
    max_criteria = "phase_len"
    cover_name = f"Number of {days}-day cumulative rainfall events (≥ {val} mm)"
    return df, max_criteria, cover_name

def ER_NWXDCONTVAR(df, xval):
    # breakpoint()
    days = int(str(xval).strip('()').split(',')[0])
    spread = str(xval).strip('()').split(',')[1]
    val = float(str(xval).strip('()').split(',')[2])

    # breakpoint()
    # To be corrected    
    spread_value = val

    df['ROLLING_XDAY'] = df.groupby('year')['parameter'].transform(lambda x: x.rolling(days, min_periods=days).sum())
    # Count how many effective days X-day rainfall exceeds threshold
    excess_frac = (df['ROLLING_XDAY'] - val) / spread_value  # fraction above threshold
    df['TARGET_DAY'] = np.clip(excess_frac, 0, 1)   # limit to [0, 1]
    df = df.drop(columns=['ROLLING_XDAY'])
    # Group by year and sum exceedances
    df = df.groupby('year')['TARGET_DAY'].sum().reset_index()
    df = df.rename(columns={'TARGET_DAY': 'parameter'})
    max_criteria = "phase_len"
    cover_name = f"Number of {days}-day cumulative rainfall events (≥ {val} mm) - Continuous Payout"
    return df, max_criteria, cover_name

## Consectuive Wet Days
def ER_CWDVAR(df, xval):
    val = float(xval)
    df.loc[:, 'TARGET_DAY'] = np.where(df['parameter'] >= val, 1, 0)
    df['time'] = pd.to_datetime(df['time'], format='%d-%m-%y')
    max_consecutive_days = {}
    for year, group in df.groupby(df['year']):
        max_consecutive_days[year] = 0
        current_consecutive_days = 0
        for tgt_day in group['TARGET_DAY']:
            if tgt_day == 1:
                current_consecutive_days += 1
                max_consecutive_days[year] = max(max_consecutive_days[year], current_consecutive_days)
            else:
                current_consecutive_days = 0

    df = pd.DataFrame(list(max_consecutive_days.items()), columns=['year', 'parameter'])
    
    max_criteria = "phase_len"
    cover_name = "Variable Number of Consecutive Wet Days (rainfall >= " + str(val) + " mm)"
    return df, max_criteria, cover_name

### Excess Rainfall Ends ###




### High Temperature Starts ###

## N Consecutive Days Average maxmum Temperature 
def HT_NAHT(df, xval):
    val = float(xval)
    rolling_avg = df.groupby('year')['parameter'].rolling(window = int(val)).mean()
    df = rolling_avg.groupby('year').max().reset_index()
    
    max_criteria = "limit"
    cover_name = str(int(val)) + " Consecutive Day Average Temperature"
    return df, max_criteria, cover_name

def HT_NAHTVAR(df, xval):
    val = float(xval)
    rolling_avg = df.groupby('year')['parameter'].rolling(window = int(val)).mean()
    df = rolling_avg.groupby('year').max().reset_index()
    
    max_criteria = "limit"
    cover_name = str(int(val)) + " Consecutive Day Average Temperature"
    return df, max_criteria, cover_name
    
## Nth Highest Temperature in a month
def HT_NTHPM(df, xval):
    n = int(xval)

    df['month'] = df['time'].dt.month

    nth_highest = (
        df.groupby(["year", "month"])["parameter"]
        .nlargest(n)
        .groupby(level=[0, 1])
        .last()
        .reset_index()
    )

    df = nth_highest[["year", "parameter"]]

    max_criteria = "limit"
    cover_name = f"{n}th Highest Daily Temperature in a Month"
    return df, max_criteria, cover_name


## Phase Average maxmum Temperature
def HT_PAHT(df, xval):
    val = float(xval)
    df = df.groupby('year')['parameter'].mean().reset_index()
    max_criteria = "limit"
    cover_name = "Phase Average Temperature"
    return df, max_criteria, cover_name

## Phase Average Temperature - Variable Payout
def HT_PAHTVAR(df, xval):
    # Calculates the average temperature of all days in the phase
    df = df.groupby('year')['parameter'].mean().reset_index()
    max_criteria = "limit"
    cover_name = "Phase Average Temperature Variable"
    return df, max_criteria, cover_name

## Phase Cumulative Positive Temperature Deviation - Variable Payout
def HT_PDHTVAR(df, xval):
    # Create a day-month key to calculate LTA per calendar day
    df['day_month'] = df['time'].dt.strftime('%m-%d')
    
    # Calculate the average temperature for each day across all years in the dataset
    lta = df.groupby('day_month')['parameter'].transform('mean')
    
    # Index = Sum of [Max(0, Daily Temp - LTA)] over the phase
    df['pos_dev'] = (df['parameter'] - lta).clip(lower=0)
    df_annual = df.groupby('year')['pos_dev'].sum().reset_index().rename(columns={'pos_dev': 'parameter'})
    
    max_criteria = "limit"
    cover_name = "Phase Cumulative Positive Temp Deviation Variable"
    return df_annual, max_criteria, cover_name

## Cumulative Positive Temperature Deviation from a Fixed Baseline - Variable Payout
def HT_CPHTVAR(df, xval):
    # xval is the baseline temperature (e.g. 35) to be subtracted from each day
    val = float(xval)
    
    # Calculate daily positive deviation
    df['parameter'] = (df['parameter'] - val).clip(lower=0)
    
    # Sum deviations over the phase per year
    df = df.groupby('year')['parameter'].sum().reset_index()
    
    max_criteria = "limit"
    cover_name = "Cumulative Heat Temperature Deviation"
    return df, max_criteria, cover_name

## Number of Hot Days
def HT_NHD(df, xval):
    val = float(xval)
    df.loc[:, 'TARGET_DAY'] = np.where(df['parameter'] >= val, 1, 0)
    df = df.groupby('year')['TARGET_DAY'].sum().reset_index()
    df = df.rename(columns={'TARGET_DAY':'parameter'})
    max_criteria = "phase_len"
    cover_name = "Number of Hot Days (temperature >= " + str(val) + " degree celsius)"
    return df, max_criteria, cover_name

## Number of Hot Days - Variable
def HT_NHDVAR(df, xval):
    val = float(xval)
    df.loc[:, 'TARGET_DAY'] = np.where(df['parameter'] >= val, 1, 0)
    df = df.groupby('year')['TARGET_DAY'].sum().reset_index()
    df = df.rename(columns={'TARGET_DAY':'parameter'})
    max_criteria = "phase_len"
    cover_name = "Variable Number of Hot Days (temperature >= " + str(val) + " degree celsius)"
    return df, max_criteria, cover_name

## Number of Hot X-day Events - Variable (based on rolling average temperature)
def HT_NHXDVAR(df, xval):
    days = int(str(xval).strip('()').split(',')[0])
    val = float(str(xval).strip('()').split(',')[1])
    # Rolling average of X-day temperature within each year
    df['ROLLING_XDAY_AVG'] = df.groupby('year')['parameter'].transform(
        lambda x: x.rolling(days, min_periods=days).mean()
    )
    
    # Flag if rolling average exceeds threshold
    df['TARGET_DAY'] = np.where(df['ROLLING_XDAY_AVG'] >= val, 1, 0)
    df = df.drop(columns=['ROLLING_XDAY_AVG'])
    
    # Count number of such hot events per year
    df = df.groupby('year')['TARGET_DAY'].sum().reset_index()
    df = df.rename(columns={'TARGET_DAY': 'parameter'})
    
    max_criteria = "phase_len"
    cover_name = f"Number of {days}-day average temperature events (≥ {val} °C)"
    return df, max_criteria, cover_name

def HT_NHXDVARMIN(df, xval):
    days = int(str(xval).strip('()').split(',')[0])
    val = float(str(xval).strip('()').split(',')[1])
    # Rolling average of X-day temperature within each year
    df['ROLLING_XDAY_AVG'] = df.groupby('year')['parameter'].transform(
        lambda x: x.rolling(days, min_periods=days).min()
    )
    
    # Flag if rolling average exceeds threshold
    df['TARGET_DAY'] = np.where(df['ROLLING_XDAY_AVG'] >= val, 1, 0)
    df = df.drop(columns=['ROLLING_XDAY_AVG'])
    
    # Count number of such hot events per year
    df = df.groupby('year')['TARGET_DAY'].sum().reset_index()
    df = df.rename(columns={'TARGET_DAY': 'parameter'})
    
    max_criteria = "phase_len"
    cover_name = f"Number of {days}-day average temperature events (≥ {val} °C)"
    return df, max_criteria, cover_name


## Consecutive Hot Days
def HT_CHD(df, xval):
    val = float(xval)
    df.loc[:, 'TARGET_DAY'] = np.where(df['parameter'] >= val, 1, 0)
    df['time'] = pd.to_datetime(df['time'], format='%d-%m-%y')
    max_consecutive_days = {}
    for year, group in df.groupby(df['year']):
        max_consecutive_days[year] = 0
        current_consecutive_days = 0
        for tgt_day in group['TARGET_DAY']:
            if tgt_day == 1:
                current_consecutive_days += 1
                max_consecutive_days[year] = max(max_consecutive_days[year], current_consecutive_days)
            else:
                current_consecutive_days = 0

    df = pd.DataFrame(list(max_consecutive_days.items()), columns=['year', 'parameter'])
        
    max_criteria = "phase_len"
    cover_name = "Number of Consecutive Hot Days (temperature >= " + str(val) + " degree celsius)"
    return df, max_criteria, cover_name

## Variable Consecutive Hot Days
def HT_CHDVAR(df, xval):
    val = float(xval)
    df.loc[:, 'TARGET_DAY'] = np.where(df['parameter'] >= val, 1, 0)
    df['time'] = pd.to_datetime(df['time'], format='%d-%m-%y')
    max_consecutive_days = {}
    for year, group in df.groupby(df['year']):
        max_consecutive_days[year] = 0
        current_consecutive_days = 0
        for tgt_day in group['TARGET_DAY']:
            if tgt_day == 1:
                current_consecutive_days += 1
                max_consecutive_days[year] = max(max_consecutive_days[year], current_consecutive_days)
            else:
                current_consecutive_days = 0

    df = pd.DataFrame(list(max_consecutive_days.items()), columns=['year', 'parameter'])
        
    max_criteria = "phase_len"
    cover_name = "Variable Number of Consecutive Hot Days (temperature >= " + str(val) + " degree celsius)"
    return df, max_criteria, cover_name

### High Temperature Ends ###



### Low Temperature Starts ###

## N Consecutive Days Average Minimum Temperature 
def LT_NALT(df, xval):
    val = float(xval)
    rolling_avg = df.groupby('year')['parameter'].rolling(window = int(val)).mean()
    df = rolling_avg.groupby('year').min().reset_index()
    
    max_criteria = "limit"
    cover_name = str(int(val)) + " Consecutive Day Average Temperature"
    return df, max_criteria, cover_name

## Phase Average Minimum Temperature
def LT_PALT(df, xval):
    val = float(xval)
    df = df.groupby('year')['parameter'].mean().reset_index()
    max_criteria = "limit"
    cover_name = "Phase Average Temperature"
    return df, max_criteria, cover_name

## Number of Cold Days
def LT_NCD(df, xval):
    val = float(xval)
    df.loc[:, 'TARGET_DAY'] = np.where(df['parameter'] <= val, 1, 0)
    df = df.groupby('year')['TARGET_DAY'].sum().reset_index()
    df = df.rename(columns={'TARGET_DAY':'parameter'})
    max_criteria = "phase_len"
    cover_name = "Number of Cold Days (temperature <= " + str(val) + " degree celsius)"
    return df, max_criteria, cover_name

## Number of Cold Days - Variable
def LT_NCDVAR(df, xval):
    val = float(xval)
    df.loc[:, 'TARGET_DAY'] = np.where(df['parameter'] <= val, 1, 0)
    df = df.groupby('year')['TARGET_DAY'].sum().reset_index()
    df = df.rename(columns={'TARGET_DAY':'parameter'})
    max_criteria = "phase_len"
    cover_name = "Variable Number of Cold Days (temperature <= " + str(val) + " degree celsius)"
    return df, max_criteria, cover_name

## Consecutive Cold Days
def LT_CCD(df, xval):
    val = float(xval)
    df.loc[:, 'TARGET_DAY'] = np.where(df['parameter'] <= val, 1, 0)
    df['time'] = pd.to_datetime(df['time'], format='%d-%m-%y')
    max_consecutive_days = {}
    for year, group in df.groupby(df['year']):
        max_consecutive_days[year] = 0
        current_consecutive_days = 0
        for tgt_day in group['TARGET_DAY']:
            if tgt_day == 1:
                current_consecutive_days += 1
                max_consecutive_days[year] = max(max_consecutive_days[year], current_consecutive_days)
            else:
                current_consecutive_days = 0

    df = pd.DataFrame(list(max_consecutive_days.items()), columns=['year', 'parameter'])
    
    max_criteria = "phase_len"
    cover_name = "Number of Consecutive Cold Days (temperature <= " + str(val) + " degree celsius)"
    return df, max_criteria, cover_name

## Variable Consecutive Cold Days
def LT_CCDVAR(df, xval):
    val = float(xval)
    df.loc[:, 'TARGET_DAY'] = np.where(df['parameter'] <= val, 1, 0)
    df['time'] = pd.to_datetime(df['time'], format='%d-%m-%y')
    max_consecutive_days = {}
    for year, group in df.groupby(df['year']):
        max_consecutive_days[year] = 0
        current_consecutive_days = 0
        for tgt_day in group['TARGET_DAY']:
            if tgt_day == 1:
                current_consecutive_days += 1
                max_consecutive_days[year] = max(max_consecutive_days[year], current_consecutive_days)
            else:
                current_consecutive_days = 0

    df = pd.DataFrame(list(max_consecutive_days.items()), columns=['year', 'parameter'])
    
    max_criteria = "phase_len"
    cover_name = "Variable Number of Consecutive Cold Days (temperature <= " + str(val) + " degree celsius)"
    return df, max_criteria, cover_name

## Nth Lowest Temperature in a month
def LT_NTHPM(df, xval):
    n = int(xval)

    df["month"] = df["time"].dt.month

    nth_lowest = (
        df.groupby(["year", "month"])["parameter"]
        .nsmallest(n)
        .groupby(level=[0, 1])
        .last()
        .reset_index()
    )

    df = nth_lowest[["year", "parameter"]]

    max_criteria = "limit"
    cover_name = f"{n}th Lowest Daily Temperature in a Month"
    return df, max_criteria, cover_name


### Low Temperature Ends ###


### Diurnal Temperature Range Starts ###

## Number of High Diurnal Temperature Range Days
def TR_NHDTRD(df, xval):
    val = float(xval)
    df.loc[:, 'TARGET_DAY'] = np.where(df['parameter'] >= val, 1, 0)
    df = df.groupby('year')['TARGET_DAY'].sum().reset_index()
    df = df.rename(columns={'TARGET_DAY':'parameter'})
    max_criteria = "phase_len"
    cover_name = "Number of High Diurnal Temperate Range Days (range >= " + str(val) + " degree celsius)"
    return df, max_criteria, cover_name

## Number of High Diurnal Temperature Range Days - Variable
def TR_NHDTRDVAR(df, xval):
    val = float(xval)
    df.loc[:, 'TARGET_DAY'] = np.where(df['parameter'] >= val, 1, 0)
    df = df.groupby('year')['TARGET_DAY'].sum().reset_index()
    df = df.rename(columns={'TARGET_DAY':'parameter'})
    max_criteria = "phase_len"
    cover_name = "Variable Number of High Diurnal Temperate Range Days (range >= " + str(val) + " degree celsius)"
    return df, max_criteria, cover_name

## Consecutive High Diurnal Temperature Range Days
def TR_CHDTRD(df, xval):
    val = float(xval)
    df.loc[:, 'TARGET_DAY'] = np.where(df['parameter'] >= val, 1, 0)
    df['time'] = pd.to_datetime(df['time'], format='%d-%m-%y')
    max_consecutive_days = {}
    for year, group in df.groupby(df['year']):
        max_consecutive_days[year] = 0
        current_consecutive_days = 0
        for tgt_day in group['TARGET_DAY']:
            if tgt_day == 1:
                current_consecutive_days += 1
                max_consecutive_days[year] = max(max_consecutive_days[year], current_consecutive_days)
            else:
                current_consecutive_days = 0

    df = pd.DataFrame(list(max_consecutive_days.items()), columns=['year', 'parameter'])
        
    max_criteria = "phase_len"
    cover_name = "Number of Consecutive High Diurnal Temperate Range Days (range >= " + str(val) + " degree celsius)"

    return df, max_criteria, cover_name

### Diurnal Temperature Range Ends ###



### RH Covers Start ###

def ERH_NHRHD(df, xval):
    val = float(xval)
    df.loc[:, 'TARGET_DAY'] = np.where(df['parameter'] >= val, 1, 0)
    df = df.groupby('year')['TARGET_DAY'].sum().reset_index()
    df = df.rename(columns={'TARGET_DAY':'parameter'})
    max_criteria = "phase_len"
    cover_name = "Number of Humid Days (RH >= " + str(val) + " percent)"
    return df, max_criteria, cover_name

def ERH_NHRHDVAR(df, xval):
    val = float(xval)
    df.loc[:, 'TARGET_DAY'] = np.where(df['parameter'] >= val, 1, 0)
    df = df.groupby('year')['TARGET_DAY'].sum().reset_index()
    df = df.rename(columns={'TARGET_DAY':'parameter'})
    max_criteria = "phase_len"
    cover_name = "Variable Number of Humid Days (RH >= " + str(val) + " percent)"
    return df, max_criteria, cover_name

def ERH_CHRHD(df, xval):
    val = float(xval)
    df.loc[:, 'TARGET_DAY'] = np.where(df['parameter'] >= val, 1, 0)
    df['time'] = pd.to_datetime(df['time'], format='%d-%m-%y')
    max_consecutive_days = {}
    for year, group in df.groupby(df['year']):
        max_consecutive_days[year] = 0
        current_consecutive_days = 0
        for tgt_day in group['TARGET_DAY']:
            if tgt_day == 1:
                current_consecutive_days += 1
                max_consecutive_days[year] = max(max_consecutive_days[year], current_consecutive_days)
            else:
                current_consecutive_days = 0

    df = pd.DataFrame(list(max_consecutive_days.items()), columns=['year', 'parameter'])
    
    max_criteria = "phase_len"
    cover_name = "Number of Consecutive Humid Days (RH >= " + str(val) + " percent)"
    return df, max_criteria, cover_name

def ERH_CHRHDVAR(df, xval):
    val = float(xval)
    df.loc[:, 'TARGET_DAY'] = np.where(df['parameter'] >= val, 1, 0)
    df['time'] = pd.to_datetime(df['time'], format='%d-%m-%y')
    max_consecutive_days = {}
    for year, group in df.groupby(df['year']):
        max_consecutive_days[year] = 0
        current_consecutive_days = 0
        for tgt_day in group['TARGET_DAY']:
            if tgt_day == 1:
                current_consecutive_days += 1
                max_consecutive_days[year] = max(max_consecutive_days[year], current_consecutive_days)
            else:
                current_consecutive_days = 0

    df = pd.DataFrame(list(max_consecutive_days.items()), columns=['year', 'parameter'])
    
    max_criteria = "phase_len"
    cover_name = "Variable Number of Consecutive Humid Days (RH >= " + str(val) + " percent)"
    return df, max_criteria, cover_name

def DRH_NLRHDVAR(df, xval):
    val = float(xval)
    df.loc[:, 'TARGET_DAY'] = np.where(df['parameter'] <= val, 1, 0)
    df = df.groupby('year')['TARGET_DAY'].sum().reset_index()
    df = df.rename(columns={'TARGET_DAY':'parameter'})
    max_criteria = "phase_len"
    cover_name = "Variable Number of Low Humidity Days (RH <= " + str(val) + " percent)"
    return df, max_criteria, cover_name

### RH Covers End ###


### Wind Speed Covers Start ###

# High Wind Speed
def HW_MWS(df, xval):
    val = float(xval)
    df = df.groupby('year')['parameter'].max().reset_index()

    max_criteria = "limit"
    cover_name = "Excess in Maximum Wind Speed"
    return df, max_criteria, cover_name

### Wind Speed Covers End ###


### Solar Radiation Covers Start ###

# Number of Low Sunshine Days
def LS_NLSD(df, xval):
    val = float(xval)
    df.loc[:, 'TARGET_DAY'] = np.where(df['parameter'] <= val, 1, 0)
    df = df.groupby('year')['TARGET_DAY'].sum().reset_index()
    df = df.rename(columns={'TARGET_DAY':'parameter'})
    max_criteria = "phase_len"
    cover_name = "Number of Low Sunshine Days (Net Solar Radiation <= " + str(val) + " kWh per sq. mtr.)"
    return df, max_criteria, cover_name

# Consecutive Low Sunshine Days
def LS_CLSD(df, xval):
    val = float(xval)
    df.loc[:, 'TARGET_DAY'] = np.where(df['parameter'] <= val, 1, 0)
    df['time'] = pd.to_datetime(df['time'], format='%d-%m-%y')
    max_consecutive_days = {}
    for year, group in df.groupby(df['year']):
        max_consecutive_days[year] = 0
        current_consecutive_days = 0
        for tgt_day in group['TARGET_DAY']:
            if tgt_day == 1:
                current_consecutive_days += 1
                max_consecutive_days[year] = max(max_consecutive_days[year], current_consecutive_days)
            else:
                current_consecutive_days = 0

    df = pd.DataFrame(list(max_consecutive_days.items()), columns=['year', 'parameter'])
    
    max_criteria = "phase_len"
    cover_name = "Number of Consecutive Low Sunshine Days (Net Solar Radiation <= " + str(val) + " kWh per sq. mtr.)"
    return df, max_criteria, cover_name

## Phase Cumulative Solar Irradiation
def LS_PCSR(df, xval):
    val = float(xval)
    df = df.groupby('year')['parameter'].sum().reset_index()
    max_criteria = "limit"
    cover_name = "Deficit in Aggregate Solar Irradiation"
    return df, max_criteria, cover_name

## Phase Cumulative Solar Irradiation - Variable Payout
def LS_PCSRVAR(df, xval):
    val = float(xval)
    df = df.groupby('year')['parameter'].sum().reset_index()
    max_criteria = "limit"
    cover_name = "Variable Deficit in Aggregate Solar Irradiation"
    return df, max_criteria, cover_name

## N Consecutive Days Cumulative Solar Irradiation
def LS_NCSR(df, xval):
    val = float(xval)
    rolling_sum = df.groupby('year')['parameter'].rolling(window = int(val)).sum()
    df = rolling_sum.groupby('year').min().reset_index()
    max_criteria = "limit"
    cover_name = "Deficit in " + str(int(val)) + " Consecutive Day Cumulative Solar Irradiation"
    return df, max_criteria, cover_name

### Solar Radiation Covers End ###



### Soil Moisture Covers Start ###

## N Consecutive Days Average Soil Moisture 
def LSM_NASM(df, xval):
    val = float(xval)
    rolling_avg = df.groupby('year')['parameter'].rolling(window = int(val)).mean()
    df = rolling_avg.groupby('year').min().reset_index()
    df['parameter'] = round(df['parameter']*100, 2)

    max_criteria = "limit"
    cover_name = "Deficit in " + str(int(val)) + " Consecutive Day Average Soil Moisture"
    return df, max_criteria, cover_name

def LSM_NASMVAR(df, xval):
    val = float(xval)
    rolling_avg = df.groupby('year')['parameter'].rolling(window = int(val)).mean()
    df = rolling_avg.groupby('year').min().reset_index()
    df['parameter'] = round(df['parameter']*100, 2)

    max_criteria = "limit"
    cover_name = "Variable Deficit in " + str(int(val)) + " Consecutive Day Average Soil Moisture"
    return df, max_criteria, cover_name


### Soil Moisture Covers End ###