#citi_week_hour_count
import pandas as pd
if __name__ == "__main__":
    df1 = pd.read_csv('citi_1to24.csv',parse_dates=['starttime', 'stoptime'])
    df2015 = pd.read_csv('citi_2015_1to12.csv',parse_dates=['starttime', 'stoptime'])
    df2016 = pd.read_csv('citi_2016_1to12.csv',parse_dates=['starttime', 'stoptime'])
    df2 = df1.assign(dayofweek=df1.starttime.dt.dayofweek, hour=df1.starttime.dt.hour)
    week_hour_count = df2.groupby(['dayofweek','hour']) \
                         .agg({'dayofweek':'count'}) \
                         .rename(columns={'dayofweek':'count'})
    week_hour_count.to_csv('citi_week_hour_count.csv')

    #citi groupby month of starttime 1to24
    df1 = pd.read_csv('citi_1to24.csv',parse_dates=['starttime', 'stoptime'])
    df2 = df1.assign(dayofweek=df1.starttime.dt.dayofweek, hour=df1.starttime.dt.hour, month=df1.starttime.dt.month)
    month_count = df2.groupby(['month']) \
                         .agg({'month':'count'}) \
                         .rename(columns={'month':'month_count'})
    month_count.to_csv('citi_month_count.csv')
    dayofweek_count = df2.groupby(['dayofweek']) \
                         .agg({'dayofweek':'count'}) \
                         .rename(columns={'dayofweek':'dayofweek_count'})
    dayofweek_count.to_csv('citi_dayofweek_count.csv')
    hour_count = df2.groupby(['hour']) \
                         .agg({'hour':'count'}) \
                         .rename(columns={'hour':'hour_count'})
    hour_count.to_csv('citi_hour_count.csv')
