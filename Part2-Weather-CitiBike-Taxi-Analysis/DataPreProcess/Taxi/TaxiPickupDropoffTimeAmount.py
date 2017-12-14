import pandas as pd
if __name__ == "__main__":
    df1 = pd.read_csv('1to24_put_dot_am.csv', usecols=['tpep_pickup_datetime','total_amount'],parse_dates=['tpep_pickup_datetime'])
    df1 = df1.assign(dayofweek=df1.tpep_pickup_datetime.dt.dayofweek, hour=df1.tpep_pickup_datetime.dt.hour)
    dayofweek_amount = df1.groupby(['dayofweek','hour']) \
                         .agg({'total_amount':'sum'}) \
                         .rename(columns={'total_amount':'amount_sum'})
    dayofweek_amount.to_csv('dayofweek_hour_amount.csv')
    monday_amount = dayofweek_amount[dayofweek_amount['dayofweek'] == 1]
    monday_amount.to_csv('monday_hour_amount.csv')
    tuesday_amount = dayofweek_amount[dayofweek_amount.dayofweek == 2]
    tuesday_amount.to_csv('tuesday_hour_amount.csv')
    wed_amount = dayofweek_amount[dayofweek_amount.dayofweek == 3]
    wed_amount.to_csv('wednesday_hour_amount.csv')
    thur_amount = dayofweek_amount[dayofweek_amount.dayofweek == 4]
    thur_amount.to_csv('thursday_hour_amount.csv')
    fri_amount = dayofweek_amount[dayofweek_amount.dayofweek == 5]
    fri_amount.to_csv('friday_hour_amount.csv')
    sat_amount = dayofweek_amount[dayofweek_amount.dayofweek == 6]
    sat_amount.to_csv('saturday_hour_amount.csv')
    sun_amount = dayofweek_amount[dayofweek_amount.dayofweek == 0]
    sun_amount.to_csv('sunday_hour_amount.csv')
