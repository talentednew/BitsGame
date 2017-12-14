#taxi groupby month, dayofweek, hour of pickuptime 1to24
import pandas as pd
if __name__ == "__main__":
    df1 = pd.read_csv('1to24_put_dot_am.csv', usecols=['tpep_pickup_datetime'],parse_dates=['tpep_pickup_datetime'])
    df2 = df1.assign(dayofweek=df1.tpep_pickup_datetime.dt.dayofweek, \
                     hour=df1.tpep_pickup_datetime.dt.hour, month=df1.tpep_pickup_datetime.dt.month, day=df1.tpep_pickup_datetime.dt.day)
    month_count = df2.groupby(['month']) \
                         .agg({'month':'count'}) \
                         .rename(columns={'month':'taxi_month_count'})
    month_count.to_csv('taxi_month_count.csv')
    dayofweek_count = df2.groupby(['dayofweek']) \
                         .agg({'dayofweek':'count'}) \
                         .rename(columns={'dayofweek':'taxi_dayofweek_count'})
    dayofweek_count.to_csv('taxi_dayofweek_count.csv')
    hour_count = df2.groupby(['hour']) \
                         .agg({'hour':'count'}) \
                         .rename(columns={'hour':'taxi_hour_count'})
    hour_count.to_csv('taxi_hour_count.csv')

    df2015 = df2[df2.tpep_pickup_datetime.dt.year == 2015]
    df2016 = df2[df2.tpep_pickup_datetime.dt.year == 2016]
    taxi_month_count_2015 = df2015.groupby(['month']) \
                            .agg({'month':'count'}) \
                            .rename(columns={'month':'taxi_month_count_2015'})
    taxi_month_count_2015.to_csv('taxi_month_count_2015.csv')

    taxi_month_count_2016 = df2016.groupby(['month']) \
                            .agg({'month':'count'}) \
                            .rename(columns={'month': 'taxi_month_count_2016'})
    taxi_month_count_2016.to_csv('taxi_month_count_2016.csv')

    taxi_day_count_2015 = df2015.groupby(['month','day']) \
                                .agg({'month':'count'}) \
                                .rename(columns={'month':'taxi_day_count_2015'})
    taxi_day_count_2015.to_csv('taxi_day_count_2015.csv')

    taxi_day_count_2016 = df2016.groupby(['month','day']) \
                                .agg({'month':'count'}) \
                                .rename(columns={'month':'taxi_day_count_2016'})
    taxi_day_count_2016.to_csv('taxi_day_count_2016.csv')

    taxi_day_count = df2.groupby(['month','day']) \
                                .agg({'month':'count'}) \
                                .rename(columns={'month':'taxi_day_count'})
    taxi_day_count.to_csv('taxi_day_count.csv')