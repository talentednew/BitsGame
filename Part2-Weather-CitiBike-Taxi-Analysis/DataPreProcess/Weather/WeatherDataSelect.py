import pandas as pd
if __name__ == "__main__":
    df2015 = pd.read_csv('Weather2015.csv',parse_dates=['YEARMODA'])
    df2016 = pd.read_csv('Weather2016.csv',parse_dates=['YEARMODA'])
    df2 = df2015.assign(day=df2015.YEARMODA.dt.day, month=df2015.YEARMODA.dt.month)
    df3 = df2016.assign(day=df2016.YEARMODA.dt.day, month=df2016.YEARMODA.dt.month)
    weather_month_mean_2015 = df2.groupby(['month']) \
                            .agg({'TEMP':'mean'}) \
                            .rename(columns={'TEMP':'TEMP_MONTH'})
    weather_month_mean_2015.to_csv('weather_month_mean_2015.csv',index=False)
    weather_month_mean_2016 = df3.groupby(['month']) \
                            .agg({'TEMP':'mean'}) \
                            .rename(columns={'TEMP':'TEMP_MONTH'})
    weather_month_mean_2016.to_csv('weather_month_mean_2016.csv',index=False)

    weather_day_mean_2015 = df2[['month','day','TEMP']]
    weather_day_mean_2015.to_csv('weather_day_mean_2015.csv',index=False)

    weather_day_mean_2016 = df3[['month','day','TEMP']]
    weather_day_mean_2016.to_csv('weather_day_mean_2016.csv',index=False)
