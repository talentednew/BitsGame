import pandas as pd
if __name__ == "__main__":
    #Data for 2015
    df_w_2015 = pd.read_csv('Weather2015.csv',parse_dates=['YEARMODA'],usecols=['YEARMODA','PRCP','SNDP'])
    df_t_2015 = pd.read_csv('taxi_day_count_2015.csv')
    df_join_2015=df_w_2015.join(df_t_2015)
    df_rain_2015=df_join[(df_join.PRCP != ' 0.00G') & (df_join.PRCP != ' 0.00I') & (df_join.PRCP != ' 0.00A') & (df_join.PRCP != ' 0.00B') \
                    & (df_join.PRCP != ' 0.00C') & (df_join.PRCP != ' 0.00D') & (df_join.PRCP != ' 0.00E') & (df_join.PRCP != ' 0.00F') \
                    & (df_join.PRCP != ' 0.00G') & (df_join.PRCP != ' 0.00H') & (df_join.PRCP != 99.9)]
    df_rain_2015 = df_rain_2015[['YEARMODA','PRCP','taxi_day_count_2015']]
    df_rain_2015.to_csv('taxi_rain_2015.csv',index=False)
    df_snow_2015 = df_join[df_join.SNDP != 999.9]
    df_snow_2015 = df_snow_2015[['YEARMODA','SNDP','taxi_day_count_2015']]
    df_snow_2015.to_csv('taxi_snow_2015.csv',index=False)

    #Data for 2016
    df_w_2016 = pd.read_csv('Weather2016.csv',parse_dates=['YEARMODA'],usecols=['YEARMODA','PRCP','SNDP'])
    df_t_2016 = pd.read_csv('taxi_day_count_2016.csv')
    df_join_2016=df_w_2016.join(df_t_2016)
    df_rain_2016=df_join_2016[(df_join_2016.PRCP != ' 0.00G') & (df_join_2016.PRCP != ' 0.00I') & (df_join_2016.PRCP != ' 0.00A') & (df_join_2016.PRCP != ' 0.00B') \
                    & (df_join_2016.PRCP != ' 0.00C') & (df_join_2016.PRCP != ' 0.00D') & (df_join_2016.PRCP != ' 0.00E') & (df_join_2016.PRCP != ' 0.00F') \
                    & (df_join_2016.PRCP != ' 0.00G') & (df_join_2016.PRCP != ' 0.00H') & (df_join_2016.PRCP != 99.9)]
    df_rain_2016 = df_rain_2016[['YEARMODA','PRCP','taxi_day_count_2016']]
    df_rain_2016.to_csv('taxi_rain_2016.csv',index=False)
    df_snow_2016 = df_join_2016[df_join_2016.SNDP != 999.9]
    df_snow_2016 = df_snow_2016[['YEARMODA','SNDP','taxi_day_count_2016']]
    df_snow_2016.to_csv('taxi_snow_2016.csv',index=False)