import pandas as pd
if __name__ == "__main__":
    df1 = pd.read_csv('201501-citibike-tripdata.csv')
    df1['starttime'] = df1['starttime'].apply(lambda x: pd.to_datetime(x, format='%m/%d/%Y %H:%M'))
    df1['stoptime'] = df1['stoptime'].apply(lambda x: pd.to_datetime(x, format='%m/%d/%Y %H:%M'))
    df2 = pd.read_csv('201502-citibike-tripdata.csv')
    df2['starttime'] = df2['starttime'].apply(lambda x: pd.to_datetime(x, format='%m/%d/%Y %H:%M'))
    df2['stoptime'] = df2['stoptime'].apply(lambda x: pd.to_datetime(x, format='%m/%d/%Y %H:%M'))
    df3 = pd.read_csv('201503-citibike-tripdata.csv')
    df3['starttime'] = df3['starttime'].apply(lambda x: pd.to_datetime(x, format='%m/%d/%Y %H:%M'))
    df3['stoptime'] = df3['stoptime'].apply(lambda x: pd.to_datetime(x, format='%m/%d/%Y %H:%M'))
    df4 = pd.read_csv('201504-citibike-tripdata.csv')
    df4['starttime'] = df4['starttime'].apply(lambda x: pd.to_datetime(x, format='%m/%d/%Y %H:%M:%S'))
    df4['stoptime'] = df4['stoptime'].apply(lambda x: pd.to_datetime(x, format='%m/%d/%Y %H:%M:%S'))
    df5 = pd.read_csv('201505-citibike-tripdata.csv')
    df5['starttime'] = df5['starttime'].apply(lambda x: pd.to_datetime(x, format='%m/%d/%Y %H:%M:%S'))
    df5['stoptime'] = df5['stoptime'].apply(lambda x: pd.to_datetime(x, format='%m/%d/%Y %H:%M:%S'))
    df6 = pd.read_csv('201506-citibike-tripdata.csv')
    df6['starttime'] = df6['starttime'].apply(lambda x: pd.to_datetime(x, format='%m/%d/%Y %H:%M'))
    df6['stoptime'] = df6['stoptime'].apply(lambda x: pd.to_datetime(x, format='%m/%d/%Y %H:%M'))
    df = df1.append(df2).append(df3).append(df4).append(df5).append(df6)
    df.to_csv('citi_2015_1to6.csv',index=False)

    # 2015_7to12
    df1 = pd.read_csv('201507-citibike-tripdata.csv')
    df1['starttime'] = df1['starttime'].apply(lambda x: pd.to_datetime(x, format='%m/%d/%Y %H:%M:%S'))
    df1['stoptime'] = df1['stoptime'].apply(lambda x: pd.to_datetime(x, format='%m/%d/%Y %H:%M:%S'))
    df2 = pd.read_csv('201508-citibike-tripdata.csv')
    df2['starttime'] = df2['starttime'].apply(lambda x: pd.to_datetime(x, format='%m/%d/%Y %H:%M:%S'))
    df2['stoptime'] = df2['stoptime'].apply(lambda x: pd.to_datetime(x, format='%m/%d/%Y %H:%M:%S'))
    df3 = pd.read_csv('201509-citibike-tripdata.csv')
    df3['starttime'] = df3['starttime'].apply(lambda x: pd.to_datetime(x, format='%m/%d/%Y %H:%M:%S'))
    df3['stoptime'] = df3['stoptime'].apply(lambda x: pd.to_datetime(x, format='%m/%d/%Y %H:%M:%S'))
    df4 = pd.read_csv('201510-citibike-tripdata.csv')
    df4['starttime'] = df4['starttime'].apply(lambda x: pd.to_datetime(x, format='%m/%d/%Y %H:%M:%S'))
    df4['stoptime'] = df4['stoptime'].apply(lambda x: pd.to_datetime(x, format='%m/%d/%Y %H:%M:%S'))
    df5 = pd.read_csv('201511-citibike-tripdata.csv')
    df5['starttime'] = df5['starttime'].apply(lambda x: pd.to_datetime(x, format='%m/%d/%Y %H:%M:%S'))
    df5['stoptime'] = df5['stoptime'].apply(lambda x: pd.to_datetime(x, format='%m/%d/%Y %H:%M:%S'))
    df6 = pd.read_csv('201512-citibike-tripdata.csv')
    df6['starttime'] = df6['starttime'].apply(lambda x: pd.to_datetime(x, format='%m/%d/%Y %H:%M:%S'))
    df6['stoptime'] = df6['stoptime'].apply(lambda x: pd.to_datetime(x, format='%m/%d/%Y %H:%M:%S'))
    df = df1.append(df2).append(df3).append(df4).append(df5).append(df6)
    df = df.dropna()
    df.to_csv('citi_2015_7to12.csv',index=False)

    # 2016_1to6
    df1 = pd.read_csv('201601-citibike-tripdata.csv')
    df1['starttime'] = df1['starttime'].apply(lambda x: pd.to_datetime(x, format='%m/%d/%Y %H:%M:%S'))
    df1['stoptime'] = df1['stoptime'].apply(lambda x: pd.to_datetime(x, format='%m/%d/%Y %H:%M:%S'))
    df2 = pd.read_csv('201602-citibike-tripdata.csv')
    df2['starttime'] = df2['starttime'].apply(lambda x: pd.to_datetime(x, format='%m/%d/%Y %H:%M:%S'))
    df2['stoptime'] = df2['stoptime'].apply(lambda x: pd.to_datetime(x, format='%m/%d/%Y %H:%M:%S'))
    df3 = pd.read_csv('201603-citibike-tripdata.csv')
    df3['starttime'] = df3['starttime'].apply(lambda x: pd.to_datetime(x, format='%m/%d/%Y %H:%M:%S'))
    df3['stoptime'] = df3['stoptime'].apply(lambda x: pd.to_datetime(x, format='%m/%d/%Y %H:%M:%S'))
    df4 = pd.read_csv('201604-citibike-tripdata.csv')
    df4['starttime'] = df4['starttime'].apply(lambda x: pd.to_datetime(x, format='%m/%d/%Y %H:%M:%S'))
    df4['stoptime'] = df4['stoptime'].apply(lambda x: pd.to_datetime(x, format='%m/%d/%Y %H:%M:%S'))
    df5 = pd.read_csv('201605-citibike-tripdata.csv')
    df5['starttime'] = df5['starttime'].apply(lambda x: pd.to_datetime(x, format='%m/%d/%Y %H:%M:%S'))
    df5['stoptime'] = df5['stoptime'].apply(lambda x: pd.to_datetime(x, format='%m/%d/%Y %H:%M:%S'))
    df6 = pd.read_csv('201606-citibike-tripdata.csv')
    df6['starttime'] = df6['starttime'].apply(lambda x: pd.to_datetime(x, format='%m/%d/%Y %H:%M:%S'))
    df6['stoptime'] = df6['stoptime'].apply(lambda x: pd.to_datetime(x, format='%m/%d/%Y %H:%M:%S'))
    df = df1.append(df2).append(df3).append(df4).append(df5).append(df6)
    df = df.dropna()
    df.to_csv('citi_2016_1to6.csv',index=False)

    # 2016_7to12
    df1 = pd.read_csv('201607-citibike-tripdata.csv')
    df1['starttime'] = df1['starttime'].apply(lambda x: pd.to_datetime(x, format='%m/%d/%Y %H:%M:%S'))
    df1['stoptime'] = df1['stoptime'].apply(lambda x: pd.to_datetime(x, format='%m/%d/%Y %H:%M:%S'))
    df2 = pd.read_csv('201608-citibike-tripdata.csv')
    df2['starttime'] = df2['starttime'].apply(lambda x: pd.to_datetime(x, format='%m/%d/%Y %H:%M:%S'))
    df2['stoptime'] = df2['stoptime'].apply(lambda x: pd.to_datetime(x, format='%m/%d/%Y %H:%M:%S'))
    df3 = pd.read_csv('201609-citibike-tripdata.csv')
    df3['starttime'] = df3['starttime'].apply(lambda x: pd.to_datetime(x, format='%m/%d/%Y %H:%M:%S'))
    df3['stoptime'] = df3['stoptime'].apply(lambda x: pd.to_datetime(x, format='%m/%d/%Y %H:%M:%S'))
    df4 = pd.read_csv('201610-citibike-tripdata.csv',parse_dates=['Start Time', 'Stop Time']). \
          rename(columns={'Trip Duration':'tripduration','Start Time':'starttime','Stop Time':'stoptime', \
                          'Start Station ID':'start station id','Start Station Name':'start station name', \
                          'Start Station Latitude':'start station latitude','Start Station Longitude':'start station longitude', \
                          'End Station ID':'end station id','End Station Name':'end station name','End Station Latitude':'end station latitude', \
                          'End Station Longitude':'end station longitude','Bike ID':'bikeid','User Type':'usertype','Birth Year':'birth year', \
                          'Gender':'gender'})
    df5 = pd.read_csv('/home/aa5194/project/citi/201611-citibike-tripdata.csv',parse_dates=['Start Time', 'Stop Time']). \
             rename(columns={'Trip Duration':'tripduration','Start Time':'starttime','Stop Time':'stoptime', \
                             'Start Station ID':'start station id','Start Station Name':'start station name', \
                             'Start Station Latitude':'start station latitude','Start Station Longitude':'start station longitude', \
                             'End Station ID':'end station id','End Station Name':'end station name','End Station Latitude':'end station latitude', \
                             'End Station Longitude':'end station longitude','Bike ID':'bikeid','User Type':'usertype','Birth Year':'birth year', \
                             'Gender':'gender'})
    df6 = pd.read_csv('/home/aa5194/project/citi/201612-citibike-tripdata.csv',parse_dates=['Start Time', 'Stop Time']). \
             rename(columns={'Trip Duration':'tripduration','Start Time':'starttime','Stop Time':'stoptime', \
                             'Start Station ID':'start station id','Start Station Name':'start station name', \
                             'Start Station Latitude':'start station latitude','Start Station Longitude':'start station longitude', \
                             'End Station ID':'end station id','End Station Name':'end station name','End Station Latitude':'end station latitude', \
                             'End Station Longitude':'end station longitude','Bike ID':'bikeid','User Type':'usertype','Birth Year':'birth year', \
                             'Gender':'gender'})
    df = df1.append(df2).append(df3).append(df4).append(df5).append(df6)
    df = df.dropna()
    df.to_csv('citi_2016_7to12.csv',index=False)

    #append 1to24 togather
    df1 = pd.read_csv('/home/aa5194/project/citi/citi_2015_1to6.csv')
    df2 = pd.read_csv('/home/aa5194/project/citi/citi_2015_7to12.csv')
    df3 = pd.read_csv('/home/aa5194/project/citi/citi_2016_1to6.csv')
    df4 = pd.read_csv('/home/aa5194/project/citi/citi_2016_7to12.csv')
    df = df1.append(df2).append(df3).append(df4)
    df.to_csv('citi_1to24.csv',index=False)
