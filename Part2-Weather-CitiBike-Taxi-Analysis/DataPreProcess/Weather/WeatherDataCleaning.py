import pandas as pd
if __name__ == "__main__":
    df2015 = pd.read_csv('NYCWeather2015.csv')
    df2016 = pd.read_csv('NYCWeather2016.csv')
    df2015['YEARMODA'] = df2015['YEARMODA'].apply(lambda x: pd.to_datetime(x, format='%Y%m%d'))
    df2016['YEARMODA'] = df2016['YEARMODA'].apply(lambda x: pd.to_datetime(x, format='%Y%m%d'))
    df2015.to_csv('Weather2015.csv',index=False)
    df2016.to_csv('Weather2016.csv',index=False)
