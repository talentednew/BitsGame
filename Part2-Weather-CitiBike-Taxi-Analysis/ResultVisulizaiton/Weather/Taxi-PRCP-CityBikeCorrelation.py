import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.mllib.stat import Statistics


weather_bike_taxi_2015_1 = spark.read.option("header",True).csv("/Users/junzhi/Trybigdata/part1/code/DataSummary/deepAnalysis/weather_bike_taxi_2015_1.csv")
weather_bike_taxi_2015_2 = spark.read.option("header",True).csv("/Users/junzhi/Trybigdata/part1/code/DataSummary/deepAnalysis/weather_bike_taxi_2015_2.csv")
weather_bike_taxi_2015_3 = spark.read.option("header",True).csv("/Users/junzhi/Trybigdata/part1/code/DataSummary/deepAnalysis/weather_bike_taxi_2015_3.csv")
weather_bike_taxi_2015_4 = spark.read.option("header",True).csv("/Users/junzhi/Trybigdata/part1/code/DataSummary/deepAnalysis/weather_bike_taxi_2015_4.csv")
weather_bike_taxi_2016_1 = spark.read.option("header",True).csv("/Users/junzhi/Trybigdata/part1/code/DataSummary/deepAnalysis/weather_bike_taxi_2016_1.csv")
weather_bike_taxi_2016_2 = spark.read.option("header",True).csv("/Users/junzhi/Trybigdata/part1/code/DataSummary/deepAnalysis/weather_bike_taxi_2016_2.csv")
weather_bike_taxi_2016_3 = spark.read.option("header",True).csv("/Users/junzhi/Trybigdata/part1/code/DataSummary/deepAnalysis/weather_bike_taxi_2016_3.csv")
weather_bike_taxi_2016_4 = spark.read.option("header",True).csv("/Users/junzhi/Trybigdata/part1/code/DataSummary/deepAnalysis/weather_bike_taxi_2016_4.csv")


features15_1 = weather_bike_taxi_2015_1.rdd.map(lambda row: row[1:])
features15_2 = weather_bike_taxi_2015_2.rdd.map(lambda row: row[1:])
features15_3 = weather_bike_taxi_2015_3.rdd.map(lambda row: row[1:])
features15_4 = weather_bike_taxi_2015_4.rdd.map(lambda row: row[1:])


features16_1 = weather_bike_taxi_2016_1.rdd.map(lambda row: row[1:])
features16_2 = weather_bike_taxi_2016_2.rdd.map(lambda row: row[1:])
features16_3 = weather_bike_taxi_2016_3.rdd.map(lambda row: row[1:])
features16_4 = weather_bike_taxi_2016_4.rdd.map(lambda row: row[1:])

corr_mat15_1=Statistics.corr(features15_1, method="pearson")
corr_mat15_2=Statistics.corr(features15_2, method="pearson")
corr_mat15_3=Statistics.corr(features15_3, method="pearson")
corr_mat15_4=Statistics.corr(features15_4, method="pearson")

corr_mat16_1=Statistics.corr(features16_1, method="pearson")
corr_mat16_2=Statistics.corr(features16_2, method="pearson")
corr_mat16_3=Statistics.corr(features16_3, method="pearson")
corr_mat16_4=Statistics.corr(features16_4, method="pearson")


sns.set(style="white")
#ax = plt.axes()
# Compute the correlation matrix
Index= ["Bikecnt","Taxicnt","TEMP","DEWP", "VISIB", "WDSP","PRCP"]
Cols = ["Bikecnt","Taxicnt","TEMP","DEWP", "VISIB", "WDSP","PRCP"]

corr15_1 = pd.DataFrame(corr_mat15_1, columns= Cols, index = Index)
corr15_2 = pd.DataFrame(corr_mat15_2, columns= Cols, index = Index)
corr15_3 = pd.DataFrame(corr_mat15_3, columns= Cols, index = Index)
corr15_4 = pd.DataFrame(corr_mat15_4, columns= Cols, index = Index)

corr16_1 = pd.DataFrame(corr_mat16_1, columns= Cols, index = Index)
corr16_2 = pd.DataFrame(corr_mat16_2, columns= Cols, index = Index)
corr16_3 = pd.DataFrame(corr_mat16_3, columns= Cols, index = Index)
corr16_4 = pd.DataFrame(corr_mat16_4, columns= Cols, index = Index)

font = {'family' : 'normal',
    'size' : 48}


#*************15 -1**************
mask = np.zeros_like(corr15_1, dtype=np.bool)
mask[np.triu_indices_from(mask)] = True
f, ax = plt.subplots(figsize=(11, 9))
cmap = sns.diverging_palette(220, 10, as_cmap=True)
ax15_1 = sns.heatmap(corr15_1.T,mask=mask, cmap=cmap,vmax=.3,square=True,linewidths=.5,ax = ax)
ax.set_title('Correlations for year 2015 Spring (Mar. to May.)')


# turn the axis label
for item in ax15_1.get_yticklabels():
    item.set_rotation(0)

for item in ax15_1.get_xticklabels():
    item.set_rotation(45)

plt.rc('font', **font)
ax15_1.get_figure().savefig("/Users/junzhi/Trybigdata/part1/code/DataSummary/deepAnalysis/2015TaxiBikeWeatherSeason_1.png")
sns.plt.close()


#*************15 - 2**************
mask = np.zeros_like(corr15_2, dtype=np.bool)
mask[np.triu_indices_from(mask)] = True
f, ax = plt.subplots(figsize=(11, 9))
cmap = sns.diverging_palette(220, 10, as_cmap=True)
ax15_2 = sns.heatmap(corr15_2.T,mask=mask, cmap=cmap,vmax=.3,square=True,linewidths=.5,ax = ax)
ax.set_title('Correlations for year 2015 Summer (Jun. to Aug.)')


# turn the axis label
for item in ax15_2.get_yticklabels():
    item.set_rotation(0)

for item in ax15_2.get_xticklabels():
    item.set_rotation(45)

plt.rc('font', **font)
ax15_2.get_figure().savefig("/Users/junzhi/Trybigdata/part1/code/DataSummary/deepAnalysis/2015TaxiBikeWeatherSeason_2.png")
sns.plt.close()

#*************15 -3**************
mask = np.zeros_like(corr15_3, dtype=np.bool)
mask[np.triu_indices_from(mask)] = True
f, ax = plt.subplots(figsize=(11, 9))
ax15_3 = sns.heatmap(corr15_3.T,mask=mask, cmap=cmap,vmax=.3,square=True,linewidths=.5,ax = ax)
ax.set_title('Correlations for year 2015 Fall (Sep. to Nov)')


# turn the axis label
for item in ax15_3.get_yticklabels():
    item.set_rotation(0)

for item in ax15_3.get_xticklabels():
    item.set_rotation(45)

plt.rc('font', **font)
ax15_3.get_figure().savefig("/Users/junzhi/Trybigdata/part1/code/DataSummary/deepAnalysis/2015TaxiBikeWeatherSeason_3.png")
sns.plt.close()


#*************15 - 4**************
mask = np.zeros_like(corr15_4, dtype=np.bool)
mask[np.triu_indices_from(mask)] = True
f, ax = plt.subplots(figsize=(11, 9))
ax15_4 = sns.heatmap(corr15_4.T,mask=mask, cmap=cmap,vmax=.3,square=True,linewidths=.5,ax = ax)
ax.set_title('Correlations for year 2015 Winter (Dec. to Feb.)')


# turn the axis label
for item in ax15_4.get_yticklabels():
    item.set_rotation(0)

for item in ax15_4.get_xticklabels():
    item.set_rotation(45)

plt.rc('font', **font)
ax15_4.get_figure().savefig("/Users/junzhi/Trybigdata/part1/code/DataSummary/deepAnalysis/2015TaxiBikeWeatherSeason_4.png")
sns.plt.close()

#*************16 -1**************
mask = np.zeros_like(corr16_1, dtype=np.bool)
mask[np.triu_indices_from(mask)] = True
f, ax = plt.subplots(figsize=(11, 9))
cmap = sns.diverging_palette(220, 10, as_cmap=True)

ax16_1 = sns.heatmap(corr16_1.T,mask=mask, cmap=cmap,vmax=.3,square=True,linewidths=.5,ax = ax)
ax.set_title('Correlations for year 2016 Spring (Mar. to May.)')


# turn the axis label
for item in ax16_1.get_yticklabels():
    item.set_rotation(0)

for item in ax16_1.get_xticklabels():
    item.set_rotation(45)

plt.rc('font', **font)
ax16_1.get_figure().savefig("/Users/junzhi/Trybigdata/part1/code/DataSummary/deepAnalysis/2016TaxiBikeWeatherSeason_1.png")
sns.plt.close()


#*************16 - 2**************
mask = np.zeros_like(corr16_2, dtype=np.bool)
mask[np.triu_indices_from(mask)] = True
f, ax = plt.subplots(figsize=(11, 9))
cmap = sns.diverging_palette(220, 10, as_cmap=True)

ax16_2 = sns.heatmap(corr16_2.T,mask=mask, cmap=cmap,vmax=.3,square=True,linewidths=.5,ax = ax)
ax.set_title('Correlations for year 2016 Summer (Jun. to Aug.)')


# turn the axis label
for item in ax16_2.get_yticklabels():
    item.set_rotation(0)

for item in ax16_2.get_xticklabels():
    item.set_rotation(45)

plt.rc('font', **font)
ax16_2.get_figure().savefig("/Users/junzhi/Trybigdata/part1/code/DataSummary/deepAnalysis/2016TaxiBikeWeatherSeason_2.png")
sns.plt.close()

#*************16 -3**************
mask = np.zeros_like(corr16_3, dtype=np.bool)
mask[np.triu_indices_from(mask)] = True
f, ax = plt.subplots(figsize=(11, 9))
cmap = sns.diverging_palette(220, 10, as_cmap=True)

ax16_3 = sns.heatmap(corr16_3.T,mask=mask, cmap=cmap,vmax=.3,square=True,linewidths=.5,ax = ax)
ax.set_title('Correlations for year 2016 Fall (Sep. to Nov.)')


# turn the axis label
for item in ax16_3.get_yticklabels():
    item.set_rotation(0)

for item in ax16_3.get_xticklabels():
    item.set_rotation(45)

plt.rc('font', **font)
ax16_3.get_figure().savefig("/Users/junzhi/Trybigdata/part1/code/DataSummary/deepAnalysis/2016TaxiBikeWeatherSeason_3.png")
sns.plt.close()


#*************16 - 4**************
mask = np.zeros_like(corr16_4, dtype=np.bool)
mask[np.triu_indices_from(mask)] = True
f, ax = plt.subplots(figsize=(11, 9))
ax16_4 = sns.heatmap(corr16_4.T,mask=mask, cmap=cmap,vmax=.3,square=True,linewidths=.5,ax = ax)
ax.set_title('Correlations for year 2016 Winter (Dec. to Feb.)')


# turn the axis label
for item in ax16_4.get_yticklabels():
    item.set_rotation(0)

for item in ax16_4.get_xticklabels():
    item.set_rotation(45)

plt.rc('font', **font)
ax16_4.get_figure().savefig("/Users/junzhi/Trybigdata/part1/code/DataSummary/deepAnalysis/2016TaxiBikeWeatherSeason_4.png")
sns.plt.close()