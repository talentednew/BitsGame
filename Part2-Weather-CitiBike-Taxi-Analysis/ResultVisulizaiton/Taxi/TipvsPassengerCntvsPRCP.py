#CORELATION HEATMAP OF PASSENGER COUNT, PRECIPATATION AND TIP FRACTION
#This program was executed by python 2.7
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.sql.types import DoubleType,FloatType, IntegerType,DateType


TipFravsPRCPvsPcnt15=spark.read.option("header",True).csv("/Users/junzhi/Trybigdata/part1/code/DataSummary/deepAnalysis/TipFravsPRCPvsPcnt15.csv")
TipFravsPRCPvsPcnt16=spark.read.option("header",True).csv("/Users/junzhi/Trybigdata/part1/code/DataSummary/deepAnalysis/TipFravsPRCPvsPcnt16.csv")

TipFravsPRCPvsPcnt15 = TipFravsPRCPvsPcnt15.withColumn("PRCP", TipFravsPRCPvsPcnt15["PRCP"].cast(FloatType())).withColumn("avg(tipFraction)", TipFravsPRCPvsPcnt15["avg(tipFraction)"].cast(FloatType())).withColumn("passenger_count", TipFravsPRCPvsPcnt15["passenger_count"].cast(IntegerType()))

TipFravsPRCPvsPcnt16 = TipFravsPRCPvsPcnt16.withColumn("PRCP", TipFravsPRCPvsPcnt16["PRCP"].cast(FloatType())).withColumn("avg(tipFraction)", TipFravsPRCPvsPcnt16["avg(tipFraction)"].cast(FloatType())).withColumn("passenger_count", TipFravsPRCPvsPcnt16["passenger_count"].cast(IntegerType()))

features15 = TipFravsPRCPvsPcnt15.rdd.map(lambda row: row[0:])
features16 = TipFravsPRCPvsPcnt16.rdd.map(lambda row: row[0:])

corrTip_15 =Statistics.corr(features15, method="pearson")
corrTip_16 =Statistics.corr(features16, method="pearson")

font = {'family' : 'normal','size' : 36}

sns.set(style="white")
Index= ["PRCP", "SharePeopleCount","tipFraction"]
Cols = ["PRCP", "SharePeopleCount","tipFraction"]
corrTip15 = pd.DataFrame(corrTip_15, columns= Cols, index = Index)
corrTip16 = pd.DataFrame(corrTip_16, columns= Cols, index = Index)

# Generate a mask for the upper triangle
mask = np.zeros_like(corrTip15, dtype=np.bool)
mask[np.triu_indices_from(mask)] = True

# Set up the matplotlib figure
f, ax = plt.subplots(figsize=(11, 9))

# Generate a custom diverging colormap
cmap = sns.diverging_palette(220, 10, as_cmap=True)

#15
axtip15 = sns.heatmap(corrTip15.T,mask=mask, cmap=cmap,vmax=.3,square=True,linewidths=.5,ax = ax)
ax.set_title('Correlations of NYCTaxis TipFraction, Rider count and precipitation in 2015')

# turn the axis label
for item in axtip15.get_yticklabels():
    item.set_rotation(0)

for item in axtip15.get_xticklabels():
    item.set_rotation(45)

plt.rc('font', **font)

axtip15.get_figure().savefig("/Users/junzhi/Trybigdata/part1/code/DataSummary/deepAnalysis/2015Fractuib_pcnt_PRCP.png")
#sns.plt.show()
sns.plt.close()

#16
f, ax = plt.subplots(figsize=(11, 9))

axtip16 = sns.heatmap(corrTip16.T,mask=mask, cmap=cmap,vmax=.3,square=True,linewidths=.5,ax = ax)

ax.set_title('Correlations of NYCTaxis TipFraction, Rider count and precipitation in 2016')

# turn the axis label
for item in axtip16.get_yticklabels():
    item.set_rotation(0)

for item in axtip16.get_xticklabels():
    item.set_rotation(45)

plt.rc('font', **font)

axtip16.get_figure().savefig("/Users/junzhi/Trybigdata/part1/code/DataSummary/deepAnalysis/2016Fractuib_pcnt_PRCP.png")
sns.plt.close()
