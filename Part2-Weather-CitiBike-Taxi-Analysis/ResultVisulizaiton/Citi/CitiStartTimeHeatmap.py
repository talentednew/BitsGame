#HEATMAP OF COUNTS OF PICKUPS, GROUPBY DAY OF WEEK AND HOUR
#This program was executed by python 2.7
import pandas as pd
import seaborn as sns; sns.set()
import numpy as np; np.random.seed(0)
import matplotlib.pyplot as plt
df = pd.read_csv('/Users/Arafat/Desktop/citiData/citi_week_hour_count.csv')
counts = df.pivot("dayofweek", "hour", "count")
ax = sns.heatmap(counts,cmap='YlOrRd')
ax.set_title('Number of Using of CitiBikes in New York City from 2015 to 2016, by Time of Using CitiBike')
ax.set_xlabel('Hour of Starting to Use (Local Time)')
ax.set_ylabel('Day of Week of Starting to Use')
sns.plt.show()