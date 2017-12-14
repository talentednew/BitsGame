#HEATMAP OF COUNTS OF PICKUPS, GROUPBY DAY OF WEEK AND HOUR
#This program was executed by python 2.7
import pandas as pd
import seaborn as sns; sns.set()
import numpy as np; np.random.seed(0)
import matplotlib.pyplot as plt
df = pd.read_csv('/Users/Arafat/Desktop/datasets/2015_2016_week_hour_count.csv')
counts = df.pivot("dayofweek", "hour", "Count")
ax = sns.heatmap(counts)
ax.set_title('Number of Pickups of Taxis in New York City from 2015 to 2016, by Time of Pickup')
ax.set_xlabel('Hour of Pickup(Local Time)')
ax.set_ylabel('Day of Week of Pickup')
sns.plt.show()

