#HEXBIN MAP OF SUM OF TOTAL AMOUNTS AND PICKUP LOCATIONS
#This program was executed by python 2.7 and jyputer notebook
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
df = pd.read_csv('/Users/Arafat/Desktop/pu_do_sum_4000w.csv')
xmin = -74.1
xmax = -73.7
ymin = 40.58
ymax = 40.90
fig, axs = plt.subplots(ncols=1,figsize=(25, 25))
fig.subplots_adjust(hspace=0.5, left=0.07, right=0.93)
ax = axs
hb = ax.hexbin(df.pickup_longitude, df.pickup_latitude, df.totalAmountSum, bins = 'log', gridsize=1000, cmap='inferno')
ax.axis([xmin, xmax, ymin, ymax])
ax.set_title("Sum of Total Income Amounts of Yellow Taxi in New York City from 2015 to 2016, by Pick Up Location", fontsize=20)
cb = fig.colorbar(hb, ax=ax)
cb.set_label('log(Sum of Total Amount)', fontsize=20)

plt.show()