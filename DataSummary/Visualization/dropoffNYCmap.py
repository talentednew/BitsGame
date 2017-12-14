#DROPOFF LOCATIONS NYC MAP
#This map was plotted with jyputer nodeboke and python 2.7
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
from matplotlib import rcParams

#Inline Plotting for Ipython Notebook
%matplotlib inline
df=pd.read_csv('/Users/Arafat/Desktop/1to18_pu_do_am_4000w.csv')
pd.options.display.mpl_style = 'default' #Better Styling
new_style = {'grid': False} #Remove grid
matplotlib.rc('axes', **new_style)
rcParams['figure.figsize'] = (17, 17) #Size of figure
rcParams['figure.dpi'] = 250
P=df.plot(kind='scatter', x='dropoff_longitude', y='dropoff_latitude',color='pink',xlim=(-74.06,-73.77),ylim=(40.61, 40.91),s=.02,alpha=.6)
P.set_axis_bgcolor('black') #Background Color