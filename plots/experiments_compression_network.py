import pandas as pd
import matplotlib.pyplot as plt

df1 = pd.read_csv('1684166569_runs_comp.csv')
#df1 = pd.read_csv('1684236574_runs_comp.csv')
'''
df = pd.DataFrame({

    'nocomp': [11, 10, 22, 38, 82],
    'snappy (2:1)': [10, 11, 11, 20, 42],
    'lzo (1:1)': [12,13, 19, 38, 82],
    'lz4 (2:1)': [11, 11, 10, 19, 43],

}, index=[100, 50, 25, 13, 6])
'''



df = pd.DataFrame({
    'nocomp': df1[df1['comp']=='nocomp'].sort_values(by=['network'], ascending=False)['time'].reset_index(drop=True).to_numpy(),
    'snappy (2:1)': df1[df1['comp']=='snappy'].sort_values(by=['network'], ascending=False)['time'].reset_index(drop=True).to_numpy(),
    'lzo (1:1)': df1[df1['comp']=='lzo'].sort_values(by=['network'], ascending=False)['time'].reset_index(drop=True).to_numpy(),
    'lz4 (2:1)': df1[df1['comp']=='lz4'].sort_values(by=['network'], ascending=False)['time'].reset_index(drop=True).to_numpy(),
}, index=[100, 50, 25, 13, 6])

print(df)
lines = df.plot.line(marker='x')
lines.set_ylim(bottom=0)

plt.title('Data transfer, different compression libs, lineitem 10m rows, CPU:.1')
plt.ylabel('Runtime (ms)')
plt.xlabel('Network Bandwidth (mbit)')
plt.tight_layout()
plt.savefig('experiments_compression_throttled_01.pdf')
