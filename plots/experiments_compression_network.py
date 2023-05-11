import pandas as pd
import matplotlib.pyplot as plt

#df = pd.read_csv('compression_runs.csv')

df = pd.DataFrame({

    'nocomp': [11461, 10745, 22847, 38869, 82418],
    'snappy (2:1)': [10897, 11349, 11066, 20204, 42546],
    'lzo (1:1)': [12405,13509, 19737, 38023, 82633],
    'lz4 (2:1)': [11346, 11079, 10967, 19783, 43599],

}, index=[100, 50, 25, 13, 6])

lines = df.plot.line(marker='x')

plt.title('Data transfer w/ different compression libs (lineitem 10m rows)')
plt.ylabel('Runtime (ms)')
plt.xlabel('Network Bandwidth (mbit)')
plt.tight_layout()
plt.savefig('experiments_compression.pdf')
