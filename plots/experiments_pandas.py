import numpy as np
import matplotlib.pyplot as plt

# fig = plt.figure(figsize=(5, 5))
labels = ['10^6', '10^7', '10^8']
xdbc = [1, 8, 99]
connectorx = [1, 8, 85]
turbodbc = [2.4, 22, 0]
psycopg2 = [5.6, 50, 0]
sqlalchemy = [7.5, 81, 0]

barWidth = 0.15  # the width of the bars

r1 = np.arange(len(labels))
r2 = [x + barWidth for x in r1]
r3 = [x + barWidth for x in r2]
r4 = [x + barWidth for x in r3]
r5 = [x + barWidth for x in r4]

plt.bar(r1, xdbc, color='white', width=barWidth, edgecolor='black', label='XDBC')
plt.bar(r2, connectorx, color='lightgray', width=barWidth, edgecolor='black', label='connectorX')
plt.bar(r3, turbodbc, color='gray', width=barWidth, edgecolor='black', label='turbodbc')
plt.bar(r4, psycopg2, color='darkgray', width=barWidth, edgecolor='black', label='psycopg2')
plt.bar(r5, sqlalchemy, color='black', width=barWidth, edgecolor='black', label='sqlalchemy')

# Add some text for labels, title and custom x-axis tick labels, etc.
plt.title('Read Pandas dataframe from PostgreSQL')
plt.ylabel('Runtime (s)')
plt.xlabel('#Tuples')
plt.xticks([r + barWidth for r in range(len(labels))], labels)
plt.legend(ncol=2)

plt.tick_params(
    axis='x',  # changes apply to the x-axis
    which='both',  # both major and minor ticks are affected
    bottom=False,  # ticks along the bottom edge are off
    top=False,  # ticks along the top edge are off
    labelbottom=True)  # labels along the bottom edge are off

plt.tight_layout()

plt.savefig('experiments_pandas.pdf')
