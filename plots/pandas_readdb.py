import numpy as np
import matplotlib.pyplot as plt

fig = plt.figure(figsize=(5, 5))
labels = ['10^7']
libpq_cp = [11]
libpq_cp_4 = [8]
libpq = [19]
libpqxx = [23]
sqlalchemy = [81]
psycopg2 = [50]
turbodbc = [22]
connectorx = [8]


barWidth = 0.25  # the width of the bars


r1 = np.arange(len(labels))
r2 = [x + barWidth for x in r1]
r3 = [x + barWidth for x in r2]
r4 = [x + barWidth for x in r3]
r5 = [x + barWidth for x in r4]
#r6 = [x + barWidth for x in r5]
#r7 = [x + barWidth for x in r6]
#r8 = [x + barWidth for x in r7]


#plt.bar(r1, libpq_cp, color='white', width=barWidth, edgecolor='black', label='XDBC [libpq_cp]')
plt.bar(r1, libpq_cp_4, color='white', width=barWidth, edgecolor='black', label='XDBC')
#plt.bar(r3, libpq, color='white', hatch='//', width=barWidth, edgecolor='black', label='XDBC [libpq]')
#plt.bar(r4, libpqxx, color='white', hatch='\\', width=barWidth, edgecolor='black', label='XDBC [libpqxx]')
plt.bar(r2, connectorx, color='lightgray', width=barWidth, edgecolor='black', label='connectorX')
plt.bar(r3, turbodbc, color='gray', width=barWidth, edgecolor='black', label='turbodbc')
plt.bar(r4, psycopg2, color='darkgray', width=barWidth, edgecolor='black', label='psycopg2')
plt.bar(r5, sqlalchemy, color='black', width=barWidth, edgecolor='black', label='sqlalchemy')


# Add some text for labels, title and custom x-axis tick labels, etc.
plt.ylabel('Runtime (s)')
plt.xlabel('#Tuples')
#ax.set_title('Scores by group and gender')
plt.xticks([r + barWidth for r in range(len(labels))], labels)
plt.legend(ncol=2)

plt.tick_params(
    axis='x',          # changes apply to the x-axis
    which='both',      # both major and minor ticks are affected
    bottom=False,      # ticks along the bottom edge are off
    top=False,         # ticks along the top edge are off
    labelbottom=False) # labels along the bottom edge are off


plt.tight_layout()

plt.savefig('experiments_pandas.pdf')