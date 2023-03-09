import numpy as np
import matplotlib.pyplot as plt

labels = ['10^7']
xdbc = [21]
jdbc = [43]


barWidth = .2  # the width of the bars

r1 = np.arange(len(labels))
r2 = [x + barWidth for x in r1]

bar_xdbc = plt.bar(r1, xdbc, color='white', width=barWidth, edgecolor='black', label='XDBC')
bar_jdbc = plt.bar(r2, jdbc, color='black', width=barWidth, edgecolor='black', label='JDBC')
#bar_xda = plt.bar(r2, libpq, color='lightgray', width=barWidth, edgecolor='black', label='XDBC [libpq]')
#bar_garlic = plt.bar(r3, libpqxx, color='gray', width=barWidth, edgecolor='black', label='XDBC [libpqxx]')
#bar_trino = plt.bar(r4, jdbc, color='#decbe4', width=barWidth, edgecolor='black', label='JDBC')



# Add some text for labels, title and custom x-axis tick labels, etc.
plt.ylabel('Runtime (s)')
#plt.xlabel('Tuples')
#ax.set_title('Scores by group and gender')
#plt.xticks([r + barWidth for r in range(len(labels))], labels)
plt.xticks([])
plt.legend()


plt.tight_layout()

plt.savefig('experiments_spark.pdf')