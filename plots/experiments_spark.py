import numpy as np
import matplotlib.pyplot as plt

#fig = plt.figure(figsize=(5, 5))
labels = ['10^6', '10^7']
xdbc = [15, 25]
jdbc = [15, 43]

barWidth = 0.2  # the width of the bars

r1 = np.arange(len(labels))
r2 = [x + barWidth for x in r1]

plt.bar(r1, xdbc, color='white', width=barWidth, edgecolor='black', label='XDBC')
plt.bar(r2, jdbc, color='lightgray', width=barWidth, edgecolor='black', label='JDBC')

# Add some text for labels, title and custom x-axis tick labels, etc.
plt.ylabel('Runtime (s)')
plt.xlabel('#Tuples')
# ax.set_title('Scores by group and gender')
plt.xticks([r + barWidth for r in range(len(labels))], labels)
plt.legend(ncol=2)
plt.title('Read Spark RDD from PostgreSQL')

plt.tick_params(
    axis='x',  # changes apply to the x-axis
    which='both',  # both major and minor ticks are affected
    bottom=False,  # ticks along the bottom edge are off
    top=False,  # ticks along the top edge are off
    labelbottom=True)  # labels along the bottom edge are off

plt.tight_layout()

plt.savefig('experiments_spark.pdf')
