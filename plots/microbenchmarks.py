import matplotlib.pyplot as plt


labels = ['PG row', 'PG col', 'CH row', 'CH col']
read = [66, 70, 43, 27]
write = [100, 110, 9, 8]

width = 0.35       # the width of the bars: can also be len(x) sequence

fig, ax = plt.subplots()

ax.bar(labels, read, width, label='Read', color='pink')
ax.bar(labels, write, width, bottom=read, label='Write', color='purple')

ax.set_ylabel('Runtime (s)')
ax.set_title('I/O Performance with row/col layouts')
ax.legend()

plt.savefig('microbenchmarks.pdf')