#!/bin/python3
import matplotlib.pyplot as plt
import numpy as np

labels = ['Repartition', 'Repartition-100','Broadcast', 'Broadcast-100']

times=[]


with open('outputs/times_2nd.txt') as file:
    for line in file:
        tokens=line.split()
        print(tokens)
        time = float(tokens[1])
        times.append(time)

print(times)
x_ticks = np.arange(0, len(labels), 1)

# for querys in times.items():
# query = querys[0]
y_Axis = times
fig, ax = plt.subplots()
ax.set_facecolor('#f2f5f0')
ax.set_ylabel("Times(s)")
ax.set_xlabel("Type")
ax.set_xlim(-0.5, len(labels) - 0.5)
ax.xaxis.set_ticks(x_ticks)
ax.bar(x_ticks,y_Axis)
ax.set_xticklabels(labels, rotation=0)
plt.title("Repartition & Broadcast")
plt.savefig("plot_times_100.png",bbox_inches="tight")

####################

labels = ['SQL-Not-Optimized','SQL-Optimized']

times=[]


with open('outputs/times_2nd_sql.txt') as file:
    for line in file:
        tokens=line.split()
        time = float(tokens[2])
        times.append(time)

print(times)
x_ticks = np.arange(0, len(labels), 1)

# for querys in times.items():
# query = querys[0]
y_Axis = times
fig, ax = plt.subplots()
ax.set_facecolor('#f2f5f0')
ax.set_ylabel("Times(s)")
ax.set_xlabel("Type")
ax.set_xlim(-0.5, len(labels) - 0.5)
ax.xaxis.set_ticks(x_ticks)
ax.bar(x_ticks,y_Axis)
ax.set_xticklabels(labels, rotation=0)
plt.title("SQL OptimizedSQL vs Not-Optimized")
plt.savefig("plot_times_sql.png",bbox_inches="tight")
