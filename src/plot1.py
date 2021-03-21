#!/bin/python3
import matplotlib.pyplot as plt
import numpy as np

labels = ['RDD', 'SQL-CSV', 'SQL-PAR']

times={}


with open('outputs/times_1st.txt') as file:
    for line in file:
        tokens=line.split()
        time = float(tokens[1])*60
        name = tokens[0].split("-")[0]
        # print(name)
        if (name in times):
            times[name].append(time)
        else:
            times[name]=[time]

# print(times)
x_ticks = np.arange(0, len(labels), 1)

for querys in times.items():
    query = querys[0]
    y_Axis = querys[1]
    fig, ax = plt.subplots()
    ax.set_facecolor('#f2f5f0')
    ax.set_ylabel("Times(s)")
    ax.set_xlabel("Type")
    ax.set_xlim(-0.5, len(labels) - 0.5)
    ax.xaxis.set_ticks(x_ticks)
    ax.bar(x_ticks,y_Axis)
    ax.set_xticklabels(labels, rotation=0)
    plt.title(query)
    plt.savefig("plot_times_" + str(query) + ".png",bbox_inches="tight")



