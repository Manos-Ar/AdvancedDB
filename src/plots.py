import matplotlib.pyplot as plt

labels = ['RDD', 'SQL-CSV', 'SQL-PAR']
rdd = []
sql_csv = []
sql_par = []

i=0
with open('times.txt') as file:
    for line in file:
        if(i==0):
            rdd.append(eval(line))
        if(i==1):
            sql_csv.append(eval(line))
        if(i==2):
            sql_par.append(eval(line))
            i=-1
        i+=1


for i in range(5):
    temp = []
    temp.append(rdd[i])
    temp.append(sql_csv[i])
    temp.append(sql_par[i])

    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.set_title('Q{}'.format(i+1))
    ax.bar(labels,temp)
    #plt.savefig("Q{}_time_plot".format(i))
    plt.show()
