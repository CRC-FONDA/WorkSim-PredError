import os
import sys
import seaborn as sns
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
from PyPDF2 import PdfFileMerger

files = []

def get_scheduler_count(lines: list[str]):
    first_scheduler_name = lines[1].split(',')[5]
    count = 1
    for line in lines[2:]:
        element = line.split(',')[5]
        if element == first_scheduler_name:
            break
        count += 1
    return count


#sns.set(rc={'figure.figsize':(12,8)})

for u in range(len(sys.argv) - 1):
    print(str(u + 1) + '/' + str(len(sys.argv) - 1))
    filename = sys.argv[u + 1]

    if not (os.path.isfile(filename)):
        print('file ' + filename + ' does not exist')
    else:

        with open(filename) as ff:
            lines = ff.readlines()

            
            
            #first_scheduler = lines[]
            

        #read number of seeds
        #num_seeds = int(lines[len(lines) - 1].split(',')[1]) + 1

        #calculate number of schedulers
        num_schedulers = get_scheduler_count(lines)

        order = []
        for i in range(num_schedulers):
            order.append(str(lines[i + 1].split(',')[5]))

        numbernodes = lines[1:][0].split(',')[3]

        #temp
        #order[0] = ['SCALING']

        title = ''
        for s in ['chipseq', 'methylseq', 'eager', 'viralrecon', 'sarek']:
            if (s in filename):
                title = 'Workflow: ' + s
                print(s)
        title = title + ', VMs: ' + str(numbernodes)# + ', Runs: ' + str(num_seeds)

        results = pd.read_csv(filename, index_col=False)




        plt.figure(figsize=(5,4))
        #plt.figure(figsize=(10,8))

        #hide ax labels
        ax1 = plt.axes()
        x_axis = ax1.axes.get_xaxis()
        x_axis.label.set_visible(False)
        y_axis = ax1.axes.get_yaxis()
        y_axis.label.set_visible(False)

        ax = sns.boxplot(x="Scheduler", y="Runtime", data=results, order=order)

        ax.set_title(title)
        ax.set_xticklabels(ax.get_xticklabels(),rotation=90)

        #print(sns.__file__)

        #outliers = [flier.get_ydata() for flier in bp["fliers"]]
        #boxes = [box.get_ydata() for box in bp["boxes"]]
        #medians = [median.get_ydata() for median in bp["medians"]]
        #whiskers = [whiskers.get_ydata() for whiskers in bp["whiskers"]]



        plt.savefig(filename + '.pdf', bbox_inches = "tight")

        plt.savefig(filename + '.png', bbox_inches = "tight")

        files.append(filename + '.pdf')
        #files.append(filename + '.png')


merger = PdfFileMerger()
for pdf in files:
    merger.append(pdf)

merger.write('results.pdf')
merger.close()

os.system('xdg-open results.pdf')
