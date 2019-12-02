from pyspark import SparkContext, SparkConf
import numpy as np
import time


conf = SparkConf().setAppName("KNN")
sc = SparkContext(conf=conf)

start = time.time()
def distances (dt):
    idts = dt[1]
    ts = list(map(float, dt[0][0:11]))
    cltr = int(dt[0][11])
    
    
    x = [(float('inf'),0)]*k.value
    
    for i in tr:
        if not idts == i[1]:
            dist = sum((p-q)**2 for p, q in zip(i[0][0:11], ts)) ** .5
            for j in range(len(x)):
                if dist < x[j][0]:
                    for z in range(len(x)-1,j,-1):
                        x[z]=x[z-1]
                    x[j]=(dist,i[0][11])
                    break
                
    return (cltr,x)


def guess_class(dt):
    rclass = dt[0]
    freq = 0
    predict = 0
    for i in range(len(dt[1])):
        tfreq = 1
        tpredict = dt[1][i][1]
        for j in range(i+1,len(dt[1])):
            if tpredict == dt[1][j][1]:
                tfreq +=1
        if tfreq > freq:
            predict = tpredict
            freq = tfreq
    return (rclass,predict)

def correct(dt):
    if dt[0]==dt[1]:
        return 1
    else:
        return 0


ts = sc.textFile("datasets/medium.txt").zipWithUniqueId()\
    .map(lambda line: (line[0].split(','), line[1]))\
    .map(lambda line: (list(map(float, line[0])),line[1]))

tr = ts.collect()
k = sc.broadcast(5)


k_vals = ts.map(distances)
guess_class = k_vals.map(guess_class)
correct = guess_class.map(correct)
accuracy = correct.mean()
end = time.time()
print('The time to run is:', end - start)
print('accuracy')