from pyspark import SparkContext, SparkConf
import numpy as np
import time


conf = SparkConf().setAppName("KNN")
sc = SparkContext(conf=conf)


start = time.time()


def distances (dt):
    idts = dt[0][1]
    test = dt[0][0].split(",")
    ts = list(map(float, test[0:11]))
    clts = int(test[11])
    idtr = dt[1][1]
    train = dt[1][0].split(",")
    tr = list(map(float, train[0:11]))
    cltr = int(train[11])
    
    dist = sum((p-q)**2 for p, q in zip(ts, tr)) ** .5
    
    if idts == idtr:
        dist = float("inf")
    
    return (idts,(dist,cltr,clts))
    
def create_klist(value):
    x = [(float('inf'),0)]*k.value
    x[0] = value
    return x

def merge_klist(x, value):
    for i in range(len(x)):
        if value[0]<x[i][0]:
            for j in range(len(x)-1,i,-1):
                x[j]=x[j-1]
            x[i]=value
            break
    return x

def merge_combiners(x,y):
    l = 0
    for i in range(len(x)):
        if y[l][0]<x[i][0]:
            for j in range(len(x)-1,i,-1):
                x[j]=x[j-1]
            x[i]=y[l]
            l+=1
    return x

def guess_class(dt):
    rclass = dt[1][0][2]
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

tr = sc.textFile("datasets/medium.txt").zipWithUniqueId()
k = sc.broadcast(1)

cart = tr.cartesian(tr)




dist = cart.map(distances).collect()
end = time.time()
print('The time to run is:', end - start)
'''
k_vals = dist.combineByKey(create_klist, merge_klist, merge_combiners)
guess_class = k_vals.map(guess_class)



def correct(dt):
    if dt[0]==dt[1]:
        return 1
    else:
        return 0
    
correct = guess_class.map(correct)
accuracy = correct.mean()
print(accuracy)
'''