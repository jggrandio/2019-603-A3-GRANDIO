import time
import sys

start = time.time()
dataset = sys.argv[1]

f = open(dataset,'r')
data = []
k = int(sys.argv[2])
kitems= []
for a in f:
    data.append(list(map(float,a.split(','))))

for i in range(len(data)):
    ts = data [i][0:7]
    cltr = int(data[i][7])
    x = [(float('inf'),0)]*k
    
    for j in range(len(data)):
        if not i==j:
            dist = sum((p-q)*(p-q) for p, q in zip(data[j][0:7], ts))
            if dist < x[len(x)-1][0]:
                for z in range(len(x)):
                    if dist < x[z][0]:
                        x.insert(z,(dist,data[j][7]))
                        x.pop()
                        break
        
    kitems.append(x)

for z in range(len(kitems)):
    freq = 0
    predict = 0
    for i in range(len(kitems[z])):
        tfreq = 1
        tpredict = kitems[z][i][1]
        for j in range(i+1,len(kitems[z])):
            if tpredict == kitems[z][j][1]:
                tfreq +=1
        if tfreq > freq:
            predict = tpredict
            freq = tfreq
    kitems[z]=predict
    
right = 0

for i in range(len(kitems)):
    if kitems[i] == data[i][7]:
        right+=1
accuracy = right/len(kitems)
end = time.time()
print('The time to run is:', end - start)
print(accuracy)
