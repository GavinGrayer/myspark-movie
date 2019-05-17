num = 0

with open('./ratings.dat','r',encoding='utf-8') as f:
    
    with open('./small.dat','w',encoding='utf-8') as f2:
        line =f.readline()
        line =f.readline()
        while line is not None:
            if num >= 90000:
               break
            f2.write(line)
            line =f.readline()
            num+=1