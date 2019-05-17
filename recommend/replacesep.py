import pandas as pd

user_info = pd.read_table('./users.dat',sep="::")
##不保存行索引  不保存列名
user_info.to_csv('./user_info.csv',sep=";",header=0,index=0)