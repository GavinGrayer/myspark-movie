import pandas as pd

movies = pd.read_table('./movies.dat',sep="::")
ratings = pd.read_table('./ratings.dat',sep="::")
#movie_info = pd.merge(movies,ratings)
user_movie_info = pd.merge(ratings,movies)
#movie_info.drop('timestamp',1,inplace=True)
##不保存行索引  不保存列名
# movie_info.to_csv('./movie_time_info.csv',sep=";",header=0,index=0)
user_movie_info.to_csv('./user_movie_info.csv',sep=";")