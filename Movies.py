


from initialisation import init
import pandas as pd
import numpy as np



class Movies:

	def __init__(self):
		self.spark = init.spark
		self.movies=init.movies.toPandas()
		self.qualified=init.qualified_movies
		if self.qualified.count()<=0 :
			self.qualified=self.build_datast_Movies_IMDN()
		else:
			self.qualified=self.qualified.toPandas()
		self.qualified["title"]=self.qualified["title"].str.upper()
		self.qualified["actors"]=self.qualified["actors"].str.upper()
		self.qualified["genre"]=self.qualified["genre"].str.upper()
		self.qualified["director"]=self.qualified["director"].str.upper()	

	def build_datast_Movies_IMDN(self):
		vote_counts = self.movies[self.movies['vote_count'].notnull()]['vote_count'].astype('int')
		vote_averages = self.movies[self.movies['vote_average'].notnull()]['vote_average'].astype('float')
		self.C = vote_averages.mean()
		self.m = vote_counts.quantile(0.95)
		self.movies['year'] = pd.to_datetime(self.movies['released'], errors='coerce').apply(lambda x: str(x).split('-')[0] if x != np.nan else np.nan)
		self.movies['vote_count']=self.movies[self.movies['vote_count'].notnull()]['vote_count'].astype('int')
		qualified = self.movies[(self.movies['vote_count'] >= self.m) & (self.movies['vote_count'].notnull()) & (self.movies['vote_average'].notnull())][['movieId','title','vote_count', 'vote_average','genre','actors','director']]
		qualified['vote_count'] = qualified['vote_count'].astype('int')
		qualified['vote_average'] = qualified['vote_average'].astype('float')
		qualified['wr'] = qualified.apply(self.weighted_rating, axis=1)
		qualified = qualified.sort_values('wr', ascending=False)
		qualified["title"]=self.qualified["title"].str.upper()
		qualified["actors"]=self.qualified["actors"].str.upper()
		qualified["genre"]=self.qualified["genre"].str.upper()
		qualified["director"]=self.qualified["director"].str.upper()
		self.spark.createDataFrame(qualified).write.format('csv').option('header',False).mode('overwrite').option('sep',',').save('file:///home/hadoop_user/Desktop/PFE/datasets/movies/qualified.csv')
		return qualified

	def weighted_rating(self,x):
		v = x['vote_count']
		R = x['vote_average']
		return (v/(v+self.m) * R) + (self.m/(self.m+v) * self.C)

	def SearchingByTitle(self,title):
		Movies = self.qualified[self.qualified['title'].str.contains(title.upper())]['movieId']
		try:
			movie=Movies.apply(pd.Series).stack().tolist()
		except Exception as e:
			movie=[]
		return movie	

	def SearchingActor(self,actor):
		Movies = self.qualified[self.qualified['actors'].str.contains(actor.upper())]['movieId']
		try:
			movie=Movies.apply(pd.Series).stack().tolist()
		except Exception as e:
			movie=[]
		return movie
	def getMoviesByGenre(self,genre):
		Movies = self.qualified[self.qualified['genre'].str.contains(genre.upper())]['movieId']
		try:
			movie=Movies.apply(pd.Series).stack().tolist()
		except Exception as e:
			movie=[]
		return movie	

	def getMoviesByDirector(self,director):
		Movies = self.qualified[self.qualified['director'].str.contains(director.upper())]['movieId']
		try:
			movie=Movies.apply(pd.Series).stack().tolist()
		except Exception as e:
			movie=[]
		return movie	

	
	def getMoviesFromIds(self,movieIds):
		movies=[]
		for movieId in movieIds :
			movies=movies+(self.movies[self.movies['movieId']==movieId].values.tolist())
		return movies	
	def qualified1(self):
		return self.qualified['movieId'].values.tolist()

	
	def getGenre(self,Movie_id):
		movie=self.movies[self.movies['movieId']==str(Movie_id)]
		return movie["genre"]	
















