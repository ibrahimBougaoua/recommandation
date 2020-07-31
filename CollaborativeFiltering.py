import findspark
findspark.find()
findspark.init()

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import *
from pyspark.sql.functions import lit, col
from pyspark.ml.tuning import TrainValidationSplit, ParamGridBuilder
from pyspark import SparkContext, SparkConf
from initialisation import init


class Als_movie:
		
	def __init__(self):

		self.spark=init.spark
		self.data = init.movie_ratings.select("userId","movieId","rating")
		self.datafinal=init.als_recomendation_movies
		if self.datafinal.count()<=0 :
			self.datafinal=self.train()
			

	def train(self):

		als = ALS( maxIter=5,regParam=0.13,userCol="userId", itemCol="movieId", ratingCol="rating",coldStartStrategy="drop")
		self.MoviesModel = als.fit(self.data)
		userRecs = self.MoviesModel.recommendForAllUsers(10)
		
		return userRecs
		
	def save(self):
		self.datafinal.withColumn("recommendations", col("recommendations").cast("string")).toPandas().to_csv('file:///home/hadoop_user/Desktop/PFE/datasets/movies/als_recomendation_movies.csv', index=False)
    
	def RecommendForUserById(self,userId):
		
			
		userRecs = self.datafinal.filter(self.datafinal['userId'] == userId).collect()
		userRecs = userRecs[0]
		
		return self.Collections(userRecs[1])
	def Collections(self,data):
		Collection = []
		for rows in data:
			Collection.append(rows[0])
		return Collection
	def retrain(self,userID,list):
		predictions = self.MoviesModel.transform(list)



		


class Als_book:
		
	def __init__(self):

		self.spark=init.spark
		 
		
		self.data = init.book_ratings.select("userId","book_id","ratings")
		self.datafinal=init.als_recomendation_movies
		if self.datafinal.count()<=0 :
			self.datafinal=self.train()
			

	def train(self):

		als = ALS( maxIter=5,regParam=0.13,userCol="userId", itemCol="book_id", ratingCol="ratings",coldStartStrategy="drop")
		self.BookModel = als.fit(self.data)
		userRecs = self.BookModel.recommendForAllUsers(30)
		
		return userRecs
		
	def save(self):
		self.datafinal.withColumn("recommendations", col("recommendations").cast("string")).toPandas().to_csv('file:///home/hadoop_user/Desktop/PFE/datasets/movies/als_recomendation_books.csv', index=False)
    
	def RecommendForUserById(self,userId):
		
			
		userRecs = self.datafinal.filter(self.datafinal['userId'] == userId).collect()
		
		userRecs = userRecs[0]
		
		return self.Collections(userRecs[1])
	def Collections(self,data):
		Collection = []
		for rows in data:
			Collection.append(rows[0])
		return Collection



































