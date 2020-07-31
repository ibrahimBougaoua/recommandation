import findspark
findspark.find()
findspark.init()

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import *
from pyspark.sql.functions import lit, col
import json
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from initialisation import init

class KMeans_movies:

	def __init__(self):
		
		self.spark =init.spark
		self.data = init.movie_tags
		self.cluster=init.movie_cluster
		if self.cluster.count()<=0: 
			self.cluster=self.train()


	def train(self):
		assembler = VectorAssembler(inputCols=["FilmNoir","Action","Adventure","Horror","Romance","War","History","Western","Documentary","SciFi","Sport","Drama","Thriller","Music","Crime","News","Fantasy","Biography","Animation","Family","Comedy","Mystery","Musical","Short"],outputCol="features")
		transformdatafinal = assembler.transform(self.data).select("features","movieId")
		Kmeans = KMeans( featuresCol="features",predictionCol="cluster",k=5)
		model = Kmeans.fit(transformdatafinal)
		cluster=model.transform(transformdatafinal).select("movieId","cluster")
		cluster.write.format('csv').option('header',False).mode('overwrite').option('sep',',').save('file:///home/hadoop_user/Desktop/PFE/datasets/movies/movie_cluster.csv')
		return cluster
		

	def Recommended(self,id):
		

		self.cluster.createOrReplaceTempView("clmovie")
		sqlRatings = self.spark.sql("SELECT movieId FROM clmovie WHERE  cluster in ( SELECT cluster FROM clmovie WHERE movieId = %d ) limit 20" % id)
		movieCollection = []

		for row in sqlRatings.collect():
			movieCollection.append(row[0])
		return movieCollection

class KMeans_books:

	def __init__(self):
		
		self.spark =init.spark
		self.data = init.book_tags
		self.cluster=init.book_cluster
		if self.cluster.count()<=0: 
			self.cluster=self.train()


	def train(self):
		assembler = VectorAssembler(inputCols=init.book_header_tags,outputCol="features")
		transformdatafinal = assembler.transform(self.data).select("features","book_id")
		Kmeans = KMeans( featuresCol="features",predictionCol="cluster",k=5)
		model = Kmeans.fit(transformdatafinal)
		cluster=model.transform(transformdatafinal).select("book_id","cluster")
		cluster.write.format('csv').option('header',False).mode('overwrite').option('sep',',').save('file:///home/hadoop_user/Desktop/PFE/datasets/lbooks/book_cluster.csv')
		return cluster
		

	def Recommended(self,id):
		

		self.cluster.createOrReplaceTempView("clbook")
		sqlRatings = self.spark.sql("SELECT bookId FROM clbook WHERE  cluster in ( SELECT cluster FROM clbook WHERE bookId = %d ) limit 20" % id)
		bookCollection = []

		for row in sqlRatings.collect():
			bookCollection.append(row[0])
		return bookCollection




class cosine_movie :
	
	def __init__(self):

		self.spark = init.spark
		self.movies = init.movies
		self.dataPD=self.transormDataSet()
		self.cosine_sim=self.train()
		
		
	def transormDataSet(self) :
		features = ['title','genre','director','writer','actors']
		data=self.movies.toPandas()
		for feature in features:
			data[feature] = data[feature].fillna('')
		data["combined_features"] = data.apply(self.combine_features,axis=1)
		return data

	def train(self):
		cv = CountVectorizer() 
		count_matrix = cv.fit_transform(self.dataPD["combined_features"])	
		cosine_sim = cosine_similarity(count_matrix)
		return cosine_sim
		

	def combine_features(self,row):
		return row['title']+" "+row['writer']+" "+row['genre']+" "+row['director']+" "+row['actors']

	def get_title_from_ID(self,index):
		return self.dataPD[self.dataPD.movieId == index]["title"].values[0]
	def get_ID_from_title(self,title):
		return self.dataPD[self.dataPD.title == title]["movieId"].values[0]

	def cosine_similar(self,id):
		if self.cosine_sim.size==0 :
			self.train()
		similar_movies = list(enumerate(self.cosine_sim[id]))
		sorted_similar_movies = sorted(similar_movies,key=lambda x:x[1],reverse=True)[1:]
		i=0
		similar=[]
		for element in sorted_similar_movies:
			similar.append(element[0])
			i=i+1
			if i>20:
				break
		return similar 	


class cosine_book :
	
	def __init__(self):

		self.spark = init.spark
		self.books = init.books
		self.dataPD=self.transormDataSet()
		self.cosine_sim=self.train()
		if self.cosine_sim.size==0 :
			self.train()
		
		
	def transormDataSet(self) :
		features = ['title','genre','authors','original_title']
		data=self.books.toPandas()
		for feature in features:
			data[feature] = data[feature].fillna('')
		data["combined_features"] = data.apply(self.combine_features,axis=1)
		return data

	def train(self):
		cv = CountVectorizer() 
		count_matrix = cv.fit_transform(self.dataPD["combined_features"])	
		cosine_sim = cosine_similarity(count_matrix)
		return cosine_sim
		

	def combine_features(self,row):
		return row['title']+" "+row['authors']+" "+row['genre']+" "+row['original_title']

	def get_title_from_ID(self,index):
		return self.dataPD[self.dataPD.book_id == index]["title"].values[0]
	def get_ID_from_title(self,title):
		return np.int32(self.dataPD[self.dataPD.title == title]["book_id"].values[0])

	def cosine_similar(self,ID):
		
		
		similar_books = list(enumerate(self.cosine_sim[ID]))
		sorted_similar_books = sorted(similar_books,key=lambda x:x[1],reverse=True)[1:]
		i=0
		similar=[]
		for element in sorted_similar_books:
			similar.append(element[0])
			i=i+1
			if i>20:
				break
		return similar 	
