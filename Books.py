

from initialisation import init
import pandas as pd
import numpy as np


class Books:

	def __init__(self):
		self.books=init.books.toPandas()
		self.spark = init.spark
		self.qualified=init.qualified_books
		if self.qualified.count()<=0 :
			self.qualified=self.build_datast_Books_IMDN()
		else:
			self.qualified=self.qualified.toPandas()
		self.qualified['genre']=self.qualified['genre'].str.upper()
		self.qualified['title']=self.qualified['title'].str.upper()
		self.qualified['authors']=self.qualified['authors'].str.upper()

        
	def build_datast_Books_IMDN(self):
		self.books=self.books.drop(self.books[self.books.ratings_count==' '].index)
		self.books=self.books.drop(self.books[self.books.average_rating==' '].index)
		vote_counts = self.books[self.books['ratings_count'].notnull()]['ratings_count'].astype('int')
		vote_averages = self.books[self.books['average_rating'].notnull()]['average_rating'].astype('float')
		self.C = vote_averages.mean()
		self.m = vote_counts.quantile(0.95)
		self.books['ratings_count']=self.books[self.books['ratings_count'].notnull()]['ratings_count'].astype('int')
		qualified = self.books[(self.books['ratings_count'] >= self.m) & (self.books['ratings_count'].notnull()) & (self.books['average_rating'].notnull())][['book_id','title','ratings_count', 'average_rating','genre','authors']]
		qualified['ratings_count'] = qualified['ratings_count'].astype('int')
		qualified['average_rating'] = qualified['average_rating'].astype('float')
		qualified['wr'] = qualified.apply(self.weighted_rating, axis=1)
		qualified = qualified.sort_values('wr', ascending=False)
		qualified['genre']=self.qualified['genre'].str.upper()
		qualified['title']=self.qualified['title'].str.upper()
		qualified['authors']=self.qualified['authors'].str.upper()
		self.spark.createDataFrame(qualified).write.format('csv').option('header',False).mode('overwrite').option('sep',',').save('file:///home/hadoop_user/Desktop/PFE/datasets/lbooks/qualified.csv')
		return qualified

	def weighted_rating(self,x):
		v = x['ratings_count']
		R = x['average_rating']
		return (v/(v+self.m) * R) + (self.m/(self.m+v) * self.C)

	def SearchingByTitle(self,title):
		books = self.qualified[self.qualified['title'].str.contains(title.upper())]['book_id']
		try:
			book=books.apply(pd.Series).stack().tolist()
		except Exception as e:
			book=[]
		return book
	def SearchingAuthors(self,author):
		books = self.qualified[self.qualified['authors'].str.contains(author.upper())]['book_id']
		try:
			book=books.apply(pd.Series).stack().tolist()
		except Exception as e:
			book=[]
		return book
	def getBooksByGenre(self,genre):
		books = self.qualified[self.qualified['genre'].str.contains(genre.upper())]['book_id']
		try:
			book=books.apply(pd.Series).stack().tolist()
		except Exception as e:
			book=[]
		return book
	def getbooksFromIds(self,bookIds):
		books=[]
		for bookId in bookIds :
			books=books+(self.books[self.books['book_id']==str(bookId)].values.tolist())
		return books
	def qualified1(self):
		return self.qualified['book_id'].values.tolist()
	def getGenre(self,book_id):
		book=self.books[self.books['book_id']==str(book_id)]
		return book["genre"] 
		