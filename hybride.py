from ContentBasedRecommendation import KMeans_books,KMeans_movies,cosine_book
from CollaborativeFiltering import Als_movie,Als_book
from Books import Books
from Movies import Movies 
from jobs import jobs
from initialisation import init
import random

users = init.users
users.createOrReplaceTempView("users")



class hybride_book():

    def __init__(self):
        self.KMeans_books = KMeans_books()
        self.books=Books()
        self.Als_book=Als_book()
        self.cosine=cosine_book()
        self.max_id_in_datset_book=2533
        self.bookRatings = init.bookRatings
        self.bookRatings.createOrReplaceTempView("bookRatings")

    def Recommended(self,historique_book,historique_title,historique_auther,historique_tags,id_user):
        
        filtered=[]
        for x in historique_book :
            liste= random.choices(self.KMeans_books.Recommended(x),k=3)
            for y in liste:
                if y<=self.max_id_in_datset_book:
                    filtered.append(y)
        for tag in historique_tags:
            try:
                liste=random.choices(self.books.getBooksByGenre(tag),k=3)
            except Exception as e:
                pass
            
            for y in liste:
                if y<=self.max_id_in_datset_book:
                    filtered.append(y)
        for ht in historique_title :
            liste=random.choices(self.books.SearchingByTitle(ht),k=3)
            for y in liste:
                if y<=self.max_id_in_datset_book:
                    filtered.append(y)
        for ha in historique_auther :
            liste=random.choices(self.books.SearchingAuthors(ha),k=3)
            for y in liste:
                if y<=self.max_id_in_datset_book:
                    filtered.append(y)          

        if id_user!=0:
            RecommendedBooks=self.books.qualified1()    
        else:
            try:
                RecommendedBooks=self.Als_book.RecommendForUserById(id_user)
            except Exception as e:
                RecommendedBooks=self.books.qualified1()
        for item in RecommendedBooks:
            if item <= self.max_id_in_datset_book:
                filtered.append(item)
    
        return self.books.getbooksFromIds(list(set(filtered)))
    def similaireToBook(self,book_id):
        return self.books.getbooksFromIds(self.cosine.cosine_similar(book_id))
    def rechercherBookByTittre(self,titre):
        return self.books.getbooksFromIds(self.books.SearchingByTitle(titre))
    def rechercherBookByAuther(self,auther) :
        return self.books.getbooksFromIds(self.books.SearchingAuthors(auther))
    def rechercherBookByGenrs(self,genre):
        return self.books.getbooksFromIds(self.books.getBooksByGenre(genre))
    def topRated(self):
        return self.books.getbooksFromIds(self.books.qualified1())
    def topWached(self):
        b=self.books.books.sort_values(by=['views'])
        return self.books.getbooksFromIds(b['book_id'].tolist())
    def booksByUserAge(self,ids):
        return self.books.getbooksFromIds(ids)
    def booksByUserCountry(self,ids):
        return self.books.getbooksFromIds(ids)
    def booksByAbonnements(self,auther):
        return self.books.getbooksFromIds(self.books.SearchingAuthors(auther))
    def bookIdsByAge(self,age):
        age_min=age-5
        age_max=age+15
        data = self.spark.sql("SELECT DISTINCT book_id FROM users,bookRatings WHERE (age >= %d and age <=%d ) AND (id=userId) AND (ratings >= 4) limit 12" %age_min %age_max )
        Collection = []
        for row in data.collect():
            Collection.append(row[0])
        return Collection

    def bookIdsByCountry(self,city):
        data = self.spark.sql("SELECT DISTINCT book_id FROM users,bookRatings WHERE (id=userId) AND (ratings >= 4) AND city=%s"%city)
        Collection = []
        for row in data.collect():
            Collection.append(row[0])
        return Collection
    def bookIdsBySexe(self,sexe):
        #sexe =  Female or Male  
        data = self.spark.sql("SELECT DISTINCT book_id FROM users,bookRatings WHERE (id=userId) AND (ratings >= 4) AND gender=%d limit 12" %sexe)
        Collection = []
        for row in data.collect():
            Collection.append(row[0])
        return Collection



                

        
class hybride_movie():

    def __init__(self):
        self.spark=init.spark
        self.KMeans_movies = KMeans_movies()
        self.spark=init.spark
        self.Als_movie=Als_movie()
        self.movies=Movies()
        self.max_id_in_datset_movie=7997
        self.movieRatings = init.movie_ratings
        self.movieRatings.createOrReplaceTempView("movieRatings")
    def Recommended(self,historique_movie,historique_title,historique_actor,historique_tags,id_user):
        
        filtered=[]
        for x in historique_movie :
            liste= random.choices(self.KMeans_movies.Recommended(x),k=3)
            for y in liste:
                if y<=self.max_id_in_datset_movie:
                    filtered.append(y)
        for tag in historique_tags:
            try:
                liste=random.choices(self.movies.getMoviesByGenre(tag),k=3)
            except Exception as e:
                pass
            
            for y in liste:
                if y<=self.max_id_in_datset_movie:
                    filtered.append(y)
        for ht in historique_title :
            liste=random.choices(self.movies.SearchingByTitle(ht),k=3)
            for y in liste:
                if y<=self.max_id_in_datset_movie:
                    filtered.append(y)
        for ha in historique_actor :
            liste=random.choices(self.movies.SearchingActor(ha),k=3)
            for y in liste:
                if y<=self.max_id_in_datset_movie:
                    filtered.append(y)          

        if id_user!=0:
            RecommendedMovies=self.movies.qualified1()  
        else:
            try:
                RecommendedMovies=self.Als_movie.RecommendForUserById(id_user)
            except Exception as e:
                RecommendedBooks=self.books.qualified1()
        for item in RecommendedMovies:
            if item <= self.max_id_in_datset_movie:
                filtered.append(item)
    
        return self.movies.getMoviesFromIds(list(set(filtered)))

    def similaireToMovie(self,movie_id):
        return self.movies.getMoviesFromIds(self.KMeans_movies.Recommended(movie_id))
    def rechercherMovieByTittre(self,titre):
        return self.movies.getMoviesFromIds(self.movies.SearchingByTitle(titre))
    def rechercherMovieByActor(self,Actor):
        return self.movies.getMoviesFromIds(self.movies.SearchingActor(Actor))
    def rechercherMovieByGenrs(self,genre):
        return self.movies.getMoviesFromIds(self.movies.getMoviesByGenre(genre))        
    def topRated(self):
        return self.movies.getMoviesFromIds(self.movies.qualified1())
    def topWached(self):
        m=self.movies.movies.sort_values(by=['views'])
        return self.movies.getMoviesFromIds(m['movieId'].tolist())
    def moviesByUserAge(self,ids):
        return self.movies.getMoviesFromIds(ids)
    def moviesByUserCountry(self,ids):
        return self.movies.getMoviesFromIds(ids)
    def moviesByAbonnements(self,Actor):
        return self.movies.getMoviesFromIds(self.movies.SearchingActor(Actor))
    def movieIdsByAge(self,age):
        data = self.spark.sql("SELECT DISTINCT movieId FROM users,movieRatings WHERE (age >= %d and age <= %d ) AND (id=userId) AND (rating >= 4) limit 12" % ( age - 5 , age + 15) )
        Collection = []
        for row in data.collect():
            Collection.append(row[0])
        return Collection

    def movieIdsByCountry(self,city):
        data = self.spark.sql("SELECT DISTINCT movieId  FROM users,movieRatings WHERE (id=userId) AND (rating >= 4) and (city='%s') limit 12" %city)
        Collection = []
        for row in data.collect():
            Collection.append(row[0])
        return Collection


    def movieIdsBySexe(self,sexe):
        #sexe =  Female or Male 
        data = self.spark.sql("SELECT DISTINCT movieId  FROM users,movieRatings WHERE (id=userId) AND (rating >= 4) and (gender='%s') limit 12" %sexe)
        Collection = []
        for row in data.collect():
            Collection.append(row[0])
        return Collection 


class hybride_job():
    
    def __init__(self):
        self.jobs = jobs()
    def Recommended():
        data=[]
        jb=self.jobs.jobs[:20]
        for id in jb['jobId']:
            data.append(self.jobs.getJobFromId(id))

        return data

    def searchByMajor(self,major):
        return jobs.getJobsFromIds(self.jobs.getJobOffersFromMajor(major))

    def searchBySkills(self,skill):
        return jobs.getJobsFromIds(self.jobs.getJobOffersBasedOnSkillsNeeded(skill))

    def searchByCompany(self,company):
        return jobs.getJobsFromIds(self.jobs.getJobOffersFromCoumpany(company))


## suite ..
#.
#.
#.
#etc        
        

        


