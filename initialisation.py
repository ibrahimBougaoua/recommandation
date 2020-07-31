

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from numpy import genfromtxt
from pyspark.sql.functions import col
import pandas as pd 

class init:


    path='D:\datasets'




    
    spark = SparkSession \
            .builder \
            .appName("Python Spark SQL basic example") \
            .config("spark.some.config.option", "some-value") \
            .master("local")\
            .getOrCreate()
    users_schema = 'id Integer,gender STRING,age Integer,city STRING ,major STRING,yearsExperience Integer '
    users = spark.read.csv(path=path+r'\user.csv', schema = users_schema, sep=',',header=True)
    jobs_schema='jobpost STRING, date STRING,Company STRING, RequiredQual STRING, Eligibility STRING, Title STRING, JobDescription STRING, JobRequirment STRING, combined_features STRING, Skills STRING, Location STRING, jobId Integer,check STRING'
    #jobs= spark.read.csv(path=path+r'\jobOffers.csv', schema = jobs_schema, sep=',',header=True)
    jobs=pd.read_csv(path+r'\jobOffers.csv')

 
    movie_schema = 'movieId Integer,vote_average Double,vote_count Integer,title STRING,released STRING,year STRING,runtime STRING,genre STRING,director STRING,writer STRING,actors STRING,plot STRING,country STRING,poster STRING,rating1 STRING,production STRING,boxOffice STRING,views STRING'
    movies = spark.read.csv(path=path+r'\movies\movies.csv', schema = movie_schema, sep=',',header=True)


    movie_tags_schema = 'movieId Integer,FilmNoir Integer,Action Integer,Adventure Integer,Horror Integer,Romance Integer,War Integer,History Integer,Western Integer,Documentary Integer,SciFi Integer,Sport Integer,Drama Integer,Thriller Integer,Music Integer,Crime Integer,News Integer,Fantasy Integer,Biography Integer,Animation Integer,Family Integer,Comedy Integer,Mystery Integer,Musical Integer,Short Integer'
    movie_tags =spark.read.csv(path=path+r'\movies\mov.csv', schema = movie_tags_schema, sep=',')   

    book_schema = 'book_id Integer, ratings_count Integer,average_rating Double,title STRING,original_title STRING,year Integer,authors STRING,genre STRING,description STRING,rating Integer,image_url STRING,small_image_url STRING,views STRING'
    books = spark.read.csv(path=path+r'\books\books.csv', header=True, sep=',')

    movie_ratings_schema = 'userId Integer,movieId Integer ,rating Double,timestamp Integer'
    movie_ratings = spark.read.csv(path=path+r'\movies\latings.csv',schema = movie_ratings_schema, sep=',')     

    book_ratings_schema='userId Integer,book_id Integer ,ratings Double,timestamp Integer'
    book_ratings=spark.read.csv(path=path+r'\books\ratings.csv',schema = book_ratings_schema, sep=',')


    header="Parenting,Superman,Superheroes,BDSM,Psychology,Mental Health,History,Contemporary Romance,Design,Currency,Comedy,German Literature,Marriage,Space,Productivity,Mental Illness,1st Grade,Africa,Victorian,Cultural,High School,Erotic Romance,True Crime,Romantic Suspense,Islam,European History,Legal Thriller,Art,Personal Development,Young Adult Contemporary,Biology,Suspense,Science Fiction,Buisness,Military Fiction,Storytime,Sports and Games,Sweden,Buddhism,Adult,Evolution,Food,Military,Communication,Womens Fiction,Nature,Japanese Literature,Inspirational,Popular Science,Abuse,Literary Fiction,Zombies,Religion,Forgotten Realms,Civil War,20th Century,Physics,Medicine,Photography,Star Wars,Christian Non Fiction,Medieval,Holiday,Cyberpunk,Music,Dark Fantasy,Knitting,Writing,Israel,Australia,Alternate History,Street Art,Spirituality,Eastern Africa,Cooking,Young Adult Fantasy,Retellings,Banned Books,Paranormal,American Revolution,Self Help,Jewish,Teaching,Womens,Neuroscience,Nonfiction,Role Playing Games,Journalism,Southern,Collections,Business,Economics,Regency,Historical Fiction,Architecture,High Fantasy,Westerns,Science Fiction Fantasy,Fairy Tales,Kids,Spanish Literature,Novella,Asian Literature,Graphic Novels Comics,College,Italy,Tudor Period,Novels,Crafts,Philosophy,African American,Drawing,Action,Christmas,Demons,War,Scotland,Mathematics,Comic Book,Cookbooks,Russia,Money,School,Relationships,Scandinavian Literature,Number,Indian Literature,Language,China,Race,Biography,Folklore,Movies,Teen,Theology,Food and Drink,New York,Adventure,Horror,Nutrition,Sports Romance,Health,English History,Fae,Gothic,Football,Humor,New Adult,Dungeons and Dragons,Romantic,Noir,Travel,Ghosts,Feminism,Asia,Christian Living,Classic Literature,Chick Lit,Dogs,Books About Books,Dystopia,Short Stories,Skepticism,Prayer,Media Tie In,Werewolves,Leadership,Epic,Plays,Sudan,Young Adult,Greece,French Literature,Paranormal Romance,Espionage,Book Club,Poetry,Arthurian,Taoism,Memoir,New Age,Young Adult Romance,Anthologies,Mountaineering,Sequential Art,Survival,Social Science,LGBT,Audiobook,India,Canada,Baseball,19th Century,Crime,South Africa,Medical,Shapeshifters,Childrens Classics,Comics,Erotica,Fiction,Counting,Germany,World War II,Coming Of Age,American,Graphic Novels,British Literature,Academic,18th Century,Urban Fantasy,Apocalyptic,Sociology,Ancient,Gardening,Mystery,Nigeria,Presidents,Classics,Epic Fantasy,Humanities,Cats,Fairies,Reference,Batman,Witches,Irish Literature,Detective,Marvel,Historical Romance,Spain,Western Africa,Italian Literature,Pre K,Adult Fiction,Entrepreneurship,Vampires,Horses,Mythology,Family,Steampunk,Time Travel,Biography Memoir,Funny,Road Trip,Astronomy,Military History,Fitness,Modern Classics,Church,Autobiography,American Civil War,Unfinished,Comics Manga,Animal Fiction,Microhistory,Picture Books,Dc Comics,Thriller,Christian Fiction,Education,Dragons,Angels,Eastern Philosophy,European Literature,Shojo,Culture,Adoption,Sports,Juvenile,Northern Africa,Aliens,Queer,Literature,Atheism,Spy Thriller,How To,Graffiti,Psychological Thriller,Realistic Fiction,Japan,Romance,Contemporary,Space Opera,Drama,Comix,Love,Theatre,Fantasy,Middle Grade,Death,Environment,16th Century,Personal Finance,Ethiopia,Murder Mystery,Holocaust,Science,Anthropology,Dragonlance,The United States Of America,Pakistan,Magical Realism,Ireland,GLBT,Chapter Books,Lds,France,American History,Art Design,Finance,Magic,Childrens,Read For School,Pop Culture,Faith,Management,Christianity,Mystery Thriller,Post Apocalyptic,Political Science,Russian Literature,Art History,Animals,Southern Africa,Christian,Politics,Supernatural,Historical,Essays,Manga"

    book_header_tags=header.split(",")
    
    book_tags_test=spark.read.csv(path=path+r'\books\genres.csv',header=True ,sep=',')
    book_tags=book_tags_test.select([col(c).cast("Integer") for c in book_tags_test.columns])
    book_cluster_schema='bookId Integer,cluster Integer'
    book_cluster=spark.read.csv(path=path+r'\books\book_cluster.csv', schema =book_cluster_schema,sep=',')

    movie_cluster_schema='movieId Integer,cluster Integer'
    movie_cluster=spark.read.csv(path=path+r'\movies\movie_cluster.csv',schema =movie_cluster_schema,sep=',')

    als_recomendation_movies=spark.read.csv(path=path+r'\movies\als_recomendation.csv',sep=',',header=True)
    
    qualified_movies_schema='movieId Integer,title STRING,vote_count Integer,vote_average Double,genre STRING,actors STRING,director STRING'
    qualified_movies=spark.read.csv(path=path+r'\movies\qualified.csv',sep=',',schema=qualified_movies_schema)


    qualified_books_schema='book_id Integer,title STRING,vote_count Integer,vote_average Double,genre STRING,authors STRING'
    qualified_books=spark.read.csv(path=path+r'\books\qualified.csv',sep=',',schema=qualified_books_schema)
