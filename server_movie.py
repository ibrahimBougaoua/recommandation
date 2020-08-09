from flask import Flask,render_template, url_for ,flash,redirect,session,request,jsonify,json,make_response
from flask_bcrypt import Bcrypt
from flask_cors import CORS

from hybride import hybride_movie
from flask_pymongo import PyMongo
from functools import wraps
from collections import Counter

import random
import jwt
import datetime
from datetime import timedelta

app = Flask(__name__, static_url_path='/static',template_folder="templates")
app.secret_key = '5df4hg5fg4jh56fg4j564gj564hg56j4g5h64j56hg4j5h45j45h4j'
CORS(app)

app.config['BASE_URL'] = 'http://127.0.0.1:5000'  # Running on localhost
app.config['JWT_SECRET_KEY'] = 'super-secret'  # Change this!
app.config['JWT_TOKEN_LOCATION'] = ['cookies']
app.config['JWT_COOKIE_CSRF_PROTECT'] = True
app.config['JWT_CSRF_CHECK_FORM'] = True

app.config['MONGO_DBNAME'] = 'movie'
app.config['MONGO_URI'] = 'mongodb://localhost:27017/movie'

bcrypt = Bcrypt()
mongo  = PyMongo(app)
movies =hybride_movie()

######################################################## movie Service ######################################################
def recommandation(email):
   
    user=mongo.db.user.find_one({"email":email})
    #userId=user["telephone"]        #user["userId"]
    userId=user["telephone"]        #user["userId"]
    dateNow=datetime.datetime.now()

          ##### movies views ######
    HMovies=mongo.db.moviesViews.find({"email":email})
    
    data1=[]

    for elem in HMovies :
        if elem["date"]+timedelta(days=7)>=dateNow : 
            data1.append(elem["movie"])
    

          #####  movie title    ######
    
    MTitle=mongo.db.moviesTitle.find({"email":email})
    data2=[]
    for elem in MTitle :
        if elem["date"]+timedelta(days=7)>=dateNow : 
            data2.append(elem["movie"])
    
          ##########  moviesAUTHoR  #########
    MAuthor=mongo.db.moviesAuthor.find({"email":email})
    data3=[]
    for elem in MAuthor :
        if elem["date"]+timedelta(days=7)>=dateNow : 
            data3.append(elem["movie"])  

        ##########  movies Tag  #########
    MTag1=mongo.db.moviesTag.find({"email":email})
    MTag2=mongo.db.moviesGenres.find({"email":email})

    data4=[]
    for elem in MTag1 : 
        data4.append(elem["movie"])
    for elem in MTag2 :
        data4.append(elem["movie"])
    
    data5=[]
    counts = Counter(data4)
    for elem in counts.most_common(10):
        data5.append(elem[0])

    return random.choices(movies.Recommended(data1,data2,data3,data5,int(userId)),k=16)

def token_required(f):
    @wraps(f)
    def decorated(*args,**kwargs):
        token = None

        if 'x-access-token' in request.headers:
            token = request.headers['x-access-token']

        if not token:
            return jsonify({'message':'token is missing'}), 401

        try:
            data = jwt.decode(token,app.secret_key)
            user = mongo.db.users.find_one({"email":data['email']})
        except :
            return jsonify({'message':'token is invalid'}), 401

        return f(user,*args,**kwargs)

    return decorated

@app.route('/unprotected', methods=('GET','POST'))
def unprotected():
    return jsonify({'message':'show enable'})

@app.route('/protected', methods=('GET','POST'))
@token_required
def protected(user):
    print(user)
    return jsonify({'message':'show disable'})

# Route /movie/abonnements/email api Page
@app.route('/movie/abonnements/<email>')
def Abonnements(email):
    data = []
    for x in mongo.db.abonnements.find({"email": email},{ "_id": 0, "email": 1, "actor": 1 }):
        data.append(movies.moviesByAbonnements(x["actor"]))
    
    return json.dumps(data)

# Route /movie/abonnements/actor/<actor> api Page
@app.route('/movie/abonnements/actor/<actor>')
def moviesActor(actor):
    return json.dumps(movies.moviesByAbonnements(actor))

# Route //movie/abonnements/user/<email> api Page
@app.route('/movie/abonnements/user/<email>')
def AbonnementsActors(email):
    actors = []
    for x in mongo.db.abonnements.find({"email": email},{ "_id": 0, "email": 1, "actor": 1 }):
        actors.append(x["actor"])
    return json.dumps(actors)

# Route /movie/abonnement/new api Page
@app.route('/movie/abonnement/new', methods=('GET','POST'))
def new_Abonnement():
    _email = request.args.get("_email")
    _actor = request.args.get("_actor")
    user = mongo.db.abonnements.find_one({"actor": _actor})
    if user is None:
        mongodb   = mongo.db.abonnements
        mongodb.insert({"email": _email,"actor": _actor})
        return 'Abonnement added successfully !'

    return 'abonnement already exists !'

# Route /movie/abonnement/delete api Page
@app.route('/movie/abonnement/delete', methods=('GET','POST'))
def delete_Abonnement():
    _email = request.args.get("_email")
    _actor = request.args.get("_actor")
    user = mongo.db.abonnements.delete_one({"email": _email,"author": _actor})
    if user is None:
        return 'Abonnement no delete successfully !'

    return 'abonnement delete successfully !'

# Route /movie/check/abonnement/user/<email>/actor/<actor> api Page
@app.route('/movie/check/abonnement/user/<email>/actor/<actor>', methods=('GET','POST'))
def ifAbonner(email,actor):
    user = mongo.db.abonnements.find_one({"email": email,"actor": actor})
    if user is not None:
        return 'no'
    return 'yes'

# Route /movie/showlater/new/email/<email>/id/<int:_id> api Page
@app.route('/movie/showlater/new/email/<email>/id/<int:_id>', methods=('GET','POST'))
def new_show_later(email,_id):
    _movie = movies.movies.getMoviesFromIds([_id])
    movie_show_later = mongo.db.moviesList.find_one({"email": email,"movie": _movie})
    if movie_show_later is None:
        mongodb   = mongo.db.moviesList
        mongodb.insert({"email": email,"movie": _movie})
        return 'movie added successfully !'

    return 'movie already exists !'

# Route /movie/check/showlater/user/<email>/id/<_id> api Page
@app.route('/movie/check/showlater/user/<email>/id/<_id>', methods=('GET','POST'))
def ifShowlater(email,_id):
    _movie = movies.movies.getMoviesFromIds([_id])
    print(_movie)
    user = mongo.db.moviesList.find_one({"email": email,"movie": _movie})
    if user is None:
        return 'no'
    return 'yes'

# Route /movie/showlater/delete api Page
@app.route('/movie/showlater/delete', methods=('GET','POST'))
def delete_show_later():
    _email = request.args.get("_email")
    _id = request.args.get("_id")
    _movie = movies.movies.getMoviesFromIds([_id])
    user = mongo.db.moviesList.delete_one({"email": _email,"movie": _movie})
    if user is None:
        return 'show later no delete successfully !'

    return 'show later delete successfully !'

# Route /movie/showlater/<email> api Page
@app.route('/movie/showlater/<email>')
def show_list(email):
    data = []
    for x in mongo.db.moviesList.find({"email": email},{ "_id": 0, "movie": 1}):
        _id = x["movie"]
        m = _id[0]
        data.append(m[0])
    print(data)
    return json.dumps(movies.movies.getMoviesFromIds(data))

# Route /movie/singup/ api Page
@app.route('/movie/singup/', methods=('GET','POST'))
def singup():

    error = []

    fisrtname = request.args.get("fisrtname")
    lastname = request.args.get("lastname")
    email = request.args.get("email")
    password = request.args.get("password")
    tags = request.args.get("tags")
    sexe = request.args.get("sexe")
    age = request.args.get("age")
    country = request.args.get("country")
    telephone = request.args.get("telephone")
    
    email_find = mongo.db.user.find_one({"email": email})
    telephone_find = mongo.db.user.find_one({"telephone": telephone})

    if not fisrtname:
        error.append('fisrtname is empty.')

    if not lastname:
        error.append('lastname is empty.')

    if not email:
        error.append('email is empty.')

    if not password:
        error.append('password is empty.')

    if not sexe:
        error.append('sex is empty.')

    if not tags:
        error.append('tags is empty.')

    if not age:
        error.append('age is empty.')

    if not country:
        error.append('country is empty.')

    if not telephone:
        error.append('telephone is empty.')

    if email_find is not None:
        error.append('Email exists.')

    if telephone_find is not None:
        error.append('Telephone exists.')

    if error:
        return json.dumps(error)
    else:

        for x in tags.split(","):
            mongo.db.moviesTag.insert({"email" : email,"movie" : x  })
        
        password = bcrypt.generate_password_hash(password)
        mongodb   = mongo.db.user
        mongodb.insert({ "fisrtname" : fisrtname, "lastname" : lastname, "email" : email, "password" : password , "sexe" : sexe, "age" : age, "country" : country, "telephone" : telephone})
        token = jwt.encode({'username':email,'exp':datetime.datetime.utcnow()+datetime.timedelta(minutes=30)},app.secret_key)
        ret = {
            'token': token.decode('UTF-8'),  # test
            'email':  email # ['foo', 'bar']
        }
        return jsonify(ret), 200

    return make_response('Could not verify',401,{'WWW-Authenticate':'Basic releam="Login required"'})

# Route /movie/user/information/<email> api Page
@app.route('/movie/user/information/<email>', methods=('GET','POST'))
def getUserInformation(email):
    user_find = mongo.db.user.find_one({"email": email})
    if user_find:
        data = []
        data.append(user_find["fisrtname"])
        data.append(user_find["lastname"])
        data.append(user_find["email"])
        data.append(user_find["sexe"])
        data.append(user_find["age"])
        data.append(user_find["country"])
        data.append(user_find["telephone"])
        return json.dumps(data)

    return json.dumps(['get User Information is invalid'])

# Route /movie/user/update/<user_email> api Page
@app.route('/movie/user/update/<user_email>', methods=('GET','POST'))
def updateUserInformation(user_email):

    error = []

    fisrtname = request.args.get("fisrtname")
    lastname = request.args.get("lastname")
    email = request.args.get("email")
    password = request.args.get("password")
    sex = request.args.get("sex")
    age = request.args.get("age")
    country = request.args.get("country")
    telephone = request.args.get("telephone")

    if not fisrtname:
        error.append('fisrtname is empty.')

    if not lastname:
        error.append('lastname is empty.')

    if not email:
        error.append('email is empty.')

    if not sex:
        error.append('sex is empty.')

    if not age:
        error.append('age is empty.')

    if not country:
        error.append('country is empty.')

    if not telephone:
        error.append('telephone is empty.')

    
    if error:
        return make_response('Could not verify',401,{'WWW-Authenticate':'Basic releam="Login required"'})
    else:
        mongodb   = mongo.db.user
        if not password:
            user = mongo.db.user.find_one({"email": user_email})
            mongodb.update_one({'email': user_email},{'$set': { "fisrtname" : fisrtname, "lastname" : lastname, "email" : email, "password" : user["password"],"sex" : sex, "age" : age, "country" : country, "telephone" : telephone }}, upsert=False)
        else:
            password = bcrypt.generate_password_hash(password)
            mongodb.update_one({'email': user_email},{'$set': { "fisrtname" : fisrtname }}, upsert=False)
        error.append('user updated successfully.')
    return json.dumps(error)

# Route /movie/login api Page
@app.route('/movie/login', methods=('GET','POST'))
def login_jwt():

    username = request.args.get("username")
    password = request.args.get("password")

    print(username)
    print(password)

    if  not username or not password:
        return make_response('Could not verify',401,{'WWW-Authenticate':'Basic releam="Login required"'})

    user = mongo.db.user.find_one({"email": username})
    if user is not None:
        if bcrypt.check_password_hash(user["password"], password):
            token = jwt.encode({'username':username,'exp':datetime.datetime.utcnow()+datetime.timedelta(minutes=30)},app.secret_key)
            ret = {
                'token': token.decode('UTF-8'),  # test
                'email':  user["email"] # ['foo', 'bar']
            }
            return jsonify(ret), 200
    return make_response('Could not verify',401,{'WWW-Authenticate':'Basic releam="Login required"'})

# Route /movie/search Page
@app.route('/movie/search', methods=('GET', 'POST'))
def searchMovies():

    search = request.args.get("search")
    cate   = request.args.get("cate")
    email  = request.args.get("email")

    moviesSearch = []
    
    if cate == 'title':
        moviesSearch = movies.rechercherMovieByTittre(search)[:10]
        #print(moviesSearch)
        user = mongo.db.moviesTitle.find_one({"email" : email,"movie": search ,"date":datetime.datetime.now()})
        if user is None:
            mongo.db.moviesTitle.insert({"email" : email,"movie" : search ,"date":datetime.datetime.now() })

    if cate == 'actor':
        moviesSearch = movies.rechercherMovieByActor(search)[:10]
        user = mongo.db.moviesActor.find_one({"email" : email,"movie": search ,"date":datetime.datetime.now()})
        if user is None:
            mongo.db.moviesActor.insert({"email" : email,"movie" : search ,"date":datetime.datetime.now() })

    if cate == 'tag':
        moviesSearch = movies.rechercherMovieByGenrs(search)[:10]
        mongo.db.moviesTag.insert({"email" : email,"movie" : search  })

    return json.dumps(moviesSearch)

# Route /movie/related/id/<int:movieId> api Page
@app.route('/movie/related/id/<int:movieId>', methods=('GET', 'POST'))
def MovieRelatedMovies(movieId):
    relatedMovies=movies.similaireToMovie(movieId)
    return json.dumps(relatedMovies[:16])

# Route /movie/single/id/<int:movieId>/email/<email> api Page
# Route /movie/single/id/<int:movieId> api Page
@app.route('/movie/single/id/<int:movieId>/email/<email>', methods=('GET', 'POST'))
@app.route('/movie/single/id/<int:movieId>', methods=('GET', 'POST'))
def MovieSingle(movieId,email=None):
    if email is not None: # distanct movies
        mongo.db.moviesViews.insert({"email" : email,"movie" : movieId ,"date":datetime.datetime.now()})
        for elem in movies.movies.getGenre(movieId) :
            mongo.db.moviesGenres.insert({"email":email,"movie":elem})    
    return json.dumps(movies.movies.getMoviesFromIds([movieId]))

# Route /movie/single/id/<int:movieId>/rating/<int:rating>/email/<email> api Page
@app.route('/movie/single/id/<int:movieId>/rating/<int:rating>/email/<email>', methods=('GET', 'POST'))
def MovieRating(movieId,rating=None,email=None):
    if rating != None :
        moviesRatings = mongo.db.moviesRating.find_one({"email" : email,"movieId": movieId})
        if moviesRatings is None:
            mongo.db.moviesRating.insert({"email" : email,"movieId" : movieId,"rating" : rating })
        else:
            mongo.db.moviesRating.update_one({'email': email},{'$set': { "email" : email,"movieId" : movieId,"rating" : rating }}, upsert=False)
    return json.dumps([rating])

# Route /movie/single/rating/id/<int:movieId>/email/<email> api Page
@app.route('/movie/single/rating/id/<int:movieId>/email/<email>', methods=('GET', 'POST'))
def getMovieRating(movieId,rating=None,email=None):
    moviesRatings = mongo.db.moviesRating.find_one({"email" : email,"movieId": movieId})
    if moviesRatings is None:
        return json.dumps([0])
    else:
        return json.dumps([moviesRatings["rating"]])

# Route /movie/history/views/email/<email> api Page
@app.route('/movie/history/views/email/<email>', methods=('GET', 'POST'))
def moviesViews(email=None):
    data = []
    loop = mongo.db.moviesViews.find({"email": email},{ "_id": 0, "movie": 1 })
    if loop is not None:
        print(loop)
        for x in loop:
            demo = x["movie"]
            if demo:
                data=data+movies.movies.getMoviesFromIds([demo])
    return json.dumps(data)

# Route /movie/history/title/email/<email> api Page
@app.route('/movie/history/title/email/<email>', methods=('GET', 'POST'))
def moviesTitle(email=None):
    data = []
    loop = mongo.db.moviesTitle.find({"email": email},{ "_id": 0, "movie": 1 })
    if loop is not None:
        for x in loop:
            demo = x["movie"]
            if demo:
                data.append(demo)
    return json.dumps(data)

# Route /movie/history/actor/email/<email> api Page
@app.route('/movie/history/actor/email/<email>', methods=('GET', 'POST'))
def moviesActors(email=None):
    data = []
    loop = mongo.db.moviesActor.find({"email": email},{ "_id": 0, "movie": 1 })
    if loop is not None:
        for x in loop:
            demo = x["movie"]
            if demo:
                data.append(demo)
    return json.dumps(data)

# Route /movie/history/tag/email/<email> api Page
@app.route('/movie/history/tag/email/<email>', methods=('GET', 'POST'))
def moviesTag(email=None):
    data = []
    loop = mongo.db.moviesTag.find({"email": email},{ "_id": 0, "movie": 1 })
    if loop is not None:
        for x in loop:
            demo = x["movie"]
            if demo:
                data.append(demo)
    return json.dumps(data)

# Route /movie/recommended/<email> api Page
@app.route('/movie/recommended/<email>', methods=('GET', 'POST'))
def MovieRecommended(email):
    return json.dumps(recommandation(email))

# Route /movie/age/<email> api Page
@app.route('/movie/age/<email>')
def moviesByUserAge(email):
    userData = mongo.db.user.find_one({"email" : email})
    #return json.dumps(movies.movies.getMoviesFromIds(movies.movieIdsByAge(int(userData["age"]))[:16]))
    return json.dumps(movies.movieIdsByAge(38.8))[:16]

# Route /movie/country api Page
@app.route('/movie/country/<email>')
def moviesByUserCountry(email):
    userData = mongo.db.user.find_one({"email" : email})
    #return json.dumps(movies.movies.getMoviesFromIds(movies.movieIdsByCountry(userData["country"])[:16]))
    return json.dumps(movies.movieIdsByCountry("NC")[:16])


################################## Books by Sexe ##########################################
@app.route('/movie/sexe/<email>')
def moviesByUserSexe(email):
    userData = mongo.db.user.find_one({"email" : email})
    #return json.dumps(movies.movies.getMoviesFromIds(movies.movieIdsBySexe(userData["sexe"])[:16]))
    return json.dumps(movies.movieIdsBySexe("Female")[:16])




# Route /movie/top api Page
@app.route('/movie/top', methods=('GET', 'POST'))
def MovieTop():
    return json.dumps(movies.topRated()[1:10])

# Route /movie/most api Page
@app.route('/movie/most', methods=('GET', 'POST'))
def MovieMost():
    return json.dumps(movies.topWached()[1:10])

# Route /dashboard/information api Page
@app.route('/dashboard/information', methods=('GET', 'POST'))
def dashboardInformation():
    a = mongo.db.users.count()
    b = mongo.db.abonnements.count()
    c = 7997
    return json.dumps([a,b,c])

# Route /dashboard/chart/bar api Page
@app.route('/dashboard/chart/bar', methods=('GET', 'POST'))
def dashboardChartBar():
    labels = ['2000','2001','2002','2003','2004','2005','2006','2007','2008','2009','2010','2011']
    data = [22,19,27,23,22,24,17,25,23,24,20,19]
    return json.dumps([labels,data])

# Route /dashboard/chart/pie api Page
@app.route('/dashboard/chart/pie', methods=('GET', 'POST'))
def dashboardChartPie():
    labels = ['Toy Story','Jumanji','Grumpier Old Men','Waiting to Exhale','Heat','Sabrina','GoldenEye','Nixon','Balto','Copycat','Assassins','Powder']
    data = [5415,1128,2413,2470,305,539,423,173,756,364,828,1343]
    return json.dumps([labels,data])

# Route /dashboard/chart/line api Page
@app.route('/dashboard/chart/line', methods=('GET', 'POST'))
def dashboardChartLine():
    labels = ['adventure','animation','children','comedy','fantasy','children','fantasy','romance','drama','action','crime','thriller','horror','mystery','sci-Fi','documentary','imax']
    data = [5415,1128,2413,2470,305,539,423,173,756,364,828,1343,173,756,364,828,1343]
    return json.dumps([labels,data])

# Route /dashboard/chart/horizontalBar api Page
@app.route('/dashboard/chart/horizontalBar', methods=('GET', 'POST'))
def dashboardChartHorizontalBar():
    labels = ['Toy Story','Jumanji','Grumpier Old Men','Waiting to Exhale','Heat','Sabrina','GoldenEye','Nixon','Balto','Copycat','Assassins']
    data = [5415,1128,2413,2470,305,539,423,173,756,364,828]
    return json.dumps([labels,data])

##############################################################################################################

if __name__ == '__main__':
    app.run(port=5000,debug=True)