from flask import Flask,render_template, url_for ,flash,redirect,session,request,jsonify,json,make_response
from flask_bcrypt import Bcrypt
from flask_cors import CORS

from hybride import hybride_book
from flask_pymongo import PyMongo
from functools import wraps

import random
import jwt
import datetime
from datetime import timedelta

app = Flask(__name__, static_url_path='/static',template_folder="templates")
app.secret_key = '5df4hg5fg4jh56fg4j564gj564hg56j4g5h64j56hg4j5h45j45h4j'
CORS(app)

app.config['BASE_URL'] = 'http://127.0.0.1:5000'  #Running on localhost
app.config['JWT_SECRET_KEY'] = 'super-secret'  # Change this!
app.config['JWT_TOKEN_LOCATION'] = ['cookies']
app.config['JWT_COOKIE_CSRF_PROTECT'] = True
app.config['JWT_CSRF_CHECK_FORM'] = True

app.config['MONGO_DBNAME'] = 'movie'
app.config['MONGO_URI'] = 'mongodb://localhost:27017/movie'

bcrypt = Bcrypt()
mongo  = PyMongo(app)

books=hybride_book()

######################################################## book Service ######################################################
def recommandation(email):
   
    user=mongo.db.user.find_one({"email":email})
    userId=user["telephone"]        #user["userId"]
    dateNow=datetime.datetime.now()

          ##### books views ######
    HBooks=mongo.db.booksViews.find({"email":email})
    
    data1=[]

    for elem in HBooks :
        if elem["date"]+timedelta(days=7)>=dateNow : 
            data1.append(elem["book"])
    

          #####  book title    ######
    
    BTitle=mongo.db.booksTitle.find({"email":email})
    data2=[]
    for elem in BTitle :
        if elem["date"]+timedelta(days=7)>=dateNow : 
            data2.append(elem["book"])
    
          ##########  booksAUTHoR  #########
    BAuthor=mongo.db.booksAuthor.find({"email":email})
    data3=[]
    for elem in BAuthor :
        if elem["date"]+timedelta(days=7)>=dateNow : 
            data3.append(elem["book"])  

        ##########  books Tag  #########
    BTag1=mongo.db.booksTag.find({"email":email})
    BTag2=mongo.db.booksGenres.find({"email":email})

    data4=[]
    for elem in BTag1 : 
        data4.append(elem["book"])
    for elem in BTag2 :
        data4.append(elem["book"])

    counts = Counter(data4)
    for elem in counts.most_common(10):
        data5.append(elem[0])

    return random.choices(books.recommended(data1,data2,data3,data5,userId),16)


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

# Route /book/abonnements/email api Page
@app.route('/book/abonnements/<email>')
def Abonnements(email):
    data = []
    for x in mongo.db.abonnements.find({"email": email},{ "_id": 0, "email": 1, "author": 1 }):
        data.append(books.booksByAbonnements(x["author"]))
    print(data)
    return json.dumps(data)   

# Route /book/abonnements/email api Page
@app.route('/book/abonnements/author/<author>')
def booksAuthor(author):
    return json.dumps(books.booksByAbonnements(author))

# Route /book/abonnements/authors api Page
@app.route('/book/abonnements/user/<email>') 
def AbonnementsAuthors(email):
    authors = []
    for x in mongo.db.abonnements.find({},{ "_id": 0, "email": email, "author": 1 }):
        authors.append(x["author"])
    return json.dumps(authors)

@app.route('/book/abonnement/new', methods=('GET','POST'))
def new_Abonnement():
    _email = request.args.get("_email")
    _author = request.args.get("_author")
    user = mongo.db.abonnements.find_one({"email": _email,"author": _author})
    if user is None:
        mongodb   = mongo.db.abonnements
        mongodb.insert({"email": _email,"author": _author})
        return 'Abonnement added successfully !'

    return 'abonnement already exists !'

@app.route('/book/abonnement/delete', methods=('GET','POST'))
def delete_Abonnement():
    _email = request.args.get("_email")
    _author = request.args.get("_author")
    user = mongo.db.abonnements.delete_one({"email": _email,"author": _author})
    if user is None:
        return 'Abonnement no delete successfully !'

    return 'abonnement delete successfully !'

@app.route('/book/check/abonnement/user/<email>/author/<author>', methods=('GET','POST'))
def ifAbonner(email,author):
    user = mongo.db.abonnements.find_one({"email": email,"author": author})
    if user is None:
        return 'no'
    return 'yes'

@app.route('/book/showlater/new/email/<email>/id/<int:_id>', methods=('GET','POST'))
def new_show_later(email,_id):
    _book = books.books.getbooksFromIds([_id])
    movie_show_later = mongo.db.booksList.find_one({"email": email,"book": _book})
    if movie_show_later is None:
        mongodb   = mongo.db.booksList
        mongodb.insert({"email": email,"book": _book})
        return 'book added successfully !'

    return 'book already exists !'


@app.route('/book/check/showlater/user/<email>/id/<_id>', methods=('GET','POST'))
def ifShowlater(email,_id):
    _book = books.books.getbooksFromIds([_id])
    user = mongo.db.booksList.find_one({"email": email,"book": _book})
    if user is None:
        return 'no'
    return 'yes'

@app.route('/book/showlater/delete', methods=('GET','POST'))
def delete_show_later():
    _email = request.args.get("_email")
    _id = request.args.get("_id")
    _book = books.books.getbooksFromIds([_id])
    user = mongo.db.booksList.delete_one({"email": _email,"book": _book})
    if user is None:
        return 'show later no delete successfully !'

    return 'show later delete successfully !'

# Route /book/showlater/email api Page
@app.route('/book/showlater/<email>')
def show_list(email):
    data = []
    for x in mongo.db.booksList.find({"email": email},{ "_id": 0, "book": 1}):
        _id = x["book"]
        m = _id[0]
        data.append(m[0])
    
    return json.dumps(books.books.getbooksFromIds(data))


@app.route('/book/singup/', methods=('GET','POST'))
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
            mongo.db.booksTag.insert({"email" : email,"movie" : x  })
        
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

@app.route('/book/user/information/<email>', methods=('GET','POST'))
def getUserInformation(email):
    user_find = mongo.db.users.find_one({"email": email})
    if user_find:
        data = []
        data.append(user_find["fisrtname"])
        data.append(user_find["lastname"])
        data.append(user_find["email"])
        data.append(user_find["sex"])
        data.append(user_find["age"])
        data.append(user_find["country"])
        data.append(user_find["telephone"])
        return json.dumps(data)

    return json.dumps(['get User Information is invalid'])


@app.route('/book/user/update/<user_email>', methods=('GET','POST'))
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

    
    
    #email_find = mongo.db.users.find_one({"email": email})
    #telephone_find = mongo.db.users.find_one({"telephone": telephone})

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

    #if email_find is not None:
    #   error.append('Email exists.')

    #if telephone_find is not None:
    #   error.append('Telephone exists.')

    if error:
        return json.dumps(error)
    else:
        mongodb   = mongo.db.users
        if not password:
            user = mongo.db.users.find_one({"email": user_email})
            mongodb.update_one({'email': user_email},{'$set': { "fisrtname" : fisrtname, "lastname" : lastname, "email" : email, "password" : user["password"],"sex" : sex, "age" : age, "country" : country, "telephone" : telephone }}, upsert=False)
        else:
            password = bcrypt.generate_password_hash(password)
            mongodb.update_one({'email': user_email},{'$set': { "fisrtname" : fisrtname }}, upsert=False)
        error.append('user updated successfully.')
    return json.dumps(error)

@app.route('/book/login', methods=('GET','POST'))
def login_jwt():

    username = request.args.get("username")
    password = request.args.get("password")

    if  not username or not password:
        return make_response('Could not verify',401,{'WWW-Authenticate':'Basic releam="Login required"'})

    user = mongo.db.users.find_one({"email": username})
    if user is not None:
        if bcrypt.check_password_hash(user["password"], password):
            token = jwt.encode({'username':username,'exp':datetime.datetime.utcnow()+datetime.timedelta(minutes=30)},app.secret_key)
            ret = {
                'token': token.decode('UTF-8'),  # test
                'email':  user["email"] # ['foo', 'bar']
            }
            return jsonify(ret), 200
    return make_response('Could not verify',401,{'WWW-Authenticate':'Basic releam="Login required"'})

# Route /book/search Page
@app.route('/book/search', methods=('GET', 'POST'))
def searchBook():

    search = request.args.get("search")
    cate   = request.args.get("cate")
    email  = request.args.get("email")


    booksSearch = []
    
    if cate == 'title':
        booksSearch = books.rechercherBookByTittre(search)[:10]
        #print(booksSearch)
        user = mongo.db.booksTitle.find_one({"email" : email,"book": search ,"date":datetime.datetime.now()})
        if user is None:
            mongo.db.booksTitle.insert({"email" : email,"book" : search ,"date":datetime.datetime.now()})

    if cate == 'author':
        booksSearch = books.rechercherBookByAuther(search)[:10]
        user = mongo.db.booksAuthor.find_one({"email" : email,"book": search ,"date":datetime.datetime.now()})
        if user is None:
            mongo.db.booksAuthor.insert({"email" : email,"book" : search ,"date":datetime.datetime.now()})

    if cate == 'tag':
        booksSearch = books.rechercherBookByGenrs(search)[:10]  
        mongo.db.booksTag.insert({"email" : email,"book" : search })

    return json.dumps(booksSearch)

@app.route('/book/related/id/<int:bookId>', methods=('GET', 'POST'))
def bookRelatedMovies(bookId):
    relatedMovies=books.similaireToBook(bookId)
    return json.dumps(relatedMovies[:16])

@app.route('/book/single/id/<int:bookId>/email/<email>', methods=('GET', 'POST'))
@app.route('/book/single/id/<int:bookId>', methods=('GET', 'POST'))
def bookSingle(bookId,email=None):
    if email is not None:
        mongo.db.booksViews.insert({"email" : email,"book" : bookId ,"date":datetime.datetime.now()})
        for elem in books.books.getGenre(bookId) :
            mongo.db.booksGenres.insert({"email":email,"book":elem})
            ##############################################################################
    return json.dumps(books.books.getbooksFromIds([bookId]))

@app.route('/book/single/id/<int:bookId>/rating/<int:rating>/email/<email>', methods=('GET', 'POST'))
def bookRating(bookId,rating=None,email=None):
    if rating != None :
        bookRatings = mongo.db.booksRating.find_one({"email" : email,"bookId": bookId})
        if bookRatings is None:
            mongo.db.booksRating.insert({"email" : email,"bookId" : bookId,"rating" : rating })
        else:
            mongo.db.booksRating.update_one({'email': email},{'$set': { "email" : email,"bookId" : bookId,"rating" : rating }}, upsert=False)
    return json.dumps([rating])

@app.route('/book/single/rating/id/<int:bookId>/email/<email>', methods=('GET', 'POST'))
def getBookRating(bookId,rating=None,email=None):
    bookRatings = mongo.db.booksRating.find_one({"email" : email,"bookId": bookId})
    if bookRatings is None:
        return json.dumps([0])
    else:
        return json.dumps([bookRatings["rating"]])

@app.route('/book/history/views/email/<email>', methods=('GET', 'POST'))
def booksViews(email=None):
    data = []
    loop = mongo.db.booksViews.find({"email": email},{ "_id": 0, "book": 1 })
    if loop is not None:
        for x in loop:
            demo = x["book"]
            if demo:
                data=data+books.books.getbooksFromIds([demo])
    return json.dumps(data) ##################################################################

@app.route('/book/history/title/email/<email>', methods=('GET', 'POST'))
def booksTitle(email=None):
    data = []
    loop = mongo.db.booksTitle.find({"email": email},{ "_id": 0, "book": 1 })
    if loop is not None:
        for x in loop:
            demo = x["book"]
            if demo:
                data.append(demo)
    return json.dumps(data) ################################################################

@app.route('/book/history/author/email/<email>', methods=('GET', 'POST'))
def booksAuthors(email=None):
    data = []
    loop = mongo.db.booksAuthor.find({"email": email},{ "_id": 0, "book": 1 })
    if loop is not None:
        for x in loop:
            demo = x["book"]
            if demo:
                data.append(demo)
    return json.dumps(data) #################################################################

@app.route('/book/history/tag/email/<email>', methods=('GET', 'POST'))
def BooksTag(email=None):
    data = []
    loop = mongo.db.booksTag.find({"email": email},{ "_id": 0, "book": 1 ,"hiden":1})
    if loop is not None:
        for x in loop:
            demo = x["book"]
            if demo:
                data.append(demo) 
    return json.dumps(data) ####################################################################

# Route /book/recommended api Page
@app.route('/book/recommended/<email>', methods=('GET', 'POST'))
def booksRecommended(email=None):
        
    return json.dumps(recommandation(email))

# Route /book/age api Page
@app.route('/book/age/<email>')
def booksByUserAge(email):
    userData = mongo.db.users.find_one({"email" : email})
    return json.dumps(books.books.getbooksFromIds(books.bookIdsByAge(int(userData["age"]))[:16]))

# Route /book/country api Page
@app.route('/book/country/<email>')
def booksByUserCountry(email):
    userData = mongo.db.users.find_one({"email" : email})
    return json.dumps(books.books.getbooksFromIds(books.bookIdsByCountry(userData["country"])[:16]))


################################## Books by Sexe ##########################################
@app.route('/book/sexe/<email>')
def booksByUserCountry(email):
    userData = mongo.db.users.find_one({"email" : email})
    return json.dumps(books.books.getbooksFromIds(books.bookIdsBySexe(userData["sexe"])[:16]))


# Route /book/top api Page
@app.route('/book/top', methods=('GET', 'POST'))
def bookTop():
    return json.dumps(books.topRated()[1:10])

# Route /book/top api Page
@app.route('/book/most', methods=('GET', 'POST'))
def bookMost():
    return json.dumps(books.topWached()[1:10])

# Route /dashboard/information api Page
@app.route('/dashboard/information', methods=('GET', 'POST'))
def dashboardInformation():
    a = mongo.db.users.count()
    b = mongo.db.abonnements.count()
    c = 2533
    return json.dumps([a,b,c])

# Route /dashboard/chart api Page
@app.route('/dashboard/chart/bar', methods=('GET', 'POST'))
def dashboardChartBar():
    labels = ['2000','2001','2002','2003','2004','2005','2006','2007','2008','2009','2010','2011']
    data = [22,19,27,23,22,24,17,25,23,24,20,19]
    return json.dumps([labels,data])

# Route /dashboard/chart api Page
@app.route('/dashboard/chart/pie', methods=('GET', 'POST'))
def dashboardChartPie():
    labels = ['Toy Story','Jumanji','Grumpier Old Men','Waiting to Exhale','Heat','Sabrina','GoldenEye','Nixon','Balto','Copycat','Assassins','Powder']
    data = [5415,1128,2413,2470,305,539,423,173,756,364,828,1343]
    return json.dumps([labels,data])

# Route /dashboard/chart api Page
@app.route('/dashboard/chart/line', methods=('GET', 'POST'))
def dashboardChartLine():
    labels = ['adventure','animation','children','comedy','fantasy','children','fantasy','romance','drama','action','crime','thriller','horror','mystery','sci-Fi','documentary','imax']
    data = [5415,1128,2413,2470,305,539,423,173,756,364,828,1343,173,756,364,828,1343]
    return json.dumps([labels,data])

# Route /dashboard/chart api Page
@app.route('/dashboard/chart/horizontalBar', methods=('GET', 'POST'))
def dashboardChartHorizontalBar():
    labels = ['Toy Story','Jumanji','Grumpier Old Men','Waiting to Exhale','Heat','Sabrina','GoldenEye','Nixon','Balto','Copycat','Assassins']
    data = [5415,1128,2413,2470,305,539,423,173,756,364,828]
    return json.dumps([labels,data])

##############################################################################################################

if __name__ == '__main__':
    app.run(port=5001,debug=True)