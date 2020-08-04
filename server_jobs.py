from flask import Flask,render_template, url_for ,flash,redirect,session,request,jsonify,json,make_response
from flask_bcrypt import Bcrypt
from flask_cors import CORS

from hybride import hybride_job
from jobs import jobs
from flask_pymongo import PyMongo
from functools import wraps

import random
import jwt
import datetime
from datetime import timedelta

app = Flask(__name__, static_url_path='/static',template_folder="templates")
app.secret_key = '5df4hg5fg4jh56fg4j564gj564hg56j4g5h64j56hg4j5h45j45h4j'
CORS(app)

app.config['BASE_URL'] = 'http://127.0.0.1:5002'  # Running on localhost
app.config['JWT_SECRET_KEY'] = 'super-secret'  # Change this!
app.config['JWT_TOKEN_LOCATION'] = ['cookies']
app.config['JWT_COOKIE_CSRF_PROTECT'] = True
app.config['JWT_CSRF_CHECK_FORM'] = True

app.config['MONGO_DBNAME'] = 'movie'
app.config['MONGO_URI'] = 'mongodb://localhost:27017/movie'

bcrypt = Bcrypt()
mongo  = PyMongo(app)

job = jobs()
#print(job.getJobOffersFromMajor("Java Developer"))

jobs = hybride_job()

#print(jobs.searchByCity("JavaDeveloper")[:10])
#print(jobs.searchByMajor("Java Developer")[:10])
#print(jobs.searchBySkills("R")[:10])

######################################################## Jobs Service ######################################################

# Route /jobs/test
@app.route('/jobs/test', methods=('GET','POST'))
def test():
    return jobs.jobById()

# function token_required
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

# Route /unprotected/
@app.route('/unprotected', methods=('GET','POST'))
def unprotected():
    return jsonify({'message':'show enable'})

# Route /protected/
@app.route('/protected', methods=('GET','POST'))
@token_required
def protected(user):
    print(user)
    return jsonify({'message':'show disable'})

# Route /job/singup/
@app.route('/job/singup/', methods=('GET','POST'))
def singup():

    error = []

    fisrtname = request.args.get("fisrtname")
    lastname = request.args.get("lastname")
    email = request.args.get("email")
    password = request.args.get("password")
    majors = request.args.get("majors")
    skills = request.args.get("skills")
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

    if not majors:
        error.append('majors is empty.')

    if not skills:
        error.append('skills is empty.')

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

        for x in majors.split(","):
            mongo.db.jobMajors.insert({"email" : email,"majors" : x  })

        for y in skills.split(","):
            mongo.db.jobSkills.insert({"email" : email,"skills" : y  })
        
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

# Route /job/user/information/
@app.route('/job/user/information/<email>', methods=('GET','POST'))
def getUserInformation(email):
    user_find = mongo.db.user.find_one({"email": email})
    if user_find:
        data = []
        data.append(user_find["fisrtname"])
        data.append(user_find["lastname"])
        data.append(user_find["email"])
        data.append(user_find["sex"])
        data.append(user_find["majors"])
        data.append(user_find["skills"])
        data.append(user_find["age"])
        data.append(user_find["country"])
        data.append(user_find["telephone"])
        return json.dumps(data)

    return json.dumps(['get User Information is invalid'])

# Route /job/user/update/
@app.route('/job/user/update/<user_email>', methods=('GET','POST'))
def updateUserInformation(user_email):

    error = []

    fisrtname = request.args.get("fisrtname")
    lastname = request.args.get("lastname")
    email = request.args.get("email")
    password = request.args.get("password")
    sex = request.args.get("majors")
    sex = request.args.get("skills")
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

# Route /job/login/
@app.route('/job/login/', methods=('GET','POST'))
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

# Route /job/search Page
@app.route('/job/search/', methods=('GET', 'POST'))
def searchMovies():

    search = request.args.get("search")
    cate   = request.args.get("cate")
    email  = request.args.get("email")

    jobSearch = []

    if cate == 'majors':
        jobSearch = jobs.searchByMajor(search)[:10] #"Java Developer"
        user = mongo.db.jobSkills.find_one({"email" : email,"job": search ,"date":datetime.datetime.now()})
        if user is None:
            mongo.db.jobSkills.insert({"email" : email,"job" : search ,"date":datetime.datetime.now() })

    if cate == 'skills':
        jobSearch = jobs.searchBySkills(search)[:10] #"R"
        user = mongo.db.jobMajor.find_one({"email" : email,"job": search ,"date":datetime.datetime.now()})
        if user is None:
            mongo.db.jobMajor.insert({"email" : email,"job" : search ,"date":datetime.datetime.now() })

    if cate == 'company':
        jobSearch = jobs.searchByCompany(search)[:10] #"Yerevan Brandy Company"
        user = mongo.db.jobCompany.find_one({"email" : email,"job": search ,"date":datetime.datetime.now()})
        if user is None:
            mongo.db.jobCompany.insert({"email" : email,"job" : search ,"date":datetime.datetime.now() })

    return json.dumps(jobSearch)

# Route /job/single/id/<int:jobId>/email/<email>
# Route /job/single/id/<int:jobId>
@app.route('/job/single/id/<int:jobId>/email/<email>', methods=('GET', 'POST'))
@app.route('/job/single/id/<int:jobId>', methods=('GET', 'POST'))
def JobSingle(jobId,email=None):
    if email is not None: # distanct job
        mongo.db.jobsViews.insert({"email" : email,"jobId" : jobId ,"date":datetime.datetime.now()})
    return json.dumps(job.getJobFromIds([jobId]))

# Route /job/check/showlater/user/<email>/id/<jobId>
@app.route('/job/check/showlater/user/<email>/id/<jobId>', methods=('GET','POST'))
def ifShowlater(email,jobId):
    user = mongo.db.jobsList.find_one({"email": email,"jobId": jobId})
    if user is not None:
        return 'no'
    return 'yes'

# Route /job/showlater/new/email/<email>/id/<int:jobId>
@app.route('/job/showlater/new/email/<email>/id/<int:jobId>', methods=('GET','POST'))
def new_show_later(email,jobId):
    listOfJobs = mongo.db.jobsList.find_one({"email" : email,"jobId": jobId})
    if listOfJobs is None:
        mongo.db.jobsList.insert({"email" : email,"jobId" : jobId })
        return 'job added successfully !'

    return 'job already exists !'

# Route /job/showlater/email api Page
@app.route('/job/showlater/<email>')
def show_list(email):
    data = []
    for x in mongo.db.jobsList.find({"email": email},{ "_id": 0, "jobId": 1}):
        data.append(x['jobId'])
    print(data)
    return json.dumps(job.getJobFromIds(data))

# Route /job/showlater/delete
@app.route('/job/showlater/delete', methods=('GET','POST'))
def delete_show_later():
    _email = request.args.get("_email")
    _id = request.args.get("_id")
    user = mongo.db.jobsList.delete_one({"email": _email,"jobId": _id})
    if user is None:
        return 'show later no delete successfully !'

    return 'show later delete successfully !'

# Route /job/related/skills/
@app.route('/job/related/id/<int:jobId>', methods=('GET', 'POST'))
def relatedJobs(jobId):
    return json.dumps(job.getJobFromIds([1,2,3,4,5,6]))#json.dumps(job.cosine_similar(jobId))

# Route /job/recommended/skills/
@app.route('/job/recommended/skills/<skills>', methods=('GET', 'POST'))
def recommendedBySkills(skills):
    return json.dumps(jobs.searchBySkills(skills)[:10])

# Route /job/recommended/majors/
@app.route('/job/recommended/majors/<majors>', methods=('GET', 'POST'))
def recommendedByMajors(majors):
    return json.dumps(jobs.searchByMajor(majors)[:10])

# Route /job/recommended/city/
@app.route('/job/recommended/city/<city>', methods=('GET', 'POST'))
def recommendedByCity(city):
    return json.dumps(jobs.searchByCity(city)[:10])

##############################################################################################################

if __name__ == '__main__':
    app.run(port=5002,debug=True)