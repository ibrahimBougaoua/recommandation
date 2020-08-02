from initialisation import init

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity

class jobs():
    """docstring for jobs"""
    def __init__(self):
        self.jobs=init.jobs
        self.cosine_sim=self.train()

    def train(self):

        cv = CountVectorizer() 
        count_matrix = cv.fit_transform(self.jobs["combined_features"])    
        cosine_sim = cosine_similarity(count_matrix)    

    def getJobOffersFromCoumpany(self,company):
        jobs=self.jobs[self.jobs["Company"]==company]
        return jobs['jobId'].to_list()  

    def getJobOffersFromMajor(self,major):
        jobs=self.jobs[self.jobs["Title"]==major]
        return jobs['jobId'].to_list()

    def getJobOffersBasedOnSkillsNeeded(self,skill):
        self.jobs['check']= self.jobs['Skills'].apply(lambda x: 'true' if skill.lower() in map(str.lower,x) else 'False')
        jobs=self.jobs[self.jobs['check']=='true']
    
        return jobs['jobId'].to_list()

    def getJobFromId(self,id):
        return self.jobs[self.jobs['jobId']==id]

    def getJobsFromIds(self,Ids):
        listJobs=[]
        for id in Ids :
            listJobs=listJobs+self.jobs[self.jobs['jobId']==id].values.tolist()
        return listJobs

    def getJobFromIds(self,ids):
        liste=[]
        liste2=array()
        for id in ids :
            liste=liste+self.jobs[self.jobs['jobId']==id].values.tolist()
        for elem in liste :
            elem.pop(0)
            elem.pop(2)
            elem.pop(4)
            elem.pop(4)
            elem.pop(4)
            liste2.append(elem)
        return liste2


    def getJobsOfferInACity(self,city):
        jobs=self.jobs[self.jobs['Location']==city]
        return jobs['jobId'].to_list()

    def cosine_similar(self,ID):
        
        similar_books = list(enumerate(self.cosine_sim[ID]))
        sorted_similar_books = sorted(similar_books,key=lambda x:x[1],reverse=True)[1:]
        
        similar=[]
        for element in sorted_similar_books:
            
            similar.append(element[0])
        return similar[:20]
        
