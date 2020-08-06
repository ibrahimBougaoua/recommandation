from initialisation import init

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity

class jobs():
    """docstring for jobs"""
    def __init__(self):
        self.jobs=init.jobs
        self.jobs=self.jobs.fillna("Rien")
        self.cosine_sim=self.train()


    def train(self):

        cv = CountVectorizer() 
        count_matrix = cv.fit_transform(self.jobs["combined_features"])    
        cosine_sim = cosine_similarity(count_matrix)    

        print(type(cv))
        print(type(count_matrix))
        print(type(cosine_sim))

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

    def getJobFromIds(self,ids):
        liste=[]
        liste2=[]
        for id in ids :
            liste=liste+self.jobs[self.jobs['jobId']==id].values.tolist()
        for elem in liste :
            elem.pop(4)
            liste2.append(elem)
        return liste2

    def getJobsOfferInACity(self,city):
        jobs=self.jobs[self.jobs['Location']==city]
        return jobs['jobId'].to_list()

    def getJobTitlesFromAComany(company):
        return self.jobs[self.jobs['Company']==company]['Title'].value_counts().keys().tolist()

    def getJobsFromAComapnyAndATitle(company,title):
        return self.jobs[(self.jobs['Company']==company) & (self.jobs['Title']==title)]['jobId'].values.tolist()

    def cosine_similar(self,ID):
        print(type(self.cosine_sim))
        similar_books = list(enumerate(self.cosine_sim[ID]))
        sorted_similar_books = sorted(similar_books,key=lambda x:x[1],reverse=True)[1:]
        
        similar=[]
        for element in sorted_similar_books:
            
            similar.append(element[0])
        return similar[:20]
        
