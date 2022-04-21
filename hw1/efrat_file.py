
# coding: utf-8

# In[18]:

import redis
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import time
import datetime


# # Setup
# functions for connecting and setting up the databases 

# In[99]:

current_env = 'production' # 'docker' for development on docker, 'production' for real server
current_user = 'stud10'
ENV = {"docker": '172.17.0.1',"production":'bdl1.eng.tau.ac.il'}
companiesSet = current_user+":company:names"
ojOSet = current_user+":oj"
cand_app_set = current_user+":candidate_application"
redis_db_num = 10


# ### Connections

# In[100]:

def connect_to_redis(port=6379):
    r = redis.StrictRedis(host=ENV[current_env], port=port, socket_connect_timeout=10)
    r.ping() # send ping to verify that a connection established to redis
    print('connected to redis on "{}", port {}'.format(ENV[current_env], port)) 
    return r

def connect_to_mongo(port=27017):
    if current_env == 'docker':
        client = MongoClient(host=ENV[current_env],port=port, connectTimeoutMS=10000)
        # client.admin.command('ping') # try to ping mongo server
        print('connected to MongoDB on "{}", port {}'.format(ENV[current_env], port))
    
    elif current_env == 'production':
        client = MongoClient()
    return client


# ### Initializiation 

# In[101]:

def setup_mongo(client):
    db = client[current_user]
    companies = db.companies
    print("companies collection created in {} db".format(current_user))
    return db, companies

def reset_redis(r):
    r.flushdb()
    print('redis database number {} is clean'.format(redis_db_num))

def reset_mongo(client):
    client[current_user].companies.drop()
    print("mongo is clean")

def restart():
    # connect to dbs
    r = connect_to_redis()
    client = connect_to_mongo()
    # clean dbs
    reset_redis(r)
    reset_mongo(client)
    # setup the databases
    db, companies = setup_mongo(client)
    print("restart completed")
    return r, client, db, companies


# # Operation 1 - Add a new company

# In[22]:

def is_company_exists(r, companies, company_name):
    # check if company name in comapny set on redis
    if r.sismember(companiesSet, company_name):
        return True
    
    # if not, verify against mongo
    elif companies.find_one({"company_name": company_name}) is not None:
        return True
    
    else:
        return False
    
def add_company(company_dict,r=None, companies=None):
    if r is None:
        r = connect_to_redis()
    if companies is None:
        companies = connect_to_mongo()[current_user].companies
        
    # getting dict for the company and insert it to db
    if 'company_name' not in company_dict:
        # raise ValueError("company dict must have company_name field") #no errors allowed
        print("company dict must have company_name field, no changes were commited to db")
        return None

    else:
        company_name = company_dict['company_name']
    
    # verify that company name is unique
    if(is_company_exists(r, companies, company_name)):
        # raise ValueError("company name already taken") #no errors allowed
        print("company name already taken, no changes were commited to db")
        return None
        
    # and then insert to mongo and redis
    company_dict['jobs_list'] = [] # initiate jobs list
    companies.insert_one(company_dict)
    r.sadd(companiesSet, company_dict['company_name'])
    print("%s Added successfully!"%(company_name))


# # Operation 2 - Add a new job position

# In[23]:

def generate_job_id(companies, company_name):
    match = {'$match' : {'company_name':company_name}}
    project = {'$project': { 'max_id': { '$size':'$jobs_list' }}}
    res = companies.aggregate([match,project])
    return list(res)[0]['max_id'] + 1

def add_job(job_dict, company_name, r=None, companies=None):
    # ASSUMPTION: there is no option to delete jobs (so counting jobs can be used to generate job id)
    if r is None:
        r = connect_to_redis()
    if companies is None:
        companies = connect_to_mongo()[current_user].companies
    if not (is_company_exists(r, companies, company_name)):
        raise ValueError("company doesn't exist")
    
    # generate job id and insert to the company object
    job_dict['job_id'] = generate_job_id(companies, company_name)
    job_dict['application_list'] = [] # initiate application list
    companies.update_one({'company_name': company_name}, {'$push': {'jobs_list': job_dict}}, upsert = True)
    if job_dict['status'] == 'open':
        r.zincrby(ojOSet,"%s:%s"%(job_dict['location'],job_dict['job_title']),amount=1)
    print("job with id %s was added to %s jobs successfully!"%(job_dict['job_id'], company_name))


# # Operation 3 - Add a new application

# In[24]:

def is_job_open(companies, company_name, job_id):
    res = companies.find_one(
        { 
            "company_name": company_name,
        },
        { "jobs_list": { "$elemMatch": { "job_id": int(job_id), "status": "open" }}}
    )
    return('jobs_list' in res)

def is_already_submitted(companies, company_name, job_id, email):
    res = companies.find_one(
        { 
            "company_name": company_name,
        },
        { "jobs_list": { "$elemMatch": { "job_id": int(job_id), "application_list": {"$elemMatch": {'email':email}} }}}
    )
    return('jobs_list' in res) # if mail doesn't exists an empty object will returned

def new_application(candidate, application_time, job_id, company_name, r=None, companies=None):
    if r is None:
        r = connect_to_redis()
    if companies is None:
        companies = connect_to_mongo()[current_user].companies

    if (not is_job_open(companies, company_name, job_id)):
        print("you are trying to apply to a closed job")
        return -1

    if (is_already_submitted(companies, company_name, job_id, candidate['email'])):
        print("you have already sent application for this job")
        return -2

    # update in mongo
    d = datetime.datetime.strptime(application_time, "%d-%m-%Y %H:%M:%S")
    candidate['application_date'] = d
    companies.update_one({"company_name": company_name,"jobs_list": {"$elemMatch":{"job_id":int(job_id)}}},{'$push':{'jobs_list.$.application_list':candidate}})

    #update in redis
    unix_d = time.mktime(d.timetuple())
    #check if company already exists in redis
    key_string = "candidate_applications:{}".format(candidate['email'])
    comps = r.zrange(key_string,0,-1,withscores=True)
    comp = [x for x in comps if x[0]==company_name]
    # if comany already exists - change vlaue to most recent date
    if len(comp)>0:
        old_timestamp = comp[0][1]
        if unix_d>old_timestamp:
            r.zadd(key_string,unix_d,company_name)
    else:
        r.zadd(key_string,unix_d,company_name)
        
    print("{} submited application for job number {} at {}".format(candidate['candidate_name'],job_id, company_name))


# # Operation 4 - Update job status

# In[50]:

def update_job_status(company_name, job_id, new_status, r=None, companies=None):
    if r is None:
        r = connect_to_redis()
    if companies is None:
        companies = connect_to_mongo()[current_user].companies
    
    res = companies.aggregate([
    { "$unwind": "$jobs_list"},
    { "$match": {"jobs_list.job_id": 1, 'company_name':'TAU'}},
    { "$project": { "jobs_list.status" : 1, 'jobs_list.location': 1, 'jobs_list.job_title': 1}}
    ])

    for job in res:
        old_status = job['jobs_list']['status']
        location = job['jobs_list']['location']
        job_title = job['jobs_list']['job_title']
    
    if old_status == 'open' and new_status == 'close':
        r.zincrby(ojOSet,"%s:%s"%(location,job_title),amount=-1)
    
    elif new_status == 'open' and old_status == 'close':
        r.zincrby(ojOSet,"%s:%s"%(location,job_title),amount=1)
    
    companies.update_one({"company_name": company_name,"jobs_list": {"$elemMatch":{"job_id":int(job_id)}}},{'$set':{'jobs_list.$.status':new_status}})
    print("job number: {} at {} is now: {}".format(job_id, company_name, new_status))


# # Operation 5 - show latest companies

# In[26]:

def show_latest_10_companies(candidate_email):
    key_string = "candidate_applications:{}".format(candidate_email)
    return r.zrevrange(key_string, 0, 9, withscores=False)


# # Operation 6 - show number of open jobs

# In[55]:

def show_number_of_jobs(location,title):
    key_string = "%s:%s"%(location,title)
    results = r.zrevrange(ojOSet, 0, -1, withscores=True)
    num = 0
    for res in results:
        if res[0] == key_string:
            num = res[1]
    return num


# # Recovery

# In[113]:

def recovery():
    r = connect_to_redis()
    companies_list = list(companies.find())
    for comp in companies_list:
        company_name = comp['company_name']
        for job in comp['jobs_list']:
                status = job['status']
                location = job['location']
                job_title = job['job_title']
                if status == 'open':
                    r.zincrby(ojOSet,"%s:%s"%(location,job_title),amount=1)

                applications = job['application_list']
                for app in applications:
                    email = app['email']
                    application_date = app['application_date']

                    unix_d = time.mktime(application_date.timetuple())
                    #check if company already exists in redis
                    key_string = "candidate_applications:{}".format(email)
                    comps = r.zrange(key_string, 0, -1, withscores=True)
                    comp = [x for x in comps if x[0]==company_name]
                    # if comany already exists - change vlaue to most recent date
                    if len(comp) > 0:
                        old_timestamp = comp[0][1]
                        if unix_d > old_timestamp:
                            r.zadd(key_string, unix_d, company_name)
                    else:
                        r.zadd(key_string, unix_d, company_name)  


# # Run all operations

# In[28]:

r = connect_to_redis()
client = connect_to_mongo()

# clean the databases
reset_redis(r)
reset_mongo(client)

# setup the databases
db, companies = setup_mongo(client)


# In[29]:

# Operation 1 - Add a new company
add_company({'company_name':'TAU', 'company_description':'University'}, r=r, companies=companies)


# In[30]:

# Operation 2 - Add a new job position
add_job({'job_title':'bi developer', 'location': 'Tel Aviv','requirements':['python','big data','mongodb'],'status':'open','publish_date':'01-02-2020'},'TAU', r=r, companies=companies)


# In[31]:

# Operation 3 - Add a new application
new_application({'candidate_name':'laura', 'email':'laura@gmail.com','linkedin':'https://www.linkedin.com/in/laura/', 'skills': ['python','sql']},'01-02-2020 15:00:00', '1','TAU', r=r, companies=companies)


# In[59]:

# Operation 4 - Update job status
update_job_status('TAU','1','close', r=r, companies=companies)
update_job_status('TAU','1','open', r=r, companies=companies)


# In[33]:

# Operation 5 - Show latest 10 companies
show_latest_10_companies('lebron@gmail.com')


# In[60]:

# Operation 6 - Show number of jobs
show_number_of_jobs('Tel Aviv','bi developer')


# # Tests 

# In[35]:

list(db['companies'].find({}))


# In[36]:

match = {'$match' : {'company_name':'TAU'}}
project = {'$project': { 'max_id': { '$size':'$jobs_list' }}}
# project = {'$project': { 'count': { '$size':'$jobs_list' }}}
res = companies.aggregate([match,project])
list(res)


# In[49]:

res = companies.aggregate([
    { "$unwind": "$jobs_list"},
    { "$match": {"jobs_list.job_id": 1, 'company_name':'TAU'}},
    { "$project": { "jobs_list.status" : 1, 'jobs_list.location': 1, 'jobs_list.job_title': 1}}
    ])

for job in res:
    print job['jobs_list']['location']


# In[ ]:

new_application({'candidate_name':'laura', 'email':'laura@gmail.com','linkedin':'https://www.linkedin.com/in/laura/', 'skills': ['python','sql']},'14-02-2022 15:00:00', '3','TAU', r=r, companies=companies)


# In[ ]:

r.zrange('candidate_applications:laura@gmail.com',0,-1,withscores=True)


# In[ ]:

a = r.zrange('moti:luhim',0,-1,withscores=True)
b = [x for x in a if x[0]=='haim']
b


# In[ ]:

r.keys('stud10:')


# In[ ]:

# r.get('ojOSet')

results = r.zrevrange(ojOSet, 0, -1, withscores=True)
for res in results:
    if res[0] == 'Tel Aviv:bi developer':
        print res[1]

