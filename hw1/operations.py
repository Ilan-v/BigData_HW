import time  
import redis
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

current_env = 'docker' # 'docker' for development on docker, 'production' for real server
current_user = 'stud22'

ENV = {"docker": '172.17.0.1',"production":'bdl1.eng.tau.ac.il'}
companiesSet = current_user+":company:names"
ojOSet = current_user+":oj"

def connect_to_redis(port=6379):
    r = redis.StrictRedis(host=ENV[current_env], port=port, socket_connect_timeout=10)
    r.ping() # send ping to verify that a connection established to redis
    print('connected to redis on "{}", port {}'.format(ENV[current_env], port)) 
    return r

def connect_to_mongo(port=27017):
    client = MongoClient(host=ENV[current_env],port=port, connectTimeoutMS=10000)
    client.admin.command('ping') # try to ping mongo server
    print('connected to MongoDB on "{}", port {}'.format(ENV[current_env], port))
    return client

def setup_redis(r):
    pass

def setup_mongo(client):
    db = client[current_user]
    companies = db.companies
    print("companies collection created in {} db".format(current_user))
    return db, companies

def reset_redis(r):
    print("deleting companies set ({})".format(companiesSet))
    r.delete(companiesSet)
    print("deleting open jobs ({})".format(ojOSet))
    r.delete(ojOSet)
    print("redis is clean")

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
    setup_redis(r)
    db, companies = setup_mongo(client)
    print("restart completed")
    return r, client, db, companies


# Operation 1 - Add a new company
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
        raise ValueError("company dict must have company_name field")
    else:
        company_name = company_dict['company_name']
    
    # verify that company name is unique
    if(is_company_exists(r, companies, company_name)):
        raise ValueError("company name already taken")
        
    # and then insert to mongo and redis
    company_dict['jobs_list'] = [] # initiate jobs list
    companies.insert_one(company_dict)
    r.sadd(companiesSet, company_dict['company_name'])
    print("%s Added successfully!"%(company_name))

# Operation 2 - Add a new job position
def generate_job_id(companies, company_name):
    match = {'$match' : {'company_name':company_name}}
    project = {'$project': { 'count': { '$size':'$jobs_list' }}}
    res = companies.aggregate([match,project])
    return list(res)[0]['count'] + 1

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
    r.zincrby(ojOSet,1,"%s:%s"%(job_dict['location'],job_dict['job_title']))
    print("%s Added to %s jobs successfully!"%(job_dict['job_id'], company_name))

# Operation 3 - Add a new application
def is_job_open(companies, company_name, job_id):
    res = companies.find_one(
        { 
            "company_name": company_name,
        },
        { "jobs_list": { "$elemMatch": { "job_id": int(job_id), "status": "open" }}}
    )
    return('jobs_list' in res)

def new_application(candidate, application_time, job_id, company_name, r=None, companies=None):
    if r is None:
        r = connect_to_redis()
    if companies is None:
        companies = connect_to_mongo()[current_user].companies

    if (not is_job_open(companies, company_name, job_id)):
        print("you are trying to apply to a closed job")
        return -1

    #TODO: check email before update
    #TODO: add application (update)

    

if __name__ == '__main__':
    # connect to the databases
    r = connect_to_redis()
    client = connect_to_mongo()

    # clean the databases
    reset_redis(r)
    reset_mongo(client)

    # setup the databases
    db, companies = setup_mongo(client)

    # Operation 1 - Add a new company
    add_company({'company_name':'TAU', 'company_description':'University'}, r=r, companies=companies)

    # Operation 2 - Add a new job position
    add_job({'job_title':'bi developer', 'location': 'Tel Aviv','requirements':['python','big data','mongodb'],'status':'open','publish_date':'01-02-2020'},'TAU', r=r, companies=companies)

    # Operation 3 - Add a new application
    new_application({'candidate_name':'laura', 'email':'laura@gmail.com','linkedin':'https://www.linkedin.com/in/laura/', 'skills': ['python','sql']},'01-02-2020 15:00:00', '1','TAU', r=r, companies=companies)

    res = companies.find_one(
        { 
            "company_name": 'TAU',
        },
        { "jobs_list": { "$elemMatch": { "job_id": 1, "status": "open" }}}
    )
    '''
    res = companies.find_one(
        { 
            "company_name": 'TAU',
        })
    '''
    print(res)