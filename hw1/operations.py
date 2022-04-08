import time  
import redis
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

current_env = 'docker' # 'docker' for development on docker, 'production' for real server
current_user = 'stud22'

ENV = {"docker": '172.17.0.1',"production":'bdl1.eng.tau.ac.il'}

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
