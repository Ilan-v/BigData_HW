import pytest
from operations import *

# Test redis connection
def test_successfull_redis_connection():
    r = connect_to_redis()
    assert(r.ping())

def test_fail_redis_connection():
    # should fail to connect with the Satan's port :P
    try:
        connect_to_redis(port=666)
        pytest.fail("could connect with wrong port...")
    except: # there is already ping inside the function so any kind of exception here is fine
        return True


# Test mongo connection
def test_successfull_mongo_connection():
    client = connect_to_mongo()
    assert('ok' in client.admin.command('ping'))

@pytest.mark.slow
def test_fail_mongo_connection():
    # should fail to connect with the Satan's port :P
    try:
        connect_to_mongo(port=666)
        pytest.fail("could connect with wrong port...")
    except: # there is already ping inside the function so any kind of exception here is fine
        return True

#  Test resets and setup
def test_mongo_setup():
    client = connect_to_mongo()
    db, companies = setup_mongo(client) # setup mongo
    # insert dummy record, mongo won't create db untill it has to insert record
    companies.insert_one({'company_name':'TAU', 'company_description':'University'})

    if (bool(current_user not in client.list_database_names())):
        pytest.fail("db '{}' not found on mongo".format(current_user))
    else:
        if (bool('companies' not in db.list_collection_names())):
            pytest.fail("collection 'companies' not found on mongo")
    return True

def test_mongo_reset():
    client = connect_to_mongo()
    _, _ = setup_mongo(client)
    reset_mongo(client)

    if (bool(current_user in client.list_database_names())):
        pytest.fail("db '{}' found on mongo after reset".format(current_user))
    else:
        return True

def test_redis_setup():
    return True

def test_redis_reset():
    r = connect_to_redis()
    r.sadd(companiesSet, "ibm") # add a set
    reset_redis(r)
    assert(not r.sismember(companiesSet,'ibm'))
    # TODO: add test for oj

# Test Operation 1
def test_successfull_add_company():
    r, _, _, companies = restart()
    add_company({'company_name':'TAU', 'company_description':'University'})
    if (b'TAU' not in r.smembers(companiesSet)):
        pytest.fail("failed to add tau to set on redis '{}'".format(r.smembers(companiesSet)))
    if (companies.find_one({"company_name": 'TAU'}) is None):
        pytest.fail("failed to add tau to companies collection on redis")

def test_duplicate_add_company():
    restart()
    add_company({'company_name':'TAU', 'company_description':'University'})
    try:
        add_company({'company_name':'TAU', 'company_description':'Another University'})
        pytest.fail("two companies with same name added to the db")
    except:
        return True