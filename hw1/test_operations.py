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
def test_add_company():
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

# Test Operation 2
def test_generate_job_id():
    r, _, _, companies = restart()
    add_company({'company_name':'TAU', 'company_description':'University'})
    add_job({'job_title':'frontend developer', 'location': 'Tel Aviv','requirements':['python','big data','mongodb'],'status':'open','publish_date':'01-02-2020'},'TAU', r=r, companies=companies)
    add_job({'job_title':'backend developer', 'location': 'Tel Aviv','requirements':['python','big data','mongodb'],'status':'open','publish_date':'01-02-2020'},'TAU', r=r, companies=companies)
    add_job({'job_title':'fullstack developer', 'location': 'Tel Aviv','requirements':['python','big data','mongodb'],'status':'open','publish_date':'01-02-2020'},'TAU', r=r, companies=companies)
    # get job ids and compare them to [1,2,3]
    jobs = companies.find_one(
        { 
            "company_name": 'TAU',
            "jobs_list.job_id": 3
        },
        { "jobs_list": 1 }
    )
    assert(list(i['job_id'] for i in jobs['jobs_list']) == [1,2,3])

def test_add_job():
    r, _, _, companies = restart()
    add_company({'company_name':'TAU', 'company_description':'University'})
    add_job({'job_title':'fullstack developer', 'location': 'Tel Aviv','requirements':['python','big data','mongodb'],'status':'open','publish_date':'01-02-2020'},'TAU', r=r, companies=companies)
    assert(b'Tel Aviv:fullstack developer' in r.zrange(ojOSet, 0, -1))

# Test Operation 3
def test_is_job_open():
    r, _, _, companies = restart()
    add_company({'company_name':'TAU', 'company_description':'University'})
    add_job({'job_title':'fullstack developer', 'location': 'Tel Aviv','requirements':['python','big data','mongodb'],'status':'open','publish_date':'01-02-2020'},'TAU', r=r, companies=companies)
    if (not is_job_open(companies, 'TAU', 1)):
        pytest.fail("open job not marked as open")
    add_company({'company_name':'IBM', 'company_description':'...'})
    add_job({'job_title':'fullstack developer', 'location': 'Tel Aviv','requirements':['python','big data','mongodb'],'status':'open','publish_date':'01-02-2020'},'IBM', r=r, companies=companies)
    if (not is_job_open(companies, 'TAU', 1)):
        pytest.fail("open job in a different company affected the results")
    add_job({'job_title':'fullstack developer', 'location': 'Tel Aviv','requirements':['python','big data','mongodb'],'status':'closed','publish_date':'01-02-2020'},'TAU', r=r, companies=companies)
    if (is_job_open(companies, 'TAU', 2)):
        pytest.fail("closed job marked as open")

def test_duplicate_email():
    r, _, _, companies = restart()
    add_company({'company_name':'TAU', 'company_description':'University'})
    add_job({'job_title':'fullstack developer', 'location': 'Tel Aviv','requirements':['python','big data','mongodb'],'status':'open','publish_date':'01-02-2020'},'TAU', r=r, companies=companies)
    new_application({'candidate_name':'laura', 'email':'laura@gmail.com','linkedin':'https://www.linkedin.com/in/laura/', 'skills': ['python','sql']},'01-02-2020 15:00:00', '1','TAU')
    a = new_application({'candidate_name':'laura', 'email':'laura@gmail.com','linkedin':'https://www.linkedin.com/in/laura/', 'skills': ['python','sql']},'01-02-2020 15:00:00', '1','TAU')
    if a is not -2:
        pytest.fail("two application with same email added to the db")
    else:
        return True


# Test Operation 4
def test_update_job_status():
    r, _, _, companies = restart()
    add_company({'company_name':'TAU', 'company_description':'University'})
    add_job({'job_title':'fullstack developer', 'location': 'Tel Aviv','requirements':['python','big data','mongodb'],'status':'open','publish_date':'01-02-2020'},'TAU', r=r, companies=companies)
    update_job_status('TAU','1','close', r=r, companies=companies)
    if (is_job_open(companies, 'TAU', 1)):
        pytest.fail("job status doesn't changed")
    update_job_status('TAU','1','open', r=r, companies=companies)
    if (not is_job_open(companies, 'TAU', 1)):
        pytest.fail("job status doesn't changed to open again")


