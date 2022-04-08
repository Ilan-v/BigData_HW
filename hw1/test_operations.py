import operations

# Test redis connection
def test_successfull_redis_connection():
    r = operations.connect_to_redis()
    assert(r.ping())

def test_fail_redis_connection():
    # should fail to connect with the Satan's port :P
    try:
        r = operations.connect_to_redis(port=666)
        pytest.fail("could connect with wrong port...")
    except: # there is already ping inside the function so any kind of exception here is fine
        return True


# Test mongo connection
def test_successfull_mongo_connection():
    client = operations.connect_to_mongo()
    assert('ok' in client.admin.command('ping'))

def test_fail_mongo_connection():
    # should fail to connect with the Satan's port :P
    try:
        client = operations.connect_to_mongo(port=666)
        pytest.fail("could connect with wrong port...")
    except: # there is already ping inside the function so any kind of exception here is fine
        return True



