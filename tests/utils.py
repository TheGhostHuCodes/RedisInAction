from _pytest.fixtures import FixtureRequest
import redis


def redis_conn(request: FixtureRequest) -> redis.StrictRedis:
    conn = redis.Redis()
    request.addfinalizer(conn.flushall)
    return conn
