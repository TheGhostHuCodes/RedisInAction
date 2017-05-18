import uuid

import pytest
import redis

from tests.utils import redis_conn
from web_app.app_utils import check_token, update_token

pytest.fixture(scope='function', name='conn')(redis_conn)


def test_login_updates_token(conn: redis.StrictRedis):
    test_token = uuid.uuid4()
    update_token(conn, test_token, 42)
    assert check_token(conn, test_token) == b'42'
