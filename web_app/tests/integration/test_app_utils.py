from unittest.mock import patch
import uuid

import pytest
import redis

from tests.utils import redis_conn
from web_app.app_utils import check_token, update_token, clean_sessions

pytest.fixture(scope='function', name='conn')(redis_conn)


def test_login_updates_token(conn: redis.StrictRedis):
    test_token = uuid.uuid4()
    update_token(conn, test_token, 42)
    assert check_token(conn, test_token) == b'42'


TEST_SESSION_NUMBER_LIMIT = 10


@patch("web_app.app_utils.SESSION_NUMBER_LIMIT", TEST_SESSION_NUMBER_LIMIT)
def test_sessions_cleaned_when_sessions_exceed_session_number_limit(
        conn: redis.StrictRedis):
    for user_id in range(TEST_SESSION_NUMBER_LIMIT + 1):
        update_token(conn, uuid.uuid4(), user_id)
    clean_sessions(conn)
    assert conn.hlen('login:') == TEST_SESSION_NUMBER_LIMIT


@patch("web_app.app_utils.SESSION_NUMBER_LIMIT", TEST_SESSION_NUMBER_LIMIT)
def test_sessions_not_cleaned_when_sessions_below_session_number_limit(
        conn: redis.StrictRedis):
    for user_id in range(TEST_SESSION_NUMBER_LIMIT - 1):
        update_token(conn, uuid.uuid4(), user_id)
    clean_sessions(conn)
    assert conn.hlen('login:') == TEST_SESSION_NUMBER_LIMIT - 1
