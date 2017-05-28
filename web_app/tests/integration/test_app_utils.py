from unittest.mock import patch
import uuid

import pytest
import redis

from tests.utils import redis_conn
from web_app.app_utils import (check_session, update_session, clean_sessions,
                               add_to_cart, cache_request)

pytest.fixture(scope='function', name='conn')(redis_conn)


def test_login_updates_token(conn: redis.StrictRedis):
    test_token = uuid.uuid4()
    update_session(conn, test_token, 42)
    assert check_session(conn, test_token) == b'42'


TEST_SESSION_NUMBER_LIMIT = 10


@patch("web_app.app_utils.SESSION_NUMBER_LIMIT", TEST_SESSION_NUMBER_LIMIT)
def test_sessions_cleaned_when_sessions_exceed_session_number_limit(
        conn: redis.StrictRedis):
    for user_id in range(TEST_SESSION_NUMBER_LIMIT + 1):
        update_session(conn, uuid.uuid4(), user_id)
    clean_sessions(conn)
    assert conn.hlen('login:') == TEST_SESSION_NUMBER_LIMIT


@patch("web_app.app_utils.SESSION_NUMBER_LIMIT", TEST_SESSION_NUMBER_LIMIT)
def test_sessions_not_cleaned_when_sessions_below_session_number_limit(
        conn: redis.StrictRedis):
    for user_id in range(TEST_SESSION_NUMBER_LIMIT - 1):
        update_session(conn, uuid.uuid4(), user_id)
    clean_sessions(conn)
    assert conn.hlen('login:') == TEST_SESSION_NUMBER_LIMIT - 1


def test_adding_item_to_cart_adds_items(conn: redis.StrictRedis):
    test_token = uuid.uuid4()
    update_session(conn, test_token, 42)
    add_to_cart(conn, test_token, 'Horse', 8)
    assert conn.hget('cart:' + str(test_token), b'Horse') == b'8'


def test_adding_negative_count_ofitem_to_cart_removes_item(
        conn: redis.StrictRedis):
    test_token = uuid.uuid4()
    update_session(conn, test_token, 42)
    add_to_cart(conn, test_token, 'Horse', 8)
    add_to_cart(conn, test_token, 'Horse', -1)
    assert conn.hget('cart:' + str(test_token), b'Horse') is None


def test_cache_request_caches_echo_request(conn: redis.StrictRedis):
    my_request = "My Request"
    cache_request(conn, my_request, lambda x: x)
    assert conn.get('cache:' + str(hash(my_request))) == my_request.encode(
        'utf-8')
