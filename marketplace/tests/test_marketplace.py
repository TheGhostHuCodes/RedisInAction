import time
from unittest import mock

import pytest
import redis

from marketplace.marketplace import list_item
from tests.utils import redis_conn

pytest.fixture(scope='function', name='conn')(redis_conn)


def _add_to_inventory(conn: redis.StrictRedis, user_id: int, item: str):
    conn.sadd("inventory:{user_id}".format(user_id=user_id), item)


def test_item_in_inventory_can_be_listed(conn: redis.StrictRedis):
    user_id = 1
    item_id = "box"
    _add_to_inventory(conn, user_id, item_id)
    assert list_item(conn, item_id, user_id, 50) is True
    assert conn.zrange('market:', 0, 0)[0] == "{item_id}.{user_id}".format(
        item_id=item_id, user_id=user_id).encode('utf-8')


def test_item_not_in_inventory_can_not_be_listed(conn: redis.StrictRedis):
    assert list_item(conn, item_id="box", seller_id=1, price=50) is None


def test_list_item_returns_false_after_attempting_to_list_for_too_long(
        conn: redis.StrictRedis):
    mock_time = mock.Mock()
    mock_time.side_effect = [time.time(), time.time() + 6]

    with mock.patch('marketplace.marketplace.time.time', mock_time):
        assert list_item(conn, item_id="box", seller_id=1, price=50) is False
