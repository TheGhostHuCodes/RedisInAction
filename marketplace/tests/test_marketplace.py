import time
from typing import Set
from unittest import mock

import pytest
import redis

from marketplace.marketplace import list_item, purchase_item, User
from tests.utils import redis_conn

pytest.fixture(scope='function', name='conn')(redis_conn)


def _add_to_inventory(conn: redis.StrictRedis, user_id: int, item: str):
    conn.sadd("inventory:{user_id}".format(user_id=user_id), item)


def _add_user(conn: redis.StrictRedis, user_id: int, user: User) -> None:
    user_key = "users:{user_id}".format(user_id=user_id)
    conn.hmset(user_key, {"name": user.name, "funds": user.funds})


def _get_user_funds(conn: redis.StrictRedis, user_id: int) -> int:
    user_key = "users:{user_id}".format(user_id=user_id)
    return int(conn.hget(user_key, "funds").decode('utf-8'))


def _get_user_inventory(conn: redis.StrictRedis, user_id: int) -> Set[str]:
    inventory = "inventory:{user_id}".format(user_id=user_id)
    return {s.decode('utf-8') for s in conn.smembers(inventory)}


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


def test_item_in_marketplace_can_be_bought(conn: redis.StrictRedis):
    seller_id = 1
    buyer_id = 2
    item_id = "box"
    _add_user(conn, seller_id, User(name="Frank", funds=0))
    _add_user(conn, buyer_id, User(name="Sam", funds=51))
    _add_to_inventory(conn, seller_id, item_id)

    assert list_item(conn, item_id, seller_id, 50) is True
    assert purchase_item(conn, buyer_id, item_id, seller_id, 50) is True
    assert _get_user_funds(conn, seller_id) == 50
    assert _get_user_funds(conn, buyer_id) == 1
    assert _get_user_inventory(conn, buyer_id) == {
        item_id,
    }
    assert _get_user_inventory(conn, seller_id) == set()


def test_item_can_not_be_bought_with_insufficient_funds(
        conn: redis.StrictRedis):
    seller_id = 1
    buyer_id = 2
    item_id = "box"
    _add_user(conn, seller_id, User(name="Frank", funds=0))
    _add_user(conn, buyer_id, User(name="Sam", funds=1))
    _add_to_inventory(conn, seller_id, item_id)

    assert list_item(conn, item_id, seller_id, 50) is True
    assert purchase_item(conn, buyer_id, item_id, seller_id, 50) is None
    assert _get_user_funds(conn, seller_id) == 0
    assert _get_user_funds(conn, buyer_id) == 1
