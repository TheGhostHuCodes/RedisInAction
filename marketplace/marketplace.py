import time
from typing import NamedTuple, Optional

import redis

User = NamedTuple('User', [('name', str), ('funds', int)])


def list_item(conn: redis.StrictRedis,
              item_id: str,
              seller_id: int,
              price: int) -> Optional[bool]:
    inventory = "inventory:{}".format(seller_id)
    item = "{}.{}".format(item_id, seller_id)
    end = time.time() + 5
    pipe = conn.pipeline()

    while time.time() < end:
        try:
            pipe.watch(inventory)
            if not pipe.sismember(inventory, item_id):
                pipe.unwatch()
                return None

            pipe.multi()
            pipe.zadd("market:", item, price)
            pipe.srem(inventory, item_id)
            pipe.execute()
            return True
        except redis.exceptions.WatchError:
            pass
    return False


def purchase_item(conn: redis.StrictRedis,
                  buyer_id: int,
                  item_id: str,
                  seller_id: int,
                  list_price: int):
    buyer = "users:{buyer_id}".format(buyer_id=buyer_id)
    seller = "users:{seller_id}".format(seller_id=seller_id)
    item = "{item_id}.{seller_id}".format(item_id=item_id, seller_id=seller_id)
    inventory = "inventory:{buyer_id}".format(buyer_id=buyer_id)
    end = time.time() + 10
    pipe = conn.pipeline()

    while time.time() < end:
        try:
            pipe.watch("market:", buyer)
            price = pipe.zscore("market:", item)
            funds = int(pipe.hget(buyer, "funds"))
            if price != list_price or price > funds:
                pipe.unwatch()
                return None

            pipe.multi()
            pipe.hincrby(seller, "funds", int(price))
            pipe.hincrby(buyer, "funds", int(-price))
            pipe.sadd(inventory, item_id)
            pipe.zrem("market:", item)
            pipe.execute()
            return True
        except redis.exceptions.WatchError:
            pass
    return False
