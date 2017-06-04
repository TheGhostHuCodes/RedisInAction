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
