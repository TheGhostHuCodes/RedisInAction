import json
import time
from typing import Callable, List, Optional
import uuid

import collections
import redis

SESSION_NUMBER_LIMIT = 10000000

Item = collections.namedtuple('Item', ['name', 'quantity', 'description'])


def check_session(conn: redis.StrictRedis, session: uuid.UUID) -> bytes:
    return conn.hget('login:', str(session))


def update_session(conn: redis.StrictRedis,
                   session: uuid.UUID,
                   user: int,
                   item: Optional[Item]=None) -> None:
    timestamp = time.time()
    conn.hset('login:', str(session), user)
    conn.zadd('recent:', str(session), timestamp)
    if item:
        conn.zadd('viewed:' + str(session), item, timestamp)
        # Keep only the last 25 used ordered by timestamp.
        conn.zremrangebyrank('viewed:' + str(session), 0, -26)
        conn.zincrby('viewed:', item, -1)


def clean_sessions(conn: redis.StrictRedis) -> None:
    size = conn.zcard('recent:')
    if size <= SESSION_NUMBER_LIMIT:
        return
    end_index = min(size - SESSION_NUMBER_LIMIT, 100)
    sessions = conn.zrange('recent:', 0, end_index - 1)  # type: List[bytes]

    session_keys = []
    for session in sessions:
        session_keys.append('viewed:' + session.decode('utf-8'))
        session_keys.append('cart:' + session.decode('utf-8'))

    conn.delete(*session_keys)
    conn.hdel('login:', *sessions)
    conn.zrem('recent:', *sessions)


def add_to_cart(conn: redis.StrictRedis,
                session: uuid.UUID,
                item: str,
                count: int) -> None:
    if count <= 0:
        conn.hdel('cart:' + str(session), item)
    else:
        conn.hset('cart:' + str(session), item, count)


def _can_cache(conn: redis.StrictRedis, request: str) -> bool:
    rank = conn.zrank('viewed:', request)
    return rank is not None and rank < 10000


def _hash_request(request: str) -> int:
    return hash(request)


def cache_request(conn: redis.StrictRedis,
                  request: str,
                  callback: Callable[[
                      str,
                  ], str]) -> str:
    if not _can_cache(conn, request):
        return callback(request)

    page_key = 'cache:' + str(_hash_request(request))
    content = conn.get(page_key)

    if not content:
        content = callback(request)
        conn.setex(page_key, content, 300)

    return content


def schedule_row_cache(conn: redis.StrictRedis, row_id: str,
                       delay: float) -> None:
    conn.zadd('delay:', row_id, delay)
    conn.zadd('schedule:', row_id, time.time())


def get_inventory(row_id: str) -> Item:
    return Item(
        name='my_item',
        quantity=42,
        description='The answer to life, the universe and everything.')


def cache_next_row(conn: redis.StrictRedis) -> None:
    next_row = conn.zrange('schedule:', 0, 0, withscores=True)
    now = time.time()
    if not next_row or next_row[0][1] > now:
        return
    row_id = next_row[0][0].decode('utf-8')

    delay = conn.zscore('delay:', row_id)
    if delay <= 0:
        conn.zrem('delay:', row_id)
        conn.zrem('schedule:', row_id)
        conn.delete('inv:', row_id)
        return

    row = get_inventory(row_id)
    conn.zadd('schedule:', row_id, now + delay)
    conn.set('inv:' + row_id, json.dumps(row._asdict()))


def rescale_viewed(conn: redis.StrictRedis) -> None:
    conn.zremrangebyrank('viewed:', 20000, -1)
    conn.zinterstore('viewed:', {'viewed:': 0.5})
