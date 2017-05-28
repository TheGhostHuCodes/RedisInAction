import time
from typing import Callable, List
import uuid

import redis

SESSION_NUMBER_LIMIT = 10000000


def check_session(conn: redis.StrictRedis, session: uuid.UUID) -> bytes:
    return conn.hget('login:', str(session))


def update_session(conn: redis.StrictRedis,
                   session: uuid.UUID,
                   user: int,
                   item=None) -> None:
    timestamp = time.time()
    conn.hset('login:', str(session), user)
    conn.zadd('recent:', str(session), timestamp)
    if item:
        conn.zadd('viewed:' + str(session), item, timestamp)
        # Keep only the last 25 used ordered by timestamp.
        conn.zremrangebyrank('viewed:' + str(session), 0, -26)


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
    return True


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
