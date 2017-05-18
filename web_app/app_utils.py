import time
from typing import List

import redis

SESSION_NUMBER_LIMIT = 10000000


def check_token(conn: redis.StrictRedis, token: str) -> bytes:
    return conn.hget('login:', token)


def update_token(conn: redis.StrictRedis, token: str, user: int,
                 item=None) -> None:
    timestamp = time.time()
    conn.hset('login:', token, user)
    conn.zadd('recent:', token, timestamp)
    if item:
        conn.zadd('viewed:' + token, item, timestamp)
        # Keep only the last 25 used ordered by timestamp.
        conn.zremrangebyrank('viewed:' + token, 0, -26)


def clean_sessions(conn: redis.StrictRedis) -> None:
    size = conn.zcard('recent:')
    if size <= SESSION_NUMBER_LIMIT:
        return
    end_index = min(size - SESSION_NUMBER_LIMIT, 100)
    tokens = conn.zrange('recent:', 0, end_index - 1)  # type: List[bytes]

    session_keys = []
    for token in tokens:
        session_keys.append('viewed:' + token.decode('utf-8'))

    conn.delete(*session_keys)
    conn.hdel('login:', *tokens)
    conn.zrem('recent:', *tokens)
