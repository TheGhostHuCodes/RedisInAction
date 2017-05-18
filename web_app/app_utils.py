import time
from typing import List
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

    conn.delete(*session_keys)
    conn.hdel('login:', *sessions)
    conn.zrem('recent:', *sessions)
