import time

import redis



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
