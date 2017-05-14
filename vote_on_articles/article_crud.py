import time
from typing import List, Optional

import redis

ONE_WEEK_IN_SECONDS = 7 * 86400
VOTE_SCORE = 432


def post_article(conn: redis.StrictRedis, user: int, title: str,
                 link: str) -> str:
    article_id = str(conn.incr('article:'))

    voted = 'up_voted:' + article_id
    conn.sadd(voted, user)
    conn.expire(voted, ONE_WEEK_IN_SECONDS)

    now = time.time()
    article = 'article:' + article_id
    conn.hmset(article, {
        'title': title,
        'link': link,
        'poster': user,
        'time': now,
        'votes': 1,
    })

    conn.zadd('score:', article, now + VOTE_SCORE)
    conn.zadd('time:', article, now)

    return article_id


ARTICLES_PER_PAGE = 25


def get_articles(conn: redis.StrictRedis, page: int,
                 order: str='score:') -> List[dict]:
    start = (page - 1) * ARTICLES_PER_PAGE
    end = start + ARTICLES_PER_PAGE

    ids = conn.zrevrange(order, start, end)
    articles = []
    for article_id in ids:
        article_data = conn.hgetall(article_id)
        article_data[b'id'] = article_id
        articles.append(article_data)
    return articles


def article_vote(conn: redis.StrictRedis,
                 user: int,
                 article: str,
                 direction: str='up') -> None:
    cutoff = time.time() - ONE_WEEK_IN_SECONDS
    if conn.zscore('time:', article) < cutoff:
        return

    article_id = article.partition(':')[-1]
    if direction == 'up':
        if conn.smove('down_voted:' + article_id, 'up_voted:' + article_id,
                      user):
            conn.zincrby('score:', article, 2 * VOTE_SCORE)
            conn.hincrby(article, 'votes', 2)
        elif conn.sadd('up_voted:' + article_id, user):
            conn.zincrby('score:', article, VOTE_SCORE)
            conn.hincrby(article, 'votes', 1)
    if direction == 'down':
        if conn.smove('up_voted:' + article_id, 'down_voted:' + article_id,
                      user):
            conn.zincrby('score:', article, -2 * VOTE_SCORE)
            conn.hincrby(article, 'votes', -2)
        elif conn.sadd('down_voted:' + article_id, user):
            conn.zincrby('score:', article, -VOTE_SCORE)
            conn.hincrby(article, 'votes', -1)


def add_remove_groups(conn: redis.StrictRedis,
                      article_id: str,
                      to_add: Optional[List[str]]=None,
                      to_remove: Optional[List[str]]=None) -> None:
    article = 'article:' + article_id
    _to_add = to_add if to_add else []
    _to_remove = to_remove if to_remove else []
    for group in _to_add:
        conn.sadd('group:' + group, article)
    for group in _to_remove:
        conn.srem('group:' + group, article)


def get_group_articles(conn: redis.StrictRedis,
                       group: str,
                       page: int,
                       order: str='score:') -> List[dict]:
    key = order + group
    if not conn.exists(key):
        conn.zinterstore(key, ['group:' + group, order], aggregate='max')
        conn.expire(key, 60)
    return get_articles(conn, page, key)
