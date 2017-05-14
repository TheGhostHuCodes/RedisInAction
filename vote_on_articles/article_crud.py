import time
from typing import List

import redis

ONE_WEEK_IN_SECONDS = 7 * 86400
VOTE_SCORE = 432


def post_article(conn: redis.StrictRedis, user: int, title: str,
                 link: str) -> str:
    article_id = str(conn.incr('article:'))

    voted = 'voted:' + article_id
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


def article_vote(conn: redis.StrictRedis, user: int, article: str) -> None:
    cutoff = time.time() - ONE_WEEK_IN_SECONDS
    if conn.zscore('time:', article) < cutoff:
        return

    article_id = article.partition(':')[-1]
    if conn.sadd('voted:' + article_id, user):
        conn.zincrby('score:', article, VOTE_SCORE)
        conn.hincrby(article, 'votes', 1)
