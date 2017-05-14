import time
import unittest.mock as mock

import pytest
from _pytest.fixtures import FixtureRequest
import redis

from vote_on_articles.article_crud import article_vote, get_articles, ONE_WEEK_IN_SECONDS, post_article


@pytest.fixture(scope='function', name='conn')
def redis_conn(request: FixtureRequest) -> redis.StrictRedis:
    conn = redis.Redis()
    request.addfinalizer(conn.flushall)
    return conn


def add_articles(conn: redis.StrictRedis, n: int, user: int=1):
    for i in range(n):
        post_article(conn, user, 'test title {}'.format(i),
                     'test link {}'.format(i))


def test_insert_one_article_returns_article_id_1(conn: redis.StrictRedis):
    article_id = post_article(conn, 1, 'test title', 'test link')
    assert int(article_id) == 1


def test_vote_up_article_increases_vote_count(conn: redis.StrictRedis):
    article_id = post_article(conn, 1, 'test title', 'test link')
    article = 'article:' + article_id
    article_vote(conn, 2, article)
    result = conn.hget(article, 'votes')
    assert int(result) == 2


def test_same_user_can_only_vote_up_article_once(conn: redis.StrictRedis):
    article_id = post_article(conn, 1, 'test title', 'test link')
    article = 'article:' + article_id
    article_vote(conn, 2, article)
    article_vote(conn, 2, article)
    result = conn.hget(article, 'votes')
    assert int(result) == 2


def test_can_not_vote_on_articles_older_than_one_week(conn: redis.StrictRedis):
    mock_time = mock.Mock()
    mock_time.return_value = time.time() + ONE_WEEK_IN_SECONDS + 1

    article_id = post_article(conn, 1, 'test title', 'test link')
    article = 'article:' + article_id
    with mock.patch('vote_on_articles.article_crud.time.time', mock_time):
        article_vote(conn, 2, article)

    result = conn.hget(article, 'votes')
    assert int(result) == 1


def test_get_articles_gets_articles_with_one_vote_in_insert_order(
        conn: redis.StrictRedis):
    add_articles(conn, 5)
    articles = get_articles(conn, 1)
    titles = [article[b'title'] for article in articles]
    assert len(titles) == 5
    assert [
        b'test title 4', b'test title 3', b'test title 2', b'test title 1',
        b'test title 0'
    ] == titles


def test_get_articles_gets_articles_with_many_votes_before_articles_with_one_vote(
        conn: redis.StrictRedis):
    add_articles(conn, 5)
    article_vote(conn, 2, 'article:3')

    articles = get_articles(conn, 1)

    titles = [article[b'title'] for article in articles]
    assert len(titles) == 5
    assert [
        b'test title 2', b'test title 4', b'test title 3', b'test title 1',
        b'test title 0'
    ] == titles