import time
import unittest.mock as mock

import pytest
import redis

from vote_on_articles.article_crud import (add_remove_groups, article_vote,
                                           get_articles, get_group_articles,
                                           ONE_WEEK_IN_SECONDS, post_article)
from tests.utils import redis_conn

pytest.fixture(scope='function', name='conn')(redis_conn)


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


def test_article_can_be_added_to_a_single_group(conn: redis.StrictRedis):
    article_id = post_article(conn, 1, 'test title', 'test link')
    add_remove_groups(conn, article_id, ['test_group'])
    test_group_articles = get_group_articles(conn, 'test_group', 1)
    assert len(test_group_articles) == 1


def test_article_can_be_added_and_removed_from_a_single_group(
        conn: redis.StrictRedis):
    article_id = post_article(conn, 1, 'test title', 'test link')

    add_remove_groups(conn, article_id, to_add=['test_group'])
    test_group_articles = get_group_articles(conn, 'test_group', 1)
    assert len(test_group_articles) == 1

    # The intersection ZSET score:test_group is cached for 60 seconds. Let's
    # look at the SET membership instead.
    add_remove_groups(conn, article_id, to_remove=['test_group'])
    article_ids_in_test_group = conn.smembers('group:test_group')
    assert len(article_ids_in_test_group) == 0


def test_vote_down_article_decreases_vote_count(conn: redis.StrictRedis):
    article_id = post_article(conn, 1, 'test title', 'test link')
    article = 'article:' + article_id
    article_vote(conn, 2, article, direction='down')
    result = conn.hget(article, 'votes')
    assert int(result) == 0


def test_same_user_can_only_vote_down_article_once(conn: redis.StrictRedis):
    article_id = post_article(conn, 1, 'test title', 'test link')
    article = 'article:' + article_id
    article_vote(conn, 2, article, direction='down')
    article_vote(conn, 2, article, direction='down')
    result = conn.hget(article, 'votes')
    assert int(result) == 0


def test_same_user_can_vote_down_then_up(conn: redis.StrictRedis):
    article_id = post_article(conn, 1, 'test title', 'test link')
    article = 'article:' + article_id
    article_vote(conn, 2, article, direction='down')
    article_vote(conn, 2, article, direction='up')
    result = conn.hget(article, 'votes')
    assert int(result) == 2


def test_same_user_can_vote_up_then_down(conn: redis.StrictRedis):
    article_id = post_article(conn, 1, 'test title', 'test link')
    article = 'article:' + article_id
    article_vote(conn, 2, article, direction='up')
    article_vote(conn, 2, article, direction='down')
    result = conn.hget(article, 'votes')
    assert int(result) == 0
