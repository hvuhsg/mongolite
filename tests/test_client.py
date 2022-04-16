import os
import shutil

import pytest

from mongolite import MongoClient


@pytest.fixture(scope="function")
def client():
    with MongoClient(dirpath='test', database='default_db') as client:
        yield client

    shutil.rmtree('test')


@pytest.fixture(scope="function")
def client_without_default():
    with MongoClient(dirpath='test') as client:
        yield client

    try:
        os.rmdir('test')
    except OSError:
        pass


def test_create_dir():
    with MongoClient(dirpath='test_client') as client:
        client.get_database('db_test')
        client.drop_database('db_test')

    assert os.path.exists('test_client')
    shutil.rmtree('test_client')


def test_create_db(client):
    db = client.get_database('db')

    assert os.path.exists(client.path + "/db")


def test_drop_db(client):
    db = client.get_database('db')
    client.drop_database(db.name)

    assert not os.path.exists(client.path + "/db")


def test_get_default_db(client):
    db = client.get_default_database()

    assert os.path.exists(client.path + "/default_db")


def test_get_default_with_param(client):
    db = client.get_default_database(default='def')

    assert os.path.exists(client.path + "/def")
