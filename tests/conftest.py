# -*- coding: utf-8 -*-
"""Defines fixtures available to all tests."""

import pytest
from flask_webtest import TestApp

from doorman.application import create_app
from doorman.database import db as _db
from doorman.models import Rule
from doorman.settings import TestConfig

from .factories import NodeFactory, PackFactory, RuleFactory, TagFactory


@pytest.yield_fixture(scope='function')
def app():
    """An application for the tests."""
    _app = create_app(config=TestConfig)
    ctx = _app.test_request_context()
    ctx.push()

    try:
        yield _app
    finally:
        ctx.pop()


@pytest.yield_fixture(scope='function')
def api():
    """An api instance for the tests, no manager"""
    import os
    # the mere presence of the env var should prevent the manage
    # blueprint from being registered
    os.environ['DOORMAN_NO_MANAGER'] = '1'

    _app = create_app(config=TestConfig)
    ctx = _app.test_request_context()
    ctx.push()

    try:
        yield _app
    finally:
        ctx.pop()


@pytest.fixture(scope='function')
def testapp(app, db):
    """A Webtest app."""
    return TestApp(app, db)


@pytest.fixture(scope='function')
def testapi(api, db):
    return TestApp(api, db)


@pytest.yield_fixture(scope='function')
def db(app):
    """A database for the tests."""
    _db.app = app
    with app.app_context():
        _db.create_all()

    yield _db

    # Explicitly close DB connection
    _db.session.close()
    _db.drop_all()


@pytest.fixture
def node(db):
    """A node for the tests."""
    node = NodeFactory(host_identifier='foobar', enroll_secret='foobar')
    db.session.commit()
    return node


@pytest.fixture
def rule(db):
    rule = RuleFactory(
        name='testrule',
        description='kung = $kung',
        alerters=[],
        conditions={}
    )
    db.session.commit()
    return rule


@pytest.fixture
def pack(db):
    pack = PackFactory(
        name='foobar pack',
        description='this is the foobar pack',
    )
    db.session.commit()
    return pack


@pytest.fixture
def tag(db):
    tag = TagFactory(
        value='foobar'
    )
    db.session.commit()
    return tag
