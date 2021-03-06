# stdlib
import time
import contextlib

# 3rd party
import psycopg2
from nose.tools import eq_
from nose.plugins.attrib import attr

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
)

# project
from ddtrace import Tracer
from ddtrace.contrib.sqlalchemy import trace_engine
from ddtrace.ext import sql as sqlx
from ddtrace.ext import errors as errorsx
from ddtrace.ext import net as netx

# testing
from ..config import POSTGRES_CONFIG
from ...test_tracer import DummyWriter


Base = declarative_base()


class Player(Base):
    __tablename__ = 'players'

    id = Column(Integer, primary_key=True)
    name = Column(String)


def test_sqlite():
    engine_args = {'url': 'sqlite:///:memory:'}
    meta = {sqlx.DB: ":memory:"}
    _test_create_engine(engine_args, "sqlite-foo", "sqlite", meta)


@attr('postgres')
def test_postgres():
    u = 'postgresql://%(user)s:%(password)s@%(host)s:%(port)s/%(dbname)s' % POSTGRES_CONFIG
    engine_args = {'url' : u}
    meta = {
        sqlx.DB: POSTGRES_CONFIG["dbname"],
        netx.TARGET_HOST: POSTGRES_CONFIG['host'],
        netx.TARGET_PORT: str(POSTGRES_CONFIG['port']),
    }

    _test_create_engine(engine_args, "pg-foo", "postgres", meta)


@attr('postgres')
def test_postgres_creator_func():
    def _creator():
        return psycopg2.connect(**POSTGRES_CONFIG)

    engine_args = {'url' : 'postgresql://', 'creator' : _creator}

    meta = {
        netx.TARGET_HOST: POSTGRES_CONFIG['host'],
        netx.TARGET_PORT: str(POSTGRES_CONFIG['port']),
        sqlx.DB: POSTGRES_CONFIG["dbname"],
    }

    _test_create_engine(engine_args, "pg-foo", "postgres", meta)


def _test_create_engine(engine_args, service, vendor, expected_meta):
    url = engine_args.pop("url")
    engine = create_engine(url, **engine_args)
    try:
        _test_engine(engine, service, vendor, expected_meta)
    finally:
        engine.dispose()


def _test_engine(engine, service, vendor, expected_meta):
    """ a test suite for various sqlalchemy engines. """
    tracer = Tracer()
    tracer.writer = DummyWriter()

    # create an engine and start tracing.
    trace_engine(engine, tracer, service=service)
    start = time.time()

    @contextlib.contextmanager
    def _connect():
        try:
            conn = engine.connect()
            yield conn
        finally:
            conn.close()

    with _connect() as conn:
        try:
            conn.execute("delete from players")
        except Exception:
            pass

    # boilerplate
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    # do an ORM insert
    wayne = Player(id=1, name="wayne")
    session.add(wayne)
    session.commit()

    out = list(session.query(Player).filter_by(name="nothing"))
    eq_(len(out), 0)

    # do a regular old query that works
    with _connect() as conn:
        rows = conn.execute("select * from players").fetchall()
        eq_(len(rows), 1)
        eq_(rows[0]['name'], 'wayne')

    with _connect() as conn:
        try:
            conn.execute("select * from foo_Bah_blah")
        except Exception:
            pass
        else:
            assert 0

    end = time.time()

    spans = tracer.writer.pop()
    for span in spans:
        eq_(span.name, "%s.query" % vendor)
        eq_(span.service, service)
        eq_(span.span_type, "sql")

        for k, v in expected_meta.items():
            eq_(span.meta[k], v)

        # FIXME[matt] could be finer grained but i'm lazy
        assert start < span.start < end
        assert span.duration
        assert span.duration < end - start

    by_rsc = {s.resource:s for s in spans}

    # ensure errors work
    s = by_rsc["select * from foo_Bah_blah"]
    eq_(s.error, 1)
    assert "foo_Bah_blah" in s.get_tag(errorsx.ERROR_MSG)
    assert "foo_Bah_blah" in s.get_tag(errorsx.ERROR_STACK)

    expected = [
        "select * from players",
        "select * from foo_Bah_blah",
    ]

    for i in expected:
        assert i in by_rsc, "%s not in %s" % (i, by_rsc.keys())

    # ensure we have the service types
    services = tracer.writer.pop_services()
    expected = {
        service : {"app":vendor, "app_type":"db"}
    }
    eq_(services, expected)
