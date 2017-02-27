"""
An integration test that uses a real Redis client
that we expect to be implicitly traced via `ddtrace-run`
"""

from __future__ import print_function

import redis
import os

from tests.contrib.config import REDIS_CONFIG
from ddtrace import Pin
from ddtrace.encoding import JSONEncoder, MsgpackEncoder
from ddtrace.writer import AgentWriter

from nose.tools import eq_, ok_

class DummyWriter(AgentWriter):
    """ DummyWriter is a small fake writer used for tests. not thread-safe. """

    def __init__(self):
        # original call
        super(DummyWriter, self).__init__()
        # dummy components
        self.spans = []
        self.services = {}
        self.json_encoder = JSONEncoder()
        self.msgpack_encoder = MsgpackEncoder()

    def write(self, spans=None, services=None):
        if spans:
            # the traces encoding expect a list of traces so we
            # put spans in a list like we do in the real execution path
            # with both encoders
            self.json_encoder.encode_traces([spans])
            self.msgpack_encoder.encode_traces([spans])
            self.spans += spans

        if services:
            self.json_encoder.encode_services(services)
            self.msgpack_encoder.encode_services(services)
            self.services.update(services)

    def pop(self):
        # dummy method
        s = self.spans
        self.spans = []
        return s

    def pop_services(self):
        # dummy method
        s = self.services
        self.services = {}
        return s

if __name__ == '__main__':
    r = redis.Redis(port=REDIS_CONFIG['port'])
    pin = Pin.get_from(r)
    ok_(pin)
    eq_(pin.app, 'redis')
    eq_(pin.service, 'redis')

    pin.tracer.writer = DummyWriter()
    r.flushall()
    spans = pin.tracer.writer.pop()

    eq_(len(spans), 1)
    eq_(spans[0].service, 'redis')
    eq_(spans[0].resource, 'FLUSHALL')

    long_cmd = "mget %s" % " ".join(map(str, range(1000)))
    us = r.execute_command(long_cmd)

    spans = pin.tracer.writer.pop()
    eq_(len(spans), 1)
    span = spans[0]
    eq_(span.service, 'redis')
    eq_(span.name, 'redis.command')
    eq_(span.span_type, 'redis')
    eq_(span.error, 0)
    meta = {
        'out.host': u'localhost',
        'out.port': str(REDIS_CONFIG['port']),
        'out.redis_db': u'0',
    }
    for k, v in meta.items():
        eq_(span.get_tag(k), v)

    assert span.get_tag('redis.raw_command').startswith(u'mget 0 1 2 3')
    assert span.get_tag('redis.raw_command').endswith(u'...')

    print("Test success")
