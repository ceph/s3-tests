import nose
import random
import string
import yaml

from s3tests.fuzz_headers import *

from nose.tools import eq_ as eq
from nose.plugins.attrib import attr

from .utils import assert_raises

_decision_graph = {}

def check_access_denied(fn, *args, **kwargs):
    e = assert_raises(boto.exception.S3ResponseError, fn, *args, **kwargs)
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'AccessDenied')


def read_graph():
    graph_file = open('request_decision_graph.yml', 'r')
    return yaml.safe_load(graph_file)


def test_assemble_decision():
    graph = read_graph()
    prng = random.Random(1)
    decision = assemble_decision(graph, prng)
    decision['path']
    decision['method']
    decision['body']
    decision['headers']

