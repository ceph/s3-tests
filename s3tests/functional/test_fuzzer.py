import sys
import itertools
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


def build_graph():
    graph = {}
    graph['start'] = {
        'set': {},
        'choices': ['node2']
    }
    graph['leaf'] = {
        'set': {
            'key1': 'value1',
            'key2': 'value2'
        },
        'headers': [
            ['1-2', 'random-header-{random 5-10 printable}', '{random 20-30 punctuation}']
        ],
        'choices': []
    }
    graph['node1'] = {
        'set': {
            'key3': 'value3',
            'header_val': [
                '3 h1',
                '2 h2',
                'h3'
            ]
        },
        'headers': [
            ['1-1', 'my-header', '{header_val}'],
        ],
        'choices': ['leaf']
    }
    graph['node2'] = {
        'set': {
            'randkey': 'value-{random 10-15 printable}',
            'path': '/{bucket_readable}',
            'indirect_key1': '{key1}'
        },
        'choices': ['leaf']
    }
    graph['bad_node'] = {
        'set': {
            'key1': 'value1'
        },
        'choices': ['leaf']
    }
    graph['nonexistant_child_node'] = {
        'set': {},
        'choices': ['leafy_greens']
    }
    graph['weighted_node'] = {
        'set': {
            'k1': [
                'foo',
                '2 bar',
                '1 baz'
            ]
        },
        'choices': [
            'foo',
            '2 bar',
            '1 baz'
        ]
    }
    graph['null_choice_node'] = {
        'set': {},
        'choices': [None]
    }
    graph['weighted_null_choice_node'] = {
        'set': {},
        'choices': ['3 null']
    }
    return graph


def test_load_graph():
    graph_file = open('request_decision_graph.yml', 'r')
    graph = yaml.safe_load(graph_file)
    graph['start']


def test_descend_leaf_node():
    graph = build_graph()
    prng = random.Random(1)
    decision = descend_graph(graph, 'leaf', prng)

    eq(decision['key1'], 'value1')
    eq(decision['key2'], 'value2')
    e = assert_raises(KeyError, lambda x: decision[x], 'key3')


def test_descend_node():
    graph = build_graph()
    prng = random.Random(1)
    decision = descend_graph(graph, 'node1', prng)

    eq(decision['key1'], 'value1')
    eq(decision['key2'], 'value2')
    eq(decision['key3'], 'value3')


def test_descend_bad_node():
    graph = build_graph()
    prng = random.Random(1)
    assert_raises(KeyError, descend_graph, graph, 'bad_node', prng)


def test_descend_nonexistant_child():
    graph = build_graph()
    prng = random.Random(1)
    assert_raises(KeyError, descend_graph, graph, 'nonexistant_child_node', prng)


def test_SpecialVariables_dict():
    prng = random.Random(1)
    testdict = {'foo': 'bar'}
    tester = SpecialVariables(testdict, prng)

    eq(tester['foo'], 'bar')
    eq(tester['random 10-15 printable'], '[/pNI$;92@')


def test_SpecialVariables_binary():
    prng = random.Random(1)
    tester = SpecialVariables({}, prng)

    eq(tester['random 10-15 binary'], '\xdfj\xf1\xd80>a\xcd\xc4\xbb')


def test_assemble_decision():
    graph = build_graph()
    prng = random.Random(1)
    decision = assemble_decision(graph, prng)

    eq(decision['key1'], 'value1')
    eq(decision['key2'], 'value2')
    eq(decision['randkey'], 'value-{random 10-15 printable}')
    eq(decision['indirect_key1'], '{key1}')
    eq(decision['path'], '/{bucket_readable}')
    assert_raises(KeyError, lambda x: decision[x], 'key3')


def test_expand_key():
    prng = random.Random(1)
    test_decision = {
        'key1': 'value1',
        'randkey': 'value-{random 10-15 printable}',
        'indirect': '{key1}',
        'dbl_indirect': '{indirect}'
    }
    decision = SpecialVariables(test_decision, prng)

    randkey = expand_key(decision, test_decision['randkey'])
    indirect = expand_key(decision, test_decision['indirect'])
    dbl_indirect = expand_key(decision, test_decision['dbl_indirect'])

    eq(indirect, 'value1')
    eq(dbl_indirect, 'value1')
    eq(randkey, 'value-[/pNI$;92@')


def test_expand_loop():
    prng = random.Random(1)
    test_decision = {
        'key1': '{key2}',
        'key2': '{key1}',
    }
    decision = SpecialVariables(test_decision, prng)
    assert_raises(RuntimeError, expand_key, decision, test_decision['key1'])


def test_expand_decision():
    graph = build_graph()
    prng = random.Random(1)

    decision = assemble_decision(graph, prng)
    decision.update({'bucket_readable': 'my-readable-bucket'})

    request = expand_decision(decision, prng)

    eq(request['key1'], 'value1')
    eq(request['indirect_key1'], 'value1')
    eq(request['path'], '/my-readable-bucket')
    eq(request['randkey'], 'value-cx+*~G@&uW_[OW3')
    assert_raises(KeyError, lambda x: decision[x], 'key3')


def test_weighted_choices():
    graph = build_graph()
    prng = random.Random(1)

    choices_made = {}
    for _ in xrange(1000):
        choice = make_choice(graph['weighted_node']['choices'], prng)
        if choices_made.has_key(choice):
            choices_made[choice] += 1
        else:
            choices_made[choice] = 1

    foo_percentage = choices_made['foo'] / 1000.0
    bar_percentage = choices_made['bar'] / 1000.0
    baz_percentage = choices_made['baz'] / 1000.0
    nose.tools.assert_almost_equal(foo_percentage, 0.25, 1)
    nose.tools.assert_almost_equal(bar_percentage, 0.50, 1)
    nose.tools.assert_almost_equal(baz_percentage, 0.25, 1)


def test_null_choices():
    graph = build_graph()
    prng = random.Random(1)
    choice = make_choice(graph['null_choice_node']['choices'], prng)

    eq(choice, '')


def test_weighted_null_choices():
    graph = build_graph()
    prng = random.Random(1)
    choice = make_choice(graph['weighted_null_choice_node']['choices'], prng)

    eq(choice, '')


def test_null_child():
    graph = build_graph()
    prng = random.Random(1)
    decision = descend_graph(graph, 'null_choice_node', prng)

    eq(decision, {})


def test_weighted_set():
    graph = build_graph()
    prng = random.Random(1)

    choices_made = {}
    for _ in xrange(1000):
        choice = make_choice(graph['weighted_node']['set']['k1'], prng)
        if choices_made.has_key(choice):
            choices_made[choice] += 1
        else:
            choices_made[choice] = 1

    foo_percentage = choices_made['foo'] / 1000.0
    bar_percentage = choices_made['bar'] / 1000.0
    baz_percentage = choices_made['baz'] / 1000.0
    nose.tools.assert_almost_equal(foo_percentage, 0.25, 1)
    nose.tools.assert_almost_equal(bar_percentage, 0.50, 1)
    nose.tools.assert_almost_equal(baz_percentage, 0.25, 1)


def test_header_presence():
    graph = build_graph()
    prng = random.Random(1)
    decision = descend_graph(graph, 'node1', prng)

    c1 = itertools.count()
    c2 = itertools.count()
    for header, value in decision['headers']:
        if header == 'my-header':
            eq(value, '{header_val}')
            nose.tools.assert_true(next(c1) < 1)
        elif header == 'random-header-{random 5-10 printable}':
            eq(value, '{random 20-30 punctuation}')
            nose.tools.assert_true(next(c2) < 2)
        else:
            raise KeyError('unexpected header found: %s' % header)

    nose.tools.assert_true(next(c1))
    nose.tools.assert_true(next(c2))


def test_header_expansion():
    graph = build_graph()
    prng = random.Random(1)
    decision = descend_graph(graph, 'node1', prng)
    expanded_decision = expand_decision(decision, prng)

    for header, value in expanded_decision['headers']:
        if header == 'my-header':
            nose.tools.assert_true(value in ['h1', 'h2', 'h3'])
        elif header.startswith('random-header-'):
            nose.tools.assert_true(20 <= len(value) <= 30)
            nose.tools.assert_true(string.strip(value, SpecialVariables.charsets['punctuation']) is '')
        else:
            raise KeyError('unexpected header found: "%s"' % header)

