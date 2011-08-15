from boto.s3.connection import S3Connection
from optparse import OptionParser
from boto import UserAgent
from . import common

import traceback
import itertools
import random
import string
import struct
import yaml
import sys
import re


class DecisionGraphError(Exception):
    """ Raised when a node in a graph tries to set a header or
        key that was previously set by another node
    """
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class RecursionError(Exception):
    """Runaway recursion in string formatting"""

    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return '{0.__doc__}: {0.msg!r}'.format(self)


def assemble_decision(decision_graph, prng):
    """ Take in a graph describing the possible decision space and a random
        number generator and traverse the graph to build a decision
    """
    return descend_graph(decision_graph, 'start', prng)


def descend_graph(decision_graph, node_name, prng):
    """ Given a graph and a particular node in that graph, set the values in
        the node's "set" list, pick a choice from the "choice" list, and
        recurse.  Finally, return dictionary of values
    """
    node = decision_graph[node_name]

    try:
        choice = make_choice(node['choices'], prng)
        if choice == '':
            decision = {}
        else:
            decision = descend_graph(decision_graph, choice, prng)
    except IndexError:
        decision = {}

    for key, choices in node['set'].iteritems():
        if key in decision:
            raise DecisionGraphError("Node %s tried to set '%s', but that key was already set by a lower node!" %(node_name, key))
        decision[key] = make_choice(choices, prng)

    if 'headers' in node:
        decision.setdefault('headers', [])

        for desc in node['headers']:
            try:
                (repetition_range, header, value) = desc
            except ValueError:
                (header, value) = desc
                repetition_range = '1'

            try:
                size_min, size_max = repetition_range.split('-', 1)
            except ValueError:
                size_min = size_max = repetition_range

            size_min = int(size_min)
            size_max = int(size_max)

            num_reps = prng.randint(size_min, size_max)
            if header in [h for h, v in decision['headers']]:
                    raise DecisionGraphError("Node %s tried to add header '%s', but that header already exists!" %(node_name, header))
            for _ in xrange(num_reps):
                decision['headers'].append([header, value])

    return decision


def make_choice(choices, prng):
    """ Given a list of (possibly weighted) options or just a single option!,
        choose one of the options taking weights into account and return the
        choice
    """
    if isinstance(choices, str):
        return choices
    weighted_choices = []
    for option in choices:
        if option is None:
            weighted_choices.append('')
            continue
        try:
            (weight, value) = option.split(None, 1)
        except ValueError:
            weight = '1'
            value = option

        weight = int(weight)
        if value == 'null' or value == 'None':
            value = ''

        for _ in xrange(weight):
            weighted_choices.append(value)

    return prng.choice(weighted_choices)


def expand_headers(decision):
    expanded_headers = []
    for header in decision['headers']:
        h = expand(decision, header[0])
        v = expand(decision, header[1])
        expanded_headers.append([h, v])
    return expanded_headers


def expand(decision, value):
    c = itertools.count()
    fmt = RepeatExpandingFormatter()
    new = fmt.vformat(value, [], decision)
    return new


class RepeatExpandingFormatter(string.Formatter):

    def __init__(self, _recursion=0):
        super(RepeatExpandingFormatter, self).__init__()
        # this class assumes it is always instantiated once per
        # formatting; use that to detect runaway recursion
        self._recursion = _recursion

    def get_value(self, key, args, kwargs):
        val = super(RepeatExpandingFormatter, self).get_value(key, args, kwargs)
        if self._recursion > 5:
            raise RecursionError(key)
        fmt = self.__class__(_recursion=self._recursion+1)
        # must use vformat not **kwargs so our SpecialVariables is not
        # downgraded to just a dict
        n = fmt.vformat(val, args, kwargs)
        return n


class SpecialVariables(dict):
    charsets = {
        'printable': string.printable,
        'punctuation': string.punctuation,
        'whitespace': string.whitespace,
        'digits': string.digits
    }

    def __init__(self, orig_dict, prng):
        super(SpecialVariables, self).__init__(orig_dict)
        self.prng = prng


    def __getitem__(self, key):
        fields = key.split(None, 1)
        fn = getattr(self, 'special_{name}'.format(name=fields[0]), None)
        if fn is None:
            return super(SpecialVariables, self).__getitem__(key)

        if len(fields) == 1:
            fields.append('')
        return fn(fields[1])


    def special_random(self, args):
        arg_list = args.split()
        try:
            size_min, size_max = arg_list[0].split('-', 1)
        except ValueError:
            size_min = size_max = arg_list[0]
        except IndexError:
            size_min = '0'
            size_max = '1000'

        size_min = int(size_min)
        size_max = int(size_max)
        length = self.prng.randint(size_min, size_max)

        try:
            charset_arg = arg_list[1]
        except IndexError:
            charset_arg = 'printable'

        if charset_arg == 'binary':
            num_bytes = length + 8
            tmplist = [self.prng.getrandbits(64) for _ in xrange(num_bytes / 8)]
            tmpstring = struct.pack((num_bytes / 8) * 'Q', *tmplist)
            tmpstring = tmpstring[0:length]
        else:
            charset = self.charsets[charset_arg]
            tmpstring = ''.join([self.prng.choice(charset) for _ in xrange(length)]) # Won't scale nicely

        return tmpstring.replace('{', '{{').replace('}', '}}')


def parse_options():
    parser = OptionParser()
    parser.add_option('-O', '--outfile', help='write output to FILE. Defaults to STDOUT', metavar='FILE')
    parser.add_option('--seed', dest='seed', type='int',  help='initial seed for the random number generator', metavar='SEED')
    parser.add_option('--seed-file', dest='seedfile', help='read seeds for specific requests from FILE', metavar='FILE')
    parser.add_option('-n', dest='num_requests', type='int',  help='issue NUM requests before stopping', metavar='NUM')
    parser.add_option('--decision-graph', dest='graph_filename',  help='file in which to find the request decision graph', metavar='NUM')

    parser.set_defaults(num_requests=5)
    parser.set_defaults(graph_filename='request_decision_graph.yml')
    return parser.parse_args()


def randomlist(seed=None):
    """ Returns an infinite generator of random numbers
    """
    rng = random.Random(seed)
    while True:
        yield rng.random()


def _main():
    """ The main script
    """
    (options, args) = parse_options()
    random.seed(options.seed if options.seed else None)
    s3_connection = common.s3.main

    request_seeds = None
    if options.seedfile:
        FH = open(options.seedfile, 'r')
        request_seeds = FH.readlines()
    else:
        random_list = randomlist(options.seed)
        request_seeds = itertools.islice(random_list, options.num_requests)


    graph_file = open(options.graph_filename, 'r')
    decision_graph = yaml.safe_load(graph_file)

    constants = {
        'bucket_readable': 'TODO',
        'bucket_not_readable': 'TODO',
        'bucket_writable' : 'TODO',
        'bucket_not_writable' : 'TODO',
        'object_readable' : 'TODO',
        'object_not_readable' : 'TODO',
        'object_writable' : 'TODO',
        'object_not_writable' : 'TODO',
    }

    for request_seed in request_seeds:
        prng = random.Random(request_seed)
        decision = assemble_decision(decision_graph, prng)
        decision.update(constants)
        request = expand_decision(decision, prng) 

        response = s3_connection.make_request(request['method'], request['path'], data=request['body'], headers=request['headers'], override_num_retries=0)

        if response.status == 500 or response.status == 503:
            print 'Request generated with seed %d failed:\n%s' % (request_seed, request)
        pass


def main():
    common.setup()
    try:
        _main()
    except Exception as e:
        traceback.print_exc()
        common.teardown()

