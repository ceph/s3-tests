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

    for key in node['set']:
        if decision.has_key(key):
            raise KeyError("Node %s tried to set '%s', but that key was already set by a lower node!" %(node_name, key))
        decision[key] = make_choice(node['set'][key], prng)

    if node.has_key('headers'):
        if not decision.has_key('headers'):
            decision['headers'] = []

        for desc in node['headers']:
            if len(desc) == 3:
                repetition_range = desc.pop(0)
                try:
                    size_min, size_max = [int(x) for x in repetition_range.split('-')]
                except IndexError:
                    size_min = size_max = int(repetition_range)
            else:
                size_min = size_max = 1
            num_reps = prng.randint(size_min, size_max)
            for _ in xrange(num_reps):
                header = desc[0]
                value = desc[1]
                if header in [h for h, v in decision['headers']]:
                    if not re.search('{[a-zA-Z_0-9 -]+}', header):
                        raise KeyError("Node %s tried to add header '%s', but that header already exists!" %(node_name, header))
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
        fields = option.split(None, 1)
        if len(fields) == 1:
            weight = 1
            value = fields[0]
        else:
            weight = int(fields[0])
            value = fields[1]
            if value == 'null' or value == 'None':
                value = ''
        for _ in xrange(weight):
            weighted_choices.append(value)

    return prng.choice(weighted_choices)


def expand_decision(decision, prng):
    """ Take in a decision and a random number generator.  Expand variables in
        decision's values and headers until all values are fully expanded and
        build a request out of the information
    """
    special_decision = SpecialVariables(decision, prng)
    for key in special_decision:
        if not key == 'headers':
            decision[key] = expand_key(special_decision, decision[key])
        else:
            for header in special_decision[key]:
                header[0] = expand_key(special_decision, header[0])
                header[1] = expand_key(special_decision, header[1])
    return decision


def expand_key(decision, value):
    c = itertools.count()
    fmt = string.Formatter()
    old = value
    while True:
        new = fmt.vformat(old, [], decision)
        if new == old.replace('{{', '{').replace('}}', '}'):
            return old
        if next(c) > 5:
            raise RuntimeError
        old = new

class SpecialVariables(dict):
    charsets = {
        'binary': 'binary',
        'printable': string.printable,
        'punctuation': string.punctuation,
        'whitespace': string.whitespace,
        'digits': string.digits
    }

    def __init__(self, orig_dict, prng):
        self.update(orig_dict)
        self.prng = prng


    def __getitem__(self, key):
        fields = key.split(None, 1)
        fn = getattr(self, 'special_{name}'.format(name=fields[0]), None)
        if fn is None:
            return super(SpecialVariables, self).__getitem__(key)

        if len(fields) == 1:
            fields.apppend('')
        return fn(fields[1])


    def special_random(self, args):
        arg_list = args.split()
        try:
            size_min, size_max = [int(x) for x in arg_list[0].split('-')]
        except IndexError:
            size_min = 0
            size_max = 1000
        try:
            charset = self.charsets[arg_list[1]]
        except IndexError:
            charset = self.charsets['printable']

        length = self.prng.randint(size_min, size_max)
        if charset is 'binary':
            num_bytes = length + 8
            tmplist = [self.prng.getrandbits(64) for _ in xrange(num_bytes / 8)]
            tmpstring = struct.pack((num_bytes / 8) * 'Q', *tmplist)
            return tmpstring[0:length]
        else:
            tmpstring = ''.join([self.prng.choice(charset) for _ in xrange(length)]) # Won't scale nicely; won't do binary
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


def randomlist(n, seed=None):
    """ Returns a generator function that spits out a list of random numbers n elements long.
    """
    rng = random.Random()
    rng.seed(seed if seed else None)
    for _ in xrange(n):
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
        request_seeds = randomlist(options.num_requests, options.seed)

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

