from boto.s3.connection import S3Connection
from optparse import OptionParser
from boto import UserAgent
from . import common

import traceback
import random
import string
import yaml
import sys


def assemble_decision(decision_graph, prng):
    """ Take in a graph describing the possible decision space and a random
        number generator and traverse the graph to build a decision
    """
    raise NotImplementedError


def expand_decision(decision, prng):
    """ Take in a decision and a random number generator.  Expand variables in
        decision's values and headers until all values are fully expanded and
        build a request out of the information
    """
    raise NotImplementedError


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
        'bucket_writable' : 'TODO',
        'bucket_nonexistant' : 'TODO',
        'object_readable' : 'TODO',
        'object_writable' : 'TODO',
        'object_nonexistant' : 'TODO'
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

