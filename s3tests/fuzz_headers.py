from boto.s3 import S3Connection
from optparse import OptionParser
from . import common

import traceback
import random
import string
import sys


class FuzzyRequest(object):
    # Initialized with a seed to be reproducible.
    # string repr needs to look like:
    # METHOD PATH HTTP/1.1
    # HEADER_KEY: HEADER_VALUE[, HEADER_VALUE...]
    # [	: HEADER_VALUE[, HEADER_VALUE...]
    # <additional headers>
    #
    # BODY
    pass

def parse_options():
    parser = OptionParser()
    parser.add_option('-O', '--outfile', help='write output to FILE. Defaults to STDOUT', metavar='FILE')
    parser.add_option('--seed', dest='seed', help='initial seed for the random number generator', metavar='SEED')
    parser.add_option('--seed-file', dest='seedfile', help='read seeds for specific requests from FILE', metavar='FILE')
    parser.add_option('-n', dest='num_requests', help='issue NUM requests before stopping', metavar='NUM')

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
    s3_connection = config.s3.main

    request_seeds
    if options.seedfile:
        FH = open(options.seedfile, 'r')
        request_seeds = FH.readlines()
    else:
        request_seeds = randomlist(options.num_requests, options.seed)

    for i in request_seeds:
        fuzzy = FuzzyRequest(request_seed)

        http_connection = s3_connection.get_http_connection(s3_connection.host, s3_connection.is_secure)
        http_connection.request(fuzzy.method, fuzzy.path, body=fuzzy.body, headers=fuzzy.headers)

        response = http_connection.getresponse()
        if response.status == 500 or response.status == 503:
            print 'Request generated with seed %d failed:\n%s' % (request_seed, fuzzy)


def main():
    common.setup()
    try:
        _main()
    except Exception as e:
        traceback.print_exc()
        common.teardown()

