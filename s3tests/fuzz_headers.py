from boto.s3.connection import S3Connection
from optparse import OptionParser
from boto import UserAgent
from . import common

import traceback
import random
import string
import sys


class FuzzyRequest(object):
    """ FuzzyRequests are initialized with a random seed and generate data to
        get sent as valid or valid-esque HTTP requests for targeted fuzz testing
    """
    def __init__(self, seed):
        self.random = random.Random()
        self.seed = seed
        self.random.seed(self.seed)

        self._generate_method()
        self._generate_path()
        self._generate_body()
        self._generate_headers()


    def __str__(self):
        s = '%s %s HTTP/1.1\n' % (self.method, self.path)
        for header, value in self.headers.iteritems():
            s += '%s: ' %header
            if isinstance(value, list):
                for val in value:
                    s += '%s ' %val
            else:
                s += value
            s += '\n'
        s += '\n' # Blank line after headers are done.
        s += '%s\r\n\r\n' %self.body
        return s


    def _generate_method(self):
        METHODS = ['GET', 'POST', 'HEAD', 'PUT']
        self.method = self.random.choice(METHODS)


    def _generate_path(self):
        path_charset = string.letters + string.digits
        path_len = self.random.randint(0,100)
        self.path = ''
        for _ in xrange(path_len):
            self.path += self.random.choice(path_charset)
        self.auth_path = self.path # Not sure how important this is for these tests


    def _generate_body(self):
        body_charset = string.printable
        body_len = self.random.randint(0, 1000)
        self.body = ''
        for _ in xrange(body_len):
            self.body += self.random.choice(body_charset)


    def _generate_headers(self):
        self.headers = {'Foo': 'bar', 'baz': ['a', 'b', 'c']} #FIXME


    def authorize(self, connection):
        #Stolen shamelessly from boto's connection.py
        connection._auth_handler.add_auth(self)
        self.headers['User-Agent'] = UserAgent
        if not self.headers.has_key('Content-Length'):
            self.headers['Content-Length'] = str(len(self.body))


def parse_options():
    parser = OptionParser()
    parser.add_option('-O', '--outfile', help='write output to FILE. Defaults to STDOUT', metavar='FILE')
    parser.add_option('--seed', dest='seed', type='int',  help='initial seed for the random number generator', metavar='SEED')
    parser.add_option('--seed-file', dest='seedfile', help='read seeds for specific requests from FILE', metavar='FILE')
    parser.add_option('-n', dest='num_requests', type='int',  help='issue NUM requests before stopping', metavar='NUM')

    parser.set_defaults(num_requests=5)
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

    for request_seed in request_seeds:
        fuzzy = FuzzyRequest(request_seed)
        fuzzy.authorize(s3_connection)
        print fuzzy.seed, fuzzy
        #http_connection = s3_connection.get_http_connection(s3_connection.host, s3_connection.is_secure)
        #http_connection.request(fuzzy.method, fuzzy.path, body=fuzzy.body, headers=fuzzy.headers)

        #response = http_connection.getresponse()
        #if response.status == 500 or response.status == 503:
            #print 'Request generated with seed %d failed:\n%s' % (fuzzy.seed, fuzzy)


def main():
    common.setup()
    try:
        _main()
    except Exception as e:
        traceback.print_exc()
        common.teardown()

