#! /usr/bin/python

from boto.s3.connection import OrdinaryCallingFormat
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from optparse import OptionParser
from realistic import RandomContentFile
import realistic
import traceback
import random
import common
import yaml
import boto
import sys


def parse_opts():
    parser = OptionParser();
    parser.add_option('-O' , '--outfile', help='write output to FILE. Defaults to STDOUT', metavar='FILE')
    parser.add_option('-b' , '--bucket', dest='bucket', help='push objects to BUCKET', metavar='BUCKET')
    parser.add_option('--seed', dest='seed', help='optional seed for the random number generator')

    return parser.parse_args()


def connect_s3(host, access_key, secret_key):
    conn = S3Connection(
        calling_format = OrdinaryCallingFormat(),
        is_secure = False,
        host = host,
        aws_access_key_id = access_key,
        aws_secret_access_key = secret_key)

    return conn


def get_random_files(quantity, mean, stddev, seed):
    """Create file-like objects with pseudorandom contents.
       IN:
           number of files to create
           mean file size in bytes
           standard deviation from mean file size
           seed for PRNG
       OUT:
           list of file handles
    """
    file_generator = realistic.files(mean, stddev, seed)
    return [file_generator.next() for _ in xrange(quantity)]


def upload_objects(bucket, files, seed):
    """Upload a bunch of files to an S3 bucket
       IN:
         boto S3 bucket object
         list of file handles to upload
         seed for PRNG
       OUT:
         list of boto S3 key objects
    """
    keys = []
    name_generator = realistic.names(15, 4,seed=seed)

    for fp in files:
        print >> sys.stderr, 'sending file with size %dB' % fp.size
        key = Key(bucket)
        key.key = name_generator.next()
        key.set_contents_from_file(fp)
        keys.append(key)

    return keys


def main():
    '''To run the static content load test, make sure you've bootstrapped your
       test environment and set up your config.yml file, then run the following:
          S3TEST_CONF=config.yml virtualenv/bin/python generate_objects.py -O urls.txt --seed 1234

        This creates a bucket with your S3 credentials (from config.yml) and
        fills it with garbage objects as described in generate_objects.conf.
        It writes a list of URLS to those objects to ./urls.txt.

        Once you have objcts in your bucket, run the siege benchmarking program:
            siege -rc ./siege.conf -r 5

        This tells siege to read the ./siege.conf config file which tells it to
        use the urls in ./urls.txt and log to ./siege.log. It hits each url in
        urls.txt 5 times (-r flag).

        Results are printed to the terminal and written in CSV format to
        ./siege.log
    '''
    (options, args) = parse_opts();

    #SETUP
    random.seed(options.seed if options.seed else None)
    conn = common.s3.main

    if options.outfile:
        OUTFILE = open(options.outfile, 'w')
    elif common.config.file_generation.url_file:
        OUTFILE = open(common.config.file_generation.url_file, 'w')
    else:
        OUTFILE = sys.stdout

    if options.bucket:
        bucket = conn.create_bucket(options.bucket)
    else:
        bucket = common.get_new_bucket()

    keys = []
    print >> OUTFILE, 'bucket: %s' % bucket.name
    print >> sys.stderr, 'setup complete, generating files'
    for profile in common.config.file_generation.groups:
        seed = random.random()
        files = get_random_files(profile[0], profile[1], profile[2], seed)
        keys += upload_objects(bucket, files, seed)

    print >> sys.stderr, 'finished sending files. generating urls'
    for key in keys:
        print >> OUTFILE, key.generate_url(30758400) #valid for 1 year

    print >> sys.stderr, 'done'


if __name__ == '__main__':
    common.setup()
    try:
        main()
    except Exception as e:
        traceback.print_exc()
        common.teardown()

