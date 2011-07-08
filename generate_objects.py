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
    parser.add_option('-a' , '--access-key', dest='access_key', help='use S3 access key KEY', metavar='KEY')
    parser.add_option('-s' , '--secret-key', dest='secret_key', help='use S3 secret key KEY', metavar='KEY')
    parser.add_option('-b' , '--bucket', dest='bucket', help='push objects to BUCKET', metavar='BUCKET')
    parser.add_option('--checksum', dest='checksum', action='store_true', help='include the md5 checksum with the object urls')
    parser.add_option('--host', dest='host', help='use S3 gateway at HOST', metavar='HOST')
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


def generate_objects(bucket, quantity, mean, stddev, seed, checksum=False):
    """Generate random objects with sizes across a normal distribution
       specified by mean and standard deviation and write them to bucket.
       IN:
         boto S3 bucket object
         Number of files
         mean file size in bytes
         standard deviation from mean file size
         seed for RNG
         flag to tell the method to append md5 checksums to the output
       OUT:
         list of urls (strings) to objects valid for 1 hour.
         If "checksum" is true, each output string consists of the url
         followed by the md5 checksum.
    """
    urls = []
    file_generator = realistic.files(mean, stddev, seed)
    name_generator = realistic.names(15, 4,seed=seed)
    for _ in xrange(quantity):
        fp = file_generator.next()
        print >> sys.stderr, 'sending file with size %dB' % fp.size
        key = Key(bucket)
        key.key = name_generator.next()
        key.set_contents_from_file(fp)
        url = key.generate_url(30758400) #valid for 1 year
        if checksum:
            url += ' %s' % key.md5
        urls.append(url)

    return urls


def main():
    '''To run the static content load test, make sure you've bootstrapped your
       test environment and set up your config.yml file, then run the following:
          S3TEST_CONF=config.yml virtualenv/bin/python generate_objects.py -a S3_ACCESS_KEY -s S3_SECRET_KEY -O urls.txt --seed 1234 && siege -rc ./siege.conf -r 5

        This creates a bucket with your S3 credentials and fills it with
        garbage objects as described in generate_objects.conf. It writes a
        list of URLS to those objects to ./urls.txt.  siege then reads the
        ./siege.conf config file which tells it to read from ./urls.txt and
        log to ./siege.log and hammers each url in urls.txt 5 times (-r flag).
       
        Results are printed to the terminal and written in CSV format to
        ./siege.log

        S3 credentials and output file may also be specified in config.yml
        under s3.main and file_generation.url_file
    '''
    (options, args) = parse_opts();

    #SETUP
    random.seed(options.seed if options.seed else None)
    if options.outfile:
        OUTFILE = open(options.outfile, 'w')
    elif common.config.file_generation.url_file:
        OUTFILE = open(common.config.file_generation.url_file, 'w')
    else:
        OUTFILE = sys.stdout

    if options.access_key and options.secret_key:
        host = options.host if options.host else common.config.s3.defaults.host
        conn = connect_s3(host, options.access_key, options.secret_key)
    else:
        conn = common.s3.main

    if options.bucket:
        bucket = get_bucket(conn, options.bucket)
    else:
        bucket = common.get_new_bucket()

    urls = []

    print >> OUTFILE, 'bucket: %s' % bucket.name
    print >> sys.stderr, 'setup complete, generating files'
    for profile in common.config.file_generation.groups:
        seed = random.random()
        urls += generate_objects(bucket, profile[0], profile[1], profile[2], seed, options.checksum)
    print >> sys.stderr, 'finished sending files. generating  urls and sending to S3'

    url_string = '\n'.join(urls)
    url_key = Key(bucket)
    url_key.key = 'urls'
    url_key.set_contents_from_string(url_string)
    print >> OUTFILE, url_string
    print >> sys.stderr, 'done'


if __name__ == '__main__':
    common.setup()
    try:
        main()
    except Exception as e:
        traceback.print_exc()
        common.teardown()

