#! /usr/bin/python

from boto.s3.connection import OrdinaryCallingFormat
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from optparse import OptionParser
from realistic import RandomContentFile
import realistic
import random
import yaml
import boto
import sys

DHO_HOST = 'objects.dreamhost.com'

def parse_opts():
    parser = OptionParser();
    parser.add_option('-O' , '--outfile', help='write output to FILE. Defaults to STDOUT', metavar='FILE')
    parser.add_option('-a' , '--access-key', dest='access_key', help='use S3 access key KEY', metavar='KEY')
    parser.add_option('-s' , '--secret-key', dest='secret_key', help='use S3 secret key KEY', metavar='KEY')
    parser.add_option('-b' , '--bucket', dest='bucket', help='push objects to BUCKET', metavar='BUCKET')
    parser.add_option('--checksum', dest='checksum', action='store_true', help='include the md5 checksum with the object urls')
    parser.add_option('--host', dest='host', help='use S3 gateway at HOST', metavar='HOST')
    parser.add_option('--seed', dest='seed', help='optional seed for the random number generator')

    parser.set_defaults(host=DHO_HOST)

    return parser.parse_args()


def parse_config(config_files):
    configurations = []
    for file in config_files:
        FILE = open(file, 'r')
        configurations = configurations + yaml.load(FILE.read())
        FILE.close()
    return configurations


def get_bucket(conn, existing_bucket):
    if existing_bucket:
        return conn.get_bucket(existing_bucket)
    else:
        goop = '%x' % random.getrandbits(64)
        bucket = conn.create_bucket(goop)
        bucket.set_acl('public-read')
        return bucket


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
        url = key.generate_url(3600) #valid for 1 hour
        if checksum:
            url += ' %s' % key.md5
        urls.append(url)

    return urls


def main():
    (options, args) = parse_opts();

    #SETUP
    random.seed(options.seed if options.seed else None)
    if options.outfile:
        OUTFILE = open(options.outfile, 'w')
    else:
        OUTFILE = sys.stdout

    conn = connect_s3(options.host, options.access_key, options.secret_key)
    bucket = get_bucket(conn, options.bucket)
    urls = []

    print >> OUTFILE, 'bucket: %s' % bucket.name
    print >> sys.stderr, 'setup complete, generating files'
    for profile in parse_config(args):
        seed = random.random()
        urls += generate_objects(bucket, profile[0], profile[1], profile[2], seed, options.checksum)
    print >> sys.stderr, 'finished sending files. Saving urls to S3'

    url_string = '\n'.join(urls)
    url_key = Key(bucket)
    url_key.key = 'urls'
    url_key.set_contents_from_string(url_string)
    print >> OUTFILE, url_string
    print >> sys.stderr, 'done'


if __name__ == '__main__':
    main()

