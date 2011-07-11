#! /usr/bin/python

from boto.s3.key import Key
from optparse import OptionParser
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
    return parser.parse_args()


def get_bucket_properties(bucket):
    """Get and return the following properties from bucket:
        Name
        All Grants
    """
    grants = [(grant.display_name, grant.permission) for grant in bucket.list_grants()]
    return (bucket.name, grants)


def get_key_properties(key):
    """Get and return the following properties from key:
        Name
        Size
        All Grants
        All Metadata
    """
    grants = [(grant.display_name, grant.permission) for grant in key.get_acl().acl.grants]
    return (key.name, key.size, grants, key.metadata)


def main():
    '''To run the static content load test, make sure you've bootstrapped your
       test environment and set up your config.yml file, then run the following:
          S3TEST_CONF=config.yml virtualenv/bin/python verify_client.py -O output.txt test-bucket-name

       S3 authentication information for the bucket's owner must be in config.yml to create the connection.
    '''
    (options, args) = parse_opts();

    #SETUP
    conn = common.s3.main

    if options.outfile:
        OUTFILE = open(options.outfile, 'w')
    else:
        OUTFILE = sys.stdout

    try:
        bucket = conn.get_bucket(args[0])
    except S3ResponseError as e:
        print >> sys.stderr, "S3 claims %s isn't a valid bucket...maybe the user you specified in config.yml doesn't have access to it?" %args[0]
        common.teardown()
        return

    (name, grants) = get_bucket_properties(bucket)
    print >> OUTFILE, "Bucket Name: %s" % name
    for grant in grants:
        print >> OUTFILE, "\tgrant %s %s" %(grant)

    for key in bucket.list():
        full_key = bucket.get_key(key.name)
        (name, size, grants, metadata) = get_key_properties(full_key)
        print >> OUTFILE, name
        print >> OUTFILE, "\tsize:  %s" %size
        for grant in grants:
            print >> OUTFILE, "\tgrant %s %s" %(grant)
        for metadata_key in metadata:
            print >> OUTFILE, "\tmetadata %s: %s" %(metadata_key, metadata[metadata_key])



if __name__ == '__main__':
    common.setup()
    try:
        main()
    except Exception as e:
        traceback.print_exc()
        common.teardown()

