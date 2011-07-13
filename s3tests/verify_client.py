#! /usr/bin/python

from boto.s3.key import Key
from optparse import OptionParser
import traceback
import common
import bunch
import yaml
import sys


def parse_opts():
    parser = OptionParser();
    parser.add_option('-O' , '--outfile', help='write output to FILE. Defaults to STDOUT', metavar='FILE')
    parser.add_option('-b' , '--blueprint', help='populate buckets according to blueprint file BLUEPRINT.  Used to get baseline results to compare client results against.', metavar='BLUEPRINT')
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


def read_blueprint(infile):
    """Takes a filename as input and returns a "bunch" describing buckets
       and objects to upload to an S3-like object store.  This can be used
       to confirm that buckets created by another client match those created
       by boto.
    """
    try:
        INFILE = open(infile, 'r')
        blueprint = bunch.bunchify(yaml.safe_load(INFILE))
    except Exception as e:
        print >> sys.stderr, "There was an error reading the blueprint file, %s:" %infile
        print >> sys.stderr, traceback.print_exc()

    return blueprint


def populate_from_blueprint(conn, blueprint, prefix=''):
    """Take a connection and a blueprint.  Create buckets and upload objects
       according to the blueprint.  Prefix will be added to each bucket name.
    """
    buckets = []
    for bucket in blueprint:
        b = conn.create_bucket(prefix + bucket.name)
        for user in bucket.perms:
            b.add_user_grant(bucket.perms[user], user)
        for key in bucket.objects:
            k = Key(b)
            k.key = key.name
            k.metadata = bunch.unbunchify(key.metadata)
            k.set_contents_from_string(key.content)
            for user in key.perms:
                k.add_user_grant(key.perms[user], user)
        buckets.append(b)
    return buckets



def main():
    """Client results validation tool make sure you've bootstrapped your
       test environment and set up your config.yml file, then run the
       following:
          S3TEST_CONF=config.yml virtualenv/bin/python verify_client.py -O output.txt test-bucket-name

       S3 authentication information for the bucket's owner must be in
       config.yml to create the connection.
    """
    (options, args) = parse_opts()

    #SETUP
    conn = common.s3.main

    if options.outfile:
        OUTFILE = open(options.outfile, 'w')
    else:
        OUTFILE = sys.stdout

    blueprint = None
    if options.blueprint:
        blueprint = read_blueprint(options.blueprint)
    if blueprint:
        populate_from_blueprint(conn, blueprint, common.prefix)

    for bucket_name in args:
        try:
            bucket = conn.get_bucket(bucket_name)
        except S3ResponseError as e:
            print >> sys.stderr, "S3 claims %s isn't a valid bucket...maybe the user you specified in config.yml doesn't have access to it?" %bucket_name
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

