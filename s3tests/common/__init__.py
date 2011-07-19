import boto.s3.connection
import bunch
import itertools
import os
import random
import string
import sys
import yaml

s3 = bunch.Bunch()
config = bunch.Bunch()
prefix = ''

# For those scripts that use a context, these are pretty univerally needed.
context = bunch.Bunch(
    bucket=None,

    # Save stdout/stderr in case they get fudged with.
    real_stdout=sys.stdout,
    real_stderr=sys.stderr,
)

bucket_counter = itertools.count(1)
key_counter = itertools.count(1)

def choose_bucket_prefix(template, max_len=30):
    """
    Choose a prefix for our test buckets, so they're easy to identify.

    Use template and feed it more and more random filler, until it's
    as long as possible but still below max_len.
    """
    rand = ''.join(
        random.choice(string.ascii_lowercase + string.digits)
        for c in range(255)
        )

    while rand:
        s = template.format(random=rand)
        if len(s) <= max_len:
            return s
        rand = rand[:-1]

    raise RuntimeError(
        'Bucket prefix template is impossible to fulfill: {template!r}'.format(
            template=template,
            ),
        )

def nuke_prefixed_buckets():
    for name, conn in s3.items():
        print 'Cleaning buckets from connection {name}'.format(name=name)
        for bucket in conn.get_all_buckets():
            if bucket.name.startswith(prefix):
                print 'Cleaning bucket {bucket}'.format(bucket=bucket)
                try:
                    bucket.set_canned_acl('private')
                    # TODO: deleted_cnt and the while loop is a work around for rgw
                    # not sending the
                    deleted_cnt = 1
                    while deleted_cnt:
                        deleted_cnt = 0
                        for key in bucket.list():
                            print 'Cleaning bucket {bucket} key {key}'.format(
                                bucket=bucket,
                                key=key,
                                )
                            key.set_canned_acl('private')
                            key.delete()
                            deleted_cnt += 1
                    bucket.delete()
                except boto.exception.S3ResponseError as e:
                    # TODO workaround for buggy rgw that fails to send
                    # error_code, remove
                    if (e.status == 403
                        and e.error_code is None
                        and e.body == ''):
                        e.error_code = 'AccessDenied'
                    if e.error_code != 'AccessDenied':
                        print 'GOT UNWANTED ERROR', e.error_code
                        raise
                    # seems like we're not the owner of the bucket; ignore
                    pass

    print 'Done with cleanup of test buckets.'

def setup():
    global s3, config, prefix
    s3.clear()
    config.clear()

    try:
        path = os.environ['S3TEST_CONF']
    except KeyError:
        raise RuntimeError(
            'To run tests, point environment '
            + 'variable S3TEST_CONF to a config file.',
            )
    with file(path) as f:
        g = yaml.safe_load_all(f)
        for new in g:
            config.update(bunch.bunchify(new))

    # These 3 should always be present.
    if 's3' not in config:
        raise RuntimeError('Your config file is missing the s3 section!')
    if 'defaults' not in config.s3:
        raise RuntimeError('Your config file is missing the s3.defaults section!')
    if 'fixtures' not in config:
        raise RuntimeError('Your config file is missing the fixtures section!')

    template = config.fixtures.get('bucket prefix', 'test-{random}-')
    prefix = choose_bucket_prefix(template=template)
    if prefix == '':
        raise RuntimeError("Empty Prefix! Aborting!")

    defaults = config.s3.defaults
    for section in config.s3.keys():
        if section == 'defaults':
            continue
        section_config = config.s3[section]

        kwargs = bunch.Bunch()
        conn_args = bunch.Bunch(
            port='port',
            host='host',
            is_secure='is_secure',
            access_key='aws_access_key_id',
            secret_key='aws_secret_access_key',
            )
        for cfg_key in conn_args.keys():
            conn_key = conn_args[cfg_key]

            if section_config.has_key(cfg_key):
                kwargs[conn_key] = section_config[cfg_key]
            elif defaults.has_key(cfg_key):
                kwargs[conn_key] = defaults[cfg_key]

        conn = boto.s3.connection.S3Connection(
            # TODO support & test all variations
            calling_format=boto.s3.connection.OrdinaryCallingFormat(),
            **kwargs
            )
        s3[section] = conn

    # WARNING! we actively delete all buckets we see with the prefix
    # we've chosen! Choose your prefix with care, and don't reuse
    # credentials!

    # We also assume nobody else is going to use buckets with that
    # prefix. This is racy but given enough randomness, should not
    # really fail.
    nuke_prefixed_buckets()

def get_new_bucket(connection=None):
    """
    Get a bucket that exists and is empty.

    Always recreates a bucket from scratch. This is useful to also
    reset ACLs and such.
    """
    if connection is None:
        connection = s3.main
    name = '{prefix}{num}'.format(
        prefix=prefix,
        num=next(bucket_counter),
        )
    # the only way for this to fail with a pre-existing bucket is if
    # someone raced us between setup nuke_prefixed_buckets and here;
    # ignore that as astronomically unlikely
    bucket = connection.create_bucket(name)
    return bucket

def teardown():
    nuke_prefixed_buckets()

def fill_pools(*args):
    for pool in args:
        while not pool.full():
            pool.spawn()

def get_next_key(bucket=None):
    return bucket.new_key("seqkey-{num}".format(num=next(key_counter)))
