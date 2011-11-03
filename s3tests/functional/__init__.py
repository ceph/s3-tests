import ConfigParser
import boto.exception
import boto.s3.connection
import bunch
import itertools
import os
import random
import string

s3 = bunch.Bunch()
config = bunch.Bunch()

# this will be assigned by setup()
prefix = None

def get_prefix():
    assert prefix is not None
    return prefix

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


def nuke_prefixed_buckets(prefix):
    for name, conn in s3.items():
        print 'Cleaning buckets from connection {name} prefix {prefix!r}.'.format(
            name=name,
            prefix=prefix,
            )
        for bucket in conn.get_all_buckets():
            if bucket.name.startswith(prefix):
                print 'Cleaning bucket {bucket}'.format(bucket=bucket)
                try:
                    bucket.set_canned_acl('private')
                    for key in bucket.list():
                        print 'Cleaning bucket {bucket} key {key}'.format(
                            bucket=bucket,
                            key=key,
                            )
                        key.set_canned_acl('private')
                        key.delete()
                    bucket.delete()
                except boto.exception.S3ResponseError as e:
                    if e.error_code != 'AccessDenied':
                        print 'GOT UNWANTED ERROR', e.error_code
                        raise
                    # seems like we're not the owner of the bucket; ignore
                    pass

    print 'Done with cleanup of test buckets.'


# nosetests --processes=N with N>1 is safe
_multiprocess_can_split_ = True

def setup():

    cfg = ConfigParser.RawConfigParser()
    try:
        path = os.environ['S3TEST_CONF']
    except KeyError:
        raise RuntimeError(
            'To run tests, point environment '
            + 'variable S3TEST_CONF to a config file.',
            )
    with file(path) as f:
        cfg.readfp(f)

    global prefix
    try:
        template = cfg.get('fixtures', 'bucket prefix')
    except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
        template = 'test-{random}-'
    prefix = choose_bucket_prefix(template=template)

    s3.clear()
    config.clear()
    calling_formats = dict(
        ordinary=boto.s3.connection.OrdinaryCallingFormat(),
        subdomain=boto.s3.connection.SubdomainCallingFormat(),
        vhost=boto.s3.connection.VHostCallingFormat(),
        )
    for section in cfg.sections():
        try:
            (type_, name) = section.split(None, 1)
        except ValueError:
            continue
        if type_ != 's3':
            continue
        try:
            port = cfg.getint(section, 'port')
        except ConfigParser.NoOptionError:
            port = None

        try:
            raw_calling_format = cfg.get(section, 'calling_format')
        except ConfigParser.NoOptionError:
            raw_calling_format = 'ordinary'

        try:
            calling_format = calling_formats[raw_calling_format]
        except KeyError:
            raise RuntimeError(
                'calling_format unknown: %r' % raw_calling_format
                )

        config[name] = bunch.Bunch()
        for var in [
            'user_id',
            'display_name',
            'email',
            ]:
            try:
                config[name][var] = cfg.get(section, var)
            except ConfigParser.NoOptionError:
                pass
        conn = boto.s3.connection.S3Connection(
            aws_access_key_id=cfg.get(section, 'access_key'),
            aws_secret_access_key=cfg.get(section, 'secret_key'),
            is_secure=cfg.getboolean(section, 'is_secure'),
            port=port,
            host=cfg.get(section, 'host'),
            # TODO test vhost calling format
            calling_format=calling_format,
            )
        s3[name] = conn

    # WARNING! we actively delete all buckets we see with the prefix
    # we've chosen! Choose your prefix with care, and don't reuse
    # credentials!

    # We also assume nobody else is going to use buckets with that
    # prefix. This is racy but given enough randomness, should not
    # really fail.
    nuke_prefixed_buckets(prefix=prefix)


def teardown():
    # remove our buckets here also, to avoid littering
    nuke_prefixed_buckets(prefix=prefix)


bucket_counter = itertools.count(1)


def get_new_bucket_name():
    """
    Get a bucket name that probably does not exist.

    We make every attempt to use a unique random prefix, so if a
    bucket by this name happens to exist, it's ok if tests give
    false negatives.
    """
    name = '{prefix}{num}'.format(
        prefix=prefix,
        num=next(bucket_counter),
        )
    return name


def get_new_bucket(connection=None):
    """
    Get a bucket that exists and is empty.

    Always recreates a bucket from scratch. This is useful to also
    reset ACLs and such.
    """
    if connection is None:
        connection = s3.main
    name = get_new_bucket_name()
    # the only way for this to fail with a pre-existing bucket is if
    # someone raced us between setup nuke_prefixed_buckets and here;
    # ignore that as astronomically unlikely
    bucket = connection.create_bucket(name)
    return bucket
