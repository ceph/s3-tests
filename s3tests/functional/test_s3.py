from cStringIO import StringIO
import boto.exception
import boto.s3.connection
import boto.s3.acl
import bunch
import nose
import operator
import random
import string
import socket
import ssl

from nose.tools import eq_ as eq
from nose.plugins.attrib import attr

from .utils import assert_raises
import AnonymousAuth

from email.header import decode_header

from . import (
    nuke_prefixed_buckets,
    get_new_bucket,
    s3,
    config,
    get_prefix,
    )


NONEXISTENT_EMAIL = 'doesnotexist@dreamhost.com.invalid'


def check_access_denied(fn, *args, **kwargs):
    e = assert_raises(boto.exception.S3ResponseError, fn, *args, **kwargs)
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'AccessDenied')


def check_grants(got, want):
    """
    Check that grants list in got matches the dictionaries in want,
    in any order.
    """
    eq(len(got), len(want))
    got = sorted(got, key=operator.attrgetter('id'))
    want = sorted(want, key=operator.itemgetter('id'))
    for g, w in zip(got, want):
        w = dict(w)
        eq(g.permission, w.pop('permission'))
        eq(g.id, w.pop('id'))
        eq(g.display_name, w.pop('display_name'))
        eq(g.uri, w.pop('uri'))
        eq(g.email_address, w.pop('email_address'))
        eq(g.type, w.pop('type'))
        eq(w, {})

def test_bucket_list_empty():
    bucket = get_new_bucket()
    l = bucket.list()
    l = list(l)
    eq(l, [])

@attr('fails_on_rgw')
@attr('fails_on_dho')
def test_bucket_list_many():
    bucket = get_new_bucket()
    keys = ['foo', 'bar', 'baz']
    for s in keys:
        key = bucket.new_key(s)
        key.set_contents_from_string(s)
    # bucket.list() is high-level and will not set us set max-keys,
    # using it would require using >1000 keys to test, and that would
    # be too slow; use the lower-level call bucket.get_all_keys()
    # instead
    l = bucket.get_all_keys(max_keys=2)
    eq(len(l), 2)
    eq(l.is_truncated, True)
    names = [e.name for e in l]
    eq(names, ['bar', 'baz'])

    l = bucket.get_all_keys(max_keys=2, marker=names[-1])
    eq(len(l), 1)
    eq(l.is_truncated, False)
    names = [e.name for e in l]
    eq(names, ['foo'])

def test_bucket_notexist():
    name = '{prefix}foo'.format(prefix=get_prefix())
    print 'Trying bucket {name!r}'.format(name=name)
    e = assert_raises(boto.exception.S3ResponseError, s3.main.get_bucket, name)
    eq(e.status, 404)
    eq(e.reason, 'Not Found')
    eq(e.error_code, 'NoSuchBucket')


def test_bucket_delete_notexist():
    name = '{prefix}foo'.format(prefix=get_prefix())
    print 'Trying bucket {name!r}'.format(name=name)
    e = assert_raises(boto.exception.S3ResponseError, s3.main.delete_bucket, name)
    eq(e.status, 404)
    eq(e.reason, 'Not Found')
    eq(e.error_code, 'NoSuchBucket')

def test_bucket_delete_nonempty():
    bucket = get_new_bucket()

    # fill up bucket
    key = bucket.new_key('foo')
    key.set_contents_from_string('foocontent')

    # try to delete
    e = assert_raises(boto.exception.S3ResponseError, bucket.delete)
    eq(e.status, 409)
    eq(e.reason, 'Conflict')
    eq(e.error_code, 'BucketNotEmpty')

def test_object_write_to_nonexist_bucket():
    name = '{prefix}foo'.format(prefix=get_prefix())
    print 'Trying bucket {name!r}'.format(name=name)
    bucket = s3.main.get_bucket(name, validate=False)
    key = bucket.new_key('foo123bar')
    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'foo')
    eq(e.status, 404)
    eq(e.reason, 'Not Found')
    eq(e.error_code, 'NoSuchBucket')


def test_bucket_create_delete():
    name = '{prefix}foo'.format(prefix=get_prefix())
    print 'Trying bucket {name!r}'.format(name=name)
    bucket = s3.main.create_bucket(name)
    # make sure it's actually there
    s3.main.get_bucket(bucket.name)
    bucket.delete()
    # make sure it's gone
    e = assert_raises(boto.exception.S3ResponseError, bucket.delete)
    eq(e.status, 404)
    eq(e.reason, 'Not Found')
    eq(e.error_code, 'NoSuchBucket')


def test_object_read_notexist():
    bucket = get_new_bucket()
    key = bucket.new_key('foobar')
    e = assert_raises(boto.exception.S3ResponseError, key.get_contents_as_string)
    eq(e.status, 404)
    eq(e.reason, 'Not Found')
    eq(e.error_code, 'NoSuchKey')


# While the test itself passes, there's a SAX parser error during teardown. It
# seems to be a boto bug.  It happens with both amazon and dho.
def test_object_create_unreadable():
    bucket = get_new_bucket()
    key = bucket.new_key('\x07')
    key.set_contents_from_string('bar')


# This should test the basic lifecycle of the key
def test_object_write_read_update_read_delete():
    bucket = get_new_bucket()
    # Write
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    # Read
    got = key.get_contents_as_string()
    eq(got, 'bar')
    # Update
    key.set_contents_from_string('soup')
    # Read
    got = key.get_contents_as_string()
    eq(got, 'soup')
    # Delete
    key.delete()


def _set_get_metadata(metadata, bucket=None):
    if bucket is None:
        bucket = get_new_bucket()
    key = boto.s3.key.Key(bucket)
    key.key = ('foo')
    key.set_metadata('meta1', metadata)
    key.set_contents_from_string('bar')
    key2 = bucket.get_key('foo')
    return key2.get_metadata('meta1')
 

def test_object_set_get_metadata_none_to_good():
    got = _set_get_metadata('mymeta')
    eq(got, 'mymeta')


def test_object_set_get_metadata_none_to_empty():
    got = _set_get_metadata('')
    eq(got, '')


def test_object_set_get_metadata_overwrite_to_good():
    bucket = get_new_bucket()
    got = _set_get_metadata('oldmeta', bucket)
    eq(got, 'oldmeta')
    got = _set_get_metadata('newmeta', bucket)
    eq(got, 'newmeta')


def test_object_set_get_metadata_overwrite_to_empty():
    bucket = get_new_bucket()
    got = _set_get_metadata('oldmeta', bucket)
    eq(got, 'oldmeta')
    got = _set_get_metadata('', bucket)
    eq(got, '')


# UTF-8 encoded data should pass straight through
def test_object_set_get_unicode_metadata():
    bucket = get_new_bucket()
    key = boto.s3.key.Key(bucket)
    key.key = (u'foo')
    key.set_metadata('meta1', u"Hello World\xe9")
    key.set_contents_from_string('bar')
    key2 = bucket.get_key('foo')
    got = key2.get_metadata('meta1')
    eq(got, u"Hello World\xe9")


def test_object_set_get_non_utf8_metadata():
    bucket = get_new_bucket()
    key = boto.s3.key.Key(bucket)
    key.key = ('foo')
    key.set_metadata('meta1', '\x04mymeta')
    key.set_contents_from_string('bar')
    key2 = bucket.get_key('foo')
    got = key2.get_metadata('meta1')
    eq(got, '=?UTF-8?Q?=04mymeta?=')


def _set_get_metadata_unreadable(metadata, bucket=None):
    got = _set_get_metadata(metadata, bucket)
    got = decode_header(got)
    return got


@attr('fails_on_dho')
def test_object_set_get_metadata_empty_to_unreadable_prefix():
    metadata = '\x04w'
    got = _set_get_metadata_unreadable(metadata)
    eq(got, [(metadata, 'utf-8')])


@attr('fails_on_dho')
def test_object_set_get_metadata_empty_to_unreadable_suffix():
    metadata = 'h\x04'
    got = _set_get_metadata_unreadable(metadata)
    eq(got, [(metadata, 'utf-8')])


@attr('fails_on_dho')
def test_object_set_get_metadata_empty_to_unreadable_infix():
    metadata = 'h\x04w'
    got = _set_get_metadata_unreadable(metadata)
    eq(got, [(metadata, 'utf-8')])


@attr('fails_on_dho')
def test_object_set_get_metadata_overwrite_to_unreadable_prefix():
    metadata = '\x04w'
    got = _set_get_metadata_unreadable(metadata)
    eq(got, [(metadata, 'utf-8')])
    metadata2 = '\x05w'
    got2 = _set_get_metadata_unreadable(metadata2)
    eq(got2, [(metadata2, 'utf-8')])


@attr('fails_on_dho')
def test_object_set_get_metadata_overwrite_to_unreadable_suffix():
    metadata = 'h\x04'
    got = _set_get_metadata_unreadable(metadata)
    eq(got, [(metadata, 'utf-8')])
    metadata2 = 'h\x05'
    got2 = _set_get_metadata_unreadable(metadata2)
    eq(got2, [(metadata2, 'utf-8')])


@attr('fails_on_dho')
def test_object_set_get_metadata_overwrite_to_unreadable_infix():
    metadata = 'h\x04w'
    got = _set_get_metadata_unreadable(metadata)
    eq(got, [(metadata, 'utf-8')])
    metadata2 = 'h\x05w'
    got2 = _set_get_metadata_unreadable(metadata2)
    eq(got2, [(metadata2, 'utf-8')])


def test_object_write_file():
    # boto Key.set_contents_from_file / .send_file uses Expect:
    # 100-Continue, so this test exercises that (though a bit too
    # subtly)
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    data = StringIO('bar')
    key.set_contents_from_file(fp=data)
    got = key.get_contents_as_string()
    eq(got, 'bar')


def check_bad_bucket_name(name):
    e = assert_raises(boto.exception.S3ResponseError, s3.main.create_bucket, name)
    eq(e.status, 400)
    eq(e.reason, 'Bad Request')
    eq(e.error_code, 'InvalidBucketName')


# AWS does not enforce all documented bucket restrictions.
# http://docs.amazonwebservices.com/AmazonS3/2006-03-01/dev/index.html?BucketRestrictions.html
@attr('fails_on_aws')
def test_bucket_create_naming_bad_starts_nonalpha():
    check_bad_bucket_name('_alphasoup')


def test_bucket_create_naming_bad_short_empty():
    # bucket creates where name is empty look like PUTs to the parent
    # resource (with slash), hence their error response is different
    e = assert_raises(boto.exception.S3ResponseError, s3.main.create_bucket, '')
    eq(e.status, 405)
    eq(e.reason, 'Method Not Allowed')
    eq(e.error_code, 'MethodNotAllowed')


def test_bucket_create_naming_bad_short_one():
    check_bad_bucket_name('a')


def test_bucket_create_naming_bad_short_two():
    check_bad_bucket_name('aa')

def test_bucket_create_naming_bad_long():
    check_bad_bucket_name(256*'a')
    check_bad_bucket_name(280*'a')
    check_bad_bucket_name(3000*'a')


def check_good_bucket_name(name, _prefix=None):
    # prefixing to make then unique

    # tests using this with the default prefix must *not* rely on
    # being able to set the initial character, or exceed the max len

    # tests using this with a custom prefix are responsible for doing
    # their own setup/teardown nukes, with their custom prefix; this
    # should be very rare
    if _prefix is None:
        _prefix = get_prefix()
    s3.main.create_bucket('{prefix}{name}'.format(
            prefix=_prefix,
            name=name,
            ))


def _test_bucket_create_naming_good_long(length):
    prefix = get_prefix()
    assert len(prefix) < 255
    num = length - len(prefix)
    s3.main.create_bucket('{prefix}{name}'.format(
            prefix=prefix,
            name=num*'a',
            ))


def test_bucket_create_naming_good_long_250():
    _test_bucket_create_naming_good_long(250)


def test_bucket_create_naming_good_long_251():
    _test_bucket_create_naming_good_long(251)


def test_bucket_create_naming_good_long_252():
    _test_bucket_create_naming_good_long(252)


def test_bucket_create_naming_good_long_253():
    _test_bucket_create_naming_good_long(253)


def test_bucket_create_naming_good_long_254():
    _test_bucket_create_naming_good_long(254)


def test_bucket_create_naming_good_long_255():
    _test_bucket_create_naming_good_long(255)

def test_bucket_list_long_name():
    prefix = get_prefix()
    length = 251
    num = length - len(prefix)
    bucket = s3.main.create_bucket('{prefix}{name}'.format(
            prefix=prefix,
            name=num*'a',
            ))
    got = bucket.list()
    got = list(got)
    eq(got, [])


# AWS does not enforce all documented bucket restrictions.
# http://docs.amazonwebservices.com/AmazonS3/2006-03-01/dev/index.html?BucketRestrictions.html
@attr('fails_on_aws')
def test_bucket_create_naming_bad_ip():
    check_bad_bucket_name('192.168.5.123')


def test_bucket_create_naming_bad_punctuation():
    # characters other than [a-zA-Z0-9._-]
    check_bad_bucket_name('alpha!soup')


# test_bucket_create_naming_dns_* are valid but not recommended

def test_bucket_create_naming_dns_underscore():
    check_good_bucket_name('foo_bar')


def test_bucket_create_naming_dns_long():
    prefix = get_prefix()
    assert len(prefix) < 50
    num = 100 - len(prefix)
    check_good_bucket_name(num * 'a')


def test_bucket_create_naming_dns_dash_at_end():
    check_good_bucket_name('foo-')


def test_bucket_create_naming_dns_dot_dot():
    check_good_bucket_name('foo..bar')


def test_bucket_create_naming_dns_dot_dash():
    check_good_bucket_name('foo.-bar')


def test_bucket_create_naming_dns_dash_dot():
    check_good_bucket_name('foo-.bar')


def test_bucket_create_exists():
    bucket = get_new_bucket()
    # REST idempotency means this should be a nop
    s3.main.create_bucket(bucket.name)


def test_bucket_create_exists_nonowner():
    # Names are shared across a global namespace. As such, no two
    # users can create a bucket with that same name.
    bucket = get_new_bucket()
    e = assert_raises(boto.exception.S3CreateError, s3.alt.create_bucket, bucket.name)
    eq(e.status, 409)
    eq(e.reason, 'Conflict')
    eq(e.error_code, 'BucketAlreadyExists')


def test_bucket_delete_nonowner():
    bucket = get_new_bucket()
    check_access_denied(s3.alt.delete_bucket, bucket.name)


def test_bucket_acl_default():
    bucket = get_new_bucket()
    policy = bucket.get_acl()
    print repr(policy)
    eq(policy.owner.type, None)
    eq(policy.owner.id, config.main.user_id)
    eq(policy.owner.display_name, config.main.display_name)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            ],
        )


def test_bucket_acl_canned():
    bucket = get_new_bucket()
    # Since it defaults to private, set it public-read first
    bucket.set_acl('public-read')
    policy = bucket.get_acl()
    print repr(policy)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission='READ',
                id=None,
                display_name=None,
                uri='http://acs.amazonaws.com/groups/global/AllUsers',
                email_address=None,
                type='Group',
                ),
            ],
        )

    # Then back to private.
    bucket.set_acl('private')
    policy = bucket.get_acl()
    print repr(policy)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            ],
        )


def test_bucket_acl_canned_publicreadwrite():
    bucket = get_new_bucket()
    bucket.set_acl('public-read-write')
    policy = bucket.get_acl()
    print repr(policy)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission='READ',
                id=None,
                display_name=None,
                uri='http://acs.amazonaws.com/groups/global/AllUsers',
                email_address=None,
                type='Group',
                ),
            dict(
                permission='WRITE',
                id=None,
                display_name=None,
                uri='http://acs.amazonaws.com/groups/global/AllUsers',
                email_address=None,
                type='Group',
                ),
            ],
        )


def test_bucket_acl_canned_authenticatedread():
    bucket = get_new_bucket()
    bucket.set_acl('authenticated-read')
    policy = bucket.get_acl()
    print repr(policy)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission='READ',
                id=None,
                display_name=None,
                uri='http://acs.amazonaws.com/groups/global/AuthenticatedUsers',
                email_address=None,
                type='Group',
                ),
            ],
        )


def test_object_acl_default():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    policy = key.get_acl()
    print repr(policy)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            ],
        )


def test_object_acl_canned():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    # Since it defaults to private, set it public-read first
    key.set_acl('public-read')
    policy = key.get_acl()
    print repr(policy)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission='READ',
                id=None,
                display_name=None,
                uri='http://acs.amazonaws.com/groups/global/AllUsers',
                email_address=None,
                type='Group',
                ),
            ],
        )

    # Then back to private.
    key.set_acl('private')
    policy = key.get_acl()
    print repr(policy)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            ],
        )


def test_object_acl_canned_publicreadwrite():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    key.set_acl('public-read-write')
    policy = key.get_acl()
    print repr(policy)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission='READ',
                id=None,
                display_name=None,
                uri='http://acs.amazonaws.com/groups/global/AllUsers',
                email_address=None,
                type='Group',
                ),
            dict(
                permission='WRITE',
                id=None,
                display_name=None,
                uri='http://acs.amazonaws.com/groups/global/AllUsers',
                email_address=None,
                type='Group',
                ),
            ],
        )


def test_object_acl_canned_authenticatedread():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    key.set_acl('authenticated-read')
    policy = key.get_acl()
    print repr(policy)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission='READ',
                id=None,
                display_name=None,
                uri='http://acs.amazonaws.com/groups/global/AuthenticatedUsers',
                email_address=None,
                type='Group',
                ),
            ],
        )


def test_bucket_acl_canned_private_to_private():
    bucket = get_new_bucket()
    bucket.set_acl('private')


def _make_acl_xml(acl):
    return '<?xml version="1.0" encoding="UTF-8"?><AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Owner><ID>' + config.main.user_id + '</ID></Owner>' + acl.to_xml() + '</AccessControlPolicy>'


def _build_bucket_acl_xml(permission, bucket=None):
    acl = boto.s3.acl.ACL()
    acl.add_user_grant(permission=permission, user_id=config.main.user_id)
    XML = _make_acl_xml(acl)
    if bucket is None:
        bucket = get_new_bucket()
    bucket.set_xml_acl(XML)
    policy = bucket.get_acl()
    print repr(policy)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission=permission,
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            ],
        )


def test_bucket_acl_xml_fullcontrol():
    _build_bucket_acl_xml('FULL_CONTROL')


def test_bucket_acl_xml_write():
    _build_bucket_acl_xml('WRITE')


def test_bucket_acl_xml_writeacp():
    _build_bucket_acl_xml('WRITE_ACP')


def test_bucket_acl_xml_read():
    _build_bucket_acl_xml('READ')


def test_bucket_acl_xml_readacp():
    _build_bucket_acl_xml('READ_ACP')


def _build_object_acl_xml(permission):
    acl = boto.s3.acl.ACL()
    acl.add_user_grant(permission=permission, user_id=config.main.user_id)
    XML = _make_acl_xml(acl)
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    key.set_xml_acl(XML)
    policy = key.get_acl()
    print repr(policy)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission=permission,
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            ],
        )


def test_object_acl_xml():
    _build_object_acl_xml('FULL_CONTROL')


def test_object_acl_xml_write():
    _build_object_acl_xml('WRITE')


def test_object_acl_xml_writeacp():
    _build_object_acl_xml('WRITE_ACP')


def test_object_acl_xml_read():
    _build_object_acl_xml('READ')


def test_object_acl_xml_readacp():
    _build_object_acl_xml('READ_ACP')


def _bucket_acl_grant_userid(permission):
    bucket = get_new_bucket()
    # add alt user
    policy = bucket.get_acl()
    policy.acl.add_user_grant(permission, config.alt.user_id)
    bucket.set_acl(policy)
    policy = bucket.get_acl()
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission=permission,
                id=config.alt.user_id,
                display_name=config.alt.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            ],
        )

    return bucket


def _check_bucket_acl_grant_can_read(bucket):
    bucket2 = s3.alt.get_bucket(bucket.name)


def _check_bucket_acl_grant_cant_read(bucket):
    check_access_denied(s3.alt.get_bucket, bucket.name)


def _check_bucket_acl_grant_can_readacp(bucket):
    bucket2 = s3.alt.get_bucket(bucket.name, validate=False)
    bucket2.get_acl()


def _check_bucket_acl_grant_cant_readacp(bucket):
    bucket2 = s3.alt.get_bucket(bucket.name, validate=False)
    check_access_denied(bucket2.get_acl)


def _check_bucket_acl_grant_can_write(bucket):
    bucket2 = s3.alt.get_bucket(bucket.name, validate=False)
    key = bucket2.new_key('foo-write')
    key.set_contents_from_string('bar')


def _check_bucket_acl_grant_cant_write(bucket):
    bucket2 = s3.alt.get_bucket(bucket.name, validate=False)
    key = bucket2.new_key('foo-write')
    check_access_denied(key.set_contents_from_string, 'bar')


def _check_bucket_acl_grant_can_writeacp(bucket):
    bucket2 = s3.alt.get_bucket(bucket.name, validate=False)
    bucket2.set_acl('public-read')


def _check_bucket_acl_grant_cant_writeacp(bucket):
    bucket2 = s3.alt.get_bucket(bucket.name, validate=False)
    check_access_denied(bucket2.set_acl, 'public-read')


def test_bucket_acl_grant_userid_fullcontrol():
    bucket = _bucket_acl_grant_userid('FULL_CONTROL')

    # alt user can read
    _check_bucket_acl_grant_can_read(bucket)
    # can read acl
    _check_bucket_acl_grant_can_readacp(bucket)
    # can write
    _check_bucket_acl_grant_can_write(bucket)
    # can write acl
    _check_bucket_acl_grant_can_writeacp(bucket)


def test_bucket_acl_grant_userid_read():
    bucket = _bucket_acl_grant_userid('READ')

    # alt user can read
    _check_bucket_acl_grant_can_read(bucket)
    # can't read acl
    _check_bucket_acl_grant_cant_readacp(bucket)
    # can't write
    _check_bucket_acl_grant_cant_write(bucket)
    # can't write acl
    _check_bucket_acl_grant_cant_writeacp(bucket)


def test_bucket_acl_grant_userid_readacp():
    bucket = _bucket_acl_grant_userid('READ_ACP')

    # alt user can't read
    _check_bucket_acl_grant_cant_read(bucket)
    # can read acl
    _check_bucket_acl_grant_can_readacp(bucket)
    # can't write
    _check_bucket_acl_grant_cant_write(bucket)
    # can't write acp
    #_check_bucket_acl_grant_cant_writeacp_can_readacp(bucket)
    _check_bucket_acl_grant_cant_writeacp(bucket)

def test_bucket_acl_grant_userid_write():
    bucket = _bucket_acl_grant_userid('WRITE')

    # alt user can't read
    _check_bucket_acl_grant_cant_read(bucket)
    # can't read acl
    _check_bucket_acl_grant_cant_readacp(bucket)
    # can write
    _check_bucket_acl_grant_can_write(bucket)
    # can't write acl
    _check_bucket_acl_grant_cant_writeacp(bucket)


def test_bucket_acl_grant_userid_writeacp():
    bucket = _bucket_acl_grant_userid('WRITE_ACP')

    # alt user can't read
    _check_bucket_acl_grant_cant_read(bucket)
    # can't read acl
    _check_bucket_acl_grant_cant_readacp(bucket)
    # can't write
    _check_bucket_acl_grant_cant_write(bucket)
    # can write acl
    _check_bucket_acl_grant_can_writeacp(bucket)


@attr('fails_on_dho')
def test_bucket_acl_grant_nonexist_user():
    bucket = get_new_bucket()
    # add alt user
    bad_user_id = '_foo'
    policy = bucket.get_acl()
    policy.acl.add_user_grant('FULL_CONTROL', bad_user_id)
    print policy.to_xml()
    e = assert_raises(boto.exception.S3ResponseError, bucket.set_acl, policy)
    eq(e.status, 400)
    eq(e.reason, 'Bad Request')
    eq(e.error_code, 'InvalidArgument')


def test_bucket_acl_no_grants():
    bucket = get_new_bucket()

    # write content to the bucket
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')

    # clear grants
    policy = bucket.get_acl()
    policy.acl.grants = []

    # remove read/write permission
    bucket.set_acl(policy)

    # can read
    bucket.get_key('foo')

    # can't write
    key = bucket.new_key('baz')
    check_access_denied(key.set_contents_from_string, 'bar')

    # can read acl
    bucket.get_acl()

    # can write acl
    bucket.set_acl('private')


# This test will fail on DH Objects. DHO allows multiple users with one account, which
# would violate the uniqueness requirement of a user's email. As such, DHO users are
# created without an email.
@attr('fails_on_dho')
def test_bucket_acl_grant_email():
    bucket = get_new_bucket()
    # add alt user
    policy = bucket.get_acl()
    policy.acl.add_email_grant('FULL_CONTROL', config.alt.email)
    bucket.set_acl(policy)
    policy = bucket.get_acl()
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission='FULL_CONTROL',
                id=config.alt.user_id,
                display_name=config.alt.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            ],
        )

    # alt user can write
    bucket2 = s3.alt.get_bucket(bucket.name)
    key = bucket2.new_key('foo')
    key.set_contents_from_string('bar')


def test_bucket_acl_grant_email_notexist():
    # behavior not documented by amazon
    bucket = get_new_bucket()
    policy = bucket.get_acl()
    policy.acl.add_email_grant('FULL_CONTROL', NONEXISTENT_EMAIL)
    e = assert_raises(boto.exception.S3ResponseError, bucket.set_acl, policy)
    eq(e.status, 400)
    eq(e.reason, 'Bad Request')
    eq(e.error_code, 'UnresolvableGrantByEmailAddress')


def test_bucket_acl_revoke_all():
    # revoke all access, including the owner's access
    bucket = get_new_bucket()
    policy = bucket.get_acl()
    policy.acl.grants = []
    bucket.set_acl(policy)
    policy = bucket.get_acl()
    eq(len(policy.acl.grants), 0)


# TODO rgw log_bucket.set_as_logging_target() gives 403 Forbidden
# http://tracker.newdream.net/issues/984
@attr('fails_on_rgw')
def test_logging_toggle():
    bucket = get_new_bucket()
    log_bucket = s3.main.create_bucket(bucket.name + '-log')
    log_bucket.set_as_logging_target()
    bucket.enable_logging(target_bucket=log_bucket, target_prefix=bucket.name)
    bucket.disable_logging()


def _setup_access(bucket_acl, object_acl):
    """
    Simple test fixture: create a bucket with given ACL, with objects:

    - a: given ACL
    - b: default ACL
    """
    obj = bunch.Bunch()
    bucket = get_new_bucket()
    bucket.set_acl(bucket_acl)
    obj.a = bucket.new_key('foo')
    obj.a.set_contents_from_string('foocontent')
    obj.a.set_acl(object_acl)
    obj.b = bucket.new_key('bar')
    obj.b.set_contents_from_string('barcontent')

    obj.bucket2 = s3.alt.get_bucket(bucket.name, validate=False)
    obj.a2 = obj.bucket2.new_key(obj.a.name)
    obj.b2 = obj.bucket2.new_key(obj.b.name)
    obj.new = obj.bucket2.new_key('new')

    return obj


def get_bucket_key_names(bucket):
    return frozenset(k.name for k in bucket.list())


def test_access_bucket_private_object_private():
    # all the test_access_* tests follow this template
    obj = _setup_access(bucket_acl='private', object_acl='private')
    # acled object read fail
    check_access_denied(obj.a2.get_contents_as_string)
    # acled object write fail
    check_access_denied(obj.a2.set_contents_from_string, 'barcontent')
    # default object read fail
    check_access_denied(obj.b2.get_contents_as_string)
    # default object write fail
    check_access_denied(obj.b2.set_contents_from_string, 'baroverwrite')
    # bucket read fail
    check_access_denied(get_bucket_key_names, obj.bucket2)
    # bucket write fail
    check_access_denied(obj.new.set_contents_from_string, 'newcontent')


def test_access_bucket_private_object_publicread():
    obj = _setup_access(bucket_acl='private', object_acl='public-read')
    eq(obj.a2.get_contents_as_string(), 'foocontent')
    check_access_denied(obj.a2.set_contents_from_string, 'foooverwrite')
    check_access_denied(obj.b2.get_contents_as_string)
    check_access_denied(obj.b2.set_contents_from_string, 'baroverwrite')
    check_access_denied(get_bucket_key_names, obj.bucket2)
    check_access_denied(obj.new.set_contents_from_string, 'newcontent')


def test_access_bucket_private_object_publicreadwrite():
    obj = _setup_access(bucket_acl='private', object_acl='public-read-write')
    eq(obj.a2.get_contents_as_string(), 'foocontent')
    ### TODO: it seems AWS denies this write, even when we expected it
    ### to complete; as it is unclear what the actual desired behavior
    ### is (the docs are somewhat unclear), we'll just codify current
    ### AWS behavior, at least for now.
    # obj.a2.set_contents_from_string('foooverwrite')
    check_access_denied(obj.a2.set_contents_from_string, 'foooverwrite')
    check_access_denied(obj.b2.get_contents_as_string)
    check_access_denied(obj.b2.set_contents_from_string, 'baroverwrite')
    check_access_denied(get_bucket_key_names, obj.bucket2)
    check_access_denied(obj.new.set_contents_from_string, 'newcontent')


def test_access_bucket_publicread_object_private():
    obj = _setup_access(bucket_acl='public-read', object_acl='private')
    check_access_denied(obj.a2.get_contents_as_string)
    check_access_denied(obj.a2.set_contents_from_string, 'barcontent')
    ### TODO: i don't understand why this gets denied, but codifying what
    ### AWS does
    # eq(obj.b2.get_contents_as_string(), 'barcontent')
    check_access_denied(obj.b2.get_contents_as_string)
    check_access_denied(obj.b2.set_contents_from_string, 'baroverwrite')
    eq(get_bucket_key_names(obj.bucket2), frozenset(['foo', 'bar']))
    check_access_denied(obj.new.set_contents_from_string, 'newcontent')


def test_access_bucket_publicread_object_publicread():
    obj = _setup_access(bucket_acl='public-read', object_acl='public-read')
    eq(obj.a2.get_contents_as_string(), 'foocontent')
    check_access_denied(obj.a2.set_contents_from_string, 'foooverwrite')
    ### TODO: i don't understand why this gets denied, but codifying what
    ### AWS does
    # eq(obj.b2.get_contents_as_string(), 'barcontent')
    check_access_denied(obj.b2.get_contents_as_string)
    check_access_denied(obj.b2.set_contents_from_string, 'baroverwrite')
    eq(get_bucket_key_names(obj.bucket2), frozenset(['foo', 'bar']))
    check_access_denied(obj.new.set_contents_from_string, 'newcontent')


def test_access_bucket_publicread_object_publicreadwrite():
    obj = _setup_access(bucket_acl='public-read', object_acl='public-read-write')
    eq(obj.a2.get_contents_as_string(), 'foocontent')
    ### TODO: it seems AWS denies this write, even when we expected it
    ### to complete; as it is unclear what the actual desired behavior
    ### is (the docs are somewhat unclear), we'll just codify current
    ### AWS behavior, at least for now.
    # obj.a2.set_contents_from_string('foooverwrite')
    check_access_denied(obj.a2.set_contents_from_string, 'foooverwrite')
    ### TODO: i don't understand why this gets denied, but codifying what
    ### AWS does
    # eq(obj.b2.get_contents_as_string(), 'barcontent')
    check_access_denied(obj.b2.get_contents_as_string)
    check_access_denied(obj.b2.set_contents_from_string, 'baroverwrite')
    eq(get_bucket_key_names(obj.bucket2), frozenset(['foo', 'bar']))
    check_access_denied(obj.new.set_contents_from_string, 'newcontent')


def test_access_bucket_publicreadwrite_object_private():
    obj = _setup_access(bucket_acl='public-read-write', object_acl='private')
    check_access_denied(obj.a2.get_contents_as_string)
    obj.a2.set_contents_from_string('barcontent')
    ### TODO: i don't understand why this gets denied, but codifying what
    ### AWS does
    # eq(obj.b2.get_contents_as_string(), 'barcontent')
    check_access_denied(obj.b2.get_contents_as_string)
    obj.b2.set_contents_from_string('baroverwrite')
    eq(get_bucket_key_names(obj.bucket2), frozenset(['foo', 'bar']))
    obj.new.set_contents_from_string('newcontent')


def test_access_bucket_publicreadwrite_object_publicread():
    obj = _setup_access(bucket_acl='public-read-write', object_acl='public-read')
    eq(obj.a2.get_contents_as_string(), 'foocontent')
    obj.a2.set_contents_from_string('barcontent')
    ### TODO: i don't understand why this gets denied, but codifying what
    ### AWS does
    # eq(obj.b2.get_contents_as_string(), 'barcontent')
    check_access_denied(obj.b2.get_contents_as_string)
    obj.b2.set_contents_from_string('baroverwrite')
    eq(get_bucket_key_names(obj.bucket2), frozenset(['foo', 'bar']))
    obj.new.set_contents_from_string('newcontent')


def test_access_bucket_publicreadwrite_object_publicreadwrite():
    obj = _setup_access(bucket_acl='public-read-write', object_acl='public-read-write')
    eq(obj.a2.get_contents_as_string(), 'foocontent')
    obj.a2.set_contents_from_string('foooverwrite')
    ### TODO: i don't understand why this gets denied, but codifying what
    ### AWS does
    # eq(obj.b2.get_contents_as_string(), 'barcontent')
    check_access_denied(obj.b2.get_contents_as_string)
    obj.b2.set_contents_from_string('baroverwrite')
    eq(get_bucket_key_names(obj.bucket2), frozenset(['foo', 'bar']))
    obj.new.set_contents_from_string('newcontent')

def test_object_set_valid_acl():
    XML_1 = '<?xml version="1.0" encoding="UTF-8"?><AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Owner><ID>' + config.main.user_id + '</ID></Owner><AccessControlList><Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser"><ID>' + config.main.user_id + '</ID></Grantee><Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>'
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    key.set_xml_acl(XML_1)

def test_object_giveaway():
    CORRECT_ACL = '<?xml version="1.0" encoding="UTF-8"?><AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Owner><ID>' + config.main.user_id + '</ID></Owner><AccessControlList><Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser"><ID>' + config.main.user_id + '</ID></Grantee><Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>'
    WRONG_ACL = '<?xml version="1.0" encoding="UTF-8"?><AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Owner><ID>' + config.alt.user_id + '</ID></Owner><AccessControlList><Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser"><ID>' + config.alt.user_id + '</ID></Grantee><Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>'
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    key.set_xml_acl(CORRECT_ACL)
    e = assert_raises(boto.exception.S3ResponseError, key.set_xml_acl, WRONG_ACL)
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'AccessDenied')

def test_buckets_create_then_list():
    create_buckets = [get_new_bucket() for i in xrange(5)]
    list_buckets = s3.main.get_all_buckets()
    names = frozenset(bucket.name for bucket in list_buckets)
    for bucket in create_buckets:
        if bucket.name not in names:
            raise RuntimeError("S3 implementation's GET on Service did not return bucket we created: %r", bucket.name)

# Common code to create a connection object, which'll use bad authorization information
def _create_connection_bad_auth():
    # We're going to need to manually build a connection using bad authorization info.
    # But to save the day, lets just hijack the settings from s3.main. :)
    main = s3.main
    conn = boto.s3.connection.S3Connection(
        aws_access_key_id='badauth',
        aws_secret_access_key='roflmao',
        is_secure=main.is_secure,
        port=main.port,
        host=main.host,
        calling_format=main.calling_format,
        )
    return conn

def test_list_buckets_anonymous():
    # Get a connection with bad authorization, then change it to be our new Anonymous auth mechanism,
    # emulating standard HTTP access.
    #
    # While it may have been possible to use httplib directly, doing it this way takes care of also
    # allowing us to vary the calling format in testing.
    conn = _create_connection_bad_auth()
    conn._auth_handler = AnonymousAuth.AnonymousAuthHandler(None, None, None) # Doesn't need this
    buckets = conn.get_all_buckets()
    eq(len(buckets), 0)

def test_list_buckets_bad_auth():
    conn = _create_connection_bad_auth()
    e = assert_raises(boto.exception.S3ResponseError, conn.get_all_buckets)
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'AccessDenied')

# this test goes outside the user-configure prefix because it needs to
# control the initial character of the bucket name
@attr('fails_on_rgw')
@nose.with_setup(
    setup=lambda: nuke_prefixed_buckets(prefix='a'+get_prefix()),
    teardown=lambda: nuke_prefixed_buckets(prefix='a'+get_prefix()),
    )
def test_bucket_create_naming_good_starts_alpha():
    check_good_bucket_name('foo', _prefix='a'+get_prefix())

# this test goes outside the user-configure prefix because it needs to
# control the initial character of the bucket name
@attr('fails_on_rgw')
@nose.with_setup(
    setup=lambda: nuke_prefixed_buckets(prefix='0'+get_prefix()),
    teardown=lambda: nuke_prefixed_buckets(prefix='0'+get_prefix()),
    )
def test_bucket_create_naming_good_starts_digit():
    check_good_bucket_name('foo', _prefix='0'+get_prefix())

def test_bucket_create_naming_good_contains_period():
    check_good_bucket_name('aaa.111')

def test_bucket_create_naming_good_contains_hyphen():
    check_good_bucket_name('aaa-111')

def test_object_copy_same_bucket():
    bucket = get_new_bucket()
    key = bucket.new_key('foo123bar')
    key.set_contents_from_string('foo')
    key.copy(bucket, 'bar321foo')
    key2 = bucket.get_key('bar321foo')
    eq(key2.get_contents_as_string(), 'foo')

def test_object_copy_diff_bucket():
    buckets = [get_new_bucket(), get_new_bucket()]
    key = buckets[0].new_key('foo123bar')
    key.set_contents_from_string('foo')
    key.copy(buckets[1], 'bar321foo')
    key2 = buckets[1].get_key('bar321foo')
    eq(key2.get_contents_as_string(), 'foo')

# is this a necessary check? a NoneType object is being touched here
# it doesn't get to the S3 level
def test_object_copy_not_owned_bucket():
    buckets = [get_new_bucket(), get_new_bucket(s3.alt)]
    print repr(buckets[1])
    key = buckets[0].new_key('foo123bar')
    key.set_contents_from_string('foo')

    try:
        key.copy(buckets[1], 'bar321foo')
    except AttributeError:
        pass

def transfer_part(bucket, mp_id, mp_keyname, i, part):
    """Transfer a part of a multipart upload. Designed to be run in parallel.
    """
    mp = boto.s3.multipart.MultiPartUpload(bucket)
    mp.key_name = mp_keyname
    mp.id = mp_id
    part_out = StringIO(part)
    mp.upload_part_from_file(part_out, i+1)

def generate_random(mb_size):
    mb = 1024 * 1024
    chunk = 1024
    part_size_mb = 5
    allowed = string.ascii_letters
    for x in range(0, mb_size, part_size_mb):
        strpart = ''.join([allowed[random.randint(0, len(allowed) - 1)] for x in xrange(chunk)])
        s = ''
        left = mb_size - x
        this_part_size = min(left, part_size_mb)
        for y in range(this_part_size * mb / chunk):
            s = s + strpart
        yield s
        if (x == mb_size):
            return

def _multipart_upload(bucket, s3_key_name, mb_size, do_list=None):
    upload = bucket.initiate_multipart_upload(s3_key_name)
    for i, part in enumerate(generate_random(mb_size)):
        transfer_part(bucket, upload.id, upload.key_name, i, part)

    if do_list is not None:
        l = bucket.list_multipart_uploads()
        l = list(l)

    return upload

def test_multipart_upload():
    bucket = get_new_bucket()
    key="mymultipart"
    upload = _multipart_upload(bucket, key, 30)
    upload.complete_upload()

def test_abort_multipart_upload():
    bucket = get_new_bucket()
    key="mymultipart"
    upload = _multipart_upload(bucket, key, 10)
    upload.cancel_upload()


def test_list_multipart_upload():
    bucket = get_new_bucket()
    key="mymultipart"
    upload1 = _multipart_upload(bucket, key, 5, 1)
    upload2 = _multipart_upload(bucket, key, 5, 1)

    key2="mymultipart2"
    upload3 = _multipart_upload(bucket, key2, 5, 1)

    upload1.cancel_upload()
    upload2.cancel_upload()
    upload3.cancel_upload()

def _simple_http_req_100_cont(host, port, is_secure, method, resource):
    req = '{method} {resource} HTTP/1.1\r\nHost: {host}\r\nAccept-Encoding: identity\r\nContent-Length: 123\r\nExpect: 100-continue\r\n\r\n'.format(
            method=method,
            resource=resource,
            host=host,
            )

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if is_secure:
        s = ssl.wrap_socket(s);
    s.settimeout(5)
    s.connect((host, port))
    s.send(req)

    try:
        data = s.recv(1024)
    except socket.error, msg:
        print 'got response: ', msg
        print 'most likely server doesn\'t support 100-continue'

    s.close()
    l = data.split(' ')

    assert l[0].startswith('HTTP')

    return l[1]

def test_100_continue():
    bucket = get_new_bucket()
    objname = 'testobj'
    resource = '/{bucket}/{obj}'.format(bucket=bucket.name, obj=objname)

    status = _simple_http_req_100_cont(s3.main.host, s3.main.port, s3.main.is_secure, 'PUT', resource)
    eq(status, '403')

    bucket.set_acl('public-read-write')

    status = _simple_http_req_100_cont(s3.main.host, s3.main.port, s3.main.is_secure, 'PUT', resource)
    eq(status, '100')

def _test_bucket_acls_changes_persistent(bucket):
    perms = ('FULL_CONTROL', 'WRITE', 'WRITE_ACP', 'READ', 'READ_ACP')
    for p in perms:
        _build_bucket_acl_xml(p, bucket)

def test_bucket_acls_changes_persistent():
    bucket = get_new_bucket()
    _test_bucket_acls_changes_persistent(bucket);

def test_stress_bucket_acls_changes():
    bucket = get_new_bucket()
    for i in xrange(10):
        _test_bucket_acls_changes_persistent(bucket);

class FakeFile(object):
    def __init__(self, size, char='A', interrupt=None):
        self.offset = 0
        self.size = size
        self.char = char
        self.interrupt = interrupt

    def seek(self, offset):
        self.offset = offset

    def tell(self):
        return self.offset

    def read(self, size=-1):
        if size < 0:
            size = self.size - self.offset
        count = min(size, self.size - self.offset)
        self.offset += count

        # Sneaky! do stuff before we return (the last time)
        if self.interrupt != None and self.offset == self.size and count > 0:
            self.interrupt()

        return self.char*count

class FakeFileVerifier(object):
    def __init__(self, char=None):
        self.char = char
        self.size = 0

    def write(self, data):
        size = len(data)
        if self.char == None:
            self.char = data[0]
        self.size += size
        eq(data, self.char*size)

def _verify_atomic_key_data(key, size=-1, char=None):
    fp_verify = FakeFileVerifier(char)
    key.get_contents_to_file(fp_verify)
    if size >= 0:
        eq(fp_verify.size, size)

def _test_atomic_write(file_size):
    bucket = get_new_bucket()
    objname = 'testobj'
    key = bucket.new_key(objname)

    # create <file_size> file of A's
    fp_a = FakeFile(file_size, 'A')
    key.set_contents_from_file(fp_a)

    # verify A's
    _verify_atomic_key_data(key, file_size, 'A')

    # create <file_size> file of B's
    # but try to verify the file before we finish writing all the B's
    fp_b = FakeFile(file_size, 'B',
        lambda: _verify_atomic_key_data(key, file_size)
        )
    key.set_contents_from_file(fp_b)

    # verify B's
    _verify_atomic_key_data(key, file_size, 'B')

def test_atomic_write_1mb():
    _test_atomic_write(1024*1024)

def test_atomic_write_4mb():
    _test_atomic_write(1024*1024*4)

def test_atomic_write_8mb():
    _test_atomic_write(1024*1024*8)

def _test_atomic_dual_write(file_size):
    bucket = get_new_bucket()
    objname = 'testobj'
    key = bucket.new_key(objname)

    # get a second key object (for the same key)
    # so both can be writing without interfering
    key2 = bucket.new_key(objname)

    # write <file_size> file of B's
    # but before we're done, try to write all A's
    fp_a = FakeFile(file_size, 'A')
    fp_b = FakeFile(file_size, 'B',
        lambda: key2.set_contents_from_file(fp_a)
        )
    key.set_contents_from_file(fp_b)

    # verify the file
    _verify_atomic_key_data(key, file_size)

def test_atomic_dual_write_1mb():
    _test_atomic_dual_write(1024*1024)

def test_atomic_dual_write_4mb():
    _test_atomic_dual_write(1024*1024*4)

def test_atomic_dual_write_8mb():
    _test_atomic_dual_write(1024*1024*8)

def test_ranged_request_response_code():
    content = 'testcontent'

    bucket = get_new_bucket()
    key = bucket.new_key('testobj')
    key.set_contents_from_string(content)

    key.open('r', headers={'Range': 'bytes=4-7'})
    status = key.resp.status
    fetched_content = ''
    for data in key:
        fetched_content += data;
    key.close()

    eq(fetched_content, content[4:8])
    eq(status, 206)

