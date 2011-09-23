from cStringIO import StringIO
import boto.connection
import boto.exception
import boto.s3.connection
import boto.s3.acl
import boto.utils
import bunch
import nose
import operator
import random
import string
import socket
import ssl

from boto.s3.connection import S3Connection

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


_orig_conn = {}
_orig_authorize = None
_custom_headers = {}
_remove_headers = []
boto_type = None


# HeaderS3Connection and _our_authorize are necessary to be able to arbitrarily
# overwrite headers. Depending on the version of boto, one or the other is
# necessary. We later determine in setup what needs to be used.

def _update_headers(headers):
    global _custom_headers, _remove_headers

    headers.update(_custom_headers)

    for header in _remove_headers:
        try:
            del headers[header]
        except KeyError:
            pass


# Note: We need to update the headers twice. The first time so the
# authentication signing is done correctly. The second time to overwrite any
# headers modified or created in the authentication step.

class HeaderS3Connection(S3Connection):
    def fill_in_auth(self, http_request, **kwargs):
        _update_headers(http_request.headers)
        S3Connection.fill_in_auth(self, http_request, **kwargs)
        _update_headers(http_request.headers)

        return http_request


def _our_authorize(self, connection, **kwargs):
    _update_headers(self.headers)
    _orig_authorize(self, connection, **kwargs)
    _update_headers(self.headers)


def setup():
    global boto_type

    # we determine what we need to replace by the existence of particular
    # attributes. boto 2.0rc1 as fill_in_auth for S3Connection, while boto 2.0
    # has authorize for HTTPRequest.
    if hasattr(S3Connection, 'fill_in_auth'):
        global _orig_conn

        boto_type = 'S3Connection'
        for conn in s3:
            _orig_conn[conn] = s3[conn]
            header_conn = HeaderS3Connection(
                aws_access_key_id=s3[conn].aws_access_key_id,
                aws_secret_access_key=s3[conn].aws_secret_access_key,
                is_secure=s3[conn].is_secure,
                port=s3[conn].port,
                host=s3[conn].host,
                calling_format=s3[conn].calling_format
                )

            s3[conn] = header_conn
    elif hasattr(boto.connection.HTTPRequest, 'authorize'):
        global _orig_authorize

        boto_type = 'HTTPRequest'

        _orig_authorize = boto.connection.HTTPRequest.authorize
        boto.connection.HTTPRequest.authorize = _our_authorize
    else:
        raise RuntimeError


def teardown():
    global boto_type

    # replace original functionality depending on the boto version
    if boto_type is 'S3Connection':
        global _orig_conn
        for conn in s3:
            s3[conn] = _orig_conn[conn]
        _orig_conn = {}
    elif boto_type is 'HTTPRequest':
        global _orig_authorize

        boto.connection.HTTPRequest.authorize = _orig_authorize
        _orig_authorize = None
    else:
        raise RuntimeError


def _clear_custom_headers():
    global _custom_headers, _remove_headers
    _custom_headers = {}
    _remove_headers = []


def _add_custom_headers(headers=None, remove=None):
    global _custom_headers, _remove_headers
    if not _custom_headers:
        _custom_headers = {}

    if headers is not None:
        _custom_headers.update(headers)
    if remove is not None:
        _remove_headers.extend(remove)


def _setup_bad_object(headers=None, remove=None):
    bucket = get_new_bucket()

    _add_custom_headers(headers=headers, remove=remove)
    return bucket.new_key('foo')
 

@nose.with_setup(teardown=_clear_custom_headers)
@attr('fails_on_dho')
def test_object_create_bad_md5_invalid():
    key = _setup_bad_object({'Content-MD5':'AWS HAHAHA'})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 400)
    eq(e.reason, 'Bad Request')
    eq(e.error_code, 'InvalidDigest')


@nose.with_setup(teardown=_clear_custom_headers)
@attr('fails_on_dho')
def test_object_create_bad_md5_wrong():
    key = _setup_bad_object({'Content-MD5':'YWJyYWNhZGFicmE='})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 400)
    eq(e.reason, 'Bad Request')
    eq(e.error_code, 'InvalidDigest')


@nose.with_setup(teardown=_clear_custom_headers)
@attr('fails_on_dho')
def test_object_create_bad_md5_empty():
    key = _setup_bad_object({'Content-MD5': ''})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 400)
    eq(e.reason, 'Bad Request')
    eq(e.error_code, 'InvalidDigest')


@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_md5_unreadable():
    key = _setup_bad_object({'Content-MD5': '\x07'})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    assert e.error_code in ('AccessDenied', 'SignatureDoesNotMatch')


@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_md5_none():
    key = _setup_bad_object(remove=('Content-MD5',))
    key.set_contents_from_string('bar')


# strangely, amazon doesn't report an error with a non-expect 100 also, our
# error comes back as html, and not xml as I normally expect
@nose.with_setup(teardown=_clear_custom_headers)
@attr('fails_on_dho')
@attr('fails_on_rgw')
def test_object_create_bad_expect_mismatch():
    key = _setup_bad_object({'Expect': 200})
    key.set_contents_from_string('bar')


# this is a really long test, and I don't know if it's valid...
# again, accepts this with no troubles
@nose.with_setup(teardown=_clear_custom_headers)
@attr('fails_on_dho')
def test_object_create_bad_expect_empty():
    key = _setup_bad_object({'Expect': ''})
    key.set_contents_from_string('bar')


@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_expect_none():
    key = _setup_bad_object(remove=('Expect',))
    key.set_contents_from_string('bar')


# this is a really long test..
@nose.with_setup(teardown=_clear_custom_headers)
@attr('fails_on_dho')
@attr('fails_on_rgw')
def test_object_create_bad_expect_unreadable():
    key = _setup_bad_object({'Expect': '\x07'})
    key.set_contents_from_string('bar')


@nose.with_setup(teardown=_clear_custom_headers)
@attr('fails_on_dho')
@attr('fails_on_rgw')
def test_object_create_bad_contentlength_empty():
    key = _setup_bad_object({'Content-Length': ''})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 400)
    eq(e.reason, 'Bad Request')
    eq(e.error_code, None)


@nose.with_setup(teardown=_clear_custom_headers)
@attr('fails_on_dho')
@attr('fails_on_rgw')
def test_object_create_bad_contentlength_negative():
    key = _setup_bad_object({'Content-Length': -1})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 400)
    eq(e.reason, 'Bad Request')
    eq(e.error_code, None)


@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_contentlength_none():
    key = _setup_bad_object(remove=('Content-Length',))

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 411)
    eq(e.reason, 'Length Required')
    eq(e.error_code,'MissingContentLength')


@nose.with_setup(teardown=_clear_custom_headers)
@attr('fails_on_dho')
@attr('fails_on_rgw')
def test_object_create_bad_contentlength_unreadable():
    key = _setup_bad_object({'Content-Length': '\x07'})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 400)
    eq(e.reason, 'Bad Request')
    eq(e.error_code, None)


@nose.with_setup(teardown=_clear_custom_headers)
@attr('fails_on_dho')
@attr('fails_on_rgw')
def test_object_create_bad_contentlength_mismatch_above():
    content = 'bar'
    length = len(content) + 1

    key = _setup_bad_object({'Content-Length': length})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, content)
    eq(e.status, 400)
    eq(e.reason, 'Bad Request')
    eq(e.error_code, 'RequestTimeout')


@nose.with_setup(teardown=_clear_custom_headers)
@attr('fails_on_dho')
def test_object_create_bad_contentlength_mismatch_below():
    content = 'bar'
    length = len(content) - 1
    key = _setup_bad_object({'Content-Length': length})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, content)
    eq(e.status, 400)
    # dho is 'Bad request', which doesn't match the http response code
    eq(e.reason, 'Bad Request')
    eq(e.error_code, 'BadDigest')


@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_contenttype_invalid():
    key = _setup_bad_object({'Content-Type': 'text/plain'})
    key.set_contents_from_string('bar')


@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_contenttype_empty():
    key = _setup_bad_object({'Content-Type': ''})
    key.set_contents_from_string('bar')


@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_contenttype_none():
    key = _setup_bad_object(remove=('Content-Type',))
    key.set_contents_from_string('bar')


@nose.with_setup(teardown=_clear_custom_headers)
@attr('fails_on_rgw')
@attr('fails_on_dho')
def test_object_create_bad_contenttype_unreadable():
    key = _setup_bad_object({'Content-Type': '\x08'})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    assert e.error_code in ('AccessDenied', 'SignatureDoesNotMatch')


@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_contenttype_none():
    key = _setup_bad_object(remove=('Content-Type',))
    key.set_contents_from_string('bar')


@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_ua_invalid():
    key = _setup_bad_object({'User-Agent': ''})
    key.set_contents_from_string('bar')


@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_ua_empty():
    key = _setup_bad_object({'User-Agent': ''})
    key.set_contents_from_string('bar')


@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_ua_unreadable():
    key = _setup_bad_object({'User-Agent': '\x07'})
    key.set_contents_from_string('bar')


@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_ua_none():
    key = _setup_bad_object(remove=('User-Agent',))
    key.set_contents_from_string('bar')


@nose.with_setup(teardown=_clear_custom_headers)
@attr('fails_on_dho')
@attr('fails_on_rgw')
def test_object_create_bad_authorization_invalid():
    key = _setup_bad_object({'Authorization': 'AWS HAHAHA'})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 400)
    eq(e.reason, 'Bad Request')
    eq(e.error_code, 'InvalidArgument')


# the teardown is really messed up here. check it out
@nose.with_setup(teardown=_clear_custom_headers)
@attr('fails_on_rgw')
def test_object_create_bad_authorization_unreadable():
    key = _setup_bad_object({'Authorization': '\x07'})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'AccessDenied')


@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_authorization_empty():
    key = _setup_bad_object({'Authorization': ''})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'AccessDenied')


# the teardown is really messed up here. check it out
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_authorization_none():
    key = _setup_bad_object(remove=('Authorization',))

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'AccessDenied')


@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_authorization_incorrect():
    key = _setup_bad_object({'Authorization': 'AWS AKIAIGR7ZNNBHC5BKSUA:FWeDfwojDSdS2Ztmpfeubhd9isU='})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    assert e.error_code in ('AccessDenied', 'SignatureDoesNotMatch')


@nose.with_setup(teardown=_clear_custom_headers)
@attr('fails_on_dho')
def test_object_create_bad_date_invalid():
    key = _setup_bad_object({'Date': 'Bad Date'})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'AccessDenied')


@nose.with_setup(teardown=_clear_custom_headers)
@attr('fails_on_dho')
def test_object_create_bad_date_empty():
    key = _setup_bad_object({'Date': ''})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'AccessDenied')


@nose.with_setup(teardown=_clear_custom_headers)
@attr('fails_on_dho')
def test_object_create_bad_date_unreadable():
    key = _setup_bad_object({'Date': '\x07'})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'AccessDenied')


@nose.with_setup(teardown=_clear_custom_headers)
@attr('fails_on_dho')
def test_object_create_bad_date_none():
    key = _setup_bad_object(remove=('Date',))

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'AccessDenied')


@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_date_before_today():
    key = _setup_bad_object({'Date': 'Tue, 07 Jul 2010 21:53:04 GMT'})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'RequestTimeTooSkewed')


@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_date_after_today():
    key = _setup_bad_object({'Date': 'Tue, 07 Jul 2030 21:53:04 GMT'})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'RequestTimeTooSkewed')


@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_date_before_epoch():
    key = _setup_bad_object({'Date': 'Tue, 07 Jul 1950 21:53:04 GMT'})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'AccessDenied')


@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_date_after_end():
    key = _setup_bad_object({'Date': 'Tue, 07 Jul 9999 21:53:04 GMT'})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'RequestTimeTooSkewed')


@nose.with_setup(teardown=_clear_custom_headers)
@attr('fails_on_dho')
@attr('fails_on_rgw')
def test_bucket_create_contentlength_none():
    _add_custom_headers(remove=('Content-Length',))
    get_new_bucket()


@nose.with_setup(teardown=_clear_custom_headers)
@attr('fails_on_rgw')
def test_object_acl_create_contentlength_none():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('blah')

    _add_custom_headers(remove=('Content-Length',))
    key.set_acl('public-read')
