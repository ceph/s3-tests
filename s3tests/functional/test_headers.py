from cStringIO import StringIO
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


_orig_merge_meta = None
_custom_headers = None
_remove_headers = None

def setup():

    # Replace boto.utils.merge_data
    global _orig_merge_meta
    assert _orig_merge_meta is None

    _orig_merge_meta = boto.utils.merge_meta
    boto.utils.merge_meta = _our_merge_meta

    _clear_custom_headers()

def teardown():

    # Restore boto.utils.merge_data
    global _orig_merge_meta
    assert _orig_merge_meta is not None

    boto.utils.merge_meta = _orig_merge_meta
    _orig_merge_meta = None


def _our_merge_meta(*args, **kwargs):
    """
    Our implementation of boto.utils.merge_meta. The intent here is to make
    sure we can overload whichever headers we need to.
    """

    global _orig_merge_meta, _custom_headers, _remove_headers
    final_headers = _orig_merge_meta(*args, **kwargs)
    final_headers.update(_custom_headers)

    print _remove_headers
    for header in _remove_headers:
        del final_headers[header]

    print final_headers
    return final_headers


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
def test_object_create_bad_md5():
    key = _setup_bad_object({'Content-MD5':'AWS HAHAHA'})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 400)
    eq(e.reason, 'Bad Request')
    eq(e.error_code, 'InvalidDigest')


# strangely, amazon doesn't report an error with a non-expect 100 also, our
# error comes back as html, and not xml as I normally expect
@nose.with_setup(teardown=_clear_custom_headers)
@attr('fails_on_dho')
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


# this is a really long test..
@nose.with_setup(teardown=_clear_custom_headers)
@attr('fails_on_dho')
def test_object_create_bad_expect_utf8():
    key = _setup_bad_object({'Expect': '\x07'})
    key.set_contents_from_string('bar')


@nose.with_setup(teardown=_clear_custom_headers)
@attr('fails_on_dho')
def test_object_create_bad_contentlength_empty():
    key = _setup_bad_object({'Content-Length': ''})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 400)
    eq(e.reason, 'Bad Request')
    eq(e.error_code, None)


@nose.with_setup(teardown=_clear_custom_headers)
@attr('fails_on_dho')
def test_object_create_bad_contentlength_zero():
    key = _setup_bad_object({'Content-Length': 0})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 400)
    eq(e.reason, 'Bad Request')
    eq(e.error_code, 'BadDigest')


@nose.with_setup(teardown=_clear_custom_headers)
@attr('fails_on_dho')
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
def test_object_create_bad_contenttype_unreadable():
    key = _setup_bad_object({'Content-Type': '\x08'})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    assert e.error_code in ('AccessDenied', 'SignatureDoesNotMatch')
