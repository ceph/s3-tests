from __future__ import print_function
import sys
from cStringIO import StringIO
import collections
import boto.exception
import boto.s3.connection
import boto.s3.acl
import bunch
import datetime
import time
import email.utils
import isodate
import nose
import operator
import socket
import ssl
import os
import requests
import base64
import hmac
import sha
import pytz
import json
import httplib2
import threading
import itertools
import string
import random

import xml.etree.ElementTree as ET

from httplib import HTTPConnection, HTTPSConnection
from urlparse import urlparse

from nose.tools import eq_ as eq, ok_ as ok
from nose.plugins.attrib import attr
from nose.plugins.skip import SkipTest

from .utils import assert_raises
from .utils import generate_random
from .utils import region_sync_meta
import AnonymousAuth

from email.header import decode_header
from ordereddict import OrderedDict

from boto.s3.cors import CORSConfiguration

from . import (
    get_new_bucket,
    get_new_bucket_name,
    s3,
    config,
    _make_raw_request,
    choose_bucket_prefix,
    )

WEBSITE_CONFIGS_XMLFRAG = {
        'IndexDoc': '<IndexDocument><Suffix>${IndexDocument_Suffix}</Suffix></IndexDocument>',
        'IndexDocErrorDoc': '<IndexDocument><Suffix>${IndexDocument_Suffix}</Suffix></IndexDocument><ErrorDocument><Key>${ErrorDocument_Key}</Key></ErrorDocument>',
        'RedirectAll': '<RedirectAllRequestsTo><HostName>${RedirectAllRequestsTo_HostName}</HostName></RedirectAllRequestsTo>',
        'RedirectAll+Protocol': '<RedirectAllRequestsTo><HostName>${RedirectAllRequestsTo_HostName}</HostName><Protocol>${RedirectAllRequestsTo_Protocol}</Protocol></RedirectAllRequestsTo>',
        }

def make_website_config(xml_fragment):
    """
    Take the tedious stuff out of the config
    """
    return '<?xml version="1.0" encoding="UTF-8"?><WebsiteConfiguration xmlns="http://doc.s3.amazonaws.com/doc/2006-03-01/">' + xml_fragment + '</WebsiteConfiguration>'

def get_website_url(proto, bucket, path):
    """
    Return the URL to a website page
    """
    domain = config['main']['host']
    if('s3website_domain' in config['main']):
        domain = config['main']['s3website_domain']
    elif('s3website_domain' in config['alt']):
        domain = config['DEFAULT']['s3website_domain']
    path = path.lstrip('/')
    return "%s://%s.%s/%s" % (proto, bucket, domain, path)

def _test_website_populate_fragment(xml_fragment, fields):
    f = {
          'IndexDocument_Suffix': choose_bucket_prefix(template='index-{random}.html', max_len=32),
          'ErrorDocument_Key': choose_bucket_prefix(template='error-{random}.html', max_len=32),
          'RedirectAllRequestsTo_HostName': choose_bucket_prefix(template='{random}.{random}.com', max_len=32),
        }
    f.update(fields)
    xml_fragment = string.Template(xml_fragment).safe_substitute(**f)
    return xml_fragment, f

def _test_website_prep(bucket, xml_template, hardcoded_fields = {}):
    xml_fragment, f = _test_website_populate_fragment(xml_template, hardcoded_fields)
    config_xml = make_website_config(xml_fragment)
    print(config_xml)
    bucket.set_website_configuration_xml(config_xml)
    eq (config_xml, bucket.get_website_configuration_xml())
    return f

def __website_expected_reponse_status(res, status, reason):
    if not isinstance(status, collections.Container):
        status = set([status])
    if not isinstance(reason, collections.Container):
        reason = set([reason])

    ok(res.status in status, 'HTTP status code mismatch')
    ok(res.reason in reason, 'HTTP reason mismatch')

def _website_expected_error_response(res, bucket_name, status, reason, code):
    body = res.read()
    print(body)
    __website_expected_reponse_status(res, status, reason)
    ok('<li>Code: '+code+'</li>' in body, 'HTML should contain "Code: %s" ' % (code, ))
    ok(('<li>BucketName: %s</li>' % (bucket_name, )) in body, 'HTML should contain bucket name')

def _website_request(bucket_name, path, method='GET'):
    url = get_website_url('http', bucket_name, path)
    print("url", url)

    o = urlparse(url)
    path = o.path + '?' + o.query
    request_headers={}
    request_headers['Host'] = o.hostname
    print('Request: {method} {path} {headers}'.format(method=method, path=path, headers=' '.join(map(lambda t: t[0]+':'+t[1]+"\n", request_headers.items()))))
    res = _make_raw_request(config.main.host, config.main.port, method, path, request_headers=request_headers, secure=False)
    for (k,v) in res.getheaders():
        print(k,v)
    return res

# ---------- Non-existant buckets via the website endpoint
@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='non-existant bucket via website endpoint should give NoSuchBucket, exposing security risk')
@attr('s3website')
@attr('fails_on_rgw')
def test_website_nonexistant_bucket_s3():
    bucket_name = get_new_bucket_name()
    res = _website_request(bucket_name, '')
    _website_expected_error_response(res, bucket_name, 404, 'Not Found', 'NoSuchBucket')

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='non-existant bucket via website endpoint should give Forbidden, keeping bucket identity secure')
@attr('s3website')
@attr('fails_on_s3')
def test_website_nonexistant_bucket_rgw():
    bucket_name = get_new_bucket_name()
    res = _website_request(bucket_name, '')
    _website_expected_error_response(res, bucket_name, 403, 'Forbidden', 'AccessDenied')

#------------- IndexDocument only, successes
@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='non-empty public buckets via s3website return page for /, where page is public')
@attr('s3website')
def test_website_public_bucket_list_public_index():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDoc'])
    bucket.make_public()
    indexhtml = bucket.new_key(f['IndexDocument_Suffix'])
    indexstring = choose_bucket_prefix(template='<html><body>{random}</body></html>', max_len=256)
    indexhtml.set_contents_from_string(indexstring)
    indexhtml.make_public()

    res = _website_request(bucket.name, '')
    body = res.read()
    print(body)
    eq(body, indexstring) # default content should match index.html set content
    __website_expected_reponse_status(res, 200, 'OK')
    indexhtml.delete()
    bucket.delete()

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='non-empty private buckets via s3website return page for /, where page is private')
@attr('s3website')
def test_website_private_bucket_list_public_index():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDoc'])
    bucket.set_canned_acl('private')
    indexhtml = bucket.new_key(f['IndexDocument_Suffix'])
    indexstring = choose_bucket_prefix(template='<html><body>{random}</body></html>', max_len=256)
    indexhtml.set_contents_from_string(indexstring)
    indexhtml.make_public()

    res = _website_request(bucket.name, '')
    __website_expected_reponse_status(res, 200, 'OK')
    body = res.read()
    print(body)
    eq(body, indexstring, 'default content should match index.html set content')
    indexhtml.delete()
    bucket.delete()


# ---------- IndexDocument only, failures
@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='empty private buckets via s3website return a 403 for /')
@attr('s3website')
def test_website_private_bucket_list_empty():
    bucket = get_new_bucket()
    bucket.set_canned_acl('private')
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDoc'])

    res = _website_request(bucket.name, '')
    _website_expected_error_response(res, bucket.name, 403, 'Forbidden', 'AccessDenied')
    bucket.delete()

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='empty public buckets via s3website return a 404 for /')
@attr('s3website')
def test_website_public_bucket_list_empty():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDoc'])
    bucket.make_public()

    res = _website_request(bucket.name, '')
    _website_expected_error_response(res, bucket.name, 404, 'Not Found', 'NoSuchKey')
    bucket.delete()

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='non-empty public buckets via s3website return page for /, where page is private')
@attr('s3website')
def test_website_public_bucket_list_private_index():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDoc'])
    bucket.make_public()
    indexhtml = bucket.new_key(f['IndexDocument_Suffix'])
    indexstring = choose_bucket_prefix(template='<html><body>{random}</body></html>', max_len=256)
    indexhtml.set_contents_from_string(indexstring)
    indexhtml.set_canned_acl('private')

    res = _website_request(bucket.name, '')
    _website_expected_error_response(res, bucket.name, 403, 'Forbidden', 'AccessDenied')
    indexhtml.delete()
    bucket.delete()

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='non-empty private buckets via s3website return page for /, where page is private')
@attr('s3website')
def test_website_private_bucket_list_private_index():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDoc'])
    bucket.set_canned_acl('private')
    indexhtml = bucket.new_key(f['IndexDocument_Suffix'])
    indexstring = choose_bucket_prefix(template='<html><body>{random}</body></html>', max_len=256)
    indexhtml.set_contents_from_string(indexstring)
    indexhtml.set_canned_acl('private')

    res = _website_request(bucket.name, '')
    _website_expected_error_response(res, bucket.name, 403, 'Forbidden', 'AccessDenied')

    indexhtml.delete()
    bucket.delete()

# ---------- IndexDocument & ErrorDocument, failures due to errordoc assigned but missing
@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='empty private buckets via s3website return a 403 for /, missing errordoc')
@attr('s3website')
def test_website_private_bucket_list_empty_missingerrordoc():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDocErrorDoc'])
    bucket.set_canned_acl('private')

    res = _website_request(bucket.name, '')
    _website_expected_error_response(res, bucket.name, 403, 'Forbidden', 'AccessDenied')
    body = res.read()
    print(body)
    
    bucket.delete()

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='empty public buckets via s3website return a 404 for /, missing errordoc')
@attr('s3website')
def test_website_public_bucket_list_empty_missingerrordoc():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDocErrorDoc'])
    bucket.make_public()

    res = _website_request(bucket.name, '')
    _website_expected_error_response(res, bucket.name, 404, 'Not Found', 'NoSuchKey')
    bucket.delete()

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='non-empty public buckets via s3website return page for /, where page is private, missing errordoc')
@attr('s3website')
def test_website_public_bucket_list_private_index_missingerrordoc():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDocErrorDoc'])
    bucket.make_public()
    indexhtml = bucket.new_key(f['IndexDocument_Suffix'])
    indexstring = choose_bucket_prefix(template='<html><body>{random}</body></html>', max_len=256)
    indexhtml.set_contents_from_string(indexstring)
    indexhtml.set_canned_acl('private')

    res = _website_request(bucket.name, '')
    _website_expected_error_response(res, bucket.name, 403, 'Forbidden', 'AccessDenied')

    indexhtml.delete()
    bucket.delete()

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='non-empty private buckets via s3website return page for /, where page is private, missing errordoc')
@attr('s3website')
def test_website_private_bucket_list_private_index_missingerrordoc():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDocErrorDoc'])
    bucket.set_canned_acl('private')
    indexhtml = bucket.new_key(f['IndexDocument_Suffix'])
    indexstring = choose_bucket_prefix(template='<html><body>{random}</body></html>', max_len=256)
    indexhtml.set_contents_from_string(indexstring)
    indexhtml.set_canned_acl('private')

    res = _website_request(bucket.name, '')
    _website_expected_error_response(res, bucket.name, 403, 'Forbidden', 'AccessDenied')

    indexhtml.delete()
    bucket.delete()

# ---------- IndexDocument & ErrorDocument, failures due to errordoc assigned but not accessible
@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='empty private buckets via s3website return a 403 for /, blocked errordoc')
@attr('s3website')
def test_website_private_bucket_list_empty_blockederrordoc():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDocErrorDoc'])
    bucket.set_canned_acl('private')
    errorhtml = bucket.new_key(f['ErrorDocument_Key'])
    errorstring = choose_bucket_prefix(template='<html><body>{random}</body></html>', max_len=256)
    errorhtml.set_contents_from_string(errorstring)
    errorhtml.set_canned_acl('private')

    res = _website_request(bucket.name, '')
    _website_expected_error_response(res, bucket.name, 403, 'Forbidden', 'AccessDenied')
    body = res.read()
    print(body)
    ok(errorstring not in body, 'error content should match error.html set content')

    errorhtml.delete()
    bucket.delete()

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='empty public buckets via s3website return a 404 for /, blocked errordoc')
@attr('s3website')
def test_website_public_bucket_list_empty_blockederrordoc():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDocErrorDoc'])
    bucket.make_public()
    errorhtml = bucket.new_key(f['ErrorDocument_Key'])
    errorstring = choose_bucket_prefix(template='<html><body>{random}</body></html>', max_len=256)
    errorhtml.set_contents_from_string(errorstring)
    errorhtml.set_canned_acl('private')

    res = _website_request(bucket.name, '')
    _website_expected_error_response(res, bucket.name, 404, 'Not Found', 'NoSuchKey')
    body = res.read()
    print(body)
    ok(errorstring not in body, 'error content should match error.html set content')

    errorhtml.delete()
    bucket.delete()

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='non-empty public buckets via s3website return page for /, where page is private, blocked errordoc')
@attr('s3website')
def test_website_public_bucket_list_private_index_blockederrordoc():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDocErrorDoc'])
    bucket.make_public()
    indexhtml = bucket.new_key(f['IndexDocument_Suffix'])
    indexstring = choose_bucket_prefix(template='<html><body>{random}</body></html>', max_len=256)
    indexhtml.set_contents_from_string(indexstring)
    indexhtml.set_canned_acl('private')
    errorhtml = bucket.new_key(f['ErrorDocument_Key'])
    errorstring = choose_bucket_prefix(template='<html><body>{random}</body></html>', max_len=256)
    errorhtml.set_contents_from_string(errorstring)
    errorhtml.set_canned_acl('private')

    res = _website_request(bucket.name, '')
    _website_expected_error_response(res, bucket.name, 403, 'Forbidden', 'AccessDenied')
    body = res.read()
    print(body)
    ok(errorstring not in body, 'error content should match error.html set content')

    indexhtml.delete()
    errorhtml.delete()
    bucket.delete()

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='non-empty private buckets via s3website return page for /, where page is private, blocked errordoc')
@attr('s3website')
def test_website_private_bucket_list_private_index_blockederrordoc():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDocErrorDoc'])
    bucket.set_canned_acl('private')
    indexhtml = bucket.new_key(f['IndexDocument_Suffix'])
    indexstring = choose_bucket_prefix(template='<html><body>{random}</body></html>', max_len=256)
    indexhtml.set_contents_from_string(indexstring)
    indexhtml.set_canned_acl('private')
    errorhtml = bucket.new_key(f['ErrorDocument_Key'])
    errorstring = choose_bucket_prefix(template='<html><body>{random}</body></html>', max_len=256)
    errorhtml.set_contents_from_string(errorstring)
    errorhtml.set_canned_acl('private')

    res = _website_request(bucket.name, '')
    _website_expected_error_response(res, bucket.name, 403, 'Forbidden', 'AccessDenied')
    body = res.read()
    print(body)
    ok(errorstring not in body, 'error content should match error.html set content')

    indexhtml.delete()
    errorhtml.delete()
    bucket.delete()

# ---------- IndexDocument & ErrorDocument, failures with errordoc available
@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='empty private buckets via s3website return a 403 for /, good errordoc')
@attr('s3website')
def test_website_private_bucket_list_empty_gooderrordoc():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDocErrorDoc'])
    bucket.set_canned_acl('private')
    errorhtml = bucket.new_key(f['ErrorDocument_Key'])
    errorstring = choose_bucket_prefix(template='<html><body>{random}</body></html>', max_len=256)
    errorhtml.set_contents_from_string(errorstring)
    errorhtml.set_canned_acl('public-read')

    res = _website_request(bucket.name, '')
    _website_expected_error_response(res, bucket.name, 403, 'Forbidden', 'AccessDenied')
    body = res.read()
    print(body)
    eq(body, errorstring, 'error content should match error.html set content')

    errorhtml.delete()
    bucket.delete()

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='empty public buckets via s3website return a 404 for /, good errordoc')
@attr('s3website')
def test_website_public_bucket_list_empty_gooderrordoc():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDocErrorDoc'])
    bucket.make_public()
    errorhtml = bucket.new_key(f['ErrorDocument_Key'])
    errorstring = choose_bucket_prefix(template='<html><body>{random}</body></html>', max_len=256)
    errorhtml.set_contents_from_string(errorstring)
    errorhtml.set_canned_acl('public-read')

    res = _website_request(bucket.name, '')
    _website_expected_error_response(res, bucket.name, 404, 'Not Found', 'NoSuchKey')
    body = res.read()
    print(body)
    eq(body, errorstring, 'error content should match error.html set content')

    errorhtml.delete()
    bucket.delete()

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='non-empty public buckets via s3website return page for /, where page is private')
@attr('s3website')
def test_website_public_bucket_list_private_index_gooderrordoc():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDocErrorDoc'])
    bucket.make_public()
    indexhtml = bucket.new_key(f['IndexDocument_Suffix'])
    indexstring = choose_bucket_prefix(template='<html><body>{random}</body></html>', max_len=256)
    indexhtml.set_contents_from_string(indexstring)
    indexhtml.set_canned_acl('private')
    errorhtml = bucket.new_key(f['ErrorDocument_Key'])
    errorstring = choose_bucket_prefix(template='<html><body>{random}</body></html>', max_len=256)
    errorhtml.set_contents_from_string(errorstring)
    errorhtml.set_canned_acl('public-read')

    res = _website_request(bucket.name, '')
    _website_expected_error_response(res, bucket.name, 403, 'Forbidden', 'AccessDenied')
    body = res.read()
    print(body)
    eq(body, errorstring, 'error content should match error.html set content')

    indexhtml.delete()
    errorhtml.delete()
    bucket.delete()

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='non-empty private buckets via s3website return page for /, where page is private')
@attr('s3website')
def test_website_private_bucket_list_private_index_gooderrordoc():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDocErrorDoc'])
    bucket.set_canned_acl('private')
    indexhtml = bucket.new_key(f['IndexDocument_Suffix'])
    indexstring = choose_bucket_prefix(template='<html><body>{random}</body></html>', max_len=256)
    indexhtml.set_contents_from_string(indexstring)
    indexhtml.set_canned_acl('private')
    errorhtml = bucket.new_key(f['ErrorDocument_Key'])
    errorstring = choose_bucket_prefix(template='<html><body>{random}</body></html>', max_len=256)
    errorhtml.set_contents_from_string(errorstring)
    errorhtml.set_canned_acl('public-read')

    res = _website_request(bucket.name, '')
    _website_expected_error_response(res, bucket.name, 403, 'Forbidden', 'AccessDenied')
    body = res.read()
    print(body)
    eq(body, errorstring, 'error content should match error.html set content')

    indexhtml.delete()
    errorhtml.delete()
    bucket.delete()
