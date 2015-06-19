from __future__ import print_function
import sys
import collections
import nose
import string
import random
from pprint import pprint

from urlparse import urlparse

from nose.tools import eq_ as eq, ok_ as ok
from nose.plugins.attrib import attr

from . import (
    get_new_bucket,
    get_new_bucket_name,
    s3,
    config,
    _make_raw_request,
    choose_bucket_prefix,
    )

from ..common import with_setup_kwargs
from ..xmlhelper import normalize_xml_whitespace

IGNORE_FIELD = 'IGNORETHIS'

WEBSITE_CONFIGS_XMLFRAG = {
        'IndexDoc': '<IndexDocument><Suffix>${IndexDocument_Suffix}</Suffix></IndexDocument>${RoutingRules}',
        'IndexDocErrorDoc': '<IndexDocument><Suffix>${IndexDocument_Suffix}</Suffix></IndexDocument><ErrorDocument><Key>${ErrorDocument_Key}</Key></ErrorDocument>${RoutingRules}',
        'RedirectAll': '<RedirectAllRequestsTo><HostName>${RedirectAllRequestsTo_HostName}</HostName></RedirectAllRequestsTo>${RoutingRules}',
        'RedirectAll+Protocol': '<RedirectAllRequestsTo><HostName>${RedirectAllRequestsTo_HostName}</HostName><Protocol>${RedirectAllRequestsTo_Protocol}</Protocol></RedirectAllRequestsTo>${RoutingRules}',
        }

def make_website_config(xml_fragment):
    """
    Take the tedious stuff out of the config
    """
    return '<?xml version="1.0" encoding="UTF-8"?><WebsiteConfiguration xmlns="http://doc.s3.amazonaws.com/doc/2006-03-01/">' + xml_fragment + '</WebsiteConfiguration>'

def get_website_url(**kwargs):
    """
    Return the URL to a website page
    """
    proto, bucket, hostname, path = 'http', None, None, '/'

    if 'proto' in kwargs:
        proto = kwargs['proto']
    if 'bucket' in kwargs:
        bucket = kwargs['bucket']
    if 'hostname' in kwargs:
        hostname = kwargs['hostname']
    if 'path' in kwargs:
        path = kwargs['path']
    
    domain = config['main']['host']
    if('s3website_domain' in config['main']):
        domain = config['main']['s3website_domain']
    elif('s3website_domain' in config['alt']):
        domain = config['DEFAULT']['s3website_domain']
    if hostname is None:
        hostname = '%s.%s' % (bucket, domain)
    path = path.lstrip('/')
    return "%s://%s/%s" % (proto, hostname, path)

def _test_website_populate_fragment(xml_fragment, fields):
    for k in ['RoutingRules']:
      if k in fields.keys() and len(fields[k]) > 0:
         fields[k] = '<%s>%s</%s>' % (k, fields[k], k)
    f = {
          'IndexDocument_Suffix': choose_bucket_prefix(template='index-{random}.html', max_len=32),
          'ErrorDocument_Key': choose_bucket_prefix(template='error-{random}.html', max_len=32),
          'RedirectAllRequestsTo_HostName': choose_bucket_prefix(template='{random}.{random}.com', max_len=32),
          'RoutingRules': ''
        }
    f.update(fields)
    xml_fragment = string.Template(xml_fragment).safe_substitute(**f)
    return xml_fragment, f

def _test_website_prep(bucket, xml_template, hardcoded_fields = {}):
    xml_fragment, f = _test_website_populate_fragment(xml_template, hardcoded_fields)
    config_xml1 = make_website_config(xml_fragment)
    bucket.set_website_configuration_xml(config_xml1)
    config_xml1 = normalize_xml_whitespace(config_xml1, pretty_print=True) # Do it late, so the system gets weird whitespace
    #print("config_xml1\n", config_xml1)
    config_xml2 = bucket.get_website_configuration_xml()
    config_xml2 = normalize_xml_whitespace(config_xml2, pretty_print=True) # For us to read
    #print("config_xml2\n", config_xml2)
    eq (config_xml1, config_xml2)
    f['WebsiteConfiguration'] = config_xml2
    return f

def __website_expected_reponse_status(res, status, reason):
    if not isinstance(status, collections.Container):
        status = set([status])
    if not isinstance(reason, collections.Container):
        reason = set([reason])

    if status is not IGNORE_FIELD:
        ok(res.status in status, 'HTTP code was %s should be %s' % (res.status, status))
    if reason is not IGNORE_FIELD:
        ok(res.reason in reason, 'HTTP reason was was %s should be %s' % (res.reason, reason))

def _website_expected_error_response(res, bucket_name, status, reason, code):
    body = res.read()
    print(body)
    __website_expected_reponse_status(res, status, reason)
    if code is not IGNORE_FIELD:
        ok('<li>Code: '+code+'</li>' in body, 'HTML should contain "Code: %s" ' % (code, ))
    if bucket_name is not IGNORE_FIELD:
        ok(('<li>BucketName: %s</li>' % (bucket_name, )) in body, 'HTML should contain bucket name')

def _website_expected_redirect_response(res, status, reason, new_url):
    body = res.read()
    print(body)
    __website_expected_reponse_status(res, status, reason)
    loc = res.getheader('Location', None)
    eq(loc, new_url, 'Location header should be set "%s" != "%s"' % (loc,new_url,))
    ok(len(body) == 0, 'Body of a redirect should be empty')

def _website_request(bucket_name, path, method='GET'):
    url = get_website_url(proto='http', bucket=bucket_name, path=path)
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
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDoc'])
    bucket.set_canned_acl('private')

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

# ------ RedirectAll tests
@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='RedirectAllRequestsTo without protocol should TODO')
@attr('s3website')
def test_website_bucket_private_redirectall_base():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['RedirectAll'])
    bucket.set_canned_acl('private')

    res = _website_request(bucket.name, '')
    # RGW returns "302 Found" per RFC2616
    # S3 returns 302 Moved Temporarily per RFC1945
    new_url = 'http://%s/' % f['RedirectAllRequestsTo_HostName']
    _website_expected_redirect_response(res, 302, ['Found', 'Moved Temporarily'], new_url)

    bucket.delete()

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='RedirectAllRequestsTo without protocol should TODO')
@attr('s3website')
def test_website_bucket_private_redirectall_path():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['RedirectAll'])
    bucket.set_canned_acl('private')

    pathfragment = choose_bucket_prefix(template='/{random}', max_len=16)

    res = _website_request(bucket.name, pathfragment)
    # RGW returns "302 Found" per RFC2616
    # S3 returns 302 Moved Temporarily per RFC1945
    new_url = 'http://%s%s' % (f['RedirectAllRequestsTo_HostName'], pathfragment)
    _website_expected_redirect_response(res, 302, ['Found', 'Moved Temporarily'], new_url)

    bucket.delete()

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='RedirectAllRequestsTo without protocol should TODO')
@attr('s3website')
def test_website_bucket_private_redirectall_path_upgrade():
    bucket = get_new_bucket()
    x = string.Template(WEBSITE_CONFIGS_XMLFRAG['RedirectAll+Protocol']).safe_substitute(RedirectAllRequestsTo_Protocol='https')
    f = _test_website_prep(bucket, x)
    bucket.set_canned_acl('private')

    pathfragment = choose_bucket_prefix(template='/{random}', max_len=16)

    res = _website_request(bucket.name, pathfragment)
    # RGW returns "302 Found" per RFC2616
    # S3 returns 302 Moved Temporarily per RFC1945
    new_url = 'https://%s%s' % (f['RedirectAllRequestsTo_HostName'], pathfragment)
    _website_expected_redirect_response(res, 302, ['Found', 'Moved Temporarily'], new_url)

    bucket.delete()

# ------ x-amz redirect tests
@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='x-amz-website-redirect-location should not fire without websiteconf')
@attr('s3website')
@attr('x-amz-website-redirect-location')
def test_websute_xredirect_nonwebsite():
    bucket = get_new_bucket()
    #f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['RedirectAll'])
    #bucket.set_canned_acl('private')

    k = bucket.new_key('page')
    content = 'wrong-content'
    headers = {'x-amz-website-redirect-location': '/relative'}
    k.set_contents_from_string(content, headers=headers)
    k.make_public()

    res = _website_request(bucket.name, '/page')
    # RGW returns "302 Found" per RFC2616
    # S3 returns 302 Moved Temporarily per RFC1945
    #_website_expected_redirect_response(res, 302, ['Found', 'Moved Temporarily'], new_url)
    __website_expected_reponse_status(res, 200, 'OK')
    body = res.read()
    print(body)
    eq(body, content, 'default content should match index.html set content')

    k.delete()
    bucket.delete()

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='x-amz-website-redirect-location should fire websiteconf, relative path')
@attr('s3website')
@attr('x-amz-website-redirect-location')
def test_websute_xredirect_relative():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDoc'])
    bucket.make_public()

    k = bucket.new_key('page')
    content = 'wrong-content'
    headers = {'x-amz-website-redirect-location': '/relative'}
    k.set_contents_from_string(content, headers=headers)
    k.make_public()

    res = _website_request(bucket.name, '/page')
    # RGW returns "302 Found" per RFC2616
    # S3 returns 302 Moved Temporarily per RFC1945
    new_url =  get_website_url(bucket_name=bucket.name, path='/relative')
    _website_expected_redirect_response(res, 302, ['Found', 'Moved Temporarily'], new_url)

    k.delete()
    bucket.delete()

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='x-amz-website-redirect-location should fire websiteconf, absolute')
@attr('s3website')
@attr('x-amz-website-redirect-location')
def test_websute_xredirect_abs():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDoc'])
    bucket.make_public()

    k = bucket.new_key('page')
    content = 'wrong-content'
    headers = {'x-amz-website-redirect-location': 'http://example.com/foo'}
    k.set_contents_from_string(content, headers=headers)
    k.make_public()

    res = _website_request(bucket.name, '/page')
    # RGW returns "302 Found" per RFC2616
    # S3 returns 302 Moved Temporarily per RFC1945
    new_url =  get_website_url(proto='http', hostname='example.com', path='/foo')
    _website_expected_redirect_response(res, 302, ['Found', 'Moved Temporarily'], new_url)

    k.delete()
    bucket.delete()

# ------ RoutingRules tests

# RoutingRules
ROUTING_RULES = {
    'empty': '',
    'AmazonExample1': \
"""
    <RoutingRule>
    <Condition>
      <KeyPrefixEquals>docs/</KeyPrefixEquals>
    </Condition>
    <Redirect>
      <ReplaceKeyPrefixWith>documents/</ReplaceKeyPrefixWith>
    </Redirect>
    </RoutingRule>
""",
    'AmazonExample1+Protocol=https': \
"""
    <RoutingRule>
    <Condition>
      <KeyPrefixEquals>docs/</KeyPrefixEquals>
    </Condition>
    <Redirect>
      <Protocol>https</Protocol>
      <ReplaceKeyPrefixWith>documents/</ReplaceKeyPrefixWith>
    </Redirect>
    </RoutingRule>
""",
    'AmazonExample1+Protocol=https+Hostname=xyzzy': \
"""
    <RoutingRule>
    <Condition>
      <KeyPrefixEquals>docs/</KeyPrefixEquals>
    </Condition>
    <Redirect>
      <Protocol>https</Protocol>
      <HostName>xyzzy</HostName>
      <ReplaceKeyPrefixWith>documents/</ReplaceKeyPrefixWith>
    </Redirect>
    </RoutingRule>
""",
    'AmazonExample1+Protocol=http2': \
"""
    <RoutingRule>
    <Condition>
      <KeyPrefixEquals>docs/</KeyPrefixEquals>
    </Condition>
    <Redirect>
      <Protocol>http2</Protocol>
      <ReplaceKeyPrefixWith>documents/</ReplaceKeyPrefixWith>
    </Redirect>
    </RoutingRule>
""",
   'AmazonExample2': \
"""
    <RoutingRule>
    <Condition>
       <KeyPrefixEquals>images/</KeyPrefixEquals>
    </Condition>
    <Redirect>
      <ReplaceKeyWith>folderdeleted.html</ReplaceKeyWith>
    </Redirect>
    </RoutingRule>
""",
   'AmazonExample2+HttpRedirectCode=314': \
"""
    <RoutingRule>
    <Condition>
       <KeyPrefixEquals>images/</KeyPrefixEquals>
    </Condition>
    <Redirect>
      <HttpRedirectCode>314</HttpRedirectCode>
      <ReplaceKeyWith>folderdeleted.html</ReplaceKeyWith>
    </Redirect>
    </RoutingRule>
""",
   'AmazonExample3': \
"""
    <RoutingRule>
    <Condition>
      <HttpErrorCodeReturnedEquals>404</HttpErrorCodeReturnedEquals >
    </Condition>
    <Redirect>
      <HostName>ec2-11-22-333-44.compute-1.amazonaws.com</HostName>
      <ReplaceKeyPrefixWith>report-404/</ReplaceKeyPrefixWith>
    </Redirect>
    </RoutingRule>
""",
   'AmazonExample3+KeyPrefixEquals': \
"""
    <RoutingRule>
    <Condition>
      <KeyPrefixEquals>images/</KeyPrefixEquals>
      <HttpErrorCodeReturnedEquals>404</HttpErrorCodeReturnedEquals>
    </Condition>
    <Redirect>
      <HostName>ec2-11-22-333-44.compute-1.amazonaws.com</HostName>
      <ReplaceKeyPrefixWith>report-404/</ReplaceKeyPrefixWith>
    </Redirect>
    </RoutingRule>
""",
}

ROUTING_RULES_TESTS = [
  dict(xml=dict(RoutingRules=ROUTING_RULES['empty']), url='', location=None, code=200),
  dict(xml=dict(RoutingRules=ROUTING_RULES['empty']), url='/', location=None, code=200), 
  dict(xml=dict(RoutingRules=ROUTING_RULES['empty']), url='/x', location=None, code=404), 
  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample1']), url='/', location=None, code=200), 
  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample1']), url='/x', location=None, code=404), 
  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample1']), url='/docs/', location=dict(proto='http',bucket='{bucket_name}',path='/documents/'), code=301), 
  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample1']), url='/docs/x', location=dict(proto='http',bucket='{bucket_name}',path='/documents/x'), code=301), 
  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample1+Protocol=https']), url='/', location=None, code=200), 
  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample1+Protocol=https']), url='/x', location=None, code=404), 
  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample1+Protocol=https']), url='/docs/', location=dict(proto='https',bucket='{bucket_name}',path='/documents/'), code=301), 
  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample1+Protocol=https']), url='/docs/x', location=dict(proto='https',bucket='{bucket_name}',path='/documents/x'), code=301), 
  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample1+Protocol=http2']), url='/', location=None, code=200), 
  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample1+Protocol=http2']), url='/x', location=None, code=404), 
  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample1+Protocol=http2']), url='/docs/', location=dict(proto='http2',bucket='{bucket_name}',path='/documents/'), code=301), 
  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample1+Protocol=http2']), url='/docs/x', location=dict(proto='http2',bucket='{bucket_name}',path='/documents/x'), code=301), 
  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample1+Protocol=https+Hostname=xyzzy']), url='/', location=None, code=200), 
  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample1+Protocol=https+Hostname=xyzzy']), url='/x', location=None, code=404), 
  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample1+Protocol=https+Hostname=xyzzy']), url='/docs/', location=dict(proto='https',hostname='xyzzy',path='/documents/'), code=301), 
  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample1+Protocol=https+Hostname=xyzzy']), url='/docs/x', location=dict(proto='https',hostname='xyzzy',path='/documents/x'), code=301), 
  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample2']), url='/images/', location=dict(proto='http',bucket='{bucket_name}',path='/folderdeleted.html'), code=301), 
  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample2']), url='/images/x', location=dict(proto='http',bucket='{bucket_name}',path='/folderdeleted.html'), code=301), 
  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample2+HttpRedirectCode=314']), url='/images/', location=dict(proto='http',bucket='{bucket_name}',path='/folderdeleted.html'), code=314), 
  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample2+HttpRedirectCode=314']), url='/images/x', location=dict(proto='http',bucket='{bucket_name}',path='/folderdeleted.html'), code=314), 
  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample3']), url='/x', location=dict(proto='http',bucket='ec2-11-22-333-44.compute-1.amazonaws.com',path='/report-404/x'), code=301), 
  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample3']), url='/images/x', location=dict(proto='http',bucket='ec2-11-22-333-44.compute-1.amazonaws.com',path='/report-404/images/x'), code=301), 
  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample3+KeyPrefixEquals']), url='/x', location=None, code=404), 
  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample3+KeyPrefixEquals']), url='/images/x', location=dict(proto='http',bucket='ec2-11-22-333-44.compute-1.amazonaws.com',path='/report-404/x'), code=301), 
]

def routing_setup():
  kwargs = {'obj':[]}
  bucket = get_new_bucket()
  kwargs['bucket'] = bucket
  kwargs['obj'].append(bucket)
  f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDocErrorDoc'])
  kwargs.update(f)
  bucket.set_canned_acl('public-read')

  k = bucket.new_key(f['IndexDocument_Suffix'])
  kwargs['obj'].append(k)
  s = choose_bucket_prefix(template='<html><h1>Index</h1><body>{random}</body></html>', max_len=64)
  k.set_contents_from_string(s)
  k.set_canned_acl('public-read')

  k = bucket.new_key(f['ErrorDocument_Key'])
  kwargs['obj'].append(k)
  s = choose_bucket_prefix(template='<html><h1>Error</h1><body>{random}</body></html>', max_len=64)
  k.set_contents_from_string(s)
  k.set_canned_acl('public-read')

  return kwargs

def routing_teardown(**kwargs):
  for o in reversed(kwargs['obj']):
    print('Deleting', str(o))
    o.delete()
  
           
@with_setup_kwargs(setup=routing_setup, teardown=routing_teardown) 
def routing_check(*args, **kwargs):
    bucket = kwargs['bucket']
    args=args[0]
    #print(args)
    pprint(args)
    xml_fields = kwargs.copy()
    xml_fields.update(args['xml'])
    pprint(xml_fields)
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDocErrorDoc'], hardcoded_fields=xml_fields)
    #print(f)
    config_xml2 = bucket.get_website_configuration_xml()
    config_xml2 = normalize_xml_whitespace(config_xml2, pretty_print=True) # For us to read
    res = _website_request(bucket.name, args['url'])
    print(config_xml2)
    # RGW returns "302 Found" per RFC2616
    # S3 returns 302 Moved Temporarily per RFC1945
    new_url = args['location']
    if new_url is not None:
        new_url = get_website_url(**new_url)
        new_url = new_url.format(bucket_name=bucket.name)
    if args['code'] >= 200 and args['code'] < 300:
        #body = res.read()
        #print(body)
        #eq(body, args['content'], 'default content should match index.html set content')
        ok(res.getheader('Content-Length', -1) > 0)
    elif args['code'] >= 300 and args['code'] < 400:
        _website_expected_redirect_response(res, args['code'], IGNORE_FIELD, new_url)
    elif args['code'] >= 400:
        _website_expected_error_response(res, bucket.name, args['code'], IGNORE_FIELD, IGNORE_FIELD)
    else:
        assert(False)

@attr('RoutingRules')
def testGEN_routing():

    for t in ROUTING_RULES_TESTS:
        yield routing_check, t

    

