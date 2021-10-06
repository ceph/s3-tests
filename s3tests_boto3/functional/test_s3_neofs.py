import requests
import time

from nose.plugins.attrib import attr
from botocore.exceptions import ClientError
from botocore.credentials import Credentials
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from nose.tools import eq_ as eq
from .utils import assert_raises
from .utils import _get_status_and_error_code
from .utils import _get_status

from . import (
    get_client,
    get_new_bucket,
    get_new_bucket_name,
    get_new_bucket_resource,
    get_config_endpoint,
    get_main_aws_access_key,
    get_main_aws_secret_key,
)


def _setup_bucket_acl(bucket_acl=None):
    """
    set up a new bucket with specified acl
    """
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(ACL=bucket_acl, Bucket=bucket_name)

    return bucket_name


def _create_objects(bucket=None, bucket_name=None, keys=[]):
    """
    Populate a (specified or new) bucket with objects with
    specified names (and contents identical to their names).
    """
    if bucket_name is None:
        bucket_name = get_new_bucket_name()
    if bucket is None:
        bucket = get_new_bucket_resource(name=bucket_name)

    for key in keys:
        obj = bucket.put_object(Body=key, Key=key)

    return bucket_name


def _get_post_url(bucket_name):
    endpoint = get_config_endpoint()
    return '{endpoint}/{bucket_name}'.format(endpoint=endpoint, bucket_name=bucket_name)


def _cors_request_and_check(method, url, headers, expected_status, expected_headers, with_creds=False):
    request = AWSRequest(method=method, url=url, headers=headers)

    if with_creds:
        credentials = Credentials(get_main_aws_access_key(), get_main_aws_secret_key())
        client = get_client()
        SigV4Auth(credentials, "service_name", client.meta.region_name).add_auth(request)

    r = requests.request(method=method, url=url, headers=dict(request.headers))
    print(r.headers)
    if expected_status is not None:
        eq(r.status_code, expected_status)

    for h, v in expected_headers.items():
        print(h, ':', r.headers.get(h), '==', v)
        assert r.headers.get(h) == v


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all keys with list-objects-v2')
@attr(assertion='no pagination, empty continuationtoken')
@attr('list-objects-v2')
def test_bucket_listv2_continuationtoken_empty():
    key_names = ['bar', 'baz', 'foo', 'quxx']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    e = assert_raises(ClientError, client.list_objects_v2, Bucket=bucket_name, ContinuationToken='')
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 400)
    eq(error_code, 'InvalidArgument')


@attr(resource='bucket')
@attr(method='put')
@attr(operation='set cors')
@attr(assertion='succeeds')
@attr('cors')
def test_set_cors():
    bucket_name = get_new_bucket()
    client = get_client()
    allowed_methods = ['GET', 'PUT']
    allowed_origins = ['origin1', 'origin2']

    cors_config = {
        'CORSRules': [
            {'AllowedMethods': allowed_methods,
             'AllowedOrigins': allowed_origins,
             },
        ]
    }

    e = assert_raises(ClientError, client.get_bucket_cors, Bucket=bucket_name)
    status = _get_status(e.response)
    eq(status, 404)

    client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration=cors_config)
    response = client.get_bucket_cors(Bucket=bucket_name)
    eq(response['CORSRules'][0]['AllowedMethods'], allowed_methods)
    eq(response['CORSRules'][0]['AllowedOrigins'], allowed_origins)

    client.delete_bucket_cors(Bucket=bucket_name)
    e = assert_raises(ClientError, client.get_bucket_cors, Bucket=bucket_name)
    status = _get_status(e.response)
    eq(status, 404)


@attr(resource='bucket')
@attr(method='get')
@attr(operation='check cors response when origin header set')
@attr(assertion='returning cors header')
@attr('cors')
def test_cors_origin_response():
    bucket_name = _setup_bucket_acl(bucket_acl='public-read')
    client = get_client()

    cors_config = {
        'CORSRules': [
            {'AllowedMethods': ['PUT', 'DELETE'],
             'AllowedOrigins': ['http://www.example1.com'],
             'AllowedHeaders': ['*'],
             },
            {'AllowedMethods': ['GET'],
             'AllowedOrigins': ['*'],
             'AllowedHeaders': ['*']
             },
            {'AllowedMethods': ['DELETE'],
             'AllowedOrigins': ['http://www.example2.com', 'http://www.example3.com'],
             'AllowedHeaders': ['*'],
             },
        ]
    }

    e = assert_raises(ClientError, client.get_bucket_cors, Bucket=bucket_name)
    status = _get_status(e.response)
    eq(status, 404)

    client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration=cors_config)

    time.sleep(3)

    url = _get_post_url(bucket_name)

    obj_url = '{u}/{o}'.format(u=url, o='bar')

    response_origin_header = 'Access-Control-Allow-Origin'
    response_methods_header = 'Access-Control-Allow-Methods'
    no_origin_header = {response_origin_header: None}

    _cors_request_and_check(method='GET', url=url, headers={},
                            expected_status=200, expected_headers=no_origin_header)

    _cors_request_and_check(method='GET', url=url, headers={'Origin': 'http://www.example1.com'},
                            expected_status=200, expected_headers={response_origin_header: '*'})
    _cors_request_and_check(method='GET', url=obj_url, headers={'Origin': 'http://www.example1.com'},
                            expected_status=404, expected_headers={response_origin_header: '*'})
    _cors_request_and_check(method='PUT', url=obj_url, headers={'Origin': 'http://www.example1.com'},
                            expected_status=200,
                            expected_headers={response_origin_header: 'http://www.example1.com'})
    _cors_request_and_check(method='DELETE', url=obj_url, headers={'Origin': 'http://www.example1.com'},
                            expected_status=204,
                            expected_headers={response_origin_header: 'http://www.example1.com'})

    _cors_request_and_check(method='GET', url=url, headers={'Origin': 'http://www.example3.com'},
                            expected_status=200, expected_headers={response_origin_header: '*'})
    _cors_request_and_check(method='GET', url=obj_url, headers={'Origin': 'http://www.example3.com'},
                            expected_status=404, expected_headers={response_origin_header: '*'})
    _cors_request_and_check(method='PUT', url=obj_url, headers={'Origin': 'http://www.example3.com'},
                            expected_status=200, expected_headers=no_origin_header)
    _cors_request_and_check(method='DELETE', url=obj_url, headers={'Origin': 'http://www.example3.com'},
                            expected_status=204,
                            expected_headers={response_origin_header: 'http://www.example3.com'})

    _cors_request_and_check(method='GET', url=url, headers={'Origin': 'http://not.exists'},
                            expected_status=200, expected_headers={response_origin_header: '*'})
    _cors_request_and_check(method='GET', url=obj_url, headers={'Origin': 'http://not.exists'},
                            expected_status=404, expected_headers={response_origin_header: '*'})
    _cors_request_and_check(method='PUT', url=obj_url, headers={'Origin': 'http://not.exists'},
                            expected_status=200, expected_headers=no_origin_header)
    _cors_request_and_check(method='DELETE', url=obj_url, headers={'Origin': 'http://not.exists'},
                            expected_status=204, expected_headers=no_origin_header)

    _cors_request_and_check(method='OPTIONS', url=url,
                            headers={'Origin': 'http://www.example1.com',
                                     'Access-Control-Request-Method': 'GET'},
                            expected_status=200,
                            expected_headers={response_origin_header: '*', response_methods_header: 'GET'})
    _cors_request_and_check(method='OPTIONS', url=url,
                            headers={'Origin': 'http://www.example1.com',
                                     'Access-Control-Request-Method': 'DELETE'},
                            expected_status=200,
                            expected_headers={response_origin_header: 'http://www.example1.com',
                                              response_methods_header: 'PUT, DELETE'})
    _cors_request_and_check(method='OPTIONS', url=url,
                            headers={'Origin': 'http://www.example2.com',
                                     'Access-Control-Request-Method': 'DELETE'},
                            expected_status=200,
                            expected_headers={response_origin_header: 'http://www.example2.com',
                                              response_methods_header: 'DELETE'})
    _cors_request_and_check(method='OPTIONS', url=url,
                            headers={'Origin': 'http://www.example2.com',
                                     'Access-Control-Request-Method': 'PUT'},
                            expected_status=403, expected_headers=no_origin_header)

    _cors_request_and_check(method='OPTIONS', url=url,
                            headers={'Origin': 'http://not.exists', 'Access-Control-Request-Method': 'GET'},
                            expected_status=200, expected_headers={response_origin_header: '*',
                                                                   response_methods_header: 'GET'})
    _cors_request_and_check(method='OPTIONS', url=url,
                            headers={'Origin': 'http://not.exists', 'Access-Control-Request-Method': 'PUT'},
                            expected_status=403, expected_headers=no_origin_header)

    # with_credentials
    _cors_request_and_check(method='GET', url=url, headers={'Origin': 'http://www.example1.com'},
                            expected_status=None,
                            expected_headers={response_origin_header: 'http://www.example1.com'},
                            with_creds=True)
    _cors_request_and_check(method='PUT', url=obj_url, headers={'Origin': 'http://www.example1.com'},
                            expected_status=None,
                            expected_headers={response_origin_header: 'http://www.example1.com'},
                            with_creds=True)
    _cors_request_and_check(method='GET', url=url, headers={'Origin': 'http://not.exists'},
                            expected_status=None,
                            expected_headers={response_origin_header: 'http://not.exists'},
                            with_creds=True)


@attr(resource='bucket')
@attr(method='get')
@attr(operation='check cors response when origin is set to wildcard')
@attr(assertion='returning cors header')
@attr('cors')
def test_cors_origin_wildcard():
    bucket_name = _setup_bucket_acl(bucket_acl='public-read')
    client = get_client()

    cors_config = {
        'CORSRules': [
            {'AllowedMethods': ['GET'],
             'AllowedOrigins': ['*'],
             },
        ]
    }

    e = assert_raises(ClientError, client.get_bucket_cors, Bucket=bucket_name)
    status = _get_status(e.response)
    eq(status, 404)

    client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration=cors_config)

    time.sleep(3)

    url = _get_post_url(bucket_name)

    response_origin_header = 'Access-Control-Allow-Origin'
    no_origin_header = {response_origin_header: None}

    _cors_request_and_check(method='GET', url=url, headers={},
                            expected_status=200, expected_headers=no_origin_header)
    _cors_request_and_check(method='GET', url=url, headers={'Origin': 'http://www.example1.com'},
                            expected_status=200, expected_headers={response_origin_header: '*'})


@attr(resource='bucket')
@attr(method='get')
@attr(operation='check cors response when Access-Control-Request-Headers is set in option request')
@attr(assertion='returning cors header')
@attr('cors')
def test_cors_header_option():
    bucket_name = _setup_bucket_acl(bucket_acl='public-read')
    client = get_client()

    cors_config = {
        'CORSRules': [
            {'AllowedMethods': ['GET'],
             'AllowedOrigins': ['*'],
             'AllowedHeaders': ['x-amz-meta-header1'],
             },
            {'AllowedMethods': ['PUT'],
             'AllowedOrigins': ['http://www.example.com'],
             'AllowedHeaders': ['x-amz-meta-header2', 'x-amz-meta-header3'],
             },
            {'AllowedMethods': ['DELETE'],
             'AllowedOrigins': ['http://www.example1.com'],
             'AllowedHeaders': [],
             },
            {'AllowedMethods': ['POST'],
             'AllowedOrigins': ['http://www.example2.com'],
             'AllowedHeaders': ['*'],
             },
        ]
    }

    e = assert_raises(ClientError, client.get_bucket_cors, Bucket=bucket_name)
    status = _get_status(e.response)
    eq(status, 404)

    client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration=cors_config)

    time.sleep(3)

    url = _get_post_url(bucket_name)

    response_origin_header = 'Access-Control-Allow-Origin'
    response_methods_header = 'Access-Control-Allow-Methods'
    response_headers_header = 'Access-Control-Allow-Headers'
    no_origin_header = {response_origin_header: None}

    _cors_request_and_check(method='OPTIONS', url=url,
                            headers={'Origin': 'http://any.origin',
                                     'Access-Control-Request-Method': 'GET',
                                     'Access-Control-Request-Headers': 'x-amz-meta-header1'},
                            expected_status=200,
                            expected_headers={response_origin_header: '*',
                                              response_methods_header: 'GET',
                                              response_headers_header: 'x-amz-meta-header1'
                                              })
    _cors_request_and_check(method='OPTIONS', url=url,
                            headers={'Origin': 'http://www.example.com',
                                     'Access-Control-Request-Method': 'GET',
                                     'Access-Control-Request-Headers': 'x-amz-meta-header1'},
                            expected_status=200,
                            expected_headers={response_origin_header: '*',
                                              response_methods_header: 'GET',
                                              response_headers_header: 'x-amz-meta-header1'
                                              })
    _cors_request_and_check(method='OPTIONS', url=url,
                            headers={'Origin': 'http://www.example.com',
                                     'Access-Control-Request-Method': 'PUT',
                                     'Access-Control-Request-Headers': 'x-amz-meta-header1'},
                            expected_status=403,
                            expected_headers=no_origin_header)
    _cors_request_and_check(method='OPTIONS', url=url,
                            headers={'Origin': 'http://www.example.com',
                                     'Access-Control-Request-Method': 'PUT',
                                     'Access-Control-Request-Headers': 'x-amz-meta-header2'},
                            expected_status=200,
                            expected_headers={response_origin_header: 'http://www.example.com',
                                              response_methods_header: 'PUT',
                                              response_headers_header: 'x-amz-meta-header2'
                                              })
    _cors_request_and_check(method='OPTIONS', url=url,
                            headers={'Origin': 'http://www.example.com',
                                     'Access-Control-Request-Method': 'PUT',
                                     'Access-Control-Request-Headers': 'x-amz-meta-header3'},
                            expected_status=200,
                            expected_headers={response_origin_header: 'http://www.example.com',
                                              response_methods_header: 'PUT',
                                              response_headers_header: 'x-amz-meta-header3'
                                              })
    _cors_request_and_check(method='OPTIONS', url=url,
                            headers={'Origin': 'http://www.example.com',
                                     'Access-Control-Request-Method': 'PUT',
                                     'Access-Control-Request-Headers': 'x-amz-meta-header2, x-amz-meta-header3'},
                            expected_status=200,
                            expected_headers={response_origin_header: 'http://www.example.com',
                                              response_methods_header: 'PUT',
                                              response_headers_header: 'x-amz-meta-header2, x-amz-meta-header3'
                                              })
    _cors_request_and_check(method='OPTIONS', url=url,
                            headers={'Origin': 'http://www.example.com',
                                     'Access-Control-Request-Method': 'PUT'},
                            expected_status=200,
                            expected_headers={response_origin_header: 'http://www.example.com',
                                              response_methods_header: 'PUT',
                                              response_headers_header: None
                                              })
    _cors_request_and_check(method='OPTIONS', url=url,
                            headers={'Origin': 'http://www.example1.com',
                                     'Access-Control-Request-Method': 'DELETE',
                                     'Access-Control-Request-Headers': 'x-amz-meta-header'},
                            expected_status=403,
                            expected_headers=no_origin_header)
    _cors_request_and_check(method='OPTIONS', url=url,
                            headers={'Origin': 'http://www.example1.com',
                                     'Access-Control-Request-Method': 'DELETE'},
                            expected_status=200,
                            expected_headers={response_origin_header: 'http://www.example1.com',
                                              response_methods_header: 'DELETE',
                                              response_headers_header: None
                                              })
    _cors_request_and_check(method='OPTIONS', url=url,
                            headers={'Origin': 'http://www.example2.com',
                                     'Access-Control-Request-Method': 'POST',
                                     'Access-Control-Request-Headers': 'x-amz-meta-header'
                                     },
                            expected_status=200,
                            expected_headers={response_origin_header: 'http://www.example2.com',
                                              response_methods_header: 'POST',
                                              response_headers_header: 'x-amz-meta-header'
                                              })
    _cors_request_and_check(method='OPTIONS', url=url,
                            headers={'Origin': 'http://www.example2.com',
                                     'Access-Control-Request-Method': 'POST'
                                     },
                            expected_status=200,
                            expected_headers={response_origin_header: 'http://www.example2.com',
                                              response_methods_header: 'POST',
                                              response_headers_header: None
                                              })
