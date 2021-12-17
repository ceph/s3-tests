import boto3
from botocore.exceptions import ClientError
import json
import os
import time

import boto3
from nose.tools import eq_ as eq
from nose.plugins.attrib import attr
import nose
from botocore.exceptions import ClientError
from email.utils import formatdate

from .utils import assert_raises
from .utils import _get_status_and_error_code
from .utils import _get_status

from . import (
    get_client,
    get_v2_client,
    get_new_bucket,
    get_new_bucket_name,
    )

def _add_header_create_object(headers, client=None):
    """ Create a new bucket, add an object w/header customizations
    """
    bucket_name = get_new_bucket()
    if client == None:
        client = get_client()
    key_name = 'foo'
    
    # pass in custom headers before PutObject call
    add_headers = (lambda **kwargs: kwargs['params']['headers'].update(headers))
    #add_headers = (lambda **kwargs: print(kwargs['params']['headers']))
    client.meta.events.register('before-call.s3.PutObject', add_headers)
    client.put_object(Bucket=bucket_name, Key=key_name)

    return bucket_name, key_name


def _add_header_create_bad_object(headers, client=None):
    """ Create a new bucket, add an object with a header. This should cause a failure 
    """
    bucket_name = get_new_bucket()
    if client == None:
        client = get_client()
    key_name = 'foo'

    # pass in custom headers before PutObject call
    add_headers = (lambda **kwargs: kwargs['params']['headers'].update(headers))
    client.meta.events.register('before-call.s3.PutObject', add_headers)
    e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key=key_name, Body='bar')

    return e


def _remove_header_create_object(remove, client=None):
    """ Create a new bucket, add an object without a header
    """
    bucket_name = get_new_bucket()
    if client == None:
        client = get_client()
    key_name = 'foo'

    # remove custom headers before PutObject call
    def remove_header(**kwargs):
        if (remove in kwargs['params']['headers']):
            del kwargs['params']['headers'][remove]

    client.meta.events.register('before-call.s3.PutObject', remove_header)
    client.put_object(Bucket=bucket_name, Key=key_name)

    return bucket_name, key_name

def _remove_header_create_bad_object(remove, client=None):
    """ Create a new bucket, add an object without a header. This should cause a failure
    """
    bucket_name = get_new_bucket()
    if client == None:
        client = get_client()
    key_name = 'foo'

    # remove custom headers before PutObject call
    def remove_header(**kwargs):
        if (remove in kwargs['params']['headers']):
            del kwargs['params']['headers'][remove]

    client.meta.events.register('before-call.s3.PutObject', remove_header)
    e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key=key_name, Body='bar')

    return e


def _add_header_create_bucket(headers, client=None):
    """ Create a new bucket, w/header customizations
    """
    bucket_name = get_new_bucket_name()
    if client == None:
        client = get_client()

    # pass in custom headers before PutObject call
    add_headers = (lambda **kwargs: kwargs['params']['headers'].update(headers))
    client.meta.events.register('before-call.s3.CreateBucket', add_headers)
    client.create_bucket(Bucket=bucket_name)

    return bucket_name


def _add_header_create_bad_bucket(headers=None, client=None):
    """ Create a new bucket, w/header customizations that should cause a failure 
    """
    bucket_name = get_new_bucket_name()
    if client == None:
        client = get_client()

    # pass in custom headers before PutObject call
    add_headers = (lambda **kwargs: kwargs['params']['headers'].update(headers))
    client.meta.events.register('before-call.s3.CreateBucket', add_headers)
    e = assert_raises(ClientError, client.create_bucket, Bucket=bucket_name)

    return e


def _remove_header_create_bucket(remove, client=None):
    """ Create a new bucket, without a header
    """
    bucket_name = get_new_bucket_name()
    if client == None:
        client = get_client()

    # remove custom headers before PutObject call
    def remove_header(**kwargs):        
        if (remove in kwargs['params']['headers']):
            del kwargs['params']['headers'][remove]

    client.meta.events.register('before-call.s3.CreateBucket', remove_header)
    client.create_bucket(Bucket=bucket_name)

    return bucket_name

def _remove_header_create_bad_bucket(remove, client=None):
    """ Create a new bucket, without a header. This should cause a failure
    """
    bucket_name = get_new_bucket_name()
    if client == None:
        client = get_client()

    # remove custom headers before PutObject call
    def remove_header(**kwargs):
        if (remove in kwargs['params']['headers']):
            del kwargs['params']['headers'][remove]

    client.meta.events.register('before-call.s3.CreateBucket', remove_header)
    e = assert_raises(ClientError, client.create_bucket, Bucket=bucket_name)

    return e

def tag(*tags):
    def wrap(func):
        for tag in tags:
            setattr(func, tag, True)
        return func
    return wrap

# put-replication-policy test below #####################
def is_data_equal(src_bucket, dest_bucket):
    s3 = boto3.resource('s3')

    src_bucket = s3.Bucket(src_bucket)
    dest_bucket = s3.Bucket(dest_bucket)

    src_key=[]
    src_body=[]
    for obj in src_bucket.objects.all():
        src_key.append(obj.key)
        src_body.append(obj.get()['Body'].read())

    time.sleep(200)

    dest_key=[]
    dest_body=[]
    for obj in dest_bucket.objects.all():
        dest_key.append(obj.key)
        dest_body.append(obj.get()['Body'].read())
    
    print((dest_key == src_key) and (dest_body == src_body))
    if dest_key != src_key:
        print("Replication does not copy the key value as expected: src_key=" + str(src_key) + " and dest_key="+ str(dest_key))
    elif dest_body != src_body:
        print("Replication does not copy the object body as expected: src_body=" + str(src_body) + " and dest_body="+ str(dest_body))
    return ((dest_key == src_key) and (dest_body == src_body))


def add_data(file_name, bucket_name):
    s3_client = boto3.client('s3')
    s3_client.upload_file(file_name, bucket_name, "Tax/test")


def create_iam_role(role_name):
    json_data=json.loads('{"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": {"Service": "s3.amazonaws.com"}, "Action": "sts:AssumeRole"}]}')
    role_name=role_name

    session = boto3.session.Session(profile_name='default')
    iam = session.client('iam')
    e = iam.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=json.dumps(json_data),
    )

    response = e['ResponseMetadata']['HTTPStatusCode']
    print(response)
    eq(response, 200)
    role_name = e['Role']['RoleName']
    eq(role_name, 'role-test')

def create_replication_policy(role_name, policy_name, src_bucket, dest_bucket):

    role_permissions_policy=json.loads('{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObjectVersionForReplication","s3:GetObjectVersionAcl","s3:GetObjectVersionTagging"],"Resource":["arn:aws:s3:::'+src_bucket+'/*"]},{"Effect":"Allow","Action":["s3:ListBucket","s3:GetReplicationConfiguration"],"Resource":["arn:aws:s3:::'+src_bucket+'"]},{"Effect":"Allow","Action":["s3:ReplicateObject","s3:ReplicateDelete","s3:ReplicateTags"],"Resource":"arn:aws:s3:::'+dest_bucket+'/*"}]}')

    client = boto3.client('iam')
    response = client.put_role_policy(
        PolicyDocument=json.dumps(role_permissions_policy),
        PolicyName=policy_name,
        RoleName=role_name,
    )
    response=client.get_role(RoleName=role_name)
    arn = response['Role']['Arn']
    replication_config=json.loads('{"Role": "'+arn+'","Rules": [{"Status": "Enabled","Priority": 1,"DeleteMarkerReplication": { "Status": "Disabled" },"Filter" : { "Prefix": "Tax"},"Destination": {"Bucket": "arn:aws:s3:::'+dest_bucket+'"}}]}')
    client = boto3.client('s3')
    response = client.put_bucket_replication(Bucket=src_bucket, ReplicationConfiguration=replication_config)
    
    eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

def enable_versioning(bucket_name):
    s3 = boto3.resource('s3')
    versioning = s3.BucketVersioning(bucket_name)
    versioning.enable()

def cleanup_policy(role_name, policy_name):
    client = boto3.client('iam')
    response = client.delete_role_policy(
        RoleName=role_name,
        PolicyName=policy_name
    )
    response = client.delete_role(
        RoleName=role_name
    )

#
# common tests
#

def create_file():
    filename="sample.txt"
    fp = open('sample.txt', 'w')
    fp.write('sample text')
    fp.close()
    return filename

def get_replication_status(src_bucket):
    client = boto3.client('s3')
    #s3 = boto3.resource('s3')
    time.sleep(100)
    response = client.head_object(Bucket=src_bucket, Key="Tax/test")
    return response

@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/invalid MD5')
@attr(assertion='fails 400')
def test_example():
    
    src_bucket = get_new_bucket()
    dest_bucket = get_new_bucket()
    
    enable_versioning(src_bucket)
    enable_versioning(dest_bucket)
    
    role_name="role-test"
    policy_name='policy-test'
    
    create_iam_role(role_name)
    create_replication_policy(role_name, policy_name, src_bucket, dest_bucket)
    file_name=create_file()
    add_data(file_name, src_bucket)
    response = get_replication_status(src_bucket) 
    status = _get_status(response)
    #print("replication response ", status)
    response = is_data_equal(src_bucket, dest_bucket)
    eq(response, True)
    eq(status, 200)
    cleanup_policy(role_name, policy_name)
    #teardown()    
################# put-replication-policy above ##########################
'''
@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/invalid MD5')
@attr(assertion='fails 400')
def test_object_create_bad_md5_invalid_short():
    e = _add_header_create_bad_object({'Content-MD5':'YWJyYWNhZGFicmE='})
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 400)
    eq(error_code, 'InvalidDigest')


@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/mismatched MD5')
@attr(assertion='fails 400')
def test_object_create_bad_md5_bad():
    e = _add_header_create_bad_object({'Content-MD5':'rL0Y20xC+Fzt72VPzMSk2A=='})
    print("tests")
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 400)
    eq(error_code, 'BadDigest')

@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/empty MD5')
@attr(assertion='fails 400')
def test_object_create_bad_md5_empty():
    e = _add_header_create_bad_object({'Content-MD5':''})
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 400)
    eq(error_code, 'InvalidDigest')

@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/no MD5 header')
@attr(assertion='succeeds')
def test_object_create_bad_md5_none():
    bucket_name, key_name = _remove_header_create_object('Content-MD5')
    client = get_client()
    client.put_object(Bucket=bucket_name, Key=key_name, Body='bar')

@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/Expect 200')
@attr(assertion='garbage, but S3 succeeds!')
def test_object_create_bad_expect_mismatch():
    bucket_name, key_name = _add_header_create_object({'Expect': 200})
    client = get_client()
    client.put_object(Bucket=bucket_name, Key=key_name, Body='bar')

@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/empty expect')
@attr(assertion='succeeds ... should it?')
def test_object_create_bad_expect_empty():
    bucket_name, key_name = _add_header_create_object({'Expect': ''})
    client = get_client()
    client.put_object(Bucket=bucket_name, Key=key_name, Body='bar')

@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/no expect')
@attr(assertion='succeeds')
def test_object_create_bad_expect_none():
    bucket_name, key_name = _remove_header_create_object('Expect')
    client = get_client()
    client.put_object(Bucket=bucket_name, Key=key_name, Body='bar')

@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/empty content length')
@attr(assertion='fails 400')
# TODO: remove 'fails_on_rgw' and once we have learned how to remove the content-length header
@attr('fails_on_rgw')
def test_object_create_bad_contentlength_empty():
    e = _add_header_create_bad_object({'Content-Length':''})
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 400)

@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/negative content length')
@attr(assertion='fails 400')
@attr('fails_on_mod_proxy_fcgi')
def test_object_create_bad_contentlength_negative():
    client = get_client()
    bucket_name = get_new_bucket()
    key_name = 'foo'
    e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key=key_name, ContentLength=-1)
    status = _get_status(e.response)
    eq(status, 400)

@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/no content length')
@attr(assertion='fails 411')
# TODO: remove 'fails_on_rgw' and once we have learned how to remove the content-length header
@attr('fails_on_rgw')
def test_object_create_bad_contentlength_none():
    remove = 'Content-Length'
    e = _remove_header_create_bad_object('Content-Length')
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 411)
    eq(error_code, 'MissingContentLength')

@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/content length too long')
@attr(assertion='fails 400')
# TODO: remove 'fails_on_rgw' and once we have learned how to remove the content-length header
@attr('fails_on_rgw')
def test_object_create_bad_contentlength_mismatch_above():
    content = 'bar'
    length = len(content) + 1

    client = get_client()
    bucket_name = get_new_bucket()
    key_name = 'foo'
    headers = {'Content-Length': str(length)}
    #add_headers = (lambda **kwargs: kwargs['params']['headers'].update(headers))
    add_headers = (lambda **kwargs: kwargs['request'].headers.add_header('Content-Length', str(length)) )
    client.meta.events.register('before-sign.s3.PutObject', add_headers)

    e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key=key_name, Body=content)
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 400)

@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/content type text/plain')
@attr(assertion='succeeds')
def test_object_create_bad_contenttype_invalid():
    bucket_name, key_name = _add_header_create_object({'Content-Type': 'text/plain'})
    client = get_client()
    client.put_object(Bucket=bucket_name, Key=key_name, Body='bar')

@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/empty content type')
@attr(assertion='succeeds')
def test_object_create_bad_contenttype_empty():
    client = get_client()
    key_name = 'foo'
    bucket_name = get_new_bucket()
    client.put_object(Bucket=bucket_name, Key=key_name, Body='bar', ContentType='')

@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/no content type')
@attr(assertion='succeeds')
def test_object_create_bad_contenttype_none():
    bucket_name = get_new_bucket()
    key_name = 'foo'
    client = get_client()
    # as long as ContentType isn't specified in put_object it isn't going into the request
    client.put_object(Bucket=bucket_name, Key=key_name, Body='bar')


@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/empty authorization')
@attr(assertion='fails 403')
# TODO: remove 'fails_on_rgw' and once we have learned how to remove the authorization header
@attr('fails_on_rgw')
def test_object_create_bad_authorization_empty():
    e = _add_header_create_bad_object({'Authorization': ''})
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 403)

@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/date and x-amz-date')
@attr(assertion='succeeds')
# TODO: remove 'fails_on_rgw' and once we have learned how to pass both the 'Date' and 'X-Amz-Date' header during signing and not 'X-Amz-Date' before
@attr('fails_on_rgw')
def test_object_create_date_and_amz_date():
    date = formatdate(usegmt=True)
    print("the date I wanna know ", date)
    bucket_name, key_name = _add_header_create_object({'X-Amz-Date': date, 'Date': date})#, 'X-Amz-Date': date})
    client = get_client()
    client.put_object(Bucket=bucket_name, Key=key_name, Body='bar')

@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/x-amz-date and no date')
@attr(assertion='succeeds')
# TODO: remove 'fails_on_rgw' and once we have learned how to pass both the 'Date' and 'X-Amz-Date' header during signing and not 'X-Amz-Date' before
@attr('fails_on_rgw')
def test_object_create_amz_date_and_no_date():
    date = formatdate(usegmt=True)
    bucket_name, key_name = _add_header_create_object({'Date': '', 'X-Amz-Date': date})
    client = get_client()
    client.put_object(Bucket=bucket_name, Key=key_name, Body='bar')

# the teardown is really messed up here. check it out
@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/no authorization')
@attr(assertion='fails 403')
# TODO: remove 'fails_on_rgw' and once we have learned how to remove the authorization header
@attr('fails_on_rgw')
def test_object_create_bad_authorization_none():
    e = _remove_header_create_bad_object('Authorization')
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 403)

@tag('auth_common')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/no content length')
@attr(assertion='succeeds')
# TODO: remove 'fails_on_rgw' and once we have learned how to remove the content-length header
@attr('fails_on_rgw')
def test_bucket_create_contentlength_none():
    remove = 'Content-Length'
    _remove_header_create_bucket(remove)

@tag('auth_common')
@attr(resource='bucket')
@attr(method='acls')
@attr(operation='set w/no content length')
@attr(assertion='succeeds')
# TODO: remove 'fails_on_rgw' and once we have learned how to remove the content-length header
@attr('fails_on_rgw')
def test_object_acl_create_contentlength_none():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

    remove = 'Content-Length'
    def remove_header(**kwargs):
        if (remove in kwargs['params']['headers']):
            del kwargs['params']['headers'][remove]

    client.meta.events.register('before-call.s3.PutObjectAcl', remove_header)
    client.put_object_acl(Bucket=bucket_name, Key='foo', ACL='public-read')

@tag('auth_common')
@attr(resource='bucket')
@attr(method='acls')
@attr(operation='set w/invalid permission')
@attr(assertion='fails 400')
def test_bucket_put_bad_canned_acl():
    bucket_name = get_new_bucket()
    client = get_client()

    headers = {'x-amz-acl': 'public-ready'}
    add_headers = (lambda **kwargs: kwargs['params']['headers'].update(headers))
    client.meta.events.register('before-call.s3.PutBucketAcl', add_headers)

    e = assert_raises(ClientError, client.put_bucket_acl, Bucket=bucket_name, ACL='public-read')
    status = _get_status(e.response)
    eq(status, 400)

@tag('auth_common')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/expect 200')
@attr(assertion='garbage, but S3 succeeds!')
def test_bucket_create_bad_expect_mismatch():
    bucket_name = get_new_bucket_name()
    client = get_client()

    headers = {'Expect': 200}
    add_headers = (lambda **kwargs: kwargs['params']['headers'].update(headers))
    client.meta.events.register('before-call.s3.CreateBucket', add_headers)
    client.create_bucket(Bucket=bucket_name)

@tag('auth_common')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/expect empty')
@attr(assertion='garbage, but S3 succeeds!')
def test_bucket_create_bad_expect_empty():
    headers = {'Expect': ''}
    _add_header_create_bucket(headers)

@tag('auth_common')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/empty content length')
@attr(assertion='fails 400')
# TODO: The request isn't even making it to the RGW past the frontend
# This test had 'fails_on_rgw' before the move to boto3
@attr('fails_on_rgw')
def test_bucket_create_bad_contentlength_empty():
    headers = {'Content-Length': ''}
    e = _add_header_create_bad_bucket(headers)
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 400)

@tag('auth_common')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/negative content length')
@attr(assertion='fails 400')
@attr('fails_on_mod_proxy_fcgi')
def test_bucket_create_bad_contentlength_negative():
    headers = {'Content-Length': '-1'}
    e = _add_header_create_bad_bucket(headers)
    status = _get_status(e.response)
    eq(status, 400)

@tag('auth_common')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/no content length')
@attr(assertion='succeeds')
# TODO: remove 'fails_on_rgw' and once we have learned how to remove the content-length header
@attr('fails_on_rgw')
def test_bucket_create_bad_contentlength_none():
    remove = 'Content-Length'
    _remove_header_create_bucket(remove)

@tag('auth_common')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/empty authorization')
@attr(assertion='fails 403')
# TODO: remove 'fails_on_rgw' and once we have learned how to manipulate the authorization header
@attr('fails_on_rgw')
def test_bucket_create_bad_authorization_empty():
    headers = {'Authorization': ''}
    e = _add_header_create_bad_bucket(headers)
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 403)
    eq(error_code, 'AccessDenied')

@tag('auth_common')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/no authorization')
@attr(assertion='fails 403')
# TODO: remove 'fails_on_rgw' and once we have learned how to manipulate the authorization header
@attr('fails_on_rgw')
def test_bucket_create_bad_authorization_none():
    e = _remove_header_create_bad_bucket('Authorization')
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 403)
    eq(error_code, 'AccessDenied')

@tag('auth_aws2')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/invalid MD5')
@attr(assertion='fails 400')
def test_object_create_bad_md5_invalid_garbage_aws2():
    v2_client = get_v2_client()
    headers = {'Content-MD5': 'AWS HAHAHA'}
    e = _add_header_create_bad_object(headers, v2_client)
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 400)
    eq(error_code, 'InvalidDigest')

@tag('auth_aws2')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/content length too short')
@attr(assertion='fails 400')
# TODO: remove 'fails_on_rgw' and once we have learned how to manipulate the Content-Length header
@attr('fails_on_rgw')
def test_object_create_bad_contentlength_mismatch_below_aws2():
    v2_client = get_v2_client()
    content = 'bar'
    length = len(content) - 1
    headers = {'Content-Length': str(length)}
    e = _add_header_create_bad_object(headers, v2_client)
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 400)
    eq(error_code, 'BadDigest')

@tag('auth_aws2')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/incorrect authorization')
@attr(assertion='fails 403')
# TODO: remove 'fails_on_rgw' and once we have learned how to manipulate the authorization header
@attr('fails_on_rgw')
def test_object_create_bad_authorization_incorrect_aws2():
    v2_client = get_v2_client()
    headers = {'Authorization': 'AWS AKIAIGR7ZNNBHC5BKSUB:FWeDfwojDSdS2Ztmpfeubhd9isU='}
    e = _add_header_create_bad_object(headers, v2_client)
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 403)
    eq(error_code, 'InvalidDigest')

@tag('auth_aws2')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/invalid authorization')
@attr(assertion='fails 400')
# TODO: remove 'fails_on_rgw' and once we have learned how to manipulate the authorization header
@attr('fails_on_rgw')
def test_object_create_bad_authorization_invalid_aws2():
    v2_client = get_v2_client()
    headers = {'Authorization': 'AWS HAHAHA'}
    e = _add_header_create_bad_object(headers, v2_client)
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 400)
    eq(error_code, 'InvalidArgument')

@tag('auth_aws2')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/empty user agent')
@attr(assertion='succeeds')
def test_object_create_bad_ua_empty_aws2():
    v2_client = get_v2_client()
    headers = {'User-Agent': ''}
    bucket_name, key_name = _add_header_create_object(headers, v2_client)
    v2_client.put_object(Bucket=bucket_name, Key=key_name, Body='bar')

@tag('auth_aws2')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/no user agent')
@attr(assertion='succeeds')
def test_object_create_bad_ua_none_aws2():
    v2_client = get_v2_client()
    remove = 'User-Agent'
    bucket_name, key_name = _remove_header_create_object(remove, v2_client)
    v2_client.put_object(Bucket=bucket_name, Key=key_name, Body='bar')

@tag('auth_aws2')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/invalid date')
@attr(assertion='fails 403')
def test_object_create_bad_date_invalid_aws2():
    v2_client = get_v2_client()
    headers = {'x-amz-date': 'Bad Date'}
    e = _add_header_create_bad_object(headers, v2_client)
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 403)
    eq(error_code, 'AccessDenied')

@tag('auth_aws2')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/empty date')
@attr(assertion='fails 403')
def test_object_create_bad_date_empty_aws2():
    v2_client = get_v2_client()
    headers = {'x-amz-date': ''}
    e = _add_header_create_bad_object(headers, v2_client)
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 403)
    eq(error_code, 'AccessDenied')

@tag('auth_aws2')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/no date')
@attr(assertion='fails 403')
# TODO: remove 'fails_on_rgw' and once we have learned how to remove the date header
@attr('fails_on_rgw')
def test_object_create_bad_date_none_aws2():
    v2_client = get_v2_client()
    remove = 'x-amz-date'
    e = _remove_header_create_bad_object(remove, v2_client)
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 403)
    eq(error_code, 'AccessDenied')

@tag('auth_aws2')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/date in past')
@attr(assertion='fails 403')
def test_object_create_bad_date_before_today_aws2():
    v2_client = get_v2_client()
    headers = {'x-amz-date': 'Tue, 07 Jul 2010 21:53:04 GMT'}
    e = _add_header_create_bad_object(headers, v2_client)
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 403)
    eq(error_code, 'RequestTimeTooSkewed')

@tag('auth_aws2')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/date before epoch')
@attr(assertion='fails 403')
def test_object_create_bad_date_before_epoch_aws2():
    v2_client = get_v2_client()
    headers = {'x-amz-date': 'Tue, 07 Jul 1950 21:53:04 GMT'}
    e = _add_header_create_bad_object(headers, v2_client)
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 403)
    eq(error_code, 'AccessDenied')

@tag('auth_aws2')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/date after 9999')
@attr(assertion='fails 403')
def test_object_create_bad_date_after_end_aws2():
    v2_client = get_v2_client()
    headers = {'x-amz-date': 'Tue, 07 Jul 9999 21:53:04 GMT'}
    e = _add_header_create_bad_object(headers, v2_client)
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 403)
    eq(error_code, 'RequestTimeTooSkewed')

@tag('auth_aws2')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/invalid authorization')
@attr(assertion='fails 400')
# TODO: remove 'fails_on_rgw' and once we have learned how to remove the date header
@attr('fails_on_rgw')
def test_bucket_create_bad_authorization_invalid_aws2():
    v2_client = get_v2_client()
    headers = {'Authorization': 'AWS HAHAHA'}
    e = _add_header_create_bad_bucket(headers, v2_client)
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 400)
    eq(error_code, 'InvalidArgument')

@tag('auth_aws2')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/empty user agent')
@attr(assertion='succeeds')
def test_bucket_create_bad_ua_empty_aws2():
    v2_client = get_v2_client()
    headers = {'User-Agent': ''}
    _add_header_create_bucket(headers, v2_client)

@tag('auth_aws2')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/no user agent')
@attr(assertion='succeeds')
def test_bucket_create_bad_ua_none_aws2():
    v2_client = get_v2_client()
    remove = 'User-Agent'
    _remove_header_create_bucket(remove, v2_client)

@tag('auth_aws2')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/invalid date')
@attr(assertion='fails 403')
def test_bucket_create_bad_date_invalid_aws2():
    v2_client = get_v2_client()
    headers = {'x-amz-date': 'Bad Date'}
    e = _add_header_create_bad_bucket(headers, v2_client)
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 403)
    eq(error_code, 'AccessDenied')

@tag('auth_aws2')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/empty date')
@attr(assertion='fails 403')
def test_bucket_create_bad_date_empty_aws2():
    v2_client = get_v2_client()
    headers = {'x-amz-date': ''}
    e = _add_header_create_bad_bucket(headers, v2_client)
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 403)
    eq(error_code, 'AccessDenied')

@tag('auth_aws2')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/no date')
@attr(assertion='fails 403')
# TODO: remove 'fails_on_rgw' and once we have learned how to remove the date header
@attr('fails_on_rgw')
def test_bucket_create_bad_date_none_aws2():
    v2_client = get_v2_client()
    remove = 'x-amz-date'
    e = _remove_header_create_bad_bucket(remove, v2_client)
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 403)
    eq(error_code, 'AccessDenied')

@tag('auth_aws2')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/date in past')
@attr(assertion='fails 403')
def test_bucket_create_bad_date_before_today_aws2():
    v2_client = get_v2_client()
    headers = {'x-amz-date': 'Tue, 07 Jul 2010 21:53:04 GMT'}
    e = _add_header_create_bad_bucket(headers, v2_client)
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 403)
    eq(error_code, 'RequestTimeTooSkewed')

@tag('auth_aws2')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/date in future')
@attr(assertion='fails 403')
def test_bucket_create_bad_date_after_today_aws2():
    v2_client = get_v2_client()
    headers = {'x-amz-date': 'Tue, 07 Jul 2030 21:53:04 GMT'}
    e = _add_header_create_bad_bucket(headers, v2_client)
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 403)
    eq(error_code, 'RequestTimeTooSkewed')

@tag('auth_aws2')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/date before epoch')
@attr(assertion='fails 403')
def test_bucket_create_bad_date_before_epoch_aws2():
    v2_client = get_v2_client()
    headers = {'x-amz-date': 'Tue, 07 Jul 1950 21:53:04 GMT'}
    e = _add_header_create_bad_bucket(headers, v2_client)
    status, error_code = _get_status_and_error_code(e.response)
    eq(status, 403)
    eq(error_code, 'AccessDenied')
'''
