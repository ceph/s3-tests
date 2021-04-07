import boto3
import botocore.session
from botocore.exceptions import ClientError
from botocore.exceptions import ParamValidationError
from nose.tools import eq_ as eq
from nose.plugins.attrib import attr
from nose.plugins.skip import SkipTest
import isodate
import email.utils
import datetime
import threading
import re
import pytz
from collections import OrderedDict
import requests
import json
import base64
import hmac
import hashlib
import xml.etree.ElementTree as ET
import time
import operator
import nose
import os
import string
import random
import socket
import ssl
import logging
from collections import namedtuple

from email.header import decode_header

from . import(
    get_iam_client,
    get_sts_client,
    get_client,
    get_alt_user_id,
    get_config_endpoint,
    get_new_bucket_name,
    get_parameter_name,
    get_main_aws_access_key,
    get_main_aws_secret_key,
    get_thumbprint,
    get_aud,
    get_token,
    get_realm_name,
    check_webidentity
    )

log = logging.getLogger(__name__)

def create_role(iam_client,path,rolename,policy_document,description,sessionduration,permissionboundary):
    role_err=None
    if rolename is None:
        rolename=get_parameter_name()
    try:
    	role_response = iam_client.create_role(Path=path,RoleName=rolename,AssumeRolePolicyDocument=policy_document,)
    except ClientError as e:
    	role_err = e.response['Code']
    return (role_err,role_response,rolename)

def put_role_policy(iam_client,rolename,policyname,role_policy):
    role_err=None
    if policyname is None:
        policyname=get_parameter_name() 
    try:
        role_response = iam_client.put_role_policy(RoleName=rolename,PolicyName=policyname,PolicyDocument=role_policy)
    except ClientError as e:
    	role_err = e.response['Code']
    return (role_err,role_response)

def put_user_policy(iam_client,username,policyname,policy_document):
    role_err=None
    if policyname is None:
        policyname=get_parameter_name()
    try:
        role_response = iam_client.put_user_policy(UserName=username,PolicyName=policyname,PolicyDocument=policy_document)
    except ClientError as e:
        role_err = e.response['Code']
    return (role_err,role_response)

@attr(resource='get session token')
@attr(method='get')
@attr(operation='check')
@attr(assertion='s3 ops only accessible by temporary credentials')
@attr('test_of_sts')
def test_get_session_token():
    iam_client=get_iam_client()
    sts_client=get_sts_client()
    sts_user_id=get_alt_user_id()
    default_endpoint=get_config_endpoint()
    
    user_policy = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Deny\",\"Action\":\"s3:*\",\"Resource\":[\"*\"],\"Condition\":{\"BoolIfExists\":{\"sts:authentication\":\"false\"}}},{\"Effect\":\"Allow\",\"Action\":\"sts:GetSessionToken\",\"Resource\":\"*\",\"Condition\":{\"BoolIfExists\":{\"sts:authentication\":\"false\"}}}]}"
    (resp_err,resp)=put_user_policy(iam_client,sts_user_id,None,user_policy)
    eq(resp['ResponseMetadata']['HTTPStatusCode'],200)
    
    response=sts_client.get_session_token()
    eq(response['ResponseMetadata']['HTTPStatusCode'],200)
    
    s3_client=boto3.client('s3',
                aws_access_key_id = response['Credentials']['AccessKeyId'],
		aws_secret_access_key = response['Credentials']['SecretAccessKey'],
                aws_session_token = response['Credentials']['SessionToken'],
		endpoint_url=default_endpoint,
		region_name='',
		)
    bucket_name = get_new_bucket_name()
    s3bucket = s3_client.create_bucket(Bucket=bucket_name)
    eq(s3bucket['ResponseMetadata']['HTTPStatusCode'],200)
    finish=s3_client.delete_bucket(Bucket=bucket_name)

@attr(resource='get session token')
@attr(method='get')
@attr(operation='check')
@attr(assertion='s3 ops denied by permanent credentials')
@attr('test_of_sts')
def test_get_session_token_permanent_creds_denied():
    s3bucket_error=None
    iam_client=get_iam_client()
    sts_client=get_sts_client()
    sts_user_id=get_alt_user_id()
    default_endpoint=get_config_endpoint()
    s3_main_access_key=get_main_aws_access_key()
    s3_main_secret_key=get_main_aws_secret_key()
    
    user_policy = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Deny\",\"Action\":\"s3:*\",\"Resource\":[\"*\"],\"Condition\":{\"BoolIfExists\":{\"sts:authentication\":\"false\"}}},{\"Effect\":\"Allow\",\"Action\":\"sts:GetSessionToken\",\"Resource\":\"*\",\"Condition\":{\"BoolIfExists\":{\"sts:authentication\":\"false\"}}}]}"
    (resp_err,resp)=put_user_policy(iam_client,sts_user_id,None,user_policy)
    eq(resp['ResponseMetadata']['HTTPStatusCode'],200)
    
    response=sts_client.get_session_token()
    eq(response['ResponseMetadata']['HTTPStatusCode'],200)
    
    s3_client=boto3.client('s3',
                aws_access_key_id = s3_main_access_key,
		aws_secret_access_key = s3_main_secret_key,
                aws_session_token = response['Credentials']['SessionToken'],
		endpoint_url=default_endpoint,
		region_name='',
		)
    bucket_name = get_new_bucket_name()
    try:
        s3bucket = s3_client.create_bucket(Bucket=bucket_name)
    except ClientError as e:
        s3bucket_error = e.response.get("Error", {}).get("Code")
    eq(s3bucket_error,'AccessDenied')

@attr(resource='assume role')
@attr(method='get')
@attr(operation='check')
@attr(assertion='role policy allows all s3 ops')
@attr('test_of_sts')
def test_assume_role_allow():
    iam_client=get_iam_client()    
    sts_client=get_sts_client()
    sts_user_id=get_alt_user_id()
    default_endpoint=get_config_endpoint()
    role_session_name=get_parameter_name()
    
    policy_document = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":[\"arn:aws:iam:::user/"+sts_user_id+"\"]},\"Action\":[\"sts:AssumeRole\"]}]}"
    (role_error,role_response,general_role_name)=create_role(iam_client,'/',None,policy_document,None,None,None)
    eq(role_response['Role']['Arn'],'arn:aws:iam:::role/'+general_role_name+'')
    
    role_policy = "{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Allow\",\"Action\":\"s3:*\",\"Resource\":\"arn:aws:s3:::*\"}}"
    (role_err,response)=put_role_policy(iam_client,general_role_name,None,role_policy)
    eq(response['ResponseMetadata']['HTTPStatusCode'],200)
    
    resp=sts_client.assume_role(RoleArn=role_response['Role']['Arn'],RoleSessionName=role_session_name)
    eq(resp['ResponseMetadata']['HTTPStatusCode'],200)
    
    s3_client = boto3.client('s3',
		aws_access_key_id = resp['Credentials']['AccessKeyId'],
		aws_secret_access_key = resp['Credentials']['SecretAccessKey'],
		aws_session_token = resp['Credentials']['SessionToken'],
		endpoint_url=default_endpoint,
		region_name='',
		)
    bucket_name = get_new_bucket_name()
    s3bucket = s3_client.create_bucket(Bucket=bucket_name)
    eq(s3bucket['ResponseMetadata']['HTTPStatusCode'],200)
    bkt = s3_client.delete_bucket(Bucket=bucket_name)
    eq(bkt['ResponseMetadata']['HTTPStatusCode'],204)

@attr(resource='assume role')
@attr(method='get')
@attr(operation='check')
@attr(assertion='role policy denies all s3 ops')
@attr('test_of_sts')
def test_assume_role_deny():
    s3bucket_error=None
    iam_client=get_iam_client()    
    sts_client=get_sts_client()
    sts_user_id=get_alt_user_id()
    default_endpoint=get_config_endpoint()
    role_session_name=get_parameter_name()
    
    policy_document = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":[\"arn:aws:iam:::user/"+sts_user_id+"\"]},\"Action\":[\"sts:AssumeRole\"]}]}"
    (role_error,role_response,general_role_name)=create_role(iam_client,'/',None,policy_document,None,None,None)
    eq(role_response['Role']['Arn'],'arn:aws:iam:::role/'+general_role_name+'')
    
    role_policy = "{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Deny\",\"Action\":\"s3:*\",\"Resource\":\"arn:aws:s3:::*\"}}"
    (role_err,response)=put_role_policy(iam_client,general_role_name,None,role_policy)
    eq(response['ResponseMetadata']['HTTPStatusCode'],200)
    
    resp=sts_client.assume_role(RoleArn=role_response['Role']['Arn'],RoleSessionName=role_session_name)
    eq(resp['ResponseMetadata']['HTTPStatusCode'],200)
    
    s3_client = boto3.client('s3',
		aws_access_key_id = resp['Credentials']['AccessKeyId'],
		aws_secret_access_key = resp['Credentials']['SecretAccessKey'],
		aws_session_token = resp['Credentials']['SessionToken'],
		endpoint_url=default_endpoint,
		region_name='',
		)
    bucket_name = get_new_bucket_name()
    try:
        s3bucket = s3_client.create_bucket(Bucket=bucket_name)
    except ClientError as e:
        s3bucket_error = e.response.get("Error", {}).get("Code")
    eq(s3bucket_error,'AccessDenied')

@attr(resource='assume role')
@attr(method='get')
@attr(operation='check')
@attr(assertion='creds expire so all s3 ops fails')
@attr('test_of_sts')
def test_assume_role_creds_expiry():
    iam_client=get_iam_client()    
    sts_client=get_sts_client()
    sts_user_id=get_alt_user_id()
    default_endpoint=get_config_endpoint()
    role_session_name=get_parameter_name()
    
    policy_document = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":[\"arn:aws:iam:::user/"+sts_user_id+"\"]},\"Action\":[\"sts:AssumeRole\"]}]}"
    (role_error,role_response,general_role_name)=create_role(iam_client,'/',None,policy_document,None,None,None)
    eq(role_response['Role']['Arn'],'arn:aws:iam:::role/'+general_role_name+'')
    
    role_policy = "{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Allow\",\"Action\":\"s3:*\",\"Resource\":\"arn:aws:s3:::*\"}}"
    (role_err,response)=put_role_policy(iam_client,general_role_name,None,role_policy)
    eq(response['ResponseMetadata']['HTTPStatusCode'],200)
    
    resp=sts_client.assume_role(RoleArn=role_response['Role']['Arn'],RoleSessionName=role_session_name,DurationSeconds=900)
    eq(resp['ResponseMetadata']['HTTPStatusCode'],200)
    time.sleep(900)
    
    s3_client = boto3.client('s3',
		aws_access_key_id = resp['Credentials']['AccessKeyId'],
		aws_secret_access_key = resp['Credentials']['SecretAccessKey'],
		aws_session_token = resp['Credentials']['SessionToken'],
		endpoint_url=default_endpoint,
		region_name='',
		)
    bucket_name = get_new_bucket_name()
    try:
        s3bucket = s3_client.create_bucket(Bucket=bucket_name)
    except ClientError as e:
        s3bucket_error = e.response.get("Error", {}).get("Code")
    eq(s3bucket_error,'AccessDenied')

@attr(resource='assume role')
@attr(method='head')
@attr(operation='check')
@attr(assertion='HEAD fails with 403 when role policy denies s3:ListBucket')
@attr('test_of_sts')
def test_assume_role_deny_head_nonexistent():
    # create a bucket with the normal s3 client
    bucket_name = get_new_bucket_name()
    get_client().create_bucket(Bucket=bucket_name)

    iam_client=get_iam_client()
    sts_client=get_sts_client()
    sts_user_id=get_alt_user_id()
    default_endpoint=get_config_endpoint()
    role_session_name=get_parameter_name()

    policy_document = '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":["arn:aws:iam:::user/'+sts_user_id+'"]},"Action":["sts:AssumeRole"]}]}'
    (role_error,role_response,general_role_name)=create_role(iam_client,'/',None,policy_document,None,None,None)
    eq(role_response['Role']['Arn'],'arn:aws:iam:::role/'+general_role_name)

    # allow GetObject but deny ListBucket
    role_policy = '{"Version":"2012-10-17","Statement":{"Effect":"Allow","Action":"s3:GetObject","Principal":"*","Resource":"arn:aws:s3:::*"}}'
    (role_err,response)=put_role_policy(iam_client,general_role_name,None,role_policy)
    eq(response['ResponseMetadata']['HTTPStatusCode'],200)

    resp=sts_client.assume_role(RoleArn=role_response['Role']['Arn'],RoleSessionName=role_session_name)
    eq(resp['ResponseMetadata']['HTTPStatusCode'],200)

    s3_client = boto3.client('s3',
		aws_access_key_id = resp['Credentials']['AccessKeyId'],
		aws_secret_access_key = resp['Credentials']['SecretAccessKey'],
		aws_session_token = resp['Credentials']['SessionToken'],
		endpoint_url=default_endpoint,
		region_name='')
    status=200
    try:
        s3_client.head_object(Bucket=bucket_name, Key='nonexistent')
    except ClientError as e:
        status = e.response['ResponseMetadata']['HTTPStatusCode']
    eq(status,403)

@attr(resource='assume role')
@attr(method='head')
@attr(operation='check')
@attr(assertion='HEAD fails with 404 when role policy allows s3:ListBucket')
@attr('test_of_sts')
def test_assume_role_allow_head_nonexistent():
    # create a bucket with the normal s3 client
    bucket_name = get_new_bucket_name()
    get_client().create_bucket(Bucket=bucket_name)

    iam_client=get_iam_client()
    sts_client=get_sts_client()
    sts_user_id=get_alt_user_id()
    default_endpoint=get_config_endpoint()
    role_session_name=get_parameter_name()

    policy_document = '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":["arn:aws:iam:::user/'+sts_user_id+'"]},"Action":["sts:AssumeRole"]}]}'
    (role_error,role_response,general_role_name)=create_role(iam_client,'/',None,policy_document,None,None,None)
    eq(role_response['Role']['Arn'],'arn:aws:iam:::role/'+general_role_name)

    # allow GetObject and ListBucket
    role_policy = '{"Version":"2012-10-17","Statement":{"Effect":"Allow","Action":["s3:GetObject","s3:ListBucket"],"Principal":"*","Resource":"arn:aws:s3:::*"}}'
    (role_err,response)=put_role_policy(iam_client,general_role_name,None,role_policy)
    eq(response['ResponseMetadata']['HTTPStatusCode'],200)

    resp=sts_client.assume_role(RoleArn=role_response['Role']['Arn'],RoleSessionName=role_session_name)
    eq(resp['ResponseMetadata']['HTTPStatusCode'],200)

    s3_client = boto3.client('s3',
		aws_access_key_id = resp['Credentials']['AccessKeyId'],
		aws_secret_access_key = resp['Credentials']['SecretAccessKey'],
		aws_session_token = resp['Credentials']['SessionToken'],
		endpoint_url=default_endpoint,
		region_name='')
    status=200
    try:
        s3_client.head_object(Bucket=bucket_name, Key='nonexistent')
    except ClientError as e:
        status = e.response['ResponseMetadata']['HTTPStatusCode']
    eq(status,404)


@attr(resource='assume role with web identity')
@attr(method='get')
@attr(operation='check')
@attr(assertion='assuming role through web token')
@attr('webidentity_test')
def test_assume_role_with_web_identity():
    check_webidentity()
    iam_client=get_iam_client()    
    sts_client=get_sts_client()
    default_endpoint=get_config_endpoint()
    role_session_name=get_parameter_name()
    thumbprint=get_thumbprint()
    aud=get_aud()
    token=get_token()
    realm=get_realm_name()
    
    oidc_response = iam_client.create_open_id_connect_provider(
    Url='http://localhost:8080/auth/realms/{}'.format(realm),
    ThumbprintList=[
        thumbprint,
    ],
    )
    
    policy_document = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Federated\":[\""+oidc_response["OpenIDConnectProviderArn"]+"\"]},\"Action\":[\"sts:AssumeRoleWithWebIdentity\"],\"Condition\":{\"StringEquals\":{\"localhost:8080/auth/realms/"+realm+":app_id\":\""+aud+"\"}}}]}"
    (role_error,role_response,general_role_name)=create_role(iam_client,'/',None,policy_document,None,None,None)
    eq(role_response['Role']['Arn'],'arn:aws:iam:::role/'+general_role_name+'')
    
    role_policy = "{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Allow\",\"Action\":\"s3:*\",\"Resource\":\"arn:aws:s3:::*\"}}"
    (role_err,response)=put_role_policy(iam_client,general_role_name,None,role_policy)
    eq(response['ResponseMetadata']['HTTPStatusCode'],200)
    
    resp=sts_client.assume_role_with_web_identity(RoleArn=role_response['Role']['Arn'],RoleSessionName=role_session_name,WebIdentityToken=token)
    eq(resp['ResponseMetadata']['HTTPStatusCode'],200)
    
    s3_client = boto3.client('s3',
		aws_access_key_id = resp['Credentials']['AccessKeyId'],
		aws_secret_access_key = resp['Credentials']['SecretAccessKey'],
		aws_session_token = resp['Credentials']['SessionToken'],
		endpoint_url=default_endpoint,
		region_name='',
		)
    bucket_name = get_new_bucket_name()
    s3bucket = s3_client.create_bucket(Bucket=bucket_name)
    eq(s3bucket['ResponseMetadata']['HTTPStatusCode'],200)
    bkt = s3_client.delete_bucket(Bucket=bucket_name)
    eq(bkt['ResponseMetadata']['HTTPStatusCode'],204)
    
    oidc_remove=iam_client.delete_open_id_connect_provider(
    OpenIDConnectProviderArn=oidc_response["OpenIDConnectProviderArn"]
    )

'''
@attr(resource='assume role with web identity')
@attr(method='get')
@attr(operation='check')
@attr(assertion='assume_role_with_web_token creds expire')
@attr('webidentity_test')
def test_assume_role_with_web_identity_invalid_webtoken():
    resp_error=None
    iam_client=get_iam_client()
    sts_client=get_sts_client()
    default_endpoint=get_config_endpoint()
    role_session_name=get_parameter_name()
    thumbprint=get_thumbprint()
    aud=get_aud()
    token=get_token()
    realm=get_realm_name()

    oidc_response = iam_client.create_open_id_connect_provider(
    Url='http://localhost:8080/auth/realms/{}'.format(realm),
    ThumbprintList=[
        thumbprint,
    ],
    )

    policy_document = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Federated\":[\""+oidc_response["OpenIDConnectProviderArn"]+"\"]},\"Action\":[\"sts:AssumeRoleWithWebIdentity\"],\"Condition\":{\"StringEquals\":{\"localhost:8080/auth/realms/"+realm+":app_id\":\""+aud+"\"}}}]}"
    (role_error,role_response,general_role_name)=create_role(iam_client,'/',None,policy_document,None,None,None)
    eq(role_response['Role']['Arn'],'arn:aws:iam:::role/'+general_role_name+'')

    role_policy = "{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Allow\",\"Action\":\"s3:*\",\"Resource\":\"arn:aws:s3:::*\"}}"
    (role_err,response)=put_role_policy(iam_client,general_role_name,None,role_policy)
    eq(response['ResponseMetadata']['HTTPStatusCode'],200)

    resp=""
    try:
        resp=sts_client.assume_role_with_web_identity(RoleArn=role_response['Role']['Arn'],RoleSessionName=role_session_name,WebIdentityToken='abcdef')
    except InvalidIdentityTokenException as e:
        log.debug('{}'.format(resp))
        log.debug('{}'.format(e.response.get("Error", {}).get("Code")))
        log.debug('{}'.format(e))
        resp_error = e.response.get("Error", {}).get("Code")
    eq(resp_error,'AccessDenied')

    oidc_remove=iam_client.delete_open_id_connect_provider(
    OpenIDConnectProviderArn=oidc_response["OpenIDConnectProviderArn"]
    )
'''
