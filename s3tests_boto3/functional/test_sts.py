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
    check_webidentity,
    get_iam_access_key,
    get_iam_secret_key,
    get_sub,
    get_azp,
    get_user_token
    )

log = logging.getLogger(__name__)

def create_role(iam_client,path,rolename,policy_document,description,sessionduration,permissionboundary,tag_list=None):
    role_err=None
    if rolename is None:
        rolename=get_parameter_name()
    if tag_list is None:
        tag_list = []
    try:
        role_response = iam_client.create_role(Path=path,RoleName=rolename,AssumeRolePolicyDocument=policy_document,Tags=tag_list)
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

def get_s3_client_using_iam_creds():
    iam_access_key = get_iam_access_key()
    iam_secret_key = get_iam_secret_key()
    default_endpoint = get_config_endpoint()

    s3_client_iam_creds = boto3.client('s3',
                              aws_access_key_id = iam_access_key,
                              aws_secret_access_key = iam_secret_key,
                              endpoint_url=default_endpoint,
                              region_name='',
                          )

    return s3_client_iam_creds

def create_oidc_provider(iam_client, url, clientidlist, thumbprintlist):
    oidc_arn = None
    oidc_error = None
    clientids = []
    if clientidlist is None:
        clientidlist=clientids
    try:
        oidc_response = iam_client.create_open_id_connect_provider(
            Url=url,
            ClientIDList=clientidlist,
            ThumbprintList=thumbprintlist,
        )
        oidc_arn = oidc_response['OpenIDConnectProviderArn']
        print (oidc_arn)
    except ClientError as e:
        oidc_error = e.response['Code']
        print (oidc_error)
        try:
            oidc_error = None
            print (url)
            if url.startswith('http://'):
                url = url[len('http://'):]
            elif url.startswith('https://'):
                url = url[len('https://'):]
            elif url.startswith('www.'):
                url = url[len('www.'):]
            oidc_arn = 'arn:aws:iam:::oidc-provider/{}'.format(url)
            print (url)
            print (oidc_arn)
            oidc_response = iam_client.get_open_id_connect_provider(OpenIDConnectProviderArn=oidc_arn)
        except ClientError as e:
            oidc_arn = None
    return (oidc_arn, oidc_error)

def get_s3_resource_using_iam_creds():
    iam_access_key = get_iam_access_key()
    iam_secret_key = get_iam_secret_key()
    default_endpoint = get_config_endpoint()

    s3_res_iam_creds = boto3.resource('s3',
                              aws_access_key_id = iam_access_key,
                              aws_secret_access_key = iam_secret_key,
                              endpoint_url=default_endpoint,
                              region_name='',
                          )

    return s3_res_iam_creds

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
@attr('token_claims_trust_policy_test')
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

#######################
# Session Policy Tests
#######################

@attr(resource='assume role with web identity')
@attr(method='get')
@attr(operation='check')
@attr(assertion='checking session policy working for two different buckets')
@attr('webidentity_test')
@attr('session_policy')
def test_session_policy_check_on_different_buckets():
    check_webidentity()
    iam_client=get_iam_client()
    sts_client=get_sts_client()
    default_endpoint=get_config_endpoint()
    role_session_name=get_parameter_name()
    thumbprint=get_thumbprint()
    aud=get_aud()
    token=get_token()
    realm=get_realm_name()

    url = 'http://localhost:8080/auth/realms/{}'.format(realm)
    thumbprintlist = [thumbprint]
    (oidc_arn,oidc_error) = create_oidc_provider(iam_client, url, None, thumbprintlist)
    if oidc_error is not None:
        raise RuntimeError('Unable to create/get openid connect provider {}'.format(oidc_error))

    policy_document = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Federated\":[\""+oidc_arn+"\"]},\"Action\":[\"sts:AssumeRoleWithWebIdentity\"],\"Condition\":{\"StringEquals\":{\"localhost:8080/auth/realms/"+realm+":app_id\":\""+aud+"\"}}}]}"
    (role_error,role_response,general_role_name)=create_role(iam_client,'/',None,policy_document,None,None,None)
    eq(role_response['Role']['Arn'],'arn:aws:iam:::role/'+general_role_name+'')

    role_policy_new = "{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Allow\",\"Action\":\"s3:*\",\"Resource\":[\"arn:aws:s3:::test2\",\"arn:aws:s3:::test2/*\"]}}"

    (role_err,response)=put_role_policy(iam_client,general_role_name,None,role_policy_new)
    eq(response['ResponseMetadata']['HTTPStatusCode'],200)

    session_policy = "{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Allow\",\"Action\":[\"s3:GetObject\",\"s3:PutObject\"],\"Resource\":[\"arn:aws:s3:::test1\",\"arn:aws:s3:::test1/*\"]}}"

    resp=sts_client.assume_role_with_web_identity(RoleArn=role_response['Role']['Arn'],RoleSessionName=role_session_name,WebIdentityToken=token,Policy=session_policy)
    eq(resp['ResponseMetadata']['HTTPStatusCode'],200)

    s3_client = boto3.client('s3',
                aws_access_key_id = resp['Credentials']['AccessKeyId'],
                aws_secret_access_key = resp['Credentials']['SecretAccessKey'],
                aws_session_token = resp['Credentials']['SessionToken'],
                endpoint_url=default_endpoint,
                region_name='',
                )

    bucket_name_1 = 'test1'
    try:
        s3bucket = s3_client.create_bucket(Bucket=bucket_name_1)
    except ClientError as e:
        s3bucket_error = e.response.get("Error", {}).get("Code")
    eq(s3bucket_error, 'AccessDenied')

    bucket_name_2 = 'test2'
    try:
        s3bucket = s3_client.create_bucket(Bucket=bucket_name_2)
    except ClientError as e:
        s3bucket_error = e.response.get("Error", {}).get("Code")
    eq(s3bucket_error, 'AccessDenied')

    bucket_body = 'please-write-something'
    #body.encode(encoding='utf_8')
    try:
        s3_put_obj = s3_client.put_object(Body=bucket_body, Bucket=bucket_name_1, Key="test-1.txt")
    except ClientError as e:
        s3_put_obj_error = e.response.get("Error", {}).get("Code")
    eq(s3_put_obj_error,'NoSuchBucket')

    oidc_remove=iam_client.delete_open_id_connect_provider(
    OpenIDConnectProviderArn=oidc_arn
    )


@attr(resource='assume role with web identity')
@attr(method='put')
@attr(operation='check')
@attr(assertion='checking session policy working for same bucket')
@attr('webidentity_test')
@attr('session_policy')
def test_session_policy_check_on_same_bucket():
    check_webidentity()
    iam_client=get_iam_client()
    sts_client=get_sts_client()
    default_endpoint=get_config_endpoint()
    role_session_name=get_parameter_name()
    thumbprint=get_thumbprint()
    aud=get_aud()
    token=get_token()
    realm=get_realm_name()

    url = 'http://localhost:8080/auth/realms/{}'.format(realm)
    thumbprintlist = [thumbprint]
    (oidc_arn,oidc_error) = create_oidc_provider(iam_client, url, None, thumbprintlist)
    if oidc_error is not None:
        raise RuntimeError('Unable to create/get openid connect provider {}'.format(oidc_error))

    policy_document = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Federated\":[\""+oidc_arn+"\"]},\"Action\":[\"sts:AssumeRoleWithWebIdentity\"],\"Condition\":{\"StringEquals\":{\"localhost:8080/auth/realms/"+realm+":app_id\":\""+aud+"\"}}}]}"
    (role_error,role_response,general_role_name)=create_role(iam_client,'/',None,policy_document,None,None,None)
    eq(role_response['Role']['Arn'],'arn:aws:iam:::role/'+general_role_name+'')

    role_policy_new = "{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Allow\",\"Action\":\"s3:*\",\"Resource\":[\"*\"]}}"

    (role_err,response)=put_role_policy(iam_client,general_role_name,None,role_policy_new)
    eq(response['ResponseMetadata']['HTTPStatusCode'],200)

    s3_client_iam_creds = get_s3_client_using_iam_creds()

    bucket_name_1 = 'test1'
    s3bucket = s3_client_iam_creds.create_bucket(Bucket=bucket_name_1)
    eq(s3bucket['ResponseMetadata']['HTTPStatusCode'],200)

    session_policy = "{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Allow\",\"Action\":[\"s3:GetObject\",\"s3:PutObject\"],\"Resource\":[\"arn:aws:s3:::test1\",\"arn:aws:s3:::test1/*\"]}}"

    resp=sts_client.assume_role_with_web_identity(RoleArn=role_response['Role']['Arn'],RoleSessionName=role_session_name,WebIdentityToken=token,Policy=session_policy)
    eq(resp['ResponseMetadata']['HTTPStatusCode'],200)

    s3_client = boto3.client('s3',
                aws_access_key_id = resp['Credentials']['AccessKeyId'],
                aws_secret_access_key = resp['Credentials']['SecretAccessKey'],
                aws_session_token = resp['Credentials']['SessionToken'],
                endpoint_url=default_endpoint,
                region_name='',
                )

    bucket_body = 'this is a test file'
    s3_put_obj = s3_client.put_object(Body=bucket_body, Bucket=bucket_name_1, Key="test-1.txt")
    eq(s3_put_obj['ResponseMetadata']['HTTPStatusCode'],200)

    oidc_remove=iam_client.delete_open_id_connect_provider(
    OpenIDConnectProviderArn=oidc_arn
    )


@attr(resource='assume role with web identity')
@attr(method='get')
@attr(operation='check')
@attr(assertion='checking put_obj op denial')
@attr('webidentity_test')
@attr('session_policy')
def test_session_policy_check_put_obj_denial():
    check_webidentity()
    iam_client=get_iam_client()
    iam_access_key=get_iam_access_key()
    iam_secret_key=get_iam_secret_key()
    sts_client=get_sts_client()
    default_endpoint=get_config_endpoint()
    role_session_name=get_parameter_name()
    thumbprint=get_thumbprint()
    aud=get_aud()
    token=get_token()
    realm=get_realm_name()

    url = 'http://localhost:8080/auth/realms/{}'.format(realm)
    thumbprintlist = [thumbprint]
    (oidc_arn,oidc_error) = create_oidc_provider(iam_client, url, None, thumbprintlist)
    if oidc_error is not None:
        raise RuntimeError('Unable to create/get openid connect provider {}'.format(oidc_error))

    policy_document = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Federated\":[\""+oidc_arn+"\"]},\"Action\":[\"sts:AssumeRoleWithWebIdentity\"],\"Condition\":{\"StringEquals\":{\"localhost:8080/auth/realms/"+realm+":app_id\":\""+aud+"\"}}}]}"
    (role_error,role_response,general_role_name)=create_role(iam_client,'/',None,policy_document,None,None,None)
    eq(role_response['Role']['Arn'],'arn:aws:iam:::role/'+general_role_name+'')

    role_policy_new = "{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Allow\",\"Action\":\"s3:*\",\"Resource\":[\"*\"]}}"

    (role_err,response)=put_role_policy(iam_client,general_role_name,None,role_policy_new)
    eq(response['ResponseMetadata']['HTTPStatusCode'],200)

    s3_client_iam_creds = get_s3_client_using_iam_creds()

    bucket_name_1 = 'test1'
    s3bucket = s3_client_iam_creds.create_bucket(Bucket=bucket_name_1)
    eq(s3bucket['ResponseMetadata']['HTTPStatusCode'],200)

    session_policy = "{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Allow\",\"Action\":[\"s3:GetObject\"],\"Resource\":[\"arn:aws:s3:::test1\",\"arn:aws:s3:::test1/*\"]}}"

    resp=sts_client.assume_role_with_web_identity(RoleArn=role_response['Role']['Arn'],RoleSessionName=role_session_name,WebIdentityToken=token,Policy=session_policy)
    eq(resp['ResponseMetadata']['HTTPStatusCode'],200)

    s3_client = boto3.client('s3',
                aws_access_key_id = resp['Credentials']['AccessKeyId'],
                aws_secret_access_key = resp['Credentials']['SecretAccessKey'],
                aws_session_token = resp['Credentials']['SessionToken'],
                endpoint_url=default_endpoint,
                region_name='',
                )

    bucket_body = 'this is a test file'
    try:
        s3_put_obj = s3_client.put_object(Body=bucket_body, Bucket=bucket_name_1, Key="test-1.txt")
    except ClientError as e:
        s3_put_obj_error = e.response.get("Error", {}).get("Code")
    eq(s3_put_obj_error, 'AccessDenied')

    oidc_remove=iam_client.delete_open_id_connect_provider(
    OpenIDConnectProviderArn=oidc_arn
    )


@attr(resource='assume role with web identity')
@attr(method='get')
@attr(operation='check')
@attr(assertion='checking put_obj working by swapping policies')
@attr('webidentity_test')
@attr('session_policy')
def test_swapping_role_policy_and_session_policy():
    check_webidentity()
    iam_client=get_iam_client()
    iam_access_key=get_iam_access_key()
    iam_secret_key=get_iam_secret_key()
    sts_client=get_sts_client()
    default_endpoint=get_config_endpoint()
    role_session_name=get_parameter_name()
    thumbprint=get_thumbprint()
    aud=get_aud()
    token=get_token()
    realm=get_realm_name()

    url = 'http://localhost:8080/auth/realms/{}'.format(realm)
    thumbprintlist = [thumbprint]
    (oidc_arn,oidc_error) = create_oidc_provider(iam_client, url, None, thumbprintlist)
    if oidc_error is not None:
        raise RuntimeError('Unable to create/get openid connect provider {}'.format(oidc_error))

    policy_document = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Federated\":[\""+oidc_arn+"\"]},\"Action\":[\"sts:AssumeRoleWithWebIdentity\"],\"Condition\":{\"StringEquals\":{\"localhost:8080/auth/realms/"+realm+":app_id\":\""+aud+"\"}}}]}"
    (role_error,role_response,general_role_name)=create_role(iam_client,'/',None,policy_document,None,None,None)
    eq(role_response['Role']['Arn'],'arn:aws:iam:::role/'+general_role_name+'')

    role_policy_new = "{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Allow\",\"Action\":[\"s3:GetObject\",\"s3:PutObject\"],\"Resource\":[\"arn:aws:s3:::test1\",\"arn:aws:s3:::test1/*\"]}}"

    (role_err,response)=put_role_policy(iam_client,general_role_name,None,role_policy_new)
    eq(response['ResponseMetadata']['HTTPStatusCode'],200)

    s3_client_iam_creds = get_s3_client_using_iam_creds()

    bucket_name_1 = 'test1'
    s3bucket = s3_client_iam_creds.create_bucket(Bucket=bucket_name_1)
    eq(s3bucket['ResponseMetadata']['HTTPStatusCode'],200)

    session_policy = "{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Allow\",\"Action\":\"s3:*\",\"Resource\":[\"*\"]}}"

    resp=sts_client.assume_role_with_web_identity(RoleArn=role_response['Role']['Arn'],RoleSessionName=role_session_name,WebIdentityToken=token,Policy=session_policy)
    eq(resp['ResponseMetadata']['HTTPStatusCode'],200)

    s3_client = boto3.client('s3',
                aws_access_key_id = resp['Credentials']['AccessKeyId'],
                aws_secret_access_key = resp['Credentials']['SecretAccessKey'],
                aws_session_token = resp['Credentials']['SessionToken'],
                endpoint_url=default_endpoint,
                region_name='',
                )
    bucket_body = 'this is a test file'
    s3_put_obj = s3_client.put_object(Body=bucket_body, Bucket=bucket_name_1, Key="test-1.txt")
    eq(s3_put_obj['ResponseMetadata']['HTTPStatusCode'],200)

    oidc_remove=iam_client.delete_open_id_connect_provider(
    OpenIDConnectProviderArn=oidc_arn
    )

@attr(resource='assume role with web identity')
@attr(method='put')
@attr(operation='check')
@attr(assertion='checking put_obj working by setting different permissions to role and session policy')
@attr('webidentity_test')
@attr('session_policy')
def test_session_policy_check_different_op_permissions():
    check_webidentity()
    iam_client=get_iam_client()
    iam_access_key=get_iam_access_key()
    iam_secret_key=get_iam_secret_key()
    sts_client=get_sts_client()
    default_endpoint=get_config_endpoint()
    role_session_name=get_parameter_name()
    thumbprint=get_thumbprint()
    aud=get_aud()
    token=get_token()
    realm=get_realm_name()

    url = 'http://localhost:8080/auth/realms/{}'.format(realm)
    thumbprintlist = [thumbprint]
    (oidc_arn,oidc_error) = create_oidc_provider(iam_client, url, None, thumbprintlist)
    if oidc_error is not None:
        raise RuntimeError('Unable to create/get openid connect provider {}'.format(oidc_error))

    policy_document = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Federated\":[\""+oidc_arn+"\"]},\"Action\":[\"sts:AssumeRoleWithWebIdentity\"],\"Condition\":{\"StringEquals\":{\"localhost:8080/auth/realms/"+realm+":app_id\":\""+aud+"\"}}}]}"
    (role_error,role_response,general_role_name)=create_role(iam_client,'/',None,policy_document,None,None,None)
    eq(role_response['Role']['Arn'],'arn:aws:iam:::role/'+general_role_name+'')

    role_policy_new = "{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Allow\",\"Action\":[\"s3:PutObject\"],\"Resource\":[\"arn:aws:s3:::test1\",\"arn:aws:s3:::test1/*\"]}}"

    (role_err,response)=put_role_policy(iam_client,general_role_name,None,role_policy_new)
    eq(response['ResponseMetadata']['HTTPStatusCode'],200)

    s3_client_iam_creds = get_s3_client_using_iam_creds()

    bucket_name_1 = 'test1'
    s3bucket = s3_client_iam_creds.create_bucket(Bucket=bucket_name_1)
    eq(s3bucket['ResponseMetadata']['HTTPStatusCode'],200)

    session_policy = "{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Allow\",\"Action\":[\"s3:GetObject\"],\"Resource\":[\"arn:aws:s3:::test1\",\"arn:aws:s3:::test1/*\"]}}"

    resp=sts_client.assume_role_with_web_identity(RoleArn=role_response['Role']['Arn'],RoleSessionName=role_session_name,WebIdentityToken=token,Policy=session_policy)
    eq(resp['ResponseMetadata']['HTTPStatusCode'],200)

    s3_client = boto3.client('s3',
                aws_access_key_id = resp['Credentials']['AccessKeyId'],
                aws_secret_access_key = resp['Credentials']['SecretAccessKey'],
                aws_session_token = resp['Credentials']['SessionToken'],
                endpoint_url=default_endpoint,
                region_name='',
                )

    bucket_body = 'this is a test file'
    try:
        s3_put_obj = s3_client.put_object(Body=bucket_body, Bucket=bucket_name_1, Key="test-1.txt")
    except ClientError as e:
        s3_put_obj_error = e.response.get("Error", {}).get("Code")
    eq(s3_put_obj_error, 'AccessDenied')

    oidc_remove=iam_client.delete_open_id_connect_provider(
    OpenIDConnectProviderArn=oidc_arn
    )


@attr(resource='assume role with web identity')
@attr(method='put')
@attr(operation='check')
@attr(assertion='checking op behaviour with deny effect')
@attr('webidentity_test')
@attr('session_policy')
def test_session_policy_check_with_deny_effect():
    check_webidentity()
    iam_client=get_iam_client()
    iam_access_key=get_iam_access_key()
    iam_secret_key=get_iam_secret_key()
    sts_client=get_sts_client()
    default_endpoint=get_config_endpoint()
    role_session_name=get_parameter_name()
    thumbprint=get_thumbprint()
    aud=get_aud()
    token=get_token()
    realm=get_realm_name()

    url = 'http://localhost:8080/auth/realms/{}'.format(realm)
    thumbprintlist = [thumbprint]
    (oidc_arn,oidc_error) = create_oidc_provider(iam_client, url, None, thumbprintlist)
    if oidc_error is not None:
        raise RuntimeError('Unable to create/get openid connect provider {}'.format(oidc_error))

    policy_document = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Federated\":[\""+oidc_arn+"\"]},\"Action\":[\"sts:AssumeRoleWithWebIdentity\"],\"Condition\":{\"StringEquals\":{\"localhost:8080/auth/realms/"+realm+":app_id\":\""+aud+"\"}}}]}"
    (role_error,role_response,general_role_name)=create_role(iam_client,'/',None,policy_document,None,None,None)
    eq(role_response['Role']['Arn'],'arn:aws:iam:::role/'+general_role_name+'')

    role_policy_new = "{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Deny\",\"Action\":\"s3:*\",\"Resource\":[\"*\"]}}"

    (role_err,response)=put_role_policy(iam_client,general_role_name,None,role_policy_new)
    eq(response['ResponseMetadata']['HTTPStatusCode'],200)

    s3_client_iam_creds = get_s3_client_using_iam_creds()

    bucket_name_1 = 'test1'
    s3bucket = s3_client_iam_creds.create_bucket(Bucket=bucket_name_1)
    eq(s3bucket['ResponseMetadata']['HTTPStatusCode'],200)

    session_policy = "{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Allow\",\"Action\":[\"s3:PutObject\"],\"Resource\":[\"arn:aws:s3:::test1\",\"arn:aws:s3:::test1/*\"]}}"

    resp=sts_client.assume_role_with_web_identity(RoleArn=role_response['Role']['Arn'],RoleSessionName=role_session_name,WebIdentityToken=token,Policy=session_policy)
    eq(resp['ResponseMetadata']['HTTPStatusCode'],200)

    s3_client = boto3.client('s3',
                aws_access_key_id = resp['Credentials']['AccessKeyId'],
                aws_secret_access_key = resp['Credentials']['SecretAccessKey'],
                aws_session_token = resp['Credentials']['SessionToken'],
                endpoint_url=default_endpoint,
                region_name='',
                )
    bucket_body = 'this is a test file'
    try:
        s3_put_obj = s3_client.put_object(Body=bucket_body, Bucket=bucket_name_1, Key="test-1.txt")
    except ClientError as e:
        s3_put_obj_error = e.response.get("Error", {}).get("Code")
    eq(s3_put_obj_error, 'AccessDenied')

    oidc_remove=iam_client.delete_open_id_connect_provider(
    OpenIDConnectProviderArn=oidc_arn
    )


@attr(resource='assume role with web identity')
@attr(method='put')
@attr(operation='check')
@attr(assertion='checking put_obj working with deny and allow on same op')
@attr('webidentity_test')
@attr('session_policy')
def test_session_policy_check_with_deny_on_same_op():
    check_webidentity()
    iam_client=get_iam_client()
    iam_access_key=get_iam_access_key()
    iam_secret_key=get_iam_secret_key()
    sts_client=get_sts_client()
    default_endpoint=get_config_endpoint()
    role_session_name=get_parameter_name()
    thumbprint=get_thumbprint()
    aud=get_aud()
    token=get_token()
    realm=get_realm_name()

    url = 'http://localhost:8080/auth/realms/{}'.format(realm)
    thumbprintlist = [thumbprint]
    (oidc_arn,oidc_error) = create_oidc_provider(iam_client, url, None, thumbprintlist)
    if oidc_error is not None:
        raise RuntimeError('Unable to create/get openid connect provider {}'.format(oidc_error))

    policy_document = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Federated\":[\""+oidc_arn+"\"]},\"Action\":[\"sts:AssumeRoleWithWebIdentity\"],\"Condition\":{\"StringEquals\":{\"localhost:8080/auth/realms/"+realm+":app_id\":\""+aud+"\"}}}]}"
    (role_error,role_response,general_role_name)=create_role(iam_client,'/',None,policy_document,None,None,None)
    eq(role_response['Role']['Arn'],'arn:aws:iam:::role/'+general_role_name+'')

    role_policy_new = "{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Allow\",\"Action\":[\"s3:PutObject\"],\"Resource\":[\"arn:aws:s3:::test1\",\"arn:aws:s3:::test1/*\"]}}"

    (role_err,response)=put_role_policy(iam_client,general_role_name,None,role_policy_new)
    eq(response['ResponseMetadata']['HTTPStatusCode'],200)

    s3_client_iam_creds = get_s3_client_using_iam_creds()

    bucket_name_1 = 'test1'
    s3bucket = s3_client_iam_creds.create_bucket(Bucket=bucket_name_1)
    eq(s3bucket['ResponseMetadata']['HTTPStatusCode'],200)

    session_policy = "{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Deny\",\"Action\":[\"s3:PutObject\"],\"Resource\":[\"arn:aws:s3:::test1\",\"arn:aws:s3:::test1/*\"]}}"

    resp=sts_client.assume_role_with_web_identity(RoleArn=role_response['Role']['Arn'],RoleSessionName=role_session_name,WebIdentityToken=token,Policy=session_policy)
    eq(resp['ResponseMetadata']['HTTPStatusCode'],200)

    s3_client = boto3.client('s3',
                aws_access_key_id = resp['Credentials']['AccessKeyId'],
                aws_secret_access_key = resp['Credentials']['SecretAccessKey'],
                aws_session_token = resp['Credentials']['SessionToken'],
                endpoint_url=default_endpoint,
                region_name='',
                )

    bucket_body = 'this is a test file'
    try:
        s3_put_obj = s3_client.put_object(Body=bucket_body, Bucket=bucket_name_1, Key="test-1.txt")
    except ClientError as e:
        s3_put_obj_error = e.response.get("Error", {}).get("Code")
    eq(s3_put_obj_error, 'AccessDenied')

    oidc_remove=iam_client.delete_open_id_connect_provider(
    OpenIDConnectProviderArn=oidc_arn
    )

@attr(resource='assume role with web identity')
@attr(method='put')
@attr(operation='check')
@attr(assertion='checking op when bucket policy has role arn')
@attr('webidentity_test')
@attr('session_policy')
def test_session_policy_bucket_policy_role_arn():
    check_webidentity()
    iam_client=get_iam_client()
    sts_client=get_sts_client()
    default_endpoint=get_config_endpoint()
    role_session_name=get_parameter_name()
    thumbprint=get_thumbprint()
    aud=get_aud()
    token=get_token()
    realm=get_realm_name()

    url = 'http://localhost:8080/auth/realms/{}'.format(realm)
    thumbprintlist = [thumbprint]
    (oidc_arn,oidc_error) = create_oidc_provider(iam_client, url, None, thumbprintlist)
    if oidc_error is not None:
        raise RuntimeError('Unable to create/get openid connect provider {}'.format(oidc_error))

    policy_document = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Federated\":[\""+oidc_arn+"\"]},\"Action\":[\"sts:AssumeRoleWithWebIdentity\"],\"Condition\":{\"StringEquals\":{\"localhost:8080/auth/realms/"+realm+":app_id\":\""+aud+"\"}}}]}"
    (role_error,role_response,general_role_name)=create_role(iam_client,'/',None,policy_document,None,None,None)
    eq(role_response['Role']['Arn'],'arn:aws:iam:::role/'+general_role_name+'')
    role_policy = "{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Allow\",\"Action\":\"s3:*\",\"Resource\":[\"*\"]}}"

    (role_err,response)=put_role_policy(iam_client,general_role_name,None,role_policy)
    eq(response['ResponseMetadata']['HTTPStatusCode'],200)

    s3client_iamcreds = get_s3_client_using_iam_creds()
    bucket_name_1 = 'test1'
    s3bucket = s3client_iamcreds.create_bucket(Bucket=bucket_name_1)
    eq(s3bucket['ResponseMetadata']['HTTPStatusCode'],200)

    resource1 = "arn:aws:s3:::" + bucket_name_1
    resource2 = "arn:aws:s3:::" + bucket_name_1 + "/*"
    rolearn = "arn:aws:iam:::role/" + general_role_name
    bucket_policy = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [{
        "Effect": "Allow",
        "Principal": {"AWS": "{}".format(rolearn)},
        "Action": ["s3:GetObject","s3:PutObject"],
        "Resource": [
            "{}".format(resource1),
            "{}".format(resource2)
          ]
        }]
     })
    s3client_iamcreds.put_bucket_policy(Bucket=bucket_name_1, Policy=bucket_policy)
    session_policy = "{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Allow\",\"Action\":[\"s3:PutObject\"],\"Resource\":[\"arn:aws:s3:::test1\",\"arn:aws:s3:::test1/*\"]}}"

    resp=sts_client.assume_role_with_web_identity(RoleArn=role_response['Role']['Arn'],RoleSessionName=role_session_name,WebIdentityToken=token,Policy=session_policy)
    eq(resp['ResponseMetadata']['HTTPStatusCode'],200)

    s3_client = boto3.client('s3',
                aws_access_key_id = resp['Credentials']['AccessKeyId'],
                aws_secret_access_key = resp['Credentials']['SecretAccessKey'],
                aws_session_token = resp['Credentials']['SessionToken'],
                endpoint_url=default_endpoint,
                region_name='',
                )
    bucket_body = 'this is a test file'
    s3_put_obj = s3_client.put_object(Body=bucket_body, Bucket=bucket_name_1, Key="test-1.txt")
    eq(s3_put_obj['ResponseMetadata']['HTTPStatusCode'],200)

    try:
        obj = s3_client.get_object(Bucket=bucket_name_1, Key="test-1.txt")
    except ClientError as e:
        s3object_error = e.response.get("Error", {}).get("Code")
    eq(s3object_error, 'AccessDenied')

    oidc_remove=iam_client.delete_open_id_connect_provider(
    OpenIDConnectProviderArn=oidc_arn
    )

@attr(resource='assume role with web identity')
@attr(method='get')
@attr(operation='check')
@attr(assertion='checking op when bucket policy has session arn')
@attr('webidentity_test')
@attr('session_policy')
def test_session_policy_bucket_policy_session_arn():
    check_webidentity()
    iam_client=get_iam_client()
    sts_client=get_sts_client()
    default_endpoint=get_config_endpoint()
    role_session_name=get_parameter_name()
    thumbprint=get_thumbprint()
    aud=get_aud()
    token=get_token()
    realm=get_realm_name()

    url = 'http://localhost:8080/auth/realms/{}'.format(realm)
    thumbprintlist = [thumbprint]
    (oidc_arn,oidc_error) = create_oidc_provider(iam_client, url, None, thumbprintlist)
    if oidc_error is not None:
        raise RuntimeError('Unable to create/get openid connect provider {}'.format(oidc_error))

    policy_document = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Federated\":[\""+oidc_arn+"\"]},\"Action\":[\"sts:AssumeRoleWithWebIdentity\"],\"Condition\":{\"StringEquals\":{\"localhost:8080/auth/realms/"+realm+":app_id\":\""+aud+"\"}}}]}"
    (role_error,role_response,general_role_name)=create_role(iam_client,'/',None,policy_document,None,None,None)
    eq(role_response['Role']['Arn'],'arn:aws:iam:::role/'+general_role_name+'')
    role_policy = "{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Allow\",\"Action\":\"s3:*\",\"Resource\":[\"*\"]}}"

    (role_err,response)=put_role_policy(iam_client,general_role_name,None,role_policy)
    eq(response['ResponseMetadata']['HTTPStatusCode'],200)

    s3client_iamcreds = get_s3_client_using_iam_creds()
    bucket_name_1 = 'test1'
    s3bucket = s3client_iamcreds.create_bucket(Bucket=bucket_name_1)
    eq(s3bucket['ResponseMetadata']['HTTPStatusCode'],200)

    resource1 = "arn:aws:s3:::" + bucket_name_1
    resource2 = "arn:aws:s3:::" + bucket_name_1 + "/*"
    rolesessionarn = "arn:aws:iam:::assumed-role/" + general_role_name + "/" + role_session_name
    bucket_policy = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [{
        "Effect": "Allow",
        "Principal": {"AWS": "{}".format(rolesessionarn)},
        "Action": ["s3:GetObject","s3:PutObject"],
        "Resource": [
            "{}".format(resource1),
            "{}".format(resource2)
          ]
        }]
    })
    s3client_iamcreds.put_bucket_policy(Bucket=bucket_name_1, Policy=bucket_policy)
    session_policy = "{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Allow\",\"Action\":[\"s3:PutObject\"],\"Resource\":[\"arn:aws:s3:::test1\",\"arn:aws:s3:::test1/*\"]}}"

    resp=sts_client.assume_role_with_web_identity(RoleArn=role_response['Role']['Arn'],RoleSessionName=role_session_name,WebIdentityToken=token,Policy=session_policy)
    eq(resp['ResponseMetadata']['HTTPStatusCode'],200)

    s3_client = boto3.client('s3',
                aws_access_key_id = resp['Credentials']['AccessKeyId'],
                aws_secret_access_key = resp['Credentials']['SecretAccessKey'],
                aws_session_token = resp['Credentials']['SessionToken'],
                endpoint_url=default_endpoint,
                region_name='',
                )
    bucket_body = 'this is a test file'
    s3_put_obj = s3_client.put_object(Body=bucket_body, Bucket=bucket_name_1, Key="test-1.txt")
    eq(s3_put_obj['ResponseMetadata']['HTTPStatusCode'],200)


    s3_get_obj = s3_client.get_object(Bucket=bucket_name_1, Key="test-1.txt")
    eq(s3_get_obj['ResponseMetadata']['HTTPStatusCode'],200)

    oidc_remove=iam_client.delete_open_id_connect_provider(
    OpenIDConnectProviderArn=oidc_arn
    )

@attr(resource='assume role with web identity')
@attr(method='put')
@attr(operation='check')
@attr(assertion='checking copy object op with role, session and bucket policy')
@attr('webidentity_test')
@attr('session_policy')
def test_session_policy_copy_object():
    check_webidentity()
    iam_client=get_iam_client()
    sts_client=get_sts_client()
    default_endpoint=get_config_endpoint()
    role_session_name=get_parameter_name()
    thumbprint=get_thumbprint()
    aud=get_aud()
    token=get_token()
    realm=get_realm_name()

    url = 'http://localhost:8080/auth/realms/{}'.format(realm)
    thumbprintlist = [thumbprint]
    (oidc_arn,oidc_error) = create_oidc_provider(iam_client, url, None, thumbprintlist)
    if oidc_error is not None:
        raise RuntimeError('Unable to create/get openid connect provider {}'.format(oidc_error))

    policy_document = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Federated\":[\""+oidc_arn+"\"]},\"Action\":[\"sts:AssumeRoleWithWebIdentity\"],\"Condition\":{\"StringEquals\":{\"localhost:8080/auth/realms/"+realm+":app_id\":\""+aud+"\"}}}]}"
    (role_error,role_response,general_role_name)=create_role(iam_client,'/',None,policy_document,None,None,None)
    eq(role_response['Role']['Arn'],'arn:aws:iam:::role/'+general_role_name+'')
    role_policy = "{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Allow\",\"Action\":\"s3:*\",\"Resource\":[\"*\"]}}"

    (role_err,response)=put_role_policy(iam_client,general_role_name,None,role_policy)
    eq(response['ResponseMetadata']['HTTPStatusCode'],200)

    s3client_iamcreds = get_s3_client_using_iam_creds()
    bucket_name_1 = 'test1'
    s3bucket = s3client_iamcreds.create_bucket(Bucket=bucket_name_1)
    eq(s3bucket['ResponseMetadata']['HTTPStatusCode'],200)

    resource1 = "arn:aws:s3:::" + bucket_name_1
    resource2 = "arn:aws:s3:::" + bucket_name_1 + "/*"
    rolesessionarn = "arn:aws:iam:::assumed-role/" + general_role_name + "/" + role_session_name
    print (rolesessionarn)
    bucket_policy = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [{
        "Effect": "Allow",
        "Principal": {"AWS": "{}".format(rolesessionarn)},
        "Action": ["s3:GetObject","s3:PutObject"],
        "Resource": [
            "{}".format(resource1),
            "{}".format(resource2)
          ]
        }]
     })
    s3client_iamcreds.put_bucket_policy(Bucket=bucket_name_1, Policy=bucket_policy)
    session_policy = "{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Allow\",\"Action\":[\"s3:PutObject\"],\"Resource\":[\"arn:aws:s3:::test1\",\"arn:aws:s3:::test1/*\"]}}"

    resp=sts_client.assume_role_with_web_identity(RoleArn=role_response['Role']['Arn'],RoleSessionName=role_session_name,WebIdentityToken=token,Policy=session_policy)
    eq(resp['ResponseMetadata']['HTTPStatusCode'],200)

    s3_client = boto3.client('s3',
                aws_access_key_id = resp['Credentials']['AccessKeyId'],
                aws_secret_access_key = resp['Credentials']['SecretAccessKey'],
                aws_session_token = resp['Credentials']['SessionToken'],
                endpoint_url=default_endpoint,
                region_name='',
                )
    bucket_body = 'this is a test file'
    s3_put_obj = s3_client.put_object(Body=bucket_body, Bucket=bucket_name_1, Key="test-1.txt")
    eq(s3_put_obj['ResponseMetadata']['HTTPStatusCode'],200)

    copy_source = {
    'Bucket': bucket_name_1,
    'Key': 'test-1.txt'
    }

    s3_client.copy(copy_source, bucket_name_1, "test-2.txt")

    s3_get_obj = s3_client.get_object(Bucket=bucket_name_1, Key="test-2.txt")
    eq(s3_get_obj['ResponseMetadata']['HTTPStatusCode'],200)

    oidc_remove=iam_client.delete_open_id_connect_provider(
    OpenIDConnectProviderArn=oidc_arn
    )

@attr(resource='assume role with web identity')
@attr(method='put')
@attr(operation='check')
@attr(assertion='checking op is denied when no role policy')
@attr('webidentity_test')
@attr('session_policy')
def test_session_policy_no_bucket_role_policy():
    check_webidentity()
    iam_client=get_iam_client()
    sts_client=get_sts_client()
    default_endpoint=get_config_endpoint()
    role_session_name=get_parameter_name()
    thumbprint=get_thumbprint()
    aud=get_aud()
    token=get_token()
    realm=get_realm_name()

    url = 'http://localhost:8080/auth/realms/{}'.format(realm)
    thumbprintlist = [thumbprint]
    (oidc_arn,oidc_error) = create_oidc_provider(iam_client, url, None, thumbprintlist)
    if oidc_error is not None:
        raise RuntimeError('Unable to create/get openid connect provider {}'.format(oidc_error))

    policy_document = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Federated\":[\""+oidc_arn+"\"]},\"Action\":[\"sts:AssumeRoleWithWebIdentity\"],\"Condition\":{\"StringEquals\":{\"localhost:8080/auth/realms/"+realm+":app_id\":\""+aud+"\"}}}]}"
    (role_error,role_response,general_role_name)=create_role(iam_client,'/',None,policy_document,None,None,None)
    eq(role_response['Role']['Arn'],'arn:aws:iam:::role/'+general_role_name+'')

    s3client_iamcreds = get_s3_client_using_iam_creds()
    bucket_name_1 = 'test1'
    s3bucket = s3client_iamcreds.create_bucket(Bucket=bucket_name_1)
    eq(s3bucket['ResponseMetadata']['HTTPStatusCode'],200)

    session_policy = "{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Allow\",\"Action\":[\"s3:PutObject\",\"s3:GetObject\"],\"Resource\":[\"arn:aws:s3:::test1\",\"arn:aws:s3:::test1/*\"]}}"

    resp=sts_client.assume_role_with_web_identity(RoleArn=role_response['Role']['Arn'],RoleSessionName=role_session_name,WebIdentityToken=token,Policy=session_policy)
    eq(resp['ResponseMetadata']['HTTPStatusCode'],200)

    s3_client = boto3.client('s3',
                aws_access_key_id = resp['Credentials']['AccessKeyId'],
                aws_secret_access_key = resp['Credentials']['SecretAccessKey'],
                aws_session_token = resp['Credentials']['SessionToken'],
                endpoint_url=default_endpoint,
                region_name='',
                )
    bucket_body = 'this is a test file'
    try:
        s3_put_obj = s3_client.put_object(Body=bucket_body, Bucket=bucket_name_1, Key="test-1.txt")
    except ClientError as e:
        s3putobj_error = e.response.get("Error", {}).get("Code")
    eq(s3putobj_error, 'AccessDenied')

    oidc_remove=iam_client.delete_open_id_connect_provider(
    OpenIDConnectProviderArn=oidc_arn
    )

@attr(resource='assume role with web identity')
@attr(method='put')
@attr(operation='check')
@attr(assertion='checking op is denied when resource policy denies')
@attr('webidentity_test')
@attr('session_policy')
def test_session_policy_bucket_policy_deny():
    check_webidentity()
    iam_client=get_iam_client()
    sts_client=get_sts_client()
    default_endpoint=get_config_endpoint()
    role_session_name=get_parameter_name()
    thumbprint=get_thumbprint()
    aud=get_aud()
    token=get_token()
    realm=get_realm_name()

    url = 'http://localhost:8080/auth/realms/{}'.format(realm)
    thumbprintlist = [thumbprint]
    (oidc_arn,oidc_error) = create_oidc_provider(iam_client, url, None, thumbprintlist)
    if oidc_error is not None:
        raise RuntimeError('Unable to create/get openid connect provider {}'.format(oidc_error))

    policy_document = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Federated\":[\""+oidc_arn+"\"]},\"Action\":[\"sts:AssumeRoleWithWebIdentity\"],\"Condition\":{\"StringEquals\":{\"localhost:8080/auth/realms/"+realm+":app_id\":\""+aud+"\"}}}]}"
    (role_error,role_response,general_role_name)=create_role(iam_client,'/',None,policy_document,None,None,None)
    eq(role_response['Role']['Arn'],'arn:aws:iam:::role/'+general_role_name+'')
    role_policy = "{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Allow\",\"Action\":\"s3:*\",\"Resource\":[\"*\"]}}"

    (role_err,response)=put_role_policy(iam_client,general_role_name,None,role_policy)
    eq(response['ResponseMetadata']['HTTPStatusCode'],200)

    s3client_iamcreds = get_s3_client_using_iam_creds()
    bucket_name_1 = 'test1'
    s3bucket = s3client_iamcreds.create_bucket(Bucket=bucket_name_1)
    eq(s3bucket['ResponseMetadata']['HTTPStatusCode'],200)

    resource1 = "arn:aws:s3:::" + bucket_name_1
    resource2 = "arn:aws:s3:::" + bucket_name_1 + "/*"
    rolesessionarn = "arn:aws:iam:::assumed-role/" + general_role_name + "/" + role_session_name
    bucket_policy = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [{
        "Effect": "Deny",
        "Principal": {"AWS": "{}".format(rolesessionarn)},
        "Action": ["s3:GetObject","s3:PutObject"],
        "Resource": [
            "{}".format(resource1),
            "{}".format(resource2)
          ]
        }]
    })
    s3client_iamcreds.put_bucket_policy(Bucket=bucket_name_1, Policy=bucket_policy)
    session_policy = "{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Allow\",\"Action\":[\"s3:PutObject\"],\"Resource\":[\"arn:aws:s3:::test1\",\"arn:aws:s3:::test1/*\"]}}"

    resp=sts_client.assume_role_with_web_identity(RoleArn=role_response['Role']['Arn'],RoleSessionName=role_session_name,WebIdentityToken=token,Policy=session_policy)
    eq(resp['ResponseMetadata']['HTTPStatusCode'],200)

    s3_client = boto3.client('s3',
                aws_access_key_id = resp['Credentials']['AccessKeyId'],
                aws_secret_access_key = resp['Credentials']['SecretAccessKey'],
                aws_session_token = resp['Credentials']['SessionToken'],
                endpoint_url=default_endpoint,
                region_name='',
                )
    bucket_body = 'this is a test file'

    try:
        s3_put_obj = s3_client.put_object(Body=bucket_body, Bucket=bucket_name_1, Key="test-1.txt")
    except ClientError as e:
        s3putobj_error = e.response.get("Error", {}).get("Code")
    eq(s3putobj_error, 'AccessDenied')

    oidc_remove=iam_client.delete_open_id_connect_provider(
    OpenIDConnectProviderArn=oidc_arn
    )

@attr(resource='assume role with web identity')
@attr(method='get')
@attr(operation='check')
@attr(assertion='assuming role using web token using sub in trust policy')
@attr('webidentity_test')
@attr('token_claims_trust_policy_test')
def test_assume_role_with_web_identity_with_sub():
    check_webidentity()
    iam_client=get_iam_client()
    sts_client=get_sts_client()
    default_endpoint=get_config_endpoint()
    role_session_name=get_parameter_name()
    thumbprint=get_thumbprint()
    sub=get_sub()
    token=get_token()
    realm=get_realm_name()

    oidc_response = iam_client.create_open_id_connect_provider(
    Url='http://localhost:8080/auth/realms/{}'.format(realm),
    ThumbprintList=[
        thumbprint,
    ],
    )

    policy_document = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Federated\":[\""+oidc_response["OpenIDConnectProviderArn"]+"\"]},\"Action\":[\"sts:AssumeRoleWithWebIdentity\"],\"Condition\":{\"StringEquals\":{\"localhost:8080/auth/realms/"+realm+":sub\":\""+sub+"\"}}}]}"
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

@attr(resource='assume role with web identity')
@attr(method='get')
@attr(operation='check')
@attr(assertion='assuming role using web token using azp in trust policy')
@attr('webidentity_test')
@attr('token_claims_trust_policy_test')
def test_assume_role_with_web_identity_with_azp():
    check_webidentity()
    iam_client=get_iam_client()
    sts_client=get_sts_client()
    default_endpoint=get_config_endpoint()
    role_session_name=get_parameter_name()
    thumbprint=get_thumbprint()
    azp=get_azp()
    token=get_token()
    realm=get_realm_name()

    oidc_response = iam_client.create_open_id_connect_provider(
    Url='http://localhost:8080/auth/realms/{}'.format(realm),
    ThumbprintList=[
        thumbprint,
    ],
    )

    policy_document = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Federated\":[\""+oidc_response["OpenIDConnectProviderArn"]+"\"]},\"Action\":[\"sts:AssumeRoleWithWebIdentity\"],\"Condition\":{\"StringEquals\":{\"localhost:8080/auth/realms/"+realm+":azp\":\""+azp+"\"}}}]}"
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

@attr(resource='assume role with web identity')
@attr(method='get')
@attr(operation='check')
@attr(assertion='assuming role using web token using aws:RequestTag in trust policy')
@attr('webidentity_test')
@attr('abac_test')
@attr('token_request_tag_trust_policy_test')
def test_assume_role_with_web_identity_with_request_tag():
    check_webidentity()
    iam_client=get_iam_client()
    sts_client=get_sts_client()
    default_endpoint=get_config_endpoint()
    role_session_name=get_parameter_name()
    thumbprint=get_thumbprint()
    user_token=get_user_token()
    realm=get_realm_name()

    oidc_response = iam_client.create_open_id_connect_provider(
    Url='http://localhost:8080/auth/realms/{}'.format(realm),
    ThumbprintList=[
        thumbprint,
    ],
    )

    policy_document = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Federated\":[\""+oidc_response["OpenIDConnectProviderArn"]+"\"]},\"Action\":[\"sts:AssumeRoleWithWebIdentity\",\"sts:TagSession\"],\"Condition\":{\"StringEquals\":{\"aws:RequestTag/Department\":\"Engineering\"}}}]}"
    (role_error,role_response,general_role_name)=create_role(iam_client,'/',None,policy_document,None,None,None)
    eq(role_response['Role']['Arn'],'arn:aws:iam:::role/'+general_role_name+'')

    role_policy = "{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Allow\",\"Action\":\"s3:*\",\"Resource\":\"arn:aws:s3:::*\"}}"
    (role_err,response)=put_role_policy(iam_client,general_role_name,None,role_policy)
    eq(response['ResponseMetadata']['HTTPStatusCode'],200)

    resp=sts_client.assume_role_with_web_identity(RoleArn=role_response['Role']['Arn'],RoleSessionName=role_session_name,WebIdentityToken=user_token)
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

@attr(resource='assume role with web identity')
@attr(method='get')
@attr(operation='check')
@attr(assertion='assuming role using web token with aws:PrincipalTag in role policy')
@attr('webidentity_test')
@attr('abac_test')
@attr('token_principal_tag_role_policy_test')
def test_assume_role_with_web_identity_with_principal_tag():
    check_webidentity()
    iam_client=get_iam_client()
    sts_client=get_sts_client()
    default_endpoint=get_config_endpoint()
    role_session_name=get_parameter_name()
    thumbprint=get_thumbprint()
    user_token=get_user_token()
    realm=get_realm_name()

    oidc_response = iam_client.create_open_id_connect_provider(
    Url='http://localhost:8080/auth/realms/{}'.format(realm),
    ThumbprintList=[
        thumbprint,
    ],
    )

    policy_document = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Federated\":[\""+oidc_response["OpenIDConnectProviderArn"]+"\"]},\"Action\":[\"sts:AssumeRoleWithWebIdentity\",\"sts:TagSession\"],\"Condition\":{\"StringEquals\":{\"aws:RequestTag/Department\":\"Engineering\"}}}]}"
    (role_error,role_response,general_role_name)=create_role(iam_client,'/',None,policy_document,None,None,None)
    eq(role_response['Role']['Arn'],'arn:aws:iam:::role/'+general_role_name+'')

    role_policy = "{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Allow\",\"Action\":\"s3:*\",\"Resource\":\"arn:aws:s3:::*\",\"Condition\":{\"StringEquals\":{\"aws:PrincipalTag/Department\":\"Engineering\"}}}}"
    (role_err,response)=put_role_policy(iam_client,general_role_name,None,role_policy)
    eq(response['ResponseMetadata']['HTTPStatusCode'],200)

    resp=sts_client.assume_role_with_web_identity(RoleArn=role_response['Role']['Arn'],RoleSessionName=role_session_name,WebIdentityToken=user_token)
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

@attr(resource='assume role with web identity')
@attr(method='get')
@attr(operation='check')
@attr(assertion='assuming role using web token with aws:PrincipalTag in role policy')
@attr('webidentity_test')
@attr('abac_test')
@attr('token_principal_tag_role_policy_test')
def test_assume_role_with_web_identity_for_all_values():
    check_webidentity()
    iam_client=get_iam_client()
    sts_client=get_sts_client()
    default_endpoint=get_config_endpoint()
    role_session_name=get_parameter_name()
    thumbprint=get_thumbprint()
    user_token=get_user_token()
    realm=get_realm_name()

    oidc_response = iam_client.create_open_id_connect_provider(
    Url='http://localhost:8080/auth/realms/{}'.format(realm),
    ThumbprintList=[
        thumbprint,
    ],
    )

    policy_document = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Federated\":[\""+oidc_response["OpenIDConnectProviderArn"]+"\"]},\"Action\":[\"sts:AssumeRoleWithWebIdentity\",\"sts:TagSession\"],\"Condition\":{\"StringEquals\":{\"aws:RequestTag/Department\":\"Engineering\"}}}]}"
    (role_error,role_response,general_role_name)=create_role(iam_client,'/',None,policy_document,None,None,None)
    eq(role_response['Role']['Arn'],'arn:aws:iam:::role/'+general_role_name+'')

    role_policy = "{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Allow\",\"Action\":\"s3:*\",\"Resource\":\"arn:aws:s3:::*\",\"Condition\":{\"ForAllValues:StringEquals\":{\"aws:PrincipalTag/Department\":[\"Engineering\",\"Marketing\"]}}}}"
    (role_err,response)=put_role_policy(iam_client,general_role_name,None,role_policy)
    eq(response['ResponseMetadata']['HTTPStatusCode'],200)

    resp=sts_client.assume_role_with_web_identity(RoleArn=role_response['Role']['Arn'],RoleSessionName=role_session_name,WebIdentityToken=user_token)
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

@attr(resource='assume role with web identity')
@attr(method='get')
@attr(operation='check')
@attr(assertion='assuming role using web token with aws:PrincipalTag in role policy')
@attr('webidentity_test')
@attr('abac_test')
@attr('token_principal_tag_role_policy_test')
def test_assume_role_with_web_identity_for_all_values_deny():
    check_webidentity()
    iam_client=get_iam_client()
    sts_client=get_sts_client()
    default_endpoint=get_config_endpoint()
    role_session_name=get_parameter_name()
    thumbprint=get_thumbprint()
    user_token=get_user_token()
    realm=get_realm_name()

    oidc_response = iam_client.create_open_id_connect_provider(
    Url='http://localhost:8080/auth/realms/{}'.format(realm),
    ThumbprintList=[
        thumbprint,
    ],
    )

    policy_document = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Federated\":[\""+oidc_response["OpenIDConnectProviderArn"]+"\"]},\"Action\":[\"sts:AssumeRoleWithWebIdentity\",\"sts:TagSession\"],\"Condition\":{\"StringEquals\":{\"aws:RequestTag/Department\":\"Engineering\"}}}]}"
    (role_error,role_response,general_role_name)=create_role(iam_client,'/',None,policy_document,None,None,None)
    eq(role_response['Role']['Arn'],'arn:aws:iam:::role/'+general_role_name+'')

    #ForAllValues: The condition returns true if every key value in the request matches at least one value in the policy
    role_policy = "{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Allow\",\"Action\":\"s3:*\",\"Resource\":\"arn:aws:s3:::*\",\"Condition\":{\"ForAllValues:StringEquals\":{\"aws:PrincipalTag/Department\":[\"Engineering\"]}}}}"
    (role_err,response)=put_role_policy(iam_client,general_role_name,None,role_policy)
    eq(response['ResponseMetadata']['HTTPStatusCode'],200)

    resp=sts_client.assume_role_with_web_identity(RoleArn=role_response['Role']['Arn'],RoleSessionName=role_session_name,WebIdentityToken=user_token)
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

    oidc_remove=iam_client.delete_open_id_connect_provider(
    OpenIDConnectProviderArn=oidc_response["OpenIDConnectProviderArn"]
    )

@attr(resource='assume role with web identity')
@attr(method='get')
@attr(operation='check')
@attr(assertion='assuming role using web token with aws:TagKeys in trust policy')
@attr('webidentity_test')
@attr('abac_test')
@attr('token_tag_keys_test')
def test_assume_role_with_web_identity_tag_keys_trust_policy():
    check_webidentity()
    iam_client=get_iam_client()
    sts_client=get_sts_client()
    default_endpoint=get_config_endpoint()
    role_session_name=get_parameter_name()
    thumbprint=get_thumbprint()
    user_token=get_user_token()
    realm=get_realm_name()

    oidc_response = iam_client.create_open_id_connect_provider(
    Url='http://localhost:8080/auth/realms/{}'.format(realm),
    ThumbprintList=[
        thumbprint,
    ],
    )

    policy_document = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Federated\":[\""+oidc_response["OpenIDConnectProviderArn"]+"\"]},\"Action\":[\"sts:AssumeRoleWithWebIdentity\",\"sts:TagSession\"],\"Condition\":{\"StringEquals\":{\"aws:TagKeys\":\"Department\"}}}]}"
    (role_error,role_response,general_role_name)=create_role(iam_client,'/',None,policy_document,None,None,None)
    eq(role_response['Role']['Arn'],'arn:aws:iam:::role/'+general_role_name+'')

    role_policy = "{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Allow\",\"Action\":\"s3:*\",\"Resource\":\"arn:aws:s3:::*\",\"Condition\":{\"ForAnyValue:StringEquals\":{\"aws:PrincipalTag/Department\":[\"Engineering\"]}}}}"
    (role_err,response)=put_role_policy(iam_client,general_role_name,None,role_policy)
    eq(response['ResponseMetadata']['HTTPStatusCode'],200)

    resp=sts_client.assume_role_with_web_identity(RoleArn=role_response['Role']['Arn'],RoleSessionName=role_session_name,WebIdentityToken=user_token)
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

@attr(resource='assume role with web identity')
@attr(method='get')
@attr(operation='check')
@attr(assertion='assuming role using web token with aws:TagKeys in role permission policy')
@attr('webidentity_test')
@attr('abac_test')
@attr('token_tag_keys_test')
def test_assume_role_with_web_identity_tag_keys_role_policy():
    check_webidentity()
    iam_client=get_iam_client()
    sts_client=get_sts_client()
    default_endpoint=get_config_endpoint()
    role_session_name=get_parameter_name()
    thumbprint=get_thumbprint()
    user_token=get_user_token()
    realm=get_realm_name()

    oidc_response = iam_client.create_open_id_connect_provider(
    Url='http://localhost:8080/auth/realms/{}'.format(realm),
    ThumbprintList=[
        thumbprint,
    ],
    )

    policy_document = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Federated\":[\""+oidc_response["OpenIDConnectProviderArn"]+"\"]},\"Action\":[\"sts:AssumeRoleWithWebIdentity\",\"sts:TagSession\"],\"Condition\":{\"StringEquals\":{\"aws:RequestTag/Department\":\"Engineering\"}}}]}"
    (role_error,role_response,general_role_name)=create_role(iam_client,'/',None,policy_document,None,None,None)
    eq(role_response['Role']['Arn'],'arn:aws:iam:::role/'+general_role_name+'')

    role_policy = "{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Allow\",\"Action\":\"s3:*\",\"Resource\":\"arn:aws:s3:::*\",\"Condition\":{\"StringEquals\":{\"aws:TagKeys\":[\"Department\"]}}}}"
    (role_err,response)=put_role_policy(iam_client,general_role_name,None,role_policy)
    eq(response['ResponseMetadata']['HTTPStatusCode'],200)

    resp=sts_client.assume_role_with_web_identity(RoleArn=role_response['Role']['Arn'],RoleSessionName=role_session_name,WebIdentityToken=user_token)
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

@attr(resource='assume role with web identity')
@attr(method='put')
@attr(operation='check')
@attr(assertion='assuming role using web token with s3:ResourceTag in role permission policy')
@attr('webidentity_test')
@attr('abac_test')
@attr('token_resource_tags_test')
def test_assume_role_with_web_identity_resource_tag():
    check_webidentity()
    iam_client=get_iam_client()
    sts_client=get_sts_client()
    default_endpoint=get_config_endpoint()
    role_session_name=get_parameter_name()
    thumbprint=get_thumbprint()
    user_token=get_user_token()
    realm=get_realm_name()

    s3_res_iam_creds = get_s3_resource_using_iam_creds()

    s3_client_iam_creds = s3_res_iam_creds.meta.client

    bucket_name = get_new_bucket_name()
    s3bucket = s3_client_iam_creds.create_bucket(Bucket=bucket_name)
    eq(s3bucket['ResponseMetadata']['HTTPStatusCode'],200)

    bucket_tagging = s3_res_iam_creds.BucketTagging(bucket_name)
    Set_Tag = bucket_tagging.put(Tagging={'TagSet':[{'Key':'Department', 'Value': 'Engineering'},{'Key':'Department', 'Value': 'Marketing'}]})

    oidc_response = iam_client.create_open_id_connect_provider(
    Url='http://localhost:8080/auth/realms/{}'.format(realm),
    ThumbprintList=[
        thumbprint,
    ],
    )

    policy_document = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Federated\":[\""+oidc_response["OpenIDConnectProviderArn"]+"\"]},\"Action\":[\"sts:AssumeRoleWithWebIdentity\",\"sts:TagSession\"],\"Condition\":{\"StringEquals\":{\"aws:RequestTag/Department\":\"Engineering\"}}}]}"
    (role_error,role_response,general_role_name)=create_role(iam_client,'/',None,policy_document,None,None,None)
    eq(role_response['Role']['Arn'],'arn:aws:iam:::role/'+general_role_name+'')

    role_policy = "{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Allow\",\"Action\":\"s3:*\",\"Resource\":\"arn:aws:s3:::*\",\"Condition\":{\"StringEquals\":{\"s3:ResourceTag/Department\":[\"Engineering\"]}}}}"
    (role_err,response)=put_role_policy(iam_client,general_role_name,None,role_policy)
    eq(response['ResponseMetadata']['HTTPStatusCode'],200)

    resp=sts_client.assume_role_with_web_identity(RoleArn=role_response['Role']['Arn'],RoleSessionName=role_session_name,WebIdentityToken=user_token)
    eq(resp['ResponseMetadata']['HTTPStatusCode'],200)

    s3_client = boto3.client('s3',
        aws_access_key_id = resp['Credentials']['AccessKeyId'],
        aws_secret_access_key = resp['Credentials']['SecretAccessKey'],
        aws_session_token = resp['Credentials']['SessionToken'],
        endpoint_url=default_endpoint,
        region_name='',
        )

    bucket_body = 'this is a test file'
    s3_put_obj = s3_client.put_object(Body=bucket_body, Bucket=bucket_name, Key="test-1.txt")
    eq(s3_put_obj['ResponseMetadata']['HTTPStatusCode'],200)

    oidc_remove=iam_client.delete_open_id_connect_provider(
    OpenIDConnectProviderArn=oidc_response["OpenIDConnectProviderArn"]
    )

@attr(resource='assume role with web identity')
@attr(method='put')
@attr(operation='check')
@attr(assertion='assuming role using web token with s3:ResourceTag with missing tags on bucket')
@attr('webidentity_test')
@attr('abac_test')
@attr('token_resource_tags_test')
def test_assume_role_with_web_identity_resource_tag_deny():
    check_webidentity()
    iam_client=get_iam_client()
    sts_client=get_sts_client()
    default_endpoint=get_config_endpoint()
    role_session_name=get_parameter_name()
    thumbprint=get_thumbprint()
    user_token=get_user_token()
    realm=get_realm_name()

    s3_res_iam_creds = get_s3_resource_using_iam_creds()

    s3_client_iam_creds = s3_res_iam_creds.meta.client

    bucket_name = get_new_bucket_name()
    s3bucket = s3_client_iam_creds.create_bucket(Bucket=bucket_name)
    eq(s3bucket['ResponseMetadata']['HTTPStatusCode'],200)

    oidc_response = iam_client.create_open_id_connect_provider(
    Url='http://localhost:8080/auth/realms/{}'.format(realm),
    ThumbprintList=[
        thumbprint,
    ],
    )

    policy_document = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Federated\":[\""+oidc_response["OpenIDConnectProviderArn"]+"\"]},\"Action\":[\"sts:AssumeRoleWithWebIdentity\",\"sts:TagSession\"],\"Condition\":{\"StringEquals\":{\"aws:RequestTag/Department\":\"Engineering\"}}}]}"
    (role_error,role_response,general_role_name)=create_role(iam_client,'/',None,policy_document,None,None,None)
    eq(role_response['Role']['Arn'],'arn:aws:iam:::role/'+general_role_name+'')

    role_policy = "{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Allow\",\"Action\":\"s3:*\",\"Resource\":\"arn:aws:s3:::*\",\"Condition\":{\"StringEquals\":{\"s3:ResourceTag/Department\":[\"Engineering\"]}}}}"
    (role_err,response)=put_role_policy(iam_client,general_role_name,None,role_policy)
    eq(response['ResponseMetadata']['HTTPStatusCode'],200)

    resp=sts_client.assume_role_with_web_identity(RoleArn=role_response['Role']['Arn'],RoleSessionName=role_session_name,WebIdentityToken=user_token)
    eq(resp['ResponseMetadata']['HTTPStatusCode'],200)

    s3_client = boto3.client('s3',
        aws_access_key_id = resp['Credentials']['AccessKeyId'],
        aws_secret_access_key = resp['Credentials']['SecretAccessKey'],
        aws_session_token = resp['Credentials']['SessionToken'],
        endpoint_url=default_endpoint,
        region_name='',
        )

    bucket_body = 'this is a test file'
    try:
        s3_put_obj = s3_client.put_object(Body=bucket_body, Bucket=bucket_name, Key="test-1.txt")
    except ClientError as e:
        s3_put_obj_error = e.response.get("Error", {}).get("Code")
    eq(s3_put_obj_error,'AccessDenied')

    oidc_remove=iam_client.delete_open_id_connect_provider(
    OpenIDConnectProviderArn=oidc_response["OpenIDConnectProviderArn"]
    )

@attr(resource='assume role with web identity')
@attr(method='put')
@attr(operation='check')
@attr(assertion='assuming role using web token with s3:ResourceTag with wrong resource tag in policy')
@attr('webidentity_test')
@attr('abac_test')
@attr('token_resource_tags_test')
def test_assume_role_with_web_identity_wrong_resource_tag_deny():
    check_webidentity()
    iam_client=get_iam_client()
    sts_client=get_sts_client()
    default_endpoint=get_config_endpoint()
    role_session_name=get_parameter_name()
    thumbprint=get_thumbprint()
    user_token=get_user_token()
    realm=get_realm_name()

    s3_res_iam_creds = get_s3_resource_using_iam_creds()

    s3_client_iam_creds = s3_res_iam_creds.meta.client

    bucket_name = get_new_bucket_name()
    s3bucket = s3_client_iam_creds.create_bucket(Bucket=bucket_name)
    eq(s3bucket['ResponseMetadata']['HTTPStatusCode'],200)

    bucket_tagging = s3_res_iam_creds.BucketTagging(bucket_name)
    Set_Tag = bucket_tagging.put(Tagging={'TagSet':[{'Key':'Department', 'Value': 'WrongResourcetag'}]})

    oidc_response = iam_client.create_open_id_connect_provider(
    Url='http://localhost:8080/auth/realms/{}'.format(realm),
    ThumbprintList=[
        thumbprint,
    ],
    )

    policy_document = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Federated\":[\""+oidc_response["OpenIDConnectProviderArn"]+"\"]},\"Action\":[\"sts:AssumeRoleWithWebIdentity\",\"sts:TagSession\"],\"Condition\":{\"StringEquals\":{\"aws:RequestTag/Department\":\"Engineering\"}}}]}"
    (role_error,role_response,general_role_name)=create_role(iam_client,'/',None,policy_document,None,None,None)
    eq(role_response['Role']['Arn'],'arn:aws:iam:::role/'+general_role_name+'')

    role_policy = "{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Allow\",\"Action\":\"s3:*\",\"Resource\":\"arn:aws:s3:::*\",\"Condition\":{\"StringEquals\":{\"s3:ResourceTag/Department\":[\"Engineering\"]}}}}"
    (role_err,response)=put_role_policy(iam_client,general_role_name,None,role_policy)
    eq(response['ResponseMetadata']['HTTPStatusCode'],200)

    resp=sts_client.assume_role_with_web_identity(RoleArn=role_response['Role']['Arn'],RoleSessionName=role_session_name,WebIdentityToken=user_token)
    eq(resp['ResponseMetadata']['HTTPStatusCode'],200)

    s3_client = boto3.client('s3',
        aws_access_key_id = resp['Credentials']['AccessKeyId'],
        aws_secret_access_key = resp['Credentials']['SecretAccessKey'],
        aws_session_token = resp['Credentials']['SessionToken'],
        endpoint_url=default_endpoint,
        region_name='',
        )

    bucket_body = 'this is a test file'
    try:
        s3_put_obj = s3_client.put_object(Body=bucket_body, Bucket=bucket_name, Key="test-1.txt")
    except ClientError as e:
        s3_put_obj_error = e.response.get("Error", {}).get("Code")
    eq(s3_put_obj_error,'AccessDenied')

    oidc_remove=iam_client.delete_open_id_connect_provider(
    OpenIDConnectProviderArn=oidc_response["OpenIDConnectProviderArn"]
    )

@attr(resource='assume role with web identity')
@attr(method='put')
@attr(operation='check')
@attr(assertion='assuming role using web token with s3:ResourceTag matching aws:PrincipalTag in role permission policy')
@attr('webidentity_test')
@attr('abac_test')
@attr('token_resource_tags_test')
def test_assume_role_with_web_identity_resource_tag_princ_tag():
    check_webidentity()
    iam_client=get_iam_client()
    sts_client=get_sts_client()
    default_endpoint=get_config_endpoint()
    role_session_name=get_parameter_name()
    thumbprint=get_thumbprint()
    user_token=get_user_token()
    realm=get_realm_name()

    s3_res_iam_creds = get_s3_resource_using_iam_creds()

    s3_client_iam_creds = s3_res_iam_creds.meta.client

    bucket_name = get_new_bucket_name()
    s3bucket = s3_client_iam_creds.create_bucket(Bucket=bucket_name)
    eq(s3bucket['ResponseMetadata']['HTTPStatusCode'],200)

    bucket_tagging = s3_res_iam_creds.BucketTagging(bucket_name)
    Set_Tag = bucket_tagging.put(Tagging={'TagSet':[{'Key':'Department', 'Value': 'Engineering'}]})

    oidc_response = iam_client.create_open_id_connect_provider(
    Url='http://localhost:8080/auth/realms/{}'.format(realm),
    ThumbprintList=[
        thumbprint,
    ],
    )

    policy_document = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Federated\":[\""+oidc_response["OpenIDConnectProviderArn"]+"\"]},\"Action\":[\"sts:AssumeRoleWithWebIdentity\",\"sts:TagSession\"],\"Condition\":{\"StringEquals\":{\"aws:RequestTag/Department\":\"Engineering\"}}}]}"
    (role_error,role_response,general_role_name)=create_role(iam_client,'/',None,policy_document,None,None,None)
    eq(role_response['Role']['Arn'],'arn:aws:iam:::role/'+general_role_name+'')

    role_policy = "{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Allow\",\"Action\":\"s3:*\",\"Resource\":\"arn:aws:s3:::*\",\"Condition\":{\"StringEquals\":{\"s3:ResourceTag/Department\":[\"${aws:PrincipalTag/Department}\"]}}}}"
    (role_err,response)=put_role_policy(iam_client,general_role_name,None,role_policy)
    eq(response['ResponseMetadata']['HTTPStatusCode'],200)

    resp=sts_client.assume_role_with_web_identity(RoleArn=role_response['Role']['Arn'],RoleSessionName=role_session_name,WebIdentityToken=user_token)
    eq(resp['ResponseMetadata']['HTTPStatusCode'],200)

    s3_client = boto3.client('s3',
        aws_access_key_id = resp['Credentials']['AccessKeyId'],
        aws_secret_access_key = resp['Credentials']['SecretAccessKey'],
        aws_session_token = resp['Credentials']['SessionToken'],
        endpoint_url=default_endpoint,
        region_name='',
        )

    bucket_body = 'this is a test file'
    tags = 'Department=Engineering&Department=Marketing'
    key = "test-1.txt"
    s3_put_obj = s3_client.put_object(Body=bucket_body, Bucket=bucket_name, Key=key, Tagging=tags)
    eq(s3_put_obj['ResponseMetadata']['HTTPStatusCode'],200)

    s3_get_obj = s3_client.get_object(Bucket=bucket_name, Key=key)
    eq(s3_get_obj['ResponseMetadata']['HTTPStatusCode'],200)

    oidc_remove=iam_client.delete_open_id_connect_provider(
    OpenIDConnectProviderArn=oidc_response["OpenIDConnectProviderArn"]
    )

@attr(resource='assume role with web identity')
@attr(method='put')
@attr(operation='check')
@attr(assertion='assuming role using web token with s3:ResourceTag used to test copy object')
@attr('webidentity_test')
@attr('abac_test')
@attr('token_resource_tags_test')
def test_assume_role_with_web_identity_resource_tag_copy_obj():
    check_webidentity()
    iam_client=get_iam_client()
    sts_client=get_sts_client()
    default_endpoint=get_config_endpoint()
    role_session_name=get_parameter_name()
    thumbprint=get_thumbprint()
    user_token=get_user_token()
    realm=get_realm_name()

    s3_res_iam_creds = get_s3_resource_using_iam_creds()

    s3_client_iam_creds = s3_res_iam_creds.meta.client

    #create two buckets and add same tags to both
    bucket_name = get_new_bucket_name()
    s3bucket = s3_client_iam_creds.create_bucket(Bucket=bucket_name)
    eq(s3bucket['ResponseMetadata']['HTTPStatusCode'],200)

    bucket_tagging = s3_res_iam_creds.BucketTagging(bucket_name)
    Set_Tag = bucket_tagging.put(Tagging={'TagSet':[{'Key':'Department', 'Value': 'Engineering'}]})

    copy_bucket_name = get_new_bucket_name()
    s3bucket = s3_client_iam_creds.create_bucket(Bucket=copy_bucket_name)
    eq(s3bucket['ResponseMetadata']['HTTPStatusCode'],200)

    bucket_tagging = s3_res_iam_creds.BucketTagging(copy_bucket_name)
    Set_Tag = bucket_tagging.put(Tagging={'TagSet':[{'Key':'Department', 'Value': 'Engineering'}]})

    oidc_response = iam_client.create_open_id_connect_provider(
    Url='http://localhost:8080/auth/realms/{}'.format(realm),
    ThumbprintList=[
        thumbprint,
    ],
    )

    policy_document = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Federated\":[\""+oidc_response["OpenIDConnectProviderArn"]+"\"]},\"Action\":[\"sts:AssumeRoleWithWebIdentity\",\"sts:TagSession\"],\"Condition\":{\"StringEquals\":{\"aws:RequestTag/Department\":\"Engineering\"}}}]}"
    (role_error,role_response,general_role_name)=create_role(iam_client,'/',None,policy_document,None,None,None)
    eq(role_response['Role']['Arn'],'arn:aws:iam:::role/'+general_role_name+'')

    role_policy = "{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Allow\",\"Action\":\"s3:*\",\"Resource\":\"arn:aws:s3:::*\",\"Condition\":{\"StringEquals\":{\"s3:ResourceTag/Department\":[\"${aws:PrincipalTag/Department}\"]}}}}"
    (role_err,response)=put_role_policy(iam_client,general_role_name,None,role_policy)
    eq(response['ResponseMetadata']['HTTPStatusCode'],200)

    resp=sts_client.assume_role_with_web_identity(RoleArn=role_response['Role']['Arn'],RoleSessionName=role_session_name,WebIdentityToken=user_token)
    eq(resp['ResponseMetadata']['HTTPStatusCode'],200)

    s3_client = boto3.client('s3',
        aws_access_key_id = resp['Credentials']['AccessKeyId'],
        aws_secret_access_key = resp['Credentials']['SecretAccessKey'],
        aws_session_token = resp['Credentials']['SessionToken'],
        endpoint_url=default_endpoint,
        region_name='',
        )

    bucket_body = 'this is a test file'
    tags = 'Department=Engineering'
    key = "test-1.txt"
    s3_put_obj = s3_client.put_object(Body=bucket_body, Bucket=bucket_name, Key=key, Tagging=tags)
    eq(s3_put_obj['ResponseMetadata']['HTTPStatusCode'],200)

    #copy to same bucket
    copy_source = {
    'Bucket': bucket_name,
    'Key': 'test-1.txt'
    }

    s3_client.copy(copy_source, bucket_name, "test-2.txt")

    s3_get_obj = s3_client.get_object(Bucket=bucket_name, Key="test-2.txt")
    eq(s3_get_obj['ResponseMetadata']['HTTPStatusCode'],200)

    #copy to another bucket
    copy_source = {
    'Bucket': bucket_name,
    'Key': 'test-1.txt'
    }

    s3_client.copy(copy_source, copy_bucket_name, "test-1.txt")

    s3_get_obj = s3_client.get_object(Bucket=copy_bucket_name, Key="test-1.txt")
    eq(s3_get_obj['ResponseMetadata']['HTTPStatusCode'],200)

    oidc_remove=iam_client.delete_open_id_connect_provider(
    OpenIDConnectProviderArn=oidc_response["OpenIDConnectProviderArn"]
    )

@attr(resource='assume role with web identity')
@attr(method='put')
@attr(operation='check')
@attr(assertion='assuming role using web token with iam:ResourceTag in role trust policy')
@attr('webidentity_test')
@attr('abac_test')
@attr('token_role_tags_test')
def test_assume_role_with_web_identity_role_resource_tag():
    check_webidentity()
    iam_client=get_iam_client()
    sts_client=get_sts_client()
    default_endpoint=get_config_endpoint()
    role_session_name=get_parameter_name()
    thumbprint=get_thumbprint()
    user_token=get_user_token()
    realm=get_realm_name()

    s3_res_iam_creds = get_s3_resource_using_iam_creds()

    s3_client_iam_creds = s3_res_iam_creds.meta.client

    bucket_name = get_new_bucket_name()
    s3bucket = s3_client_iam_creds.create_bucket(Bucket=bucket_name)
    eq(s3bucket['ResponseMetadata']['HTTPStatusCode'],200)

    bucket_tagging = s3_res_iam_creds.BucketTagging(bucket_name)
    Set_Tag = bucket_tagging.put(Tagging={'TagSet':[{'Key':'Department', 'Value': 'Engineering'},{'Key':'Department', 'Value': 'Marketing'}]})

    oidc_response = iam_client.create_open_id_connect_provider(
    Url='http://localhost:8080/auth/realms/{}'.format(realm),
    ThumbprintList=[
        thumbprint,
    ],
    )

    #iam:ResourceTag refers to the tag attached to role, hence the role is allowed to be assumed only when it has a tag matching the policy.
    policy_document = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Federated\":[\""+oidc_response["OpenIDConnectProviderArn"]+"\"]},\"Action\":[\"sts:AssumeRoleWithWebIdentity\",\"sts:TagSession\"],\"Condition\":{\"StringEquals\":{\"iam:ResourceTag/Department\":\"Engineering\"}}}]}"
    tags_list = [
            {'Key':'Department','Value':'Engineering'},
            {'Key':'Department','Value':'Marketing'}
        ]

    (role_error,role_response,general_role_name)=create_role(iam_client,'/',None,policy_document,None,None,None,tags_list)
    eq(role_response['Role']['Arn'],'arn:aws:iam:::role/'+general_role_name+'')

    role_policy = "{\"Version\":\"2012-10-17\",\"Statement\":{\"Effect\":\"Allow\",\"Action\":\"s3:*\",\"Resource\":\"arn:aws:s3:::*\",\"Condition\":{\"StringEquals\":{\"s3:ResourceTag/Department\":[\"Engineering\"]}}}}"
    (role_err,response)=put_role_policy(iam_client,general_role_name,None,role_policy)
    eq(response['ResponseMetadata']['HTTPStatusCode'],200)

    resp=sts_client.assume_role_with_web_identity(RoleArn=role_response['Role']['Arn'],RoleSessionName=role_session_name,WebIdentityToken=user_token)
    eq(resp['ResponseMetadata']['HTTPStatusCode'],200)

    s3_client = boto3.client('s3',
        aws_access_key_id = resp['Credentials']['AccessKeyId'],
        aws_secret_access_key = resp['Credentials']['SecretAccessKey'],
        aws_session_token = resp['Credentials']['SessionToken'],
        endpoint_url=default_endpoint,
        region_name='',
        )

    bucket_body = 'this is a test file'
    s3_put_obj = s3_client.put_object(Body=bucket_body, Bucket=bucket_name, Key="test-1.txt")
    eq(s3_put_obj['ResponseMetadata']['HTTPStatusCode'],200)

    oidc_remove=iam_client.delete_open_id_connect_provider(
    OpenIDConnectProviderArn=oidc_response["OpenIDConnectProviderArn"]
    )
