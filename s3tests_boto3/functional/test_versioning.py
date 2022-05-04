import boto3
from botocore.exceptions import ClientError
import json
import os
import time
from multiprocessing import Process
import boto3
from nose.tools import eq_ as eq
from nose.plugins.attrib import attr
import nose
from botocore.exceptions import ClientError
from email.utils import formatdate
import filecmp
from .utils import assert_raises
from .utils import _get_status_and_error_code
from .utils import _get_status

from . import (
    get_client,
    get_v2_client,
    get_new_bucket,
    get_new_bucket_name,
    )


def tag(*tags):
    def wrap(func):
        for tag in tags:
            setattr(func, tag, True)
        return func
    return wrap

def put_bucket_versioning_(versioning_status):
    client = boto3.client('s3')
    bucket = get_new_bucket()
    response = client.put_bucket_versioning(
             Bucket=bucket,
             VersioningConfiguration={
                    'Status': versioning_status
             }
    )
    return bucket

def versioning_status(bucket):
    client = boto3.client('s3')
    response = client.get_bucket_versioning(Bucket=bucket)
    status = response['Status']
    return status    

@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='put valid replication policy and make sure data is replicated')
@attr(assertion='passes')
def test_put_bucket_versioning():
    client = boto3.client('s3')
    bucket = get_new_bucket()
    response = client.put_bucket_versioning(
             Bucket=bucket,
             VersioningConfiguration={
                    'Status': 'Enabled'
             }
    )
     
    status = _get_status(response)
    eq(status, 200)

@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='put valid replication policy and make sure data is replicated')
@attr(assertion='passes')
def test_get_bucket_versioning():
    client = boto3.client('s3')
    bucket = put_bucket_versioning_('Enabled')
    response = client.get_bucket_versioning(
             Bucket=bucket,
    )

    status = _get_status(response)
    eq(status, 200)

@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='put valid replication policy and make sure data is replicated')
@attr(assertion='passes')
def test_put_bucket_versioning_enable():
    client = boto3.client('s3')
    bucket = put_bucket_versioning_('Enabled')
    status = versioning_status(bucket)
    eq(status, 'Enabled')

@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='put valid replication policy and make sure data is replicated')
@attr(assertion='passes')
def test_put_bucket_versioning_suspended():
    client = boto3.client('s3')
    bucket = put_bucket_versioning_('Suspended')
    status = versioning_status(bucket)
    eq(status, 'Suspended')

