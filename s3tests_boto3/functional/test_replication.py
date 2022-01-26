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

def _get_replication_config_status(response):
    return response['ReplicationConfiguration']['Rules'][0]['Status']

def is_data_equal(src_bucket, dest_bucket):
    s3 = boto3.resource('s3')

    src_bucket = s3.Bucket(src_bucket)
    dest_bucket = s3.Bucket(dest_bucket)

    src_key=[]
    src_body=[]
    for obj in src_bucket.objects.all():
        src_key.append(obj.key)
        src_body.append(obj.get()['Body'].read())
    
    # allow time for data to be replicated across buckets
    time.sleep(200)

    dest_key=[]
    dest_body=[]
    for obj in dest_bucket.objects.all():
        dest_key.append(obj.key)
        dest_body.append(obj.get()['Body'].read())
    
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

def create_replication_policy_(role_name, policy_name, src_bucket, dest_bucket):

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

def cleanup_policy():
    role_name = 'role-test'
    policy_name = 'policy-test'
    client = boto3.client('iam')
    response = client.delete_role_policy(
        RoleName=role_name,
        PolicyName=policy_name
    )
    response = client.delete_role(
        RoleName=role_name
    )

def create_file():
    filename="sample.txt"
    fp = open('sample.txt', 'w')
    fp.write('sample text')
    fp.close()
    return filename

def get_replication_status(src_bucket):
    client = boto3.client('s3') 
    time.sleep(100)
    response = client.head_object(Bucket=src_bucket, Key="Tax/test")
    return response

def create_replication_policy():
    src_bucket = get_new_bucket()
    dest_bucket = get_new_bucket()

    enable_versioning(src_bucket)
    enable_versioning(dest_bucket)

    role_name="role-test"
    policy_name='policy-test'

    create_iam_role(role_name)
    create_replication_policy_(role_name, policy_name, src_bucket, dest_bucket)
    file_name=create_file()
    add_data(file_name, src_bucket)
    
    return src_bucket, dest_bucket


'''
####################### put-replication-policy test below #####################
@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='put valid replication policy and make sure data is replicated')
@attr(assertion='passes')
def test_put_bucket_replication():
    
    #src_bucket = get_new_bucket()
    #dest_bucket = get_new_bucket()
    
    #enable_versioning(src_bucket)
    #enable_versioning(dest_bucket)
    
    #role_name="role-test"
    #policy_name='policy-test'
    
    #create_iam_role(role_name)
    #create_replication_policy(role_name, policy_name, src_bucket, dest_bucket)
    src_bucket, dest_bucket = create_replication_policy()

    file_name=create_file()
    add_data(file_name, src_bucket)
    response = get_replication_status(src_bucket) 
    status = _get_status(response)
    
    response = is_data_equal(src_bucket, dest_bucket)
    eq(response, True)
    eq(status, 200)
    cleanup_policy()
    
################# put_bucket_replication above ##########################

################# get_bucket_replication below ##########################
@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='test get-replication-policy with correct test')
@attr(assertion='passes with correct status')
def test_get_bucket_replication():
    
    #src_bucket = get_new_bucket()
    #dest_bucket = get_new_bucket()

    #enable_versioning(src_bucket)
    #enable_versioning(dest_bucket)
    
    #role_name="role-test"
    #policy_name='policy-test'

    #create_iam_role(role_name)
    #create_replication_policy(role_name, policy_name, src_bucket, dest_bucket)
    src_bucket, dest_bucket = create_replication_policy()
    file_name=create_file()
    add_data(file_name, src_bucket)
    response = get_replication_status(src_bucket)
    status = _get_status(response)

    client = client = boto3.client('s3')
    response = client.get_bucket_replication(Bucket=src_bucket)
    status_get_replication = _get_replication_config_status(response)
    http_status_get_replication = _get_status(response)
 
    eq(status_get_replication, 'Enabled')
    eq(http_status_get_replication, 200)
    eq(status, 200)
    cleanup_policy()

############### get_bucket_replication above ############################


################# delete_bucket_replication below ##########################
@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='test get-replication-policy with correct test')
@attr(assertion='passes with correct status')
def test_delete_bucket_replication():

    #src_bucket = get_new_bucket()
    #dest_bucket = get_new_bucket()

    #enable_versioning(src_bucket)
    #enable_versioning(dest_bucket)

    #role_name="role-test"
    #policy_name='policy-test'

    #create_iam_role(role_name)
    #create_replication_policy(role_name, policy_name, src_bucket, dest_bucket)

    #file_name=create_file()
    #add_data(file_name, src_bucket)
    src_bucket, dest_bucket = create_replication_policy()
    client = boto3.client('s3')    

    response = client.get_bucket_replication(Bucket=src_bucket)
    print('before: ', response, '\n')
    response = client.delete_bucket_replication(Bucket=src_bucket)
    time.sleep(20)    
    try:
        response = client.get_bucket_replication(Bucket=src_bucket)
    except ClientError as ce:
        print(ce)
    print('after: ', response, '\n')
    status = _get_status(response)    
    eq(status, 204)
    cleanup_policy()

############### delete_bucket_replication above ############################

@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='test get-replication-policy with correct test')
@attr(assertion='passes with correct status')
def test_delete_bucket_replication_on_non_existing_bucket():
    bucket_name = get_new_bucket_name()
    bucket_name = bucket_name + 'xzyjdfkpayhe909'
    response = ''
    client = boto3.client('s3')
    try:
        response = client.delete_bucket_replication(Bucket=bucket_name)
    except Exception as e:
        print(e)
    eq(response, '')

def create_replication_policy_with_replication_policy_disabled(role_name, policy_name, src_bucket, dest_bucket):
    role_permissions_policy=json.loads('{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObjectVersionForReplication","s3:GetObjectVersionAcl","s3:GetObjectVersionTagging"],"Resource":["arn:aws:s3:::'+src_bucket+'/*"]},{"Effect":"Allow","Action":["s3:ListBucket","s3:GetReplicationConfiguration"],"Resource":["arn:aws:s3:::'+src_bucket+'"]},{"Effect":"Allow","Action":["s3:ReplicateObject","s3:ReplicateDelete","s3:ReplicateTags"],"Resource":"arn:aws:s3:::'+dest_bucket+'/*"}]}')

    client = boto3.client('iam')
    response = client.put_role_policy(
        PolicyDocument=json.dumps(role_permissions_policy),
        PolicyName=policy_name,
        RoleName=role_name,
    )
    response=client.get_role(RoleName=role_name)
    arn = response['Role']['Arn']
    replication_config=json.loads('{"Role": "'+arn+'","Rules": [{"Status": "Disabled","Priority": 1,"DeleteMarkerReplication": { "Status": "Disabled" },"Filter" : { "Prefix": "Tax"},"Destination": {"Bucket": "arn:aws:s3:::'+dest_bucket+'"}}]}')
    client = boto3.client('s3')
    response = client.put_bucket_replication(Bucket=src_bucket, ReplicationConfiguration=replication_config)

@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='test get-replication-policy with correct test')
@attr(assertion='passes with correct status')
def test_disable_bucket_replication_on_bucket():

    src_bucket = get_new_bucket()
    dest_bucket = get_new_bucket()

    enable_versioning(src_bucket)
    enable_versioning(dest_bucket)

    role_name="role-test"
    policy_name='policy-test'

    create_iam_role(role_name)
    create_replication_policy_with_replication_policy_disabled(role_name, policy_name, src_bucket, dest_bucket)

    file_name=create_file()
    add_data(file_name, src_bucket)
    
    client = boto3.client('s3')
    response = client.get_bucket_replication(Bucket=src_bucket)
    replication_status = _get_replication_config_status(response)
    
    print(is_data_equal(src_bucket, dest_bucket))
    print(replication_status)
    eq(is_data_equal(src_bucket, dest_bucket), "False")
    eq(replication_status, "Disabled")
    cleanup_policy()

def get_replication_status_with_prefix(src_bucket, prefix):
    client = boto3.client('s3')
    time.sleep(100)
    response = client.head_object(Bucket=src_bucket, Key=prefix+"test")
    return response

def create_replication_with_optional_prefix_filtering(role_name, policy_name, src_bucket, dest_bucket, dest_bucket1, dest_bucket2):
    role_permissions_policy=json.loads('{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObjectVersionForReplication","s3:GetObjectVersionAcl","s3:GetObjectVersionTagging"],"Resource":["arn:aws:s3:::'+src_bucket+'/*"]},{"Effect":"Allow","Action":["s3:ListBucket","s3:GetReplicationConfiguration"],"Resource":["arn:aws:s3:::'+src_bucket+'"]},{"Effect":"Allow","Action":["s3:ReplicateObject","s3:ReplicateDelete","s3:ReplicateTags"],"Resource":["arn:aws:s3:::'+dest_bucket+'/*", "arn:aws:s3:::'+dest_bucket1+'/*", "arn:aws:s3:::'+dest_bucket2+'/*"]}]}')

    client = boto3.client('iam')
    response = client.put_role_policy(
        PolicyDocument=json.dumps(role_permissions_policy),
        PolicyName=policy_name,
        RoleName=role_name,
    )
    response=client.get_role(RoleName=role_name)
    arn = response['Role']['Arn']
    replication_config=json.loads('{"Role": "'+arn+'","Rules": [{"Status": "Enabled","Priority": 1,"DeleteMarkerReplication": { "Status": "Disabled" }, "Filter" : { "Prefix": "dest2/" },"Destination": {"Bucket": "arn:aws:s3:::'+dest_bucket2+'"}}, {"Status": "Enabled","Priority": 2,"DeleteMarkerReplication": { "Status": "Disabled" }, "Filter" : { "Prefix": "dest1/"},"Destination": {"Bucket": "arn:aws:s3:::'+dest_bucket1+'"}}, {"Status": "Enabled","Priority": 3,"DeleteMarkerReplication": { "Status": "Disabled" }, "Filter" : { "Prefix": ""},"Destination": {"Bucket": "arn:aws:s3:::'+dest_bucket+'"}}]}')
    client = boto3.client('s3')
    response = client.put_bucket_replication(Bucket=src_bucket, ReplicationConfiguration=replication_config) 

def add_data_with_prefixes(file_name, bucket_name, prefix):
    s3_client = boto3.client('s3')
    s3_client.upload_file(file_name, bucket_name, prefix+"test")

def check_filtered_replication_worked(src_bucket, dest_bucket, prefix):
    s3 = boto3.resource('s3')

    src_bucket = s3.Bucket(src_bucket)
    dest_bucket = s3.Bucket(dest_bucket)
    
    print(src_bucket, dest_bucket)   
    src_key=[]
    src_body=[]
    for obj in src_bucket.objects.all():
        if obj.key == prefix:
            print("obj key: ", obj.key)
            print(obj.key == prefix)
            src_key.append(obj.key)
            src_body.append(obj.get()['Body'].read())

    time.sleep(180)

    dest_key=[]
    dest_body=[]
    for obj in dest_bucket.objects.all():
        if obj.key == prefix:
            dest_key.append(obj.key)
            dest_body.append(obj.get()['Body'].read())
    
    print(dest_key, src_key, dest_body, src_body)
    print((dest_key == src_key) and (dest_body == src_body))
    
    if dest_key != src_key:
        print("Replication does not copy the key value as expected: src_key=" + str(src_key) + " and dest_key="+ str(dest_key))
    elif dest_body != src_body:
        print("Replication does not copy the object body as expected: src_body=" + str(src_body) + " and dest_body="+ str(dest_body))
    return ((dest_key == src_key) and (dest_body == src_body))

@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='test get-replication-policy with correct test')
@attr(assertion='passes with correct status')
def test_bucket_replication_with_conditional_replication_filter():

    src_bucket = get_new_bucket()
    dest_bucket = get_new_bucket()
    dest_bucket1 = get_new_bucket()
    dest_bucket2 = get_new_bucket()

    enable_versioning(src_bucket)
    enable_versioning(dest_bucket)
    enable_versioning(dest_bucket1)
    enable_versioning(dest_bucket2)

    role_name="role-test"
    policy_name='policy-test'

    create_iam_role(role_name)
    create_replication_with_optional_prefix_filtering(role_name, policy_name, src_bucket, dest_bucket, dest_bucket1, dest_bucket2)
    client = boto3.client('s3')
    response = client.get_bucket_replication(Bucket=src_bucket)
    
    replication_status = _get_replication_config_status(response)
    file_name=create_file()
    add_data_with_prefixes(file_name, src_bucket, "")
    add_data_with_prefixes(file_name, src_bucket, "dest1/")
    add_data_with_prefixes(file_name, src_bucket, "dest2/")
        
    head_object_data = get_replication_status_with_prefix(src_bucket, "dest2/")
    
    eq(check_filtered_replication_worked(src_bucket, dest_bucket, ""), True)
    eq(check_filtered_replication_worked(src_bucket, dest_bucket2, "dest2/"), True)
    eq(check_filtered_replication_worked(src_bucket, dest_bucket1, "dest1/"), True)
    
    cleanup_policy()
'''
def create_large_object(data):
    filename="client"+data+".txt"
    fp = open(filename, 'a')
    for i in range(0,1000000000):
        fp.write(data)
    fp.close()
    
    return filename

def add_client_data(file_name, bucket_name):
    s3_client = boto3.client('s3')
    s3_client.upload_file(file_name, bucket_name, "client")

def compare_data(local_file, bucket):
    s3 = boto3.resource('s3')

    bucket = s3.Bucket(bucket)
    time.sleep(200)
    print(bucket)
    src_key=[]
    src_body=[]
    for obj in bucket.objects.all():
        src_key.append(obj.key)
        src_body.append(obj.get()['Body'].read())
    print(src_body)
    f = open(local_file)
    text = f.read()
    f.close()
    print("text")
    print(text)
    #return ((dest_key == src_key) and (dest_body == src_body))

def process_helper(data, bucket_name):
    file_name = create_large_object(data)
    add_client_data(file_name, bucket_name)

@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='test get-replication-policy with correct test')
@attr(assertion='passes with correct status')
def test_simultaneous_writes_from_two_users():
    
    bucket = get_new_bucket()

    client_a = Process(target=process_helper, args=("a", bucket,))
    client_b = Process(target=process_helper, args=("b", bucket,))

    client_a.start()
    client_b.start()

    client_a.join()
    client_b.join()

    local_file="clienta.txt"
    compare_data(local_file, bucket)
    local_file="clientb.txt"
    compare_data(local_file, bucket)
'''



