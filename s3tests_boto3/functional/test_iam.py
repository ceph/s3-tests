import json
import datetime
import time

from botocore.exceptions import ClientError
import pytest

from s3tests_boto3.functional.utils import assert_raises
from s3tests_boto3.functional.test_s3 import _multipart_upload
from . import (
    configfile,
    setup_teardown,
    get_alt_client,
    get_iam_client,
    get_iam_root_client,
    make_iam_name,
    get_iam_path_prefix,
    get_new_bucket,
    get_new_bucket_name,
    get_iam_s3client,
    get_alt_iam_client,
    get_alt_user_id,
)
from .utils import _get_status, _get_status_and_error_code


@pytest.mark.user_policy
@pytest.mark.iam_tenant
def test_put_user_policy():
    client = get_iam_client()

    policy_document = json.dumps(
        {"Version": "2012-10-17",
         "Statement": {
             "Effect": "Allow",
             "Action": "*",
             "Resource": "*"}}
    )
    response = client.put_user_policy(PolicyDocument=policy_document, PolicyName='AllAccessPolicy',
                                      UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    response = client.delete_user_policy(PolicyName='AllAccessPolicy',
                                         UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200


@pytest.mark.user_policy
@pytest.mark.iam_tenant
def test_put_user_policy_invalid_user():
    client = get_iam_client()

    policy_document = json.dumps(
        {"Version": "2012-10-17",
         "Statement": {
             "Effect": "Allow",
             "Action": "*",
             "Resource": "*"}}
    )
    e = assert_raises(ClientError, client.put_user_policy, PolicyDocument=policy_document,
                      PolicyName='AllAccessPolicy', UserName="some-non-existing-user-id")
    status = _get_status(e.response)
    assert status == 404


@pytest.mark.user_policy
@pytest.mark.iam_tenant
def test_put_user_policy_parameter_limit():
    client = get_iam_client()

    policy_document = json.dumps(
        {"Version": "2012-10-17",
         "Statement": [{
             "Effect": "Allow",
             "Action": "*",
             "Resource": "*"}] * 1000
         }
    )
    e = assert_raises(ClientError, client.put_user_policy, PolicyDocument=policy_document,
                      PolicyName='AllAccessPolicy' * 10, UserName=get_alt_user_id())
    status = _get_status(e.response)
    assert status == 400


@pytest.mark.user_policy
@pytest.mark.iam_tenant
@pytest.mark.fails_on_rgw
def test_put_user_policy_invalid_element():
    client = get_iam_client()

    # With Version other than 2012-10-17
    policy_document = json.dumps(
        {"Version": "2010-10-17",
         "Statement": [{
             "Effect": "Allow",
             "Action": "*",
             "Resource": "*"}]
         }
    )
    e = assert_raises(ClientError, client.put_user_policy, PolicyDocument=policy_document,
                      PolicyName='AllAccessPolicy', UserName=get_alt_user_id())
    status = _get_status(e.response)
    assert status == 400

    # With no Statement
    policy_document = json.dumps(
        {
            "Version": "2012-10-17",
        }
    )
    e = assert_raises(ClientError, client.put_user_policy, PolicyDocument=policy_document,
                      PolicyName='AllAccessPolicy', UserName=get_alt_user_id())
    status = _get_status(e.response)
    assert status == 400

    # with same Sid for 2 statements
    policy_document = json.dumps(
        {"Version": "2012-10-17",
         "Statement": [
             {"Sid": "98AB54CF",
              "Effect": "Allow",
              "Action": "*",
              "Resource": "*"},
             {"Sid": "98AB54CF",
              "Effect": "Allow",
              "Action": "*",
              "Resource": "*"}]
         }
    )
    e = assert_raises(ClientError, client.put_user_policy, PolicyDocument=policy_document,
                      PolicyName='AllAccessPolicy', UserName=get_alt_user_id())
    status = _get_status(e.response)
    assert status == 400

    # with Principal
    policy_document = json.dumps(
        {"Version": "2012-10-17",
         "Statement": [{
             "Effect": "Allow",
             "Action": "*",
             "Resource": "*",
             "Principal": "arn:aws:iam:::username"}]
         }
    )
    e = assert_raises(ClientError, client.put_user_policy, PolicyDocument=policy_document,
                      PolicyName='AllAccessPolicy', UserName=get_alt_user_id())
    status = _get_status(e.response)
    assert status == 400


@pytest.mark.user_policy
@pytest.mark.iam_tenant
def test_put_existing_user_policy():
    client = get_iam_client()

    policy_document = json.dumps(
        {"Version": "2012-10-17",
         "Statement": {
             "Effect": "Allow",
             "Action": "*",
             "Resource": "*"}
         }
    )
    response = client.put_user_policy(PolicyDocument=policy_document, PolicyName='AllAccessPolicy',
                                      UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    client.put_user_policy(PolicyDocument=policy_document, PolicyName='AllAccessPolicy',
                           UserName=get_alt_user_id())
    client.delete_user_policy(PolicyName='AllAccessPolicy', UserName=get_alt_user_id())


@pytest.mark.user_policy
@pytest.mark.iam_tenant
def test_list_user_policy():
    client = get_iam_client()

    policy_document = json.dumps(
        {"Version": "2012-10-17",
         "Statement": {
             "Effect": "Allow",
             "Action": "*",
             "Resource": "*"}
         }
    )
    response = client.put_user_policy(PolicyDocument=policy_document, PolicyName='AllAccessPolicy',
                                      UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    response = client.list_user_policies(UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    client.delete_user_policy(PolicyName='AllAccessPolicy', UserName=get_alt_user_id())


@pytest.mark.user_policy
@pytest.mark.iam_tenant
def test_list_user_policy_invalid_user():
    client = get_iam_client()
    e = assert_raises(ClientError, client.list_user_policies, UserName="some-non-existing-user-id")
    status = _get_status(e.response)
    assert status == 404


@pytest.mark.user_policy
@pytest.mark.iam_tenant
def test_get_user_policy():
    client = get_iam_client()

    policy_document = json.dumps(
        {"Version": "2012-10-17",
         "Statement": {
             "Effect": "Allow",
             "Action": "*",
             "Resource": "*"}}
    )
    response = client.put_user_policy(PolicyDocument=policy_document, PolicyName='AllAccessPolicy',
                                      UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    response = client.get_user_policy(PolicyName='AllAccessPolicy', UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    response = client.delete_user_policy(PolicyName='AllAccessPolicy',
                                         UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200


@pytest.mark.user_policy
@pytest.mark.iam_tenant
def test_get_user_policy_invalid_user():
    client = get_iam_client()

    policy_document = json.dumps(
        {"Version": "2012-10-17",
         "Statement": {
             "Effect": "Allow",
             "Action": "*",
             "Resource": "*"}}
    )
    response = client.put_user_policy(PolicyDocument=policy_document, PolicyName='AllAccessPolicy',
                                      UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    e = assert_raises(ClientError, client.get_user_policy, PolicyName='AllAccessPolicy',
                      UserName="some-non-existing-user-id")
    status = _get_status(e.response)
    assert status == 404
    client.delete_user_policy(PolicyName='AllAccessPolicy', UserName=get_alt_user_id())


@pytest.mark.user_policy
@pytest.mark.iam_tenant
@pytest.mark.fails_on_rgw
def test_get_user_policy_invalid_policy_name():
    client = get_iam_client()

    policy_document = json.dumps(
        {"Version": "2012-10-17",
         "Statement": {
             "Effect": "Allow",
             "Action": "*",
             "Resource": "*"}}
    )
    client.put_user_policy(PolicyDocument=policy_document, PolicyName='AllAccessPolicy',
                           UserName=get_alt_user_id())
    e = assert_raises(ClientError, client.get_user_policy, PolicyName='non-existing-policy-name',
                      UserName=get_alt_user_id())
    status = _get_status(e.response)
    assert status == 404
    client.delete_user_policy(PolicyName='AllAccessPolicy', UserName=get_alt_user_id())


@pytest.mark.user_policy
@pytest.mark.iam_tenant
@pytest.mark.fails_on_rgw
def test_get_deleted_user_policy():
    client = get_iam_client()

    policy_document = json.dumps(
        {"Version": "2012-10-17",
         "Statement": {
             "Effect": "Allow",
             "Action": "*",
             "Resource": "*"}}
    )
    client.put_user_policy(PolicyDocument=policy_document, PolicyName='AllAccessPolicy',
                           UserName=get_alt_user_id())
    client.delete_user_policy(PolicyName='AllAccessPolicy', UserName=get_alt_user_id())
    e = assert_raises(ClientError, client.get_user_policy, PolicyName='AllAccessPolicy',
                      UserName=get_alt_user_id())
    status = _get_status(e.response)
    assert status == 404


@pytest.mark.user_policy
@pytest.mark.iam_tenant
def test_get_user_policy_from_multiple_policies():
    client = get_iam_client()

    policy_document_allow = json.dumps(
        {"Version": "2012-10-17",
         "Statement": {
             "Effect": "Allow",
             "Action": "*",
             "Resource": "*"}}
    )

    response = client.put_user_policy(PolicyDocument=policy_document_allow,
                                      PolicyName='AllowAccessPolicy1',
                                      UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    response = client.put_user_policy(PolicyDocument=policy_document_allow,
                                      PolicyName='AllowAccessPolicy2',
                                      UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    response = client.get_user_policy(PolicyName='AllowAccessPolicy2',
                                      UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    response = client.delete_user_policy(PolicyName='AllowAccessPolicy1',
                                         UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    response = client.delete_user_policy(PolicyName='AllowAccessPolicy2',
                                         UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200


@pytest.mark.user_policy
@pytest.mark.iam_tenant
def test_delete_user_policy():
    client = get_iam_client()

    policy_document_allow = json.dumps(
        {"Version": "2012-10-17",
         "Statement": {
             "Effect": "Allow",
             "Action": "*",
             "Resource": "*"}}
    )

    response = client.put_user_policy(PolicyDocument=policy_document_allow,
                                      PolicyName='AllowAccessPolicy',
                                      UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    response = client.delete_user_policy(PolicyName='AllowAccessPolicy',
                                         UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200


@pytest.mark.user_policy
@pytest.mark.iam_tenant
def test_delete_user_policy_invalid_user():
    client = get_iam_client()

    policy_document_allow = json.dumps(
        {"Version": "2012-10-17",
         "Statement": {
             "Effect": "Allow",
             "Action": "*",
             "Resource": "*"}}
    )

    response = client.put_user_policy(PolicyDocument=policy_document_allow,
                                      PolicyName='AllowAccessPolicy',
                                      UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    e = assert_raises(ClientError, client.delete_user_policy, PolicyName='AllAccessPolicy',
                      UserName="some-non-existing-user-id")
    status = _get_status(e.response)
    assert status == 404
    response = client.delete_user_policy(PolicyName='AllowAccessPolicy',
                                         UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200


@pytest.mark.user_policy
@pytest.mark.iam_tenant
def test_delete_user_policy_invalid_policy_name():
    client = get_iam_client()

    policy_document_allow = json.dumps(
        {"Version": "2012-10-17",
         "Statement": {
             "Effect": "Allow",
             "Action": "*",
             "Resource": "*"}}
    )

    response = client.put_user_policy(PolicyDocument=policy_document_allow,
                                      PolicyName='AllowAccessPolicy',
                                      UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    e = assert_raises(ClientError, client.delete_user_policy, PolicyName='non-existing-policy-name',
                      UserName=get_alt_user_id())
    status = _get_status(e.response)
    assert status == 404
    response = client.delete_user_policy(PolicyName='AllowAccessPolicy',
                                         UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200


@pytest.mark.user_policy
@pytest.mark.iam_tenant
def test_delete_user_policy_from_multiple_policies():
    client = get_iam_client()

    policy_document_allow = json.dumps(
        {"Version": "2012-10-17",
         "Statement": {
             "Effect": "Allow",
             "Action": "*",
             "Resource": "*"}}
    )

    response = client.put_user_policy(PolicyDocument=policy_document_allow,
                                      PolicyName='AllowAccessPolicy1',
                                      UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    response = client.put_user_policy(PolicyDocument=policy_document_allow,
                                      PolicyName='AllowAccessPolicy2',
                                      UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    response = client.put_user_policy(PolicyDocument=policy_document_allow,
                                      PolicyName='AllowAccessPolicy3',
                                      UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    response = client.delete_user_policy(PolicyName='AllowAccessPolicy1',
                                         UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    response = client.delete_user_policy(PolicyName='AllowAccessPolicy2',
                                         UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    response = client.get_user_policy(PolicyName='AllowAccessPolicy3',
                                      UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    response = client.delete_user_policy(PolicyName='AllowAccessPolicy3',
                                         UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200


@pytest.mark.user_policy
@pytest.mark.iam_tenant
def test_allow_bucket_actions_in_user_policy():
    client = get_iam_client()
    s3_client_alt = get_alt_client()

    s3_client_iam = get_iam_s3client()
    bucket = get_new_bucket(client=s3_client_iam)
    s3_client_iam.put_object(Bucket=bucket, Key='foo', Body='bar')

    policy_document_allow = json.dumps(
        {"Version": "2012-10-17",
         "Statement": {
             "Effect": "Allow",
             "Action": ["s3:ListBucket", "s3:DeleteBucket"],
             "Resource": f"arn:aws:s3:::{bucket}"}}
    )

    response = client.put_user_policy(PolicyDocument=policy_document_allow,
                                      PolicyName='AllowAccessPolicy', UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    response = s3_client_alt.list_objects(Bucket=bucket)
    object_found = False
    for object_received in response['Contents']:
        if "foo" == object_received['Key']:
            object_found = True
            break
    if not object_found:
        raise AssertionError("Object is not listed")

    response = s3_client_iam.delete_object(Bucket=bucket, Key='foo')
    assert response['ResponseMetadata']['HTTPStatusCode'] == 204

    response = s3_client_alt.delete_bucket(Bucket=bucket)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 204

    response = s3_client_iam.list_buckets()
    for bucket in response['Buckets']:
        if bucket == bucket['Name']:
            raise AssertionError("deleted bucket is getting listed")

    response = client.delete_user_policy(PolicyName='AllowAccessPolicy',
                                         UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200


@pytest.mark.user_policy
@pytest.mark.iam_tenant
@pytest.mark.fails_on_dbstore
def test_deny_bucket_actions_in_user_policy():
    client = get_iam_client()
    s3_client = get_alt_client()
    bucket = get_new_bucket(client=s3_client)

    policy_document_deny = json.dumps(
        {"Version": "2012-10-17",
         "Statement": {
             "Effect": "Deny",
             "Action": ["s3:ListAllMyBuckets", "s3:DeleteBucket"],
             "Resource": "arn:aws:s3:::*"}}
    )

    response = client.put_user_policy(PolicyDocument=policy_document_deny,
                                      PolicyName='DenyAccessPolicy',
                                      UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    e = assert_raises(ClientError, s3_client.list_buckets, Bucket=bucket)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == 'AccessDenied'
    e = assert_raises(ClientError, s3_client.delete_bucket, Bucket=bucket)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == 'AccessDenied'
    response = client.delete_user_policy(PolicyName='DenyAccessPolicy',
                                         UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    response = s3_client.delete_bucket(Bucket=bucket)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 204


@pytest.mark.user_policy
@pytest.mark.iam_tenant
def test_allow_object_actions_in_user_policy():
    client = get_iam_client()
    s3_client_alt = get_alt_client()
    s3_client_iam = get_iam_s3client()
    bucket = get_new_bucket(client=s3_client_iam)

    policy_document_allow = json.dumps(
        {"Version": "2012-10-17",
         "Statement": {
             "Effect": "Allow",
             "Action": ["s3:PutObject", "s3:GetObject", "s3:DeleteObject"],
             "Resource": f"arn:aws:s3:::{bucket}/*"}}
    )
    response = client.put_user_policy(PolicyDocument=policy_document_allow,
                                      PolicyName='AllowAccessPolicy', UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    s3_client_alt.put_object(Bucket=bucket, Key='foo', Body='bar')
    response = s3_client_alt.get_object(Bucket=bucket, Key='foo')
    body = response['Body'].read()
    if type(body) is bytes:
        body = body.decode()
    assert body == "bar"
    response = s3_client_alt.delete_object(Bucket=bucket, Key='foo')
    assert response['ResponseMetadata']['HTTPStatusCode'] == 204

    e = assert_raises(ClientError, s3_client_iam.get_object, Bucket=bucket, Key='foo')
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == 'NoSuchKey'
    response = s3_client_iam.delete_bucket(Bucket=bucket)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 204
    response = client.delete_user_policy(PolicyName='AllowAccessPolicy',
                                         UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200


@pytest.mark.user_policy
@pytest.mark.iam_tenant
@pytest.mark.fails_on_dbstore
def test_deny_object_actions_in_user_policy():
    client = get_iam_client()
    s3_client_alt = get_alt_client()
    bucket = get_new_bucket(client=s3_client_alt)
    s3_client_alt.put_object(Bucket=bucket, Key='foo', Body='bar')

    policy_document_deny = json.dumps(
        {"Version": "2012-10-17",
         "Statement": [{
             "Effect": "Deny",
             "Action": ["s3:PutObject", "s3:GetObject", "s3:DeleteObject"],
             "Resource": f"arn:aws:s3:::{bucket}/*"}, {
             "Effect": "Allow",
             "Action": ["s3:DeleteBucket"],
             "Resource": f"arn:aws:s3:::{bucket}"}]}
    )
    client.put_user_policy(PolicyDocument=policy_document_deny, PolicyName='DenyAccessPolicy',
                           UserName=get_alt_user_id())

    e = assert_raises(ClientError, s3_client_alt.put_object, Bucket=bucket, Key='foo')
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == 'AccessDenied'
    e = assert_raises(ClientError, s3_client_alt.get_object, Bucket=bucket, Key='foo')
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == 'AccessDenied'
    e = assert_raises(ClientError, s3_client_alt.delete_object, Bucket=bucket, Key='foo')
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == 'AccessDenied'

    response = client.delete_user_policy(PolicyName='DenyAccessPolicy',
                                         UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200


@pytest.mark.user_policy
@pytest.mark.iam_tenant
def test_allow_multipart_actions_in_user_policy():
    client = get_iam_client()
    s3_client_alt = get_alt_client()
    s3_client_iam = get_iam_s3client()
    bucket = get_new_bucket(client=s3_client_iam)

    policy_document_allow = json.dumps(
        {"Version": "2012-10-17",
         "Statement": {
             "Effect": "Allow",
             "Action": ["s3:ListBucketMultipartUploads", "s3:AbortMultipartUpload"],
             "Resource": "arn:aws:s3:::*"}}
    )
    response = client.put_user_policy(PolicyDocument=policy_document_allow,
                                      PolicyName='AllowAccessPolicy', UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    key = "mymultipart"
    mb = 1024 * 1024

    (upload_id, _, _) = _multipart_upload(client=s3_client_iam, bucket_name=bucket, key=key,
                                          size=5 * mb)
    response = s3_client_alt.list_multipart_uploads(Bucket=bucket)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    response = s3_client_alt.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 204

    response = s3_client_iam.delete_bucket(Bucket=bucket)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 204
    response = client.delete_user_policy(PolicyName='AllowAccessPolicy',
                                         UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200


@pytest.mark.user_policy
@pytest.mark.iam_tenant
@pytest.mark.fails_on_dbstore
def test_deny_multipart_actions_in_user_policy():
    client = get_iam_client()
    s3_client = get_alt_client()
    bucket = get_new_bucket(client=s3_client)

    policy_document_deny = json.dumps(
        {"Version": "2012-10-17",
         "Statement": {
             "Effect": "Deny",
             "Action": ["s3:ListBucketMultipartUploads", "s3:AbortMultipartUpload"],
             "Resource": "arn:aws:s3:::*"}}
    )
    response = client.put_user_policy(PolicyDocument=policy_document_deny,
                                      PolicyName='DenyAccessPolicy',
                                      UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    key = "mymultipart"
    mb = 1024 * 1024

    (upload_id, _, _) = _multipart_upload(client=s3_client, bucket_name=bucket, key=key,
                                          size=5 * mb)

    e = assert_raises(ClientError, s3_client.list_multipart_uploads, Bucket=bucket)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == 'AccessDenied'

    e = assert_raises(ClientError, s3_client.abort_multipart_upload, Bucket=bucket,
                      Key=key, UploadId=upload_id)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == 'AccessDenied'

    response = s3_client.delete_bucket(Bucket=bucket)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 204
    response = client.delete_user_policy(PolicyName='DenyAccessPolicy',
                                         UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200


@pytest.mark.user_policy
@pytest.mark.iam_tenant
@pytest.mark.fails_on_dbstore
def test_allow_tagging_actions_in_user_policy():
    client = get_iam_client()
    s3_client_alt = get_alt_client()
    s3_client_iam = get_iam_s3client()
    bucket = get_new_bucket(client=s3_client_iam)

    policy_document_allow = json.dumps(
        {"Version": "2012-10-17",
         "Statement": {
             "Effect": "Allow",
             "Action": ["s3:PutBucketTagging", "s3:GetBucketTagging",
                        "s3:PutObjectTagging", "s3:GetObjectTagging"],
             "Resource": f"arn:aws:s3:::*"}}
    )
    client.put_user_policy(PolicyDocument=policy_document_allow, PolicyName='AllowAccessPolicy',
                           UserName=get_alt_user_id())
    tags = {'TagSet': [{'Key': 'Hello', 'Value': 'World'}, ]}

    response = s3_client_alt.put_bucket_tagging(Bucket=bucket, Tagging=tags)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    response = s3_client_alt.get_bucket_tagging(Bucket=bucket)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    assert response['TagSet'][0]['Key'] == 'Hello'
    assert response['TagSet'][0]['Value'] == 'World'

    obj_key = 'obj'
    response = s3_client_iam.put_object(Bucket=bucket, Key=obj_key, Body='obj_body')
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    response = s3_client_alt.put_object_tagging(Bucket=bucket, Key=obj_key, Tagging=tags)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    response = s3_client_alt.get_object_tagging(Bucket=bucket, Key=obj_key)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    assert response['TagSet'] == tags['TagSet']

    response = s3_client_iam.delete_object(Bucket=bucket, Key=obj_key)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 204
    response = s3_client_iam.delete_bucket(Bucket=bucket)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 204
    response = client.delete_user_policy(PolicyName='AllowAccessPolicy',
                                         UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200


@pytest.mark.user_policy
@pytest.mark.iam_tenant
@pytest.mark.fails_on_dbstore
def test_deny_tagging_actions_in_user_policy():
    client = get_iam_client()
    s3_client = get_alt_client()
    bucket = get_new_bucket(client=s3_client)

    policy_document_deny = json.dumps(
        {"Version": "2012-10-17",
         "Statement": {
             "Effect": "Deny",
             "Action": ["s3:PutBucketTagging", "s3:GetBucketTagging",
                        "s3:PutObjectTagging", "s3:DeleteObjectTagging"],
             "Resource": "arn:aws:s3:::*"}}
    )
    client.put_user_policy(PolicyDocument=policy_document_deny, PolicyName='DenyAccessPolicy',
                           UserName=get_alt_user_id())
    tags = {'TagSet': [{'Key': 'Hello', 'Value': 'World'}, ]}

    e = assert_raises(ClientError, s3_client.put_bucket_tagging, Bucket=bucket, Tagging=tags)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == 'AccessDenied'
    e = assert_raises(ClientError, s3_client.get_bucket_tagging, Bucket=bucket)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == 'AccessDenied'

    obj_key = 'obj'
    response = s3_client.put_object(Bucket=bucket, Key=obj_key, Body='obj_body')
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    e = assert_raises(ClientError, s3_client.put_object_tagging, Bucket=bucket, Key=obj_key,
                      Tagging=tags)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == 'AccessDenied'
    e = assert_raises(ClientError, s3_client.delete_object_tagging, Bucket=bucket, Key=obj_key)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == 'AccessDenied'

    response = s3_client.delete_object(Bucket=bucket, Key=obj_key)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 204
    response = s3_client.delete_bucket(Bucket=bucket)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 204
    response = client.delete_user_policy(PolicyName='DenyAccessPolicy',
                                         UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200


@pytest.mark.user_policy
@pytest.mark.iam_tenant
@pytest.mark.fails_on_dbstore
def test_verify_conflicting_user_policy_statements():
    s3client = get_alt_client()
    bucket = get_new_bucket(client=s3client)
    policy_document = json.dumps(
        {"Version": "2012-10-17",
         "Statement": [
             {"Sid": "98AB54CG",
              "Effect": "Allow",
              "Action": "s3:ListBucket",
              "Resource": f"arn:aws:s3:::{bucket}"},
             {"Sid": "98AB54CA",
              "Effect": "Deny",
              "Action": "s3:ListBucket",
              "Resource": f"arn:aws:s3:::{bucket}"}
         ]}
    )
    client = get_iam_client()
    response = client.put_user_policy(PolicyDocument=policy_document, PolicyName='DenyAccessPolicy',
                                      UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    e = assert_raises(ClientError, s3client.list_objects, Bucket=bucket)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == 'AccessDenied'
    response = client.delete_user_policy(PolicyName='DenyAccessPolicy',
                                         UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200


@pytest.mark.user_policy
@pytest.mark.iam_tenant
@pytest.mark.fails_on_dbstore
def test_verify_conflicting_user_policies():
    s3client = get_alt_client()
    bucket = get_new_bucket(client=s3client)
    policy_allow = json.dumps(
        {"Version": "2012-10-17",
         "Statement": {"Sid": "98AB54CG",
                       "Effect": "Allow",
                       "Action": "s3:ListBucket",
                       "Resource": f"arn:aws:s3:::{bucket}"}}
    )
    policy_deny = json.dumps(
        {"Version": "2012-10-17",
         "Statement": {"Sid": "98AB54CGZ",
                       "Effect": "Deny",
                       "Action": "s3:ListBucket",
                       "Resource": f"arn:aws:s3:::{bucket}"}}
    )
    client = get_iam_client()
    response = client.put_user_policy(PolicyDocument=policy_allow, PolicyName='AllowAccessPolicy',
                                      UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    response = client.put_user_policy(PolicyDocument=policy_deny, PolicyName='DenyAccessPolicy',
                                      UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    e = assert_raises(ClientError, s3client.list_objects, Bucket=bucket)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == 'AccessDenied'
    response = client.delete_user_policy(PolicyName='AllowAccessPolicy',
                                         UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    response = client.delete_user_policy(PolicyName='DenyAccessPolicy',
                                         UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200


@pytest.mark.user_policy
@pytest.mark.iam_tenant
def test_verify_allow_iam_actions():
    policy1 = json.dumps(
        {"Version": "2012-10-17",
         "Statement": {"Sid": "98AB54CGA",
                       "Effect": "Allow",
                       "Action": ["iam:PutUserPolicy", "iam:GetUserPolicy",
                                  "iam:ListUserPolicies", "iam:DeleteUserPolicy"],
                       "Resource": f"arn:aws:iam:::user/{get_alt_user_id()}"}}
    )
    client1 = get_iam_client()
    iam_client_alt = get_alt_iam_client()

    response = client1.put_user_policy(PolicyDocument=policy1, PolicyName='AllowAccessPolicy',
                                       UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    response = iam_client_alt.get_user_policy(PolicyName='AllowAccessPolicy',
                                       UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    response = iam_client_alt.list_user_policies(UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    response = iam_client_alt.delete_user_policy(PolicyName='AllowAccessPolicy',
                                          UserName=get_alt_user_id())
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200


def nuke_user_keys(client, name):
    p = client.get_paginator('list_access_keys')
    for response in p.paginate(UserName=name):
        for key in response['AccessKeyMetadata']:
            try:
                client.delete_access_key(UserName=name, AccessKeyId=key['AccessKeyId'])
            except:
                pass

def nuke_user_policies(client, name):
    p = client.get_paginator('list_user_policies')
    for response in p.paginate(UserName=name):
        for policy in response['PolicyNames']:
            try:
                client.delete_user_policy(UserName=name, PolicyName=policy)
            except:
                pass

def nuke_attached_user_policies(client, name):
    p = client.get_paginator('list_attached_user_policies')
    for response in p.paginate(UserName=name):
        for policy in response['AttachedPolicies']:
            try:
                client.detach_user_policy(UserName=name, PolicyArn=policy['PolicyArn'])
            except:
                pass

def nuke_user(client, name):
    # delete access keys, user policies, etc
    try:
        nuke_user_keys(client, name)
    except:
        pass
    try:
        nuke_user_policies(client, name)
    except:
        pass
    try:
        nuke_attached_user_policies(client, name)
    except:
        pass
    client.delete_user(UserName=name)

def nuke_users(client, **kwargs):
    p = client.get_paginator('list_users')
    for response in p.paginate(**kwargs):
        for user in response['Users']:
            try:
                nuke_user(client, user['UserName'])
            except:
                pass

def nuke_role_policies(client, name):
    p = client.get_paginator('list_role_policies')
    for response in p.paginate(RoleName=name):
        for policy in response['PolicyNames']:
            try:
                client.delete_role_policy(RoleName=name, PolicyName=policy)
            except:
                pass

def nuke_role(client, name):
    # delete role policies, etc
    try:
        nuke_role_policies(client, name)
    except:
        pass
    client.delete_role(RoleName=name)

def nuke_roles(client, **kwargs):
    p = client.get_paginator('list_roles')
    for response in p.paginate(**kwargs):
        for role in response['Roles']:
            try:
                nuke_role(client, role['RoleName'])
            except:
                pass

# fixture for iam account root user
@pytest.fixture
def iam_root(configfile):
    client = get_iam_root_client()
    try:
        arn = client.get_user()['User']['Arn']
        if not arn.endswith(':root'):
            pytest.skip('[iam root] user does not have :root arn')
    except ClientError as e:
        pytest.skip('[iam root] user does not belong to an account')

    yield client
    nuke_users(client, PathPrefix=get_iam_path_prefix())
    nuke_roles(client, PathPrefix=get_iam_path_prefix())


# IAM User apis
@pytest.mark.iam_account
@pytest.mark.iam_user
def test_account_user_create(iam_root):
    path = get_iam_path_prefix()
    name1 = make_iam_name('U1')
    response = iam_root.create_user(UserName=name1, Path=path)
    user = response['User']
    assert user['Path'] == path
    assert user['UserName'] == name1
    assert len(user['UserId'])
    assert user['Arn'].startswith('arn:aws:iam:')
    assert user['Arn'].endswith(f':user{path}{name1}')
    assert user['CreateDate'] > datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)

    path2 = get_iam_path_prefix() + 'foo/'
    with pytest.raises(iam_root.exceptions.EntityAlreadyExistsException):
        iam_root.create_user(UserName=name1, Path=path2)

    name2 = make_iam_name('U2')
    response = iam_root.create_user(UserName=name2, Path=path2)
    user = response['User']
    assert user['Path'] == path2
    assert user['UserName'] == name2

@pytest.mark.iam_account
@pytest.mark.iam_user
def test_account_user_case_insensitive_name(iam_root):
    path = get_iam_path_prefix()
    name_upper = make_iam_name('U1')
    name_lower = make_iam_name('u1')
    response = iam_root.create_user(UserName=name_upper, Path=path)
    user = response['User']

    # name is case-insensitive, so 'u1' should also conflict
    with pytest.raises(iam_root.exceptions.EntityAlreadyExistsException):
        iam_root.create_user(UserName=name_lower)

    # search for 'u1' should return the same 'U1' user
    response = iam_root.get_user(UserName=name_lower)
    assert user == response['User']

    # delete for 'u1' should delete the same 'U1' user
    iam_root.delete_user(UserName=name_lower)

    with pytest.raises(iam_root.exceptions.NoSuchEntityException):
        iam_root.get_user(UserName=name_lower)

@pytest.mark.iam_account
@pytest.mark.iam_user
def test_account_user_delete(iam_root):
    path = get_iam_path_prefix()
    name = make_iam_name('U1')
    with pytest.raises(iam_root.exceptions.NoSuchEntityException):
        iam_root.delete_user(UserName=name)

    response = iam_root.create_user(UserName=name, Path=path)
    uid = response['User']['UserId']
    create_date = response['User']['CreateDate']

    iam_root.delete_user(UserName=name)

    response = iam_root.create_user(UserName=name, Path=path)
    assert uid != response['User']['UserId']
    assert create_date <= response['User']['CreateDate']

def user_list_names(client, **kwargs):
    p = client.get_paginator('list_users')
    usernames = []
    for response in p.paginate(**kwargs):
        usernames += [u['UserName'] for u in response['Users']]
    return usernames

@pytest.mark.iam_account
@pytest.mark.iam_user
def test_account_user_list(iam_root):
    path = get_iam_path_prefix()
    response = iam_root.list_users(PathPrefix=path)
    assert len(response['Users']) == 0
    assert response['IsTruncated'] == False

    name1 = make_iam_name('aa')
    name2 = make_iam_name('Ab')
    name3 = make_iam_name('ac')
    name4 = make_iam_name('Ad')

    # sort order is independent of CreateDate, Path, and UserName capitalization
    iam_root.create_user(UserName=name4, Path=path+'w/')
    iam_root.create_user(UserName=name3, Path=path+'x/')
    iam_root.create_user(UserName=name2, Path=path+'y/')
    iam_root.create_user(UserName=name1, Path=path+'z/')

    assert [name1, name2, name3, name4] == \
            user_list_names(iam_root, PathPrefix=path)
    assert [name1, name2, name3, name4] == \
            user_list_names(iam_root, PathPrefix=path, PaginationConfig={'PageSize': 1})

@pytest.mark.iam_account
@pytest.mark.iam_user
def test_account_user_list_path_prefix(iam_root):
    path = get_iam_path_prefix()
    response = iam_root.list_users(PathPrefix=path)
    assert len(response['Users']) == 0
    assert response['IsTruncated'] == False

    name1 = make_iam_name('a')
    name2 = make_iam_name('b')
    name3 = make_iam_name('c')
    name4 = make_iam_name('d')

    iam_root.create_user(UserName=name1, Path=path)
    iam_root.create_user(UserName=name2, Path=path)
    iam_root.create_user(UserName=name3, Path=path+'a/')
    iam_root.create_user(UserName=name4, Path=path+'a/x/')

    assert [name1, name2, name3, name4] == \
            user_list_names(iam_root, PathPrefix=path)
    assert [name1, name2, name3, name4] == \
            user_list_names(iam_root, PathPrefix=path,
                            PaginationConfig={'PageSize': 1})
    assert [name3, name4] == \
            user_list_names(iam_root, PathPrefix=path+'a')
    assert [name3, name4] == \
            user_list_names(iam_root, PathPrefix=path+'a',
                            PaginationConfig={'PageSize': 1})
    assert [name4] == \
            user_list_names(iam_root, PathPrefix=path+'a/x')
    assert [name4] == \
            user_list_names(iam_root, PathPrefix=path+'a/x',
                            PaginationConfig={'PageSize': 1})
    assert [] == user_list_names(iam_root, PathPrefix=path+'a/x/d')

@pytest.mark.iam_account
@pytest.mark.iam_user
def test_account_user_update_name(iam_root):
    path = get_iam_path_prefix()
    name1 = make_iam_name('a')
    new_name1 = make_iam_name('z')
    name2 = make_iam_name('b')
    with pytest.raises(iam_root.exceptions.NoSuchEntityException):
        iam_root.update_user(UserName=name1, NewUserName=new_name1)

    iam_root.create_user(UserName=name1, Path=path)
    iam_root.create_user(UserName=name2, Path=path+'m/')
    assert [name1, name2] == user_list_names(iam_root, PathPrefix=path)

    response = iam_root.get_user(UserName=name1)
    assert name1 == response['User']['UserName']
    uid = response['User']['UserId']

    iam_root.update_user(UserName=name1, NewUserName=new_name1)

    with pytest.raises(iam_root.exceptions.NoSuchEntityException):
        iam_root.get_user(UserName=name1)

    response = iam_root.get_user(UserName=new_name1)
    assert new_name1 == response['User']['UserName']
    assert uid == response['User']['UserId']
    assert response['User']['Arn'].endswith(f':user{path}{new_name1}')

    assert [name2, new_name1] == user_list_names(iam_root, PathPrefix=path)

@pytest.mark.iam_account
@pytest.mark.iam_user
def test_account_user_update_path(iam_root):
    path = get_iam_path_prefix()
    name1 = make_iam_name('a')
    name2 = make_iam_name('b')
    iam_root.create_user(UserName=name1, Path=path)
    iam_root.create_user(UserName=name2, Path=path+'m/')
    assert [name1, name2] == user_list_names(iam_root, PathPrefix=path)

    response = iam_root.get_user(UserName=name1)
    assert name1 == response['User']['UserName']
    assert path == response['User']['Path']
    uid = response['User']['UserId']

    iam_root.update_user(UserName=name1, NewPath=path+'z/')

    response = iam_root.get_user(UserName=name1)
    assert name1 == response['User']['UserName']
    assert f'{path}z/' == response['User']['Path']
    assert uid == response['User']['UserId']
    assert response['User']['Arn'].endswith(f':user{path}z/{name1}')

    assert [name1, name2] == user_list_names(iam_root, PathPrefix=path)


# IAM AccessKey apis
@pytest.mark.iam_account
@pytest.mark.iam_user
def test_account_user_access_key_create(iam_root):
    path = get_iam_path_prefix()
    name = make_iam_name('a')
    with pytest.raises(iam_root.exceptions.NoSuchEntityException):
        iam_root.create_access_key(UserName=name)

    iam_root.create_user(UserName=name, Path=path)

    response = iam_root.create_access_key(UserName=name)
    key = response['AccessKey']
    assert name == key['UserName']
    assert len(key['AccessKeyId'])
    assert len(key['SecretAccessKey'])
    assert 'Active' == key['Status']
    assert key['CreateDate'] > datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)

@pytest.mark.iam_account
@pytest.mark.iam_user
def test_account_current_user_access_key_create(iam_root):
    # omit the UserName argument to operate on the current authenticated
    # user (assumed to be an account root user)

    response = iam_root.create_access_key()
    key = response['AccessKey']
    keyid = key['AccessKeyId']
    assert len(keyid)
    try:
        assert len(key['SecretAccessKey'])
        assert 'Active' == key['Status']
        assert key['CreateDate'] > datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
    finally:
        # iam_root doesn't see the account root user, so clean up
        # this key manually
        iam_root.delete_access_key(AccessKeyId=keyid)

@pytest.mark.iam_account
@pytest.mark.iam_user
def test_account_user_access_key_update(iam_root):
    with pytest.raises(iam_root.exceptions.NoSuchEntityException):
        iam_root.update_access_key(UserName='nosuchuser', AccessKeyId='abcdefghijklmnopqrstu', Status='Active')

    path = get_iam_path_prefix()
    name = make_iam_name('a')
    iam_root.create_user(UserName=name, Path=path)

    response = iam_root.create_access_key(UserName=name)
    key = response['AccessKey']
    keyid = key['AccessKeyId']
    create_date = key['CreateDate']
    assert create_date > datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)

    with pytest.raises(iam_root.exceptions.NoSuchEntityException):
        iam_root.update_access_key(UserName=name, AccessKeyId='abcdefghijklmnopqrstu', Status='Active')

    iam_root.update_access_key(UserName=name, AccessKeyId=keyid, Status='Active')
    iam_root.update_access_key(UserName=name, AccessKeyId=keyid, Status='Inactive')

    response = iam_root.list_access_keys(UserName=name)
    keys = response['AccessKeyMetadata']
    assert 1 == len(keys)
    key = keys[0]
    assert name == key['UserName']
    assert keyid == key['AccessKeyId']
    assert 'Inactive' == key['Status']
    assert create_date == key['CreateDate'] # CreateDate unchanged by update_access_key()

@pytest.mark.iam_account
@pytest.mark.iam_user
def test_account_current_user_access_key_update(iam_root):
    # omit the UserName argument to operate on the current authenticated
    # user (assumed to be an account root user)

    with pytest.raises(iam_root.exceptions.NoSuchEntityException):
        iam_root.update_access_key(AccessKeyId='abcdefghijklmnopqrstu', Status='Active')

    response = iam_root.create_access_key()
    key = response['AccessKey']
    keyid = key['AccessKeyId']
    assert len(keyid)
    try:
        iam_root.update_access_key(AccessKeyId=keyid, Status='Active')
        iam_root.update_access_key(AccessKeyId=keyid, Status='Inactive')

        # find the access key id we created
        p = iam_root.get_paginator('list_access_keys')
        for response in p.paginate():
            for key in response['AccessKeyMetadata']:
                if keyid == key['AccessKeyId']:
                    assert 'Inactive' == key['Status']
                    return
        assert False, f'AccessKeyId={keyid} not found in list_access_keys()'

    finally:
        # iam_root doesn't see the account root user, so clean up
        # this key manually
        iam_root.delete_access_key(AccessKeyId=keyid)

@pytest.mark.iam_account
@pytest.mark.iam_user
def test_account_user_access_key_delete(iam_root):
    with pytest.raises(iam_root.exceptions.NoSuchEntityException):
        iam_root.delete_access_key(UserName='nosuchuser', AccessKeyId='abcdefghijklmnopqrstu')

    path = get_iam_path_prefix()
    name = make_iam_name('a')
    iam_root.create_user(UserName=name, Path=path)

    with pytest.raises(iam_root.exceptions.NoSuchEntityException):
        iam_root.delete_access_key(UserName=name, AccessKeyId='abcdefghijklmnopqrstu')

    response = iam_root.create_access_key(UserName=name)
    keyid = response['AccessKey']['AccessKeyId']

    iam_root.delete_access_key(UserName=name, AccessKeyId=keyid)

    with pytest.raises(iam_root.exceptions.NoSuchEntityException):
        iam_root.delete_access_key(UserName=name, AccessKeyId=keyid)

    response = iam_root.list_access_keys(UserName=name)
    keys = response['AccessKeyMetadata']
    assert 0 == len(keys)

@pytest.mark.iam_account
@pytest.mark.iam_user
def test_account_current_user_access_key_delete(iam_root):
    # omit the UserName argument to operate on the current authenticated
    # user (assumed to be an account root user)

    with pytest.raises(iam_root.exceptions.NoSuchEntityException):
        iam_root.delete_access_key(AccessKeyId='abcdefghijklmnopqrstu')

    response = iam_root.create_access_key()
    keyid = response['AccessKey']['AccessKeyId']

    iam_root.delete_access_key(AccessKeyId=keyid)

    with pytest.raises(iam_root.exceptions.NoSuchEntityException):
        iam_root.delete_access_key(AccessKeyId=keyid)

    # make sure list_access_keys() doesn't return the access key id we deleted
    p = iam_root.get_paginator('list_access_keys')
    for response in p.paginate():
        for key in response['AccessKeyMetadata']:
            assert keyid != key['AccessKeyId']

def user_list_key_ids(client, **kwargs):
    p = client.get_paginator('list_access_keys')
    ids = []
    for response in p.paginate(**kwargs):
        ids += [k['AccessKeyId'] for k in response['AccessKeyMetadata']]
    return ids

@pytest.mark.iam_account
@pytest.mark.iam_user
def test_account_user_access_key_list(iam_root):
    with pytest.raises(iam_root.exceptions.NoSuchEntityException):
        iam_root.list_access_keys(UserName='nosuchuser')

    path = get_iam_path_prefix()
    name = make_iam_name('a')
    iam_root.create_user(UserName=name, Path=path)

    assert [] == user_list_key_ids(iam_root, UserName=name)
    assert [] == user_list_key_ids(iam_root, UserName=name, PaginationConfig={'PageSize': 1})

    id1 = iam_root.create_access_key(UserName=name)['AccessKey']['AccessKeyId']

    assert [id1] == user_list_key_ids(iam_root, UserName=name)
    assert [id1] == user_list_key_ids(iam_root, UserName=name, PaginationConfig={'PageSize': 1})

    id2 = iam_root.create_access_key(UserName=name)['AccessKey']['AccessKeyId']
    # AccessKeysPerUser=2 is the default quota in aws

    keys = sorted([id1, id2])
    assert keys == sorted(user_list_key_ids(iam_root, UserName=name))
    assert keys == sorted(user_list_key_ids(iam_root, UserName=name, PaginationConfig={'PageSize': 1}))

def retry_on(code, tries, func, *args, **kwargs):
    for i in range(tries):
        try:
            return func(*args, **kwargs)
        except ClientError as e:
            err = e.response['Error']['Code']
            if i + 1 < tries and err in code:
                print(f'Got {err}, retrying in {i}s..')
                time.sleep(i)
                continue
            raise


@pytest.mark.iam_account
@pytest.mark.iam_user
def test_account_user_bucket_policy_allow(iam_root):
    path = get_iam_path_prefix()
    name = make_iam_name('name')
    response = iam_root.create_user(UserName=name, Path=path)
    user_arn = response['User']['Arn']
    assert user_arn.startswith('arn:aws:iam:')
    assert user_arn.endswith(f':user{path}{name}')

    key = iam_root.create_access_key(UserName=name)['AccessKey']
    client = get_iam_s3client(aws_access_key_id=key['AccessKeyId'],
                              aws_secret_access_key=key['SecretAccessKey'])

    # create a bucket with the root user
    roots3 = get_iam_root_client(service_name='s3')
    bucket = get_new_bucket(roots3)
    try:
        # the access key may take a bit to start working. retry until it returns
        # something other than InvalidAccessKeyId
        e = assert_raises(ClientError, retry_on, 'InvalidAccessKeyId', 10, client.list_objects, Bucket=bucket)
        # expect AccessDenied because no identity policy allows s3 actions
        status, error_code = _get_status_and_error_code(e.response)
        assert status == 403
        assert error_code == 'AccessDenied'

        # add a bucket policy that allows s3:ListBucket for the iam user's arn
        policy = json.dumps({
            'Version': '2012-10-17',
            'Statement': [{
                'Effect': 'Allow',
                'Principal': {'AWS': user_arn},
                'Action': 's3:ListBucket',
                'Resource': f'arn:aws:s3:::{bucket}'
                }]
            })
        roots3.put_bucket_policy(Bucket=bucket, Policy=policy)

        # verify that the iam user can eventually access it
        retry_on('AccessDenied', 10, client.list_objects, Bucket=bucket)
    finally:
        roots3.delete_bucket(Bucket=bucket)


# IAM UserPolicy apis
@pytest.mark.user_policy
@pytest.mark.iam_account
def test_account_user_policy(iam_root):
    path = get_iam_path_prefix()
    name = make_iam_name('name')
    policy_name = 'List'
    bucket_name = get_new_bucket_name()
    policy1 = json.dumps({'Version': '2012-10-17', 'Statement': [
        {'Effect': 'Deny',
         'Action': 's3:ListBucket',
         'Resource': f'arn:aws:s3:::{bucket_name}'}]})
    policy2 = json.dumps({'Version': '2012-10-17', 'Statement': [
        {'Effect': 'Allow',
         'Action': 's3:ListBucket',
         'Resource': f'arn:aws:s3:::{bucket_name}'}]})

    # Get/Put/Delete fail on nonexistent UserName
    with pytest.raises(iam_root.exceptions.NoSuchEntityException):
        iam_root.get_user_policy(UserName=name, PolicyName=policy_name)
    with pytest.raises(iam_root.exceptions.NoSuchEntityException):
        iam_root.delete_user_policy(UserName=name, PolicyName=policy_name)
    with pytest.raises(iam_root.exceptions.NoSuchEntityException):
        iam_root.put_user_policy(UserName=name, PolicyName=policy_name, PolicyDocument=policy1)

    iam_root.create_user(UserName=name, Path=path)

    # Get/Delete fail on nonexistent PolicyName
    with pytest.raises(iam_root.exceptions.NoSuchEntityException):
        iam_root.get_user_policy(UserName=name, PolicyName=policy_name)
    with pytest.raises(iam_root.exceptions.NoSuchEntityException):
        iam_root.delete_user_policy(UserName=name, PolicyName=policy_name)

    iam_root.put_user_policy(UserName=name, PolicyName=policy_name, PolicyDocument=policy1)

    response = iam_root.get_user_policy(UserName=name, PolicyName=policy_name)
    assert policy1 == json.dumps(response['PolicyDocument'])
    response = iam_root.list_user_policies(UserName=name)
    assert [policy_name] == response['PolicyNames']

    iam_root.put_user_policy(UserName=name, PolicyName=policy_name, PolicyDocument=policy2)

    response = iam_root.get_user_policy(UserName=name, PolicyName=policy_name)
    assert policy2 == json.dumps(response['PolicyDocument'])
    response = iam_root.list_user_policies(UserName=name)
    assert [policy_name] == response['PolicyNames']

    iam_root.delete_user_policy(UserName=name, PolicyName=policy_name)

    # Get/Delete fail after Delete
    with pytest.raises(iam_root.exceptions.NoSuchEntityException):
        iam_root.get_user_policy(UserName=name, PolicyName=policy_name)
    with pytest.raises(iam_root.exceptions.NoSuchEntityException):
        iam_root.delete_user_policy(UserName=name, PolicyName=policy_name)

    response = iam_root.list_user_policies(UserName=name)
    assert [] == response['PolicyNames']

@pytest.mark.user_policy
@pytest.mark.iam_account
def test_account_user_policy_managed(iam_root):
    path = get_iam_path_prefix()
    name = make_iam_name('name')
    policy1 = 'arn:aws:iam::aws:policy/AmazonS3FullAccess'
    policy2 = 'arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'

    # Attach/Detach/List fail on nonexistent UserName
    with pytest.raises(iam_root.exceptions.NoSuchEntityException):
        iam_root.attach_user_policy(UserName=name, PolicyArn=policy1)
    with pytest.raises(iam_root.exceptions.NoSuchEntityException):
        iam_root.detach_user_policy(UserName=name, PolicyArn=policy1)
    with pytest.raises(iam_root.exceptions.NoSuchEntityException):
        iam_root.list_attached_user_policies(UserName=name)

    iam_root.create_user(UserName=name, Path=path)

    # Detach fails on unattached PolicyArn
    with pytest.raises(iam_root.exceptions.NoSuchEntityException):
        iam_root.detach_user_policy(UserName=name, PolicyArn=policy1)

    iam_root.attach_user_policy(UserName=name, PolicyArn=policy1)
    iam_root.attach_user_policy(UserName=name, PolicyArn=policy1)

    response = iam_root.list_attached_user_policies(UserName=name)
    assert len(response['AttachedPolicies']) == 1
    assert 'AmazonS3FullAccess' == response['AttachedPolicies'][0]['PolicyName']
    assert policy1 == response['AttachedPolicies'][0]['PolicyArn']

    iam_root.attach_user_policy(UserName=name, PolicyArn=policy2)

    response = iam_root.list_attached_user_policies(UserName=name)
    policies = response['AttachedPolicies']
    assert len(policies) == 2
    names = [p['PolicyName'] for p in policies]
    arns = [p['PolicyArn'] for p in policies]
    assert 'AmazonS3FullAccess' in names
    assert policy1 in arns
    assert 'AmazonS3ReadOnlyAccess' in names
    assert policy2 in arns

    iam_root.detach_user_policy(UserName=name, PolicyArn=policy2)

    # Detach fails after Detach
    with pytest.raises(iam_root.exceptions.NoSuchEntityException):
        iam_root.detach_user_policy(UserName=name, PolicyArn=policy2)

    response = iam_root.list_attached_user_policies(UserName=name)
    assert len(response['AttachedPolicies']) == 1
    assert 'AmazonS3FullAccess' == response['AttachedPolicies'][0]['PolicyName']
    assert policy1 == response['AttachedPolicies'][0]['PolicyArn']

    # DeleteUser fails while policies are still attached
    with pytest.raises(iam_root.exceptions.DeleteConflictException):
        iam_root.delete_user(UserName=name)

@pytest.mark.user_policy
@pytest.mark.iam_account
def test_account_user_policy_allow(iam_root):
    path = get_iam_path_prefix()
    name = make_iam_name('name')
    bucket_name = get_new_bucket_name()
    iam_root.create_user(UserName=name, Path=path)

    key = iam_root.create_access_key(UserName=name)['AccessKey']
    client = get_iam_s3client(aws_access_key_id=key['AccessKeyId'],
                              aws_secret_access_key=key['SecretAccessKey'])

    # the access key may take a bit to start working. retry until it returns
    # something other than InvalidAccessKeyId
    e = assert_raises(ClientError, retry_on, 'InvalidAccessKeyId', 10, client.list_buckets)
    # expect AccessDenied because no identity policy allows s3 actions
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == 'AccessDenied'

    # add a user policy that allows s3 actions
    policy = json.dumps({
        'Version': '2012-10-17',
        'Statement': [{
            'Effect': 'Allow',
            'Action': 's3:*',
            'Resource': '*'
            }]
        })
    policy_name = 'AllowStar'
    iam_root.put_user_policy(UserName=name, PolicyName=policy_name, PolicyDocument=policy)

    # the policy may take a bit to start working. retry until it returns
    # something other than AccessDenied
    retry_on('AccessDenied', 10, client.list_buckets)


assume_role_policy = json.dumps({
    'Version': '2012-10-17',
    'Statement': [{
        'Effect': 'Allow',
        'Action': 'sts:AssumeRole',
        'Principal': {'AWS': '*'}
        }]
    })

# IAM Role apis
@pytest.mark.iam_account
@pytest.mark.iam_role
def test_account_role_create(iam_root):
    path = get_iam_path_prefix()
    name1 = make_iam_name('R1')
    desc = 'my role description'
    max_duration = 43200
    response = iam_root.create_role(RoleName=name1, Path=path, AssumeRolePolicyDocument=assume_role_policy, Description=desc, MaxSessionDuration=max_duration)
    role = response['Role']
    assert role['Path'] == path
    assert role['RoleName'] == name1
    assert assume_role_policy == json.dumps(role['AssumeRolePolicyDocument'])
    assert len(role['RoleId'])
    arn = role['Arn']
    assert arn.startswith('arn:aws:iam:')
    assert arn.endswith(f':role{path}{name1}')
    assert role['CreateDate'] > datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
    # AWS doesn't include these for CreateRole, only GetRole
    #assert desc == role['Description']
    #assert max_duration == role['MaxSessionDuration']

    response = iam_root.get_role(RoleName=name1)
    role = response['Role']
    assert arn == role['Arn']
    assert desc == role['Description']
    assert max_duration == role['MaxSessionDuration']

    path2 = get_iam_path_prefix() + 'foo/'
    with pytest.raises(iam_root.exceptions.EntityAlreadyExistsException):
        iam_root.create_role(RoleName=name1, Path=path2, AssumeRolePolicyDocument=assume_role_policy)

    name2 = make_iam_name('R2')
    response = iam_root.create_role(RoleName=name2, Path=path2, AssumeRolePolicyDocument=assume_role_policy)
    role = response['Role']
    assert role['Path'] == path2
    assert role['RoleName'] == name2

@pytest.mark.iam_account
@pytest.mark.iam_role
def test_account_role_case_insensitive_name(iam_root):
    path = get_iam_path_prefix()
    name_upper = make_iam_name('R1')
    name_lower = make_iam_name('r1')
    response = iam_root.create_role(RoleName=name_upper, Path=path, AssumeRolePolicyDocument=assume_role_policy)
    rid = response['Role']['RoleId']

    # name is case-insensitive, so 'r1' should also conflict
    with pytest.raises(iam_root.exceptions.EntityAlreadyExistsException):
        iam_root.create_role(RoleName=name_lower, AssumeRolePolicyDocument=assume_role_policy)

    # search for 'r1' should return the same 'R1' role
    response = iam_root.get_role(RoleName=name_lower)
    assert rid == response['Role']['RoleId']

    # delete for 'r1' should delete the same 'R1' role
    iam_root.delete_role(RoleName=name_lower)

    with pytest.raises(iam_root.exceptions.NoSuchEntityException):
        iam_root.get_role(RoleName=name_lower)

@pytest.mark.iam_account
@pytest.mark.iam_role
def test_account_role_delete(iam_root):
    path = get_iam_path_prefix()
    name = make_iam_name('U1')
    with pytest.raises(iam_root.exceptions.NoSuchEntityException):
        iam_root.delete_role(RoleName=name)

    response = iam_root.create_role(RoleName=name, Path=path, AssumeRolePolicyDocument=assume_role_policy)
    uid = response['Role']['RoleId']
    create_date = response['Role']['CreateDate']

    iam_root.delete_role(RoleName=name)

    response = iam_root.create_role(RoleName=name, Path=path, AssumeRolePolicyDocument=assume_role_policy)
    assert uid != response['Role']['RoleId']
    assert create_date <= response['Role']['CreateDate']

def role_list_names(client, **kwargs):
    p = client.get_paginator('list_roles')
    rolenames = []
    for response in p.paginate(**kwargs):
        rolenames += [u['RoleName'] for u in response['Roles']]
    return rolenames

@pytest.mark.iam_account
@pytest.mark.iam_role
def test_account_role_list(iam_root):
    path = get_iam_path_prefix()
    response = iam_root.list_roles(PathPrefix=path)
    assert len(response['Roles']) == 0
    assert response['IsTruncated'] == False

    name1 = make_iam_name('aa')
    name2 = make_iam_name('Ab')
    name3 = make_iam_name('ac')
    name4 = make_iam_name('Ad')

    # sort order is independent of CreateDate, Path, and RoleName capitalization
    iam_root.create_role(RoleName=name4, Path=path+'w/', AssumeRolePolicyDocument=assume_role_policy)
    iam_root.create_role(RoleName=name3, Path=path+'x/', AssumeRolePolicyDocument=assume_role_policy)
    iam_root.create_role(RoleName=name2, Path=path+'y/', AssumeRolePolicyDocument=assume_role_policy)
    iam_root.create_role(RoleName=name1, Path=path+'z/', AssumeRolePolicyDocument=assume_role_policy)

    assert [name1, name2, name3, name4] == \
            role_list_names(iam_root, PathPrefix=path)
    assert [name1, name2, name3, name4] == \
            role_list_names(iam_root, PathPrefix=path, PaginationConfig={'PageSize': 1})

@pytest.mark.iam_account
@pytest.mark.iam_role
def test_account_role_list_path_prefix(iam_root):
    path = get_iam_path_prefix()
    response = iam_root.list_roles(PathPrefix=path)
    assert len(response['Roles']) == 0
    assert response['IsTruncated'] == False

    name1 = make_iam_name('a')
    name2 = make_iam_name('b')
    name3 = make_iam_name('c')
    name4 = make_iam_name('d')

    iam_root.create_role(RoleName=name1, Path=path, AssumeRolePolicyDocument=assume_role_policy)
    iam_root.create_role(RoleName=name2, Path=path, AssumeRolePolicyDocument=assume_role_policy)
    iam_root.create_role(RoleName=name3, Path=path+'a/', AssumeRolePolicyDocument=assume_role_policy)
    iam_root.create_role(RoleName=name4, Path=path+'a/x/', AssumeRolePolicyDocument=assume_role_policy)

    assert [name1, name2, name3, name4] == \
            role_list_names(iam_root, PathPrefix=path)
    assert [name1, name2, name3, name4] == \
            role_list_names(iam_root, PathPrefix=path,
                            PaginationConfig={'PageSize': 1})
    assert [name3, name4] == \
            role_list_names(iam_root, PathPrefix=path+'a')
    assert [name3, name4] == \
            role_list_names(iam_root, PathPrefix=path+'a',
                            PaginationConfig={'PageSize': 1})
    assert [name4] == \
            role_list_names(iam_root, PathPrefix=path+'a/x')
    assert [name4] == \
            role_list_names(iam_root, PathPrefix=path+'a/x',
                            PaginationConfig={'PageSize': 1})
    assert [] == role_list_names(iam_root, PathPrefix=path+'a/x/d')

@pytest.mark.iam_account
@pytest.mark.iam_role
def test_account_role_update(iam_root):
    path = get_iam_path_prefix()
    name = make_iam_name('a')
    with pytest.raises(iam_root.exceptions.NoSuchEntityException):
        iam_root.update_role(RoleName=name)

    iam_root.create_role(RoleName=name, Path=path, AssumeRolePolicyDocument=assume_role_policy)

    response = iam_root.get_role(RoleName=name)
    assert name == response['Role']['RoleName']
    arn = response['Role']['Arn']
    rid = response['Role']['RoleId']

    desc = 'my role description'
    iam_root.update_role(RoleName=name, Description=desc, MaxSessionDuration=43200)

    response = iam_root.get_role(RoleName=name)
    assert rid == response['Role']['RoleId']
    assert arn == response['Role']['Arn']
    assert desc == response['Role']['Description']
    assert 43200 == response['Role']['MaxSessionDuration']
