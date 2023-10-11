import time

import pytest
import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
from botocore.exceptions import ClientError

from . import (
    get_buckets_list,
    get_client,
    get_config_endpoint,
    get_config_ssl_verify,
    get_main_aws_access_key,
    get_main_aws_secret_key,
    get_new_bucket,
    get_new_bucket_name,
    get_new_bucket_resource,
)
from .test_s3 import (
    _check_content_using_range,
    _create_key_with_random_content,
    _get_body,
    _multipart_upload,
)
from .utils import _get_status, _get_status_and_error_code, assert_raises


def _setup_bucket_acl(bucket_acl=None):
    """
    set up a new bucket with specified acl
    """
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(ACL=bucket_acl, Bucket=bucket_name)

    return bucket_name


def _get_keys(response):
    """
    return lists of strings that are the keys from a client.list_objects() response
    """
    keys = []
    if "Contents" in response:
        objects_list = response["Contents"]
        keys = [obj["Key"] for obj in objects_list]
    return keys


def _get_prefixes(response):
    """
    return lists of strings that are prefixes from a client.list_objects() response
    """
    prefixes = []
    if "CommonPrefixes" in response:
        prefix_list = response["CommonPrefixes"]
        prefixes = [prefix["Prefix"] for prefix in prefix_list]
    return prefixes


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
    return "{endpoint}/{bucket_name}".format(endpoint=endpoint, bucket_name=bucket_name)


def _cors_request_and_check(
    method, url, headers, expected_status, expected_headers, with_creds=False
):
    request = AWSRequest(method=method, url=url, headers=headers)

    if with_creds:
        credentials = Credentials(get_main_aws_access_key(), get_main_aws_secret_key())
        client = get_client()
        SigV4Auth(credentials, "service_name", client.meta.region_name).add_auth(
            request
        )

    r = requests.request(
        method=method,
        url=url,
        headers=dict(request.headers),
        verify=get_config_ssl_verify(),
    )
    print(r.headers)
    if expected_status is not None:
        assert r.status_code == expected_status

    for h, v in expected_headers.items():
        print(h, ":", r.headers.get(h), "==", v)
        assert r.headers.get(h) == v


def test_bucket_listv2_continuationtoken_empty():
    key_names = ["bar", "baz", "foo", "quxx"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    e = assert_raises(
        ClientError, client.list_objects_v2, Bucket=bucket_name, ContinuationToken=""
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == "InvalidArgument"


def test_set_cors():
    bucket_name = get_new_bucket()
    client = get_client()
    allowed_methods = ["GET", "PUT"]
    allowed_origins = ["origin1", "origin2"]

    cors_config = {
        "CORSRules": [
            {
                "AllowedMethods": allowed_methods,
                "AllowedOrigins": allowed_origins,
            },
        ]
    }

    e = assert_raises(ClientError, client.get_bucket_cors, Bucket=bucket_name)
    status = _get_status(e.response)
    assert status == 404

    client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration=cors_config)
    response = client.get_bucket_cors(Bucket=bucket_name)
    assert response["CORSRules"][0]["AllowedMethods"] == allowed_methods
    assert response["CORSRules"][0]["AllowedOrigins"] == allowed_origins

    client.delete_bucket_cors(Bucket=bucket_name)
    e = assert_raises(ClientError, client.get_bucket_cors, Bucket=bucket_name)
    status = _get_status(e.response)
    assert status == 404


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/841")
def test_cors_origin_response():
    bucket_name = _setup_bucket_acl(bucket_acl="public-read")
    client = get_client()

    cors_config = {
        "CORSRules": [
            {
                "AllowedMethods": ["PUT", "DELETE"],
                "AllowedOrigins": ["http://www.example1.com"],
                "AllowedHeaders": ["*"],
            },
            {
                "AllowedMethods": ["GET"],
                "AllowedOrigins": ["*"],
                "AllowedHeaders": ["*"],
            },
            {
                "AllowedMethods": ["DELETE"],
                "AllowedOrigins": [
                    "http://www.example2.com",
                    "http://www.example3.com",
                ],
                "AllowedHeaders": ["*"],
            },
        ]
    }

    e = assert_raises(ClientError, client.get_bucket_cors, Bucket=bucket_name)
    status = _get_status(e.response)
    assert status == 404

    client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration=cors_config)

    time.sleep(3)

    url = _get_post_url(bucket_name)

    obj_url = "{u}/{o}".format(u=url, o="bar")

    response_origin_header = "Access-Control-Allow-Origin"
    response_methods_header = "Access-Control-Allow-Methods"
    no_found_rule_header = {response_origin_header: None, response_methods_header: None}

    _cors_request_and_check(
        method="GET",
        url=url,
        headers={},
        expected_status=200,
        expected_headers=no_found_rule_header,
    )

    _cors_request_and_check(
        method="GET",
        url=url,
        headers={"Origin": "http://www.example1.com"},
        expected_status=200,
        expected_headers={response_origin_header: "*", response_methods_header: "GET"},
    )
    _cors_request_and_check(
        method="GET",
        url=obj_url,
        headers={"Origin": "http://www.example1.com"},
        expected_status=404,
        expected_headers={response_origin_header: "*", response_methods_header: "GET"},
    )
    _cors_request_and_check(
        method="PUT",
        url=obj_url,
        headers={"Origin": "http://www.example1.com"},
        expected_status=None,
        expected_headers={
            response_origin_header: "http://www.example1.com",
            response_methods_header: "PUT, DELETE",
        },
    )
    _cors_request_and_check(
        method="DELETE",
        url=obj_url,
        headers={"Origin": "http://www.example1.com"},
        expected_status=None,
        expected_headers={
            response_origin_header: "http://www.example1.com",
            response_methods_header: "PUT, DELETE",
        },
    )

    _cors_request_and_check(
        method="GET",
        url=url,
        headers={"Origin": "http://www.example3.com"},
        expected_status=200,
        expected_headers={response_origin_header: "*", response_methods_header: "GET"},
    )
    _cors_request_and_check(
        method="GET",
        url=obj_url,
        headers={"Origin": "http://www.example3.com"},
        expected_status=404,
        expected_headers={response_origin_header: "*", response_methods_header: "GET"},
    )
    _cors_request_and_check(
        method="PUT",
        url=obj_url,
        headers={"Origin": "http://www.example3.com"},
        expected_status=None,
        expected_headers=no_found_rule_header,
    )
    _cors_request_and_check(
        method="DELETE",
        url=obj_url,
        headers={"Origin": "http://www.example3.com"},
        expected_status=None,
        expected_headers={
            response_origin_header: "http://www.example3.com",
            response_methods_header: "DELETE",
        },
    )

    _cors_request_and_check(
        method="GET",
        url=url,
        headers={"Origin": "http://not.exists"},
        expected_status=200,
        expected_headers={response_origin_header: "*", response_methods_header: "GET"},
    )
    _cors_request_and_check(
        method="GET",
        url=obj_url,
        headers={"Origin": "http://not.exists"},
        expected_status=404,
        expected_headers={response_origin_header: "*", response_methods_header: "GET"},
    )
    _cors_request_and_check(
        method="PUT",
        url=obj_url,
        headers={"Origin": "http://not.exists"},
        expected_status=None,
        expected_headers=no_found_rule_header,
    )
    _cors_request_and_check(
        method="DELETE",
        url=obj_url,
        headers={"Origin": "http://not.exists"},
        expected_status=None,
        expected_headers=no_found_rule_header,
    )

    _cors_request_and_check(
        method="OPTIONS",
        url=url,
        headers={
            "Origin": "http://www.example1.com",
            "Access-Control-Request-Method": "GET",
        },
        expected_status=200,
        expected_headers={response_origin_header: "*", response_methods_header: "GET"},
    )
    _cors_request_and_check(
        method="OPTIONS",
        url=url,
        headers={
            "Origin": "http://www.example1.com",
            "Access-Control-Request-Method": "DELETE",
        },
        expected_status=200,
        expected_headers={
            response_origin_header: "http://www.example1.com",
            response_methods_header: "PUT, DELETE",
        },
    )
    _cors_request_and_check(
        method="OPTIONS",
        url=url,
        headers={
            "Origin": "http://www.example2.com",
            "Access-Control-Request-Method": "DELETE",
        },
        expected_status=200,
        expected_headers={
            response_origin_header: "http://www.example2.com",
            response_methods_header: "DELETE",
        },
    )
    _cors_request_and_check(
        method="OPTIONS",
        url=url,
        headers={
            "Origin": "http://www.example2.com",
            "Access-Control-Request-Method": "PUT",
        },
        expected_status=403,
        expected_headers=no_found_rule_header,
    )

    _cors_request_and_check(
        method="OPTIONS",
        url=url,
        headers={"Origin": "http://not.exists", "Access-Control-Request-Method": "GET"},
        expected_status=200,
        expected_headers={response_origin_header: "*", response_methods_header: "GET"},
    )
    _cors_request_and_check(
        method="OPTIONS",
        url=url,
        headers={"Origin": "http://not.exists", "Access-Control-Request-Method": "PUT"},
        expected_status=403,
        expected_headers=no_found_rule_header,
    )


@pytest.mark.skip(reason="Potential Bug")
def test_cors_origin_response_with_credentials():
    bucket_name = _setup_bucket_acl(bucket_acl="public-read")
    client = get_client()

    cors_config = {
        "CORSRules": [
            {
                "AllowedMethods": ["PUT", "DELETE"],
                "AllowedOrigins": ["http://www.example1.com"],
                "AllowedHeaders": ["*"],
            },
            {
                "AllowedMethods": ["GET"],
                "AllowedOrigins": ["*"],
                "AllowedHeaders": ["*"],
            },
            {
                "AllowedMethods": ["DELETE"],
                "AllowedOrigins": [
                    "http://www.example2.com",
                    "http://www.example3.com",
                ],
                "AllowedHeaders": ["*"],
            },
        ]
    }

    e = assert_raises(ClientError, client.get_bucket_cors, Bucket=bucket_name)
    status = _get_status(e.response)
    assert status == 404

    client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration=cors_config)

    time.sleep(3)

    url = _get_post_url(bucket_name)

    obj_url = "{u}/{o}".format(u=url, o="bar")

    response_origin_header = "Access-Control-Allow-Origin"
    response_methods_header = "Access-Control-Allow-Methods"

    # with_credentials
    _cors_request_and_check(
        method="GET",
        url=url,
        headers={"Origin": "http://www.example1.com"},
        expected_status=None,
        expected_headers={
            response_origin_header: "http://www.example1.com",
            response_methods_header: "GET",
        },
        with_creds=True,
    )
    _cors_request_and_check(
        method="PUT",
        url=obj_url,
        headers={"Origin": "http://www.example1.com"},
        expected_status=None,
        expected_headers={
            response_origin_header: "http://www.example1.com",
            response_methods_header: "PUT, DELETE",
        },
        with_creds=True,
    )
    _cors_request_and_check(
        method="GET",
        url=url,
        headers={"Origin": "http://not.exists"},
        expected_status=None,
        expected_headers={
            response_origin_header: "http://not.exists",
            response_methods_header: "GET",
        },
        with_creds=True,
    )


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/841")
def test_cors_origin_wildcard():
    bucket_name = _setup_bucket_acl(bucket_acl="public-read")
    client = get_client()

    cors_config = {
        "CORSRules": [
            {
                "AllowedMethods": ["GET"],
                "AllowedOrigins": ["*"],
            },
        ]
    }

    e = assert_raises(ClientError, client.get_bucket_cors, Bucket=bucket_name)
    status = _get_status(e.response)
    assert status == 404

    client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration=cors_config)

    time.sleep(3)

    url = _get_post_url(bucket_name)

    response_origin_header = "Access-Control-Allow-Origin"
    no_origin_header = {response_origin_header: None}

    _cors_request_and_check(
        method="GET",
        url=url,
        headers={},
        expected_status=200,
        expected_headers=no_origin_header,
    )
    _cors_request_and_check(
        method="GET",
        url=url,
        headers={"Origin": "http://www.example1.com"},
        expected_status=200,
        expected_headers={response_origin_header: "*"},
    )


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/842")
def test_cors_header_option():
    bucket_name = _setup_bucket_acl(bucket_acl="public-read")
    client = get_client()

    cors_config = {
        "CORSRules": [
            {
                "AllowedMethods": ["GET"],
                "AllowedOrigins": ["*"],
                "AllowedHeaders": ["x-amz-meta-header1"],
            },
            {
                "AllowedMethods": ["PUT"],
                "AllowedOrigins": ["http://www.example.com"],
                "AllowedHeaders": ["x-amz-meta-header2", "x-amz-meta-header3"],
            },
            {
                "AllowedMethods": ["DELETE"],
                "AllowedOrigins": ["http://www.example1.com"],
                "AllowedHeaders": [],
            },
            {
                "AllowedMethods": ["POST"],
                "AllowedOrigins": ["http://www.example2.com"],
                "AllowedHeaders": ["*"],
            },
        ]
    }

    e = assert_raises(ClientError, client.get_bucket_cors, Bucket=bucket_name)
    status = _get_status(e.response)
    assert status == 404

    client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration=cors_config)

    time.sleep(3)

    url = _get_post_url(bucket_name)

    response_origin_header = "Access-Control-Allow-Origin"
    response_methods_header = "Access-Control-Allow-Methods"
    response_headers_header = "Access-Control-Allow-Headers"
    no_origin_header = {response_origin_header: None}

    _cors_request_and_check(
        method="OPTIONS",
        url=url,
        headers={
            "Origin": "http://any.origin",
            "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Headers": "x-amz-meta-header1",
        },
        expected_status=200,
        expected_headers={
            response_origin_header: "*",
            response_methods_header: "GET",
            response_headers_header: "x-amz-meta-header1",
        },
    )
    _cors_request_and_check(
        method="OPTIONS",
        url=url,
        headers={
            "Origin": "http://www.example.com",
            "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Headers": "x-amz-meta-header1",
        },
        expected_status=200,
        expected_headers={
            response_origin_header: "*",
            response_methods_header: "GET",
            response_headers_header: "x-amz-meta-header1",
        },
    )
    _cors_request_and_check(
        method="OPTIONS",
        url=url,
        headers={
            "Origin": "http://www.example.com",
            "Access-Control-Request-Method": "PUT",
            "Access-Control-Request-Headers": "x-amz-meta-header1",
        },
        expected_status=403,
        expected_headers=no_origin_header,
    )
    _cors_request_and_check(
        method="OPTIONS",
        url=url,
        headers={
            "Origin": "http://www.example.com",
            "Access-Control-Request-Method": "PUT",
            "Access-Control-Request-Headers": "x-amz-meta-header2",
        },
        expected_status=200,
        expected_headers={
            response_origin_header: "http://www.example.com",
            response_methods_header: "PUT",
            response_headers_header: "x-amz-meta-header2",
        },
    )
    _cors_request_and_check(
        method="OPTIONS",
        url=url,
        headers={
            "Origin": "http://www.example.com",
            "Access-Control-Request-Method": "PUT",
            "Access-Control-Request-Headers": "x-amz-meta-header3",
        },
        expected_status=200,
        expected_headers={
            response_origin_header: "http://www.example.com",
            response_methods_header: "PUT",
            response_headers_header: "x-amz-meta-header3",
        },
    )
    _cors_request_and_check(
        method="OPTIONS",
        url=url,
        headers={
            "Origin": "http://www.example.com",
            "Access-Control-Request-Method": "PUT",
            "Access-Control-Request-Headers": "x-amz-meta-header2, x-amz-meta-header3",
        },
        expected_status=200,
        expected_headers={
            response_origin_header: "http://www.example.com",
            response_methods_header: "PUT",
            response_headers_header: "x-amz-meta-header2, x-amz-meta-header3",
        },
    )
    _cors_request_and_check(
        method="OPTIONS",
        url=url,
        headers={
            "Origin": "http://www.example.com",
            "Access-Control-Request-Method": "PUT",
        },
        expected_status=200,
        expected_headers={
            response_origin_header: "http://www.example.com",
            response_methods_header: "PUT",
            response_headers_header: None,
        },
    )
    _cors_request_and_check(
        method="OPTIONS",
        url=url,
        headers={
            "Origin": "http://www.example1.com",
            "Access-Control-Request-Method": "DELETE",
            "Access-Control-Request-Headers": "x-amz-meta-header",
        },
        expected_status=403,
        expected_headers=no_origin_header,
    )
    _cors_request_and_check(
        method="OPTIONS",
        url=url,
        headers={
            "Origin": "http://www.example1.com",
            "Access-Control-Request-Method": "DELETE",
        },
        expected_status=200,
        expected_headers={
            response_origin_header: "http://www.example1.com",
            response_methods_header: "DELETE",
            response_headers_header: None,
        },
    )
    _cors_request_and_check(
        method="OPTIONS",
        url=url,
        headers={
            "Origin": "http://www.example2.com",
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "x-amz-meta-header",
        },
        expected_status=200,
        expected_headers={
            response_origin_header: "http://www.example2.com",
            response_methods_header: "POST",
            response_headers_header: "x-amz-meta-header",
        },
    )
    _cors_request_and_check(
        method="OPTIONS",
        url=url,
        headers={
            "Origin": "http://www.example2.com",
            "Access-Control-Request-Method": "POST",
        },
        expected_status=200,
        expected_headers={
            response_origin_header: "http://www.example2.com",
            response_methods_header: "POST",
            response_headers_header: None,
        },
    )


def test_multipart_upload_small():
    bucket_name = get_new_bucket()
    client = get_client()

    key1 = "mymultipart"
    objlen = 1
    (upload_id, data, parts) = _multipart_upload(
        bucket_name=bucket_name, key=key1, size=objlen
    )
    response = client.complete_multipart_upload(
        Bucket=bucket_name,
        Key=key1,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )
    response = client.get_object(Bucket=bucket_name, Key=key1)
    assert response["ContentLength"] == objlen
    client.delete_object(Bucket=bucket_name, Key=key1)


def test_multipart_copy_invalid_range():
    client = get_client()
    src_key = "source"
    src_bucket_name = _create_key_with_random_content(src_key, size=5)

    response = client.create_multipart_upload(Bucket=src_bucket_name, Key="dest")
    upload_id = response["UploadId"]

    copy_source = {"Bucket": src_bucket_name, "Key": src_key}
    copy_source_range = "bytes={start}-{end}".format(start=0, end=21)

    e = assert_raises(
        ClientError,
        client.upload_part_copy,
        Bucket=src_bucket_name,
        Key="dest",
        UploadId=upload_id,
        CopySource=copy_source,
        CopySourceRange=copy_source_range,
        PartNumber=1,
    )
    status, error_code = _get_status_and_error_code(e.response)
    valid_status = [400, 416]
    if not status in valid_status:
        raise AssertionError("Invalid response " + str(status))
    assert error_code == "InvalidRange"
    client.delete_object(Bucket=src_bucket_name, Key=src_key)


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/843")
def test_multipart_upload():
    bucket_name = get_new_bucket()
    key = "mymultipart"
    content_type = "text/bla"
    objlen = 30 * 1024 * 1024
    metadata = {"foo": "bar"}
    client = get_client()

    (upload_id, data, parts) = _multipart_upload(
        bucket_name=bucket_name,
        key=key,
        size=objlen,
        content_type=content_type,
        metadata=metadata,
    )
    client.complete_multipart_upload(
        Bucket=bucket_name,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )

    response = client.head_bucket(Bucket=bucket_name)
    rgw_bytes_used = int(
        response["ResponseMetadata"]["HTTPHeaders"].get("x-rgw-bytes-used", objlen)
    )
    assert rgw_bytes_used == objlen

    rgw_object_count = int(
        response["ResponseMetadata"]["HTTPHeaders"].get("x-rgw-object-count", 1)
    )
    assert rgw_object_count == 1

    response = client.get_object(Bucket=bucket_name, Key=key)
    assert response["ContentType"] == content_type
    assert response["Metadata"]["foo"] == metadata["foo"]
    body = _get_body(response)
    assert len(body) == response["ContentLength"]
    assert body == data

    _check_content_using_range(key, bucket_name, data, 1000000)
    _check_content_using_range(key, bucket_name, data, 10000000)
    client.delete_object(Bucket=bucket_name, Key=key)


def test_bucket_create_delete():
    bucket_name = get_new_bucket()
    client = get_client()

    response = client.head_bucket(Bucket=bucket_name)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    buckets_list = get_buckets_list()
    if bucket_name not in buckets_list:
        raise RuntimeError("bucket isn't in list")

    response = client.delete_bucket(Bucket=bucket_name)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 204

    e = assert_raises(ClientError, client.head_bucket, Bucket=bucket_name)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404


def test_object_basic_workflow():
    bucket_name = get_new_bucket()
    object_name = "object"
    client = get_client()

    response = client.put_object(Bucket=bucket_name, Key=object_name, Body="foo")
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    response = client.get_object(Bucket=bucket_name, Key=object_name)
    body = _get_body(response)
    assert body == "foo"

    copy_source = {"Bucket": bucket_name, "Key": object_name}
    object_copy_name = "object-copy"
    client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key=object_copy_name)

    response = client.get_object(Bucket=bucket_name, Key=object_copy_name)
    body = _get_body(response)
    assert body == "foo"

    response = client.delete_object(Bucket=bucket_name, Key=object_name)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 204

    e = assert_raises(
        ClientError, client.get_object, Bucket=bucket_name, Key=object_name
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404


def test_list_objects():
    client = get_client()
    list_objects(client.list_objects)


def test_list_objects_v2():
    client = get_client()
    list_objects(client.list_objects_v2)


def list_objects(list_object_function):
    bucket_name = _create_objects(keys=["foo/foo2", "foo/foo3", "bar", "/baz"])

    response = list_object_function(Bucket=bucket_name)
    keys = _get_keys(response)
    assert len(keys) == 4
    assert keys == ["/baz", "bar", "foo/foo2", "foo/foo3"]

    response = list_object_function(Bucket=bucket_name, Prefix="/")
    keys = _get_keys(response)
    assert len(keys) == 1
    assert keys == ["/baz"]

    response = list_object_function(Bucket=bucket_name, Prefix="foo/")
    keys = _get_keys(response)
    assert len(keys) == 2
    assert keys == ["foo/foo2", "foo/foo3"]

    response = list_object_function(Bucket=bucket_name, Prefix="c", Delimiter="d")
    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == []
    assert prefixes == []


def test_multipart_workflow():
    bucket_name = get_new_bucket()
    object_name = "object"
    client = get_client()

    response = client.create_multipart_upload(Bucket=bucket_name, Key=object_name)
    upload_id = response["UploadId"]

    response = client.list_multipart_uploads(Bucket=bucket_name)
    uploads = response["Uploads"]
    assert len(uploads) == 1
    assert uploads[0]["UploadId"] == upload_id

    response = client.abort_multipart_upload(
        Bucket=bucket_name, Key=object_name, UploadId=upload_id
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 204

    _check_empty_list_multipart(bucket_name)

    response = client.create_multipart_upload(Bucket=bucket_name, Key=object_name)
    upload_id = response["UploadId"]

    part_content = "content"
    response = client.upload_part(
        Bucket=bucket_name,
        Key=object_name,
        UploadId=upload_id,
        PartNumber=1,
        Body=part_content,
    )
    parts = [{"ETag": response["ETag"].strip('"'), "PartNumber": 1}]

    response = client.list_parts(
        Bucket=bucket_name, Key=object_name, UploadId=upload_id
    )
    response_parts = response["Parts"]
    assert len(response_parts) == 1
    assert response_parts[0]["ETag"] == parts[0]["ETag"]

    response = client.complete_multipart_upload(
        Bucket=bucket_name,
        Key=object_name,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    _check_empty_list_multipart(bucket_name)

    response = client.get_object(Bucket=bucket_name, Key=object_name)
    body = _get_body(response)
    assert body == part_content


def _check_empty_list_multipart(bucket_name):
    client = get_client()
    response = client.list_multipart_uploads(Bucket=bucket_name)
    uploads = []
    if "Uploads" in response:
        uploads = response["Uploads"]
    assert len(uploads) == 0


def test_object_versioning_workflow():
    object_name = "object"
    bucket_name = get_new_bucket()
    client = get_client()

    response = client.put_bucket_versioning(
        Bucket=bucket_name, VersioningConfiguration={"Status": "Enabled"}
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    response = client.get_bucket_versioning(Bucket=bucket_name)
    assert response["Status"] == "Enabled"

    response = client.put_object(Bucket=bucket_name, Key=object_name, Body="version1")
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    response = client.put_object(Bucket=bucket_name, Key=object_name, Body="version2")
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    response = client.delete_object(Bucket=bucket_name, Key=object_name)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 204

    e = assert_raises(
        ClientError, client.get_object, Bucket=bucket_name, Key=object_name
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404

    response = client.list_object_versions(Bucket=bucket_name)
    objs_list = response["Versions"]
    assert len(objs_list) == 2
    delete_markers = response["DeleteMarkers"]
    assert len(delete_markers) == 1

    response = client.get_object(
        Bucket=bucket_name, Key=object_name, VersionId=objs_list[0]["VersionId"]
    )
    body = _get_body(response)
    assert body == "version2"

    response = client.get_object(
        Bucket=bucket_name, Key=object_name, VersionId=objs_list[1]["VersionId"]
    )
    body = _get_body(response)
    assert body == "version1"

    response = client.delete_object(
        Bucket=bucket_name, Key=object_name, VersionId=delete_markers[0]["VersionId"]
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 204

    response = client.get_object(Bucket=bucket_name, Key=object_name)
    body = _get_body(response)
    assert body == "version2"


def test_bucket_tagging_workflow():
    bucket_name = get_new_bucket()
    client = get_client()

    tags = {"TagSet": [{"Key": "bucket-tag-key", "Value": "bucket-tag-value"}]}

    response = client.put_bucket_tagging(Bucket=bucket_name, Tagging=tags)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    response = client.get_bucket_tagging(Bucket=bucket_name)
    assert len(response["TagSet"]) == 1
    assert response["TagSet"][0]["Key"] == "bucket-tag-key"
    assert response["TagSet"][0]["Value"] == "bucket-tag-value"

    response = client.delete_bucket_tagging(Bucket=bucket_name)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 204

    response = client.get_bucket_tagging(Bucket=bucket_name)
    assert len(response["TagSet"]) == 0


def test_object_tagging_workflow():
    bucket_name = get_new_bucket()
    object_name = "object"
    client = get_client()

    response = client.put_object(Bucket=bucket_name, Key=object_name, Body="content")
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    tags = {"TagSet": [{"Key": "object-tag-key", "Value": "object-tag-value"}]}
    response = client.put_object_tagging(
        Bucket=bucket_name, Key=object_name, Tagging=tags
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    response = client.get_object_tagging(Bucket=bucket_name, Key=object_name)
    assert len(response["TagSet"]) == 1
    assert response["TagSet"][0]["Key"] == "object-tag-key"
    assert response["TagSet"][0]["Value"] == "object-tag-value"

    new_tags = {
        "TagSet": [{"Key": "object-new-tag-key", "Value": "object-new-tag-value"}]
    }
    response = client.put_object_tagging(
        Bucket=bucket_name, Key=object_name, Tagging=new_tags
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    response = client.get_object_tagging(Bucket=bucket_name, Key=object_name)
    assert len(response["TagSet"]) == 1
    assert response["TagSet"][0]["Key"] == "object-new-tag-key"
    assert response["TagSet"][0]["Value"] == "object-new-tag-value"

    response = client.delete_object_tagging(Bucket=bucket_name, Key=object_name)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 204

    response = client.get_object_tagging(Bucket=bucket_name, Key=object_name)
    assert len(response["TagSet"]) == 0


@pytest.mark.skip(reason="Potential Bug")
def test_object_attributes():
    bucket_name = get_new_bucket()
    object_name = "object"
    client = get_client()

    response = client.put_object(Bucket=bucket_name, Key=object_name, Body="foo")
    etag = response["ETag"]

    response = client.get_object_attributes(
        Bucket=bucket_name, Key=object_name, ObjectAttributes=["ETag"]
    )
    assert response["ETag"] == etag

    response = client.get_object_attributes(
        Bucket=bucket_name,
        Key=object_name,
        ObjectAttributes=["ObjectSize", "StorageClass"],
    )
    assert response["ObjectSize"] == 3
    assert response["StorageClass"] == "STANDARD"

    response = client.put_bucket_versioning(
        Bucket=bucket_name, VersioningConfiguration={"Status": "Enabled"}
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    object_name_versioned = "object-versioned"

    response = client.put_object(
        Bucket=bucket_name, Key=object_name_versioned, Body="bar"
    )
    etag = response["ETag"]
    version_id = response["VersionId"]

    response = client.get_object_attributes(
        Bucket=bucket_name,
        Key=object_name_versioned,
        VersionId=version_id,
        ObjectAttributes=["ETag"],
    )
    assert response["ETag"] == etag
    assert response["VersionId"] == version_id

    object_name_multipart = "object-multipart"
    (upload_id, data, parts) = _multipart_upload(
        bucket_name=bucket_name, key=object_name_multipart, size=12 * 1024 * 1024
    )

    response = client.list_parts(
        Bucket=bucket_name, Key=object_name_multipart, UploadId=upload_id
    )
    response_parts = response["Parts"]
    assert len(response_parts) == len(parts)

    for i in range(3):
        assert response_parts[i]["ETag"] == parts[i]["ETag"]
        assert response_parts[i]["PartNumber"] == parts[i]["PartNumber"]

    client.complete_multipart_upload(
        Bucket=bucket_name,
        Key=object_name_multipart,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    response = client.get_object_attributes(
        Bucket=bucket_name, Key=object_name_multipart, ObjectAttributes=["ObjectParts"]
    )
    object_parts = response["ObjectParts"]["Parts"]
    assert response["ObjectParts"]["TotalPartsCount"] == 3
    for i in range(3):
        assert object_parts[i]["ChecksumSHA256"] == parts[i]["ETag"]
        assert object_parts[i]["PartNumber"] == parts[i]["PartNumber"]

    response = client.get_object_attributes(
        Bucket=bucket_name,
        Key=object_name_multipart,
        ObjectAttributes=["ObjectParts"],
        MaxParts=2,
    )
    object_parts = response["ObjectParts"]["Parts"]
    assert response["ObjectParts"]["TotalPartsCount"] == 3
    assert response["ObjectParts"]["IsTruncated"] == True
    for i in range(2):
        assert object_parts[i]["ChecksumSHA256"] == parts[i]["ETag"]
        assert object_parts[i]["PartNumber"] == parts[i]["PartNumber"]

    response = client.get_object_attributes(
        Bucket=bucket_name,
        Key=object_name_multipart,
        ObjectAttributes=["ObjectParts"],
        PartNumberMarker=3,
    )
    object_parts = response["ObjectParts"]["Parts"]
    assert response["ObjectParts"]["TotalPartsCount"] == 3
    assert response["ObjectParts"]["PartNumberMarker"] == 3
    assert object_parts[0]["ChecksumSHA256"] == parts[2]["ETag"]
    assert object_parts[0]["PartNumber"] == parts[2]["PartNumber"]
