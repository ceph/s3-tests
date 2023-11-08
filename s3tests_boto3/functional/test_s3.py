import base64
import datetime
import hashlib
import hmac
import json
import os
import random
import re
import socket
import ssl
import string
import threading
import time
import xml.etree.ElementTree as ET
from collections import OrderedDict, defaultdict, namedtuple

import botocore.session
import dateutil.parser
import isodate
import pytest
import pytz
import requests
from botocore.exceptions import ClientError

from . import (
    configured_storage_classes,
    get_alt_client,
    get_alt_display_name,
    get_alt_email,
    get_alt_user_id,
    get_bad_auth_client,
    get_buckets_list,
    get_client,
    get_cloud_client,
    get_cloud_regular_storage_class,
    get_cloud_retain_head_object,
    get_cloud_storage_class,
    get_cloud_target_path,
    get_cloud_target_storage_class,
    get_config_endpoint,
    get_config_host,
    get_config_is_secure,
    get_config_port,
    get_config_ssl_verify,
    get_lc_debug_interval,
    get_main_api_name,
    get_main_aws_access_key,
    get_main_aws_secret_key,
    get_main_display_name,
    get_main_kms_keyid,
    get_main_user_id,
    get_new_bucket,
    get_new_bucket_name,
    get_new_bucket_resource,
    get_objects_list,
    get_prefix,
    get_secondary_kms_keyid,
    get_tenant_client,
    get_unauthenticated_client,
    get_v2_client,
    nuke_prefixed_buckets,
)
from .policy import Policy, Statement, make_json_policy
from .utils import (
    _get_status,
    _get_status_and_error_code,
    assert_raises,
    generate_random,
)


def _bucket_is_empty(bucket):
    is_empty = True
    for obj in bucket.objects.all():
        is_empty = False
        break
    return is_empty


def test_bucket_list_empty():
    bucket = get_new_bucket_resource()
    is_empty = _bucket_is_empty(bucket)
    assert is_empty == True


@pytest.mark.list_objects_v2
def test_bucket_list_distinct():
    bucket1 = get_new_bucket_resource()
    bucket2 = get_new_bucket_resource()
    obj = bucket1.put_object(Body="str", Key="asdf")
    is_empty = _bucket_is_empty(bucket2)
    assert is_empty == True


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


@pytest.mark.fails_on_dbstore
def test_bucket_list_many():
    bucket_name = _create_objects(keys=["foo", "bar", "baz"])
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, MaxKeys=2)
    keys = _get_keys(response)
    assert len(keys) == 2
    assert keys == ["bar", "baz"]
    assert response["IsTruncated"] == True

    response = client.list_objects(Bucket=bucket_name, Marker="baz", MaxKeys=2)
    keys = _get_keys(response)
    assert len(keys) == 1
    assert response["IsTruncated"] == False
    assert keys == ["foo"]


@pytest.mark.list_objects_v2
@pytest.mark.fails_on_dbstore
def test_bucket_listv2_many():
    bucket_name = _create_objects(keys=["foo", "bar", "baz"])
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, MaxKeys=2)
    keys = _get_keys(response)
    assert len(keys) == 2
    assert keys == ["bar", "baz"]
    assert response["IsTruncated"] == True

    response = client.list_objects_v2(Bucket=bucket_name, StartAfter="baz", MaxKeys=2)
    keys = _get_keys(response)
    assert len(keys) == 1
    assert response["IsTruncated"] == False
    assert keys == ["foo"]


@pytest.mark.list_objects_v2
def test_basic_key_count():
    client = get_client()
    bucket_names = []
    bucket_name = get_new_bucket_name()
    client.create_bucket(Bucket=bucket_name)
    for j in range(5):
        client.put_object(Bucket=bucket_name, Key=str(j))
    response1 = client.list_objects_v2(Bucket=bucket_name)
    assert response1["KeyCount"] == 5


def test_bucket_list_delimiter_basic():
    bucket_name = _create_objects(
        keys=["foo/bar", "foo/bar/xyzzy", "quux/thud", "asdf"]
    )
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Delimiter="/")
    assert response["Delimiter"] == "/"
    keys = _get_keys(response)
    assert keys == ["asdf"]

    prefixes = _get_prefixes(response)
    assert len(prefixes) == 2
    assert prefixes == ["foo/", "quux/"]


@pytest.mark.list_objects_v2
def test_bucket_listv2_delimiter_basic():
    bucket_name = _create_objects(
        keys=["foo/bar", "foo/bar/xyzzy", "quux/thud", "asdf"]
    )
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, Delimiter="/")
    assert response["Delimiter"] == "/"
    keys = _get_keys(response)
    assert keys == ["asdf"]

    prefixes = _get_prefixes(response)
    assert len(prefixes) == 2
    assert prefixes == ["foo/", "quux/"]
    assert response["KeyCount"] == len(prefixes) + len(keys)


@pytest.mark.list_objects_v2
def test_bucket_listv2_encoding_basic():
    bucket_name = _create_objects(
        keys=["foo+1/bar", "foo/bar/xyzzy", "quux ab/thud", "asdf+b"]
    )
    client = get_client()

    response = client.list_objects_v2(
        Bucket=bucket_name, Delimiter="/", EncodingType="url"
    )
    assert response["Delimiter"] == "/"
    keys = _get_keys(response)
    assert keys == ["asdf%2Bb"]

    prefixes = _get_prefixes(response)
    assert len(prefixes) == 3
    assert prefixes == ["foo%2B1/", "foo/", "quux%20ab/"]


def test_bucket_list_encoding_basic():
    bucket_name = _create_objects(
        keys=["foo+1/bar", "foo/bar/xyzzy", "quux ab/thud", "asdf+b"]
    )
    client = get_client()

    response = client.list_objects(
        Bucket=bucket_name, Delimiter="/", EncodingType="url"
    )
    assert response["Delimiter"] == "/"
    keys = _get_keys(response)
    assert keys == ["asdf%2Bb"]

    prefixes = _get_prefixes(response)
    assert len(prefixes) == 3
    assert prefixes == ["foo%2B1/", "foo/", "quux%20ab/"]


def validate_bucket_list(
    bucket_name,
    prefix,
    delimiter,
    marker,
    max_keys,
    is_truncated,
    check_objs,
    check_prefixes,
    next_marker,
):
    client = get_client()

    response = client.list_objects(
        Bucket=bucket_name,
        Delimiter=delimiter,
        Marker=marker,
        MaxKeys=max_keys,
        Prefix=prefix,
    )
    assert response["IsTruncated"] == is_truncated
    if "NextMarker" not in response:
        response["NextMarker"] = None
    assert response["NextMarker"] == next_marker

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)

    assert len(keys) == len(check_objs)
    assert len(prefixes) == len(check_prefixes)
    assert keys == check_objs
    assert prefixes == check_prefixes

    return response["NextMarker"]


def validate_bucket_listv2(
    bucket_name,
    prefix,
    delimiter,
    continuation_token,
    max_keys,
    is_truncated,
    check_objs,
    check_prefixes,
    last=False,
):
    client = get_client()

    params = dict(
        Bucket=bucket_name, Delimiter=delimiter, MaxKeys=max_keys, Prefix=prefix
    )
    if continuation_token is not None:
        params["ContinuationToken"] = continuation_token
    else:
        params["StartAfter"] = ""
    response = client.list_objects_v2(**params)
    assert response["IsTruncated"] == is_truncated
    if "NextContinuationToken" not in response:
        response["NextContinuationToken"] = None
    if last:
        assert response["NextContinuationToken"] == None

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)

    assert len(keys) == len(check_objs)
    assert len(prefixes) == len(check_prefixes)
    assert keys == check_objs
    assert prefixes == check_prefixes

    return response["NextContinuationToken"]


@pytest.mark.fails_on_dbstore
def test_bucket_list_delimiter_prefix():
    bucket_name = _create_objects(
        keys=["asdf", "boo/bar", "boo/baz/xyzzy", "cquux/thud", "cquux/bla"]
    )

    delim = "/"
    marker = ""
    prefix = ""

    marker = validate_bucket_list(
        bucket_name, prefix, delim, "", 1, True, ["asdf"], [], "asdf"
    )
    marker = validate_bucket_list(
        bucket_name, prefix, delim, marker, 1, True, [], ["boo/"], "boo/"
    )
    marker = validate_bucket_list(
        bucket_name, prefix, delim, marker, 1, False, [], ["cquux/"], None
    )

    marker = validate_bucket_list(
        bucket_name, prefix, delim, "", 2, True, ["asdf"], ["boo/"], "boo/"
    )
    marker = validate_bucket_list(
        bucket_name, prefix, delim, marker, 2, False, [], ["cquux/"], None
    )

    prefix = "boo/"

    marker = validate_bucket_list(
        bucket_name, prefix, delim, "", 1, True, ["boo/bar"], [], "boo/bar"
    )
    marker = validate_bucket_list(
        bucket_name, prefix, delim, marker, 1, False, [], ["boo/baz/"], None
    )

    marker = validate_bucket_list(
        bucket_name, prefix, delim, "", 2, False, ["boo/bar"], ["boo/baz/"], None
    )


@pytest.mark.list_objects_v2
@pytest.mark.fails_on_dbstore
def test_bucket_listv2_delimiter_prefix():
    bucket_name = _create_objects(
        keys=["asdf", "boo/bar", "boo/baz/xyzzy", "cquux/thud", "cquux/bla"]
    )

    delim = "/"
    continuation_token = ""
    prefix = ""

    continuation_token = validate_bucket_listv2(
        bucket_name, prefix, delim, None, 1, True, ["asdf"], []
    )
    continuation_token = validate_bucket_listv2(
        bucket_name, prefix, delim, continuation_token, 1, True, [], ["boo/"]
    )
    continuation_token = validate_bucket_listv2(
        bucket_name,
        prefix,
        delim,
        continuation_token,
        1,
        False,
        [],
        ["cquux/"],
        last=True,
    )

    continuation_token = validate_bucket_listv2(
        bucket_name, prefix, delim, None, 2, True, ["asdf"], ["boo/"]
    )
    continuation_token = validate_bucket_listv2(
        bucket_name,
        prefix,
        delim,
        continuation_token,
        2,
        False,
        [],
        ["cquux/"],
        last=True,
    )

    prefix = "boo/"

    continuation_token = validate_bucket_listv2(
        bucket_name, prefix, delim, None, 1, True, ["boo/bar"], []
    )
    continuation_token = validate_bucket_listv2(
        bucket_name,
        prefix,
        delim,
        continuation_token,
        1,
        False,
        [],
        ["boo/baz/"],
        last=True,
    )

    continuation_token = validate_bucket_listv2(
        bucket_name, prefix, delim, None, 2, False, ["boo/bar"], ["boo/baz/"], last=True
    )


@pytest.mark.list_objects_v2
def test_bucket_listv2_delimiter_prefix_ends_with_delimiter():
    bucket_name = _create_objects(keys=["asdf/"])
    validate_bucket_listv2(
        bucket_name, "asdf/", "/", None, 1000, False, ["asdf/"], [], last=True
    )


def test_bucket_list_delimiter_prefix_ends_with_delimiter():
    bucket_name = _create_objects(keys=["asdf/"])
    validate_bucket_list(
        bucket_name, "asdf/", "/", "", 1000, False, ["asdf/"], [], None
    )


def test_bucket_list_delimiter_alt():
    bucket_name = _create_objects(keys=["bar", "baz", "cab", "foo"])
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Delimiter="a")
    assert response["Delimiter"] == "a"

    keys = _get_keys(response)
    # foo contains no 'a' and so is a complete key
    assert keys == ["foo"]

    # bar, baz, and cab should be broken up by the 'a' delimiters
    prefixes = _get_prefixes(response)
    assert len(prefixes) == 2
    assert prefixes == ["ba", "ca"]


@pytest.mark.list_objects_v2
def test_bucket_listv2_delimiter_alt():
    bucket_name = _create_objects(keys=["bar", "baz", "cab", "foo"])
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, Delimiter="a")
    assert response["Delimiter"] == "a"

    keys = _get_keys(response)
    # foo contains no 'a' and so is a complete key
    assert keys == ["foo"]

    # bar, baz, and cab should be broken up by the 'a' delimiters
    prefixes = _get_prefixes(response)
    assert len(prefixes) == 2
    assert prefixes == ["ba", "ca"]


@pytest.mark.fails_on_dbstore
def test_bucket_list_delimiter_prefix_underscore():
    bucket_name = _create_objects(
        keys=[
            "_obj1_",
            "_under1/bar",
            "_under1/baz/xyzzy",
            "_under2/thud",
            "_under2/bla",
        ]
    )

    delim = "/"
    marker = ""
    prefix = ""
    marker = validate_bucket_list(
        bucket_name, prefix, delim, "", 1, True, ["_obj1_"], [], "_obj1_"
    )
    marker = validate_bucket_list(
        bucket_name, prefix, delim, marker, 1, True, [], ["_under1/"], "_under1/"
    )
    marker = validate_bucket_list(
        bucket_name, prefix, delim, marker, 1, False, [], ["_under2/"], None
    )

    marker = validate_bucket_list(
        bucket_name, prefix, delim, "", 2, True, ["_obj1_"], ["_under1/"], "_under1/"
    )
    marker = validate_bucket_list(
        bucket_name, prefix, delim, marker, 2, False, [], ["_under2/"], None
    )

    prefix = "_under1/"

    marker = validate_bucket_list(
        bucket_name, prefix, delim, "", 1, True, ["_under1/bar"], [], "_under1/bar"
    )
    marker = validate_bucket_list(
        bucket_name, prefix, delim, marker, 1, False, [], ["_under1/baz/"], None
    )

    marker = validate_bucket_list(
        bucket_name,
        prefix,
        delim,
        "",
        2,
        False,
        ["_under1/bar"],
        ["_under1/baz/"],
        None,
    )


@pytest.mark.list_objects_v2
@pytest.mark.fails_on_dbstore
def test_bucket_listv2_delimiter_prefix_underscore():
    bucket_name = _create_objects(
        keys=[
            "_obj1_",
            "_under1/bar",
            "_under1/baz/xyzzy",
            "_under2/thud",
            "_under2/bla",
        ]
    )

    delim = "/"
    continuation_token = ""
    prefix = ""
    continuation_token = validate_bucket_listv2(
        bucket_name, prefix, delim, None, 1, True, ["_obj1_"], []
    )
    continuation_token = validate_bucket_listv2(
        bucket_name, prefix, delim, continuation_token, 1, True, [], ["_under1/"]
    )
    continuation_token = validate_bucket_listv2(
        bucket_name,
        prefix,
        delim,
        continuation_token,
        1,
        False,
        [],
        ["_under2/"],
        last=True,
    )

    continuation_token = validate_bucket_listv2(
        bucket_name, prefix, delim, None, 2, True, ["_obj1_"], ["_under1/"]
    )
    continuation_token = validate_bucket_listv2(
        bucket_name,
        prefix,
        delim,
        continuation_token,
        2,
        False,
        [],
        ["_under2/"],
        last=True,
    )

    prefix = "_under1/"

    continuation_token = validate_bucket_listv2(
        bucket_name, prefix, delim, None, 1, True, ["_under1/bar"], []
    )
    continuation_token = validate_bucket_listv2(
        bucket_name,
        prefix,
        delim,
        continuation_token,
        1,
        False,
        [],
        ["_under1/baz/"],
        last=True,
    )

    continuation_token = validate_bucket_listv2(
        bucket_name,
        prefix,
        delim,
        None,
        2,
        False,
        ["_under1/bar"],
        ["_under1/baz/"],
        last=True,
    )


def test_bucket_list_delimiter_percentage():
    bucket_name = _create_objects(keys=["b%ar", "b%az", "c%ab", "foo"])
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Delimiter="%")
    assert response["Delimiter"] == "%"
    keys = _get_keys(response)
    # foo contains no 'a' and so is a complete key
    assert keys == ["foo"]

    prefixes = _get_prefixes(response)
    assert len(prefixes) == 2
    # bar, baz, and cab should be broken up by the 'a' delimiters
    assert prefixes == ["b%", "c%"]


@pytest.mark.list_objects_v2
def test_bucket_listv2_delimiter_percentage():
    bucket_name = _create_objects(keys=["b%ar", "b%az", "c%ab", "foo"])
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, Delimiter="%")
    assert response["Delimiter"] == "%"
    keys = _get_keys(response)
    # foo contains no 'a' and so is a complete key
    assert keys == ["foo"]

    prefixes = _get_prefixes(response)
    assert len(prefixes) == 2
    # bar, baz, and cab should be broken up by the 'a' delimiters
    assert prefixes == ["b%", "c%"]


def test_bucket_list_delimiter_whitespace():
    bucket_name = _create_objects(keys=["b ar", "b az", "c ab", "foo"])
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Delimiter=" ")
    assert response["Delimiter"] == " "
    keys = _get_keys(response)
    # foo contains no 'a' and so is a complete key
    assert keys == ["foo"]

    prefixes = _get_prefixes(response)
    assert len(prefixes) == 2
    # bar, baz, and cab should be broken up by the 'a' delimiters
    assert prefixes == ["b ", "c "]


@pytest.mark.list_objects_v2
def test_bucket_listv2_delimiter_whitespace():
    bucket_name = _create_objects(keys=["b ar", "b az", "c ab", "foo"])
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, Delimiter=" ")
    assert response["Delimiter"] == " "
    keys = _get_keys(response)
    # foo contains no 'a' and so is a complete key
    assert keys == ["foo"]

    prefixes = _get_prefixes(response)
    assert len(prefixes) == 2
    # bar, baz, and cab should be broken up by the 'a' delimiters
    assert prefixes == ["b ", "c "]


def test_bucket_list_delimiter_dot():
    bucket_name = _create_objects(keys=["b.ar", "b.az", "c.ab", "foo"])
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Delimiter=".")
    assert response["Delimiter"] == "."
    keys = _get_keys(response)
    # foo contains no 'a' and so is a complete key
    assert keys == ["foo"]

    prefixes = _get_prefixes(response)
    assert len(prefixes) == 2
    # bar, baz, and cab should be broken up by the 'a' delimiters
    assert prefixes == ["b.", "c."]


@pytest.mark.list_objects_v2
def test_bucket_listv2_delimiter_dot():
    bucket_name = _create_objects(keys=["b.ar", "b.az", "c.ab", "foo"])
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, Delimiter=".")
    assert response["Delimiter"] == "."
    keys = _get_keys(response)
    # foo contains no 'a' and so is a complete key
    assert keys == ["foo"]

    prefixes = _get_prefixes(response)
    assert len(prefixes) == 2
    # bar, baz, and cab should be broken up by the 'a' delimiters
    assert prefixes == ["b.", "c."]


def test_bucket_list_delimiter_unreadable():
    key_names = ["bar", "baz", "cab", "foo"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Delimiter="\x0a")
    assert response["Delimiter"] == "\x0a"

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == key_names
    assert prefixes == []


@pytest.mark.list_objects_v2
def test_bucket_listv2_delimiter_unreadable():
    key_names = ["bar", "baz", "cab", "foo"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, Delimiter="\x0a")
    assert response["Delimiter"] == "\x0a"

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == key_names
    assert prefixes == []


def test_bucket_list_delimiter_empty():
    key_names = ["bar", "baz", "cab", "foo"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Delimiter="")
    # putting an empty value into Delimiter will not return a value in the response
    assert not "Delimiter" in response

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == key_names
    assert prefixes == []


@pytest.mark.list_objects_v2
def test_bucket_listv2_delimiter_empty():
    key_names = ["bar", "baz", "cab", "foo"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, Delimiter="")
    # putting an empty value into Delimiter will not return a value in the response
    assert not "Delimiter" in response

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == key_names
    assert prefixes == []


def test_bucket_list_delimiter_none():
    key_names = ["bar", "baz", "cab", "foo"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name)
    # putting an empty value into Delimiter will not return a value in the response
    assert not "Delimiter" in response

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == key_names
    assert prefixes == []


@pytest.mark.list_objects_v2
def test_bucket_listv2_delimiter_none():
    key_names = ["bar", "baz", "cab", "foo"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name)
    # putting an empty value into Delimiter will not return a value in the response
    assert not "Delimiter" in response

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == key_names
    assert prefixes == []


@pytest.mark.list_objects_v2
def test_bucket_listv2_fetchowner_notempty():
    key_names = ["foo/bar", "foo/baz", "quux"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, FetchOwner=True)
    objs_list = response["Contents"]
    assert "Owner" in objs_list[0]


@pytest.mark.list_objects_v2
def test_bucket_listv2_fetchowner_defaultempty():
    key_names = ["foo/bar", "foo/baz", "quux"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name)
    objs_list = response["Contents"]
    assert not "Owner" in objs_list[0]


@pytest.mark.list_objects_v2
def test_bucket_listv2_fetchowner_empty():
    key_names = ["foo/bar", "foo/baz", "quux"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, FetchOwner=False)
    objs_list = response["Contents"]
    assert not "Owner" in objs_list[0]


def test_bucket_list_delimiter_not_exist():
    key_names = ["bar", "baz", "cab", "foo"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Delimiter="/")
    # putting an empty value into Delimiter will not return a value in the response
    assert response["Delimiter"] == "/"

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == key_names
    assert prefixes == []


@pytest.mark.list_objects_v2
def test_bucket_listv2_delimiter_not_exist():
    key_names = ["bar", "baz", "cab", "foo"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, Delimiter="/")
    # putting an empty value into Delimiter will not return a value in the response
    assert response["Delimiter"] == "/"

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == key_names
    assert prefixes == []


@pytest.mark.fails_on_dbstore
def test_bucket_list_delimiter_not_skip_special():
    key_names = ["0/"] + ["0/%s" % i for i in range(1000, 1999)]
    key_names2 = ["1999", "1999#", "1999+", "2000"]
    key_names += key_names2
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Delimiter="/")
    assert response["Delimiter"] == "/"

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == key_names2
    assert prefixes == ["0/"]


def test_bucket_list_prefix_basic():
    key_names = ["foo/bar", "foo/baz", "quux"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Prefix="foo/")
    assert response["Prefix"] == "foo/"

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == ["foo/bar", "foo/baz"]
    assert prefixes == []


@pytest.mark.list_objects_v2
def test_bucket_listv2_prefix_basic():
    key_names = ["foo/bar", "foo/baz", "quux"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, Prefix="foo/")
    assert response["Prefix"] == "foo/"

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == ["foo/bar", "foo/baz"]
    assert prefixes == []


# just testing that we can do the delimeter and prefix logic on non-slashes
def test_bucket_list_prefix_alt():
    key_names = ["bar", "baz", "foo"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Prefix="ba")
    assert response["Prefix"] == "ba"

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == ["bar", "baz"]
    assert prefixes == []


@pytest.mark.list_objects_v2
def test_bucket_listv2_prefix_alt():
    key_names = ["bar", "baz", "foo"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, Prefix="ba")
    assert response["Prefix"] == "ba"

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == ["bar", "baz"]
    assert prefixes == []


def test_bucket_list_prefix_empty():
    key_names = ["foo/bar", "foo/baz", "quux"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Prefix="")
    assert response["Prefix"] == ""

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == key_names
    assert prefixes == []


@pytest.mark.list_objects_v2
def test_bucket_listv2_prefix_empty():
    key_names = ["foo/bar", "foo/baz", "quux"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, Prefix="")
    assert response["Prefix"] == ""

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == key_names
    assert prefixes == []


def test_bucket_list_prefix_none():
    key_names = ["foo/bar", "foo/baz", "quux"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Prefix="")
    assert response["Prefix"] == ""

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == key_names
    assert prefixes == []


@pytest.mark.list_objects_v2
def test_bucket_listv2_prefix_none():
    key_names = ["foo/bar", "foo/baz", "quux"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, Prefix="")
    assert response["Prefix"] == ""

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == key_names
    assert prefixes == []


def test_bucket_list_prefix_not_exist():
    key_names = ["foo/bar", "foo/baz", "quux"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Prefix="d")
    assert response["Prefix"] == "d"

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == []
    assert prefixes == []


@pytest.mark.list_objects_v2
def test_bucket_listv2_prefix_not_exist():
    key_names = ["foo/bar", "foo/baz", "quux"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, Prefix="d")
    assert response["Prefix"] == "d"

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == []
    assert prefixes == []


def test_bucket_list_prefix_unreadable():
    key_names = ["foo/bar", "foo/baz", "quux"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Prefix="\x0a")
    assert response["Prefix"] == "\x0a"

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == []
    assert prefixes == []


@pytest.mark.list_objects_v2
def test_bucket_listv2_prefix_unreadable():
    key_names = ["foo/bar", "foo/baz", "quux"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, Prefix="\x0a")
    assert response["Prefix"] == "\x0a"

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == []
    assert prefixes == []


def test_bucket_list_prefix_delimiter_basic():
    key_names = ["foo/bar", "foo/baz/xyzzy", "quux/thud", "asdf"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Delimiter="/", Prefix="foo/")
    assert response["Prefix"] == "foo/"
    assert response["Delimiter"] == "/"

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == ["foo/bar"]
    assert prefixes == ["foo/baz/"]


@pytest.mark.list_objects_v2
def test_bucket_listv2_prefix_delimiter_basic():
    key_names = ["foo/bar", "foo/baz/xyzzy", "quux/thud", "asdf"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, Delimiter="/", Prefix="foo/")
    assert response["Prefix"] == "foo/"
    assert response["Delimiter"] == "/"

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == ["foo/bar"]
    assert prefixes == ["foo/baz/"]


def test_bucket_list_prefix_delimiter_alt():
    key_names = ["bar", "bazar", "cab", "foo"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Delimiter="a", Prefix="ba")
    assert response["Prefix"] == "ba"
    assert response["Delimiter"] == "a"

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == ["bar"]
    assert prefixes == ["baza"]


@pytest.mark.list_objects_v2
def test_bucket_listv2_prefix_delimiter_alt():
    key_names = ["bar", "bazar", "cab", "foo"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, Delimiter="a", Prefix="ba")
    assert response["Prefix"] == "ba"
    assert response["Delimiter"] == "a"

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == ["bar"]
    assert prefixes == ["baza"]


def test_bucket_list_prefix_delimiter_prefix_not_exist():
    key_names = ["b/a/r", "b/a/c", "b/a/g", "g"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Delimiter="d", Prefix="/")

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == []
    assert prefixes == []


@pytest.mark.list_objects_v2
def test_bucket_listv2_prefix_delimiter_prefix_not_exist():
    key_names = ["b/a/r", "b/a/c", "b/a/g", "g"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, Delimiter="d", Prefix="/")

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == []
    assert prefixes == []


def test_bucket_list_prefix_delimiter_delimiter_not_exist():
    key_names = ["b/a/c", "b/a/g", "b/a/r", "g"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Delimiter="z", Prefix="b")

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == ["b/a/c", "b/a/g", "b/a/r"]
    assert prefixes == []


@pytest.mark.list_objects_v2
def test_bucket_listv2_prefix_delimiter_delimiter_not_exist():
    key_names = ["b/a/c", "b/a/g", "b/a/r", "g"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, Delimiter="z", Prefix="b")

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == ["b/a/c", "b/a/g", "b/a/r"]
    assert prefixes == []


def test_bucket_list_prefix_delimiter_prefix_delimiter_not_exist():
    key_names = ["b/a/c", "b/a/g", "b/a/r", "g"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Delimiter="z", Prefix="y")

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == []
    assert prefixes == []


@pytest.mark.list_objects_v2
def test_bucket_listv2_prefix_delimiter_prefix_delimiter_not_exist():
    key_names = ["b/a/c", "b/a/g", "b/a/r", "g"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, Delimiter="z", Prefix="y")

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == []
    assert prefixes == []


@pytest.mark.fails_on_dbstore
def test_bucket_list_maxkeys_one():
    key_names = ["bar", "baz", "foo", "quxx"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, MaxKeys=1)
    assert response["IsTruncated"] == True

    keys = _get_keys(response)
    assert keys == key_names[0:1]

    response = client.list_objects(Bucket=bucket_name, Marker=key_names[0])
    assert response["IsTruncated"] == False

    keys = _get_keys(response)
    assert keys == key_names[1:]


@pytest.mark.list_objects_v2
@pytest.mark.fails_on_dbstore
def test_bucket_listv2_maxkeys_one():
    key_names = ["bar", "baz", "foo", "quxx"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, MaxKeys=1)
    assert response["IsTruncated"] == True

    keys = _get_keys(response)
    assert keys == key_names[0:1]

    response = client.list_objects_v2(Bucket=bucket_name, StartAfter=key_names[0])
    assert response["IsTruncated"] == False

    keys = _get_keys(response)
    assert keys == key_names[1:]


def test_bucket_list_maxkeys_zero():
    key_names = ["bar", "baz", "foo", "quxx"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, MaxKeys=0)

    assert response["IsTruncated"] == False
    keys = _get_keys(response)
    assert keys == []


@pytest.mark.list_objects_v2
def test_bucket_listv2_maxkeys_zero():
    key_names = ["bar", "baz", "foo", "quxx"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, MaxKeys=0)

    assert response["IsTruncated"] == False
    keys = _get_keys(response)
    assert keys == []


def test_bucket_list_maxkeys_none():
    key_names = ["bar", "baz", "foo", "quxx"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name)
    assert response["IsTruncated"] == False
    keys = _get_keys(response)
    assert keys == key_names
    assert response["MaxKeys"] == 1000


@pytest.mark.list_objects_v2
def test_bucket_listv2_maxkeys_none():
    key_names = ["bar", "baz", "foo", "quxx"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name)
    assert response["IsTruncated"] == False
    keys = _get_keys(response)
    assert keys == key_names
    assert response["MaxKeys"] == 1000


def get_http_response_body(**kwargs):
    global http_response_body
    http_response_body = kwargs["http_response"].__dict__["_content"]


def parseXmlToJson(xml):
    response = {}

    for child in list(xml):
        if len(list(child)) > 0:
            response[child.tag] = parseXmlToJson(child)
        else:
            response[child.tag] = child.text or ""

        # one-liner equivalent
        # response[child.tag] = parseXmlToJson(child) if len(list(child)) > 0 else child.text or ''

    return response


@pytest.mark.fails_on_aws
def test_account_usage():
    # boto3.set_stream_logger(name='botocore')
    client = get_client()

    # adds the unordered query parameter
    def add_usage(**kwargs):
        kwargs["params"]["url"] += "?usage"

    client.meta.events.register("before-call.s3.ListBuckets", add_usage)
    client.meta.events.register("after-call.s3.ListBuckets", get_http_response_body)
    client.list_buckets()
    xml = ET.fromstring(http_response_body.decode("utf-8"))
    parsed = parseXmlToJson(xml)
    summary = parsed["Summary"]
    assert summary["QuotaMaxBytes"] == "-1"
    assert summary["QuotaMaxBuckets"] == "1000"
    assert summary["QuotaMaxObjCount"] == "-1"
    assert summary["QuotaMaxBytesPerBucket"] == "-1"
    assert summary["QuotaMaxObjCountPerBucket"] == "-1"


@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
def test_head_bucket_usage():
    # boto3.set_stream_logger(name='botocore')
    client = get_client()
    bucket_name = _create_objects(keys=["foo"])
    # adds the unordered query parameter
    client.meta.events.register("after-call.s3.HeadBucket", get_http_response)
    client.head_bucket(Bucket=bucket_name)
    hdrs = http_response["headers"]
    assert hdrs["X-RGW-Object-Count"] == "1"
    assert hdrs["X-RGW-Bytes-Used"] == "3"
    assert hdrs["X-RGW-Quota-User-Size"] == "-1"
    assert hdrs["X-RGW-Quota-User-Objects"] == "-1"
    assert hdrs["X-RGW-Quota-Max-Buckets"] == "1000"
    assert hdrs["X-RGW-Quota-Bucket-Size"] == "-1"
    assert hdrs["X-RGW-Quota-Bucket-Objects"] == "-1"


@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
def test_bucket_list_unordered():
    # boto3.set_stream_logger(name='botocore')
    keys_in = [
        "ado",
        "bot",
        "cob",
        "dog",
        "emu",
        "fez",
        "gnu",
        "hex",
        "abc/ink",
        "abc/jet",
        "abc/kin",
        "abc/lax",
        "abc/mux",
        "def/nim",
        "def/owl",
        "def/pie",
        "def/qed",
        "def/rye",
        "ghi/sew",
        "ghi/tor",
        "ghi/uke",
        "ghi/via",
        "ghi/wit",
        "xix",
        "yak",
        "zoo",
    ]
    bucket_name = _create_objects(keys=keys_in)
    client = get_client()

    # adds the unordered query parameter
    def add_unordered(**kwargs):
        kwargs["params"]["url"] += "&allow-unordered=true"

    client.meta.events.register("before-call.s3.ListObjects", add_unordered)

    # test simple retrieval
    response = client.list_objects(Bucket=bucket_name, MaxKeys=1000)
    unordered_keys_out = _get_keys(response)
    assert len(keys_in) == len(unordered_keys_out)
    assert keys_in.sort() == unordered_keys_out.sort()

    # test retrieval with prefix
    response = client.list_objects(Bucket=bucket_name, MaxKeys=1000, Prefix="abc/")
    unordered_keys_out = _get_keys(response)
    assert 5 == len(unordered_keys_out)

    # test incremental retrieval with marker
    response = client.list_objects(Bucket=bucket_name, MaxKeys=6)
    unordered_keys_out = _get_keys(response)
    assert 6 == len(unordered_keys_out)

    # now get the next bunch
    response = client.list_objects(
        Bucket=bucket_name, MaxKeys=6, Marker=unordered_keys_out[-1]
    )
    unordered_keys_out2 = _get_keys(response)
    assert 6 == len(unordered_keys_out2)

    # make sure there's no overlap between the incremental retrievals
    intersect = set(unordered_keys_out).intersection(unordered_keys_out2)
    assert 0 == len(intersect)

    # verify that unordered used with delimiter results in error
    e = assert_raises(
        ClientError, client.list_objects, Bucket=bucket_name, Delimiter="/"
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == "InvalidArgument"


@pytest.mark.fails_on_aws
@pytest.mark.list_objects_v2
@pytest.mark.fails_on_dbstore
def test_bucket_listv2_unordered():
    # boto3.set_stream_logger(name='botocore')
    keys_in = [
        "ado",
        "bot",
        "cob",
        "dog",
        "emu",
        "fez",
        "gnu",
        "hex",
        "abc/ink",
        "abc/jet",
        "abc/kin",
        "abc/lax",
        "abc/mux",
        "def/nim",
        "def/owl",
        "def/pie",
        "def/qed",
        "def/rye",
        "ghi/sew",
        "ghi/tor",
        "ghi/uke",
        "ghi/via",
        "ghi/wit",
        "xix",
        "yak",
        "zoo",
    ]
    bucket_name = _create_objects(keys=keys_in)
    client = get_client()

    # adds the unordered query parameter
    def add_unordered(**kwargs):
        kwargs["params"]["url"] += "&allow-unordered=true"

    client.meta.events.register("before-call.s3.ListObjects", add_unordered)

    # test simple retrieval
    response = client.list_objects_v2(Bucket=bucket_name, MaxKeys=1000)
    unordered_keys_out = _get_keys(response)
    assert len(keys_in) == len(unordered_keys_out)
    assert keys_in.sort() == unordered_keys_out.sort()

    # test retrieval with prefix
    response = client.list_objects_v2(Bucket=bucket_name, MaxKeys=1000, Prefix="abc/")
    unordered_keys_out = _get_keys(response)
    assert 5 == len(unordered_keys_out)

    # test incremental retrieval with marker
    response = client.list_objects_v2(Bucket=bucket_name, MaxKeys=6)
    unordered_keys_out = _get_keys(response)
    assert 6 == len(unordered_keys_out)

    # now get the next bunch
    response = client.list_objects_v2(
        Bucket=bucket_name, MaxKeys=6, StartAfter=unordered_keys_out[-1]
    )
    unordered_keys_out2 = _get_keys(response)
    assert 6 == len(unordered_keys_out2)

    # make sure there's no overlap between the incremental retrievals
    intersect = set(unordered_keys_out).intersection(unordered_keys_out2)
    assert 0 == len(intersect)

    # verify that unordered used with delimiter results in error
    e = assert_raises(
        ClientError, client.list_objects, Bucket=bucket_name, Delimiter="/"
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == "InvalidArgument"


def test_bucket_list_maxkeys_invalid():
    key_names = ["bar", "baz", "foo", "quxx"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    # adds invalid max keys to url
    # before list_objects is called
    def add_invalid_maxkeys(**kwargs):
        kwargs["params"]["url"] += "&max-keys=blah"

    client.meta.events.register("before-call.s3.ListObjects", add_invalid_maxkeys)

    e = assert_raises(ClientError, client.list_objects, Bucket=bucket_name)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == "InvalidArgument"


def test_bucket_list_marker_none():
    key_names = ["bar", "baz", "foo", "quxx"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name)
    assert response["Marker"] == ""


def test_bucket_list_marker_empty():
    key_names = ["bar", "baz", "foo", "quxx"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Marker="")
    assert response["Marker"] == ""
    assert response["IsTruncated"] == False
    keys = _get_keys(response)
    assert keys == key_names


@pytest.mark.list_objects_v2
@pytest.mark.skip(reason="Potential Bug")
def test_bucket_listv2_continuationtoken_empty():
    key_names = ["bar", "baz", "foo", "quxx"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, ContinuationToken="")
    assert response["ContinuationToken"] == ""
    assert response["IsTruncated"] == False
    keys = _get_keys(response)
    assert keys == key_names


@pytest.mark.list_objects_v2
def test_bucket_listv2_continuationtoken():
    key_names = ["bar", "baz", "foo", "quxx"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response1 = client.list_objects_v2(Bucket=bucket_name, MaxKeys=1)
    next_continuation_token = response1["NextContinuationToken"]

    response2 = client.list_objects_v2(
        Bucket=bucket_name, ContinuationToken=next_continuation_token
    )
    assert response2["ContinuationToken"] == next_continuation_token
    assert response2["IsTruncated"] == False
    key_names2 = ["baz", "foo", "quxx"]
    keys = _get_keys(response2)
    assert keys == key_names2


@pytest.mark.list_objects_v2
@pytest.mark.fails_on_dbstore
def test_bucket_listv2_both_continuationtoken_startafter():
    key_names = ["bar", "baz", "foo", "quxx"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response1 = client.list_objects_v2(Bucket=bucket_name, StartAfter="bar", MaxKeys=1)
    next_continuation_token = response1["NextContinuationToken"]

    response2 = client.list_objects_v2(
        Bucket=bucket_name, StartAfter="bar", ContinuationToken=next_continuation_token
    )
    assert response2["ContinuationToken"] == next_continuation_token
    assert response2["StartAfter"] == "bar"
    assert response2["IsTruncated"] == False
    key_names2 = ["foo", "quxx"]
    keys = _get_keys(response2)
    assert keys == key_names2


def test_bucket_list_marker_unreadable():
    key_names = ["bar", "baz", "foo", "quxx"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Marker="\x0a")
    assert response["Marker"] == "\x0a"
    assert response["IsTruncated"] == False
    keys = _get_keys(response)
    assert keys == key_names


@pytest.mark.list_objects_v2
def test_bucket_listv2_startafter_unreadable():
    key_names = ["bar", "baz", "foo", "quxx"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, StartAfter="\x0a")
    assert response["StartAfter"] == "\x0a"
    assert response["IsTruncated"] == False
    keys = _get_keys(response)
    assert keys == key_names


def test_bucket_list_marker_not_in_list():
    key_names = ["bar", "baz", "foo", "quxx"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Marker="blah")
    assert response["Marker"] == "blah"
    keys = _get_keys(response)
    assert keys == ["foo", "quxx"]


@pytest.mark.list_objects_v2
def test_bucket_listv2_startafter_not_in_list():
    key_names = ["bar", "baz", "foo", "quxx"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, StartAfter="blah")
    assert response["StartAfter"] == "blah"
    keys = _get_keys(response)
    assert keys == ["foo", "quxx"]


def test_bucket_list_marker_after_list():
    key_names = ["bar", "baz", "foo", "quxx"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Marker="zzz")
    assert response["Marker"] == "zzz"
    keys = _get_keys(response)
    assert response["IsTruncated"] == False
    assert keys == []


@pytest.mark.list_objects_v2
def test_bucket_listv2_startafter_after_list():
    key_names = ["bar", "baz", "foo", "quxx"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, StartAfter="zzz")
    assert response["StartAfter"] == "zzz"
    keys = _get_keys(response)
    assert response["IsTruncated"] == False
    assert keys == []


def _compare_dates(datetime1, datetime2):
    """
    changes ms from datetime1 to 0, compares it to datetime2
    """
    # both times are in datetime format but datetime1 has
    # microseconds and datetime2 does not
    datetime1 = datetime1.replace(microsecond=0)
    assert datetime1 == datetime2


@pytest.mark.fails_on_dbstore
def test_bucket_list_return_data():
    key_names = ["bar", "baz", "foo"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    data = {}
    for key_name in key_names:
        obj_response = client.head_object(Bucket=bucket_name, Key=key_name)
        acl_response = client.get_object_acl(Bucket=bucket_name, Key=key_name)
        data.update(
            {
                key_name: {
                    "DisplayName": acl_response["Owner"]["DisplayName"],
                    "ID": acl_response["Owner"]["ID"],
                    "ETag": obj_response["ETag"],
                    "LastModified": obj_response["LastModified"],
                    "ContentLength": obj_response["ContentLength"],
                }
            }
        )

    response = client.list_objects(Bucket=bucket_name)
    objs_list = response["Contents"]
    for obj in objs_list:
        key_name = obj["Key"]
        key_data = data[key_name]
        assert obj["ETag"] == key_data["ETag"]
        assert obj["Size"] == key_data["ContentLength"]
        assert obj["Owner"]["DisplayName"] == key_data["DisplayName"]
        assert obj["Owner"]["ID"] == key_data["ID"]
        _compare_dates(obj["LastModified"], key_data["LastModified"])


def test_bucket_list_return_data_versioning():
    bucket = get_new_bucket_resource()
    check_configure_versioning_retry(bucket.name, "Enabled", "Enabled")
    key_names = ["bar", "baz", "foo"]
    _create_objects(bucket=bucket, bucket_name=bucket.name, keys=key_names)

    client = get_client()
    data = {}

    for key_name in key_names:
        obj_response = client.head_object(Bucket=bucket.name, Key=key_name)
        acl_response = client.get_object_acl(Bucket=bucket.name, Key=key_name)
        data.update(
            {
                key_name: {
                    "ID": acl_response["Owner"]["ID"],
                    "DisplayName": acl_response["Owner"]["DisplayName"],
                    "ETag": obj_response["ETag"],
                    "LastModified": obj_response["LastModified"],
                    "ContentLength": obj_response["ContentLength"],
                    "VersionId": obj_response["VersionId"],
                }
            }
        )

    response = client.list_object_versions(Bucket=bucket.name)
    objs_list = response["Versions"]

    for obj in objs_list:
        key_name = obj["Key"]
        key_data = data[key_name]
        assert obj["Owner"]["DisplayName"] == key_data["DisplayName"]
        assert obj["ETag"] == key_data["ETag"]
        assert obj["Size"] == key_data["ContentLength"]
        assert obj["Owner"]["ID"] == key_data["ID"]
        assert obj["VersionId"] == key_data["VersionId"]
        _compare_dates(obj["LastModified"], key_data["LastModified"])


def test_bucket_list_objects_anonymous():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_bucket_acl(Bucket=bucket_name, ACL="public-read")

    unauthenticated_client = get_unauthenticated_client()
    unauthenticated_client.list_objects(Bucket=bucket_name)


@pytest.mark.list_objects_v2
def test_bucket_listv2_objects_anonymous():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_bucket_acl(Bucket=bucket_name, ACL="public-read")

    unauthenticated_client = get_unauthenticated_client()
    unauthenticated_client.list_objects_v2(Bucket=bucket_name)


def test_bucket_list_objects_anonymous_fail():
    bucket_name = get_new_bucket()

    unauthenticated_client = get_unauthenticated_client()
    e = assert_raises(
        ClientError, unauthenticated_client.list_objects, Bucket=bucket_name
    )

    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == "AccessDenied"


@pytest.mark.list_objects_v2
def test_bucket_listv2_objects_anonymous_fail():
    bucket_name = get_new_bucket()

    unauthenticated_client = get_unauthenticated_client()
    e = assert_raises(
        ClientError, unauthenticated_client.list_objects_v2, Bucket=bucket_name
    )

    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == "AccessDenied"


def test_bucket_notexist():
    bucket_name = get_new_bucket_name()
    client = get_client()

    e = assert_raises(ClientError, client.list_objects, Bucket=bucket_name)

    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == "NoSuchBucket"


@pytest.mark.list_objects_v2
def test_bucketv2_notexist():
    bucket_name = get_new_bucket_name()
    client = get_client()

    e = assert_raises(ClientError, client.list_objects_v2, Bucket=bucket_name)

    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == "NoSuchBucket"


def test_bucket_delete_notexist():
    bucket_name = get_new_bucket_name()
    client = get_client()

    e = assert_raises(ClientError, client.delete_bucket, Bucket=bucket_name)

    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == "NoSuchBucket"


def test_bucket_delete_nonempty():
    key_names = ["foo"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    e = assert_raises(ClientError, client.delete_bucket, Bucket=bucket_name)

    status, error_code = _get_status_and_error_code(e.response)
    assert status == 409
    assert error_code == "BucketNotEmpty"


def _do_set_bucket_canned_acl(client, bucket_name, canned_acl, i, results):
    try:
        client.put_bucket_acl(ACL=canned_acl, Bucket=bucket_name)
        results[i] = True
    except:
        results[i] = False


def _do_set_bucket_canned_acl_concurrent(client, bucket_name, canned_acl, num, results):
    t = []
    for i in range(num):
        thr = threading.Thread(
            target=_do_set_bucket_canned_acl,
            args=(client, bucket_name, canned_acl, i, results),
        )
        thr.start()
        t.append(thr)
    return t


def _do_wait_completion(t):
    for thr in t:
        thr.join()


def test_bucket_concurrent_set_canned_acl():
    bucket_name = get_new_bucket()
    client = get_client()

    num_threads = (
        50  # boto2 retry defaults to 5 so we need a thread to fail at least 5 times
    )
    # this seems like a large enough number to get through retry (if bug
    # exists)
    results = [None] * num_threads

    t = _do_set_bucket_canned_acl_concurrent(
        client, bucket_name, "public-read", num_threads, results
    )
    _do_wait_completion(t)

    for r in results:
        assert r == True


def test_object_write_to_nonexist_bucket():
    key_names = ["foo"]
    bucket_name = "whatchutalkinboutwillis"
    client = get_client()

    e = assert_raises(
        ClientError, client.put_object, Bucket=bucket_name, Key="foo", Body="foo"
    )

    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == "NoSuchBucket"


def _ev_add_te_header(request, **kwargs):
    request.headers.add_header("Transfer-Encoding", "chunked")


@pytest.mark.skip(reason="Potential Bug")
def test_object_write_with_chunked_transfer_encoding():
    bucket_name = get_new_bucket()
    client = get_client()

    client.meta.events.register_first("before-sign.*.*", _ev_add_te_header)
    response = client.put_object(Bucket=bucket_name, Key="foo", Body="bar")

    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_bucket_create_delete():
    bucket_name = get_new_bucket()
    client = get_client()
    client.delete_bucket(Bucket=bucket_name)

    e = assert_raises(ClientError, client.delete_bucket, Bucket=bucket_name)

    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == "NoSuchBucket"


def test_object_read_not_exist():
    bucket_name = get_new_bucket()
    client = get_client()

    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key="bar")

    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == "NoSuchKey"


http_response = None


def get_http_response(**kwargs):
    global http_response
    http_response = kwargs["http_response"].__dict__


@pytest.mark.fails_on_dbstore
def test_object_requestid_matches_header_on_error():
    bucket_name = get_new_bucket()
    client = get_client()

    # get http response after failed request
    client.meta.events.register("after-call.s3.GetObject", get_http_response)
    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key="bar")

    response_body = http_response["_content"]
    resp_body_xml = ET.fromstring(response_body)
    request_id = resp_body_xml.find(".//RequestId").text

    assert request_id is not None
    assert request_id == e.response["ResponseMetadata"]["RequestId"]


def _make_objs_dict(key_names):
    objs_list = []
    for key in key_names:
        obj_dict = {"Key": key}
        objs_list.append(obj_dict)
    objs_dict = {"Objects": objs_list}
    return objs_dict


def test_versioning_concurrent_multi_object_delete():
    num_objects = 5
    num_threads = 5
    bucket = get_new_bucket_resource()

    check_configure_versioning_retry(bucket.name, "Enabled", "Enabled")

    key_names = ["key_{:d}".format(x) for x in range(num_objects)]
    _create_objects(bucket=bucket, bucket_name=bucket.name, keys=key_names)

    client = get_client()
    versions = client.list_object_versions(Bucket=bucket.name)["Versions"]
    assert len(versions) == num_objects
    objs_dict = {
        "Objects": [dict((k, v[k]) for k in ["Key", "VersionId"]) for v in versions]
    }
    results = [None] * num_threads

    def do_request(n):
        results[n] = client.delete_objects(Bucket=bucket.name, Delete=objs_dict)

    t = []
    for i in range(num_threads):
        thr = threading.Thread(target=do_request, args=[i])
        thr.start()
        t.append(thr)
    _do_wait_completion(t)

    response = client.list_objects(Bucket=bucket.name)
    assert "Contents" not in response


def test_multi_object_delete():
    key_names = ["key0", "key1", "key2"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()
    response = client.list_objects(Bucket=bucket_name)
    assert len(response["Contents"]) == 3

    objs_dict = _make_objs_dict(key_names=key_names)
    response = client.delete_objects(Bucket=bucket_name, Delete=objs_dict)

    assert len(response["Deleted"]) == 3
    assert "Errors" not in response
    response = client.list_objects(Bucket=bucket_name)
    assert "Contents" not in response

    response = client.delete_objects(Bucket=bucket_name, Delete=objs_dict)
    assert len(response["Deleted"]) == 3
    assert "Errors" not in response
    response = client.list_objects(Bucket=bucket_name)
    assert "Contents" not in response


@pytest.mark.list_objects_v2
def test_multi_objectv2_delete():
    key_names = ["key0", "key1", "key2"]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()
    response = client.list_objects_v2(Bucket=bucket_name)
    assert len(response["Contents"]) == 3

    objs_dict = _make_objs_dict(key_names=key_names)
    response = client.delete_objects(Bucket=bucket_name, Delete=objs_dict)

    assert len(response["Deleted"]) == 3
    assert "Errors" not in response
    response = client.list_objects_v2(Bucket=bucket_name)
    assert "Contents" not in response

    response = client.delete_objects(Bucket=bucket_name, Delete=objs_dict)
    assert len(response["Deleted"]) == 3
    assert "Errors" not in response
    response = client.list_objects_v2(Bucket=bucket_name)
    assert "Contents" not in response


def test_multi_object_delete_key_limit():
    key_names = [f"key-{i}" for i in range(1001)]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    paginator = client.get_paginator("list_objects")
    pages = paginator.paginate(Bucket=bucket_name)
    numKeys = 0
    for page in pages:
        numKeys += len(page["Contents"])
    assert numKeys == 1001

    objs_dict = _make_objs_dict(key_names=key_names)
    e = assert_raises(
        ClientError, client.delete_objects, Bucket=bucket_name, Delete=objs_dict
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/891")
def test_multi_objectv2_delete_key_limit():
    key_names = [f"key-{i}" for i in range(1001)]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    paginator = client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket_name)
    numKeys = 0
    for page in pages:
        numKeys += len(page["Contents"])
    assert numKeys == 1001

    objs_dict = _make_objs_dict(key_names=key_names)
    e = assert_raises(
        ClientError, client.delete_objects, Bucket=bucket_name, Delete=objs_dict
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400


def test_object_head_zero_bytes():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_object(Bucket=bucket_name, Key="foo", Body="")

    response = client.head_object(Bucket=bucket_name, Key="foo")
    assert response["ContentLength"] == 0


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/845")
def test_object_write_check_etag():
    bucket_name = get_new_bucket()
    client = get_client()
    response = client.put_object(Bucket=bucket_name, Key="foo", Body="bar")
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert response["ETag"] == '"37b51d194a7513e45b56f6524f2d51f2"'


def test_object_write_cache_control():
    bucket_name = get_new_bucket()
    client = get_client()
    cache_control = "public, max-age=14400"
    client.put_object(
        Bucket=bucket_name, Key="foo", Body="bar", CacheControl=cache_control
    )

    response = client.head_object(Bucket=bucket_name, Key="foo")
    assert response["ResponseMetadata"]["HTTPHeaders"]["cache-control"] == cache_control


def test_object_write_expires():
    bucket_name = get_new_bucket()
    client = get_client()

    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)
    client.put_object(Bucket=bucket_name, Key="foo", Body="bar", Expires=expires)

    response = client.head_object(Bucket=bucket_name, Key="foo")
    _compare_dates(expires, response["Expires"])


def _get_body(response):
    body = response["Body"]
    got = body.read()
    if type(got) is bytes:
        got = got.decode()
    return got


def test_object_write_read_update_read_delete():
    bucket_name = get_new_bucket()
    client = get_client()

    # Write
    client.put_object(Bucket=bucket_name, Key="foo", Body="bar")
    # Read
    response = client.get_object(Bucket=bucket_name, Key="foo")
    body = _get_body(response)
    assert body == "bar"
    # Update
    client.put_object(Bucket=bucket_name, Key="foo", Body="soup")
    # Read
    response = client.get_object(Bucket=bucket_name, Key="foo")
    body = _get_body(response)
    assert body == "soup"
    # Delete
    client.delete_object(Bucket=bucket_name, Key="foo")


def _set_get_metadata(metadata, bucket_name=None):
    """
    create a new bucket new or use an existing
    name to create an object that bucket,
    set the meta1 property to a specified, value,
    and then re-read and return that property
    """
    if bucket_name is None:
        bucket_name = get_new_bucket()

    client = get_client()
    metadata_dict = {"meta1": metadata}
    client.put_object(Bucket=bucket_name, Key="foo", Body="bar", Metadata=metadata_dict)

    response = client.get_object(Bucket=bucket_name, Key="foo")
    return response["Metadata"]["meta1"]


def test_object_set_get_metadata_none_to_good():
    got = _set_get_metadata("mymeta")
    assert got == "mymeta"


def test_object_set_get_metadata_none_to_empty():
    e = assert_raises(ClientError, _set_get_metadata, metadata="")
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == "InvalidArgument"


def test_object_set_get_metadata_overwrite_to_empty():
    bucket_name = get_new_bucket()
    got = _set_get_metadata("oldmeta", bucket_name)
    assert got == "oldmeta"
    e = assert_raises(
        ClientError, _set_get_metadata, metadata="", bucket_name=bucket_name
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == "InvalidArgument"


# TODO: the decoding of this unicode metadata is not happening properly for unknown reasons
@pytest.mark.fails_on_rgw
@pytest.mark.skip(reason="Potential Bug")
def test_object_set_get_unicode_metadata():
    bucket_name = get_new_bucket()
    client = get_client()

    def set_unicode_metadata(**kwargs):
        kwargs["params"]["headers"]["x-amz-meta-meta1"] = "Hello World\xe9"

    client.meta.events.register("before-call.s3.PutObject", set_unicode_metadata)
    client.put_object(Bucket=bucket_name, Key="foo", Body="bar")

    response = client.get_object(Bucket=bucket_name, Key="foo")
    got = response["Metadata"]["meta1"]
    print(got)
    print("Hello World\xe9")
    assert got == "Hello World\xe9"


def _set_get_metadata_unreadable(metadata, bucket_name=None):
    """
    set and then read back a meta-data value (which presumably
    includes some interesting characters), and return a list
    containing the stored value AND the encoding with which it
    was returned.

    This should return a 400 bad request because the webserver
    rejects the request.
    """
    bucket_name = get_new_bucket()
    client = get_client()
    metadata_dict = {"meta1": metadata}
    e = assert_raises(
        ClientError,
        client.put_object,
        Bucket=bucket_name,
        Key="bar",
        Metadata=metadata_dict,
    )
    return e


def test_object_metadata_replaced_on_put():
    bucket_name = get_new_bucket()
    client = get_client()
    metadata_dict = {"meta1": "bar"}
    client.put_object(Bucket=bucket_name, Key="foo", Body="bar", Metadata=metadata_dict)

    client.put_object(Bucket=bucket_name, Key="foo", Body="bar")

    response = client.get_object(Bucket=bucket_name, Key="foo")
    got = response["Metadata"]
    assert got == {}


def test_object_write_file():
    bucket_name = get_new_bucket()
    client = get_client()
    data_str = "bar"
    data = bytes(data_str, "utf-8")
    client.put_object(Bucket=bucket_name, Key="foo", Body=data)
    response = client.get_object(Bucket=bucket_name, Key="foo")
    body = _get_body(response)
    assert body == "bar"


def _get_post_url(bucket_name):
    endpoint = get_config_endpoint()
    return "{endpoint}/{bucket_name}".format(endpoint=endpoint, bucket_name=bucket_name)


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/847")
def test_post_object_anonymous_request():
    bucket_name = get_new_bucket_name()
    client = get_client()
    url = _get_post_url(bucket_name)
    payload = OrderedDict(
        [
            ("key", "foo.txt"),
            ("acl", "public-read"),
            ("Content-Type", "text/plain"),
            ("file", ("bar")),
        ]
    )

    client.create_bucket(ACL="public-read-write", Bucket=bucket_name)
    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 204
    response = client.get_object(Bucket=bucket_name, Key="foo.txt")
    body = _get_body(response)
    assert body == "bar"


@pytest.mark.skip(reason="https://github.com/nspcc-dev/s3-tests/issues/46")
def test_post_object_authenticated_request():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {
        "expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "conditions": [
            {"bucket": bucket_name},
            ["starts-with", "$key", "foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["content-length-range", 0, 1024],
        ],
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, "utf-8")
    bytes_json_policy_document = bytes(json_policy_document, "utf-8")
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(
        hmac.new(bytes(aws_secret_access_key, "utf-8"), policy, hashlib.sha1).digest()
    )

    payload = OrderedDict(
        [
            ("key", "foo.txt"),
            ("AWSAccessKeyId", aws_access_key_id),
            ("acl", "private"),
            ("signature", signature),
            ("policy", policy),
            ("Content-Type", "text/plain"),
            ("file", ("bar")),
        ]
    )

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 204
    response = client.get_object(Bucket=bucket_name, Key="foo.txt")
    body = _get_body(response)
    assert body == "bar"


@pytest.mark.skip(reason="https://github.com/nspcc-dev/s3-tests/issues/46")
def test_post_object_authenticated_no_content_type():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(ACL="public-read-write", Bucket=bucket_name)

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {
        "expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "conditions": [
            {"bucket": bucket_name},
            ["starts-with", "$key", "foo"],
            {"acl": "private"},
            ["content-length-range", 0, 1024],
        ],
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, "utf-8")
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(
        hmac.new(bytes(aws_secret_access_key, "utf-8"), policy, hashlib.sha1).digest()
    )

    payload = OrderedDict(
        [
            ("key", "foo.txt"),
            ("AWSAccessKeyId", aws_access_key_id),
            ("acl", "private"),
            ("signature", signature),
            ("policy", policy),
            ("file", ("bar")),
        ]
    )

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 204
    response = client.get_object(Bucket=bucket_name, Key="foo.txt")
    body = _get_body(response)
    assert body == "bar"


@pytest.mark.skip(reason="https://github.com/nspcc-dev/s3-tests/issues/46")
def test_post_object_authenticated_request_bad_access_key():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(ACL="public-read-write", Bucket=bucket_name)

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {
        "expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "conditions": [
            {"bucket": bucket_name},
            ["starts-with", "$key", "foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["content-length-range", 0, 1024],
        ],
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, "utf-8")
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(
        hmac.new(bytes(aws_secret_access_key, "utf-8"), policy, hashlib.sha1).digest()
    )

    payload = OrderedDict(
        [
            ("key", "foo.txt"),
            ("AWSAccessKeyId", "foo"),
            ("acl", "private"),
            ("signature", signature),
            ("policy", policy),
            ("Content-Type", "text/plain"),
            ("file", ("bar")),
        ]
    )

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 403


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/847")
def test_post_object_set_success_code():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(ACL="public-read-write", Bucket=bucket_name)

    url = _get_post_url(bucket_name)
    payload = OrderedDict(
        [
            ("key", "foo.txt"),
            ("acl", "public-read"),
            ("success_action_status", "201"),
            ("Content-Type", "text/plain"),
            ("file", ("bar")),
        ]
    )

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 201
    message = ET.fromstring(r.content).find("Key")
    assert message.text == "foo.txt"


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/847")
def test_post_object_set_invalid_success_code():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(ACL="public-read-write", Bucket=bucket_name)

    url = _get_post_url(bucket_name)
    payload = OrderedDict(
        [
            ("key", "foo.txt"),
            ("acl", "public-read"),
            ("success_action_status", "404"),
            ("Content-Type", "text/plain"),
            ("file", ("bar")),
        ]
    )

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 204
    content = r.content.decode()
    assert content == ""


@pytest.mark.skip(reason="https://github.com/nspcc-dev/s3-tests/issues/46")
def test_post_object_upload_larger_than_chunk():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {
        "expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "conditions": [
            {"bucket": bucket_name},
            ["starts-with", "$key", "foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["content-length-range", 0, 5 * 1024 * 1024],
        ],
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, "utf-8")
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(
        hmac.new(bytes(aws_secret_access_key, "utf-8"), policy, hashlib.sha1).digest()
    )

    foo_string = "foo" * 1024 * 1024

    payload = OrderedDict(
        [
            ("key", "foo.txt"),
            ("AWSAccessKeyId", aws_access_key_id),
            ("acl", "private"),
            ("signature", signature),
            ("policy", policy),
            ("Content-Type", "text/plain"),
            ("file", foo_string),
        ]
    )

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 204
    response = client.get_object(Bucket=bucket_name, Key="foo.txt")
    body = _get_body(response)
    assert body == foo_string


@pytest.mark.skip(reason="https://github.com/nspcc-dev/s3-tests/issues/46")
def test_post_object_set_key_from_filename():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {
        "expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "conditions": [
            {"bucket": bucket_name},
            ["starts-with", "$key", "foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["content-length-range", 0, 1024],
        ],
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, "utf-8")
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(
        hmac.new(bytes(aws_secret_access_key, "utf-8"), policy, hashlib.sha1).digest()
    )

    payload = OrderedDict(
        [
            ("key", "${filename}"),
            ("AWSAccessKeyId", aws_access_key_id),
            ("acl", "private"),
            ("signature", signature),
            ("policy", policy),
            ("Content-Type", "text/plain"),
            ("file", ("foo.txt", "bar")),
        ]
    )

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 204
    response = client.get_object(Bucket=bucket_name, Key="foo.txt")
    body = _get_body(response)
    assert body == "bar"


@pytest.mark.skip(reason="https://github.com/nspcc-dev/s3-tests/issues/46")
def test_post_object_ignored_header():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {
        "expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "conditions": [
            {"bucket": bucket_name},
            ["starts-with", "$key", "foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["content-length-range", 0, 1024],
        ],
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, "utf-8")
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(
        hmac.new(bytes(aws_secret_access_key, "utf-8"), policy, hashlib.sha1).digest()
    )

    payload = OrderedDict(
        [
            ("key", "foo.txt"),
            ("AWSAccessKeyId", aws_access_key_id),
            ("acl", "private"),
            ("signature", signature),
            ("policy", policy),
            ("Content-Type", "text/plain"),
            ("x-ignore-foo", "bar"),
            ("file", ("bar")),
        ]
    )

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 204


@pytest.mark.skip(reason="https://github.com/nspcc-dev/s3-tests/issues/46")
def test_post_object_case_insensitive_condition_fields():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {
        "expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "conditions": [
            {"bUcKeT": bucket_name},
            ["StArTs-WiTh", "$KeY", "foo"],
            {"AcL": "private"},
            ["StArTs-WiTh", "$CoNtEnT-TyPe", "text/plain"],
            ["content-length-range", 0, 1024],
        ],
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, "utf-8")
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(
        hmac.new(bytes(aws_secret_access_key, "utf-8"), policy, hashlib.sha1).digest()
    )

    foo_string = "foo" * 1024 * 1024

    payload = OrderedDict(
        [
            ("kEy", "foo.txt"),
            ("AWSAccessKeyId", aws_access_key_id),
            ("aCl", "private"),
            ("signature", signature),
            ("pOLICy", policy),
            ("Content-Type", "text/plain"),
            ("file", ("bar")),
        ]
    )

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 204


@pytest.mark.skip(reason="https://github.com/nspcc-dev/s3-tests/issues/46")
def test_post_object_escaped_field_values():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {
        "expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "conditions": [
            {"bucket": bucket_name},
            ["starts-with", "$key", "\$foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["content-length-range", 0, 1024],
        ],
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, "utf-8")
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(
        hmac.new(bytes(aws_secret_access_key, "utf-8"), policy, hashlib.sha1).digest()
    )

    payload = OrderedDict(
        [
            ("key", "\$foo.txt"),
            ("AWSAccessKeyId", aws_access_key_id),
            ("acl", "private"),
            ("signature", signature),
            ("policy", policy),
            ("Content-Type", "text/plain"),
            ("file", ("bar")),
        ]
    )

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 204
    response = client.get_object(Bucket=bucket_name, Key="\$foo.txt")
    body = _get_body(response)
    assert body == "bar"


@pytest.mark.skip(reason="https://github.com/nspcc-dev/s3-tests/issues/46")
def test_post_object_success_redirect_action():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(ACL="public-read-write", Bucket=bucket_name)

    url = _get_post_url(bucket_name)
    redirect_url = _get_post_url(bucket_name)

    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {
        "expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "conditions": [
            {"bucket": bucket_name},
            ["starts-with", "$key", "foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["eq", "$success_action_redirect", redirect_url],
            ["content-length-range", 0, 1024],
        ],
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, "utf-8")
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(
        hmac.new(bytes(aws_secret_access_key, "utf-8"), policy, hashlib.sha1).digest()
    )

    payload = OrderedDict(
        [
            ("key", "foo.txt"),
            ("AWSAccessKeyId", aws_access_key_id),
            ("acl", "private"),
            ("signature", signature),
            ("policy", policy),
            ("Content-Type", "text/plain"),
            ("success_action_redirect", redirect_url),
            ("file", ("bar")),
        ]
    )

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 200
    url = r.url
    response = client.get_object(Bucket=bucket_name, Key="foo.txt")
    assert url == "{rurl}?bucket={bucket}&key={key}&etag=%22{etag}%22".format(
        rurl=redirect_url,
        bucket=bucket_name,
        key="foo.txt",
        etag=response["ETag"].strip('"'),
    )


@pytest.mark.skip(reason="https://github.com/nspcc-dev/s3-tests/issues/46")
def test_post_object_invalid_signature():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {
        "expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "conditions": [
            {"bucket": bucket_name},
            ["starts-with", "$key", "\$foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["content-length-range", 0, 1024],
        ],
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, "utf-8")
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(
        hmac.new(bytes(aws_secret_access_key, "utf-8"), policy, hashlib.sha1).digest()
    )[::-1]

    payload = OrderedDict(
        [
            ("key", "\$foo.txt"),
            ("AWSAccessKeyId", aws_access_key_id),
            ("acl", "private"),
            ("signature", signature),
            ("policy", policy),
            ("Content-Type", "text/plain"),
            ("file", ("bar")),
        ]
    )

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 403


@pytest.mark.skip(reason="https://github.com/nspcc-dev/s3-tests/issues/46")
def test_post_object_invalid_access_key():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {
        "expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "conditions": [
            {"bucket": bucket_name},
            ["starts-with", "$key", "\$foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["content-length-range", 0, 1024],
        ],
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, "utf-8")
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(
        hmac.new(bytes(aws_secret_access_key, "utf-8"), policy, hashlib.sha1).digest()
    )

    payload = OrderedDict(
        [
            ("key", "\$foo.txt"),
            ("AWSAccessKeyId", aws_access_key_id[::-1]),
            ("acl", "private"),
            ("signature", signature),
            ("policy", policy),
            ("Content-Type", "text/plain"),
            ("file", ("bar")),
        ]
    )

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 403


@pytest.mark.skip(reason="https://github.com/nspcc-dev/s3-tests/issues/46")
def test_post_object_invalid_date_format():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {
        "expiration": str(expires),
        "conditions": [
            {"bucket": bucket_name},
            ["starts-with", "$key", "\$foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["content-length-range", 0, 1024],
        ],
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, "utf-8")
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(
        hmac.new(bytes(aws_secret_access_key, "utf-8"), policy, hashlib.sha1).digest()
    )

    payload = OrderedDict(
        [
            ("key", "\$foo.txt"),
            ("AWSAccessKeyId", aws_access_key_id),
            ("acl", "private"),
            ("signature", signature),
            ("policy", policy),
            ("Content-Type", "text/plain"),
            ("file", ("bar")),
        ]
    )

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 400


@pytest.mark.skip(reason="https://github.com/nspcc-dev/s3-tests/issues/46")
def test_post_object_no_key_specified():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {
        "expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "conditions": [
            {"bucket": bucket_name},
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["content-length-range", 0, 1024],
        ],
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, "utf-8")
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(
        hmac.new(bytes(aws_secret_access_key, "utf-8"), policy, hashlib.sha1).digest()
    )

    payload = OrderedDict(
        [
            ("AWSAccessKeyId", aws_access_key_id),
            ("acl", "private"),
            ("signature", signature),
            ("policy", policy),
            ("Content-Type", "text/plain"),
            ("file", ("bar")),
        ]
    )

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 400


@pytest.mark.skip(reason="https://github.com/nspcc-dev/s3-tests/issues/46")
def test_post_object_missing_signature():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {
        "expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "conditions": [
            {"bucket": bucket_name},
            ["starts-with", "$key", "\$foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["content-length-range", 0, 1024],
        ],
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, "utf-8")
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(
        hmac.new(bytes(aws_secret_access_key, "utf-8"), policy, hashlib.sha1).digest()
    )

    payload = OrderedDict(
        [
            ("key", "foo.txt"),
            ("AWSAccessKeyId", aws_access_key_id),
            ("acl", "private"),
            ("policy", policy),
            ("Content-Type", "text/plain"),
            ("file", ("bar")),
        ]
    )

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 400


@pytest.mark.skip(reason="https://github.com/nspcc-dev/s3-tests/issues/46")
def test_post_object_missing_policy_condition():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {
        "expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "conditions": [
            ["starts-with", "$key", "\$foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["content-length-range", 0, 1024],
        ],
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, "utf-8")
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(
        hmac.new(bytes(aws_secret_access_key, "utf-8"), policy, hashlib.sha1).digest()
    )

    payload = OrderedDict(
        [
            ("key", "foo.txt"),
            ("AWSAccessKeyId", aws_access_key_id),
            ("acl", "private"),
            ("signature", signature),
            ("policy", policy),
            ("Content-Type", "text/plain"),
            ("file", ("bar")),
        ]
    )

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 403


@pytest.mark.skip(reason="https://github.com/nspcc-dev/s3-tests/issues/46")
def test_post_object_user_specified_header():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {
        "expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "conditions": [
            {"bucket": bucket_name},
            ["starts-with", "$key", "foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["content-length-range", 0, 1024],
            ["starts-with", "$x-amz-meta-foo", "bar"],
        ],
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, "utf-8")
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(
        hmac.new(bytes(aws_secret_access_key, "utf-8"), policy, hashlib.sha1).digest()
    )

    payload = OrderedDict(
        [
            ("key", "foo.txt"),
            ("AWSAccessKeyId", aws_access_key_id),
            ("acl", "private"),
            ("signature", signature),
            ("policy", policy),
            ("Content-Type", "text/plain"),
            ("x-amz-meta-foo", "barclamp"),
            ("file", ("bar")),
        ]
    )

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 204
    response = client.get_object(Bucket=bucket_name, Key="foo.txt")
    assert response["Metadata"]["foo"] == "barclamp"


@pytest.mark.skip(reason="https://github.com/nspcc-dev/s3-tests/issues/46")
def test_post_object_request_missing_policy_specified_field():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {
        "expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "conditions": [
            {"bucket": bucket_name},
            ["starts-with", "$key", "foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["content-length-range", 0, 1024],
            ["starts-with", "$x-amz-meta-foo", "bar"],
        ],
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, "utf-8")
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(
        hmac.new(bytes(aws_secret_access_key, "utf-8"), policy, hashlib.sha1).digest()
    )

    payload = OrderedDict(
        [
            ("key", "foo.txt"),
            ("AWSAccessKeyId", aws_access_key_id),
            ("acl", "private"),
            ("signature", signature),
            ("policy", policy),
            ("Content-Type", "text/plain"),
            ("file", ("bar")),
        ]
    )

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 403


@pytest.mark.skip(reason="https://github.com/nspcc-dev/s3-tests/issues/46")
def test_post_object_condition_is_case_sensitive():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {
        "expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "CONDITIONS": [
            {"bucket": bucket_name},
            ["starts-with", "$key", "foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["content-length-range", 0, 1024],
        ],
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, "utf-8")
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(
        hmac.new(bytes(aws_secret_access_key, "utf-8"), policy, hashlib.sha1).digest()
    )

    payload = OrderedDict(
        [
            ("key", "foo.txt"),
            ("AWSAccessKeyId", aws_access_key_id),
            ("acl", "private"),
            ("signature", signature),
            ("policy", policy),
            ("Content-Type", "text/plain"),
            ("file", ("bar")),
        ]
    )

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 400


@pytest.mark.skip(reason="https://github.com/nspcc-dev/s3-tests/issues/46")
def test_post_object_expires_is_case_sensitive():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {
        "EXPIRATION": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "conditions": [
            {"bucket": bucket_name},
            ["starts-with", "$key", "foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["content-length-range", 0, 1024],
        ],
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, "utf-8")
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(
        hmac.new(bytes(aws_secret_access_key, "utf-8"), policy, hashlib.sha1).digest()
    )

    payload = OrderedDict(
        [
            ("key", "foo.txt"),
            ("AWSAccessKeyId", aws_access_key_id),
            ("acl", "private"),
            ("signature", signature),
            ("policy", policy),
            ("Content-Type", "text/plain"),
            ("file", ("bar")),
        ]
    )

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 400


@pytest.mark.skip(reason="https://github.com/nspcc-dev/s3-tests/issues/46")
def test_post_object_expired_policy():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=-6000)

    policy_document = {
        "expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "conditions": [
            {"bucket": bucket_name},
            ["starts-with", "$key", "foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["content-length-range", 0, 1024],
        ],
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, "utf-8")
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(
        hmac.new(bytes(aws_secret_access_key, "utf-8"), policy, hashlib.sha1).digest()
    )

    payload = OrderedDict(
        [
            ("key", "foo.txt"),
            ("AWSAccessKeyId", aws_access_key_id),
            ("acl", "private"),
            ("signature", signature),
            ("policy", policy),
            ("Content-Type", "text/plain"),
            ("file", ("bar")),
        ]
    )

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 403


@pytest.mark.skip(reason="https://github.com/nspcc-dev/s3-tests/issues/46")
def test_post_object_invalid_request_field_value():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {
        "expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "conditions": [
            {"bucket": bucket_name},
            ["starts-with", "$key", "foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["content-length-range", 0, 1024],
            ["eq", "$x-amz-meta-foo", ""],
        ],
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, "utf-8")
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(
        hmac.new(bytes(aws_secret_access_key, "utf-8"), policy, hashlib.sha1).digest()
    )
    payload = OrderedDict(
        [
            ("key", "foo.txt"),
            ("AWSAccessKeyId", aws_access_key_id),
            ("acl", "private"),
            ("signature", signature),
            ("policy", policy),
            ("Content-Type", "text/plain"),
            ("x-amz-meta-foo", "barclamp"),
            ("file", ("bar")),
        ]
    )

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 403


@pytest.mark.skip(reason="https://github.com/nspcc-dev/s3-tests/issues/46")
def test_post_object_missing_expires_condition():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {
        "conditions": [
            {"bucket": bucket_name},
            ["starts-with", "$key", "foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["content-length-range", 0, 1024],
        ]
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, "utf-8")
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(
        hmac.new(bytes(aws_secret_access_key, "utf-8"), policy, hashlib.sha1).digest()
    )

    payload = OrderedDict(
        [
            ("key", "foo.txt"),
            ("AWSAccessKeyId", aws_access_key_id),
            ("acl", "private"),
            ("signature", signature),
            ("policy", policy),
            ("Content-Type", "text/plain"),
            ("file", ("bar")),
        ]
    )

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 400


@pytest.mark.skip(reason="https://github.com/nspcc-dev/s3-tests/issues/46")
def test_post_object_missing_conditions_list():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ")}

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, "utf-8")
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(
        hmac.new(bytes(aws_secret_access_key, "utf-8"), policy, hashlib.sha1).digest()
    )

    payload = OrderedDict(
        [
            ("key", "foo.txt"),
            ("AWSAccessKeyId", aws_access_key_id),
            ("acl", "private"),
            ("signature", signature),
            ("policy", policy),
            ("Content-Type", "text/plain"),
            ("file", ("bar")),
        ]
    )

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 400


@pytest.mark.skip(reason="https://github.com/nspcc-dev/s3-tests/issues/46")
def test_post_object_upload_size_limit_exceeded():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {
        "expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "conditions": [
            {"bucket": bucket_name},
            ["starts-with", "$key", "foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["content-length-range", 0, 0],
        ],
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, "utf-8")
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(
        hmac.new(bytes(aws_secret_access_key, "utf-8"), policy, hashlib.sha1).digest()
    )

    payload = OrderedDict(
        [
            ("key", "foo.txt"),
            ("AWSAccessKeyId", aws_access_key_id),
            ("acl", "private"),
            ("signature", signature),
            ("policy", policy),
            ("Content-Type", "text/plain"),
            ("file", ("bar")),
        ]
    )

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 400


@pytest.mark.skip(reason="https://github.com/nspcc-dev/s3-tests/issues/46")
def test_post_object_missing_content_length_argument():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {
        "expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "conditions": [
            {"bucket": bucket_name},
            ["starts-with", "$key", "foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["content-length-range", 0],
        ],
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, "utf-8")
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(
        hmac.new(bytes(aws_secret_access_key, "utf-8"), policy, hashlib.sha1).digest()
    )

    payload = OrderedDict(
        [
            ("key", "foo.txt"),
            ("AWSAccessKeyId", aws_access_key_id),
            ("acl", "private"),
            ("signature", signature),
            ("policy", policy),
            ("Content-Type", "text/plain"),
            ("file", ("bar")),
        ]
    )

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 400


@pytest.mark.skip(reason="https://github.com/nspcc-dev/s3-tests/issues/46")
def test_post_object_invalid_content_length_argument():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {
        "expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "conditions": [
            {"bucket": bucket_name},
            ["starts-with", "$key", "foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["content-length-range", -1, 0],
        ],
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, "utf-8")
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(
        hmac.new(bytes(aws_secret_access_key, "utf-8"), policy, hashlib.sha1).digest()
    )

    payload = OrderedDict(
        [
            ("key", "foo.txt"),
            ("AWSAccessKeyId", aws_access_key_id),
            ("acl", "private"),
            ("signature", signature),
            ("policy", policy),
            ("Content-Type", "text/plain"),
            ("file", ("bar")),
        ]
    )

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 400


@pytest.mark.skip(reason="https://github.com/nspcc-dev/s3-tests/issues/46")
def test_post_object_upload_size_below_minimum():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {
        "expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "conditions": [
            {"bucket": bucket_name},
            ["starts-with", "$key", "foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["content-length-range", 512, 1000],
        ],
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, "utf-8")
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(
        hmac.new(bytes(aws_secret_access_key, "utf-8"), policy, hashlib.sha1).digest()
    )

    payload = OrderedDict(
        [
            ("key", "foo.txt"),
            ("AWSAccessKeyId", aws_access_key_id),
            ("acl", "private"),
            ("signature", signature),
            ("policy", policy),
            ("Content-Type", "text/plain"),
            ("file", ("bar")),
        ]
    )

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 400


@pytest.mark.skip(reason="https://github.com/nspcc-dev/s3-tests/issues/46")
def test_post_object_upload_size_rgw_chunk_size_bug():
    # Test for https://tracker.ceph.com/issues/58627
    # TODO: if this value is different in Teuthology runs, this would need tuning
    # https://github.com/ceph/ceph/blob/main/qa/suites/rgw/verify/striping%24/stripe-greater-than-chunk.yaml
    _rgw_max_chunk_size = 4 * 2**20  # 4MiB
    min_size = _rgw_max_chunk_size
    max_size = _rgw_max_chunk_size * 3
    # [(chunk),(small)]
    test_payload_size = (
        _rgw_max_chunk_size + 200
    )  # extra bit to push it over the chunk boundary
    # it should be valid when we run this test!
    assert test_payload_size > min_size
    assert test_payload_size < max_size

    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {
        "expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "conditions": [
            {"bucket": bucket_name},
            ["starts-with", "$key", "foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["content-length-range", min_size, max_size],
        ],
    }

    test_payload = "x" * test_payload_size

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, "utf-8")
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(
        hmac.new(bytes(aws_secret_access_key, "utf-8"), policy, hashlib.sha1).digest()
    )

    payload = OrderedDict(
        [
            ("key", "foo.txt"),
            ("AWSAccessKeyId", aws_access_key_id),
            ("acl", "private"),
            ("signature", signature),
            ("policy", policy),
            ("Content-Type", "text/plain"),
            ("file", (test_payload)),
        ]
    )

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 204


@pytest.mark.skip(reason="https://github.com/nspcc-dev/s3-tests/issues/46")
def test_post_object_empty_conditions():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {
        "expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "conditions": [{}],
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, "utf-8")
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(
        hmac.new(bytes(aws_secret_access_key, "utf-8"), policy, hashlib.sha1).digest()
    )

    payload = OrderedDict(
        [
            ("key", "foo.txt"),
            ("AWSAccessKeyId", aws_access_key_id),
            ("acl", "private"),
            ("signature", signature),
            ("policy", policy),
            ("Content-Type", "text/plain"),
            ("file", ("bar")),
        ]
    )

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 400


def test_get_object_ifmatch_good():
    bucket_name = get_new_bucket()
    client = get_client()
    response = client.put_object(Bucket=bucket_name, Key="foo", Body="bar")
    etag = response["ETag"]

    response = client.get_object(Bucket=bucket_name, Key="foo", IfMatch=etag)
    body = _get_body(response)
    assert body == "bar"


def test_get_object_ifmatch_failed():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_object(Bucket=bucket_name, Key="foo", Body="bar")

    e = assert_raises(
        ClientError,
        client.get_object,
        Bucket=bucket_name,
        Key="foo",
        IfMatch='"ABCORZ"',
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 412
    assert error_code == "PreconditionFailed"


def test_get_object_ifnonematch_good():
    bucket_name = get_new_bucket()
    client = get_client()
    response = client.put_object(Bucket=bucket_name, Key="foo", Body="bar")
    etag = response["ETag"]

    e = assert_raises(
        ClientError, client.get_object, Bucket=bucket_name, Key="foo", IfNoneMatch=etag
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 304
    assert e.response["Error"]["Message"] == "Not Modified"


def test_get_object_ifnonematch_failed():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_object(Bucket=bucket_name, Key="foo", Body="bar")

    response = client.get_object(Bucket=bucket_name, Key="foo", IfNoneMatch="ABCORZ")
    body = _get_body(response)
    assert body == "bar"


def test_get_object_ifmodifiedsince_good():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_object(Bucket=bucket_name, Key="foo", Body="bar")

    response = client.get_object(
        Bucket=bucket_name, Key="foo", IfModifiedSince="Sat, 29 Oct 1994 19:43:31 GMT"
    )
    body = _get_body(response)
    assert body == "bar"


@pytest.mark.fails_on_dbstore
def test_get_object_ifmodifiedsince_failed():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_object(Bucket=bucket_name, Key="foo", Body="bar")
    response = client.get_object(Bucket=bucket_name, Key="foo")
    last_modified = str(response["LastModified"])

    last_modified = last_modified.split("+")[0]
    mtime = datetime.datetime.strptime(last_modified, "%Y-%m-%d %H:%M:%S")

    after = mtime + datetime.timedelta(seconds=1)
    after_str = time.strftime("%a, %d %b %Y %H:%M:%S GMT", after.timetuple())

    time.sleep(1)

    e = assert_raises(
        ClientError,
        client.get_object,
        Bucket=bucket_name,
        Key="foo",
        IfModifiedSince=after_str,
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 304
    assert e.response["Error"]["Message"] == "Not Modified"


@pytest.mark.fails_on_dbstore
def test_get_object_ifunmodifiedsince_good():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_object(Bucket=bucket_name, Key="foo", Body="bar")

    e = assert_raises(
        ClientError,
        client.get_object,
        Bucket=bucket_name,
        Key="foo",
        IfUnmodifiedSince="Sat, 29 Oct 1994 19:43:31 GMT",
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 412
    assert error_code == "PreconditionFailed"


def test_get_object_ifunmodifiedsince_failed():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_object(Bucket=bucket_name, Key="foo", Body="bar")

    response = client.get_object(
        Bucket=bucket_name, Key="foo", IfUnmodifiedSince="Sat, 29 Oct 2100 19:43:31 GMT"
    )
    body = _get_body(response)
    assert body == "bar"


@pytest.mark.fails_on_aws
def test_put_object_ifmatch_good():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_object(Bucket=bucket_name, Key="foo", Body="bar")

    response = client.get_object(Bucket=bucket_name, Key="foo")
    body = _get_body(response)
    assert body == "bar"

    etag = response["ETag"].replace('"', "")

    # pass in custom header 'If-Match' before PutObject call
    lf = lambda **kwargs: kwargs["params"]["headers"].update({"If-Match": etag})
    client.meta.events.register("before-call.s3.PutObject", lf)
    response = client.put_object(Bucket=bucket_name, Key="foo", Body="zar")

    response = client.get_object(Bucket=bucket_name, Key="foo")
    body = _get_body(response)
    assert body == "zar"


@pytest.mark.fails_on_dbstore
def test_put_object_ifmatch_failed():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_object(Bucket=bucket_name, Key="foo", Body="bar")
    response = client.get_object(Bucket=bucket_name, Key="foo")
    body = _get_body(response)
    assert body == "bar"

    # pass in custom header 'If-Match' before PutObject call
    lf = lambda **kwargs: kwargs["params"]["headers"].update({"If-Match": '"ABCORZ"'})
    client.meta.events.register("before-call.s3.PutObject", lf)

    e = assert_raises(
        ClientError, client.put_object, Bucket=bucket_name, Key="foo", Body="zar"
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 412
    assert error_code == "PreconditionFailed"

    response = client.get_object(Bucket=bucket_name, Key="foo")
    body = _get_body(response)
    assert body == "bar"


@pytest.mark.fails_on_aws
def test_put_object_ifmatch_overwrite_existed_good():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_object(Bucket=bucket_name, Key="foo", Body="bar")
    response = client.get_object(Bucket=bucket_name, Key="foo")
    body = _get_body(response)
    assert body == "bar"

    lf = lambda **kwargs: kwargs["params"]["headers"].update({"If-Match": "*"})
    client.meta.events.register("before-call.s3.PutObject", lf)
    response = client.put_object(Bucket=bucket_name, Key="foo", Body="zar")

    response = client.get_object(Bucket=bucket_name, Key="foo")
    body = _get_body(response)
    assert body == "zar"


@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
def test_put_object_ifmatch_nonexisted_failed():
    bucket_name = get_new_bucket()
    client = get_client()

    lf = lambda **kwargs: kwargs["params"]["headers"].update({"If-Match": "*"})
    client.meta.events.register("before-call.s3.PutObject", lf)
    e = assert_raises(
        ClientError, client.put_object, Bucket=bucket_name, Key="foo", Body="bar"
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 412
    assert error_code == "PreconditionFailed"

    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key="foo")
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == "NoSuchKey"


@pytest.mark.fails_on_aws
def test_put_object_ifnonmatch_good():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_object(Bucket=bucket_name, Key="foo", Body="bar")
    response = client.get_object(Bucket=bucket_name, Key="foo")
    body = _get_body(response)
    assert body == "bar"

    lf = lambda **kwargs: kwargs["params"]["headers"].update(
        {"If-None-Match": "ABCORZ"}
    )
    client.meta.events.register("before-call.s3.PutObject", lf)
    response = client.put_object(Bucket=bucket_name, Key="foo", Body="zar")

    response = client.get_object(Bucket=bucket_name, Key="foo")
    body = _get_body(response)
    assert body == "zar"


@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
def test_put_object_ifnonmatch_failed():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_object(Bucket=bucket_name, Key="foo", Body="bar")

    response = client.get_object(Bucket=bucket_name, Key="foo")
    body = _get_body(response)
    assert body == "bar"

    etag = response["ETag"].replace('"', "")

    lf = lambda **kwargs: kwargs["params"]["headers"].update({"If-None-Match": etag})
    client.meta.events.register("before-call.s3.PutObject", lf)
    e = assert_raises(
        ClientError, client.put_object, Bucket=bucket_name, Key="foo", Body="zar"
    )

    status, error_code = _get_status_and_error_code(e.response)
    assert status == 412
    assert error_code == "PreconditionFailed"

    response = client.get_object(Bucket=bucket_name, Key="foo")
    body = _get_body(response)
    assert body == "bar"


@pytest.mark.fails_on_aws
def test_put_object_ifnonmatch_nonexisted_good():
    bucket_name = get_new_bucket()
    client = get_client()

    lf = lambda **kwargs: kwargs["params"]["headers"].update({"If-None-Match": "*"})
    client.meta.events.register("before-call.s3.PutObject", lf)
    client.put_object(Bucket=bucket_name, Key="foo", Body="bar")

    response = client.get_object(Bucket=bucket_name, Key="foo")
    body = _get_body(response)
    assert body == "bar"


@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
def test_put_object_ifnonmatch_overwrite_existed_failed():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_object(Bucket=bucket_name, Key="foo", Body="bar")

    response = client.get_object(Bucket=bucket_name, Key="foo")
    body = _get_body(response)
    assert body == "bar"

    lf = lambda **kwargs: kwargs["params"]["headers"].update({"If-None-Match": "*"})
    client.meta.events.register("before-call.s3.PutObject", lf)
    e = assert_raises(
        ClientError, client.put_object, Bucket=bucket_name, Key="foo", Body="zar"
    )

    status, error_code = _get_status_and_error_code(e.response)
    assert status == 412
    assert error_code == "PreconditionFailed"

    response = client.get_object(Bucket=bucket_name, Key="foo")
    body = _get_body(response)
    assert body == "bar"


def _setup_bucket_object_acl(bucket_acl, object_acl, client=None):
    """
    add a foo key, and specified key and bucket acls to
    a (new or existing) bucket.
    """
    if client is None:
        client = get_client()
    bucket_name = get_new_bucket_name()
    client.create_bucket(ACL=bucket_acl, Bucket=bucket_name)
    client.put_object(ACL=object_acl, Bucket=bucket_name, Key="foo")

    return bucket_name


def _setup_bucket_acl(bucket_acl=None):
    """
    set up a new bucket with specified acl
    """
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(ACL=bucket_acl, Bucket=bucket_name)

    return bucket_name


def test_object_raw_get():
    bucket_name = _setup_bucket_object_acl("public-read", "public-read")

    unauthenticated_client = get_unauthenticated_client()
    response = unauthenticated_client.get_object(Bucket=bucket_name, Key="foo")
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_object_raw_get_bucket_gone():
    bucket_name = _setup_bucket_object_acl("public-read", "public-read")
    client = get_client()

    client.delete_object(Bucket=bucket_name, Key="foo")
    client.delete_bucket(Bucket=bucket_name)

    unauthenticated_client = get_unauthenticated_client()

    e = assert_raises(
        ClientError, unauthenticated_client.get_object, Bucket=bucket_name, Key="foo"
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == "NoSuchBucket"


def test_object_delete_key_bucket_gone():
    bucket_name = _setup_bucket_object_acl("public-read", "public-read")
    client = get_client()

    client.delete_object(Bucket=bucket_name, Key="foo")
    client.delete_bucket(Bucket=bucket_name)

    unauthenticated_client = get_unauthenticated_client()

    e = assert_raises(
        ClientError, unauthenticated_client.delete_object, Bucket=bucket_name, Key="foo"
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == "NoSuchBucket"


def test_object_raw_get_object_gone():
    bucket_name = _setup_bucket_object_acl("public-read", "public-read")
    client = get_client()

    client.delete_object(Bucket=bucket_name, Key="foo")

    unauthenticated_client = get_unauthenticated_client()

    e = assert_raises(
        ClientError, unauthenticated_client.get_object, Bucket=bucket_name, Key="foo"
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == "NoSuchKey"


def test_bucket_head():
    bucket_name = get_new_bucket()
    client = get_client()

    response = client.head_bucket(Bucket=bucket_name)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_bucket_head_notexist():
    bucket_name = get_new_bucket_name()
    client = get_client()

    e = assert_raises(ClientError, client.head_bucket, Bucket=bucket_name)

    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    # n.b., RGW does not send a response document for this operation,
    # which seems consistent with
    # https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html
    # assert error_code == 'NoSuchKey'


@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
def test_bucket_head_extended():
    bucket = get_new_bucket_resource()
    client = get_client()

    response = client.head_bucket(Bucket=bucket.name)
    assert int(response["ResponseMetadata"]["HTTPHeaders"]["x-rgw-object-count"]) == 0
    assert int(response["ResponseMetadata"]["HTTPHeaders"]["x-rgw-bytes-used"]) == 0

    _create_objects(bucket=bucket, bucket_name=bucket.name, keys=["foo", "bar", "baz"])
    response = client.head_bucket(Bucket=bucket.name)

    assert int(response["ResponseMetadata"]["HTTPHeaders"]["x-rgw-object-count"]) == 3
    assert int(response["ResponseMetadata"]["HTTPHeaders"]["x-rgw-bytes-used"]) == 9


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/852")
def test_object_raw_get_bucket_acl():
    bucket_name = _setup_bucket_object_acl("private", "public-read")

    unauthenticated_client = get_unauthenticated_client()
    response = unauthenticated_client.get_object(Bucket=bucket_name, Key="foo")
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/852")
def test_object_raw_get_object_acl():
    bucket_name = _setup_bucket_object_acl("public-read", "private")

    unauthenticated_client = get_unauthenticated_client()
    e = assert_raises(
        ClientError, unauthenticated_client.get_object, Bucket=bucket_name, Key="foo"
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == "AccessDenied"


def test_object_put_acl_mtime():
    key = "foo"
    bucket_name = get_new_bucket()
    # Enable versioning
    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")
    client = get_client()

    content = "foooz"
    client.put_object(Bucket=bucket_name, Key=key, Body=content)

    obj_response = client.head_object(Bucket=bucket_name, Key=key)
    create_mtime = obj_response["LastModified"]

    response = client.list_objects(Bucket=bucket_name)
    obj_list = response["Contents"][0]
    _compare_dates(obj_list["LastModified"], create_mtime)

    response = client.list_object_versions(Bucket=bucket_name)
    obj_list = response["Versions"][0]
    _compare_dates(obj_list["LastModified"], create_mtime)

    # set acl
    time.sleep(2)
    client.put_object_acl(ACL="private", Bucket=bucket_name, Key=key)

    # mtime should match with create mtime
    obj_response = client.head_object(Bucket=bucket_name, Key=key)
    _compare_dates(create_mtime, obj_response["LastModified"])

    response = client.list_objects(Bucket=bucket_name)
    obj_list = response["Contents"][0]
    _compare_dates(obj_list["LastModified"], create_mtime)

    response = client.list_object_versions(Bucket=bucket_name)
    obj_list = response["Versions"][0]
    _compare_dates(obj_list["LastModified"], create_mtime)


def test_object_raw_authenticated():
    bucket_name = _setup_bucket_object_acl("public-read", "public-read")

    client = get_client()
    response = client.get_object(Bucket=bucket_name, Key="foo")
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_object_raw_response_headers():
    bucket_name = _setup_bucket_object_acl("private", "private")

    client = get_client()

    response = client.get_object(
        Bucket=bucket_name,
        Key="foo",
        ResponseCacheControl="no-cache",
        ResponseContentDisposition="bla",
        ResponseContentEncoding="aaa",
        ResponseContentLanguage="esperanto",
        ResponseContentType="foo/bar",
        ResponseExpires="123",
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert response["ResponseMetadata"]["HTTPHeaders"]["content-type"] == "foo/bar"
    assert response["ResponseMetadata"]["HTTPHeaders"]["content-disposition"] == "bla"
    assert (
        response["ResponseMetadata"]["HTTPHeaders"]["content-language"] == "esperanto"
    )
    assert response["ResponseMetadata"]["HTTPHeaders"]["content-encoding"] == "aaa"
    assert response["ResponseMetadata"]["HTTPHeaders"]["cache-control"] == "no-cache"


def test_object_raw_authenticated_bucket_acl():
    bucket_name = _setup_bucket_object_acl("private", "public-read")

    client = get_client()
    response = client.get_object(Bucket=bucket_name, Key="foo")
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_object_raw_authenticated_object_acl():
    bucket_name = _setup_bucket_object_acl("public-read", "private")

    client = get_client()
    response = client.get_object(Bucket=bucket_name, Key="foo")
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_object_raw_authenticated_bucket_gone():
    bucket_name = _setup_bucket_object_acl("public-read", "public-read")
    client = get_client()

    client.delete_object(Bucket=bucket_name, Key="foo")
    client.delete_bucket(Bucket=bucket_name)

    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key="foo")
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == "NoSuchBucket"


def test_object_raw_authenticated_object_gone():
    bucket_name = _setup_bucket_object_acl("public-read", "public-read")
    client = get_client()

    client.delete_object(Bucket=bucket_name, Key="foo")

    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key="foo")
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == "NoSuchKey"


def _test_object_raw_get_x_amz_expires_not_expired(client):
    bucket_name = _setup_bucket_object_acl("public-read", "public-read", client=client)
    params = {"Bucket": bucket_name, "Key": "foo"}

    url = client.generate_presigned_url(
        ClientMethod="get_object", Params=params, ExpiresIn=100000, HttpMethod="GET"
    )

    res = requests.options(url, verify=get_config_ssl_verify()).__dict__
    assert res["status_code"] == 403

    res = requests.get(url, verify=get_config_ssl_verify()).__dict__
    assert res["status_code"] == 200


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/852")
def test_object_raw_get_x_amz_expires_not_expired():
    _test_object_raw_get_x_amz_expires_not_expired(client=get_client())


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/852")
def test_object_raw_get_x_amz_expires_not_expired_tenant():
    _test_object_raw_get_x_amz_expires_not_expired(client=get_tenant_client())


def test_object_raw_get_x_amz_expires_out_range_zero():
    bucket_name = _setup_bucket_object_acl("public-read", "public-read")
    client = get_client()
    params = {"Bucket": bucket_name, "Key": "foo"}

    url = client.generate_presigned_url(
        ClientMethod="get_object", Params=params, ExpiresIn=0, HttpMethod="GET"
    )

    res = requests.get(url, verify=get_config_ssl_verify()).__dict__
    assert res["status_code"] == 403


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/853")
def test_object_raw_get_x_amz_expires_out_max_range():
    bucket_name = _setup_bucket_object_acl("public-read", "public-read")
    client = get_client()
    params = {"Bucket": bucket_name, "Key": "foo"}

    url = client.generate_presigned_url(
        ClientMethod="get_object", Params=params, ExpiresIn=609901, HttpMethod="GET"
    )

    res = requests.get(url, verify=get_config_ssl_verify()).__dict__
    assert res["status_code"] == 403


def test_object_raw_get_x_amz_expires_out_positive_range():
    bucket_name = _setup_bucket_object_acl("public-read", "public-read")
    client = get_client()
    params = {"Bucket": bucket_name, "Key": "foo"}

    url = client.generate_presigned_url(
        ClientMethod="get_object", Params=params, ExpiresIn=-7, HttpMethod="GET"
    )

    res = requests.get(url, verify=get_config_ssl_verify()).__dict__
    assert res["status_code"] == 403


def test_object_anon_put():
    bucket_name = get_new_bucket()
    client = get_client()

    client.put_object(Bucket=bucket_name, Key="foo")

    unauthenticated_client = get_unauthenticated_client()

    e = assert_raises(
        ClientError,
        unauthenticated_client.put_object,
        Bucket=bucket_name,
        Key="foo",
        Body="foo",
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == "AccessDenied"


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/852")
def test_object_anon_put_write_access():
    bucket_name = _setup_bucket_acl("public-read-write")
    client = get_client()
    client.put_object(Bucket=bucket_name, Key="foo")

    unauthenticated_client = get_unauthenticated_client()

    response = unauthenticated_client.put_object(
        Bucket=bucket_name, Key="foo", Body="foo"
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_object_put_authenticated():
    bucket_name = get_new_bucket()
    client = get_client()

    response = client.put_object(Bucket=bucket_name, Key="foo", Body="foo")
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_object_raw_put_authenticated_expired():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_object(Bucket=bucket_name, Key="foo")

    params = {"Bucket": bucket_name, "Key": "foo"}
    url = client.generate_presigned_url(
        ClientMethod="put_object", Params=params, ExpiresIn=-1000, HttpMethod="PUT"
    )

    # params wouldn't take a 'Body' parameter so we're passing it in here
    res = requests.put(url, data="foo", verify=get_config_ssl_verify()).__dict__
    assert res["status_code"] == 403


def check_bad_bucket_name(bucket_name):
    """
    Attempt to create a bucket with a specified name, and confirm
    that the request fails because of an invalid bucket name.
    """
    client = get_client()
    e = assert_raises(ClientError, client.create_bucket, Bucket=bucket_name)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == "InvalidBucketName"


# AWS does not enforce all documented bucket restrictions.
# http://docs.amazonwebservices.com/AmazonS3/2006-03-01/dev/index.html?BucketRestrictions.html
@pytest.mark.fails_on_aws
# Breaks DNS with SubdomainCallingFormat
def test_bucket_create_naming_bad_starts_nonalpha():
    bucket_name = get_new_bucket_name()
    check_bad_bucket_name("_" + bucket_name)


def check_invalid_bucketname(invalid_name):
    """
    Send a create bucket_request with an invalid bucket name
    that will bypass the ParamValidationError that would be raised
    if the invalid bucket name that was passed in normally.
    This function returns the status and error code from the failure
    """
    client = get_client()
    valid_bucket_name = get_new_bucket_name()

    def replace_bucketname_from_url(**kwargs):
        url = kwargs["params"]["url"]
        new_url = url.replace(valid_bucket_name, invalid_name)
        kwargs["params"]["url"] = new_url

    client.meta.events.register(
        "before-call.s3.CreateBucket", replace_bucketname_from_url
    )
    e = assert_raises(ClientError, client.create_bucket, Bucket=invalid_name)
    status, error_code = _get_status_and_error_code(e.response)
    return (status, error_code)


def test_bucket_create_naming_bad_short_one():
    check_bad_bucket_name("a")


def test_bucket_create_naming_bad_short_two():
    check_bad_bucket_name("aa")


def check_good_bucket_name(name, _prefix=None):
    """
    Attempt to create a bucket with a specified name
    and (specified or default) prefix, returning the
    results of that effort.
    """
    # tests using this with the default prefix must *not* rely on
    # being able to set the initial character, or exceed the max len

    # tests using this with a custom prefix are responsible for doing
    # their own setup/teardown nukes, with their custom prefix; this
    # should be very rare
    if _prefix is None:
        _prefix = get_prefix()
    bucket_name = "{prefix}{name}".format(
        prefix=_prefix,
        name=name,
    )
    client = get_client()
    response = client.create_bucket(Bucket=bucket_name)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


def _test_bucket_create_naming_good_long(length):
    """
    Attempt to create a bucket whose name (including the
    prefix) is of a specified length.
    """
    # tests using this with the default prefix must *not* rely on
    # being able to set the initial character, or exceed the max len

    # tests using this with a custom prefix are responsible for doing
    # their own setup/teardown nukes, with their custom prefix; this
    # should be very rare
    prefix = get_new_bucket_name()
    assert len(prefix) < 63
    num = length - len(prefix)
    name = num * "a"

    bucket_name = "{prefix}{name}".format(
        prefix=prefix,
        name=name,
    )
    client = get_client()
    response = client.create_bucket(Bucket=bucket_name)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


# Breaks DNS with SubdomainCallingFormat
@pytest.mark.fails_on_aws
# Should now pass on AWS even though it has 'fails_on_aws' attr.
def test_bucket_create_naming_good_long_60():
    _test_bucket_create_naming_good_long(60)


# Breaks DNS with SubdomainCallingFormat
@pytest.mark.fails_on_aws
# Should now pass on AWS even though it has 'fails_on_aws' attr.
def test_bucket_create_naming_good_long_61():
    _test_bucket_create_naming_good_long(61)


# Breaks DNS with SubdomainCallingFormat
@pytest.mark.fails_on_aws
# Should now pass on AWS even though it has 'fails_on_aws' attr.
def test_bucket_create_naming_good_long_62():
    _test_bucket_create_naming_good_long(62)


# Breaks DNS with SubdomainCallingFormat
def test_bucket_create_naming_good_long_63():
    _test_bucket_create_naming_good_long(63)


# Breaks DNS with SubdomainCallingFormat
@pytest.mark.fails_on_aws
# Should now pass on AWS even though it has 'fails_on_aws' attr.
def test_bucket_list_long_name():
    prefix = get_new_bucket_name()
    length = 61
    num = length - len(prefix)
    name = num * "a"

    bucket_name = "{prefix}{name}".format(
        prefix=prefix,
        name=name,
    )
    bucket = get_new_bucket_resource(name=bucket_name)
    is_empty = _bucket_is_empty(bucket)
    assert is_empty == True


# AWS does not enforce all documented bucket restrictions.
# http://docs.amazonwebservices.com/AmazonS3/2006-03-01/dev/index.html?BucketRestrictions.html
@pytest.mark.fails_on_aws
def test_bucket_create_naming_bad_ip():
    check_bad_bucket_name("192.168.5.123")


# test_bucket_create_naming_dns_* are valid but not recommended
@pytest.mark.fails_on_aws
# Should now pass on AWS even though it has 'fails_on_aws' attr.
def test_bucket_create_naming_dns_underscore():
    invalid_bucketname = "foo_bar"
    status, error_code = check_invalid_bucketname(invalid_bucketname)
    assert status == 400
    assert error_code == "InvalidBucketName"


# Breaks DNS with SubdomainCallingFormat
@pytest.mark.fails_on_aws
def test_bucket_create_naming_dns_long():
    prefix = get_prefix()
    assert len(prefix) < 50
    num = 63 - len(prefix)
    check_good_bucket_name(num * "a")


# Breaks DNS with SubdomainCallingFormat
@pytest.mark.fails_on_aws
# Should now pass on AWS even though it has 'fails_on_aws' attr.
def test_bucket_create_naming_dns_dash_at_end():
    invalid_bucketname = "foo-"
    status, error_code = check_invalid_bucketname(invalid_bucketname)
    assert status == 400
    assert error_code == "InvalidBucketName"


# Breaks DNS with SubdomainCallingFormat
@pytest.mark.fails_on_aws
# Should now pass on AWS even though it has 'fails_on_aws' attr.
def test_bucket_create_naming_dns_dot_dot():
    invalid_bucketname = "foo..bar"
    status, error_code = check_invalid_bucketname(invalid_bucketname)
    assert status == 400
    assert error_code == "InvalidBucketName"


# Breaks DNS with SubdomainCallingFormat
@pytest.mark.fails_on_aws
# Should now pass on AWS even though it has 'fails_on_aws' attr.
def test_bucket_create_naming_dns_dot_dash():
    invalid_bucketname = "foo.-bar"
    status, error_code = check_invalid_bucketname(invalid_bucketname)
    assert status == 400
    assert error_code == "InvalidBucketName"


# Breaks DNS with SubdomainCallingFormat
@pytest.mark.fails_on_aws
# Should now pass on AWS even though it has 'fails_on_aws' attr.
def test_bucket_create_naming_dns_dash_dot():
    invalid_bucketname = "foo-.bar"
    status, error_code = check_invalid_bucketname(invalid_bucketname)
    assert status == 400
    assert error_code == "InvalidBucketName"


def test_bucket_create_exists():
    # aws-s3 default region allows recreation of buckets
    # but all other regions fail with BucketAlreadyOwnedByYou.
    bucket_name = get_new_bucket_name()
    client = get_client()

    client.create_bucket(Bucket=bucket_name)
    try:
        client.create_bucket(Bucket=bucket_name)
    except ClientError as e:
        status, error_code = _get_status_and_error_code(e.response)
        assert status == 409
        assert error_code == "BucketAlreadyOwnedByYou"


@pytest.mark.fails_on_dbstore
def test_bucket_get_location():
    location_constraint = get_main_api_name()
    if not location_constraint:
        pytest.skip("no api_name configured")
    bucket_name = get_new_bucket_name()
    client = get_client()

    client.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={"LocationConstraint": location_constraint},
    )

    response = client.get_bucket_location(Bucket=bucket_name)
    if location_constraint == "":
        location_constraint = None
    assert response["LocationConstraint"] == location_constraint


@pytest.mark.fails_on_dbstore
def test_bucket_create_exists_nonowner():
    # Names are shared across a global namespace. As such, no two
    # users can create a bucket with that same name.
    bucket_name = get_new_bucket_name()
    client = get_client()

    alt_client = get_alt_client()

    client.create_bucket(Bucket=bucket_name)
    e = assert_raises(ClientError, alt_client.create_bucket, Bucket=bucket_name)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 409
    assert error_code == "BucketAlreadyExists"


@pytest.mark.fails_on_dbstore
def test_bucket_recreate_overwrite_acl():
    bucket_name = get_new_bucket_name()
    client = get_client()

    client.create_bucket(Bucket=bucket_name, ACL="public-read")
    e = assert_raises(ClientError, client.create_bucket, Bucket=bucket_name)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 409
    assert error_code == "BucketAlreadyExists"


@pytest.mark.fails_on_dbstore
def test_bucket_recreate_new_acl():
    bucket_name = get_new_bucket_name()
    client = get_client()

    client.create_bucket(Bucket=bucket_name)
    e = assert_raises(
        ClientError, client.create_bucket, Bucket=bucket_name, ACL="public-read"
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 409
    assert error_code == "BucketAlreadyExists"


def check_access_denied(fn, *args, **kwargs):
    e = assert_raises(ClientError, fn, *args, **kwargs)
    status = _get_status(e.response)
    assert status == 403


def check_grants(got, want):
    """
    Check that grants list in got matches the dictionaries in want,
    in any order.
    """
    assert len(got) == len(want)

    # There are instances when got does not match due the order of item.
    if got[0]["Grantee"].get("DisplayName"):
        got.sort(
            key=lambda x: x["Grantee"].get("DisplayName")
            if x["Grantee"].get("DisplayName")
            else ""
        )
        want.sort(key=lambda x: x["DisplayName"] if x["DisplayName"] else "")

    for g, w in zip(got, want):
        w = dict(w)
        g = dict(g)
        assert g.pop("Permission", None) == w["Permission"]
        assert g["Grantee"].pop("DisplayName", None) == w["DisplayName"]
        assert g["Grantee"].pop("ID", None) == w["ID"]
        assert g["Grantee"].pop("Type", None) == w["Type"]
        assert g["Grantee"].pop("URI", None) == w["URI"]
        assert g["Grantee"].pop("EmailAddress", None) == w["EmailAddress"]
        assert g == {"Grantee": {}}


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/854")
def test_bucket_acl_default():
    bucket_name = get_new_bucket()
    client = get_client()

    response = client.get_bucket_acl(Bucket=bucket_name)

    display_name = get_main_display_name()
    user_id = get_main_user_id()

    assert response["Owner"]["DisplayName"] == display_name
    assert response["Owner"]["ID"] == user_id

    grants = response["Grants"]
    check_grants(
        grants,
        [
            dict(
                Permission="FULL_CONTROL",
                ID=user_id,
                DisplayName=display_name,
                URI=None,
                EmailAddress=None,
                Type="CanonicalUser",
            ),
        ],
    )


@pytest.mark.fails_on_aws
def test_bucket_acl_canned_during_create():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(ACL="public-read", Bucket=bucket_name)
    response = client.get_bucket_acl(Bucket=bucket_name)

    display_name = get_main_display_name()
    user_id = get_main_user_id()

    grants = response["Grants"]
    check_grants(
        grants,
        [
            dict(
                Permission="READ",
                ID=None,
                DisplayName=None,
                URI="http://acs.amazonaws.com/groups/global/AllUsers",
                EmailAddress=None,
                Type="Group",
            ),
            dict(
                Permission="FULL_CONTROL",
                ID=user_id,
                DisplayName=display_name,
                URI=None,
                EmailAddress=None,
                Type="CanonicalUser",
            ),
        ],
    )


def test_bucket_acl_canned():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(ACL="public-read", Bucket=bucket_name)
    response = client.get_bucket_acl(Bucket=bucket_name)

    display_name = get_main_display_name()
    user_id = get_main_user_id()

    grants = response["Grants"]
    check_grants(
        grants,
        [
            dict(
                Permission="READ",
                ID=None,
                DisplayName=None,
                URI="http://acs.amazonaws.com/groups/global/AllUsers",
                EmailAddress=None,
                Type="Group",
            ),
            dict(
                Permission="FULL_CONTROL",
                ID=user_id,
                DisplayName=display_name,
                URI=None,
                EmailAddress=None,
                Type="CanonicalUser",
            ),
        ],
    )

    client.put_bucket_acl(ACL="private", Bucket=bucket_name)
    response = client.get_bucket_acl(Bucket=bucket_name)

    grants = response["Grants"]
    check_grants(
        grants,
        [
            dict(
                Permission="FULL_CONTROL",
                ID=user_id,
                DisplayName=display_name,
                URI=None,
                EmailAddress=None,
                Type="CanonicalUser",
            ),
        ],
    )


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/857")
def test_bucket_acl_canned_publicreadwrite():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(ACL="public-read-write", Bucket=bucket_name)
    response = client.get_bucket_acl(Bucket=bucket_name)

    display_name = get_main_display_name()
    user_id = get_main_user_id()
    grants = response["Grants"]
    check_grants(
        grants,
        [
            dict(
                Permission="READ",
                ID=None,
                DisplayName=None,
                URI="http://acs.amazonaws.com/groups/global/AllUsers",
                EmailAddress=None,
                Type="Group",
            ),
            dict(
                Permission="WRITE",
                ID=None,
                DisplayName=None,
                URI="http://acs.amazonaws.com/groups/global/AllUsers",
                EmailAddress=None,
                Type="Group",
            ),
            dict(
                Permission="FULL_CONTROL",
                ID=user_id,
                DisplayName=display_name,
                URI=None,
                EmailAddress=None,
                Type="CanonicalUser",
            ),
        ],
    )


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/858")
def test_bucket_acl_canned_authenticatedread():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(ACL="authenticated-read", Bucket=bucket_name)
    response = client.get_bucket_acl(Bucket=bucket_name)

    display_name = get_main_display_name()
    user_id = get_main_user_id()

    grants = response["Grants"]
    check_grants(
        grants,
        [
            dict(
                Permission="READ",
                ID=None,
                DisplayName=None,
                URI="http://acs.amazonaws.com/groups/global/AuthenticatedUsers",
                EmailAddress=None,
                Type="Group",
            ),
            dict(
                Permission="FULL_CONTROL",
                ID=user_id,
                DisplayName=display_name,
                URI=None,
                EmailAddress=None,
                Type="CanonicalUser",
            ),
        ],
    )


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/859")
def test_object_acl_default():
    bucket_name = get_new_bucket()
    client = get_client()

    client.put_object(Bucket=bucket_name, Key="foo", Body="bar")
    response = client.get_object_acl(Bucket=bucket_name, Key="foo")

    display_name = get_main_display_name()
    user_id = get_main_user_id()

    grants = response["Grants"]
    check_grants(
        grants,
        [
            dict(
                Permission="FULL_CONTROL",
                ID=user_id,
                DisplayName=display_name,
                URI=None,
                EmailAddress=None,
                Type="CanonicalUser",
            ),
        ],
    )


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/895")
def test_object_acl_canned_during_create():
    bucket_name = get_new_bucket()
    client = get_client()

    client.put_object(ACL="public-read", Bucket=bucket_name, Key="foo", Body="bar")
    response = client.get_object_acl(Bucket=bucket_name, Key="foo")

    display_name = get_main_display_name()
    user_id = get_main_user_id()

    grants = response["Grants"]
    check_grants(
        grants,
        [
            dict(
                Permission="READ",
                ID=None,
                DisplayName=None,
                URI="http://acs.amazonaws.com/groups/global/AllUsers",
                EmailAddress=None,
                Type="Group",
            ),
            dict(
                Permission="FULL_CONTROL",
                ID=user_id,
                DisplayName=display_name,
                URI=None,
                EmailAddress=None,
                Type="CanonicalUser",
            ),
        ],
    )


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/895")
def test_object_acl_canned():
    bucket_name = get_new_bucket()
    client = get_client()

    # Since it defaults to private, set it public-read first
    client.put_object(ACL="public-read", Bucket=bucket_name, Key="foo", Body="bar")
    response = client.get_object_acl(Bucket=bucket_name, Key="foo")

    display_name = get_main_display_name()
    user_id = get_main_user_id()

    grants = response["Grants"]
    check_grants(
        grants,
        [
            dict(
                Permission="READ",
                ID=None,
                DisplayName=None,
                URI="http://acs.amazonaws.com/groups/global/AllUsers",
                EmailAddress=None,
                Type="Group",
            ),
            dict(
                Permission="FULL_CONTROL",
                ID=user_id,
                DisplayName=display_name,
                URI=None,
                EmailAddress=None,
                Type="CanonicalUser",
            ),
        ],
    )

    # Then back to private.
    client.put_object_acl(ACL="private", Bucket=bucket_name, Key="foo")
    response = client.get_object_acl(Bucket=bucket_name, Key="foo")
    grants = response["Grants"]

    check_grants(
        grants,
        [
            dict(
                Permission="FULL_CONTROL",
                ID=user_id,
                DisplayName=display_name,
                URI=None,
                EmailAddress=None,
                Type="CanonicalUser",
            ),
        ],
    )


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/895")
def test_object_acl_canned_publicreadwrite():
    bucket_name = get_new_bucket()
    client = get_client()

    client.put_object(
        ACL="public-read-write", Bucket=bucket_name, Key="foo", Body="bar"
    )
    response = client.get_object_acl(Bucket=bucket_name, Key="foo")

    display_name = get_main_display_name()
    user_id = get_main_user_id()

    grants = response["Grants"]
    check_grants(
        grants,
        [
            dict(
                Permission="READ",
                ID=None,
                DisplayName=None,
                URI="http://acs.amazonaws.com/groups/global/AllUsers",
                EmailAddress=None,
                Type="Group",
            ),
            dict(
                Permission="WRITE",
                ID=None,
                DisplayName=None,
                URI="http://acs.amazonaws.com/groups/global/AllUsers",
                EmailAddress=None,
                Type="Group",
            ),
            dict(
                Permission="FULL_CONTROL",
                ID=user_id,
                DisplayName=display_name,
                URI=None,
                EmailAddress=None,
                Type="CanonicalUser",
            ),
        ],
    )


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/858")
def test_object_acl_canned_authenticatedread():
    bucket_name = get_new_bucket()
    client = get_client()

    client.put_object(
        ACL="authenticated-read", Bucket=bucket_name, Key="foo", Body="bar"
    )
    response = client.get_object_acl(Bucket=bucket_name, Key="foo")

    display_name = get_main_display_name()
    user_id = get_main_user_id()

    grants = response["Grants"]
    check_grants(
        grants,
        [
            dict(
                Permission="READ",
                ID=None,
                DisplayName=None,
                URI="http://acs.amazonaws.com/groups/global/AuthenticatedUsers",
                EmailAddress=None,
                Type="Group",
            ),
            dict(
                Permission="FULL_CONTROL",
                ID=user_id,
                DisplayName=display_name,
                URI=None,
                EmailAddress=None,
                Type="CanonicalUser",
            ),
        ],
    )


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/895")
def test_object_acl_canned_bucketownerread():
    bucket_name = get_new_bucket_name()
    main_client = get_client()
    alt_client = get_alt_client()

    main_client.create_bucket(Bucket=bucket_name, ACL="public-read-write")

    alt_client.put_object(Bucket=bucket_name, Key="foo", Body="bar")

    bucket_acl_response = main_client.get_bucket_acl(Bucket=bucket_name)
    bucket_owner_id = bucket_acl_response["Grants"][2]["Grantee"]["ID"]
    bucket_owner_display_name = bucket_acl_response["Grants"][2]["Grantee"][
        "DisplayName"
    ]

    alt_client.put_object(ACL="bucket-owner-read", Bucket=bucket_name, Key="foo")
    response = alt_client.get_object_acl(Bucket=bucket_name, Key="foo")

    alt_display_name = get_alt_display_name()
    alt_user_id = get_alt_user_id()

    grants = response["Grants"]
    check_grants(
        grants,
        [
            dict(
                Permission="FULL_CONTROL",
                ID=alt_user_id,
                DisplayName=alt_display_name,
                URI=None,
                EmailAddress=None,
                Type="CanonicalUser",
            ),
            dict(
                Permission="READ",
                ID=bucket_owner_id,
                DisplayName=bucket_owner_display_name,
                URI=None,
                EmailAddress=None,
                Type="CanonicalUser",
            ),
        ],
    )


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/895")
def test_object_acl_canned_bucketownerfullcontrol():
    bucket_name = get_new_bucket_name()
    main_client = get_client()
    alt_client = get_alt_client()

    main_client.create_bucket(Bucket=bucket_name, ACL="public-read-write")

    alt_client.put_object(Bucket=bucket_name, Key="foo", Body="bar")

    bucket_acl_response = main_client.get_bucket_acl(Bucket=bucket_name)
    bucket_owner_id = bucket_acl_response["Grants"][2]["Grantee"]["ID"]
    bucket_owner_display_name = bucket_acl_response["Grants"][2]["Grantee"][
        "DisplayName"
    ]

    alt_client.put_object(
        ACL="bucket-owner-full-control", Bucket=bucket_name, Key="foo"
    )
    response = alt_client.get_object_acl(Bucket=bucket_name, Key="foo")

    alt_display_name = get_alt_display_name()
    alt_user_id = get_alt_user_id()

    grants = response["Grants"]
    check_grants(
        grants,
        [
            dict(
                Permission="FULL_CONTROL",
                ID=alt_user_id,
                DisplayName=alt_display_name,
                URI=None,
                EmailAddress=None,
                Type="CanonicalUser",
            ),
            dict(
                Permission="FULL_CONTROL",
                ID=bucket_owner_id,
                DisplayName=bucket_owner_display_name,
                URI=None,
                EmailAddress=None,
                Type="CanonicalUser",
            ),
        ],
    )


@pytest.mark.fails_on_aws
def test_object_acl_full_control_verify_owner():
    bucket_name = get_new_bucket_name()
    main_client = get_client()
    alt_client = get_alt_client()

    main_client.create_bucket(Bucket=bucket_name, ACL="public-read-write")

    main_client.put_object(Bucket=bucket_name, Key="foo", Body="bar")

    alt_user_id = get_alt_user_id()
    alt_display_name = get_alt_display_name()

    main_user_id = get_main_user_id()
    main_display_name = get_main_display_name()

    grant = {
        "Grants": [
            {
                "Grantee": {"ID": alt_user_id, "Type": "CanonicalUser"},
                "Permission": "FULL_CONTROL",
            }
        ],
        "Owner": {"DisplayName": main_display_name, "ID": main_user_id},
    }

    main_client.put_object_acl(Bucket=bucket_name, Key="foo", AccessControlPolicy=grant)

    grant = {
        "Grants": [
            {
                "Grantee": {"ID": alt_user_id, "Type": "CanonicalUser"},
                "Permission": "READ_ACP",
            }
        ],
        "Owner": {"DisplayName": main_display_name, "ID": main_user_id},
    }

    alt_client.put_object_acl(Bucket=bucket_name, Key="foo", AccessControlPolicy=grant)

    response = alt_client.get_object_acl(Bucket=bucket_name, Key="foo")
    assert response["Owner"]["ID"] == main_user_id


def add_obj_user_grant(bucket_name, key, grant):
    """
    Adds a grant to the existing grants meant to be passed into
    the AccessControlPolicy argument of put_object_acls for an object
    owned by the main user, not the alt user
    A grant is a dictionary in the form of:
    {u'Grantee': {u'Type': 'type', u'DisplayName': 'name', u'ID': 'id'}, u'Permission': 'PERM'}

    """
    client = get_client()
    main_user_id = get_main_user_id()
    main_display_name = get_main_display_name()

    response = client.get_object_acl(Bucket=bucket_name, Key=key)

    grants = response["Grants"]
    grants.append(grant)

    grant = {
        "Grants": grants,
        "Owner": {"DisplayName": main_display_name, "ID": main_user_id},
    }

    return grant


def test_object_acl_full_control_verify_attributes():
    bucket_name = get_new_bucket_name()
    main_client = get_client()
    alt_client = get_alt_client()

    main_client.create_bucket(Bucket=bucket_name, ACL="public-read-write")

    header = {"x-amz-foo": "bar"}
    # lambda to add any header
    add_header = lambda **kwargs: kwargs["params"]["headers"].update(header)

    main_client.meta.events.register("before-call.s3.PutObject", add_header)
    main_client.put_object(Bucket=bucket_name, Key="foo", Body="bar")

    response = main_client.get_object(Bucket=bucket_name, Key="foo")
    content_type = response["ContentType"]
    etag = response["ETag"]

    alt_user_id = get_alt_user_id()

    grant = {
        "Grantee": {"ID": alt_user_id, "Type": "CanonicalUser"},
        "Permission": "FULL_CONTROL",
    }

    grants = add_obj_user_grant(bucket_name, "foo", grant)

    main_client.put_object_acl(
        Bucket=bucket_name, Key="foo", AccessControlPolicy=grants
    )

    response = main_client.get_object(Bucket=bucket_name, Key="foo")
    assert content_type == response["ContentType"]
    assert etag == response["ETag"]


def test_bucket_acl_canned_private_to_private():
    bucket_name = get_new_bucket()
    client = get_client()

    response = client.put_bucket_acl(Bucket=bucket_name, ACL="private")
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


def add_bucket_user_grant(bucket_name, grant):
    """
    Adds a grant to the existing grants meant to be passed into
    the AccessControlPolicy argument of put_object_acls for an object
    owned by the main user, not the alt user
    A grant is a dictionary in the form of:
    {u'Grantee': {u'Type': 'type', u'DisplayName': 'name', u'ID': 'id'}, u'Permission': 'PERM'}
    """
    client = get_client()
    main_user_id = get_main_user_id()
    main_display_name = get_main_display_name()

    response = client.get_bucket_acl(Bucket=bucket_name)

    grants = response["Grants"]
    grants.append(grant)

    grant = {
        "Grants": grants,
        "Owner": {"DisplayName": main_display_name, "ID": main_user_id},
    }

    return grant


def _check_object_acl(permission):
    """
    Sets the permission on an object then checks to see
    if it was set
    """
    bucket_name = get_new_bucket()
    client = get_client()

    client.put_object(Bucket=bucket_name, Key="foo", Body="bar")

    response = client.get_object_acl(Bucket=bucket_name, Key="foo")

    policy = {}
    policy["Owner"] = response["Owner"]
    policy["Grants"] = response["Grants"]
    policy["Grants"][0]["Permission"] = permission

    client.put_object_acl(Bucket=bucket_name, Key="foo", AccessControlPolicy=policy)

    response = client.get_object_acl(Bucket=bucket_name, Key="foo")
    grants = response["Grants"]

    main_user_id = get_main_user_id()
    main_display_name = get_main_display_name()

    check_grants(
        grants,
        [
            dict(
                Permission=permission,
                ID=main_user_id,
                DisplayName=main_display_name,
                URI=None,
                EmailAddress=None,
                Type="CanonicalUser",
            ),
        ],
    )


@pytest.mark.fails_on_aws
def test_object_acl():
    _check_object_acl("FULL_CONTROL")


@pytest.mark.fails_on_aws
def test_object_acl_write():
    _check_object_acl("WRITE")


@pytest.mark.fails_on_aws
def test_object_acl_writeacp():
    _check_object_acl("WRITE_ACP")


@pytest.mark.fails_on_aws
def test_object_acl_read():
    _check_object_acl("READ")


@pytest.mark.fails_on_aws
def test_object_acl_readacp():
    _check_object_acl("READ_ACP")


def _bucket_acl_grant_userid(permission):
    """
    create a new bucket, grant a specific user the specified
    permission, read back the acl and verify correct setting
    """
    bucket_name = get_new_bucket()
    client = get_client()

    main_user_id = get_main_user_id()
    main_display_name = get_main_display_name()

    alt_user_id = get_alt_user_id()
    alt_display_name = get_alt_display_name()

    grant = {
        "Grantee": {"ID": alt_user_id, "Type": "CanonicalUser"},
        "Permission": permission,
    }

    grant = add_bucket_user_grant(bucket_name, grant)

    client.put_bucket_acl(Bucket=bucket_name, AccessControlPolicy=grant)

    response = client.get_bucket_acl(Bucket=bucket_name)

    grants = response["Grants"]
    check_grants(
        grants,
        [
            dict(
                Permission=permission,
                ID=alt_user_id,
                DisplayName=alt_display_name,
                URI=None,
                EmailAddress=None,
                Type="CanonicalUser",
            ),
            dict(
                Permission="FULL_CONTROL",
                ID=main_user_id,
                DisplayName=main_display_name,
                URI=None,
                EmailAddress=None,
                Type="CanonicalUser",
            ),
        ],
    )

    return bucket_name


def _check_bucket_acl_grant_can_read(bucket_name):
    """
    verify ability to read the specified bucket
    """
    alt_client = get_alt_client()
    response = alt_client.head_bucket(Bucket=bucket_name)


def _check_bucket_acl_grant_cant_read(bucket_name):
    """
    verify inability to read the specified bucket
    """
    alt_client = get_alt_client()
    check_access_denied(alt_client.head_bucket, Bucket=bucket_name)


def _check_bucket_acl_grant_can_readacp(bucket_name):
    """
    verify ability to read acls on specified bucket
    """
    alt_client = get_alt_client()
    alt_client.get_bucket_acl(Bucket=bucket_name)


def _check_bucket_acl_grant_cant_readacp(bucket_name):
    """
    verify inability to read acls on specified bucket
    """
    alt_client = get_alt_client()
    check_access_denied(alt_client.get_bucket_acl, Bucket=bucket_name)


def _check_bucket_acl_grant_can_write(bucket_name):
    """
    verify ability to write the specified bucket
    """
    alt_client = get_alt_client()
    alt_client.put_object(Bucket=bucket_name, Key="foo-write", Body="bar")


def _check_bucket_acl_grant_cant_write(bucket_name):
    """
    verify inability to write the specified bucket
    """
    alt_client = get_alt_client()
    check_access_denied(
        alt_client.put_object, Bucket=bucket_name, Key="foo-write", Body="bar"
    )


def _check_bucket_acl_grant_can_writeacp(bucket_name):
    """
    verify ability to set acls on the specified bucket
    """
    alt_client = get_alt_client()
    alt_client.put_bucket_acl(Bucket=bucket_name, ACL="public-read")


def _check_bucket_acl_grant_cant_writeacp(bucket_name):
    """
    verify inability to set acls on the specified bucket
    """
    alt_client = get_alt_client()
    check_access_denied(
        alt_client.put_bucket_acl, Bucket=bucket_name, ACL="public-read"
    )


@pytest.mark.fails_on_aws
def test_bucket_acl_grant_userid_fullcontrol():
    bucket_name = _bucket_acl_grant_userid("FULL_CONTROL")

    # alt user can read
    _check_bucket_acl_grant_can_read(bucket_name)
    # can read acl
    _check_bucket_acl_grant_can_readacp(bucket_name)
    # can write
    _check_bucket_acl_grant_can_write(bucket_name)
    # can write acl
    _check_bucket_acl_grant_can_writeacp(bucket_name)

    client = get_client()

    bucket_acl_response = client.get_bucket_acl(Bucket=bucket_name)
    owner_id = bucket_acl_response["Owner"]["ID"]
    owner_display_name = bucket_acl_response["Owner"]["DisplayName"]

    main_display_name = get_main_display_name()
    main_user_id = get_main_user_id()

    assert owner_id == main_user_id
    assert owner_display_name == main_display_name


@pytest.mark.fails_on_aws
def test_bucket_acl_grant_userid_read():
    bucket_name = _bucket_acl_grant_userid("READ")

    # alt user can read
    _check_bucket_acl_grant_can_read(bucket_name)
    # can't read acl
    _check_bucket_acl_grant_cant_readacp(bucket_name)
    # can't write
    _check_bucket_acl_grant_cant_write(bucket_name)
    # can't write acl
    _check_bucket_acl_grant_cant_writeacp(bucket_name)


@pytest.mark.fails_on_aws
def test_bucket_acl_grant_userid_readacp():
    bucket_name = _bucket_acl_grant_userid("READ_ACP")

    # alt user can't read
    _check_bucket_acl_grant_cant_read(bucket_name)
    # can read acl
    _check_bucket_acl_grant_can_readacp(bucket_name)
    # can't write
    _check_bucket_acl_grant_cant_write(bucket_name)
    # can't write acp
    # _check_bucket_acl_grant_cant_writeacp_can_readacp(bucket)
    _check_bucket_acl_grant_cant_writeacp(bucket_name)


@pytest.mark.fails_on_aws
def test_bucket_acl_grant_userid_write():
    bucket_name = _bucket_acl_grant_userid("WRITE")

    # alt user can't read
    _check_bucket_acl_grant_cant_read(bucket_name)
    # can't read acl
    _check_bucket_acl_grant_cant_readacp(bucket_name)
    # can write
    _check_bucket_acl_grant_can_write(bucket_name)
    # can't write acl
    _check_bucket_acl_grant_cant_writeacp(bucket_name)


@pytest.mark.fails_on_aws
def test_bucket_acl_grant_userid_writeacp():
    bucket_name = _bucket_acl_grant_userid("WRITE_ACP")

    # alt user can't read
    _check_bucket_acl_grant_cant_read(bucket_name)
    # can't read acl
    _check_bucket_acl_grant_cant_readacp(bucket_name)
    # can't write
    _check_bucket_acl_grant_cant_write(bucket_name)
    # can write acl
    _check_bucket_acl_grant_can_writeacp(bucket_name)


def test_bucket_acl_grant_nonexist_user():
    bucket_name = get_new_bucket()
    client = get_client()

    bad_user_id = "_foo"

    # response = client.get_bucket_acl(Bucket=bucket_name)
    grant = {
        "Grantee": {"ID": bad_user_id, "Type": "CanonicalUser"},
        "Permission": "FULL_CONTROL",
    }

    grant = add_bucket_user_grant(bucket_name, grant)

    e = assert_raises(
        ClientError,
        client.put_bucket_acl,
        Bucket=bucket_name,
        AccessControlPolicy=grant,
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == "InvalidArgument"


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/861")
def test_bucket_acl_no_grants():
    bucket_name = get_new_bucket()
    client = get_client()

    client.put_object(Bucket=bucket_name, Key="foo", Body="bar")
    response = client.get_bucket_acl(Bucket=bucket_name)
    old_grants = response["Grants"]
    policy = {}
    policy["Owner"] = response["Owner"]
    # clear grants
    policy["Grants"] = []

    # remove read/write permission
    response = client.put_bucket_acl(Bucket=bucket_name, AccessControlPolicy=policy)

    # can read
    client.get_object(Bucket=bucket_name, Key="foo")

    # can't write
    check_access_denied(client.put_object, Bucket=bucket_name, Key="baz", Body="a")

    # TODO fix this test once a fix is in for same issues in
    # test_access_bucket_private_object_private
    client2 = get_client()
    # owner can read acl
    client2.get_bucket_acl(Bucket=bucket_name)

    # owner can write acl
    client2.put_bucket_acl(Bucket=bucket_name, ACL="private")

    # set policy back to original so that bucket can be cleaned up
    policy["Grants"] = old_grants
    client2.put_bucket_acl(Bucket=bucket_name, AccessControlPolicy=policy)


def _get_acl_header(user_id=None, perms=None):
    all_headers = ["read", "write", "read-acp", "write-acp", "full-control"]
    headers = []

    if user_id == None:
        user_id = get_alt_user_id()

    if perms != None:
        for perm in perms:
            header = (
                "x-amz-grant-{perm}".format(perm=perm),
                "id={uid}".format(uid=user_id),
            )
            headers.append(header)

    else:
        for perm in all_headers:
            header = (
                "x-amz-grant-{perm}".format(perm=perm),
                "id={uid}".format(uid=user_id),
            )
            headers.append(header)

    return headers


@pytest.mark.fails_on_dho
@pytest.mark.fails_on_aws
def test_object_header_acl_grants():
    bucket_name = get_new_bucket()
    client = get_client()

    alt_user_id = get_alt_user_id()
    alt_display_name = get_alt_display_name()

    headers = _get_acl_header()

    def add_headers_before_sign(**kwargs):
        updated_headers = (
            kwargs["request"].__dict__["headers"].__dict__["_headers"] + headers
        )
        kwargs["request"].__dict__["headers"].__dict__["_headers"] = updated_headers

    client.meta.events.register("before-sign.s3.PutObject", add_headers_before_sign)

    client.put_object(Bucket=bucket_name, Key="foo_key", Body="bar")

    response = client.get_object_acl(Bucket=bucket_name, Key="foo_key")

    grants = response["Grants"]
    check_grants(
        grants,
        [
            dict(
                Permission="READ",
                ID=alt_user_id,
                DisplayName=alt_display_name,
                URI=None,
                EmailAddress=None,
                Type="CanonicalUser",
            ),
            dict(
                Permission="WRITE",
                ID=alt_user_id,
                DisplayName=alt_display_name,
                URI=None,
                EmailAddress=None,
                Type="CanonicalUser",
            ),
            dict(
                Permission="READ_ACP",
                ID=alt_user_id,
                DisplayName=alt_display_name,
                URI=None,
                EmailAddress=None,
                Type="CanonicalUser",
            ),
            dict(
                Permission="WRITE_ACP",
                ID=alt_user_id,
                DisplayName=alt_display_name,
                URI=None,
                EmailAddress=None,
                Type="CanonicalUser",
            ),
            dict(
                Permission="FULL_CONTROL",
                ID=alt_user_id,
                DisplayName=alt_display_name,
                URI=None,
                EmailAddress=None,
                Type="CanonicalUser",
            ),
        ],
    )


@pytest.mark.fails_on_dho
@pytest.mark.fails_on_aws
@pytest.mark.skip(reason="Potential Bug")
def test_bucket_header_acl_grants():
    headers = _get_acl_header()
    bucket_name = get_new_bucket_name()
    client = get_client()

    headers = _get_acl_header()

    def add_headers_before_sign(**kwargs):
        updated_headers = (
            kwargs["request"].__dict__["headers"].__dict__["_headers"] + headers
        )
        kwargs["request"].__dict__["headers"].__dict__["_headers"] = updated_headers

    client.meta.events.register("before-sign.s3.CreateBucket", add_headers_before_sign)

    client.create_bucket(Bucket=bucket_name)

    response = client.get_bucket_acl(Bucket=bucket_name)

    grants = response["Grants"]
    alt_user_id = get_alt_user_id()
    alt_display_name = get_alt_display_name()

    check_grants(
        grants,
        [
            dict(
                Permission="READ",
                ID=alt_user_id,
                DisplayName=alt_display_name,
                URI=None,
                EmailAddress=None,
                Type="CanonicalUser",
            ),
            dict(
                Permission="WRITE",
                ID=alt_user_id,
                DisplayName=alt_display_name,
                URI=None,
                EmailAddress=None,
                Type="CanonicalUser",
            ),
            dict(
                Permission="READ_ACP",
                ID=alt_user_id,
                DisplayName=alt_display_name,
                URI=None,
                EmailAddress=None,
                Type="CanonicalUser",
            ),
            dict(
                Permission="WRITE_ACP",
                ID=alt_user_id,
                DisplayName=alt_display_name,
                URI=None,
                EmailAddress=None,
                Type="CanonicalUser",
            ),
            dict(
                Permission="FULL_CONTROL",
                ID=alt_user_id,
                DisplayName=alt_display_name,
                URI=None,
                EmailAddress=None,
                Type="CanonicalUser",
            ),
        ],
    )

    alt_client = get_alt_client()

    alt_client.put_object(Bucket=bucket_name, Key="foo", Body="bar")

    # set bucket acl to public-read-write so that teardown can work
    alt_client.put_bucket_acl(Bucket=bucket_name, ACL="public-read-write")


# This test will fail on DH Objects. DHO allows multiple users with one account, which
# would violate the uniqueness requirement of a user's email. As such, DHO users are
# created without an email.
@pytest.mark.fails_on_aws
def test_bucket_acl_grant_email():
    bucket_name = get_new_bucket()
    client = get_client()

    alt_user_id = get_alt_user_id()
    alt_display_name = get_alt_display_name()
    alt_email_address = get_alt_email()

    main_user_id = get_main_user_id()
    main_display_name = get_main_display_name()

    grant = {
        "Grantee": {"EmailAddress": alt_email_address, "Type": "AmazonCustomerByEmail"},
        "Permission": "FULL_CONTROL",
    }

    grant = add_bucket_user_grant(bucket_name, grant)

    client.put_bucket_acl(Bucket=bucket_name, AccessControlPolicy=grant)

    response = client.get_bucket_acl(Bucket=bucket_name)

    grants = response["Grants"]
    check_grants(
        grants,
        [
            dict(
                Permission="FULL_CONTROL",
                ID=alt_user_id,
                DisplayName=alt_display_name,
                URI=None,
                EmailAddress=None,
                Type="CanonicalUser",
            ),
            dict(
                Permission="FULL_CONTROL",
                ID=main_user_id,
                DisplayName=main_display_name,
                URI=None,
                EmailAddress=None,
                Type="CanonicalUser",
            ),
        ],
    )


@pytest.mark.skip(reason="Potential Bug")
def test_bucket_acl_grant_email_not_exist():
    # behavior not documented by amazon
    bucket_name = get_new_bucket()
    client = get_client()

    alt_user_id = get_alt_user_id()
    alt_display_name = get_alt_display_name()
    alt_email_address = get_alt_email()

    NONEXISTENT_EMAIL = "doesnotexist@dreamhost.com.invalid"
    grant = {
        "Grantee": {"EmailAddress": NONEXISTENT_EMAIL, "Type": "AmazonCustomerByEmail"},
        "Permission": "FULL_CONTROL",
    }

    grant = add_bucket_user_grant(bucket_name, grant)

    e = assert_raises(
        ClientError,
        client.put_bucket_acl,
        Bucket=bucket_name,
        AccessControlPolicy=grant,
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == "UnresolvableGrantByEmailAddress"


@pytest.mark.skip(reason="Potential Bug")
def test_bucket_acl_revoke_all():
    # revoke all access, including the owner's access
    bucket_name = get_new_bucket()
    client = get_client()

    client.put_object(Bucket=bucket_name, Key="foo", Body="bar")
    response = client.get_bucket_acl(Bucket=bucket_name)
    old_grants = response["Grants"]
    policy = {}
    policy["Owner"] = response["Owner"]
    # clear grants
    policy["Grants"] = []

    # remove read/write permission for everyone
    client.put_bucket_acl(Bucket=bucket_name, AccessControlPolicy=policy)

    response = client.get_bucket_acl(Bucket=bucket_name)

    assert len(response["Grants"]) == 0

    # set policy back to original so that bucket can be cleaned up
    policy["Grants"] = old_grants
    client.put_bucket_acl(Bucket=bucket_name, AccessControlPolicy=policy)


# TODO rgw log_bucket.set_as_logging_target() gives 403 Forbidden
# http://tracker.newdream.net/issues/984
@pytest.mark.fails_on_rgw
@pytest.mark.skip(reason="Potential Bug")
def test_logging_toggle():
    bucket_name = get_new_bucket()
    client = get_client()

    main_display_name = get_main_display_name()
    main_user_id = get_main_user_id()

    status = {
        "LoggingEnabled": {
            "TargetBucket": bucket_name,
            "TargetGrants": [
                {
                    "Grantee": {
                        "DisplayName": main_display_name,
                        "ID": main_user_id,
                        "Type": "CanonicalUser",
                    },
                    "Permission": "FULL_CONTROL",
                }
            ],
            "TargetPrefix": "foologgingprefix",
        }
    }

    client.put_bucket_logging(Bucket=bucket_name, BucketLoggingStatus=status)
    client.get_bucket_logging(Bucket=bucket_name)
    status = {"LoggingEnabled": {}}
    client.put_bucket_logging(Bucket=bucket_name, BucketLoggingStatus=status)
    # NOTE: this does not actually test whether or not logging works


def _setup_access(bucket_acl, object_acl):
    """
    Simple test fixture: create a bucket with given ACL, with objects:
    - a: owning user, given ACL
    - a2: same object accessed by some other user
    - b: owning user, default ACL in bucket w/given ACL
    - b2: same object accessed by a some other user
    """
    bucket_name = get_new_bucket()
    client = get_client()

    key1 = "foo"
    key2 = "bar"
    newkey = "new"

    client.put_bucket_acl(Bucket=bucket_name, ACL=bucket_acl)
    client.put_object(Bucket=bucket_name, Key=key1, Body="foocontent")
    client.put_object_acl(Bucket=bucket_name, Key=key1, ACL=object_acl)
    client.put_object(Bucket=bucket_name, Key=key2, Body="barcontent")

    return bucket_name, key1, key2, newkey


def get_bucket_key_names(bucket_name):
    objs_list = get_objects_list(bucket_name)
    return frozenset(obj for obj in objs_list)


def list_bucket_storage_class(client, bucket_name):
    result = defaultdict(list)
    response = client.list_object_versions(Bucket=bucket_name)
    for k in response["Versions"]:
        result[k["StorageClass"]].append(k)

    return result


def list_bucket_versions(client, bucket_name):
    result = defaultdict(list)
    response = client.list_object_versions(Bucket=bucket_name)
    for k in response["Versions"]:
        result[response["Name"]].append(k)

    return result


def test_access_bucket_private_object_private():
    # all the test_access_* tests follow this template
    bucket_name, key1, key2, newkey = _setup_access(
        bucket_acl="private", object_acl="private"
    )

    alt_client = get_alt_client()
    # acled object read fail
    check_access_denied(alt_client.get_object, Bucket=bucket_name, Key=key1)
    # default object read fail
    check_access_denied(alt_client.get_object, Bucket=bucket_name, Key=key2)
    # bucket read fail
    check_access_denied(alt_client.list_objects, Bucket=bucket_name)

    # acled object write fail
    check_access_denied(
        alt_client.put_object, Bucket=bucket_name, Key=key1, Body="barcontent"
    )
    # NOTE: The above put's causes the connection to go bad, therefore the client can't be used
    # anymore. This can be solved either by:
    # 1) putting an empty string ('') in the 'Body' field of those put_object calls
    # 2) getting a new client hence the creation of alt_client{2,3} for the tests below
    # TODO: Test it from another host and on AWS, Report this to Amazon, if findings are identical

    alt_client2 = get_alt_client()
    # default object write fail
    check_access_denied(
        alt_client2.put_object, Bucket=bucket_name, Key=key2, Body="baroverwrite"
    )
    # bucket write fail
    alt_client3 = get_alt_client()
    check_access_denied(
        alt_client3.put_object, Bucket=bucket_name, Key=newkey, Body="newcontent"
    )


@pytest.mark.list_objects_v2
def test_access_bucket_private_objectv2_private():
    # all the test_access_* tests follow this template
    bucket_name, key1, key2, newkey = _setup_access(
        bucket_acl="private", object_acl="private"
    )

    alt_client = get_alt_client()
    # acled object read fail
    check_access_denied(alt_client.get_object, Bucket=bucket_name, Key=key1)
    # default object read fail
    check_access_denied(alt_client.get_object, Bucket=bucket_name, Key=key2)
    # bucket read fail
    check_access_denied(alt_client.list_objects_v2, Bucket=bucket_name)

    # acled object write fail
    check_access_denied(
        alt_client.put_object, Bucket=bucket_name, Key=key1, Body="barcontent"
    )
    # NOTE: The above put's causes the connection to go bad, therefore the client can't be used
    # anymore. This can be solved either by:
    # 1) putting an empty string ('') in the 'Body' field of those put_object calls
    # 2) getting a new client hence the creation of alt_client{2,3} for the tests below
    # TODO: Test it from another host and on AWS, Report this to Amazon, if findings are identical

    alt_client2 = get_alt_client()
    # default object write fail
    check_access_denied(
        alt_client2.put_object, Bucket=bucket_name, Key=key2, Body="baroverwrite"
    )
    # bucket write fail
    alt_client3 = get_alt_client()
    check_access_denied(
        alt_client3.put_object, Bucket=bucket_name, Key=newkey, Body="newcontent"
    )


@pytest.mark.skip(reason="Potential Bug")
def test_access_bucket_private_object_publicread():
    bucket_name, key1, key2, newkey = _setup_access(
        bucket_acl="private", object_acl="public-read"
    )
    alt_client = get_alt_client()
    response = alt_client.get_object(Bucket=bucket_name, Key=key1)

    body = _get_body(response)

    # a should be public-read, b gets default (private)
    assert body == "foocontent"

    check_access_denied(
        alt_client.put_object, Bucket=bucket_name, Key=key1, Body="foooverwrite"
    )
    alt_client2 = get_alt_client()
    check_access_denied(alt_client2.get_object, Bucket=bucket_name, Key=key2)
    check_access_denied(
        alt_client2.put_object, Bucket=bucket_name, Key=key2, Body="baroverwrite"
    )

    alt_client3 = get_alt_client()
    check_access_denied(alt_client3.list_objects, Bucket=bucket_name)
    check_access_denied(
        alt_client3.put_object, Bucket=bucket_name, Key=newkey, Body="newcontent"
    )


@pytest.mark.list_objects_v2
@pytest.mark.skip(reason="Potential Bug")
def test_access_bucket_private_objectv2_publicread():
    bucket_name, key1, key2, newkey = _setup_access(
        bucket_acl="private", object_acl="public-read"
    )
    alt_client = get_alt_client()
    response = alt_client.get_object(Bucket=bucket_name, Key=key1)

    body = _get_body(response)

    # a should be public-read, b gets default (private)
    assert body == "foocontent"

    check_access_denied(
        alt_client.put_object, Bucket=bucket_name, Key=key1, Body="foooverwrite"
    )
    alt_client2 = get_alt_client()
    check_access_denied(alt_client2.get_object, Bucket=bucket_name, Key=key2)
    check_access_denied(
        alt_client2.put_object, Bucket=bucket_name, Key=key2, Body="baroverwrite"
    )

    alt_client3 = get_alt_client()
    check_access_denied(alt_client3.list_objects_v2, Bucket=bucket_name)
    check_access_denied(
        alt_client3.put_object, Bucket=bucket_name, Key=newkey, Body="newcontent"
    )


@pytest.mark.skip(reason="Potential Bug")
def test_access_bucket_private_object_publicreadwrite():
    bucket_name, key1, key2, newkey = _setup_access(
        bucket_acl="private", object_acl="public-read-write"
    )
    alt_client = get_alt_client()
    response = alt_client.get_object(Bucket=bucket_name, Key=key1)

    body = _get_body(response)

    # a should be public-read-only ... because it is in a private bucket
    # b gets default (private)
    assert body == "foocontent"

    check_access_denied(
        alt_client.put_object, Bucket=bucket_name, Key=key1, Body="foooverwrite"
    )
    alt_client2 = get_alt_client()
    check_access_denied(alt_client2.get_object, Bucket=bucket_name, Key=key2)
    check_access_denied(
        alt_client2.put_object, Bucket=bucket_name, Key=key2, Body="baroverwrite"
    )

    alt_client3 = get_alt_client()
    check_access_denied(alt_client3.list_objects, Bucket=bucket_name)
    check_access_denied(
        alt_client3.put_object, Bucket=bucket_name, Key=newkey, Body="newcontent"
    )


@pytest.mark.list_objects_v2
@pytest.mark.skip(reason="Potential Bug")
def test_access_bucket_private_objectv2_publicreadwrite():
    bucket_name, key1, key2, newkey = _setup_access(
        bucket_acl="private", object_acl="public-read-write"
    )
    alt_client = get_alt_client()
    response = alt_client.get_object(Bucket=bucket_name, Key=key1)

    body = _get_body(response)

    # a should be public-read-only ... because it is in a private bucket
    # b gets default (private)
    assert body == "foocontent"

    check_access_denied(
        alt_client.put_object, Bucket=bucket_name, Key=key1, Body="foooverwrite"
    )
    alt_client2 = get_alt_client()
    check_access_denied(alt_client2.get_object, Bucket=bucket_name, Key=key2)
    check_access_denied(
        alt_client2.put_object, Bucket=bucket_name, Key=key2, Body="baroverwrite"
    )

    alt_client3 = get_alt_client()
    check_access_denied(alt_client3.list_objects_v2, Bucket=bucket_name)
    check_access_denied(
        alt_client3.put_object, Bucket=bucket_name, Key=newkey, Body="newcontent"
    )


@pytest.mark.skip(reason="Potential Bug")
def test_access_bucket_publicread_object_private():
    bucket_name, key1, key2, newkey = _setup_access(
        bucket_acl="public-read", object_acl="private"
    )
    alt_client = get_alt_client()

    # a should be private, b gets default (private)
    check_access_denied(alt_client.get_object, Bucket=bucket_name, Key=key1)
    check_access_denied(
        alt_client.put_object, Bucket=bucket_name, Key=key1, Body="barcontent"
    )

    alt_client2 = get_alt_client()
    check_access_denied(alt_client2.get_object, Bucket=bucket_name, Key=key2)
    check_access_denied(
        alt_client2.put_object, Bucket=bucket_name, Key=key2, Body="baroverwrite"
    )

    alt_client3 = get_alt_client()

    objs = get_objects_list(bucket=bucket_name, client=alt_client3)

    assert objs == ["bar", "foo"]
    check_access_denied(
        alt_client3.put_object, Bucket=bucket_name, Key=newkey, Body="newcontent"
    )


@pytest.mark.skip(reason="Potential Bug")
def test_access_bucket_publicread_object_publicread():
    bucket_name, key1, key2, newkey = _setup_access(
        bucket_acl="public-read", object_acl="public-read"
    )
    alt_client = get_alt_client()

    response = alt_client.get_object(Bucket=bucket_name, Key=key1)

    # a should be public-read, b gets default (private)
    body = _get_body(response)
    assert body == "foocontent"

    check_access_denied(
        alt_client.put_object, Bucket=bucket_name, Key=key1, Body="foooverwrite"
    )

    alt_client2 = get_alt_client()
    check_access_denied(alt_client2.get_object, Bucket=bucket_name, Key=key2)
    check_access_denied(
        alt_client2.put_object, Bucket=bucket_name, Key=key2, Body="baroverwrite"
    )

    alt_client3 = get_alt_client()

    objs = get_objects_list(bucket=bucket_name, client=alt_client3)

    assert objs == ["bar", "foo"]
    check_access_denied(
        alt_client3.put_object, Bucket=bucket_name, Key=newkey, Body="newcontent"
    )


@pytest.mark.skip(reason="Potential Bug")
def test_access_bucket_publicread_object_publicreadwrite():
    bucket_name, key1, key2, newkey = _setup_access(
        bucket_acl="public-read", object_acl="public-read-write"
    )
    alt_client = get_alt_client()

    response = alt_client.get_object(Bucket=bucket_name, Key=key1)

    body = _get_body(response)

    # a should be public-read-only ... because it is in a r/o bucket
    # b gets default (private)
    assert body == "foocontent"

    check_access_denied(
        alt_client.put_object, Bucket=bucket_name, Key=key1, Body="foooverwrite"
    )

    alt_client2 = get_alt_client()
    check_access_denied(alt_client2.get_object, Bucket=bucket_name, Key=key2)
    check_access_denied(
        alt_client2.put_object, Bucket=bucket_name, Key=key2, Body="baroverwrite"
    )

    alt_client3 = get_alt_client()

    objs = get_objects_list(bucket=bucket_name, client=alt_client3)

    assert objs == ["bar", "foo"]
    check_access_denied(
        alt_client3.put_object, Bucket=bucket_name, Key=newkey, Body="newcontent"
    )


@pytest.mark.skip(reason="Potential Bug")
def test_access_bucket_publicreadwrite_object_private():
    bucket_name, key1, key2, newkey = _setup_access(
        bucket_acl="public-read-write", object_acl="private"
    )
    alt_client = get_alt_client()

    # a should be private, b gets default (private)
    check_access_denied(alt_client.get_object, Bucket=bucket_name, Key=key1)
    alt_client.put_object(Bucket=bucket_name, Key=key1, Body="barcontent")

    check_access_denied(alt_client.get_object, Bucket=bucket_name, Key=key2)
    alt_client.put_object(Bucket=bucket_name, Key=key2, Body="baroverwrite")

    objs = get_objects_list(bucket=bucket_name, client=alt_client)
    assert objs == ["bar", "foo"]
    alt_client.put_object(Bucket=bucket_name, Key=newkey, Body="newcontent")


@pytest.mark.skip(reason="Potential Bug")
def test_access_bucket_publicreadwrite_object_publicread():
    bucket_name, key1, key2, newkey = _setup_access(
        bucket_acl="public-read-write", object_acl="public-read"
    )
    alt_client = get_alt_client()

    # a should be public-read, b gets default (private)
    response = alt_client.get_object(Bucket=bucket_name, Key=key1)

    body = _get_body(response)
    assert body == "foocontent"
    alt_client.put_object(Bucket=bucket_name, Key=key1, Body="barcontent")

    check_access_denied(alt_client.get_object, Bucket=bucket_name, Key=key2)
    alt_client.put_object(Bucket=bucket_name, Key=key2, Body="baroverwrite")

    objs = get_objects_list(bucket=bucket_name, client=alt_client)
    assert objs == ["bar", "foo"]
    alt_client.put_object(Bucket=bucket_name, Key=newkey, Body="newcontent")


@pytest.mark.skip(reason="Potential Bug")
def test_access_bucket_publicreadwrite_object_publicreadwrite():
    bucket_name, key1, key2, newkey = _setup_access(
        bucket_acl="public-read-write", object_acl="public-read-write"
    )
    alt_client = get_alt_client()
    response = alt_client.get_object(Bucket=bucket_name, Key=key1)
    body = _get_body(response)

    # a should be public-read-write, b gets default (private)
    assert body == "foocontent"
    alt_client.put_object(Bucket=bucket_name, Key=key1, Body="foooverwrite")
    check_access_denied(alt_client.get_object, Bucket=bucket_name, Key=key2)
    alt_client.put_object(Bucket=bucket_name, Key=key2, Body="baroverwrite")
    objs = get_objects_list(bucket=bucket_name, client=alt_client)
    assert objs == ["bar", "foo"]
    alt_client.put_object(Bucket=bucket_name, Key=newkey, Body="newcontent")


def test_buckets_create_then_list():
    client = get_client()
    bucket_names = []
    for i in range(5):
        bucket_name = get_new_bucket_name()
        bucket_names.append(bucket_name)

    for name in bucket_names:
        client.create_bucket(Bucket=name)

    response = client.list_buckets()
    bucket_dicts = response["Buckets"]
    buckets_list = []

    buckets_list = get_buckets_list()

    for name in bucket_names:
        if name not in buckets_list:
            raise RuntimeError(
                "S3 implementation's GET on Service did not return bucket we created: %r",
                name,
            )


def test_buckets_list_ctime():
    # check that creation times are within a day
    before = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)

    client = get_client()
    for i in range(5):
        client.create_bucket(Bucket=get_new_bucket_name())

    response = client.list_buckets()
    for bucket in response["Buckets"]:
        ctime = bucket["CreationDate"]
        assert before <= ctime, "%r > %r" % (before, ctime)


@pytest.mark.fails_on_aws
def test_list_buckets_anonymous():
    # Get a connection with bad authorization, then change it to be our new Anonymous auth mechanism,
    # emulating standard HTTP access.
    #
    # While it may have been possible to use httplib directly, doing it this way takes care of also
    # allowing us to vary the calling format in testing.
    unauthenticated_client = get_unauthenticated_client()
    response = unauthenticated_client.list_buckets()
    assert len(response["Buckets"]) == 0


def test_list_buckets_invalid_auth():
    bad_auth_client = get_bad_auth_client()
    e = assert_raises(ClientError, bad_auth_client.list_buckets)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == "InvalidAccessKeyId"


def test_list_buckets_bad_auth():
    main_access_key = get_main_aws_access_key()
    bad_auth_client = get_bad_auth_client(aws_access_key_id=main_access_key)
    e = assert_raises(ClientError, bad_auth_client.list_buckets)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == "SignatureDoesNotMatch"


@pytest.fixture
def override_prefix_a():
    nuke_prefixed_buckets(prefix="a" + get_prefix())
    yield
    nuke_prefixed_buckets(prefix="a" + get_prefix())


# this test goes outside the user-configure prefix because it needs to
# control the initial character of the bucket name
def test_bucket_create_naming_good_starts_alpha(override_prefix_a):
    check_good_bucket_name("foo", _prefix="a" + get_prefix())


@pytest.fixture
def override_prefix_0():
    nuke_prefixed_buckets(prefix="0" + get_prefix())
    yield
    nuke_prefixed_buckets(prefix="0" + get_prefix())


# this test goes outside the user-configure prefix because it needs to
# control the initial character of the bucket name
def test_bucket_create_naming_good_starts_digit(override_prefix_0):
    check_good_bucket_name("foo", _prefix="0" + get_prefix())


@pytest.mark.skip(reason="Potential Bug")
def test_bucket_create_naming_good_contains_period():
    check_good_bucket_name("aaa.111")


def test_bucket_create_naming_good_contains_hyphen():
    check_good_bucket_name("aaa-111")


@pytest.mark.skip(reason="Potential Bug")
def test_bucket_recreate_not_overriding():
    key_names = ["mykey1", "mykey2"]
    bucket_name = _create_objects(keys=key_names)

    objs_list = get_objects_list(bucket_name)
    assert key_names == objs_list

    client = get_client()
    client.create_bucket(Bucket=bucket_name)

    objs_list = get_objects_list(bucket_name)
    assert key_names == objs_list


@pytest.mark.fails_on_dbstore
def test_bucket_create_special_key_names():
    key_names = [
        " ",
        '"',
        "$",
        "%",
        "&",
        "'",
        "<",
        ">",
        "_",
        "_ ",
        "_ _",
        "__",
    ]

    bucket_name = _create_objects(keys=key_names)

    objs_list = get_objects_list(bucket_name)
    assert key_names == objs_list

    client = get_client()

    for name in key_names:
        assert name in objs_list
        response = client.get_object(Bucket=bucket_name, Key=name)
        body = _get_body(response)
        assert name == body
        client.put_object_acl(Bucket=bucket_name, Key=name, ACL="private")


def test_bucket_list_special_prefix():
    key_names = ["_bla/1", "_bla/2", "_bla/3", "_bla/4", "abcd"]
    bucket_name = _create_objects(keys=key_names)

    objs_list = get_objects_list(bucket_name)

    assert len(objs_list) == 5

    objs_list = get_objects_list(bucket_name, prefix="_bla/")
    assert len(objs_list) == 4


@pytest.mark.fails_on_dbstore
def test_object_copy_zero_size():
    key = "foo123bar"
    bucket_name = _create_objects(keys=[key])
    fp_a = FakeWriteFile(0, "")
    client = get_client()
    client.put_object(Bucket=bucket_name, Key=key, Body=fp_a)

    copy_source = {"Bucket": bucket_name, "Key": key}

    client.copy(copy_source, bucket_name, "bar321foo")
    response = client.get_object(Bucket=bucket_name, Key="bar321foo")
    assert response["ContentLength"] == 0


@pytest.mark.fails_on_dbstore
def test_object_copy_16m():
    bucket_name = get_new_bucket()
    key1 = "obj1"
    client = get_client()
    client.put_object(Bucket=bucket_name, Key=key1, Body=bytearray(16 * 1024 * 1024))

    copy_source = {"Bucket": bucket_name, "Key": key1}
    key2 = "obj2"
    client.copy_object(Bucket=bucket_name, Key=key2, CopySource=copy_source)
    response = client.get_object(Bucket=bucket_name, Key=key2)
    assert response["ContentLength"] == 16 * 1024 * 1024


@pytest.mark.fails_on_dbstore
def test_object_copy_same_bucket():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_object(Bucket=bucket_name, Key="foo123bar", Body="foo")

    copy_source = {"Bucket": bucket_name, "Key": "foo123bar"}

    client.copy(copy_source, bucket_name, "bar321foo")

    response = client.get_object(Bucket=bucket_name, Key="bar321foo")
    body = _get_body(response)
    assert "foo" == body


@pytest.mark.fails_on_dbstore
def test_object_copy_verify_contenttype():
    bucket_name = get_new_bucket()
    client = get_client()

    content_type = "text/bla"
    client.put_object(
        Bucket=bucket_name, ContentType=content_type, Key="foo123bar", Body="foo"
    )

    copy_source = {"Bucket": bucket_name, "Key": "foo123bar"}

    client.copy(copy_source, bucket_name, "bar321foo")

    response = client.get_object(Bucket=bucket_name, Key="bar321foo")
    body = _get_body(response)
    assert "foo" == body
    response_content_type = response["ContentType"]
    assert response_content_type == content_type


def test_object_copy_to_itself():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_object(Bucket=bucket_name, Key="foo123bar", Body="foo")

    copy_source = {"Bucket": bucket_name, "Key": "foo123bar"}

    e = assert_raises(ClientError, client.copy, copy_source, bucket_name, "foo123bar")
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == "InvalidRequest"


@pytest.mark.fails_on_dbstore
def test_object_copy_to_itself_with_metadata():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_object(Bucket=bucket_name, Key="foo123bar", Body="foo")
    copy_source = {"Bucket": bucket_name, "Key": "foo123bar"}
    metadata = {"foo": "bar"}

    client.copy_object(
        Bucket=bucket_name,
        CopySource=copy_source,
        Key="foo123bar",
        Metadata=metadata,
        MetadataDirective="REPLACE",
    )
    response = client.get_object(Bucket=bucket_name, Key="foo123bar")
    assert response["Metadata"] == metadata


@pytest.mark.fails_on_dbstore
def test_object_copy_diff_bucket():
    bucket_name1 = get_new_bucket()
    bucket_name2 = get_new_bucket()

    client = get_client()
    client.put_object(Bucket=bucket_name1, Key="foo123bar", Body="foo")

    copy_source = {"Bucket": bucket_name1, "Key": "foo123bar"}

    client.copy(copy_source, bucket_name2, "bar321foo")

    response = client.get_object(Bucket=bucket_name2, Key="bar321foo")
    body = _get_body(response)
    assert "foo" == body


def test_object_copy_not_owned_bucket():
    client = get_client()
    alt_client = get_alt_client()
    bucket_name1 = get_new_bucket_name()
    bucket_name2 = get_new_bucket_name()
    client.create_bucket(Bucket=bucket_name1)
    alt_client.create_bucket(Bucket=bucket_name2)

    client.put_object(Bucket=bucket_name1, Key="foo123bar", Body="foo")

    copy_source = {"Bucket": bucket_name1, "Key": "foo123bar"}

    e = assert_raises(
        ClientError, alt_client.copy, copy_source, bucket_name2, "bar321foo"
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403


@pytest.mark.skip(reason="Potential Bug")
def test_object_copy_not_owned_object_bucket():
    client = get_client()
    alt_client = get_alt_client()
    bucket_name = get_new_bucket_name()
    client.create_bucket(Bucket=bucket_name)
    client.put_object(Bucket=bucket_name, Key="foo123bar", Body="foo")

    alt_user_id = get_alt_user_id()

    grant = {
        "Grantee": {"ID": alt_user_id, "Type": "CanonicalUser"},
        "Permission": "FULL_CONTROL",
    }
    grants = add_obj_user_grant(bucket_name, "foo123bar", grant)
    client.put_object_acl(
        Bucket=bucket_name, Key="foo123bar", AccessControlPolicy=grants
    )

    grant = add_bucket_user_grant(bucket_name, grant)
    client.put_bucket_acl(Bucket=bucket_name, AccessControlPolicy=grant)

    alt_client.get_object(Bucket=bucket_name, Key="foo123bar")

    copy_source = {"Bucket": bucket_name, "Key": "foo123bar"}
    alt_client.copy(copy_source, bucket_name, "bar321foo")


@pytest.mark.fails_on_dbstore
def test_object_copy_canned_acl():
    bucket_name = get_new_bucket()
    client = get_client()
    alt_client = get_alt_client()
    client.put_object(Bucket=bucket_name, Key="foo123bar", Body="foo")

    copy_source = {"Bucket": bucket_name, "Key": "foo123bar"}
    client.copy_object(
        Bucket=bucket_name, CopySource=copy_source, Key="bar321foo", ACL="public-read"
    )
    # check ACL is applied by doing GET from another user
    alt_client.get_object(Bucket=bucket_name, Key="bar321foo")

    metadata = {"abc": "def"}
    copy_source = {"Bucket": bucket_name, "Key": "bar321foo"}
    client.copy_object(
        ACL="public-read",
        Bucket=bucket_name,
        CopySource=copy_source,
        Key="foo123bar",
        Metadata=metadata,
        MetadataDirective="REPLACE",
    )

    # check ACL is applied by doing GET from another user
    alt_client.get_object(Bucket=bucket_name, Key="foo123bar")


@pytest.mark.fails_on_dbstore
def test_object_copy_retaining_metadata():
    for size in [3, 1024 * 1024]:
        bucket_name = get_new_bucket()
        client = get_client()
        content_type = "audio/ogg"

        metadata = {"key1": "value1", "key2": "value2"}
        client.put_object(
            Bucket=bucket_name,
            Key="foo123bar",
            Metadata=metadata,
            ContentType=content_type,
            Body=bytearray(size),
        )

        copy_source = {"Bucket": bucket_name, "Key": "foo123bar"}
        client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key="bar321foo")

        response = client.get_object(Bucket=bucket_name, Key="bar321foo")
        assert content_type == response["ContentType"]
        assert metadata == response["Metadata"]
        body = _get_body(response)
        assert size == response["ContentLength"]


@pytest.mark.fails_on_dbstore
def test_object_copy_replacing_metadata():
    for size in [3, 1024 * 1024]:
        bucket_name = get_new_bucket()
        client = get_client()
        content_type = "audio/ogg"

        metadata = {"key1": "value1", "key2": "value2"}
        client.put_object(
            Bucket=bucket_name,
            Key="foo123bar",
            Metadata=metadata,
            ContentType=content_type,
            Body=bytearray(size),
        )

        metadata = {"key3": "value3", "key2": "value2"}
        content_type = "audio/mpeg"

        copy_source = {"Bucket": bucket_name, "Key": "foo123bar"}
        client.copy_object(
            Bucket=bucket_name,
            CopySource=copy_source,
            Key="bar321foo",
            Metadata=metadata,
            MetadataDirective="REPLACE",
            ContentType=content_type,
        )

        response = client.get_object(Bucket=bucket_name, Key="bar321foo")
        assert content_type == response["ContentType"]
        assert metadata == response["Metadata"]
        assert size == response["ContentLength"]


def test_object_copy_bucket_not_found():
    bucket_name = get_new_bucket()
    client = get_client()

    copy_source = {"Bucket": bucket_name + "-fake", "Key": "foo123bar"}
    e = assert_raises(ClientError, client.copy, copy_source, bucket_name, "bar321foo")
    status = _get_status(e.response)
    assert status == 404


def test_object_copy_key_not_found():
    bucket_name = get_new_bucket()
    client = get_client()

    copy_source = {"Bucket": bucket_name, "Key": "foo123bar"}
    e = assert_raises(ClientError, client.copy, copy_source, bucket_name, "bar321foo")
    status = _get_status(e.response)
    assert status == 404


@pytest.mark.fails_on_dbstore
def test_object_copy_versioned_bucket():
    bucket_name = get_new_bucket()
    client = get_client()
    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")
    size = 1 * 5
    data = bytearray(size)
    data_str = data.decode()
    key1 = "foo123bar"
    client.put_object(Bucket=bucket_name, Key=key1, Body=data)

    response = client.get_object(Bucket=bucket_name, Key=key1)
    version_id = response["VersionId"]

    # copy object in the same bucket
    copy_source = {"Bucket": bucket_name, "Key": key1, "VersionId": version_id}
    key2 = "bar321foo"
    client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key=key2)
    response = client.get_object(Bucket=bucket_name, Key=key2)
    body = _get_body(response)
    assert data_str == body
    assert size == response["ContentLength"]

    # second copy
    version_id2 = response["VersionId"]
    copy_source = {"Bucket": bucket_name, "Key": key2, "VersionId": version_id2}
    key3 = "bar321foo2"
    client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key=key3)
    response = client.get_object(Bucket=bucket_name, Key=key3)
    body = _get_body(response)
    assert data_str == body
    assert size == response["ContentLength"]

    # copy to another versioned bucket
    bucket_name2 = get_new_bucket()
    check_configure_versioning_retry(bucket_name2, "Enabled", "Enabled")
    copy_source = {"Bucket": bucket_name, "Key": key1, "VersionId": version_id}
    key4 = "bar321foo3"
    client.copy_object(Bucket=bucket_name2, CopySource=copy_source, Key=key4)
    response = client.get_object(Bucket=bucket_name2, Key=key4)
    body = _get_body(response)
    assert data_str == body
    assert size == response["ContentLength"]

    # copy to another non versioned bucket
    bucket_name3 = get_new_bucket()
    copy_source = {"Bucket": bucket_name, "Key": key1, "VersionId": version_id}
    key5 = "bar321foo4"
    client.copy_object(Bucket=bucket_name3, CopySource=copy_source, Key=key5)
    response = client.get_object(Bucket=bucket_name3, Key=key5)
    body = _get_body(response)
    assert data_str == body
    assert size == response["ContentLength"]

    # copy from a non versioned bucket
    copy_source = {"Bucket": bucket_name3, "Key": key5}
    key6 = "foo123bar2"
    client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key=key6)
    response = client.get_object(Bucket=bucket_name, Key=key6)
    body = _get_body(response)
    assert data_str == body
    assert size == response["ContentLength"]


@pytest.mark.fails_on_dbstore
def test_object_copy_versioned_url_encoding():
    bucket = get_new_bucket_resource()
    check_configure_versioning_retry(bucket.name, "Enabled", "Enabled")
    src_key = "foo?bar"
    src = bucket.put_object(Key=src_key)
    src.load()  # HEAD request tests that the key exists

    # copy object in the same bucket
    dst_key = "bar&foo"
    dst = bucket.Object(dst_key)
    dst.copy_from(
        CopySource={
            "Bucket": src.bucket_name,
            "Key": src.key,
            "VersionId": src.version_id,
        }
    )
    dst.load()  # HEAD request tests that the key exists


def generate_random(size, part_size=5 * 1024 * 1024):
    """
    Generate the specified number random data.
    (actually each MB is a repetition of the first KB)
    """
    chunk = 1024
    allowed = string.ascii_letters
    for x in range(0, size, part_size):
        strpart = "".join(
            [allowed[random.randint(0, len(allowed) - 1)] for _ in range(chunk)]
        )
        s = ""
        left = size - x
        this_part_size = min(left, part_size)
        for y in range(this_part_size // chunk):
            s = s + strpart
        if this_part_size > len(s):
            s = s + strpart[0 : this_part_size - len(s)]
        yield s
        if x == size:
            return


def _multipart_upload(
    bucket_name,
    key,
    size,
    part_size=5 * 1024 * 1024,
    client=None,
    content_type=None,
    metadata=None,
    resend_parts=[],
):
    """
    generate a multi-part upload for a random file of specifed size,
    if requested, generate a list of the parts
    return the upload descriptor
    """
    if client == None:
        client = get_client()

    if content_type == None and metadata == None:
        response = client.create_multipart_upload(Bucket=bucket_name, Key=key)
    else:
        response = client.create_multipart_upload(
            Bucket=bucket_name, Key=key, Metadata=metadata, ContentType=content_type
        )

    upload_id = response["UploadId"]
    s = ""
    parts = []
    for i, part in enumerate(generate_random(size, part_size)):
        # part_num is necessary because PartNumber for upload_part and in parts must start at 1 and i starts at 0
        part_num = i + 1
        s += part
        response = client.upload_part(
            UploadId=upload_id,
            Bucket=bucket_name,
            Key=key,
            PartNumber=part_num,
            Body=part,
        )
        parts.append({"ETag": response["ETag"].strip('"'), "PartNumber": part_num})
        if i in resend_parts:
            client.upload_part(
                UploadId=upload_id,
                Bucket=bucket_name,
                Key=key,
                PartNumber=part_num,
                Body=part,
            )

    return (upload_id, s, parts)


@pytest.mark.fails_on_dbstore
def test_object_copy_versioning_multipart_upload():
    bucket_name = get_new_bucket()
    client = get_client()
    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")

    key1 = "srcmultipart"
    key1_metadata = {"foo": "bar"}
    content_type = "text/bla"
    objlen = 30 * 1024 * 1024
    (upload_id, data, parts) = _multipart_upload(
        bucket_name=bucket_name,
        key=key1,
        size=objlen,
        content_type=content_type,
        metadata=key1_metadata,
    )
    client.complete_multipart_upload(
        Bucket=bucket_name,
        Key=key1,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )

    response = client.get_object(Bucket=bucket_name, Key=key1)
    key1_size = response["ContentLength"]
    version_id = response["VersionId"]

    # copy object in the same bucket
    copy_source = {"Bucket": bucket_name, "Key": key1, "VersionId": version_id}
    key2 = "dstmultipart"
    client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key=key2)
    response = client.get_object(Bucket=bucket_name, Key=key2)
    version_id2 = response["VersionId"]
    body = _get_body(response)
    assert data == body
    assert key1_size == response["ContentLength"]
    assert key1_metadata == response["Metadata"]
    assert content_type == response["ContentType"]

    # second copy
    copy_source = {"Bucket": bucket_name, "Key": key2, "VersionId": version_id2}
    key3 = "dstmultipart2"
    client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key=key3)
    response = client.get_object(Bucket=bucket_name, Key=key3)
    body = _get_body(response)
    assert data == body
    assert key1_size == response["ContentLength"]
    assert key1_metadata == response["Metadata"]
    assert content_type == response["ContentType"]

    # copy to another versioned bucket
    bucket_name2 = get_new_bucket()
    check_configure_versioning_retry(bucket_name2, "Enabled", "Enabled")

    copy_source = {"Bucket": bucket_name, "Key": key1, "VersionId": version_id}
    key4 = "dstmultipart3"
    client.copy_object(Bucket=bucket_name2, CopySource=copy_source, Key=key4)
    response = client.get_object(Bucket=bucket_name2, Key=key4)
    body = _get_body(response)
    assert data == body
    assert key1_size == response["ContentLength"]
    assert key1_metadata == response["Metadata"]
    assert content_type == response["ContentType"]

    # copy to another non versioned bucket
    bucket_name3 = get_new_bucket()
    copy_source = {"Bucket": bucket_name, "Key": key1, "VersionId": version_id}
    key5 = "dstmultipart4"
    client.copy_object(Bucket=bucket_name3, CopySource=copy_source, Key=key5)
    response = client.get_object(Bucket=bucket_name3, Key=key5)
    body = _get_body(response)
    assert data == body
    assert key1_size == response["ContentLength"]
    assert key1_metadata == response["Metadata"]
    assert content_type == response["ContentType"]

    # copy from a non versioned bucket
    copy_source = {"Bucket": bucket_name3, "Key": key5}
    key6 = "dstmultipart5"
    client.copy_object(Bucket=bucket_name3, CopySource=copy_source, Key=key6)
    response = client.get_object(Bucket=bucket_name3, Key=key6)
    body = _get_body(response)
    assert data == body
    assert key1_size == response["ContentLength"]
    assert key1_metadata == response["Metadata"]
    assert content_type == response["ContentType"]


def test_multipart_upload_empty():
    bucket_name = get_new_bucket()
    client = get_client()

    key1 = "mymultipart"
    objlen = 0
    (upload_id, data, parts) = _multipart_upload(
        bucket_name=bucket_name, key=key1, size=objlen
    )
    e = assert_raises(
        ClientError,
        client.complete_multipart_upload,
        Bucket=bucket_name,
        Key=key1,
        UploadId=upload_id,
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == "MalformedXML"


@pytest.mark.fails_on_dbstore
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
    # check extra client.complete_multipart_upload
    response = client.complete_multipart_upload(
        Bucket=bucket_name,
        Key=key1,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )


def _create_key_with_random_content(
    keyname, size=7 * 1024 * 1024, bucket_name=None, client=None
):
    if bucket_name is None:
        bucket_name = get_new_bucket()

    if client == None:
        client = get_client()

    data_str = str(next(generate_random(size, size)))
    data = bytes(data_str, "utf-8")
    client.put_object(Bucket=bucket_name, Key=keyname, Body=data)

    return bucket_name


def _multipart_copy(
    src_bucket_name,
    src_key,
    dest_bucket_name,
    dest_key,
    size,
    client=None,
    part_size=5 * 1024 * 1024,
    version_id=None,
):
    if client == None:
        client = get_client()

    response = client.create_multipart_upload(Bucket=dest_bucket_name, Key=dest_key)
    upload_id = response["UploadId"]

    if version_id == None:
        copy_source = {"Bucket": src_bucket_name, "Key": src_key}
    else:
        copy_source = {
            "Bucket": src_bucket_name,
            "Key": src_key,
            "VersionId": version_id,
        }

    parts = []

    i = 0
    for start_offset in range(0, size, part_size):
        end_offset = min(start_offset + part_size - 1, size - 1)
        part_num = i + 1
        copy_source_range = "bytes={start}-{end}".format(
            start=start_offset, end=end_offset
        )
        response = client.upload_part_copy(
            Bucket=dest_bucket_name,
            Key=dest_key,
            CopySource=copy_source,
            PartNumber=part_num,
            UploadId=upload_id,
            CopySourceRange=copy_source_range,
        )
        parts.append(
            {"ETag": response["CopyPartResult"]["ETag"], "PartNumber": part_num}
        )
        i = i + 1

    return (upload_id, parts)


def _check_key_content(
    src_key, src_bucket_name, dest_key, dest_bucket_name, version_id=None
):
    client = get_client()

    if version_id == None:
        response = client.get_object(Bucket=src_bucket_name, Key=src_key)
    else:
        response = client.get_object(
            Bucket=src_bucket_name, Key=src_key, VersionId=version_id
        )
    src_size = response["ContentLength"]

    response = client.get_object(Bucket=dest_bucket_name, Key=dest_key)
    dest_size = response["ContentLength"]
    dest_data = _get_body(response)
    assert src_size >= dest_size

    r = "bytes={s}-{e}".format(s=0, e=dest_size - 1)
    if version_id == None:
        response = client.get_object(Bucket=src_bucket_name, Key=src_key, Range=r)
    else:
        response = client.get_object(
            Bucket=src_bucket_name, Key=src_key, Range=r, VersionId=version_id
        )
    src_data = _get_body(response)
    assert src_data == dest_data


@pytest.mark.fails_on_dbstore
def test_multipart_copy_small():
    src_key = "foo"
    src_bucket_name = _create_key_with_random_content(src_key)

    dest_bucket_name = get_new_bucket()
    dest_key = "mymultipart"
    size = 1
    client = get_client()

    (upload_id, parts) = _multipart_copy(
        src_bucket_name, src_key, dest_bucket_name, dest_key, size
    )
    client.complete_multipart_upload(
        Bucket=dest_bucket_name,
        Key=dest_key,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )

    response = client.get_object(Bucket=dest_bucket_name, Key=dest_key)
    assert size == response["ContentLength"]
    _check_key_content(src_key, src_bucket_name, dest_key, dest_bucket_name)


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


# TODO: remove fails_on_rgw when https://tracker.ceph.com/issues/40795 is resolved
@pytest.mark.fails_on_rgw
def test_multipart_copy_improper_range():
    client = get_client()
    src_key = "source"
    src_bucket_name = _create_key_with_random_content(src_key, size=5)

    response = client.create_multipart_upload(Bucket=src_bucket_name, Key="dest")
    upload_id = response["UploadId"]

    copy_source = {"Bucket": src_bucket_name, "Key": src_key}
    test_ranges = [
        "{start}-{end}".format(start=0, end=2),
        "bytes={start}".format(start=0),
        "bytes=hello-world",
        "bytes=0-bar",
        "bytes=hello-",
        "bytes=0-2,3-5",
    ]

    for test_range in test_ranges:
        e = assert_raises(
            ClientError,
            client.upload_part_copy,
            Bucket=src_bucket_name,
            Key="dest",
            UploadId=upload_id,
            CopySource=copy_source,
            CopySourceRange=test_range,
            PartNumber=1,
        )
        status, error_code = _get_status_and_error_code(e.response)
        assert status == 400
        assert error_code == "InvalidArgument"


def test_multipart_copy_without_range():
    client = get_client()
    src_key = "source"
    src_bucket_name = _create_key_with_random_content(src_key, size=10)
    dest_bucket_name = get_new_bucket_name()
    get_new_bucket(name=dest_bucket_name)
    dest_key = "mymultipartcopy"

    response = client.create_multipart_upload(Bucket=dest_bucket_name, Key=dest_key)
    upload_id = response["UploadId"]
    parts = []

    copy_source = {"Bucket": src_bucket_name, "Key": src_key}
    part_num = 1
    copy_source_range = "bytes={start}-{end}".format(start=0, end=9)

    response = client.upload_part_copy(
        Bucket=dest_bucket_name,
        Key=dest_key,
        CopySource=copy_source,
        PartNumber=part_num,
        UploadId=upload_id,
    )

    parts.append({"ETag": response["CopyPartResult"]["ETag"], "PartNumber": part_num})
    client.complete_multipart_upload(
        Bucket=dest_bucket_name,
        Key=dest_key,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )

    response = client.get_object(Bucket=dest_bucket_name, Key=dest_key)
    assert response["ContentLength"] == 10
    _check_key_content(src_key, src_bucket_name, dest_key, dest_bucket_name)


@pytest.mark.fails_on_dbstore
def test_multipart_copy_special_names():
    src_bucket_name = get_new_bucket()

    dest_bucket_name = get_new_bucket()

    dest_key = "mymultipart"
    size = 1
    client = get_client()

    for src_key in (" ", "_", "__", "?versionId"):
        _create_key_with_random_content(src_key, bucket_name=src_bucket_name)
        (upload_id, parts) = _multipart_copy(
            src_bucket_name, src_key, dest_bucket_name, dest_key, size
        )
        response = client.complete_multipart_upload(
            Bucket=dest_bucket_name,
            Key=dest_key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
        response = client.get_object(Bucket=dest_bucket_name, Key=dest_key)
        assert size == response["ContentLength"]
        _check_key_content(src_key, src_bucket_name, dest_key, dest_bucket_name)


def _check_content_using_range(key, bucket_name, data, step):
    client = get_client()
    response = client.get_object(Bucket=bucket_name, Key=key)
    size = response["ContentLength"]

    for ofs in range(0, size, step):
        toread = size - ofs
        if toread > step:
            toread = step
        end = ofs + toread - 1
        r = "bytes={s}-{e}".format(s=ofs, e=end)
        response = client.get_object(Bucket=bucket_name, Key=key, Range=r)
        assert response["ContentLength"] == toread
        body = _get_body(response)
        assert body == data[ofs : end + 1]


@pytest.mark.fails_on_dbstore
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
    # check extra client.complete_multipart_upload
    client.complete_multipart_upload(
        Bucket=bucket_name,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )

    response = client.list_objects_v2(Bucket=bucket_name, Prefix=key)
    assert len(response["Contents"]) == 1
    assert response["Contents"][0]["Size"] == objlen

    response = client.get_object(Bucket=bucket_name, Key=key)
    assert response["ContentType"] == content_type
    assert response["Metadata"] == metadata
    body = _get_body(response)
    assert len(body) == response["ContentLength"]
    assert body == data

    _check_content_using_range(key, bucket_name, data, 1000000)
    _check_content_using_range(key, bucket_name, data, 10000000)


def check_versioning(bucket_name, status):
    client = get_client()

    try:
        response = client.get_bucket_versioning(Bucket=bucket_name)
        assert response["Status"] == status
    except KeyError:
        assert status == None


# amazon is eventual consistent, retry a bit if failed
def check_configure_versioning_retry(bucket_name, status, expected_string):
    client = get_client()
    client.put_bucket_versioning(
        Bucket=bucket_name, VersioningConfiguration={"Status": status}
    )

    read_status = None

    for i in range(5):
        try:
            response = client.get_bucket_versioning(Bucket=bucket_name)
            read_status = response["Status"]
        except KeyError:
            read_status = None

        if expected_string == read_status:
            break

        time.sleep(1)

    assert expected_string == read_status


@pytest.mark.fails_on_dbstore
def test_multipart_copy_versioned():
    src_bucket_name = get_new_bucket()
    dest_bucket_name = get_new_bucket()

    dest_key = "mymultipart"
    check_versioning(src_bucket_name, None)

    src_key = "foo"
    check_configure_versioning_retry(src_bucket_name, "Enabled", "Enabled")

    size = 15 * 1024 * 1024
    _create_key_with_random_content(src_key, size=size, bucket_name=src_bucket_name)
    _create_key_with_random_content(src_key, size=size, bucket_name=src_bucket_name)
    _create_key_with_random_content(src_key, size=size, bucket_name=src_bucket_name)

    version_id = []
    client = get_client()
    response = client.list_object_versions(Bucket=src_bucket_name)
    for ver in response["Versions"]:
        version_id.append(ver["VersionId"])

    for vid in version_id:
        (upload_id, parts) = _multipart_copy(
            src_bucket_name, src_key, dest_bucket_name, dest_key, size, version_id=vid
        )
        response = client.complete_multipart_upload(
            Bucket=dest_bucket_name,
            Key=dest_key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
        response = client.get_object(Bucket=dest_bucket_name, Key=dest_key)
        assert size == response["ContentLength"]
        _check_key_content(
            src_key, src_bucket_name, dest_key, dest_bucket_name, version_id=vid
        )


def _check_upload_multipart_resend(bucket_name, key, objlen, resend_parts):
    content_type = "text/bla"
    metadata = {"foo": "bar"}
    client = get_client()
    (upload_id, data, parts) = _multipart_upload(
        bucket_name=bucket_name,
        key=key,
        size=objlen,
        content_type=content_type,
        metadata=metadata,
        resend_parts=resend_parts,
    )
    client.complete_multipart_upload(
        Bucket=bucket_name,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )

    response = client.get_object(Bucket=bucket_name, Key=key)
    assert response["ContentType"] == content_type
    assert response["Metadata"] == metadata
    body = _get_body(response)
    assert len(body) == response["ContentLength"]
    assert body == data

    _check_content_using_range(key, bucket_name, data, 1000000)
    _check_content_using_range(key, bucket_name, data, 10000000)


@pytest.mark.fails_on_dbstore
def test_multipart_upload_resend_part():
    bucket_name = get_new_bucket()
    key = "mymultipart"
    objlen = 30 * 1024 * 1024

    _check_upload_multipart_resend(bucket_name, key, objlen, [0])
    _check_upload_multipart_resend(bucket_name, key, objlen, [1])
    _check_upload_multipart_resend(bucket_name, key, objlen, [2])
    _check_upload_multipart_resend(bucket_name, key, objlen, [1, 2])
    _check_upload_multipart_resend(bucket_name, key, objlen, [0, 1, 2, 3, 4, 5])


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/843")
def test_multipart_upload_multiple_sizes():
    bucket_name = get_new_bucket()
    key = "mymultipart"
    client = get_client()

    objlen = 5 * 1024 * 1024
    (upload_id, data, parts) = _multipart_upload(
        bucket_name=bucket_name, key=key, size=objlen
    )
    client.complete_multipart_upload(
        Bucket=bucket_name,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )

    objlen = 5 * 1024 * 1024 + 100 * 1024
    (upload_id, data, parts) = _multipart_upload(
        bucket_name=bucket_name, key=key, size=objlen
    )
    client.complete_multipart_upload(
        Bucket=bucket_name,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )

    objlen = 5 * 1024 * 1024 + 600 * 1024
    (upload_id, data, parts) = _multipart_upload(
        bucket_name=bucket_name, key=key, size=objlen
    )
    client.complete_multipart_upload(
        Bucket=bucket_name,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )

    objlen = 10 * 1024 * 1024 + 100 * 1024
    (upload_id, data, parts) = _multipart_upload(
        bucket_name=bucket_name, key=key, size=objlen
    )
    client.complete_multipart_upload(
        Bucket=bucket_name,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )

    objlen = 10 * 1024 * 1024 + 600 * 1024
    (upload_id, data, parts) = _multipart_upload(
        bucket_name=bucket_name, key=key, size=objlen
    )
    client.complete_multipart_upload(
        Bucket=bucket_name,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )

    objlen = 10 * 1024 * 1024
    (upload_id, data, parts) = _multipart_upload(
        bucket_name=bucket_name, key=key, size=objlen
    )
    client.complete_multipart_upload(
        Bucket=bucket_name,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )


@pytest.mark.fails_on_dbstore
def test_multipart_copy_multiple_sizes():
    src_key = "foo"
    src_bucket_name = _create_key_with_random_content(src_key, 12 * 1024 * 1024)

    dest_bucket_name = get_new_bucket()
    dest_key = "mymultipart"
    client = get_client()

    size = 5 * 1024 * 1024
    (upload_id, parts) = _multipart_copy(
        src_bucket_name, src_key, dest_bucket_name, dest_key, size
    )
    client.complete_multipart_upload(
        Bucket=dest_bucket_name,
        Key=dest_key,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )
    _check_key_content(src_key, src_bucket_name, dest_key, dest_bucket_name)

    size = 5 * 1024 * 1024 + 100 * 1024
    (upload_id, parts) = _multipart_copy(
        src_bucket_name, src_key, dest_bucket_name, dest_key, size
    )
    client.complete_multipart_upload(
        Bucket=dest_bucket_name,
        Key=dest_key,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )
    _check_key_content(src_key, src_bucket_name, dest_key, dest_bucket_name)

    size = 5 * 1024 * 1024 + 600 * 1024
    (upload_id, parts) = _multipart_copy(
        src_bucket_name, src_key, dest_bucket_name, dest_key, size
    )
    client.complete_multipart_upload(
        Bucket=dest_bucket_name,
        Key=dest_key,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )
    _check_key_content(src_key, src_bucket_name, dest_key, dest_bucket_name)

    size = 10 * 1024 * 1024 + 100 * 1024
    (upload_id, parts) = _multipart_copy(
        src_bucket_name, src_key, dest_bucket_name, dest_key, size
    )
    client.complete_multipart_upload(
        Bucket=dest_bucket_name,
        Key=dest_key,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )
    _check_key_content(src_key, src_bucket_name, dest_key, dest_bucket_name)

    size = 10 * 1024 * 1024 + 600 * 1024
    (upload_id, parts) = _multipart_copy(
        src_bucket_name, src_key, dest_bucket_name, dest_key, size
    )
    client.complete_multipart_upload(
        Bucket=dest_bucket_name,
        Key=dest_key,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )
    _check_key_content(src_key, src_bucket_name, dest_key, dest_bucket_name)

    size = 10 * 1024 * 1024
    (upload_id, parts) = _multipart_copy(
        src_bucket_name, src_key, dest_bucket_name, dest_key, size
    )
    client.complete_multipart_upload(
        Bucket=dest_bucket_name,
        Key=dest_key,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )
    _check_key_content(src_key, src_bucket_name, dest_key, dest_bucket_name)


def test_multipart_upload_size_too_small():
    bucket_name = get_new_bucket()
    key = "mymultipart"
    client = get_client()

    size = 100 * 1024
    (upload_id, data, parts) = _multipart_upload(
        bucket_name=bucket_name, key=key, size=size, part_size=10 * 1024
    )
    e = assert_raises(
        ClientError,
        client.complete_multipart_upload,
        Bucket=bucket_name,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == "EntityTooSmall"


def gen_rand_string(size, chars=string.ascii_uppercase + string.digits):
    return "".join(random.choice(chars) for _ in range(size))


def _do_test_multipart_upload_contents(bucket_name, key, num_parts):
    payload = gen_rand_string(5) * 1024 * 1024
    client = get_client()

    response = client.create_multipart_upload(Bucket=bucket_name, Key=key)
    upload_id = response["UploadId"]

    parts = []

    for part_num in range(0, num_parts):
        part = bytes(payload, "utf-8")
        response = client.upload_part(
            UploadId=upload_id,
            Bucket=bucket_name,
            Key=key,
            PartNumber=part_num + 1,
            Body=part,
        )
        parts.append({"ETag": response["ETag"].strip('"'), "PartNumber": part_num + 1})

    last_payload = "123" * 1024 * 1024
    last_part = bytes(last_payload, "utf-8")
    response = client.upload_part(
        UploadId=upload_id,
        Bucket=bucket_name,
        Key=key,
        PartNumber=num_parts + 1,
        Body=last_part,
    )
    parts.append({"ETag": response["ETag"].strip('"'), "PartNumber": num_parts + 1})

    client.complete_multipart_upload(
        Bucket=bucket_name,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )

    response = client.get_object(Bucket=bucket_name, Key=key)
    test_string = _get_body(response)

    all_payload = payload * num_parts + last_payload

    assert test_string == all_payload

    return all_payload


@pytest.mark.fails_on_dbstore
def test_multipart_upload_contents():
    bucket_name = get_new_bucket()
    _do_test_multipart_upload_contents(bucket_name, "mymultipart", 3)


def test_multipart_upload_overwrite_existing_object():
    bucket_name = get_new_bucket()
    client = get_client()
    key = "mymultipart"
    payload = "12345" * 1024 * 1024
    num_parts = 2
    client.put_object(Bucket=bucket_name, Key=key, Body=payload)

    response = client.create_multipart_upload(Bucket=bucket_name, Key=key)
    upload_id = response["UploadId"]

    parts = []

    for part_num in range(0, num_parts):
        response = client.upload_part(
            UploadId=upload_id,
            Bucket=bucket_name,
            Key=key,
            PartNumber=part_num + 1,
            Body=payload,
        )
        parts.append({"ETag": response["ETag"].strip('"'), "PartNumber": part_num + 1})

    client.complete_multipart_upload(
        Bucket=bucket_name,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )

    response = client.get_object(Bucket=bucket_name, Key=key)
    test_string = _get_body(response)

    assert test_string == payload * num_parts


def test_abort_multipart_upload():
    bucket_name = get_new_bucket()
    key = "mymultipart"
    objlen = 10 * 1024 * 1024
    client = get_client()

    (upload_id, data, parts) = _multipart_upload(
        bucket_name=bucket_name, key=key, size=objlen
    )
    client.abort_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id)

    response = client.list_objects_v2(Bucket=bucket_name, Prefix=key)
    assert "Contents" not in response


def test_abort_multipart_upload_not_found():
    bucket_name = get_new_bucket()
    client = get_client()
    key = "mymultipart"
    client.put_object(Bucket=bucket_name, Key=key)

    e = assert_raises(
        ClientError,
        client.abort_multipart_upload,
        Bucket=bucket_name,
        Key=key,
        UploadId="56788",
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == "NoSuchUpload"


@pytest.mark.fails_on_dbstore
def test_list_multipart_upload():
    bucket_name = get_new_bucket()
    client = get_client()
    key = "mymultipart"
    mb = 1024 * 1024

    upload_ids = []
    (upload_id1, data, parts) = _multipart_upload(
        bucket_name=bucket_name, key=key, size=5 * mb
    )
    upload_ids.append(upload_id1)
    (upload_id2, data, parts) = _multipart_upload(
        bucket_name=bucket_name, key=key, size=6 * mb
    )
    upload_ids.append(upload_id2)

    key2 = "mymultipart2"
    (upload_id3, data, parts) = _multipart_upload(
        bucket_name=bucket_name, key=key2, size=5 * mb
    )
    upload_ids.append(upload_id3)

    response = client.list_multipart_uploads(Bucket=bucket_name)
    uploads = response["Uploads"]
    resp_uploadids = []

    for i in range(0, len(uploads)):
        resp_uploadids.append(uploads[i]["UploadId"])

    for i in range(0, len(upload_ids)):
        assert True == (upload_ids[i] in resp_uploadids)

    client.abort_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id1)
    client.abort_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id2)
    client.abort_multipart_upload(Bucket=bucket_name, Key=key2, UploadId=upload_id3)


@pytest.mark.fails_on_dbstore
def test_list_multipart_upload_owner():
    bucket_name = get_new_bucket()

    client1 = get_client()
    user1 = get_main_user_id()
    name1 = get_main_display_name()

    client2 = get_alt_client()
    user2 = get_alt_user_id()
    name2 = get_alt_display_name()

    # add bucket acl for public read/write access
    client1.put_bucket_acl(Bucket=bucket_name, ACL="public-read-write")

    key1 = "multipart1"
    key2 = "multipart2"
    upload1 = client1.create_multipart_upload(Bucket=bucket_name, Key=key1)["UploadId"]
    try:
        upload2 = client2.create_multipart_upload(Bucket=bucket_name, Key=key2)[
            "UploadId"
        ]
        try:
            # match fields of an Upload from ListMultipartUploadsResult
            def match(upload, key, uploadid, userid, username):
                assert upload["Key"] == key
                assert upload["UploadId"] == uploadid
                assert upload["Initiator"]["ID"] == userid
                assert upload["Initiator"]["DisplayName"] == username
                assert upload["Owner"]["ID"] == userid
                assert upload["Owner"]["DisplayName"] == username

            # list uploads with client1
            uploads1 = client1.list_multipart_uploads(Bucket=bucket_name)["Uploads"]
            assert len(uploads1) == 2
            match(uploads1[0], key1, upload1, user1, name1)
            match(uploads1[1], key2, upload2, user2, name2)

            # list uploads with client2
            uploads2 = client2.list_multipart_uploads(Bucket=bucket_name)["Uploads"]
            assert len(uploads2) == 2
            match(uploads2[0], key1, upload1, user1, name1)
            match(uploads2[1], key2, upload2, user2, name2)
        finally:
            client2.abort_multipart_upload(
                Bucket=bucket_name, Key=key2, UploadId=upload2
            )
    finally:
        client1.abort_multipart_upload(Bucket=bucket_name, Key=key1, UploadId=upload1)


def test_multipart_upload_missing_part():
    bucket_name = get_new_bucket()
    client = get_client()
    key = "mymultipart"
    size = 1

    response = client.create_multipart_upload(Bucket=bucket_name, Key=key)
    upload_id = response["UploadId"]

    parts = []
    response = client.upload_part(
        UploadId=upload_id,
        Bucket=bucket_name,
        Key=key,
        PartNumber=1,
        Body=bytes("\x00", "utf-8"),
    )
    # 'PartNumber should be 1'
    parts.append({"ETag": response["ETag"].strip('"'), "PartNumber": 9999})

    e = assert_raises(
        ClientError,
        client.complete_multipart_upload,
        Bucket=bucket_name,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == "InvalidPart"


def test_multipart_upload_incorrect_etag():
    bucket_name = get_new_bucket()
    client = get_client()
    key = "mymultipart"
    size = 1

    response = client.create_multipart_upload(Bucket=bucket_name, Key=key)
    upload_id = response["UploadId"]

    parts = []
    response = client.upload_part(
        UploadId=upload_id,
        Bucket=bucket_name,
        Key=key,
        PartNumber=1,
        Body=bytes("\x00", "utf-8"),
    )
    # 'ETag' should be "93b885adfe0da089cdf634904fd59f71"
    parts.append({"ETag": "ffffffffffffffffffffffffffffffff", "PartNumber": 1})

    e = assert_raises(
        ClientError,
        client.complete_multipart_upload,
        Bucket=bucket_name,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == "InvalidPart"


def _simple_http_req_100_cont(host, port, is_secure, method, resource):
    """
    Send the specified request w/expect 100-continue
    and await confirmation.
    """
    req_str = "{method} {resource} HTTP/1.1\r\nHost: {host}\r\nAccept-Encoding: identity\r\nContent-Length: 123\r\nExpect: 100-continue\r\n\r\n".format(
        method=method,
        resource=resource,
        host=host,
    )

    req = bytes(req_str, "utf-8")

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if is_secure:
        s = ssl.wrap_socket(s)
    s.settimeout(5)
    s.connect((host, port))
    s.send(req)

    try:
        data = s.recv(1024)
    except socket.error as msg:
        print("got response: ", msg)
        print("most likely server doesn't support 100-continue")

    s.close()
    data_str = data.decode()
    l = data_str.split(" ")

    assert l[0].startswith("HTTP")

    return l[1]


def test_100_continue():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name)
    objname = "testobj"
    resource = "/{bucket}/{obj}".format(bucket=bucket_name, obj=objname)

    host = get_config_host()
    port = get_config_port()
    is_secure = get_config_is_secure()

    # NOTES: this test needs to be tested when is_secure is True
    status = _simple_http_req_100_cont(host, port, is_secure, "PUT", resource)
    assert status == "403"

    client.put_bucket_acl(Bucket=bucket_name, ACL="public-read-write")

    status = _simple_http_req_100_cont(host, port, is_secure, "PUT", resource)
    assert status == "100"


def test_set_cors():
    bucket_name = get_new_bucket()
    client = get_client()
    allowed_methods = ["GET", "PUT"]
    allowed_origins = ["*.get", "*.put"]

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


def _cors_request_and_check(
    func, url, headers, expect_status, expect_allow_origin, expect_allow_methods
):
    r = func(url, headers=headers, verify=get_config_ssl_verify())
    assert r.status_code == expect_status

    assert r.headers.get("access-control-allow-origin", None) == expect_allow_origin
    assert r.headers.get("access-control-allow-methods", None) == expect_allow_methods


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/841")
def test_cors_origin_response():
    bucket_name = _setup_bucket_acl(bucket_acl="public-read")
    client = get_client()

    cors_config = {
        "CORSRules": [
            {
                "AllowedMethods": ["GET"],
                "AllowedOrigins": ["*suffix"],
            },
            {
                "AllowedMethods": ["GET"],
                "AllowedOrigins": ["start*end"],
            },
            {
                "AllowedMethods": ["GET"],
                "AllowedOrigins": ["prefix*"],
            },
            {
                "AllowedMethods": ["PUT"],
                "AllowedOrigins": ["*.put"],
            },
        ]
    }

    e = assert_raises(ClientError, client.get_bucket_cors, Bucket=bucket_name)
    status = _get_status(e.response)
    assert status == 404

    client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration=cors_config)

    time.sleep(3)

    url = _get_post_url(bucket_name)

    _cors_request_and_check(requests.get, url, None, 200, None, None)
    _cors_request_and_check(
        requests.get, url, {"Origin": "foo.suffix"}, 200, "foo.suffix", "GET"
    )
    _cors_request_and_check(requests.get, url, {"Origin": "foo.bar"}, 200, None, None)
    _cors_request_and_check(
        requests.get, url, {"Origin": "foo.suffix.get"}, 200, None, None
    )
    _cors_request_and_check(
        requests.get, url, {"Origin": "startend"}, 200, "startend", "GET"
    )
    _cors_request_and_check(
        requests.get, url, {"Origin": "start1end"}, 200, "start1end", "GET"
    )
    _cors_request_and_check(
        requests.get, url, {"Origin": "start12end"}, 200, "start12end", "GET"
    )
    _cors_request_and_check(
        requests.get, url, {"Origin": "0start12end"}, 200, None, None
    )
    _cors_request_and_check(
        requests.get, url, {"Origin": "prefix"}, 200, "prefix", "GET"
    )
    _cors_request_and_check(
        requests.get, url, {"Origin": "prefix.suffix"}, 200, "prefix.suffix", "GET"
    )
    _cors_request_and_check(
        requests.get, url, {"Origin": "bla.prefix"}, 200, None, None
    )

    obj_url = "{u}/{o}".format(u=url, o="bar")
    _cors_request_and_check(
        requests.get, obj_url, {"Origin": "foo.suffix"}, 404, "foo.suffix", "GET"
    )
    _cors_request_and_check(
        requests.put,
        obj_url,
        {
            "Origin": "foo.suffix",
            "Access-Control-Request-Method": "GET",
            "content-length": "0",
        },
        403,
        "foo.suffix",
        "GET",
    )
    _cors_request_and_check(
        requests.put,
        obj_url,
        {
            "Origin": "foo.suffix",
            "Access-Control-Request-Method": "PUT",
            "content-length": "0",
        },
        403,
        None,
        None,
    )

    _cors_request_and_check(
        requests.put,
        obj_url,
        {
            "Origin": "foo.suffix",
            "Access-Control-Request-Method": "DELETE",
            "content-length": "0",
        },
        403,
        None,
        None,
    )
    _cors_request_and_check(
        requests.put,
        obj_url,
        {"Origin": "foo.suffix", "content-length": "0"},
        403,
        None,
        None,
    )

    _cors_request_and_check(
        requests.put,
        obj_url,
        {"Origin": "foo.put", "content-length": "0"},
        403,
        "foo.put",
        "PUT",
    )

    _cors_request_and_check(
        requests.get, obj_url, {"Origin": "foo.suffix"}, 404, "foo.suffix", "GET"
    )

    _cors_request_and_check(requests.options, url, None, 400, None, None)
    _cors_request_and_check(
        requests.options, url, {"Origin": "foo.suffix"}, 400, None, None
    )
    _cors_request_and_check(requests.options, url, {"Origin": "bla"}, 400, None, None)
    _cors_request_and_check(
        requests.options,
        obj_url,
        {
            "Origin": "foo.suffix",
            "Access-Control-Request-Method": "GET",
            "content-length": "0",
        },
        200,
        "foo.suffix",
        "GET",
    )
    _cors_request_and_check(
        requests.options,
        url,
        {"Origin": "foo.bar", "Access-Control-Request-Method": "GET"},
        403,
        None,
        None,
    )
    _cors_request_and_check(
        requests.options,
        url,
        {"Origin": "foo.suffix.get", "Access-Control-Request-Method": "GET"},
        403,
        None,
        None,
    )
    _cors_request_and_check(
        requests.options,
        url,
        {"Origin": "startend", "Access-Control-Request-Method": "GET"},
        200,
        "startend",
        "GET",
    )
    _cors_request_and_check(
        requests.options,
        url,
        {"Origin": "start1end", "Access-Control-Request-Method": "GET"},
        200,
        "start1end",
        "GET",
    )
    _cors_request_and_check(
        requests.options,
        url,
        {"Origin": "start12end", "Access-Control-Request-Method": "GET"},
        200,
        "start12end",
        "GET",
    )
    _cors_request_and_check(
        requests.options,
        url,
        {"Origin": "0start12end", "Access-Control-Request-Method": "GET"},
        403,
        None,
        None,
    )
    _cors_request_and_check(
        requests.options,
        url,
        {"Origin": "prefix", "Access-Control-Request-Method": "GET"},
        200,
        "prefix",
        "GET",
    )
    _cors_request_and_check(
        requests.options,
        url,
        {"Origin": "prefix.suffix", "Access-Control-Request-Method": "GET"},
        200,
        "prefix.suffix",
        "GET",
    )
    _cors_request_and_check(
        requests.options,
        url,
        {"Origin": "bla.prefix", "Access-Control-Request-Method": "GET"},
        403,
        None,
        None,
    )
    _cors_request_and_check(
        requests.options,
        url,
        {"Origin": "foo.put", "Access-Control-Request-Method": "GET"},
        403,
        None,
        None,
    )
    _cors_request_and_check(
        requests.options,
        url,
        {"Origin": "foo.put", "Access-Control-Request-Method": "PUT"},
        200,
        "foo.put",
        "PUT",
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

    _cors_request_and_check(requests.get, url, None, 200, None, None)
    _cors_request_and_check(
        requests.get, url, {"Origin": "example.origin"}, 200, "*", "GET"
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
                "ExposeHeaders": ["x-amz-meta-header1"],
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

    _cors_request_and_check(
        requests.options,
        obj_url,
        {
            "Origin": "example.origin",
            "Access-Control-Request-Headers": "x-amz-meta-header2",
            "Access-Control-Request-Method": "GET",
        },
        403,
        None,
        None,
    )


def _test_cors_options_presigned_get_object(client):
    bucket_name = _setup_bucket_object_acl("public-read", "public-read", client=client)
    params = {"Bucket": bucket_name, "Key": "foo"}

    url = client.generate_presigned_url(
        ClientMethod="get_object", Params=params, ExpiresIn=100000, HttpMethod="GET"
    )

    res = requests.options(url, verify=get_config_ssl_verify()).__dict__
    assert res["status_code"] == 400

    allowed_methods = ["GET"]
    allowed_origins = ["example"]

    cors_config = {
        "CORSRules": [
            {
                "AllowedMethods": allowed_methods,
                "AllowedOrigins": allowed_origins,
            },
        ]
    }

    client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration=cors_config)
    _cors_request_and_check(
        requests.options,
        url,
        {"Origin": "example", "Access-Control-Request-Method": "GET"},
        200,
        "example",
        "GET",
    )


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/899")
def test_cors_presigned_get_object():
    _test_cors_options_presigned_get_object(client=get_client())


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/899")
def test_cors_presigned_get_object_tenant():
    _test_cors_options_presigned_get_object(client=get_tenant_client())


@pytest.mark.tagging
@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/898")
def test_set_bucket_tagging():
    bucket_name = get_new_bucket()
    client = get_client()

    tags = {
        "TagSet": [
            {"Key": "Hello", "Value": "World"},
        ]
    }

    e = assert_raises(ClientError, client.get_bucket_tagging, Bucket=bucket_name)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == "NoSuchTagSet"

    client.put_bucket_tagging(Bucket=bucket_name, Tagging=tags)

    response = client.get_bucket_tagging(Bucket=bucket_name)
    assert len(response["TagSet"]) == 1
    assert response["TagSet"][0]["Key"] == "Hello"
    assert response["TagSet"][0]["Value"] == "World"

    response = client.delete_bucket_tagging(Bucket=bucket_name)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 204

    e = assert_raises(ClientError, client.get_bucket_tagging, Bucket=bucket_name)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == "NoSuchTagSet"


class FakeFile(object):
    """
    file that simulates seek, tell, and current character
    """

    def __init__(self, char="A", interrupt=None):
        self.offset = 0
        self.char = bytes(char, "utf-8")
        self.interrupt = interrupt

    def seek(self, offset, whence=os.SEEK_SET):
        if whence == os.SEEK_SET:
            self.offset = offset
        elif whence == os.SEEK_END:
            self.offset = self.size + offset
        elif whence == os.SEEK_CUR:
            self.offset += offset

    def tell(self):
        return self.offset


class FakeWriteFile(FakeFile):
    """
    file that simulates interruptable reads of constant data
    """

    def __init__(self, size, char="A", interrupt=None):
        FakeFile.__init__(self, char, interrupt)
        self.size = size

    def read(self, size=-1):
        if size < 0:
            size = self.size - self.offset
        count = min(size, self.size - self.offset)
        self.offset += count

        # Sneaky! do stuff before we return (the last time)
        if self.interrupt != None and self.offset == self.size and count > 0:
            self.interrupt()

        return self.char * count


class FakeReadFile(FakeFile):
    """
    file that simulates writes, interrupting after the second
    """

    def __init__(self, size, char="A", interrupt=None):
        FakeFile.__init__(self, char, interrupt)
        self.interrupted = False
        self.size = 0
        self.expected_size = size

    def write(self, chars):
        assert chars == self.char * len(chars)
        self.offset += len(chars)
        self.size += len(chars)

        # Sneaky! do stuff on the second seek
        if not self.interrupted and self.interrupt != None and self.offset > 0:
            self.interrupt()
            self.interrupted = True

    def close(self):
        assert self.size == self.expected_size


class FakeFileVerifier(object):
    """
    file that verifies expected data has been written
    """

    def __init__(self, char=None):
        self.char = char
        self.size = 0

    def write(self, data):
        size = len(data)
        if self.char == None:
            self.char = data[0]
        self.size += size
        assert data.decode() == self.char * size


def _verify_atomic_key_data(bucket_name, key, size=-1, char=None):
    """
    Make sure file is of the expected size and (simulated) content
    """
    fp_verify = FakeFileVerifier(char)
    client = get_client()
    client.download_fileobj(bucket_name, key, fp_verify)
    if size >= 0:
        assert fp_verify.size == size


def _test_atomic_read(file_size):
    """
    Create a file of A's, use it to set_contents_from_file.
    Create a file of B's, use it to re-set_contents_from_file.
    Re-read the contents, and confirm we get B's
    """
    bucket_name = get_new_bucket()
    client = get_client()

    fp_a = FakeWriteFile(file_size, "A")
    client.put_object(Bucket=bucket_name, Key="testobj", Body=fp_a)

    fp_b = FakeWriteFile(file_size, "B")
    fp_a2 = FakeReadFile(
        file_size,
        "A",
        lambda: client.put_object(Bucket=bucket_name, Key="testobj", Body=fp_b),
    )

    read_client = get_client()

    read_client.download_fileobj(bucket_name, "testobj", fp_a2)
    fp_a2.close()

    _verify_atomic_key_data(bucket_name, "testobj", file_size, "B")


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/885")
def test_atomic_read_1mb():
    _test_atomic_read(1024 * 1024)


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/885")
def test_atomic_read_4mb():
    _test_atomic_read(1024 * 1024 * 4)


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/885")
def test_atomic_read_8mb():
    _test_atomic_read(1024 * 1024 * 8)


def _test_atomic_write(file_size):
    """
    Create a file of A's, use it to set_contents_from_file.
    Verify the contents are all A's.
    Create a file of B's, use it to re-set_contents_from_file.
    Before re-set continues, verify content's still A's
    Re-read the contents, and confirm we get B's
    """
    bucket_name = get_new_bucket()
    client = get_client()
    objname = "testobj"

    # create <file_size> file of A's
    fp_a = FakeWriteFile(file_size, "A")
    client.put_object(Bucket=bucket_name, Key=objname, Body=fp_a)

    # verify A's
    _verify_atomic_key_data(bucket_name, objname, file_size, "A")

    # create <file_size> file of B's
    # but try to verify the file before we finish writing all the B's
    fp_b = FakeWriteFile(
        file_size,
        "B",
        lambda: _verify_atomic_key_data(bucket_name, objname, file_size, "A"),
    )

    client.put_object(Bucket=bucket_name, Key=objname, Body=fp_b)

    # verify B's
    _verify_atomic_key_data(bucket_name, objname, file_size, "B")


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/885")
def test_atomic_write_1mb():
    _test_atomic_write(1024 * 1024)


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/885")
def test_atomic_write_4mb():
    _test_atomic_write(1024 * 1024 * 4)


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/885")
def test_atomic_write_8mb():
    _test_atomic_write(1024 * 1024 * 8)


def _test_atomic_dual_write(file_size):
    """
    create an object, two sessions writing different contents
    confirm that it is all one or the other
    """
    bucket_name = get_new_bucket()
    objname = "testobj"
    client = get_client()
    client.put_object(Bucket=bucket_name, Key=objname)

    # write <file_size> file of B's
    # but before we're done, try to write all A's
    fp_a = FakeWriteFile(file_size, "A")

    def rewind_put_fp_a():
        fp_a.seek(0)
        client.put_object(Bucket=bucket_name, Key=objname, Body=fp_a)

    fp_b = FakeWriteFile(file_size, "B", rewind_put_fp_a)
    client.put_object(Bucket=bucket_name, Key=objname, Body=fp_b)

    # verify the file
    _verify_atomic_key_data(bucket_name, objname, file_size, "B")


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/885")
def test_atomic_dual_write_1mb():
    _test_atomic_dual_write(1024 * 1024)


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/885")
def test_atomic_dual_write_4mb():
    _test_atomic_dual_write(1024 * 1024 * 4)


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/885")
def test_atomic_dual_write_8mb():
    _test_atomic_dual_write(1024 * 1024 * 8)


def _test_atomic_conditional_write(file_size):
    """
    Create a file of A's, use it to set_contents_from_file.
    Verify the contents are all A's.
    Create a file of B's, use it to re-set_contents_from_file.
    Before re-set continues, verify content's still A's
    Re-read the contents, and confirm we get B's
    """
    bucket_name = get_new_bucket()
    objname = "testobj"
    client = get_client()

    # create <file_size> file of A's
    fp_a = FakeWriteFile(file_size, "A")
    client.put_object(Bucket=bucket_name, Key=objname, Body=fp_a)

    fp_b = FakeWriteFile(
        file_size,
        "B",
        lambda: _verify_atomic_key_data(bucket_name, objname, file_size, "A"),
    )

    # create <file_size> file of B's
    # but try to verify the file before we finish writing all the B's
    lf = lambda **kwargs: kwargs["params"]["headers"].update({"If-Match": "*"})
    client.meta.events.register("before-call.s3.PutObject", lf)
    client.put_object(Bucket=bucket_name, Key=objname, Body=fp_b)

    # verify B's
    _verify_atomic_key_data(bucket_name, objname, file_size, "B")


@pytest.mark.fails_on_aws
def test_atomic_conditional_write_1mb():
    _test_atomic_conditional_write(1024 * 1024)


def _test_atomic_dual_conditional_write(file_size):
    """
    create an object, two sessions writing different contents
    confirm that it is all one or the other
    """
    bucket_name = get_new_bucket()
    objname = "testobj"
    client = get_client()

    fp_a = FakeWriteFile(file_size, "A")
    response = client.put_object(Bucket=bucket_name, Key=objname, Body=fp_a)
    _verify_atomic_key_data(bucket_name, objname, file_size, "A")
    etag_fp_a = response["ETag"].replace('"', "")

    # write <file_size> file of C's
    # but before we're done, try to write all B's
    fp_b = FakeWriteFile(file_size, "B")
    lf = lambda **kwargs: kwargs["params"]["headers"].update({"If-Match": etag_fp_a})
    client.meta.events.register("before-call.s3.PutObject", lf)

    def rewind_put_fp_b():
        fp_b.seek(0)
        client.put_object(Bucket=bucket_name, Key=objname, Body=fp_b)

    fp_c = FakeWriteFile(file_size, "C", rewind_put_fp_b)

    e = assert_raises(
        ClientError, client.put_object, Bucket=bucket_name, Key=objname, Body=fp_c
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 412
    assert error_code == "PreconditionFailed"

    # verify the file
    _verify_atomic_key_data(bucket_name, objname, file_size, "B")


@pytest.mark.fails_on_aws
# TODO: test not passing with SSL, fix this
@pytest.mark.fails_on_rgw
def test_atomic_dual_conditional_write_1mb():
    _test_atomic_dual_conditional_write(1024 * 1024)


@pytest.mark.fails_on_aws
# TODO: test not passing with SSL, fix this
@pytest.mark.fails_on_rgw
def test_atomic_write_bucket_gone():
    bucket_name = get_new_bucket()
    client = get_client()

    def remove_bucket():
        client.delete_bucket(Bucket=bucket_name)

    objname = "foo"
    fp_a = FakeWriteFile(1024 * 1024, "A", remove_bucket)

    e = assert_raises(
        ClientError, client.put_object, Bucket=bucket_name, Key=objname, Body=fp_a
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == "NoSuchBucket"


def test_atomic_multipart_upload_write():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_object(Bucket=bucket_name, Key="foo", Body="bar")

    response = client.create_multipart_upload(Bucket=bucket_name, Key="foo")
    upload_id = response["UploadId"]

    response = client.get_object(Bucket=bucket_name, Key="foo")
    body = _get_body(response)
    assert body == "bar"

    client.abort_multipart_upload(Bucket=bucket_name, Key="foo", UploadId=upload_id)

    response = client.get_object(Bucket=bucket_name, Key="foo")
    body = _get_body(response)
    assert body == "bar"


class Counter:
    def __init__(self, default_val):
        self.val = default_val

    def inc(self):
        self.val = self.val + 1


class ActionOnCount:
    def __init__(self, trigger_count, action):
        self.count = 0
        self.trigger_count = trigger_count
        self.action = action
        self.result = 0

    def trigger(self):
        self.count = self.count + 1

        if self.count == self.trigger_count:
            self.result = self.action()


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/901")
def test_multipart_resend():
    bucket_name = get_new_bucket()
    client = get_client()
    key_name = "mymultipart"

    file_size = 8

    parts = []

    response = client.create_multipart_upload(Bucket=bucket_name, Key=key_name)
    upload_id = response["UploadId"]

    fp_a = FakeWriteFile(file_size, "A")

    response = client.upload_part(
        UploadId=upload_id, Bucket=bucket_name, Key=key_name, PartNumber=1, Body=fp_a
    )

    fp_b = FakeWriteFile(file_size, "B")

    response = client.upload_part(
            UploadId=upload_id,
            Bucket=bucket_name,
            Key=key_name,
            Body=fp_b,
            PartNumber=1,
        )
    parts.append({"ETag": response["ETag"].strip('"'), "PartNumber": 1})
    client.complete_multipart_upload(
        Bucket=bucket_name,
        Key=key_name,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )

    _verify_atomic_key_data(bucket_name, key_name, file_size, "B")


@pytest.mark.fails_on_dbstore
def test_ranged_request_response_code():
    content = "testcontent"

    bucket_name = get_new_bucket()
    client = get_client()

    client.put_object(Bucket=bucket_name, Key="testobj", Body=content)
    response = client.get_object(Bucket=bucket_name, Key="testobj", Range="bytes=4-7")

    fetched_content = _get_body(response)
    assert fetched_content == content[4:8]
    assert (
        response["ResponseMetadata"]["HTTPHeaders"]["content-range"] == "bytes 4-7/11"
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 206


def _generate_random_string(size):
    return "".join(
        random.choice(string.ascii_letters + string.digits) for _ in range(size)
    )


@pytest.mark.fails_on_dbstore
def test_ranged_big_request_response_code():
    content = _generate_random_string(8 * 1024 * 1024)

    bucket_name = get_new_bucket()
    client = get_client()

    client.put_object(Bucket=bucket_name, Key="testobj", Body=content)
    response = client.get_object(
        Bucket=bucket_name, Key="testobj", Range="bytes=3145728-5242880"
    )

    fetched_content = _get_body(response)
    assert fetched_content == content[3145728:5242881]
    assert (
        response["ResponseMetadata"]["HTTPHeaders"]["content-range"]
        == "bytes 3145728-5242880/8388608"
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 206


@pytest.mark.fails_on_dbstore
def test_ranged_request_skip_leading_bytes_response_code():
    content = "testcontent"

    bucket_name = get_new_bucket()
    client = get_client()

    client.put_object(Bucket=bucket_name, Key="testobj", Body=content)
    response = client.get_object(Bucket=bucket_name, Key="testobj", Range="bytes=4-")

    fetched_content = _get_body(response)
    assert fetched_content == content[4:]
    assert (
        response["ResponseMetadata"]["HTTPHeaders"]["content-range"] == "bytes 4-10/11"
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 206


@pytest.mark.fails_on_dbstore
def test_ranged_request_return_trailing_bytes_response_code():
    content = "testcontent"

    bucket_name = get_new_bucket()
    client = get_client()

    client.put_object(Bucket=bucket_name, Key="testobj", Body=content)
    response = client.get_object(Bucket=bucket_name, Key="testobj", Range="bytes=-7")

    fetched_content = _get_body(response)
    assert fetched_content == content[-7:]
    assert (
        response["ResponseMetadata"]["HTTPHeaders"]["content-range"] == "bytes 4-10/11"
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 206


def test_ranged_request_invalid_range():
    content = "testcontent"

    bucket_name = get_new_bucket()
    client = get_client()

    client.put_object(Bucket=bucket_name, Key="testobj", Body=content)

    # test invalid range
    e = assert_raises(
        ClientError,
        client.get_object,
        Bucket=bucket_name,
        Key="testobj",
        Range="bytes=40-50",
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 416
    assert error_code == "InvalidRange"


def test_ranged_request_empty_object():
    content = ""

    bucket_name = get_new_bucket()
    client = get_client()

    client.put_object(Bucket=bucket_name, Key="testobj", Body=content)

    # test invalid range
    e = assert_raises(
        ClientError,
        client.get_object,
        Bucket=bucket_name,
        Key="testobj",
        Range="bytes=40-50",
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 416
    assert error_code == "InvalidRange"


def test_versioning_bucket_create_suspend():
    bucket_name = get_new_bucket()
    check_versioning(bucket_name, None)

    check_configure_versioning_retry(bucket_name, "Suspended", "Suspended")
    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")
    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")
    check_configure_versioning_retry(bucket_name, "Suspended", "Suspended")


def check_obj_content(client, bucket_name, key, version_id, content):
    response = client.get_object(Bucket=bucket_name, Key=key, VersionId=version_id)
    if content is not None:
        body = _get_body(response)
        assert body == content
    else:
        assert response["DeleteMarker"] == True


def check_obj_versions(client, bucket_name, key, version_ids, contents):
    # check to see if objects is pointing at correct version

    response = client.list_object_versions(Bucket=bucket_name)
    versions = []
    versions = response["Versions"]
    # obj versions in versions come out created last to first not first to last like version_ids & contents
    versions.reverse()
    i = 0

    for version in versions:
        assert version["VersionId"] == version_ids[i]
        assert version["Key"] == key
        check_obj_content(client, bucket_name, key, version["VersionId"], contents[i])
        i += 1


def create_multiple_versions(
    client,
    bucket_name,
    key,
    num_versions,
    version_ids=None,
    contents=None,
    check_versions=True,
):
    contents = contents or []
    version_ids = version_ids or []

    for i in range(num_versions):
        body = "content-{i}".format(i=i)
        response = client.put_object(Bucket=bucket_name, Key=key, Body=body)
        version_id = response["VersionId"]

        contents.append(body)
        version_ids.append(version_id)

    #    if check_versions:
    #        check_obj_versions(client, bucket_name, key, version_ids, contents)

    return (version_ids, contents)


def remove_obj_version(client, bucket_name, key, version_ids, contents, index):
    assert len(version_ids) == len(contents)
    index = index % len(version_ids)
    rm_version_id = version_ids.pop(index)
    rm_content = contents.pop(index)

    check_obj_content(client, bucket_name, key, rm_version_id, rm_content)

    client.delete_object(Bucket=bucket_name, Key=key, VersionId=rm_version_id)

    if len(version_ids) != 0:
        check_obj_versions(client, bucket_name, key, version_ids, contents)


def clean_up_bucket(client, bucket_name, key, version_ids):
    for version_id in version_ids:
        client.delete_object(Bucket=bucket_name, Key=key, VersionId=version_id)

    client.delete_bucket(Bucket=bucket_name)


def _do_test_create_remove_versions(
    client, bucket_name, key, num_versions, remove_start_idx, idx_inc
):
    (version_ids, contents) = create_multiple_versions(
        client, bucket_name, key, num_versions
    )

    idx = remove_start_idx

    for j in range(num_versions):
        remove_obj_version(client, bucket_name, key, version_ids, contents, idx)
        idx += idx_inc

    response = client.list_object_versions(Bucket=bucket_name)
    if "Versions" in response:
        print(response["Versions"])


def test_versioning_obj_create_read_remove():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_bucket_versioning(
        Bucket=bucket_name,
        VersioningConfiguration={"MFADelete": "Disabled", "Status": "Enabled"},
    )
    key = "testobj"
    num_versions = 5

    _do_test_create_remove_versions(client, bucket_name, key, num_versions, -1, 0)
    _do_test_create_remove_versions(client, bucket_name, key, num_versions, -1, 0)
    _do_test_create_remove_versions(client, bucket_name, key, num_versions, 0, 0)
    _do_test_create_remove_versions(client, bucket_name, key, num_versions, 1, 0)
    _do_test_create_remove_versions(client, bucket_name, key, num_versions, 4, -1)
    _do_test_create_remove_versions(client, bucket_name, key, num_versions, 3, 3)


def test_versioning_obj_create_read_remove_head():
    bucket_name = get_new_bucket()

    client = get_client()
    client.put_bucket_versioning(
        Bucket=bucket_name,
        VersioningConfiguration={"MFADelete": "Disabled", "Status": "Enabled"},
    )
    key = "testobj"
    num_versions = 5

    (version_ids, contents) = create_multiple_versions(
        client, bucket_name, key, num_versions
    )

    # removes old head object, checks new one
    removed_version_id = version_ids.pop()
    contents.pop()
    num_versions = num_versions - 1

    response = client.delete_object(
        Bucket=bucket_name, Key=key, VersionId=removed_version_id
    )
    response = client.get_object(Bucket=bucket_name, Key=key)
    body = _get_body(response)
    assert body == contents[-1]

    # add a delete marker
    response = client.delete_object(Bucket=bucket_name, Key=key)
    assert response["DeleteMarker"] == True

    delete_marker_version_id = response["VersionId"]
    version_ids.append(delete_marker_version_id)

    response = client.list_object_versions(Bucket=bucket_name)
    assert len(response["Versions"]) == num_versions
    assert len(response["DeleteMarkers"]) == 1
    assert response["DeleteMarkers"][0]["VersionId"] == delete_marker_version_id

    clean_up_bucket(client, bucket_name, key, version_ids)


def test_versioning_obj_plain_null_version_removal():
    bucket_name = get_new_bucket()
    check_versioning(bucket_name, None)

    client = get_client()
    key = "testobjfoo"
    content = "fooz"
    client.put_object(Bucket=bucket_name, Key=key, Body=content)

    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")
    client.delete_object(Bucket=bucket_name, Key=key, VersionId="null")

    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key=key)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == "NoSuchKey"

    response = client.list_object_versions(Bucket=bucket_name)
    assert not "Versions" in response


def test_versioning_obj_plain_null_version_overwrite():
    bucket_name = get_new_bucket()
    check_versioning(bucket_name, None)

    client = get_client()
    key = "testobjfoo"
    content = "fooz"
    client.put_object(Bucket=bucket_name, Key=key, Body=content)

    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")

    content2 = "zzz"
    response = client.put_object(Bucket=bucket_name, Key=key, Body=content2)
    response = client.get_object(Bucket=bucket_name, Key=key)
    body = _get_body(response)
    assert body == content2

    version_id = response["VersionId"]
    client.delete_object(Bucket=bucket_name, Key=key, VersionId=version_id)
    response = client.get_object(Bucket=bucket_name, Key=key)
    body = _get_body(response)
    assert body == content

    client.delete_object(Bucket=bucket_name, Key=key, VersionId="null")

    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key=key)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == "NoSuchKey"

    response = client.list_object_versions(Bucket=bucket_name)
    assert not "Versions" in response


def test_versioning_obj_plain_null_version_overwrite_suspended():
    bucket_name = get_new_bucket()
    check_versioning(bucket_name, None)

    client = get_client()
    key = "testobjbar"
    content = "foooz"
    client.put_object(Bucket=bucket_name, Key=key, Body=content)

    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")
    check_configure_versioning_retry(bucket_name, "Suspended", "Suspended")

    content2 = "zzz"
    response = client.put_object(Bucket=bucket_name, Key=key, Body=content2)
    response = client.get_object(Bucket=bucket_name, Key=key)
    body = _get_body(response)
    assert body == content2

    response = client.list_object_versions(Bucket=bucket_name)
    # original object with 'null' version id still counts as a version
    assert len(response["Versions"]) == 1

    client.delete_object(Bucket=bucket_name, Key=key, VersionId="null")

    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key=key)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == "NoSuchKey"

    response = client.list_object_versions(Bucket=bucket_name)
    assert not "Versions" in response


def delete_suspended_versioning_obj(client, bucket_name, key, version_ids, contents):
    client.delete_object(Bucket=bucket_name, Key=key)

    # clear out old null objects in lists since they will get overwritten
    assert len(version_ids) == len(contents)
    i = 0
    for version_id in version_ids:
        if version_id == "null":
            version_ids.pop(i)
            contents.pop(i)
        i += 1

    return (version_ids, contents)


def overwrite_suspended_versioning_obj(
    client, bucket_name, key, version_ids, contents, content
):
    client.put_object(Bucket=bucket_name, Key=key, Body=content)

    # clear out old null objects in lists since they will get overwritten
    assert len(version_ids) == len(contents)
    i = 0
    for version_id in version_ids:
        if version_id == "null":
            version_ids.pop(i)
            contents.pop(i)
        i += 1

    # add new content with 'null' version id to the end
    contents.append(content)
    version_ids.append("null")

    return (version_ids, contents)


def test_versioning_obj_suspend_versions():
    bucket_name = get_new_bucket()
    client = get_client()

    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")

    key = "testobj"
    num_versions = 5

    (version_ids, contents) = create_multiple_versions(
        client, bucket_name, key, num_versions
    )

    check_configure_versioning_retry(bucket_name, "Suspended", "Suspended")

    delete_suspended_versioning_obj(client, bucket_name, key, version_ids, contents)
    delete_suspended_versioning_obj(client, bucket_name, key, version_ids, contents)

    overwrite_suspended_versioning_obj(
        client, bucket_name, key, version_ids, contents, "null content 1"
    )
    overwrite_suspended_versioning_obj(
        client, bucket_name, key, version_ids, contents, "null content 2"
    )
    delete_suspended_versioning_obj(client, bucket_name, key, version_ids, contents)
    overwrite_suspended_versioning_obj(
        client, bucket_name, key, version_ids, contents, "null content 3"
    )
    delete_suspended_versioning_obj(client, bucket_name, key, version_ids, contents)

    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")
    (version_ids, contents) = create_multiple_versions(
        client, bucket_name, key, 3, version_ids, contents
    )
    num_versions += 3

    for idx in range(num_versions):
        remove_obj_version(client, bucket_name, key, version_ids, contents, idx)

    assert len(version_ids) == 0
    assert len(version_ids) == len(contents)


def test_versioning_obj_create_versions_remove_all():
    bucket_name = get_new_bucket()
    client = get_client()

    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")

    key = "testobj"
    num_versions = 10

    (version_ids, contents) = create_multiple_versions(
        client, bucket_name, key, num_versions
    )
    for idx in range(num_versions):
        remove_obj_version(client, bucket_name, key, version_ids, contents, idx)

    assert len(version_ids) == 0
    assert len(version_ids) == len(contents)


def test_versioning_obj_create_versions_remove_special_names():
    bucket_name = get_new_bucket()
    client = get_client()

    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")

    keys = ["_testobj", "_", ":", " "]
    num_versions = 10

    for key in keys:
        (version_ids, contents) = create_multiple_versions(
            client, bucket_name, key, num_versions
        )
        for idx in range(num_versions):
            remove_obj_version(client, bucket_name, key, version_ids, contents, idx)

        assert len(version_ids) == 0
        assert len(version_ids) == len(contents)


@pytest.mark.fails_on_dbstore
def test_versioning_obj_create_overwrite_multipart():
    bucket_name = get_new_bucket()
    client = get_client()

    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")

    key = "testobj"
    num_versions = 3
    contents = []
    version_ids = []

    for i in range(num_versions):
        ret = _do_test_multipart_upload_contents(bucket_name, key, 3)
        contents.append(ret)

    response = client.list_object_versions(Bucket=bucket_name)
    for version in response["Versions"]:
        version_ids.append(version["VersionId"])

    version_ids.reverse()
    check_obj_versions(client, bucket_name, key, version_ids, contents)

    for idx in range(num_versions):
        remove_obj_version(client, bucket_name, key, version_ids, contents, idx)

    assert len(version_ids) == 0
    assert len(version_ids) == len(contents)


def test_versioning_obj_list_marker():
    bucket_name = get_new_bucket()
    client = get_client()

    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")

    key = "testobj"
    key2 = "testobj-1"
    num_versions = 5

    contents = []
    version_ids = []
    contents2 = []
    version_ids2 = []

    # for key #1
    for i in range(num_versions):
        body = "content-{i}".format(i=i)
        response = client.put_object(Bucket=bucket_name, Key=key, Body=body)
        version_id = response["VersionId"]

        contents.append(body)
        version_ids.append(version_id)

    # for key #2
    for i in range(num_versions):
        body = "content-{i}".format(i=i)
        response = client.put_object(Bucket=bucket_name, Key=key2, Body=body)
        version_id = response["VersionId"]

        contents2.append(body)
        version_ids2.append(version_id)

    response = client.list_object_versions(Bucket=bucket_name)
    versions = response["Versions"]
    # obj versions in versions come out created last to first not first to last like version_ids & contents
    versions.reverse()

    i = 0
    # test the last 5 created objects first
    for i in range(5):
        version = versions[i]
        assert version["VersionId"] == version_ids2[i]
        assert version["Key"] == key2
        check_obj_content(client, bucket_name, key2, version["VersionId"], contents2[i])
        i += 1

    # then the first 5
    for j in range(5):
        version = versions[i]
        assert version["VersionId"] == version_ids[j]
        assert version["Key"] == key
        check_obj_content(client, bucket_name, key, version["VersionId"], contents[j])
        i += 1


@pytest.mark.fails_on_dbstore
def test_versioning_copy_obj_version():
    bucket_name = get_new_bucket()
    client = get_client()

    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")

    key = "testobj"
    num_versions = 3

    (version_ids, contents) = create_multiple_versions(
        client, bucket_name, key, num_versions
    )

    for i in range(num_versions):
        new_key_name = "key_{i}".format(i=i)
        copy_source = {"Bucket": bucket_name, "Key": key, "VersionId": version_ids[i]}
        client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key=new_key_name)
        response = client.get_object(Bucket=bucket_name, Key=new_key_name)
        body = _get_body(response)
        assert body == contents[i]

    another_bucket_name = get_new_bucket()

    for i in range(num_versions):
        new_key_name = "key_{i}".format(i=i)
        copy_source = {"Bucket": bucket_name, "Key": key, "VersionId": version_ids[i]}
        client.copy_object(
            Bucket=another_bucket_name, CopySource=copy_source, Key=new_key_name
        )
        response = client.get_object(Bucket=another_bucket_name, Key=new_key_name)
        body = _get_body(response)
        assert body == contents[i]

    new_key_name = "new_key"
    copy_source = {"Bucket": bucket_name, "Key": key}
    client.copy_object(
        Bucket=another_bucket_name, CopySource=copy_source, Key=new_key_name
    )

    response = client.get_object(Bucket=another_bucket_name, Key=new_key_name)
    body = _get_body(response)
    assert body == contents[-1]


def test_versioning_multi_object_delete():
    bucket_name = get_new_bucket()
    client = get_client()

    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")

    key = "key"
    num_versions = 2

    (version_ids, contents) = create_multiple_versions(
        client, bucket_name, key, num_versions
    )
    assert len(version_ids) == 2

    # delete both versions
    objects = [{"Key": key, "VersionId": v} for v in version_ids]
    client.delete_objects(Bucket=bucket_name, Delete={"Objects": objects})

    response = client.list_object_versions(Bucket=bucket_name)
    assert not "Versions" in response

    # now remove again, should all succeed due to idempotency
    client.delete_objects(Bucket=bucket_name, Delete={"Objects": objects})

    response = client.list_object_versions(Bucket=bucket_name)
    assert not "Versions" in response


def test_versioning_multi_object_delete_with_marker():
    bucket_name = get_new_bucket()
    client = get_client()

    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")

    key = "key"
    num_versions = 2

    (version_ids, contents) = create_multiple_versions(
        client, bucket_name, key, num_versions
    )
    assert len(version_ids) == num_versions
    objects = [{"Key": key, "VersionId": v} for v in version_ids]

    # create a delete marker
    response = client.delete_object(Bucket=bucket_name, Key=key)
    assert response["DeleteMarker"]
    objects += [{"Key": key, "VersionId": response["VersionId"]}]

    # delete all versions
    client.delete_objects(Bucket=bucket_name, Delete={"Objects": objects})

    response = client.list_object_versions(Bucket=bucket_name)
    assert not "Versions" in response
    assert not "DeleteMarkers" in response

    # now remove again, should all succeed due to idempotency
    client.delete_objects(Bucket=bucket_name, Delete={"Objects": objects})

    response = client.list_object_versions(Bucket=bucket_name)
    assert not "Versions" in response
    assert not "DeleteMarkers" in response


@pytest.mark.fails_on_dbstore
def test_versioning_multi_object_delete_with_marker_create():
    bucket_name = get_new_bucket()
    client = get_client()

    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")

    key = "key"

    # use delete_objects() to create a delete marker
    response = client.delete_objects(
        Bucket=bucket_name, Delete={"Objects": [{"Key": key}]}
    )
    assert len(response["Deleted"]) == 1
    assert response["Deleted"][0]["DeleteMarker"]
    delete_marker_version_id = response["Deleted"][0]["DeleteMarkerVersionId"]

    response = client.list_object_versions(Bucket=bucket_name)
    delete_markers = response["DeleteMarkers"]

    assert len(delete_markers) == 1
    assert delete_marker_version_id == delete_markers[0]["VersionId"]
    assert key == delete_markers[0]["Key"]


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/897")
def test_versioned_object_acl():
    bucket_name = get_new_bucket()
    client = get_client()

    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")

    key = "xyz"
    num_versions = 3

    (version_ids, contents) = create_multiple_versions(
        client, bucket_name, key, num_versions
    )

    version_id = version_ids[1]

    response = client.get_object_acl(Bucket=bucket_name, Key=key, VersionId=version_id)

    display_name = get_main_display_name()
    user_id = get_main_user_id()

    assert response["Owner"]["DisplayName"] == display_name
    assert response["Owner"]["ID"] == user_id

    grants = response["Grants"]
    default_policy = [
        dict(
            Permission="FULL_CONTROL",
            ID=user_id,
            DisplayName=display_name,
            URI=None,
            EmailAddress=None,
            Type="CanonicalUser",
        ),
    ]

    check_grants(grants, default_policy)

    client.put_object_acl(
        ACL="public-read", Bucket=bucket_name, Key=key, VersionId=version_id
    )

    response = client.get_object_acl(Bucket=bucket_name, Key=key, VersionId=version_id)
    grants = response["Grants"]
    check_grants(
        grants,
        [
            dict(
                Permission="READ",
                ID=None,
                DisplayName=None,
                URI="http://acs.amazonaws.com/groups/global/AllUsers",
                EmailAddress=None,
                Type="Group",
            ),
            dict(
                Permission="FULL_CONTROL",
                ID=user_id,
                DisplayName=display_name,
                URI=None,
                EmailAddress=None,
                Type="CanonicalUser",
            ),
        ],
    )

    client.put_object(Bucket=bucket_name, Key=key)

    response = client.get_object_acl(Bucket=bucket_name, Key=key)
    grants = response["Grants"]
    check_grants(grants, default_policy)


@pytest.mark.fails_on_dbstore
def test_versioned_object_acl_no_version_specified():
    bucket_name = get_new_bucket()
    client = get_client()

    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")

    key = "xyz"
    num_versions = 3

    (version_ids, contents) = create_multiple_versions(
        client, bucket_name, key, num_versions
    )

    response = client.get_object(Bucket=bucket_name, Key=key)
    version_id = response["VersionId"]

    response = client.get_object_acl(Bucket=bucket_name, Key=key, VersionId=version_id)

    display_name = get_main_display_name()
    user_id = get_main_user_id()

    assert response["Owner"]["DisplayName"] == display_name
    assert response["Owner"]["ID"] == user_id

    grants = response["Grants"]
    default_policy = [
        dict(
            Permission="FULL_CONTROL",
            ID=user_id,
            DisplayName=display_name,
            URI=None,
            EmailAddress=None,
            Type="CanonicalUser",
        ),
    ]

    check_grants(grants, default_policy)

    client.put_object_acl(ACL="public-read", Bucket=bucket_name, Key=key)

    response = client.get_object_acl(Bucket=bucket_name, Key=key, VersionId=version_id)
    grants = response["Grants"]
    check_grants(
        grants,
        [
            dict(
                Permission="READ",
                ID=None,
                DisplayName=None,
                URI="http://acs.amazonaws.com/groups/global/AllUsers",
                EmailAddress=None,
                Type="Group",
            ),
            dict(
                Permission="FULL_CONTROL",
                ID=user_id,
                DisplayName=display_name,
                URI=None,
                EmailAddress=None,
                Type="CanonicalUser",
            ),
        ],
    )


def _do_create_object(client, bucket_name, key, i):
    body = "data {i}".format(i=i)
    client.put_object(Bucket=bucket_name, Key=key, Body=body)


def _do_remove_ver(client, bucket_name, key, version_id):
    client.delete_object(Bucket=bucket_name, Key=key, VersionId=version_id)


def _do_create_versioned_obj_concurrent(client, bucket_name, key, num):
    t = []
    for i in range(num):
        thr = threading.Thread(
            target=_do_create_object, args=(client, bucket_name, key, i)
        )
        thr.start()
        t.append(thr)
    return t


def _do_clear_versioned_bucket_concurrent(client, bucket_name):
    t = []
    response = client.list_object_versions(Bucket=bucket_name)
    for version in response.get("Versions", []):
        thr = threading.Thread(
            target=_do_remove_ver,
            args=(client, bucket_name, version["Key"], version["VersionId"]),
        )
        thr.start()
        t.append(thr)
    return t


# TODO: remove fails_on_rgw when https://tracker.ceph.com/issues/39142 is resolved
@pytest.mark.fails_on_rgw
def test_versioned_concurrent_object_create_concurrent_remove():
    bucket_name = get_new_bucket()
    client = get_client()

    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")

    key = "myobj"
    num_versions = 5

    for i in range(5):
        t = _do_create_versioned_obj_concurrent(client, bucket_name, key, num_versions)
        _do_wait_completion(t)

        response = client.list_object_versions(Bucket=bucket_name)
        versions = response["Versions"]

        assert len(versions) == num_versions

        t = _do_clear_versioned_bucket_concurrent(client, bucket_name)
        _do_wait_completion(t)

        response = client.list_object_versions(Bucket=bucket_name)
        assert not "Versions" in response


def test_versioned_concurrent_object_create_and_remove():
    bucket_name = get_new_bucket()
    client = get_client()

    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")

    key = "myobj"
    num_versions = 3

    all_threads = []

    for i in range(3):
        t = _do_create_versioned_obj_concurrent(client, bucket_name, key, num_versions)
        all_threads.append(t)

        t = _do_clear_versioned_bucket_concurrent(client, bucket_name)
        all_threads.append(t)

    for t in all_threads:
        _do_wait_completion(t)

    t = _do_clear_versioned_bucket_concurrent(client, bucket_name)
    _do_wait_completion(t)

    response = client.list_object_versions(Bucket=bucket_name)
    assert not "Versions" in response


@pytest.mark.lifecycle
@pytest.mark.skip(reason="Not Implemented")
def test_lifecycle_set():
    bucket_name = get_new_bucket()
    client = get_client()
    rules = [
        {
            "ID": "rule1",
            "Expiration": {"Days": 1},
            "Prefix": "test1/",
            "Status": "Enabled",
        },
        {
            "ID": "rule2",
            "Expiration": {"Days": 2},
            "Prefix": "test2/",
            "Status": "Disabled",
        },
    ]
    lifecycle = {"Rules": rules}
    response = client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name, LifecycleConfiguration=lifecycle
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


@pytest.mark.lifecycle
@pytest.mark.skip(reason="Not Implemented")
def test_lifecycle_get():
    bucket_name = get_new_bucket()
    client = get_client()
    rules = [
        {
            "ID": "test1/",
            "Expiration": {"Days": 31},
            "Prefix": "test1/",
            "Status": "Enabled",
        },
        {
            "ID": "test2/",
            "Expiration": {"Days": 120},
            "Prefix": "test2/",
            "Status": "Enabled",
        },
    ]
    lifecycle = {"Rules": rules}
    client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name, LifecycleConfiguration=lifecycle
    )
    response = client.get_bucket_lifecycle_configuration(Bucket=bucket_name)
    assert response["Rules"] == rules


@pytest.mark.lifecycle
@pytest.mark.skip(reason="Not Implemented")
def test_lifecycle_get_no_id():
    bucket_name = get_new_bucket()
    client = get_client()

    rules = [
        {"Expiration": {"Days": 31}, "Prefix": "test1/", "Status": "Enabled"},
        {"Expiration": {"Days": 120}, "Prefix": "test2/", "Status": "Enabled"},
    ]
    lifecycle = {"Rules": rules}
    client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name, LifecycleConfiguration=lifecycle
    )
    response = client.get_bucket_lifecycle_configuration(Bucket=bucket_name)
    current_lc = response["Rules"]

    Rule = namedtuple("Rule", ["prefix", "status", "days"])
    rules = {
        "rule1": Rule("test1/", "Enabled", 31),
        "rule2": Rule("test2/", "Enabled", 120),
    }

    for lc_rule in current_lc:
        if lc_rule["Prefix"] == rules["rule1"].prefix:
            assert lc_rule["Expiration"]["Days"] == rules["rule1"].days
            assert lc_rule["Status"] == rules["rule1"].status
            assert "ID" in lc_rule
        elif lc_rule["Prefix"] == rules["rule2"].prefix:
            assert lc_rule["Expiration"]["Days"] == rules["rule2"].days
            assert lc_rule["Status"] == rules["rule2"].status
            assert "ID" in lc_rule
        else:
            # neither of the rules we supplied was returned, something wrong
            print("rules not right")
            assert False


# The test harness for lifecycle is configured to treat days as 10 second intervals.
@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
@pytest.mark.skip(reason="Not Implemented")
def test_lifecycle_expiration():
    bucket_name = _create_objects(
        keys=[
            "expire1/foo",
            "expire1/bar",
            "keep2/foo",
            "keep2/bar",
            "expire3/foo",
            "expire3/bar",
        ]
    )
    client = get_client()
    rules = [
        {
            "ID": "rule1",
            "Expiration": {"Days": 1},
            "Prefix": "expire1/",
            "Status": "Enabled",
        },
        {
            "ID": "rule2",
            "Expiration": {"Days": 5},
            "Prefix": "expire3/",
            "Status": "Enabled",
        },
    ]
    lifecycle = {"Rules": rules}
    client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name, LifecycleConfiguration=lifecycle
    )
    response = client.list_objects(Bucket=bucket_name)
    init_objects = response["Contents"]

    lc_interval = get_lc_debug_interval()

    time.sleep(3 * lc_interval)
    response = client.list_objects(Bucket=bucket_name)
    expire1_objects = response["Contents"]

    time.sleep(lc_interval)
    response = client.list_objects(Bucket=bucket_name)
    keep2_objects = response["Contents"]

    time.sleep(3 * lc_interval)
    response = client.list_objects(Bucket=bucket_name)
    expire3_objects = response["Contents"]

    assert len(init_objects) == 6
    assert len(expire1_objects) == 4
    assert len(keep2_objects) == 4
    assert len(expire3_objects) == 2


@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
@pytest.mark.fails_on_aws
@pytest.mark.list_objects_v2
@pytest.mark.fails_on_dbstore
@pytest.mark.skip(reason="Not Implemented")
def test_lifecyclev2_expiration():
    bucket_name = _create_objects(
        keys=[
            "expire1/foo",
            "expire1/bar",
            "keep2/foo",
            "keep2/bar",
            "expire3/foo",
            "expire3/bar",
        ]
    )
    client = get_client()
    rules = [
        {
            "ID": "rule1",
            "Expiration": {"Days": 1},
            "Prefix": "expire1/",
            "Status": "Enabled",
        },
        {
            "ID": "rule2",
            "Expiration": {"Days": 5},
            "Prefix": "expire3/",
            "Status": "Enabled",
        },
    ]
    lifecycle = {"Rules": rules}
    client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name, LifecycleConfiguration=lifecycle
    )
    response = client.list_objects_v2(Bucket=bucket_name)
    init_objects = response["Contents"]

    lc_interval = get_lc_debug_interval()

    time.sleep(3 * lc_interval)
    response = client.list_objects_v2(Bucket=bucket_name)
    expire1_objects = response["Contents"]

    time.sleep(lc_interval)
    response = client.list_objects_v2(Bucket=bucket_name)
    keep2_objects = response["Contents"]

    time.sleep(3 * lc_interval)
    response = client.list_objects_v2(Bucket=bucket_name)
    expire3_objects = response["Contents"]

    assert len(init_objects) == 6
    assert len(expire1_objects) == 4
    assert len(keep2_objects) == 4
    assert len(expire3_objects) == 2


@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
@pytest.mark.fails_on_aws
@pytest.mark.skip(reason="Not Implemented")
def test_lifecycle_expiration_versioning_enabled():
    bucket_name = get_new_bucket()
    client = get_client()
    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")
    create_multiple_versions(client, bucket_name, "test1/a", 1)
    client.delete_object(Bucket=bucket_name, Key="test1/a")

    rules = [
        {
            "ID": "rule1",
            "Expiration": {"Days": 1},
            "Prefix": "test1/",
            "Status": "Enabled",
        }
    ]
    lifecycle = {"Rules": rules}
    client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name, LifecycleConfiguration=lifecycle
    )

    lc_interval = get_lc_debug_interval()

    time.sleep(3 * lc_interval)

    response = client.list_object_versions(Bucket=bucket_name)
    versions = response["Versions"]
    delete_markers = response["DeleteMarkers"]
    assert len(versions) == 1
    assert len(delete_markers) == 1


@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
@pytest.mark.fails_on_aws
@pytest.mark.skip(reason="Not Implemented")
def test_lifecycle_expiration_tags1():
    bucket_name = get_new_bucket()
    client = get_client()

    tom_key = "days1/tom"
    tom_tagset = {"TagSet": [{"Key": "tom", "Value": "sawyer"}]}

    client.put_object(Bucket=bucket_name, Key=tom_key, Body="tom_body")

    response = client.put_object_tagging(
        Bucket=bucket_name, Key=tom_key, Tagging=tom_tagset
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    lifecycle_config = {
        "Rules": [
            {
                "Expiration": {
                    "Days": 1,
                },
                "ID": "rule_tag1",
                "Filter": {
                    "Prefix": "days1/",
                    "Tag": {"Key": "tom", "Value": "sawyer"},
                },
                "Status": "Enabled",
            },
        ]
    }

    response = client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name, LifecycleConfiguration=lifecycle_config
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    lc_interval = get_lc_debug_interval()

    time.sleep(3 * lc_interval)

    try:
        expire_objects = response["Contents"]
    except KeyError:
        expire_objects = []

    assert len(expire_objects) == 0


# factor out common setup code
def setup_lifecycle_tags2(client, bucket_name):
    tom_key = "days1/tom"
    tom_tagset = {"TagSet": [{"Key": "tom", "Value": "sawyer"}]}

    client.put_object(Bucket=bucket_name, Key=tom_key, Body="tom_body")

    response = client.put_object_tagging(
        Bucket=bucket_name, Key=tom_key, Tagging=tom_tagset
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    huck_key = "days1/huck"
    huck_tagset = {
        "TagSet": [{"Key": "tom", "Value": "sawyer"}, {"Key": "huck", "Value": "finn"}]
    }

    client.put_object(Bucket=bucket_name, Key=huck_key, Body="huck_body")

    response = client.put_object_tagging(
        Bucket=bucket_name, Key=huck_key, Tagging=huck_tagset
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    lifecycle_config = {
        "Rules": [
            {
                "Expiration": {
                    "Days": 1,
                },
                "ID": "rule_tag1",
                "Filter": {
                    "Prefix": "days1/",
                    "Tag": {"Key": "tom", "Value": "sawyer"},
                    "And": {
                        "Prefix": "days1",
                        "Tags": [
                            {"Key": "huck", "Value": "finn"},
                        ],
                    },
                },
                "Status": "Enabled",
            },
        ]
    }

    response = client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name, LifecycleConfiguration=lifecycle_config
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    return response


@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
@pytest.mark.skip(reason="Not Implemented")
def test_lifecycle_expiration_tags2():
    bucket_name = get_new_bucket()
    client = get_client()

    response = setup_lifecycle_tags2(client, bucket_name)

    lc_interval = get_lc_debug_interval()

    time.sleep(3 * lc_interval)
    response = client.list_objects(Bucket=bucket_name)
    expire1_objects = response["Contents"]

    assert len(expire1_objects) == 1


@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
@pytest.mark.skip(reason="Not Implemented")
def test_lifecycle_expiration_versioned_tags2():
    bucket_name = get_new_bucket()
    client = get_client()

    # mix in versioning
    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")

    response = setup_lifecycle_tags2(client, bucket_name)

    lc_interval = get_lc_debug_interval()

    time.sleep(3 * lc_interval)
    response = client.list_objects(Bucket=bucket_name)
    expire1_objects = response["Contents"]

    assert len(expire1_objects) == 1


# setup for scenario based on vidushi mishra's in rhbz#1877737
def setup_lifecycle_noncur_tags(client, bucket_name, days):
    # first create and tag the objects (10 versions of 1)
    key = "myobject_"
    tagset = {"TagSet": [{"Key": "vidushi", "Value": "mishra"}]}

    for ix in range(10):
        body = "%s v%d" % (key, ix)
        response = client.put_object(Bucket=bucket_name, Key=key, Body=body)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        response = client.put_object_tagging(
            Bucket=bucket_name, Key=key, Tagging=tagset
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    lifecycle_config = {
        "Rules": [
            {
                "NoncurrentVersionExpiration": {
                    "NoncurrentDays": days,
                },
                "ID": "rule_tag1",
                "Filter": {
                    "Prefix": "",
                    "Tag": {"Key": "vidushi", "Value": "mishra"},
                },
                "Status": "Enabled",
            },
        ]
    }

    response = client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name, LifecycleConfiguration=lifecycle_config
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    return response


def verify_lifecycle_expiration_noncur_tags(client, bucket_name, secs):
    time.sleep(secs)
    try:
        response = client.list_object_versions(Bucket=bucket_name)
        objs_list = response["Versions"]
    except:
        objs_list = []
    return len(objs_list)


@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
@pytest.mark.skip(reason="Not Implemented")
def test_lifecycle_expiration_noncur_tags1():
    bucket_name = get_new_bucket()
    client = get_client()

    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")

    # create 10 object versions (9 noncurrent) and a tag-filter
    # noncurrent version expiration at 4 "days"
    response = setup_lifecycle_noncur_tags(client, bucket_name, 4)

    lc_interval = get_lc_debug_interval()

    num_objs = verify_lifecycle_expiration_noncur_tags(
        client, bucket_name, 2 * lc_interval
    )

    # at T+20, 10 objects should exist
    assert num_objs == 10

    num_objs = verify_lifecycle_expiration_noncur_tags(
        client, bucket_name, 5 * lc_interval
    )

    # at T+60, only the current object version should exist
    assert num_objs == 1


@pytest.mark.lifecycle
@pytest.mark.skip(reason="Not Implemented")
def test_lifecycle_id_too_long():
    bucket_name = get_new_bucket()
    client = get_client()
    rules = [
        {
            "ID": 256 * "a",
            "Expiration": {"Days": 2},
            "Prefix": "test1/",
            "Status": "Enabled",
        }
    ]
    lifecycle = {"Rules": rules}

    e = assert_raises(
        ClientError,
        client.put_bucket_lifecycle_configuration,
        Bucket=bucket_name,
        LifecycleConfiguration=lifecycle,
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == "InvalidArgument"


@pytest.mark.lifecycle
@pytest.mark.skip(reason="Not Implemented")
def test_lifecycle_same_id():
    bucket_name = get_new_bucket()
    client = get_client()
    rules = [
        {
            "ID": "rule1",
            "Expiration": {"Days": 1},
            "Prefix": "test1/",
            "Status": "Enabled",
        },
        {
            "ID": "rule1",
            "Expiration": {"Days": 2},
            "Prefix": "test2/",
            "Status": "Enabled",
        },
    ]
    lifecycle = {"Rules": rules}

    e = assert_raises(
        ClientError,
        client.put_bucket_lifecycle_configuration,
        Bucket=bucket_name,
        LifecycleConfiguration=lifecycle,
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == "InvalidArgument"


@pytest.mark.lifecycle
@pytest.mark.skip(reason="Not Implemented")
def test_lifecycle_invalid_status():
    bucket_name = get_new_bucket()
    client = get_client()
    rules = [
        {
            "ID": "rule1",
            "Expiration": {"Days": 2},
            "Prefix": "test1/",
            "Status": "enabled",
        }
    ]
    lifecycle = {"Rules": rules}

    e = assert_raises(
        ClientError,
        client.put_bucket_lifecycle_configuration,
        Bucket=bucket_name,
        LifecycleConfiguration=lifecycle,
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == "MalformedXML"

    rules = [
        {
            "ID": "rule1",
            "Expiration": {"Days": 2},
            "Prefix": "test1/",
            "Status": "disabled",
        }
    ]
    lifecycle = {"Rules": rules}

    e = assert_raises(
        ClientError,
        client.put_bucket_lifecycle,
        Bucket=bucket_name,
        LifecycleConfiguration=lifecycle,
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == "MalformedXML"

    rules = [
        {
            "ID": "rule1",
            "Expiration": {"Days": 2},
            "Prefix": "test1/",
            "Status": "invalid",
        }
    ]
    lifecycle = {"Rules": rules}

    e = assert_raises(
        ClientError,
        client.put_bucket_lifecycle_configuration,
        Bucket=bucket_name,
        LifecycleConfiguration=lifecycle,
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == "MalformedXML"


@pytest.mark.lifecycle
@pytest.mark.skip(reason="Not Implemented")
def test_lifecycle_set_date():
    bucket_name = get_new_bucket()
    client = get_client()
    rules = [
        {
            "ID": "rule1",
            "Expiration": {"Date": "2017-09-27"},
            "Prefix": "test1/",
            "Status": "Enabled",
        }
    ]
    lifecycle = {"Rules": rules}

    response = client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name, LifecycleConfiguration=lifecycle
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


@pytest.mark.lifecycle
@pytest.mark.skip(reason="Not Implemented")
def test_lifecycle_set_invalid_date():
    bucket_name = get_new_bucket()
    client = get_client()
    rules = [
        {
            "ID": "rule1",
            "Expiration": {"Date": "20200101"},
            "Prefix": "test1/",
            "Status": "Enabled",
        }
    ]
    lifecycle = {"Rules": rules}

    e = assert_raises(
        ClientError,
        client.put_bucket_lifecycle_configuration,
        Bucket=bucket_name,
        LifecycleConfiguration=lifecycle,
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400


@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
def test_lifecycle_expiration_date():
    bucket_name = _create_objects(keys=["past/foo", "future/bar"])
    client = get_client()
    rules = [
        {
            "ID": "rule1",
            "Expiration": {"Date": "2015-01-01"},
            "Prefix": "past/",
            "Status": "Enabled",
        },
        {
            "ID": "rule2",
            "Expiration": {"Date": "2030-01-01"},
            "Prefix": "future/",
            "Status": "Enabled",
        },
    ]
    lifecycle = {"Rules": rules}
    client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name, LifecycleConfiguration=lifecycle
    )
    response = client.list_objects(Bucket=bucket_name)
    init_objects = response["Contents"]

    lc_interval = get_lc_debug_interval()

    # Wait for first expiration (plus fudge to handle the timer window)
    time.sleep(3 * lc_interval)
    response = client.list_objects(Bucket=bucket_name)
    expire_objects = response["Contents"]

    assert len(init_objects) == 2
    assert len(expire_objects) == 1


@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
@pytest.mark.skip(reason="Not Implemented")
def test_lifecycle_expiration_days0():
    bucket_name = _create_objects(keys=["days0/foo", "days0/bar"])
    client = get_client()

    rules = [
        {
            "Expiration": {"Days": 0},
            "ID": "rule1",
            "Prefix": "days0/",
            "Status": "Enabled",
        }
    ]
    lifecycle = {"Rules": rules}

    # days: 0 is legal in a transition rule, but not legal in an
    # expiration rule
    response_code = ""
    try:
        response = client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name, LifecycleConfiguration=lifecycle
        )
    except botocore.exceptions.ClientError as e:
        response_code = e.response["Error"]["Code"]

    assert response_code == "InvalidArgument"


def setup_lifecycle_expiration(client, bucket_name, rule_id, delta_days, rule_prefix):
    rules = [
        {
            "ID": rule_id,
            "Expiration": {"Days": delta_days},
            "Prefix": rule_prefix,
            "Status": "Enabled",
        }
    ]
    lifecycle = {"Rules": rules}
    response = client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name, LifecycleConfiguration=lifecycle
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    key = rule_prefix + "foo"
    body = "bar"
    response = client.put_object(Bucket=bucket_name, Key=key, Body=body)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    return response


def check_lifecycle_expiration_header(response, start_time, rule_id, delta_days):
    expr_exists = "x-amz-expiration" in response["ResponseMetadata"]["HTTPHeaders"]
    if not expr_exists:
        return False
    expr_hdr = response["ResponseMetadata"]["HTTPHeaders"]["x-amz-expiration"]

    m = re.search(r'expiry-date="(.+)", rule-id="(.+)"', expr_hdr)

    expiration = dateutil.parser.parse(m.group(1))
    days_to_expire = (expiration.replace(tzinfo=None) - start_time).days == delta_days
    rule_eq_id = m.group(2) == rule_id

    return days_to_expire and rule_eq_id


@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
@pytest.mark.skip(reason="Not Implemented")
def test_lifecycle_expiration_header_put():
    bucket_name = get_new_bucket()
    client = get_client()

    now = datetime.datetime.now(None)
    response = setup_lifecycle_expiration(client, bucket_name, "rule1", 1, "days1/")
    assert check_lifecycle_expiration_header(response, now, "rule1", 1)


@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
@pytest.mark.fails_on_dbstore
def test_lifecycle_expiration_header_head():
    bucket_name = get_new_bucket()
    client = get_client()

    now = datetime.datetime.now(None)
    response = setup_lifecycle_expiration(client, bucket_name, "rule1", 1, "days1/")

    key = "days1/" + "foo"

    # stat the object, check header
    response = client.head_object(Bucket=bucket_name, Key=key)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert check_lifecycle_expiration_header(response, now, "rule1", 1)


@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
@pytest.mark.fails_on_dbstore
def test_lifecycle_expiration_header_tags_head():
    bucket_name = get_new_bucket()
    client = get_client()
    lifecycle = {
        "Rules": [
            {
                "Filter": {"Tag": {"Key": "key1", "Value": "tag1"}},
                "Status": "Enabled",
                "Expiration": {"Days": 1},
                "ID": "rule1",
            },
        ]
    }
    response = client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name, LifecycleConfiguration=lifecycle
    )
    key1 = "obj_key1"
    body1 = "obj_key1_body"
    tags1 = {
        "TagSet": [{"Key": "key1", "Value": "tag1"}, {"Key": "key5", "Value": "tag5"}]
    }
    response = client.put_object(Bucket=bucket_name, Key=key1, Body=body1)
    response = client.put_object_tagging(Bucket=bucket_name, Key=key1, Tagging=tags1)

    # stat the object, check header
    response = client.head_object(Bucket=bucket_name, Key=key1)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert check_lifecycle_expiration_header(
        response, datetime.datetime.now(None), "rule1", 1
    )

    # test that header is not returning when it should not
    lifecycle = {
        "Rules": [
            {
                "Filter": {"Tag": {"Key": "key2", "Value": "tag1"}},
                "Status": "Enabled",
                "Expiration": {"Days": 1},
                "ID": "rule1",
            },
        ]
    }
    response = client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name, LifecycleConfiguration=lifecycle
    )
    # stat the object, check header
    response = client.head_object(Bucket=bucket_name, Key=key1)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert (
        check_lifecycle_expiration_header(
            response, datetime.datetime.now(None), "rule1", 1
        )
        == False
    )


@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
@pytest.mark.fails_on_dbstore
def test_lifecycle_expiration_header_and_tags_head():
    now = datetime.datetime.now(None)
    bucket_name = get_new_bucket()
    client = get_client()
    lifecycle = {
        "Rules": [
            {
                "Filter": {
                    "And": {
                        "Tags": [
                            {"Key": "key1", "Value": "tag1"},
                            {"Key": "key5", "Value": "tag6"},
                        ]
                    }
                },
                "Status": "Enabled",
                "Expiration": {"Days": 1},
                "ID": "rule1",
            },
        ]
    }
    response = client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name, LifecycleConfiguration=lifecycle
    )
    key1 = "obj_key1"
    body1 = "obj_key1_body"
    tags1 = {
        "TagSet": [{"Key": "key1", "Value": "tag1"}, {"Key": "key5", "Value": "tag5"}]
    }
    response = client.put_object(Bucket=bucket_name, Key=key1, Body=body1)
    response = client.put_object_tagging(Bucket=bucket_name, Key=key1, Tagging=tags1)

    # stat the object, check header
    response = client.head_object(Bucket=bucket_name, Key=key1)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert (
        check_lifecycle_expiration_header(
            response, datetime.datetime.now(None), "rule1", 1
        )
        == False
    )


@pytest.mark.lifecycle
@pytest.mark.skip(reason="Not Implemented")
def test_lifecycle_set_noncurrent():
    bucket_name = _create_objects(keys=["past/foo", "future/bar"])
    client = get_client()
    rules = [
        {
            "ID": "rule1",
            "NoncurrentVersionExpiration": {"NoncurrentDays": 2},
            "Prefix": "past/",
            "Status": "Enabled",
        },
        {
            "ID": "rule2",
            "NoncurrentVersionExpiration": {"NoncurrentDays": 3},
            "Prefix": "future/",
            "Status": "Enabled",
        },
    ]
    lifecycle = {"Rules": rules}
    response = client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name, LifecycleConfiguration=lifecycle
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
def test_lifecycle_noncur_expiration():
    bucket_name = get_new_bucket()
    client = get_client()
    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")
    create_multiple_versions(client, bucket_name, "test1/a", 3)
    # not checking the object contents on the second run, because the function doesn't support multiple checks
    create_multiple_versions(client, bucket_name, "test2/abc", 3, check_versions=False)

    response = client.list_object_versions(Bucket=bucket_name)
    init_versions = response["Versions"]

    rules = [
        {
            "ID": "rule1",
            "NoncurrentVersionExpiration": {"NoncurrentDays": 2},
            "Prefix": "test1/",
            "Status": "Enabled",
        }
    ]
    lifecycle = {"Rules": rules}
    client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name, LifecycleConfiguration=lifecycle
    )

    lc_interval = get_lc_debug_interval()

    # Wait for first expiration (plus fudge to handle the timer window)
    time.sleep(5 * lc_interval)

    response = client.list_object_versions(Bucket=bucket_name)
    expire_versions = response["Versions"]
    assert len(init_versions) == 6
    assert len(expire_versions) == 4


@pytest.mark.lifecycle
@pytest.mark.skip(reason="Not Implemented")
def test_lifecycle_set_deletemarker():
    bucket_name = get_new_bucket()
    client = get_client()
    rules = [
        {
            "ID": "rule1",
            "Expiration": {"ExpiredObjectDeleteMarker": True},
            "Prefix": "test1/",
            "Status": "Enabled",
        }
    ]
    lifecycle = {"Rules": rules}
    response = client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name, LifecycleConfiguration=lifecycle
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


@pytest.mark.lifecycle
@pytest.mark.skip(reason="Not Implemented")
def test_lifecycle_set_filter():
    bucket_name = get_new_bucket()
    client = get_client()
    rules = [
        {
            "ID": "rule1",
            "Expiration": {"ExpiredObjectDeleteMarker": True},
            "Filter": {"Prefix": "foo"},
            "Status": "Enabled",
        }
    ]
    lifecycle = {"Rules": rules}
    response = client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name, LifecycleConfiguration=lifecycle
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


@pytest.mark.lifecycle
@pytest.mark.skip(reason="Not Implemented")
def test_lifecycle_set_empty_filter():
    bucket_name = get_new_bucket()
    client = get_client()
    rules = [
        {
            "ID": "rule1",
            "Expiration": {"ExpiredObjectDeleteMarker": True},
            "Filter": {},
            "Status": "Enabled",
        }
    ]
    lifecycle = {"Rules": rules}
    response = client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name, LifecycleConfiguration=lifecycle
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
def test_lifecycle_deletemarker_expiration():
    bucket_name = get_new_bucket()
    client = get_client()
    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")
    create_multiple_versions(client, bucket_name, "test1/a", 1)
    create_multiple_versions(client, bucket_name, "test2/abc", 1, check_versions=False)
    client.delete_object(Bucket=bucket_name, Key="test1/a")
    client.delete_object(Bucket=bucket_name, Key="test2/abc")

    response = client.list_object_versions(Bucket=bucket_name)
    init_versions = response["Versions"]
    deleted_versions = response["DeleteMarkers"]
    total_init_versions = init_versions + deleted_versions

    rules = [
        {
            "ID": "rule1",
            "NoncurrentVersionExpiration": {"NoncurrentDays": 1},
            "Expiration": {"ExpiredObjectDeleteMarker": True},
            "Prefix": "test1/",
            "Status": "Enabled",
        }
    ]
    lifecycle = {"Rules": rules}
    client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name, LifecycleConfiguration=lifecycle
    )

    lc_interval = get_lc_debug_interval()

    # Wait for first expiration (plus fudge to handle the timer window)
    time.sleep(7 * lc_interval)

    response = client.list_object_versions(Bucket=bucket_name)
    init_versions = response["Versions"]
    deleted_versions = response["DeleteMarkers"]
    total_expire_versions = init_versions + deleted_versions

    assert len(total_init_versions) == 4
    assert len(total_expire_versions) == 2


@pytest.mark.lifecycle
@pytest.mark.skip(reason="Not Implemented")
def test_lifecycle_set_multipart():
    bucket_name = get_new_bucket()
    client = get_client()
    rules = [
        {
            "ID": "rule1",
            "Prefix": "test1/",
            "Status": "Enabled",
            "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 2},
        },
        {
            "ID": "rule2",
            "Prefix": "test2/",
            "Status": "Disabled",
            "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 3},
        },
    ]
    lifecycle = {"Rules": rules}
    response = client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name, LifecycleConfiguration=lifecycle
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
def test_lifecycle_multipart_expiration():
    bucket_name = get_new_bucket()
    client = get_client()

    key_names = ["test1/a", "test2/"]
    upload_ids = []

    for key in key_names:
        response = client.create_multipart_upload(Bucket=bucket_name, Key=key)
        upload_ids.append(response["UploadId"])

    response = client.list_multipart_uploads(Bucket=bucket_name)
    init_uploads = response["Uploads"]

    rules = [
        {
            "ID": "rule1",
            "Prefix": "test1/",
            "Status": "Enabled",
            "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 2},
        },
    ]
    lifecycle = {"Rules": rules}
    response = client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name, LifecycleConfiguration=lifecycle
    )

    lc_interval = get_lc_debug_interval()

    # Wait for first expiration (plus fudge to handle the timer window)
    time.sleep(5 * lc_interval)

    response = client.list_multipart_uploads(Bucket=bucket_name)
    expired_uploads = response["Uploads"]
    assert len(init_uploads) == 2
    assert len(expired_uploads) == 1


@pytest.mark.lifecycle
@pytest.mark.skip(reason="Not Implemented")
def test_lifecycle_transition_set_invalid_date():
    bucket_name = get_new_bucket()
    client = get_client()
    rules = [
        {
            "ID": "rule1",
            "Expiration": {"Date": "2023-09-27"},
            "Transitions": [{"Date": "20220927", "StorageClass": "GLACIER"}],
            "Prefix": "test1/",
            "Status": "Enabled",
        }
    ]
    lifecycle = {"Rules": rules}
    e = assert_raises(
        ClientError,
        client.put_bucket_lifecycle_configuration,
        Bucket=bucket_name,
        LifecycleConfiguration=lifecycle,
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400


def _test_encryption_sse_customer_write(file_size):
    """
    Tests Create a file of A's, use it to set_contents_from_file.
    Create a file of B's, use it to re-set_contents_from_file.
    Re-read the contents, and confirm we get B's
    """
    bucket_name = get_new_bucket()
    client = get_client()
    key = "testobj"
    data = "A" * file_size
    sse_client_headers = {
        "x-amz-server-side-encryption-customer-algorithm": "AES256",
        "x-amz-server-side-encryption-customer-key": "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
        "x-amz-server-side-encryption-customer-key-md5": "DWygnHRtgiJ77HCm+1rvHw==",
    }

    lf = lambda **kwargs: kwargs["params"]["headers"].update(sse_client_headers)
    client.meta.events.register("before-call.s3.PutObject", lf)
    client.put_object(Bucket=bucket_name, Key=key, Body=data)

    lf = lambda **kwargs: kwargs["params"]["headers"].update(sse_client_headers)
    client.meta.events.register("before-call.s3.GetObject", lf)
    response = client.get_object(Bucket=bucket_name, Key=key)
    body = _get_body(response)
    assert body == data


# The test harness for lifecycle is configured to treat days as 10 second intervals.
@pytest.mark.lifecycle
@pytest.mark.lifecycle_transition
@pytest.mark.fails_on_aws
def test_lifecycle_transition():
    sc = configured_storage_classes()
    if len(sc) < 3:
        pytest.skip("requires 3 or more storage classes")

    bucket_name = _create_objects(
        keys=[
            "expire1/foo",
            "expire1/bar",
            "keep2/foo",
            "keep2/bar",
            "expire3/foo",
            "expire3/bar",
        ]
    )
    client = get_client()
    rules = [
        {
            "ID": "rule1",
            "Transitions": [{"Days": 1, "StorageClass": sc[1]}],
            "Prefix": "expire1/",
            "Status": "Enabled",
        },
        {
            "ID": "rule2",
            "Transitions": [{"Days": 6, "StorageClass": sc[2]}],
            "Prefix": "expire3/",
            "Status": "Enabled",
        },
    ]
    lifecycle = {"Rules": rules}
    client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name, LifecycleConfiguration=lifecycle
    )

    # Get list of all keys
    response = client.list_objects(Bucket=bucket_name)
    init_keys = _get_keys(response)
    assert len(init_keys) == 6

    lc_interval = get_lc_debug_interval()

    # Wait for first expiration (plus fudge to handle the timer window)
    time.sleep(4 * lc_interval)
    expire1_keys = list_bucket_storage_class(client, bucket_name)
    assert len(expire1_keys["STANDARD"]) == 4
    assert len(expire1_keys[sc[1]]) == 2
    assert len(expire1_keys[sc[2]]) == 0

    # Wait for next expiration cycle
    time.sleep(lc_interval)
    keep2_keys = list_bucket_storage_class(client, bucket_name)
    assert len(keep2_keys["STANDARD"]) == 4
    assert len(keep2_keys[sc[1]]) == 2
    assert len(keep2_keys[sc[2]]) == 0

    # Wait for final expiration cycle
    time.sleep(5 * lc_interval)
    expire3_keys = list_bucket_storage_class(client, bucket_name)
    assert len(expire3_keys["STANDARD"]) == 2
    assert len(expire3_keys[sc[1]]) == 2
    assert len(expire3_keys[sc[2]]) == 2


# The test harness for lifecycle is configured to treat days as 10 second intervals.
@pytest.mark.lifecycle
@pytest.mark.lifecycle_transition
@pytest.mark.fails_on_aws
def test_lifecycle_transition_single_rule_multi_trans():
    sc = configured_storage_classes()
    if len(sc) < 3:
        pytest.skip("requires 3 or more storage classes")

    bucket_name = _create_objects(
        keys=[
            "expire1/foo",
            "expire1/bar",
            "keep2/foo",
            "keep2/bar",
            "expire3/foo",
            "expire3/bar",
        ]
    )
    client = get_client()
    rules = [
        {
            "ID": "rule1",
            "Transitions": [
                {"Days": 1, "StorageClass": sc[1]},
                {"Days": 7, "StorageClass": sc[2]},
            ],
            "Prefix": "expire1/",
            "Status": "Enabled",
        }
    ]
    lifecycle = {"Rules": rules}
    client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name, LifecycleConfiguration=lifecycle
    )

    # Get list of all keys
    response = client.list_objects(Bucket=bucket_name)
    init_keys = _get_keys(response)
    assert len(init_keys) == 6

    lc_interval = get_lc_debug_interval()

    # Wait for first expiration (plus fudge to handle the timer window)
    time.sleep(5 * lc_interval)
    expire1_keys = list_bucket_storage_class(client, bucket_name)
    assert len(expire1_keys["STANDARD"]) == 4
    assert len(expire1_keys[sc[1]]) == 2
    assert len(expire1_keys[sc[2]]) == 0

    # Wait for next expiration cycle
    time.sleep(lc_interval)
    keep2_keys = list_bucket_storage_class(client, bucket_name)
    assert len(keep2_keys["STANDARD"]) == 4
    assert len(keep2_keys[sc[1]]) == 2
    assert len(keep2_keys[sc[2]]) == 0

    # Wait for final expiration cycle
    time.sleep(6 * lc_interval)
    expire3_keys = list_bucket_storage_class(client, bucket_name)
    assert len(expire3_keys["STANDARD"]) == 4
    assert len(expire3_keys[sc[1]]) == 0
    assert len(expire3_keys[sc[2]]) == 2


@pytest.mark.lifecycle
@pytest.mark.lifecycle_transition
def test_lifecycle_set_noncurrent_transition():
    sc = configured_storage_classes()
    if len(sc) < 3:
        pytest.skip("requires 3 or more storage classes")

    bucket = get_new_bucket()
    client = get_client()
    rules = [
        {
            "ID": "rule1",
            "Prefix": "test1/",
            "Status": "Enabled",
            "NoncurrentVersionTransitions": [
                {"NoncurrentDays": 2, "StorageClass": sc[1]},
                {"NoncurrentDays": 4, "StorageClass": sc[2]},
            ],
            "NoncurrentVersionExpiration": {"NoncurrentDays": 6},
        },
        {
            "ID": "rule2",
            "Prefix": "test2/",
            "Status": "Disabled",
            "NoncurrentVersionExpiration": {"NoncurrentDays": 3},
        },
    ]
    lifecycle = {"Rules": rules}
    response = client.put_bucket_lifecycle_configuration(
        Bucket=bucket, LifecycleConfiguration=lifecycle
    )

    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
@pytest.mark.lifecycle_transition
@pytest.mark.fails_on_aws
def test_lifecycle_noncur_transition():
    sc = configured_storage_classes()
    if len(sc) < 3:
        pytest.skip("requires 3 or more storage classes")

    bucket = get_new_bucket()
    client = get_client()
    check_configure_versioning_retry(bucket, "Enabled", "Enabled")

    rules = [
        {
            "ID": "rule1",
            "Prefix": "test1/",
            "Status": "Enabled",
            "NoncurrentVersionTransitions": [
                {"NoncurrentDays": 1, "StorageClass": sc[1]},
                {"NoncurrentDays": 5, "StorageClass": sc[2]},
            ],
            "NoncurrentVersionExpiration": {"NoncurrentDays": 9},
        }
    ]
    lifecycle = {"Rules": rules}
    response = client.put_bucket_lifecycle_configuration(
        Bucket=bucket, LifecycleConfiguration=lifecycle
    )

    create_multiple_versions(client, bucket, "test1/a", 3)
    create_multiple_versions(client, bucket, "test1/b", 3)

    init_keys = list_bucket_storage_class(client, bucket)
    assert len(init_keys["STANDARD"]) == 6

    lc_interval = get_lc_debug_interval()

    time.sleep(4 * lc_interval)
    expire1_keys = list_bucket_storage_class(client, bucket)
    assert len(expire1_keys["STANDARD"]) == 2
    assert len(expire1_keys[sc[1]]) == 4
    assert len(expire1_keys[sc[2]]) == 0

    time.sleep(4 * lc_interval)
    expire1_keys = list_bucket_storage_class(client, bucket)
    assert len(expire1_keys["STANDARD"]) == 2
    assert len(expire1_keys[sc[1]]) == 0
    assert len(expire1_keys[sc[2]]) == 4

    time.sleep(6 * lc_interval)
    expire1_keys = list_bucket_storage_class(client, bucket)
    assert len(expire1_keys["STANDARD"]) == 2
    assert len(expire1_keys[sc[1]]) == 0
    assert len(expire1_keys[sc[2]]) == 0


@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
@pytest.mark.lifecycle_transition
def test_lifecycle_plain_null_version_current_transition():
    sc = configured_storage_classes()
    if len(sc) < 2:
        pytest.skip("requires 2 or more storage classes")

    target_sc = sc[1]
    assert target_sc != "STANDARD"

    bucket = get_new_bucket()
    check_versioning(bucket, None)

    # create a plain object before enabling versioning;
    # this will be transitioned as a current version
    client = get_client()
    key = "testobjfoo"
    content = "fooz"
    client.put_object(Bucket=bucket, Key=key, Body=content)

    check_configure_versioning_retry(bucket, "Enabled", "Enabled")

    client.put_bucket_lifecycle_configuration(
        Bucket=bucket,
        LifecycleConfiguration={
            "Rules": [
                {
                    "ID": "rule1",
                    "Prefix": "testobj",
                    "Status": "Enabled",
                    "Transitions": [
                        {"Days": 1, "StorageClass": target_sc},
                    ],
                }
            ]
        },
    )

    lc_interval = get_lc_debug_interval()
    time.sleep(4 * lc_interval)

    keys = list_bucket_storage_class(client, bucket)
    assert len(keys["STANDARD"]) == 0
    assert len(keys[target_sc]) == 1


def verify_object(client, bucket, key, content=None, sc=None):
    response = client.get_object(Bucket=bucket, Key=key)

    if sc == None:
        sc = "STANDARD"

    if "StorageClass" in response:
        assert response["StorageClass"] == sc
    else:  # storage class should be STANDARD
        assert "STANDARD" == sc

    if content != None:
        body = _get_body(response)
        assert body == content


# The test harness for lifecycle is configured to treat days as 10 second intervals.
@pytest.mark.lifecycle
@pytest.mark.lifecycle_transition
@pytest.mark.cloud_transition
@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
def test_lifecycle_cloud_transition():
    cloud_sc = get_cloud_storage_class()
    if cloud_sc == None:
        pytest.skip("no cloud_storage_class configured")

    retain_head_object = get_cloud_retain_head_object()
    target_path = get_cloud_target_path()
    target_sc = get_cloud_target_storage_class()

    keys = ["expire1/foo", "expire1/bar", "keep2/foo", "keep2/bar"]
    bucket_name = _create_objects(keys=keys)
    client = get_client()
    rules = [
        {
            "ID": "rule1",
            "Transitions": [{"Days": 1, "StorageClass": cloud_sc}],
            "Prefix": "expire1/",
            "Status": "Enabled",
        }
    ]
    lifecycle = {"Rules": rules}
    client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name, LifecycleConfiguration=lifecycle
    )

    # Get list of all keys
    response = client.list_objects(Bucket=bucket_name)
    init_keys = _get_keys(response)
    assert len(init_keys) == 4

    lc_interval = get_lc_debug_interval()

    # Wait for first expiration (plus fudge to handle the timer window)
    time.sleep(10 * lc_interval)
    expire1_keys = list_bucket_storage_class(client, bucket_name)
    assert len(expire1_keys["STANDARD"]) == 2

    if retain_head_object != None and retain_head_object == "true":
        assert len(expire1_keys[cloud_sc]) == 2
    else:
        assert len(expire1_keys[cloud_sc]) == 0

    time.sleep(2 * lc_interval)
    # Check if objects copied to target path
    if target_path == None:
        target_path = "rgwx-default-" + cloud_sc.lower() + "-cloud-bucket"
    prefix = bucket_name + "/"

    cloud_client = get_cloud_client()

    time.sleep(12 * lc_interval)
    expire1_key1_str = prefix + keys[0]
    verify_object(cloud_client, target_path, expire1_key1_str, keys[0], target_sc)

    expire1_key2_str = prefix + keys[1]
    verify_object(cloud_client, target_path, expire1_key2_str, keys[1], target_sc)

    # Now verify the object on source rgw
    src_key = keys[0]
    if retain_head_object != None and retain_head_object == "true":
        # verify HEAD response
        response = client.head_object(Bucket=bucket_name, Key=keys[0])
        assert 0 == response["ContentLength"]
        assert cloud_sc == response["StorageClass"]

        # GET should return InvalidObjectState error
        e = assert_raises(
            ClientError, client.get_object, Bucket=bucket_name, Key=src_key
        )
        status, error_code = _get_status_and_error_code(e.response)
        assert status == 403
        assert error_code == "InvalidObjectState"

        # COPY of object should return InvalidObjectState error
        copy_source = {"Bucket": bucket_name, "Key": src_key}
        e = assert_raises(
            ClientError,
            client.copy,
            CopySource=copy_source,
            Bucket=bucket_name,
            Key="copy_obj",
        )
        status, error_code = _get_status_and_error_code(e.response)
        assert status == 403
        assert error_code == "InvalidObjectState"

        # DELETE should succeed
        response = client.delete_object(Bucket=bucket_name, Key=src_key)
        e = assert_raises(
            ClientError, client.get_object, Bucket=bucket_name, Key=src_key
        )
        status, error_code = _get_status_and_error_code(e.response)
        assert status == 404
        assert error_code == "NoSuchKey"


# Similar to 'test_lifecycle_transition' but for cloud transition
@pytest.mark.lifecycle
@pytest.mark.lifecycle_transition
@pytest.mark.cloud_transition
@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
def test_lifecycle_cloud_multiple_transition():
    cloud_sc = get_cloud_storage_class()
    if cloud_sc == None:
        pytest.skip("[s3 cloud] section missing cloud_storage_class")

    retain_head_object = get_cloud_retain_head_object()
    target_path = get_cloud_target_path()
    target_sc = get_cloud_target_storage_class()

    sc1 = get_cloud_regular_storage_class()

    if sc1 == None:
        pytest.skip("[s3 cloud] section missing storage_class")

    sc = ["STANDARD", sc1, cloud_sc]

    keys = ["expire1/foo", "expire1/bar", "keep2/foo", "keep2/bar"]
    bucket_name = _create_objects(keys=keys)
    client = get_client()
    rules = [
        {
            "ID": "rule1",
            "Transitions": [{"Days": 1, "StorageClass": sc1}],
            "Prefix": "expire1/",
            "Status": "Enabled",
        },
        {
            "ID": "rule2",
            "Transitions": [{"Days": 5, "StorageClass": cloud_sc}],
            "Prefix": "expire1/",
            "Status": "Enabled",
        },
        {
            "ID": "rule3",
            "Expiration": {"Days": 9},
            "Prefix": "expire1/",
            "Status": "Enabled",
        },
    ]
    lifecycle = {"Rules": rules}
    client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name, LifecycleConfiguration=lifecycle
    )

    # Get list of all keys
    response = client.list_objects(Bucket=bucket_name)
    init_keys = _get_keys(response)
    assert len(init_keys) == 4

    lc_interval = get_lc_debug_interval()

    # Wait for first expiration (plus fudge to handle the timer window)
    time.sleep(4 * lc_interval)
    expire1_keys = list_bucket_storage_class(client, bucket_name)
    assert len(expire1_keys["STANDARD"]) == 2
    assert len(expire1_keys[sc[1]]) == 2
    assert len(expire1_keys[sc[2]]) == 0

    # Wait for next expiration cycle
    time.sleep(7 * lc_interval)
    expire1_keys = list_bucket_storage_class(client, bucket_name)
    assert len(expire1_keys["STANDARD"]) == 2
    assert len(expire1_keys[sc[1]]) == 0

    if retain_head_object != None and retain_head_object == "true":
        assert len(expire1_keys[sc[2]]) == 2
    else:
        assert len(expire1_keys[sc[2]]) == 0

    # Wait for final expiration cycle
    time.sleep(12 * lc_interval)
    expire3_keys = list_bucket_storage_class(client, bucket_name)
    assert len(expire3_keys["STANDARD"]) == 2
    assert len(expire3_keys[sc[1]]) == 0
    assert len(expire3_keys[sc[2]]) == 0


# Noncurrent objects for cloud transition
@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
@pytest.mark.lifecycle_transition
@pytest.mark.cloud_transition
@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
def test_lifecycle_noncur_cloud_transition():
    cloud_sc = get_cloud_storage_class()
    if cloud_sc == None:
        pytest.skip("[s3 cloud] section missing cloud_storage_class")

    retain_head_object = get_cloud_retain_head_object()
    target_path = get_cloud_target_path()
    target_sc = get_cloud_target_storage_class()

    sc1 = get_cloud_regular_storage_class()
    if sc1 == None:
        pytest.skip("[s3 cloud] section missing storage_class")

    sc = ["STANDARD", sc1, cloud_sc]

    bucket = get_new_bucket()
    client = get_client()
    check_configure_versioning_retry(bucket, "Enabled", "Enabled")

    rules = [
        {
            "ID": "rule1",
            "Prefix": "test1/",
            "Status": "Enabled",
            "NoncurrentVersionTransitions": [
                {"NoncurrentDays": 1, "StorageClass": sc[1]},
                {"NoncurrentDays": 5, "StorageClass": sc[2]},
            ],
        }
    ]
    lifecycle = {"Rules": rules}
    response = client.put_bucket_lifecycle_configuration(
        Bucket=bucket, LifecycleConfiguration=lifecycle
    )

    keys = ["test1/a", "test1/b"]

    for k in keys:
        create_multiple_versions(client, bucket, k, 3)

    init_keys = list_bucket_storage_class(client, bucket)
    assert len(init_keys["STANDARD"]) == 6

    response = client.list_object_versions(Bucket=bucket)

    lc_interval = get_lc_debug_interval()

    time.sleep(4 * lc_interval)
    expire1_keys = list_bucket_storage_class(client, bucket)
    assert len(expire1_keys["STANDARD"]) == 2
    assert len(expire1_keys[sc[1]]) == 4
    assert len(expire1_keys[sc[2]]) == 0

    time.sleep(10 * lc_interval)
    expire1_keys = list_bucket_storage_class(client, bucket)
    assert len(expire1_keys["STANDARD"]) == 2
    assert len(expire1_keys[sc[1]]) == 0

    if retain_head_object == None or retain_head_object == "false":
        assert len(expire1_keys[sc[2]]) == 0
    else:
        assert len(expire1_keys[sc[2]]) == 4

    # check if versioned object exists on cloud endpoint
    if target_path == None:
        target_path = "rgwx-default-" + cloud_sc.lower() + "-cloud-bucket"
    prefix = bucket + "/"

    cloud_client = get_cloud_client()

    time.sleep(lc_interval)
    result = list_bucket_versions(client, bucket)

    for src_key in keys:
        for k in result[src_key]:
            expire1_key1_str = prefix + "test1/a" + "-" + k["VersionId"]
            verify_object(cloud_client, target_path, expire1_key1_str, None, target_sc)


# The test harness for lifecycle is configured to treat days as 10 second intervals.
@pytest.mark.lifecycle
@pytest.mark.lifecycle_transition
@pytest.mark.cloud_transition
@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
def test_lifecycle_cloud_transition_large_obj():
    cloud_sc = get_cloud_storage_class()
    if cloud_sc == None:
        pytest.skip("[s3 cloud] section missing cloud_storage_class")

    retain_head_object = get_cloud_retain_head_object()
    target_path = get_cloud_target_path()
    target_sc = get_cloud_target_storage_class()

    bucket = get_new_bucket()
    client = get_client()
    rules = [
        {
            "ID": "rule1",
            "Transitions": [{"Days": 1, "StorageClass": cloud_sc}],
            "Prefix": "expire1/",
            "Status": "Enabled",
        }
    ]

    keys = ["keep/multi", "expire1/multi"]
    size = 9 * 1024 * 1024
    data = "A" * size

    for k in keys:
        client.put_object(Bucket=bucket, Body=data, Key=k)
        verify_object(client, bucket, k, data)

    lifecycle = {"Rules": rules}
    response = client.put_bucket_lifecycle_configuration(
        Bucket=bucket, LifecycleConfiguration=lifecycle
    )

    lc_interval = get_lc_debug_interval()

    # Wait for first expiration (plus fudge to handle the timer window)
    time.sleep(8 * lc_interval)
    expire1_keys = list_bucket_storage_class(client, bucket)
    assert len(expire1_keys["STANDARD"]) == 1

    if retain_head_object != None and retain_head_object == "true":
        assert len(expire1_keys[cloud_sc]) == 1
    else:
        assert len(expire1_keys[cloud_sc]) == 0

    # Check if objects copied to target path
    if target_path == None:
        target_path = "rgwx-default-" + cloud_sc.lower() + "-cloud-bucket"
    prefix = bucket + "/"

    # multipart upload takes time
    time.sleep(12 * lc_interval)
    cloud_client = get_cloud_client()

    expire1_key1_str = prefix + keys[1]
    verify_object(cloud_client, target_path, expire1_key1_str, data, target_sc)


@pytest.mark.encryption
@pytest.mark.fails_on_dbstore
def test_encrypted_transfer_1b():
    _test_encryption_sse_customer_write(1)


@pytest.mark.encryption
@pytest.mark.fails_on_dbstore
def test_encrypted_transfer_1kb():
    _test_encryption_sse_customer_write(1024)


@pytest.mark.encryption
@pytest.mark.fails_on_dbstore
def test_encrypted_transfer_1MB():
    _test_encryption_sse_customer_write(1024 * 1024)


@pytest.mark.encryption
@pytest.mark.fails_on_dbstore
def test_encrypted_transfer_13b():
    _test_encryption_sse_customer_write(13)


@pytest.mark.encryption
def test_encryption_sse_c_method_head():
    bucket_name = get_new_bucket()
    client = get_client()
    data = "A" * 1000
    key = "testobj"
    sse_client_headers = {
        "x-amz-server-side-encryption-customer-algorithm": "AES256",
        "x-amz-server-side-encryption-customer-key": "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
        "x-amz-server-side-encryption-customer-key-md5": "DWygnHRtgiJ77HCm+1rvHw==",
    }

    lf = lambda **kwargs: kwargs["params"]["headers"].update(sse_client_headers)
    client.meta.events.register("before-call.s3.PutObject", lf)
    client.put_object(Bucket=bucket_name, Key=key, Body=data)

    e = assert_raises(ClientError, client.head_object, Bucket=bucket_name, Key=key)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400

    lf = lambda **kwargs: kwargs["params"]["headers"].update(sse_client_headers)
    client.meta.events.register("before-call.s3.HeadObject", lf)
    response = client.head_object(Bucket=bucket_name, Key=key)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


@pytest.mark.encryption
def test_encryption_sse_c_present():
    bucket_name = get_new_bucket()
    client = get_client()
    data = "A" * 1000
    key = "testobj"
    sse_client_headers = {
        "x-amz-server-side-encryption-customer-algorithm": "AES256",
        "x-amz-server-side-encryption-customer-key": "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
        "x-amz-server-side-encryption-customer-key-md5": "DWygnHRtgiJ77HCm+1rvHw==",
    }

    lf = lambda **kwargs: kwargs["params"]["headers"].update(sse_client_headers)
    client.meta.events.register("before-call.s3.PutObject", lf)
    client.put_object(Bucket=bucket_name, Key=key, Body=data)

    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key=key)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400


@pytest.mark.encryption
def test_encryption_sse_c_other_key():
    bucket_name = get_new_bucket()
    client = get_client()
    data = "A" * 100
    key = "testobj"
    sse_client_headers_A = {
        "x-amz-server-side-encryption-customer-algorithm": "AES256",
        "x-amz-server-side-encryption-customer-key": "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
        "x-amz-server-side-encryption-customer-key-md5": "DWygnHRtgiJ77HCm+1rvHw==",
    }
    sse_client_headers_B = {
        "x-amz-server-side-encryption-customer-algorithm": "AES256",
        "x-amz-server-side-encryption-customer-key": "6b+WOZ1T3cqZMxgThRcXAQBrS5mXKdDUphvpxptl9/4=",
        "x-amz-server-side-encryption-customer-key-md5": "arxBvwY2V4SiOne6yppVPQ==",
    }

    lf = lambda **kwargs: kwargs["params"]["headers"].update(sse_client_headers_A)
    client.meta.events.register("before-call.s3.PutObject", lf)
    client.put_object(Bucket=bucket_name, Key=key, Body=data)

    lf = lambda **kwargs: kwargs["params"]["headers"].update(sse_client_headers_B)
    client.meta.events.register("before-call.s3.GetObject", lf)
    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key=key)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400


@pytest.mark.encryption
def test_encryption_sse_c_invalid_md5():
    bucket_name = get_new_bucket()
    client = get_client()
    data = "A" * 100
    key = "testobj"
    sse_client_headers = {
        "x-amz-server-side-encryption-customer-algorithm": "AES256",
        "x-amz-server-side-encryption-customer-key": "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
        "x-amz-server-side-encryption-customer-key-md5": "AAAAAAAAAAAAAAAAAAAAAA==",
    }

    lf = lambda **kwargs: kwargs["params"]["headers"].update(sse_client_headers)
    client.meta.events.register("before-call.s3.PutObject", lf)
    e = assert_raises(
        ClientError, client.put_object, Bucket=bucket_name, Key=key, Body=data
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400


@pytest.mark.encryption
def test_encryption_sse_c_no_md5():
    bucket_name = get_new_bucket()
    client = get_client()
    data = "A" * 100
    key = "testobj"
    sse_client_headers = {
        "x-amz-server-side-encryption-customer-algorithm": "AES256",
        "x-amz-server-side-encryption-customer-key": "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
    }

    lf = lambda **kwargs: kwargs["params"]["headers"].update(sse_client_headers)
    client.meta.events.register("before-call.s3.PutObject", lf)
    e = assert_raises(
        ClientError, client.put_object, Bucket=bucket_name, Key=key, Body=data
    )


@pytest.mark.encryption
def test_encryption_sse_c_no_key():
    bucket_name = get_new_bucket()
    client = get_client()
    data = "A" * 100
    key = "testobj"
    sse_client_headers = {
        "x-amz-server-side-encryption-customer-algorithm": "AES256",
    }

    lf = lambda **kwargs: kwargs["params"]["headers"].update(sse_client_headers)
    client.meta.events.register("before-call.s3.PutObject", lf)
    e = assert_raises(
        ClientError, client.put_object, Bucket=bucket_name, Key=key, Body=data
    )


@pytest.mark.encryption
def test_encryption_key_no_sse_c():
    bucket_name = get_new_bucket()
    client = get_client()
    data = "A" * 100
    key = "testobj"
    sse_client_headers = {
        "x-amz-server-side-encryption-customer-key": "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
        "x-amz-server-side-encryption-customer-key-md5": "DWygnHRtgiJ77HCm+1rvHw==",
    }

    lf = lambda **kwargs: kwargs["params"]["headers"].update(sse_client_headers)
    client.meta.events.register("before-call.s3.PutObject", lf)
    e = assert_raises(
        ClientError, client.put_object, Bucket=bucket_name, Key=key, Body=data
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400


def _multipart_upload_enc(
    client,
    bucket_name,
    key,
    size,
    part_size,
    init_headers,
    part_headers,
    metadata,
    resend_parts,
):
    """
    generate a multi-part upload for a random file of specifed size,
    if requested, generate a list of the parts
    return the upload descriptor
    """
    if client == None:
        client = get_client()

    lf = lambda **kwargs: kwargs["params"]["headers"].update(init_headers)
    client.meta.events.register("before-call.s3.CreateMultipartUpload", lf)
    if metadata == None:
        response = client.create_multipart_upload(Bucket=bucket_name, Key=key)
    else:
        response = client.create_multipart_upload(
            Bucket=bucket_name, Key=key, Metadata=metadata
        )

    upload_id = response["UploadId"]
    s = ""
    parts = []
    for i, part in enumerate(generate_random(size, part_size)):
        # part_num is necessary because PartNumber for upload_part and in parts must start at 1 and i starts at 0
        part_num = i + 1
        s += part
        lf = lambda **kwargs: kwargs["params"]["headers"].update(part_headers)
        client.meta.events.register("before-call.s3.UploadPart", lf)
        response = client.upload_part(
            UploadId=upload_id,
            Bucket=bucket_name,
            Key=key,
            PartNumber=part_num,
            Body=part,
        )
        parts.append({"ETag": response["ETag"].strip('"'), "PartNumber": part_num})
        if i in resend_parts:
            lf = lambda **kwargs: kwargs["params"]["headers"].update(part_headers)
            client.meta.events.register("before-call.s3.UploadPart", lf)
            client.upload_part(
                UploadId=upload_id,
                Bucket=bucket_name,
                Key=key,
                PartNumber=part_num,
                Body=part,
            )

    return (upload_id, s, parts)


def _check_content_using_range_enc(
    client, bucket_name, key, data, size, step, enc_headers=None
):
    for ofs in range(0, size, step):
        toread = size - ofs
        if toread > step:
            toread = step
        end = ofs + toread - 1
        lf = lambda **kwargs: kwargs["params"]["headers"].update(enc_headers)
        client.meta.events.register("before-call.s3.GetObject", lf)
        r = "bytes={s}-{e}".format(s=ofs, e=end)
        response = client.get_object(Bucket=bucket_name, Key=key, Range=r)
        read_range = response["ContentLength"]
        body = _get_body(response)
        assert read_range == toread
        assert body == data[ofs : end + 1]


@pytest.mark.encryption
@pytest.mark.fails_on_dbstore
def test_encryption_sse_c_multipart_upload():
    bucket_name = get_new_bucket()
    client = get_client()
    key = "multipart_enc"
    content_type = "text/plain"
    objlen = 30 * 1024 * 1024
    partlen = 5 * 1024 * 1024
    metadata = {"foo": "bar"}
    enc_headers = {
        "x-amz-server-side-encryption-customer-algorithm": "AES256",
        "x-amz-server-side-encryption-customer-key": "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
        "x-amz-server-side-encryption-customer-key-md5": "DWygnHRtgiJ77HCm+1rvHw==",
        "Content-Type": content_type,
    }
    resend_parts = []

    (upload_id, data, parts) = _multipart_upload_enc(
        client,
        bucket_name,
        key,
        objlen,
        part_size=partlen,
        init_headers=enc_headers,
        part_headers=enc_headers,
        metadata=metadata,
        resend_parts=resend_parts,
    )

    lf = lambda **kwargs: kwargs["params"]["headers"].update(enc_headers)
    client.meta.events.register("before-call.s3.CompleteMultipartUpload", lf)
    client.complete_multipart_upload(
        Bucket=bucket_name,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )

    response = client.list_objects_v2(Bucket=bucket_name, Prefix=key)
    assert len(response["Contents"]) == 1
    assert response["Contents"][0]["Size"] == objlen

    lf = lambda **kwargs: kwargs["params"]["headers"].update(enc_headers)
    client.meta.events.register("before-call.s3.GetObject", lf)
    response = client.get_object(Bucket=bucket_name, Key=key)

    assert response["Metadata"] == metadata
    assert response["ResponseMetadata"]["HTTPHeaders"]["content-type"] == content_type

    body = _get_body(response)
    assert body == data
    size = response["ContentLength"]
    assert len(body) == size

    _check_content_using_range_enc(
        client, bucket_name, key, data, size, 1000000, enc_headers=enc_headers
    )
    _check_content_using_range_enc(
        client, bucket_name, key, data, size, 10000000, enc_headers=enc_headers
    )
    for i in range(-1, 2):
        _check_content_using_range_enc(
            client, bucket_name, key, data, size, partlen + i, enc_headers=enc_headers
        )


@pytest.mark.encryption
@pytest.mark.fails_on_dbstore
def test_encryption_sse_c_unaligned_multipart_upload():
    bucket_name = get_new_bucket()
    client = get_client()
    key = "multipart_enc"
    content_type = "text/plain"
    objlen = 30 * 1024 * 1024
    partlen = 1 + 5 * 1024 * 1024  # not a multiple of the 4k encryption block size
    metadata = {"foo": "bar"}
    enc_headers = {
        "x-amz-server-side-encryption-customer-algorithm": "AES256",
        "x-amz-server-side-encryption-customer-key": "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
        "x-amz-server-side-encryption-customer-key-md5": "DWygnHRtgiJ77HCm+1rvHw==",
        "Content-Type": content_type,
    }
    resend_parts = []

    (upload_id, data, parts) = _multipart_upload_enc(
        client,
        bucket_name,
        key,
        objlen,
        part_size=partlen,
        init_headers=enc_headers,
        part_headers=enc_headers,
        metadata=metadata,
        resend_parts=resend_parts,
    )

    lf = lambda **kwargs: kwargs["params"]["headers"].update(enc_headers)
    client.meta.events.register("before-call.s3.CompleteMultipartUpload", lf)
    client.complete_multipart_upload(
        Bucket=bucket_name,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )

    response = client.list_objects_v2(Bucket=bucket_name, Prefix=key)
    assert len(response["Contents"]) == 1
    assert response["Contents"][0]["Size"] == objlen

    lf = lambda **kwargs: kwargs["params"]["headers"].update(enc_headers)
    client.meta.events.register("before-call.s3.GetObject", lf)
    response = client.get_object(Bucket=bucket_name, Key=key)

    assert response["Metadata"] == metadata
    assert response["ResponseMetadata"]["HTTPHeaders"]["content-type"] == content_type

    body = _get_body(response)
    assert body == data
    size = response["ContentLength"]
    assert len(body) == size

    _check_content_using_range_enc(
        client, bucket_name, key, data, size, 1000000, enc_headers=enc_headers
    )
    _check_content_using_range_enc(
        client, bucket_name, key, data, size, 10000000, enc_headers=enc_headers
    )
    for i in range(-1, 2):
        _check_content_using_range_enc(
            client, bucket_name, key, data, size, partlen + i, enc_headers=enc_headers
        )


@pytest.mark.encryption
# TODO: remove this fails_on_rgw when I fix it
@pytest.mark.fails_on_rgw
def test_encryption_sse_c_multipart_invalid_chunks_1():
    bucket_name = get_new_bucket()
    client = get_client()
    key = "multipart_enc"
    content_type = "text/plain"
    objlen = 30 * 1024 * 1024
    metadata = {"foo": "bar"}
    init_headers = {
        "x-amz-server-side-encryption-customer-algorithm": "AES256",
        "x-amz-server-side-encryption-customer-key": "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
        "x-amz-server-side-encryption-customer-key-md5": "DWygnHRtgiJ77HCm+1rvHw==",
        "Content-Type": content_type,
    }
    part_headers = {
        "x-amz-server-side-encryption-customer-algorithm": "AES256",
        "x-amz-server-side-encryption-customer-key": "6b+WOZ1T3cqZMxgThRcXAQBrS5mXKdDUphvpxptl9/4=",
        "x-amz-server-side-encryption-customer-key-md5": "arxBvwY2V4SiOne6yppVPQ==",
    }
    resend_parts = []

    e = assert_raises(
        ClientError,
        _multipart_upload_enc,
        client=client,
        bucket_name=bucket_name,
        key=key,
        size=objlen,
        part_size=5 * 1024 * 1024,
        init_headers=init_headers,
        part_headers=part_headers,
        metadata=metadata,
        resend_parts=resend_parts,
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400


@pytest.mark.encryption
# TODO: remove this fails_on_rgw when I fix it
@pytest.mark.fails_on_rgw
def test_encryption_sse_c_multipart_invalid_chunks_2():
    bucket_name = get_new_bucket()
    client = get_client()
    key = "multipart_enc"
    content_type = "text/plain"
    objlen = 30 * 1024 * 1024
    metadata = {"foo": "bar"}
    init_headers = {
        "x-amz-server-side-encryption-customer-algorithm": "AES256",
        "x-amz-server-side-encryption-customer-key": "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
        "x-amz-server-side-encryption-customer-key-md5": "DWygnHRtgiJ77HCm+1rvHw==",
        "Content-Type": content_type,
    }
    part_headers = {
        "x-amz-server-side-encryption-customer-algorithm": "AES256",
        "x-amz-server-side-encryption-customer-key": "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
        "x-amz-server-side-encryption-customer-key-md5": "AAAAAAAAAAAAAAAAAAAAAA==",
    }
    resend_parts = []

    e = assert_raises(
        ClientError,
        _multipart_upload_enc,
        client=client,
        bucket_name=bucket_name,
        key=key,
        size=objlen,
        part_size=5 * 1024 * 1024,
        init_headers=init_headers,
        part_headers=part_headers,
        metadata=metadata,
        resend_parts=resend_parts,
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400


@pytest.mark.encryption
@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/843")
def test_encryption_sse_c_multipart_bad_download():
    bucket_name = get_new_bucket()
    client = get_client()
    key = "multipart_enc"
    content_type = "text/plain"
    objlen = 30 * 1024 * 1024
    metadata = {"foo": "bar"}
    put_headers = {
        "x-amz-server-side-encryption-customer-algorithm": "AES256",
        "x-amz-server-side-encryption-customer-key": "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
        "x-amz-server-side-encryption-customer-key-md5": "DWygnHRtgiJ77HCm+1rvHw==",
        "Content-Type": content_type,
    }
    get_headers = {
        "x-amz-server-side-encryption-customer-algorithm": "AES256",
        "x-amz-server-side-encryption-customer-key": "6b+WOZ1T3cqZMxgThRcXAQBrS5mXKdDUphvpxptl9/4=",
        "x-amz-server-side-encryption-customer-key-md5": "arxBvwY2V4SiOne6yppVPQ==",
    }
    resend_parts = []

    (upload_id, data, parts) = _multipart_upload_enc(
        client,
        bucket_name,
        key,
        objlen,
        part_size=5 * 1024 * 1024,
        init_headers=put_headers,
        part_headers=put_headers,
        metadata=metadata,
        resend_parts=resend_parts,
    )

    lf = lambda **kwargs: kwargs["params"]["headers"].update(put_headers)
    client.meta.events.register("before-call.s3.CompleteMultipartUpload", lf)
    client.complete_multipart_upload(
        Bucket=bucket_name,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )

    response = client.list_objects_v2(Bucket=bucket_name, Prefix=key)
    assert len(response["Contents"]) == 1
    assert response["Contents"][0]["Size"] == objlen

    lf = lambda **kwargs: kwargs["params"]["headers"].update(put_headers)
    client.meta.events.register("before-call.s3.GetObject", lf)
    response = client.get_object(Bucket=bucket_name, Key=key)

    assert response["Metadata"] == metadata
    assert response["ResponseMetadata"]["HTTPHeaders"]["content-type"] == content_type

    lf = lambda **kwargs: kwargs["params"]["headers"].update(get_headers)
    client.meta.events.register("before-call.s3.GetObject", lf)
    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key=key)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400


@pytest.mark.encryption
@pytest.mark.fails_on_dbstore
def test_encryption_sse_c_post_object_authenticated_request():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {
        "expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "conditions": [
            {"bucket": bucket_name},
            ["starts-with", "$key", "foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["starts-with", "$x-amz-server-side-encryption-customer-algorithm", ""],
            ["starts-with", "$x-amz-server-side-encryption-customer-key", ""],
            ["starts-with", "$x-amz-server-side-encryption-customer-key-md5", ""],
            ["content-length-range", 0, 1024],
        ],
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, "utf-8")
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(
        hmac.new(bytes(aws_secret_access_key, "utf-8"), policy, hashlib.sha1).digest()
    )

    payload = OrderedDict(
        [
            ("key", "foo.txt"),
            ("AWSAccessKeyId", aws_access_key_id),
            ("acl", "private"),
            ("signature", signature),
            ("policy", policy),
            ("Content-Type", "text/plain"),
            ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
            (
                "x-amz-server-side-encryption-customer-key",
                "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
            ),
            (
                "x-amz-server-side-encryption-customer-key-md5",
                "DWygnHRtgiJ77HCm+1rvHw==",
            ),
            ("file", ("bar")),
        ]
    )

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 204

    get_headers = {
        "x-amz-server-side-encryption-customer-algorithm": "AES256",
        "x-amz-server-side-encryption-customer-key": "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
        "x-amz-server-side-encryption-customer-key-md5": "DWygnHRtgiJ77HCm+1rvHw==",
    }
    lf = lambda **kwargs: kwargs["params"]["headers"].update(get_headers)
    client.meta.events.register("before-call.s3.GetObject", lf)
    response = client.get_object(Bucket=bucket_name, Key="foo.txt")
    body = _get_body(response)
    assert body == "bar"


@pytest.mark.encryption
@pytest.mark.fails_on_dbstore
def _test_sse_kms_customer_write(file_size, key_id="testkey-1"):
    """
    Tests Create a file of A's, use it to set_contents_from_file.
    Create a file of B's, use it to re-set_contents_from_file.
    Re-read the contents, and confirm we get B's
    """
    bucket_name = get_new_bucket()
    client = get_client()
    sse_kms_client_headers = {
        "x-amz-server-side-encryption": "aws:kms",
        "x-amz-server-side-encryption-aws-kms-key-id": key_id,
    }
    data = "A" * file_size

    lf = lambda **kwargs: kwargs["params"]["headers"].update(sse_kms_client_headers)
    client.meta.events.register("before-call.s3.PutObject", lf)
    client.put_object(Bucket=bucket_name, Key="testobj", Body=data)

    response = client.get_object(Bucket=bucket_name, Key="testobj")
    body = _get_body(response)
    assert body == data


@pytest.mark.encryption
@pytest.mark.fails_on_dbstore
def test_sse_kms_method_head():
    kms_keyid = get_main_kms_keyid()
    bucket_name = get_new_bucket()
    client = get_client()
    sse_kms_client_headers = {
        "x-amz-server-side-encryption": "aws:kms",
        "x-amz-server-side-encryption-aws-kms-key-id": kms_keyid,
    }
    data = "A" * 1000
    key = "testobj"

    lf = lambda **kwargs: kwargs["params"]["headers"].update(sse_kms_client_headers)
    client.meta.events.register("before-call.s3.PutObject", lf)
    client.put_object(Bucket=bucket_name, Key=key, Body=data)

    response = client.head_object(Bucket=bucket_name, Key=key)
    assert (
        response["ResponseMetadata"]["HTTPHeaders"]["x-amz-server-side-encryption"]
        == "aws:kms"
    )
    assert (
        response["ResponseMetadata"]["HTTPHeaders"][
            "x-amz-server-side-encryption-aws-kms-key-id"
        ]
        == kms_keyid
    )

    lf = lambda **kwargs: kwargs["params"]["headers"].update(sse_kms_client_headers)
    client.meta.events.register("before-call.s3.HeadObject", lf)
    e = assert_raises(ClientError, client.head_object, Bucket=bucket_name, Key=key)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400


@pytest.mark.encryption
@pytest.mark.fails_on_dbstore
def test_sse_kms_present():
    kms_keyid = get_main_kms_keyid()
    bucket_name = get_new_bucket()
    client = get_client()
    sse_kms_client_headers = {
        "x-amz-server-side-encryption": "aws:kms",
        "x-amz-server-side-encryption-aws-kms-key-id": kms_keyid,
    }
    data = "A" * 100
    key = "testobj"

    lf = lambda **kwargs: kwargs["params"]["headers"].update(sse_kms_client_headers)
    client.meta.events.register("before-call.s3.PutObject", lf)
    client.put_object(Bucket=bucket_name, Key=key, Body=data)

    response = client.get_object(Bucket=bucket_name, Key=key)
    body = _get_body(response)
    assert body == data


@pytest.mark.encryption
@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/878")
def test_sse_kms_no_key():
    bucket_name = get_new_bucket()
    client = get_client()
    sse_kms_client_headers = {
        "x-amz-server-side-encryption": "aws:kms",
    }
    data = "A" * 100
    key = "testobj"

    lf = lambda **kwargs: kwargs["params"]["headers"].update(sse_kms_client_headers)
    client.meta.events.register("before-call.s3.PutObject", lf)

    e = assert_raises(
        ClientError, client.put_object, Bucket=bucket_name, Key=key, Body=data
    )


@pytest.mark.encryption
@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/878")
def test_sse_kms_not_declared():
    bucket_name = get_new_bucket()
    client = get_client()
    sse_kms_client_headers = {
        "x-amz-server-side-encryption-aws-kms-key-id": "testkey-2"
    }
    data = "A" * 100
    key = "testobj"

    lf = lambda **kwargs: kwargs["params"]["headers"].update(sse_kms_client_headers)
    client.meta.events.register("before-call.s3.PutObject", lf)

    e = assert_raises(
        ClientError, client.put_object, Bucket=bucket_name, Key=key, Body=data
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400


@pytest.mark.encryption
@pytest.mark.fails_on_dbstore
def test_sse_kms_multipart_upload():
    kms_keyid = get_main_kms_keyid()
    bucket_name = get_new_bucket()
    client = get_client()
    key = "multipart_enc"
    content_type = "text/plain"
    objlen = 30 * 1024 * 1024
    metadata = {"foo": "bar"}
    enc_headers = {
        "x-amz-server-side-encryption": "aws:kms",
        "x-amz-server-side-encryption-aws-kms-key-id": kms_keyid,
        "Content-Type": content_type,
    }
    resend_parts = []

    (upload_id, data, parts) = _multipart_upload_enc(
        client,
        bucket_name,
        key,
        objlen,
        part_size=5 * 1024 * 1024,
        init_headers=enc_headers,
        part_headers=enc_headers,
        metadata=metadata,
        resend_parts=resend_parts,
    )

    lf = lambda **kwargs: kwargs["params"]["headers"].update(enc_headers)
    client.meta.events.register("before-call.s3.CompleteMultipartUpload", lf)
    client.complete_multipart_upload(
        Bucket=bucket_name,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )

    response = client.list_objects_v2(Bucket=bucket_name, Prefix=key)
    assert len(response["Contents"]) == 1
    assert response["Contents"][0]["Size"] == objlen

    lf = lambda **kwargs: kwargs["params"]["headers"].update(enc_headers)
    client.meta.events.register("before-call.s3.UploadPart", lf)

    response = client.get_object(Bucket=bucket_name, Key=key)

    assert response["Metadata"] == metadata
    assert response["ResponseMetadata"]["HTTPHeaders"]["content-type"] == content_type

    body = _get_body(response)
    assert body == data
    size = response["ContentLength"]
    assert len(body) == size

    _check_content_using_range(key, bucket_name, data, 1000000)
    _check_content_using_range(key, bucket_name, data, 10000000)


@pytest.mark.encryption
@pytest.mark.fails_on_dbstore
def test_sse_kms_multipart_invalid_chunks_1():
    kms_keyid = get_main_kms_keyid()
    kms_keyid2 = get_secondary_kms_keyid()
    bucket_name = get_new_bucket()
    client = get_client()
    key = "multipart_enc"
    content_type = "text/bla"
    objlen = 30 * 1024 * 1024
    metadata = {"foo": "bar"}
    init_headers = {
        "x-amz-server-side-encryption": "aws:kms",
        "x-amz-server-side-encryption-aws-kms-key-id": kms_keyid,
        "Content-Type": content_type,
    }
    part_headers = {
        "x-amz-server-side-encryption": "aws:kms",
        "x-amz-server-side-encryption-aws-kms-key-id": kms_keyid2,
    }
    resend_parts = []

    _multipart_upload_enc(
        client,
        bucket_name,
        key,
        objlen,
        part_size=5 * 1024 * 1024,
        init_headers=init_headers,
        part_headers=part_headers,
        metadata=metadata,
        resend_parts=resend_parts,
    )


@pytest.mark.encryption
@pytest.mark.fails_on_dbstore
def test_sse_kms_multipart_invalid_chunks_2():
    kms_keyid = get_main_kms_keyid()
    bucket_name = get_new_bucket()
    client = get_client()
    key = "multipart_enc"
    content_type = "text/plain"
    objlen = 30 * 1024 * 1024
    metadata = {"foo": "bar"}
    init_headers = {
        "x-amz-server-side-encryption": "aws:kms",
        "x-amz-server-side-encryption-aws-kms-key-id": kms_keyid,
        "Content-Type": content_type,
    }
    part_headers = {
        "x-amz-server-side-encryption": "aws:kms",
        "x-amz-server-side-encryption-aws-kms-key-id": "testkey-not-present",
    }
    resend_parts = []

    _multipart_upload_enc(
        client,
        bucket_name,
        key,
        objlen,
        part_size=5 * 1024 * 1024,
        init_headers=init_headers,
        part_headers=part_headers,
        metadata=metadata,
        resend_parts=resend_parts,
    )


@pytest.mark.encryption
@pytest.mark.fails_on_dbstore
def test_sse_kms_post_object_authenticated_request():
    kms_keyid = get_main_kms_keyid()
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {
        "expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "conditions": [
            {"bucket": bucket_name},
            ["starts-with", "$key", "foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["starts-with", "$x-amz-server-side-encryption", ""],
            ["starts-with", "$x-amz-server-side-encryption-aws-kms-key-id", ""],
            ["content-length-range", 0, 1024],
        ],
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, "utf-8")
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(
        hmac.new(bytes(aws_secret_access_key, "utf-8"), policy, hashlib.sha1).digest()
    )

    payload = OrderedDict(
        [
            ("key", "foo.txt"),
            ("AWSAccessKeyId", aws_access_key_id),
            ("acl", "private"),
            ("signature", signature),
            ("policy", policy),
            ("Content-Type", "text/plain"),
            ("x-amz-server-side-encryption", "aws:kms"),
            ("x-amz-server-side-encryption-aws-kms-key-id", kms_keyid),
            ("file", ("bar")),
        ]
    )

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 204

    response = client.get_object(Bucket=bucket_name, Key="foo.txt")
    body = _get_body(response)
    assert body == "bar"


@pytest.mark.encryption
@pytest.mark.fails_on_dbstore
def test_sse_kms_transfer_1b():
    kms_keyid = get_main_kms_keyid()
    if kms_keyid is None:
        pytest.skip("[s3 main] section missing kms_keyid")
    _test_sse_kms_customer_write(1, key_id=kms_keyid)


@pytest.mark.encryption
@pytest.mark.fails_on_dbstore
def test_sse_kms_transfer_1kb():
    kms_keyid = get_main_kms_keyid()
    if kms_keyid is None:
        pytest.skip("[s3 main] section missing kms_keyid")
    _test_sse_kms_customer_write(1024, key_id=kms_keyid)


@pytest.mark.encryption
@pytest.mark.fails_on_dbstore
def test_sse_kms_transfer_1MB():
    kms_keyid = get_main_kms_keyid()
    if kms_keyid is None:
        pytest.skip("[s3 main] section missing kms_keyid")
    _test_sse_kms_customer_write(1024 * 1024, key_id=kms_keyid)


@pytest.mark.encryption
@pytest.mark.fails_on_dbstore
def test_sse_kms_transfer_13b():
    kms_keyid = get_main_kms_keyid()
    if kms_keyid is None:
        pytest.skip("[s3 main] section missing kms_keyid")
    _test_sse_kms_customer_write(13, key_id=kms_keyid)


@pytest.mark.encryption
@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/878")
def test_sse_kms_read_declare():
    bucket_name = get_new_bucket()
    client = get_client()
    sse_kms_client_headers = {
        "x-amz-server-side-encryption": "aws:kms",
        "x-amz-server-side-encryption-aws-kms-key-id": "testkey-1",
    }
    data = "A" * 100
    key = "testobj"

    client.put_object(Bucket=bucket_name, Key=key, Body=data)
    lf = lambda **kwargs: kwargs["params"]["headers"].update(sse_kms_client_headers)
    client.meta.events.register("before-call.s3.GetObject", lf)

    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key=key)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400


@pytest.mark.bucket_policy
@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/863")
def test_bucket_policy():
    bucket_name = get_new_bucket()
    client = get_client()
    key = "asdf"
    client.put_object(Bucket=bucket_name, Key=key, Body="asdf")

    resource1 = "arn:aws:s3:::" + bucket_name
    resource2 = "arn:aws:s3:::" + bucket_name + "/*"
    policy_document = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": "*"},
                    "Action": "s3:ListBucket",
                    "Resource": ["{}".format(resource1), "{}".format(resource2)],
                }
            ],
        }
    )

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

    alt_client = get_alt_client()
    response = alt_client.list_objects(Bucket=bucket_name)
    assert len(response["Contents"]) == 1


@pytest.mark.bucket_policy
@pytest.mark.list_objects_v2
@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/863")
def test_bucketv2_policy():
    bucket_name = get_new_bucket()
    client = get_client()
    key = "asdf"
    client.put_object(Bucket=bucket_name, Key=key, Body="asdf")

    resource1 = "arn:aws:s3:::" + bucket_name
    resource2 = "arn:aws:s3:::" + bucket_name + "/*"
    policy_document = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": "*"},
                    "Action": "s3:ListBucket",
                    "Resource": ["{}".format(resource1), "{}".format(resource2)],
                }
            ],
        }
    )

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

    alt_client = get_alt_client()
    response = alt_client.list_objects_v2(Bucket=bucket_name)
    assert len(response["Contents"]) == 1


@pytest.mark.bucket_policy
@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/863")
def test_bucket_policy_acl():
    bucket_name = get_new_bucket()
    client = get_client()
    key = "asdf"
    client.put_object(Bucket=bucket_name, Key=key, Body="asdf")

    resource1 = "arn:aws:s3:::" + bucket_name
    resource2 = "arn:aws:s3:::" + bucket_name + "/*"
    policy_document = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Deny",
                    "Principal": {"AWS": "*"},
                    "Action": "s3:ListBucket",
                    "Resource": ["{}".format(resource1), "{}".format(resource2)],
                }
            ],
        }
    )

    client.put_bucket_acl(Bucket=bucket_name, ACL="authenticated-read")
    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

    alt_client = get_alt_client()
    e = assert_raises(ClientError, alt_client.list_objects, Bucket=bucket_name)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == "AccessDenied"

    client.delete_bucket_policy(Bucket=bucket_name)
    client.put_bucket_acl(Bucket=bucket_name, ACL="public-read")


@pytest.mark.bucket_policy
@pytest.mark.list_objects_v2
@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/863")
def test_bucketv2_policy_acl():
    bucket_name = get_new_bucket()
    client = get_client()
    key = "asdf"
    client.put_object(Bucket=bucket_name, Key=key, Body="asdf")

    resource1 = "arn:aws:s3:::" + bucket_name
    resource2 = "arn:aws:s3:::" + bucket_name + "/*"
    policy_document = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Deny",
                    "Principal": {"AWS": "*"},
                    "Action": "s3:ListBucket",
                    "Resource": ["{}".format(resource1), "{}".format(resource2)],
                }
            ],
        }
    )

    client.put_bucket_acl(Bucket=bucket_name, ACL="authenticated-read")
    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

    alt_client = get_alt_client()
    e = assert_raises(ClientError, alt_client.list_objects_v2, Bucket=bucket_name)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == "AccessDenied"

    client.delete_bucket_policy(Bucket=bucket_name)
    client.put_bucket_acl(Bucket=bucket_name, ACL="public-read")


@pytest.mark.bucket_policy
# TODO: remove this fails_on_rgw when I fix it
@pytest.mark.fails_on_rgw
@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/863")
def test_bucket_policy_different_tenant():
    bucket_name = get_new_bucket()
    client = get_client()
    key = "asdf"
    client.put_object(Bucket=bucket_name, Key=key, Body="asdf")

    resource1 = "arn:aws:s3::*:" + bucket_name
    resource2 = "arn:aws:s3::*:" + bucket_name + "/*"
    policy_document = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": "*"},
                    "Action": "s3:ListBucket",
                    "Resource": ["{}".format(resource1), "{}".format(resource2)],
                }
            ],
        }
    )

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

    # TODO: figure out how to change the bucketname
    def change_bucket_name(**kwargs):
        kwargs["params"][
            "url"
        ] = "http://localhost:8000/:{bucket_name}?encoding-type=url".format(
            bucket_name=bucket_name
        )
        kwargs["params"]["url_path"] = "/:{bucket_name}".format(bucket_name=bucket_name)
        kwargs["params"]["context"]["signing"]["bucket"] = ":{bucket_name}".format(
            bucket_name=bucket_name
        )
        print(kwargs["request_signer"])
        print(kwargs)

    # bucket_name = ":" + bucket_name
    tenant_client = get_tenant_client()
    tenant_client.meta.events.register("before-call.s3.ListObjects", change_bucket_name)
    response = tenant_client.list_objects(Bucket=bucket_name)
    # alt_client = get_alt_client()
    # response = alt_client.list_objects(Bucket=bucket_name)

    assert len(response["Contents"]) == 1


@pytest.mark.bucket_policy
# TODO: remove this fails_on_rgw when I fix it
@pytest.mark.fails_on_rgw
@pytest.mark.list_objects_v2
@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/863")
def test_bucketv2_policy_different_tenant():
    bucket_name = get_new_bucket()
    client = get_client()
    key = "asdf"
    client.put_object(Bucket=bucket_name, Key=key, Body="asdf")

    resource1 = "arn:aws:s3::*:" + bucket_name
    resource2 = "arn:aws:s3::*:" + bucket_name + "/*"
    policy_document = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": "*"},
                    "Action": "s3:ListBucket",
                    "Resource": ["{}".format(resource1), "{}".format(resource2)],
                }
            ],
        }
    )

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

    # TODO: figure out how to change the bucketname
    def change_bucket_name(**kwargs):
        kwargs["params"][
            "url"
        ] = "http://localhost:8000/:{bucket_name}?encoding-type=url".format(
            bucket_name=bucket_name
        )
        kwargs["params"]["url_path"] = "/:{bucket_name}".format(bucket_name=bucket_name)
        kwargs["params"]["context"]["signing"]["bucket"] = ":{bucket_name}".format(
            bucket_name=bucket_name
        )
        print(kwargs["request_signer"])
        print(kwargs)

    # bucket_name = ":" + bucket_name
    tenant_client = get_tenant_client()
    tenant_client.meta.events.register("before-call.s3.ListObjects", change_bucket_name)
    response = tenant_client.list_objects_v2(Bucket=bucket_name)
    # alt_client = get_alt_client()
    # response = alt_client.list_objects_v2(Bucket=bucket_name)

    assert len(response["Contents"]) == 1


@pytest.mark.bucket_policy
@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/863")
def test_bucket_policy_another_bucket():
    bucket_name = get_new_bucket()
    bucket_name2 = get_new_bucket()
    client = get_client()
    key = "asdf"
    key2 = "abcd"
    client.put_object(Bucket=bucket_name, Key=key, Body="asdf")
    client.put_object(Bucket=bucket_name2, Key=key2, Body="abcd")
    policy_document = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": "*"},
                    "Action": "s3:ListBucket",
                    "Resource": ["arn:aws:s3:::*", "arn:aws:s3:::*/*"],
                }
            ],
        }
    )

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)
    response = client.get_bucket_policy(Bucket=bucket_name)
    response_policy = response["Policy"]

    client.put_bucket_policy(Bucket=bucket_name2, Policy=response_policy)

    alt_client = get_alt_client()
    response = alt_client.list_objects(Bucket=bucket_name)
    assert len(response["Contents"]) == 1

    alt_client = get_alt_client()
    response = alt_client.list_objects(Bucket=bucket_name2)
    assert len(response["Contents"]) == 1


@pytest.mark.bucket_policy
@pytest.mark.list_objects_v2
@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/863")
def test_bucketv2_policy_another_bucket():
    bucket_name = get_new_bucket()
    bucket_name2 = get_new_bucket()
    client = get_client()
    key = "asdf"
    key2 = "abcd"
    client.put_object(Bucket=bucket_name, Key=key, Body="asdf")
    client.put_object(Bucket=bucket_name2, Key=key2, Body="abcd")
    policy_document = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": "*"},
                    "Action": "s3:ListBucket",
                    "Resource": ["arn:aws:s3:::*", "arn:aws:s3:::*/*"],
                }
            ],
        }
    )

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)
    response = client.get_bucket_policy(Bucket=bucket_name)
    response_policy = response["Policy"]

    client.put_bucket_policy(Bucket=bucket_name2, Policy=response_policy)

    alt_client = get_alt_client()
    response = alt_client.list_objects_v2(Bucket=bucket_name)
    assert len(response["Contents"]) == 1

    alt_client = get_alt_client()
    response = alt_client.list_objects_v2(Bucket=bucket_name2)
    assert len(response["Contents"]) == 1


@pytest.mark.bucket_policy
# TODO: remove this fails_on_rgw when I fix it
@pytest.mark.fails_on_rgw
@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/863")
def test_bucket_policy_set_condition_operator_end_with_IfExists():
    bucket_name = get_new_bucket()
    client = get_client()
    key = "foo"
    client.put_object(Bucket=bucket_name, Key=key)
    policy = (
        """{
      "Version":"2012-10-17",
      "Statement": [{
        "Sid": "Allow Public Access to All Objects",
        "Effect": "Allow",
        "Principal": "*",
        "Action": "s3:GetObject",
        "Condition": {
                    "StringLikeIfExists": {
                        "aws:Referer": "http://www.example.com/*"
                    }
                },
        "Resource": "arn:aws:s3:::%s/*"
      }
     ]
    }"""
        % bucket_name
    )
    # boto3.set_stream_logger(name='botocore')
    client.put_bucket_policy(Bucket=bucket_name, Policy=policy)

    request_headers = {"referer": "http://www.example.com/"}

    lf = lambda **kwargs: kwargs["params"]["headers"].update(request_headers)
    client.meta.events.register("before-call.s3.GetObject", lf)

    response = client.get_object(Bucket=bucket_name, Key=key)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    request_headers = {"referer": "http://www.example.com/index.html"}

    lf = lambda **kwargs: kwargs["params"]["headers"].update(request_headers)
    client.meta.events.register("before-call.s3.GetObject", lf)

    response = client.get_object(Bucket=bucket_name, Key=key)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    # the 'referer' headers need to be removed for this one
    # response = client.get_object(Bucket=bucket_name, Key=key)
    # assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    request_headers = {"referer": "http://example.com"}

    lf = lambda **kwargs: kwargs["params"]["headers"].update(request_headers)
    client.meta.events.register("before-call.s3.GetObject", lf)

    # TODO: Compare Requests sent in Boto3, Wireshark, RGW Log for both boto and boto3
    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key=key)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403

    response = client.get_bucket_policy(Bucket=bucket_name)
    print(response)


def _create_simple_tagset(count):
    tagset = []
    for i in range(count):
        tagset.append({"Key": str(i), "Value": str(i)})

    return {"TagSet": tagset}


def _make_random_string(size):
    return "".join(random.choice(string.ascii_letters) for _ in range(size))


@pytest.mark.tagging
@pytest.mark.fails_on_dbstore
def test_get_obj_tagging():
    key = "testputtags"
    bucket_name = _create_key_with_random_content(key)
    client = get_client()

    input_tagset = _create_simple_tagset(2)
    response = client.put_object_tagging(
        Bucket=bucket_name, Key=key, Tagging=input_tagset
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    response = client.get_object_tagging(Bucket=bucket_name, Key=key)
    assert response["TagSet"] == input_tagset["TagSet"]


@pytest.mark.tagging
def test_get_obj_head_tagging():
    key = "testputtags"
    bucket_name = _create_key_with_random_content(key)
    client = get_client()
    count = 2

    input_tagset = _create_simple_tagset(count)
    response = client.put_object_tagging(
        Bucket=bucket_name, Key=key, Tagging=input_tagset
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    response = client.head_object(Bucket=bucket_name, Key=key)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert response["ResponseMetadata"]["HTTPHeaders"]["x-amz-tagging-count"] == str(
        count
    )


@pytest.mark.tagging
@pytest.mark.fails_on_dbstore
def test_put_max_tags():
    key = "testputmaxtags"
    bucket_name = _create_key_with_random_content(key)
    client = get_client()

    input_tagset = _create_simple_tagset(10)
    response = client.put_object_tagging(
        Bucket=bucket_name, Key=key, Tagging=input_tagset
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    response = client.get_object_tagging(Bucket=bucket_name, Key=key)
    assert response["TagSet"] == input_tagset["TagSet"]


@pytest.mark.tagging
def test_put_excess_tags():
    key = "testputmaxtags"
    bucket_name = _create_key_with_random_content(key)
    client = get_client()

    input_tagset = _create_simple_tagset(11)
    e = assert_raises(
        ClientError,
        client.put_object_tagging,
        Bucket=bucket_name,
        Key=key,
        Tagging=input_tagset,
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == "BadRequest"

    response = client.get_object_tagging(Bucket=bucket_name, Key=key)
    assert len(response["TagSet"]) == 0


@pytest.mark.tagging
def test_put_max_kvsize_tags():
    key = "testputmaxkeysize"
    bucket_name = _create_key_with_random_content(key)
    client = get_client()

    tagset = []
    for i in range(10):
        k = _make_random_string(128)
        v = _make_random_string(256)
        tagset.append({"Key": k, "Value": v})

    input_tagset = {"TagSet": tagset}

    response = client.put_object_tagging(
        Bucket=bucket_name, Key=key, Tagging=input_tagset
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    response = client.get_object_tagging(Bucket=bucket_name, Key=key)
    for kv_pair in response["TagSet"]:
        assert kv_pair in input_tagset["TagSet"]


@pytest.mark.tagging
def test_put_excess_key_tags():
    key = "testputexcesskeytags"
    bucket_name = _create_key_with_random_content(key)
    client = get_client()

    tagset = []
    for i in range(10):
        k = _make_random_string(129)
        v = _make_random_string(256)
        tagset.append({"Key": k, "Value": v})

    input_tagset = {"TagSet": tagset}

    e = assert_raises(
        ClientError,
        client.put_object_tagging,
        Bucket=bucket_name,
        Key=key,
        Tagging=input_tagset,
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == "InvalidTag"

    response = client.get_object_tagging(Bucket=bucket_name, Key=key)
    assert len(response["TagSet"]) == 0


@pytest.mark.tagging
def test_put_excess_val_tags():
    key = "testputexcesskeytags"
    bucket_name = _create_key_with_random_content(key)
    client = get_client()

    tagset = []
    for i in range(10):
        k = _make_random_string(128)
        v = _make_random_string(257)
        tagset.append({"Key": k, "Value": v})

    input_tagset = {"TagSet": tagset}

    e = assert_raises(
        ClientError,
        client.put_object_tagging,
        Bucket=bucket_name,
        Key=key,
        Tagging=input_tagset,
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == "InvalidTag"

    response = client.get_object_tagging(Bucket=bucket_name, Key=key)
    assert len(response["TagSet"]) == 0


@pytest.mark.tagging
@pytest.mark.fails_on_dbstore
def test_put_modify_tags():
    key = "testputmodifytags"
    bucket_name = _create_key_with_random_content(key)
    client = get_client()

    tagset = []
    tagset.append({"Key": "key", "Value": "val"})
    tagset.append({"Key": "key2", "Value": "val2"})

    input_tagset = {"TagSet": tagset}

    response = client.put_object_tagging(
        Bucket=bucket_name, Key=key, Tagging=input_tagset
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    response = client.get_object_tagging(Bucket=bucket_name, Key=key)
    assert response["TagSet"] == input_tagset["TagSet"]

    tagset2 = []
    tagset2.append({"Key": "key3", "Value": "val3"})

    input_tagset2 = {"TagSet": tagset2}

    response = client.put_object_tagging(
        Bucket=bucket_name, Key=key, Tagging=input_tagset2
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    response = client.get_object_tagging(Bucket=bucket_name, Key=key)
    assert response["TagSet"] == input_tagset2["TagSet"]


@pytest.mark.tagging
@pytest.mark.fails_on_dbstore
def test_put_delete_tags():
    key = "testputmodifytags"
    bucket_name = _create_key_with_random_content(key)
    client = get_client()

    input_tagset = _create_simple_tagset(2)
    response = client.put_object_tagging(
        Bucket=bucket_name, Key=key, Tagging=input_tagset
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    response = client.get_object_tagging(Bucket=bucket_name, Key=key)
    assert response["TagSet"] == input_tagset["TagSet"]

    response = client.delete_object_tagging(Bucket=bucket_name, Key=key)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 204

    response = client.get_object_tagging(Bucket=bucket_name, Key=key)
    assert len(response["TagSet"]) == 0


@pytest.mark.tagging
@pytest.mark.fails_on_dbstore
def test_post_object_tags_anonymous_request():
    bucket_name = get_new_bucket_name()
    client = get_client()
    url = _get_post_url(bucket_name)
    client.create_bucket(ACL="public-read-write", Bucket=bucket_name)

    key_name = "foo.txt"
    input_tagset = _create_simple_tagset(2)
    # xml_input_tagset is the same as input_tagset in xml.
    # There is not a simple way to change input_tagset to xml like there is in the boto2 tetss
    xml_input_tagset = "<Tagging><TagSet><Tag><Key>0</Key><Value>0</Value></Tag><Tag><Key>1</Key><Value>1</Value></Tag></TagSet></Tagging>"

    payload = OrderedDict(
        [
            ("key", key_name),
            ("acl", "public-read"),
            ("Content-Type", "text/plain"),
            ("tagging", xml_input_tagset),
            ("file", ("bar")),
        ]
    )

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 204
    response = client.get_object(Bucket=bucket_name, Key=key_name)
    body = _get_body(response)
    assert body == "bar"

    response = client.get_object_tagging(Bucket=bucket_name, Key=key_name)
    assert response["TagSet"] == input_tagset["TagSet"]


@pytest.mark.tagging
@pytest.mark.skip(reason="https://github.com/nspcc-dev/s3-tests/issues/46")
def test_post_object_tags_authenticated_request():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {
        "expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "conditions": [
            {"bucket": bucket_name},
            ["starts-with", "$key", "foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["content-length-range", 0, 1024],
            ["starts-with", "$tagging", ""],
        ],
    }

    # xml_input_tagset is the same as `input_tagset = _create_simple_tagset(2)` in xml
    # There is not a simple way to change input_tagset to xml like there is in the boto2 tetss
    xml_input_tagset = "<Tagging><TagSet><Tag><Key>0</Key><Value>0</Value></Tag><Tag><Key>1</Key><Value>1</Value></Tag></TagSet></Tagging>"

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, "utf-8")
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(
        hmac.new(bytes(aws_secret_access_key, "utf-8"), policy, hashlib.sha1).digest()
    )

    payload = OrderedDict(
        [
            ("key", "foo.txt"),
            ("AWSAccessKeyId", aws_access_key_id),
            ("acl", "private"),
            ("signature", signature),
            ("policy", policy),
            ("tagging", xml_input_tagset),
            ("Content-Type", "text/plain"),
            ("file", ("bar")),
        ]
    )

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 204
    response = client.get_object(Bucket=bucket_name, Key="foo.txt")
    body = _get_body(response)
    assert body == "bar"


@pytest.mark.tagging
@pytest.mark.fails_on_dbstore
def test_put_obj_with_tags():
    bucket_name = get_new_bucket()
    client = get_client()
    key = "testtagobj1"
    data = "A" * 100

    tagset = []
    tagset.append({"Key": "bar", "Value": ""})
    tagset.append({"Key": "foo", "Value": "bar"})

    put_obj_tag_headers = {"x-amz-tagging": "foo=bar&bar"}

    lf = lambda **kwargs: kwargs["params"]["headers"].update(put_obj_tag_headers)
    client.meta.events.register("before-call.s3.PutObject", lf)

    client.put_object(Bucket=bucket_name, Key=key, Body=data)
    response = client.get_object(Bucket=bucket_name, Key=key)
    body = _get_body(response)
    assert body == data

    response = client.get_object_tagging(Bucket=bucket_name, Key=key)
    response_tagset = response["TagSet"]
    tagset = tagset
    assert response_tagset == tagset


def _make_arn_resource(path="*"):
    return "arn:aws:s3:::{}".format(path)


@pytest.mark.tagging
@pytest.mark.bucket_policy
@pytest.mark.fails_on_dbstore
def test_get_tags_acl_public():
    key = "testputtagsacl"
    bucket_name = _create_key_with_random_content(key)
    client = get_client()

    resource = _make_arn_resource("{}/{}".format(bucket_name, key))
    policy_document = make_json_policy("s3:GetObjectTagging", resource)

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

    input_tagset = _create_simple_tagset(10)
    response = client.put_object_tagging(
        Bucket=bucket_name, Key=key, Tagging=input_tagset
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    alt_client = get_alt_client()

    response = alt_client.get_object_tagging(Bucket=bucket_name, Key=key)
    assert response["TagSet"] == input_tagset["TagSet"]


@pytest.mark.tagging
@pytest.mark.bucket_policy
@pytest.mark.fails_on_dbstore
def test_put_tags_acl_public():
    key = "testputtagsacl"
    bucket_name = _create_key_with_random_content(key)
    client = get_client()

    resource = _make_arn_resource("{}/{}".format(bucket_name, key))
    policy_document = make_json_policy("s3:PutObjectTagging", resource)

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

    input_tagset = _create_simple_tagset(10)
    alt_client = get_alt_client()
    response = alt_client.put_object_tagging(
        Bucket=bucket_name, Key=key, Tagging=input_tagset
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    response = client.get_object_tagging(Bucket=bucket_name, Key=key)
    assert response["TagSet"] == input_tagset["TagSet"]


@pytest.mark.tagging
@pytest.mark.bucket_policy
@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/863")
def test_delete_tags_obj_public():
    key = "testputtagsacl"
    bucket_name = _create_key_with_random_content(key)
    client = get_client()

    resource = _make_arn_resource("{}/{}".format(bucket_name, key))
    policy_document = make_json_policy("s3:DeleteObjectTagging", resource)

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

    input_tagset = _create_simple_tagset(10)
    response = client.put_object_tagging(
        Bucket=bucket_name, Key=key, Tagging=input_tagset
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    alt_client = get_alt_client()

    response = alt_client.delete_object_tagging(Bucket=bucket_name, Key=key)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 204

    response = client.get_object_tagging(Bucket=bucket_name, Key=key)
    assert len(response["TagSet"]) == 0


def test_versioning_bucket_atomic_upload_return_version_id():
    bucket_name = get_new_bucket()
    client = get_client()
    key = "bar"

    # for versioning-enabled-bucket, an non-empty version-id should return
    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")
    response = client.put_object(Bucket=bucket_name, Key=key)
    version_id = response["VersionId"]

    response = client.list_object_versions(Bucket=bucket_name)
    versions = response["Versions"]
    for version in versions:
        assert version["VersionId"] == version_id

    # for versioning-default-bucket, no version-id should return.
    bucket_name = get_new_bucket()
    key = "baz"
    response = client.put_object(Bucket=bucket_name, Key=key)
    assert not "VersionId" in response

    # for versioning-suspended-bucket, no version-id should return.
    bucket_name = get_new_bucket()
    key = "baz"
    check_configure_versioning_retry(bucket_name, "Suspended", "Suspended")
    response = client.put_object(Bucket=bucket_name, Key=key)
    assert not "VersionId" in response


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/843")
def test_versioning_bucket_multipart_upload_return_version_id():
    content_type = "text/bla"
    objlen = 30 * 1024 * 1024

    bucket_name = get_new_bucket()
    client = get_client()
    key = "bar"
    metadata = {"foo": "baz"}

    # for versioning-enabled-bucket, an non-empty version-id should return
    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")

    (upload_id, data, parts) = _multipart_upload(
        bucket_name=bucket_name,
        key=key,
        size=objlen,
        client=client,
        content_type=content_type,
        metadata=metadata,
    )

    response = client.complete_multipart_upload(
        Bucket=bucket_name,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )
    version_id = response["VersionId"]

    response = client.list_object_versions(Bucket=bucket_name)
    versions = response["Versions"]
    for version in versions:
        assert version["VersionId"] == version_id

    # for versioning-default-bucket, no version-id should return.
    bucket_name = get_new_bucket()
    key = "baz"

    (upload_id, data, parts) = _multipart_upload(
        bucket_name=bucket_name,
        key=key,
        size=objlen,
        client=client,
        content_type=content_type,
        metadata=metadata,
    )

    response = client.complete_multipart_upload(
        Bucket=bucket_name,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )
    assert not "VersionId" in response

    # for versioning-suspended-bucket, no version-id should return
    bucket_name = get_new_bucket()
    key = "foo"
    check_configure_versioning_retry(bucket_name, "Suspended", "Suspended")

    (upload_id, data, parts) = _multipart_upload(
        bucket_name=bucket_name,
        key=key,
        size=objlen,
        client=client,
        content_type=content_type,
        metadata=metadata,
    )

    response = client.complete_multipart_upload(
        Bucket=bucket_name,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )
    assert not "VersionId" in response


@pytest.mark.tagging
@pytest.mark.bucket_policy
@pytest.mark.fails_on_dbstore
def test_bucket_policy_get_obj_existing_tag():
    bucket_name = _create_objects(keys=["publictag", "privatetag", "invalidtag"])
    client = get_client()

    tag_conditional = {"StringEquals": {"s3:ExistingObjectTag/security": "public"}}

    resource = _make_arn_resource("{}/{}".format(bucket_name, "*"))
    policy_document = make_json_policy(
        "s3:GetObject", resource, conditions=tag_conditional
    )

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)
    tagset = []
    tagset.append({"Key": "security", "Value": "public"})
    tagset.append({"Key": "foo", "Value": "bar"})

    input_tagset = {"TagSet": tagset}

    response = client.put_object_tagging(
        Bucket=bucket_name, Key="publictag", Tagging=input_tagset
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    tagset2 = []
    tagset2.append({"Key": "security", "Value": "private"})

    input_tagset = {"TagSet": tagset2}

    response = client.put_object_tagging(
        Bucket=bucket_name, Key="privatetag", Tagging=input_tagset
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    tagset3 = []
    tagset3.append({"Key": "security1", "Value": "public"})

    input_tagset = {"TagSet": tagset3}

    response = client.put_object_tagging(
        Bucket=bucket_name, Key="invalidtag", Tagging=input_tagset
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    alt_client = get_alt_client()
    response = alt_client.get_object(Bucket=bucket_name, Key="publictag")
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    e = assert_raises(
        ClientError, alt_client.get_object, Bucket=bucket_name, Key="privatetag"
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403

    e = assert_raises(
        ClientError, alt_client.get_object, Bucket=bucket_name, Key="invalidtag"
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403


@pytest.mark.tagging
@pytest.mark.bucket_policy
@pytest.mark.fails_on_dbstore
def test_bucket_policy_get_obj_tagging_existing_tag():
    bucket_name = _create_objects(keys=["publictag", "privatetag", "invalidtag"])
    client = get_client()

    tag_conditional = {"StringEquals": {"s3:ExistingObjectTag/security": "public"}}

    resource = _make_arn_resource("{}/{}".format(bucket_name, "*"))
    policy_document = make_json_policy(
        "s3:GetObjectTagging", resource, conditions=tag_conditional
    )

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)
    tagset = []
    tagset.append({"Key": "security", "Value": "public"})
    tagset.append({"Key": "foo", "Value": "bar"})

    input_tagset = {"TagSet": tagset}

    response = client.put_object_tagging(
        Bucket=bucket_name, Key="publictag", Tagging=input_tagset
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    tagset2 = []
    tagset2.append({"Key": "security", "Value": "private"})

    input_tagset = {"TagSet": tagset2}

    response = client.put_object_tagging(
        Bucket=bucket_name, Key="privatetag", Tagging=input_tagset
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    tagset3 = []
    tagset3.append({"Key": "security1", "Value": "public"})

    input_tagset = {"TagSet": tagset3}

    response = client.put_object_tagging(
        Bucket=bucket_name, Key="invalidtag", Tagging=input_tagset
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    alt_client = get_alt_client()
    response = alt_client.get_object_tagging(Bucket=bucket_name, Key="publictag")
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    # A get object itself should fail since we allowed only GetObjectTagging
    e = assert_raises(
        ClientError, alt_client.get_object, Bucket=bucket_name, Key="publictag"
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403

    e = assert_raises(
        ClientError, alt_client.get_object_tagging, Bucket=bucket_name, Key="privatetag"
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403

    e = assert_raises(
        ClientError, alt_client.get_object_tagging, Bucket=bucket_name, Key="invalidtag"
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403


@pytest.mark.tagging
@pytest.mark.bucket_policy
@pytest.mark.fails_on_dbstore
def test_bucket_policy_put_obj_tagging_existing_tag():
    bucket_name = _create_objects(keys=["publictag", "privatetag", "invalidtag"])
    client = get_client()

    tag_conditional = {"StringEquals": {"s3:ExistingObjectTag/security": "public"}}

    resource = _make_arn_resource("{}/{}".format(bucket_name, "*"))
    policy_document = make_json_policy(
        "s3:PutObjectTagging", resource, conditions=tag_conditional
    )

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)
    tagset = []
    tagset.append({"Key": "security", "Value": "public"})
    tagset.append({"Key": "foo", "Value": "bar"})

    input_tagset = {"TagSet": tagset}

    response = client.put_object_tagging(
        Bucket=bucket_name, Key="publictag", Tagging=input_tagset
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    tagset2 = []
    tagset2.append({"Key": "security", "Value": "private"})

    input_tagset = {"TagSet": tagset2}

    response = client.put_object_tagging(
        Bucket=bucket_name, Key="privatetag", Tagging=input_tagset
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    alt_client = get_alt_client()
    # PUT requests with object tagging are a bit wierd, if you forget to put
    # the tag which is supposed to be existing anymore well, well subsequent
    # put requests will fail

    testtagset1 = []
    testtagset1.append({"Key": "security", "Value": "public"})
    testtagset1.append({"Key": "foo", "Value": "bar"})

    input_tagset = {"TagSet": testtagset1}

    response = alt_client.put_object_tagging(
        Bucket=bucket_name, Key="publictag", Tagging=input_tagset
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    e = assert_raises(
        ClientError,
        alt_client.put_object_tagging,
        Bucket=bucket_name,
        Key="privatetag",
        Tagging=input_tagset,
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403

    testtagset2 = []
    testtagset2.append({"Key": "security", "Value": "private"})

    input_tagset = {"TagSet": testtagset2}

    response = alt_client.put_object_tagging(
        Bucket=bucket_name, Key="publictag", Tagging=input_tagset
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    # Now try putting the original tags again, this should fail
    input_tagset = {"TagSet": testtagset1}

    e = assert_raises(
        ClientError,
        alt_client.put_object_tagging,
        Bucket=bucket_name,
        Key="publictag",
        Tagging=input_tagset,
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403


@pytest.mark.tagging
@pytest.mark.bucket_policy
@pytest.mark.fails_on_dbstore
def test_bucket_policy_put_obj_copy_source():
    bucket_name = _create_objects(keys=["public/foo", "public/bar", "private/foo"])
    client = get_client()

    src_resource = _make_arn_resource("{}/{}".format(bucket_name, "*"))
    policy_document = make_json_policy("s3:GetObject", src_resource)

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

    bucket_name2 = get_new_bucket()

    tag_conditional = {
        "StringLike": {"s3:x-amz-copy-source": bucket_name + "/public/*"}
    }

    resource = _make_arn_resource("{}/{}".format(bucket_name2, "*"))
    policy_document = make_json_policy(
        "s3:PutObject", resource, conditions=tag_conditional
    )

    client.put_bucket_policy(Bucket=bucket_name2, Policy=policy_document)

    alt_client = get_alt_client()
    copy_source = {"Bucket": bucket_name, "Key": "public/foo"}

    alt_client.copy_object(Bucket=bucket_name2, CopySource=copy_source, Key="new_foo")

    # This is possible because we are still the owner, see the grants with
    # policy on how to do this right
    response = alt_client.get_object(Bucket=bucket_name2, Key="new_foo")
    body = _get_body(response)
    assert body == "public/foo"

    copy_source = {"Bucket": bucket_name, "Key": "public/bar"}
    alt_client.copy_object(Bucket=bucket_name2, CopySource=copy_source, Key="new_foo2")

    response = alt_client.get_object(Bucket=bucket_name2, Key="new_foo2")
    body = _get_body(response)
    assert body == "public/bar"

    copy_source = {"Bucket": bucket_name, "Key": "private/foo"}
    check_access_denied(
        alt_client.copy_object,
        Bucket=bucket_name2,
        CopySource=copy_source,
        Key="new_foo2",
    )


@pytest.mark.tagging
@pytest.mark.bucket_policy
@pytest.mark.fails_on_dbstore
def test_bucket_policy_put_obj_copy_source_meta():
    src_bucket_name = _create_objects(keys=["public/foo", "public/bar"])
    client = get_client()

    src_resource = _make_arn_resource("{}/{}".format(src_bucket_name, "*"))
    policy_document = make_json_policy("s3:GetObject", src_resource)

    client.put_bucket_policy(Bucket=src_bucket_name, Policy=policy_document)

    bucket_name = get_new_bucket()

    tag_conditional = {"StringEquals": {"s3:x-amz-metadata-directive": "COPY"}}

    resource = _make_arn_resource("{}/{}".format(bucket_name, "*"))
    policy_document = make_json_policy(
        "s3:PutObject", resource, conditions=tag_conditional
    )

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

    alt_client = get_alt_client()

    lf = lambda **kwargs: kwargs["params"]["headers"].update(
        {"x-amz-metadata-directive": "COPY"}
    )
    alt_client.meta.events.register("before-call.s3.CopyObject", lf)

    copy_source = {"Bucket": src_bucket_name, "Key": "public/foo"}
    alt_client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key="new_foo")

    # This is possible because we are still the owner, see the grants with
    # policy on how to do this right
    response = alt_client.get_object(Bucket=bucket_name, Key="new_foo")
    body = _get_body(response)
    assert body == "public/foo"

    # remove the x-amz-metadata-directive header
    def remove_header(**kwargs):
        if "x-amz-metadata-directive" in kwargs["params"]["headers"]:
            del kwargs["params"]["headers"]["x-amz-metadata-directive"]

    alt_client.meta.events.register("before-call.s3.CopyObject", remove_header)

    copy_source = {"Bucket": src_bucket_name, "Key": "public/bar"}
    check_access_denied(
        alt_client.copy_object,
        Bucket=bucket_name,
        CopySource=copy_source,
        Key="new_foo2",
        Metadata={"foo": "bar"},
    )


@pytest.mark.tagging
@pytest.mark.bucket_policy
@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/863")
def test_bucket_policy_put_obj_acl():
    bucket_name = get_new_bucket()
    client = get_client()

    # An allow conditional will require atleast the presence of an x-amz-acl
    # attribute a Deny conditional would negate any requests that try to set a
    # public-read/write acl
    conditional = {"StringLike": {"s3:x-amz-acl": "public*"}}

    p = Policy()
    resource = _make_arn_resource("{}/{}".format(bucket_name, "*"))
    s1 = Statement("s3:PutObject", resource)
    s2 = Statement("s3:PutObject", resource, effect="Deny", condition=conditional)

    policy_document = p.add_statement(s1).add_statement(s2).to_json()
    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

    alt_client = get_alt_client()
    key1 = "private-key"

    # if we want to be really pedantic, we should check that this doesn't raise
    # and mark a failure, however if this does raise nosetests would mark this
    # as an ERROR anyway
    response = alt_client.put_object(Bucket=bucket_name, Key=key1, Body=key1)
    # response = alt_client.put_object_acl(Bucket=bucket_name, Key=key1, ACL='private')
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    key2 = "public-key"

    lf = lambda **kwargs: kwargs["params"]["headers"].update(
        {"x-amz-acl": "public-read"}
    )
    alt_client.meta.events.register("before-call.s3.PutObject", lf)

    e = assert_raises(
        ClientError, alt_client.put_object, Bucket=bucket_name, Key=key2, Body=key2
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403


@pytest.mark.bucket_policy
@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/863")
def test_bucket_policy_put_obj_grant():
    bucket_name = get_new_bucket()
    bucket_name2 = get_new_bucket()
    client = get_client()

    # In normal cases a key owner would be the uploader of a key in first case
    # we explicitly require that the bucket owner is granted full control over
    # the object uploaded by any user, the second bucket is where no such
    # policy is enforced meaning that the uploader still retains ownership

    main_user_id = get_main_user_id()
    alt_user_id = get_alt_user_id()

    owner_id_str = "id=" + main_user_id
    s3_conditional = {"StringEquals": {"s3:x-amz-grant-full-control": owner_id_str}}

    resource = _make_arn_resource("{}/{}".format(bucket_name, "*"))
    policy_document = make_json_policy(
        "s3:PutObject", resource, conditions=s3_conditional
    )

    resource = _make_arn_resource("{}/{}".format(bucket_name2, "*"))
    policy_document2 = make_json_policy("s3:PutObject", resource)

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)
    client.put_bucket_policy(Bucket=bucket_name2, Policy=policy_document2)

    alt_client = get_alt_client()
    key1 = "key1"

    lf = lambda **kwargs: kwargs["params"]["headers"].update(
        {"x-amz-grant-full-control": owner_id_str}
    )
    alt_client.meta.events.register("before-call.s3.PutObject", lf)

    response = alt_client.put_object(Bucket=bucket_name, Key=key1, Body=key1)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def remove_header(**kwargs):
        if "x-amz-grant-full-control" in kwargs["params"]["headers"]:
            del kwargs["params"]["headers"]["x-amz-grant-full-control"]

    alt_client.meta.events.register("before-call.s3.PutObject", remove_header)

    key2 = "key2"
    response = alt_client.put_object(Bucket=bucket_name2, Key=key2, Body=key2)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    acl1_response = client.get_object_acl(Bucket=bucket_name, Key=key1)

    # user 1 is trying to get acl for the object from user2 where ownership
    # wasn't transferred
    check_access_denied(client.get_object_acl, Bucket=bucket_name2, Key=key2)

    acl2_response = alt_client.get_object_acl(Bucket=bucket_name2, Key=key2)

    assert acl1_response["Grants"][0]["Grantee"]["ID"] == main_user_id
    assert acl2_response["Grants"][0]["Grantee"]["ID"] == alt_user_id


@pytest.mark.encryption
@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/850")
def test_put_obj_enc_conflict_c_s3():
    bucket_name = get_new_bucket()
    client = get_v2_client()

    # boto3.set_stream_logger(name='botocore')

    key1_str = "testobj"

    sse_client_headers = {
        "x-amz-server-side-encryption": "AES256",
        "x-amz-server-side-encryption-customer-algorithm": "AES256",
        "x-amz-server-side-encryption-customer-key": "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
        "x-amz-server-side-encryption-customer-key-md5": "DWygnHRtgiJ77HCm+1rvHw==",
    }

    lf = lambda **kwargs: kwargs["params"]["headers"].update(sse_client_headers)
    client.meta.events.register("before-call.s3.PutObject", lf)
    e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key=key1_str)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == "InvalidArgument"


@pytest.mark.encryption
@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/850")
def test_put_obj_enc_conflict_c_kms():
    kms_keyid = get_main_kms_keyid()
    if kms_keyid is None:
        kms_keyid = "fool-me-once"
    bucket_name = get_new_bucket()
    client = get_v2_client()

    # boto3.set_stream_logger(name='botocore')

    key1_str = "testobj"

    sse_client_headers = {
        "x-amz-server-side-encryption": "aws:kms",
        "x-amz-server-side-encryption-aws-kms-key-id": kms_keyid,
        "x-amz-server-side-encryption-customer-algorithm": "AES256",
        "x-amz-server-side-encryption-customer-key": "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=",
        "x-amz-server-side-encryption-customer-key-md5": "DWygnHRtgiJ77HCm+1rvHw==",
    }

    lf = lambda **kwargs: kwargs["params"]["headers"].update(sse_client_headers)
    client.meta.events.register("before-call.s3.PutObject", lf)
    e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key=key1_str)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == "InvalidArgument"


@pytest.mark.encryption
@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/850")
def test_put_obj_enc_conflict_s3_kms():
    kms_keyid = get_main_kms_keyid()
    if kms_keyid is None:
        kms_keyid = "fool-me-once"
    bucket_name = get_new_bucket()
    client = get_v2_client()

    # boto3.set_stream_logger(name='botocore')

    key1_str = "testobj"

    sse_client_headers = {
        "x-amz-server-side-encryption": "AES256",
        "x-amz-server-side-encryption-aws-kms-key-id": kms_keyid,
    }

    lf = lambda **kwargs: kwargs["params"]["headers"].update(sse_client_headers)
    client.meta.events.register("before-call.s3.PutObject", lf)
    e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key=key1_str)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == "InvalidArgument"


@pytest.mark.encryption
@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/850")
def test_put_obj_enc_conflict_bad_enc_kms():
    kms_keyid = get_main_kms_keyid()
    if kms_keyid is None:
        kms_keyid = "fool-me-once"
    bucket_name = get_new_bucket()
    client = get_v2_client()

    # boto3.set_stream_logger(name='botocore')

    key1_str = "testobj"

    sse_client_headers = {
        "x-amz-server-side-encryption": "aes:kms",  # aes != aws
    }

    lf = lambda **kwargs: kwargs["params"]["headers"].update(sse_client_headers)
    client.meta.events.register("before-call.s3.PutObject", lf)
    e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key=key1_str)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == "InvalidArgument"


@pytest.mark.encryption
@pytest.mark.bucket_policy
@pytest.mark.sse_s3
@pytest.mark.fails_on_dbstore
def test_bucket_policy_put_obj_s3_noenc():
    bucket_name = get_new_bucket()
    client = get_v2_client()

    deny_incorrect_algo = {
        "StringNotEquals": {"s3:x-amz-server-side-encryption": "AES256"}
    }

    deny_unencrypted_obj = {"Null": {"s3:x-amz-server-side-encryption": "true"}}

    p = Policy()
    resource = _make_arn_resource("{}/{}".format(bucket_name, "*"))

    s1 = Statement(
        "s3:PutObject", resource, effect="Deny", condition=deny_incorrect_algo
    )
    s2 = Statement(
        "s3:PutObject", resource, effect="Deny", condition=deny_unencrypted_obj
    )
    policy_document = p.add_statement(s1).add_statement(s2).to_json()

    # boto3.set_stream_logger(name='botocore')

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)
    key1_str = "testobj"

    # response = client.get_bucket_policy(Bucket=bucket_name)
    # print response

    # doing this here breaks the next request w/ 400 (non-sse bug).  Do it last.
    # check_access_denied(client.put_object, Bucket=bucket_name, Key=key1_str, Body=key1_str)

    # TODO: why is this a 400 and not passing, it appears boto3 is not parsing the 200 response the rgw sends back properly
    # DEBUGGING: run the boto2 and compare the requests
    # DEBUGGING: try to run this with v2 auth (figure out why get_v2_client isn't working) to make the requests similar to what boto2 is doing
    # DEBUGGING: try to add other options to put_object to see if that makes the response better

    # first validate that writing a sse-s3 object works
    response = client.put_object(
        Bucket=bucket_name, Key=key1_str, ServerSideEncryption="AES256"
    )
    response["ResponseMetadata"]["HTTPHeaders"]["x-amz-server-side-encryption"]
    assert (
        response["ResponseMetadata"]["HTTPHeaders"]["x-amz-server-side-encryption"]
        == "AES256"
    )

    # then validate that a non-encrypted object fails.
    # (this also breaks the connection--non-sse bug, probably because the server
    #  errors out before it consumes the data...)
    check_access_denied(
        client.put_object, Bucket=bucket_name, Key=key1_str, Body=key1_str
    )


@pytest.mark.encryption
@pytest.mark.bucket_policy
@pytest.mark.sse_s3
@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/850")
def test_bucket_policy_put_obj_s3_kms():
    kms_keyid = get_main_kms_keyid()
    if kms_keyid is None:
        kms_keyid = "fool-me-twice"
    bucket_name = get_new_bucket()
    client = get_v2_client()

    deny_incorrect_algo = {
        "StringNotEquals": {"s3:x-amz-server-side-encryption": "AES256"}
    }

    deny_unencrypted_obj = {"Null": {"s3:x-amz-server-side-encryption": "true"}}

    p = Policy()
    resource = _make_arn_resource("{}/{}".format(bucket_name, "*"))

    s1 = Statement(
        "s3:PutObject", resource, effect="Deny", condition=deny_incorrect_algo
    )
    s2 = Statement(
        "s3:PutObject", resource, effect="Deny", condition=deny_unencrypted_obj
    )
    policy_document = p.add_statement(s1).add_statement(s2).to_json()

    # boto3.set_stream_logger(name='botocore')

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)
    key1_str = "testobj"

    # response = client.get_bucket_policy(Bucket=bucket_name)
    # print response

    sse_client_headers = {
        "x-amz-server-side-encryption": "aws:kms",
        "x-amz-server-side-encryption-aws-kms-key-id": kms_keyid,
    }

    lf = lambda **kwargs: kwargs["params"]["headers"].update(sse_client_headers)
    client.meta.events.register("before-call.s3.PutObject", lf)
    check_access_denied(
        client.put_object, Bucket=bucket_name, Key=key1_str, Body=key1_str
    )


@pytest.mark.encryption
@pytest.mark.fails_on_dbstore
@pytest.mark.bucket_policy
def test_bucket_policy_put_obj_kms_noenc():
    kms_keyid = get_main_kms_keyid()
    if kms_keyid is None:
        pytest.skip("[s3 main] section missing kms_keyid")
    bucket_name = get_new_bucket()
    client = get_v2_client()

    deny_incorrect_algo = {
        "StringNotEquals": {"s3:x-amz-server-side-encryption": "aws:kms"}
    }

    deny_unencrypted_obj = {"Null": {"s3:x-amz-server-side-encryption": "true"}}

    p = Policy()
    resource = _make_arn_resource("{}/{}".format(bucket_name, "*"))

    s1 = Statement(
        "s3:PutObject", resource, effect="Deny", condition=deny_incorrect_algo
    )
    s2 = Statement(
        "s3:PutObject", resource, effect="Deny", condition=deny_unencrypted_obj
    )
    policy_document = p.add_statement(s1).add_statement(s2).to_json()

    # boto3.set_stream_logger(name='botocore')

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)
    key1_str = "testobj"
    key2_str = "unicorn"

    # response = client.get_bucket_policy(Bucket=bucket_name)
    # print response

    # must do check_access_denied last - otherwise, pending data
    #  breaks next call...
    response = client.put_object(
        Bucket=bucket_name,
        Key=key1_str,
        ServerSideEncryption="aws:kms",
        SSEKMSKeyId=kms_keyid,
    )
    assert (
        response["ResponseMetadata"]["HTTPHeaders"]["x-amz-server-side-encryption"]
        == "aws:kms"
    )
    assert (
        response["ResponseMetadata"]["HTTPHeaders"][
            "x-amz-server-side-encryption-aws-kms-key-id"
        ]
        == kms_keyid
    )

    check_access_denied(
        client.put_object, Bucket=bucket_name, Key=key2_str, Body=key2_str
    )


@pytest.mark.encryption
@pytest.mark.bucket_policy
@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/850")
def test_bucket_policy_put_obj_kms_s3():
    bucket_name = get_new_bucket()
    client = get_v2_client()

    deny_incorrect_algo = {
        "StringNotEquals": {"s3:x-amz-server-side-encryption": "aws:kms"}
    }

    deny_unencrypted_obj = {"Null": {"s3:x-amz-server-side-encryption": "true"}}

    p = Policy()
    resource = _make_arn_resource("{}/{}".format(bucket_name, "*"))

    s1 = Statement(
        "s3:PutObject", resource, effect="Deny", condition=deny_incorrect_algo
    )
    s2 = Statement(
        "s3:PutObject", resource, effect="Deny", condition=deny_unencrypted_obj
    )
    policy_document = p.add_statement(s1).add_statement(s2).to_json()

    # boto3.set_stream_logger(name='botocore')

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)
    key1_str = "testobj"

    # response = client.get_bucket_policy(Bucket=bucket_name)
    # print response

    sse_client_headers = {
        "x-amz-server-side-encryption": "AES256",
    }

    lf = lambda **kwargs: kwargs["params"]["headers"].update(sse_client_headers)
    client.meta.events.register("before-call.s3.PutObject", lf)
    check_access_denied(
        client.put_object, Bucket=bucket_name, Key=key1_str, Body=key1_str
    )


@pytest.mark.tagging
@pytest.mark.bucket_policy
# TODO: remove this fails_on_rgw when I fix it
@pytest.mark.fails_on_rgw
@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/863")
def test_bucket_policy_put_obj_request_obj_tag():
    bucket_name = get_new_bucket()
    client = get_client()

    tag_conditional = {"StringEquals": {"s3:RequestObjectTag/security": "public"}}

    p = Policy()
    resource = _make_arn_resource("{}/{}".format(bucket_name, "*"))

    s1 = Statement("s3:PutObject", resource, effect="Allow", condition=tag_conditional)
    policy_document = p.add_statement(s1).to_json()

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

    alt_client = get_alt_client()
    key1_str = "testobj"
    check_access_denied(
        alt_client.put_object, Bucket=bucket_name, Key=key1_str, Body=key1_str
    )

    headers = {"x-amz-tagging": "security=public"}
    lf = lambda **kwargs: kwargs["params"]["headers"].update(headers)
    client.meta.events.register("before-call.s3.PutObject", lf)
    # TODO: why is this a 400 and not passing
    alt_client.put_object(Bucket=bucket_name, Key=key1_str, Body=key1_str)


@pytest.mark.tagging
@pytest.mark.bucket_policy
@pytest.mark.fails_on_dbstore
def test_bucket_policy_get_obj_acl_existing_tag():
    bucket_name = _create_objects(keys=["publictag", "privatetag", "invalidtag"])
    client = get_client()

    tag_conditional = {"StringEquals": {"s3:ExistingObjectTag/security": "public"}}

    resource = _make_arn_resource("{}/{}".format(bucket_name, "*"))
    policy_document = make_json_policy(
        "s3:GetObjectAcl", resource, conditions=tag_conditional
    )

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)
    tagset = []
    tagset.append({"Key": "security", "Value": "public"})
    tagset.append({"Key": "foo", "Value": "bar"})

    input_tagset = {"TagSet": tagset}

    response = client.put_object_tagging(
        Bucket=bucket_name, Key="publictag", Tagging=input_tagset
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    tagset2 = []
    tagset2.append({"Key": "security", "Value": "private"})

    input_tagset = {"TagSet": tagset2}

    response = client.put_object_tagging(
        Bucket=bucket_name, Key="privatetag", Tagging=input_tagset
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    tagset3 = []
    tagset3.append({"Key": "security1", "Value": "public"})

    input_tagset = {"TagSet": tagset3}

    response = client.put_object_tagging(
        Bucket=bucket_name, Key="invalidtag", Tagging=input_tagset
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    alt_client = get_alt_client()
    response = alt_client.get_object_acl(Bucket=bucket_name, Key="publictag")
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    # A get object itself should fail since we allowed only GetObjectTagging
    e = assert_raises(
        ClientError, alt_client.get_object, Bucket=bucket_name, Key="publictag"
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403

    e = assert_raises(
        ClientError, alt_client.get_object_tagging, Bucket=bucket_name, Key="privatetag"
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403

    e = assert_raises(
        ClientError, alt_client.get_object_tagging, Bucket=bucket_name, Key="invalidtag"
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403


@pytest.mark.fails_on_dbstore
def test_object_lock_put_obj_lock():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    conf = {
        "ObjectLockEnabled": "Enabled",
        "Rule": {"DefaultRetention": {"Mode": "GOVERNANCE", "Days": 1}},
    }
    response = client.put_object_lock_configuration(
        Bucket=bucket_name, ObjectLockConfiguration=conf
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    conf = {
        "ObjectLockEnabled": "Enabled",
        "Rule": {"DefaultRetention": {"Mode": "COMPLIANCE", "Years": 1}},
    }
    response = client.put_object_lock_configuration(
        Bucket=bucket_name, ObjectLockConfiguration=conf
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    response = client.get_bucket_versioning(Bucket=bucket_name)
    assert response["Status"] == "Enabled"


def test_object_lock_put_obj_lock_invalid_bucket():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name)
    conf = {
        "ObjectLockEnabled": "Enabled",
        "Rule": {"DefaultRetention": {"Mode": "GOVERNANCE", "Days": 1}},
    }
    e = assert_raises(
        ClientError,
        client.put_object_lock_configuration,
        Bucket=bucket_name,
        ObjectLockConfiguration=conf,
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 409
    assert error_code == "InvalidBucketState"


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/869")
def test_object_lock_put_obj_lock_with_days_and_years():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    conf = {
        "ObjectLockEnabled": "Enabled",
        "Rule": {"DefaultRetention": {"Mode": "GOVERNANCE", "Days": 1, "Years": 1}},
    }
    e = assert_raises(
        ClientError,
        client.put_object_lock_configuration,
        Bucket=bucket_name,
        ObjectLockConfiguration=conf,
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == "MalformedXML"


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/869")
def test_object_lock_put_obj_lock_invalid_days():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    conf = {
        "ObjectLockEnabled": "Enabled",
        "Rule": {"DefaultRetention": {"Mode": "GOVERNANCE", "Days": 0}},
    }
    e = assert_raises(
        ClientError,
        client.put_object_lock_configuration,
        Bucket=bucket_name,
        ObjectLockConfiguration=conf,
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == "InvalidRetentionPeriod"


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/869")
def test_object_lock_put_obj_lock_invalid_years():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    conf = {
        "ObjectLockEnabled": "Enabled",
        "Rule": {"DefaultRetention": {"Mode": "GOVERNANCE", "Years": -1}},
    }
    e = assert_raises(
        ClientError,
        client.put_object_lock_configuration,
        Bucket=bucket_name,
        ObjectLockConfiguration=conf,
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == "InvalidRetentionPeriod"


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/869")
def test_object_lock_put_obj_lock_invalid_mode():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    conf = {
        "ObjectLockEnabled": "Enabled",
        "Rule": {"DefaultRetention": {"Mode": "abc", "Years": 1}},
    }
    e = assert_raises(
        ClientError,
        client.put_object_lock_configuration,
        Bucket=bucket_name,
        ObjectLockConfiguration=conf,
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == "MalformedXML"

    conf = {
        "ObjectLockEnabled": "Enabled",
        "Rule": {"DefaultRetention": {"Mode": "governance", "Years": 1}},
    }
    e = assert_raises(
        ClientError,
        client.put_object_lock_configuration,
        Bucket=bucket_name,
        ObjectLockConfiguration=conf,
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == "MalformedXML"


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/869")
def test_object_lock_put_obj_lock_invalid_status():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    conf = {
        "ObjectLockEnabled": "Disabled",
        "Rule": {"DefaultRetention": {"Mode": "GOVERNANCE", "Years": 1}},
    }
    e = assert_raises(
        ClientError,
        client.put_object_lock_configuration,
        Bucket=bucket_name,
        ObjectLockConfiguration=conf,
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == "MalformedXML"


@pytest.mark.fails_on_dbstore
def test_object_lock_suspend_versioning():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    e = assert_raises(
        ClientError,
        client.put_bucket_versioning,
        Bucket=bucket_name,
        VersioningConfiguration={"Status": "Suspended"},
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 409
    assert error_code == "InvalidBucketState"


@pytest.mark.fails_on_dbstore
def test_object_lock_get_obj_lock():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    conf = {
        "ObjectLockEnabled": "Enabled",
        "Rule": {"DefaultRetention": {"Mode": "GOVERNANCE", "Days": 1}},
    }
    client.put_object_lock_configuration(
        Bucket=bucket_name, ObjectLockConfiguration=conf
    )
    response = client.get_object_lock_configuration(Bucket=bucket_name)
    assert response["ObjectLockConfiguration"] == conf


def test_object_lock_get_obj_lock_invalid_bucket():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name)
    e = assert_raises(
        ClientError, client.get_object_lock_configuration, Bucket=bucket_name
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == "ObjectLockConfigurationNotFoundError"


@pytest.mark.fails_on_dbstore
def test_object_lock_put_obj_retention():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    key = "file1"
    response = client.put_object(Bucket=bucket_name, Body="abc", Key=key)
    version_id = response["VersionId"]
    retention = {
        "Mode": "GOVERNANCE",
        "RetainUntilDate": datetime.datetime(2030, 1, 1, tzinfo=pytz.UTC),
    }
    response = client.put_object_retention(
        Bucket=bucket_name, Key=key, Retention=retention
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    client.delete_object(
        Bucket=bucket_name,
        Key=key,
        VersionId=version_id,
        BypassGovernanceRetention=True,
    )


def test_object_lock_put_obj_retention_invalid_bucket():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name)
    key = "file1"
    client.put_object(Bucket=bucket_name, Body="abc", Key=key)
    retention = {
        "Mode": "GOVERNANCE",
        "RetainUntilDate": datetime.datetime(2030, 1, 1, tzinfo=pytz.UTC),
    }
    e = assert_raises(
        ClientError,
        client.put_object_retention,
        Bucket=bucket_name,
        Key=key,
        Retention=retention,
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == "ObjectLockConfigurationNotFoundError"


@pytest.mark.fails_on_dbstore
def test_object_lock_put_obj_retention_invalid_mode():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    key = "file1"
    client.put_object(Bucket=bucket_name, Body="abc", Key=key)
    retention = {
        "Mode": "governance",
        "RetainUntilDate": datetime.datetime(2030, 1, 1, tzinfo=pytz.UTC),
    }
    e = assert_raises(
        ClientError,
        client.put_object_retention,
        Bucket=bucket_name,
        Key=key,
        Retention=retention,
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == "MalformedXML"

    retention = {
        "Mode": "abc",
        "RetainUntilDate": datetime.datetime(2030, 1, 1, tzinfo=pytz.UTC),
    }
    e = assert_raises(
        ClientError,
        client.put_object_retention,
        Bucket=bucket_name,
        Key=key,
        Retention=retention,
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == "MalformedXML"


@pytest.mark.fails_on_dbstore
def test_object_lock_get_obj_retention():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    key = "file1"
    response = client.put_object(Bucket=bucket_name, Body="abc", Key=key)
    version_id = response["VersionId"]
    retention = {
        "Mode": "GOVERNANCE",
        "RetainUntilDate": datetime.datetime(2030, 1, 1, tzinfo=pytz.UTC),
    }
    client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention)
    response = client.get_object_retention(Bucket=bucket_name, Key=key)
    assert response["Retention"] == retention
    client.delete_object(
        Bucket=bucket_name,
        Key=key,
        VersionId=version_id,
        BypassGovernanceRetention=True,
    )


@pytest.mark.fails_on_dbstore
def test_object_lock_get_obj_retention_iso8601():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    key = "file1"
    response = client.put_object(Bucket=bucket_name, Body="abc", Key=key)
    version_id = response["VersionId"]
    date = datetime.datetime.today() + datetime.timedelta(days=365)
    retention = {"Mode": "GOVERNANCE", "RetainUntilDate": date}
    client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention)
    client.meta.events.register("after-call.s3.HeadObject", get_http_response)
    client.head_object(Bucket=bucket_name, VersionId=version_id, Key=key)
    retain_date = http_response["headers"]["x-amz-object-lock-retain-until-date"]
    isodate.parse_datetime(retain_date)
    client.delete_object(
        Bucket=bucket_name,
        Key=key,
        VersionId=version_id,
        BypassGovernanceRetention=True,
    )


def test_object_lock_get_obj_retention_invalid_bucket():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name)
    key = "file1"
    client.put_object(Bucket=bucket_name, Body="abc", Key=key)
    e = assert_raises(
        ClientError, client.get_object_retention, Bucket=bucket_name, Key=key
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == "ObjectLockConfigurationNotFoundError"


@pytest.mark.fails_on_dbstore
def test_object_lock_put_obj_retention_versionid():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    key = "file1"
    client.put_object(Bucket=bucket_name, Body="abc", Key=key)
    response = client.put_object(Bucket=bucket_name, Body="abc", Key=key)
    version_id = response["VersionId"]
    retention = {
        "Mode": "GOVERNANCE",
        "RetainUntilDate": datetime.datetime(2030, 1, 1, tzinfo=pytz.UTC),
    }
    client.put_object_retention(
        Bucket=bucket_name, Key=key, VersionId=version_id, Retention=retention
    )
    response = client.get_object_retention(
        Bucket=bucket_name, Key=key, VersionId=version_id
    )
    assert response["Retention"] == retention
    client.delete_object(
        Bucket=bucket_name,
        Key=key,
        VersionId=version_id,
        BypassGovernanceRetention=True,
    )


@pytest.mark.fails_on_dbstore
def test_object_lock_put_obj_retention_override_default_retention():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    conf = {
        "ObjectLockEnabled": "Enabled",
        "Rule": {"DefaultRetention": {"Mode": "GOVERNANCE", "Days": 1}},
    }
    client.put_object_lock_configuration(
        Bucket=bucket_name, ObjectLockConfiguration=conf
    )
    key = "file1"
    response = client.put_object(Bucket=bucket_name, Body="abc", Key=key)
    version_id = response["VersionId"]
    retention = {
        "Mode": "GOVERNANCE",
        "RetainUntilDate": datetime.datetime(2030, 1, 1, tzinfo=pytz.UTC),
    }
    client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention)
    response = client.get_object_retention(Bucket=bucket_name, Key=key)
    assert response["Retention"] == retention
    client.delete_object(
        Bucket=bucket_name,
        Key=key,
        VersionId=version_id,
        BypassGovernanceRetention=True,
    )


@pytest.mark.fails_on_dbstore
def test_object_lock_put_obj_retention_increase_period():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    key = "file1"
    response = client.put_object(Bucket=bucket_name, Body="abc", Key=key)
    version_id = response["VersionId"]
    retention1 = {
        "Mode": "GOVERNANCE",
        "RetainUntilDate": datetime.datetime(2030, 1, 1, tzinfo=pytz.UTC),
    }
    client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention1)
    retention2 = {
        "Mode": "GOVERNANCE",
        "RetainUntilDate": datetime.datetime(2030, 1, 3, tzinfo=pytz.UTC),
    }
    client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention2)
    response = client.get_object_retention(Bucket=bucket_name, Key=key)
    assert response["Retention"] == retention2
    client.delete_object(
        Bucket=bucket_name,
        Key=key,
        VersionId=version_id,
        BypassGovernanceRetention=True,
    )


@pytest.mark.fails_on_dbstore
def test_object_lock_put_obj_retention_shorten_period():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    key = "file1"
    response = client.put_object(Bucket=bucket_name, Body="abc", Key=key)
    version_id = response["VersionId"]
    retention = {
        "Mode": "GOVERNANCE",
        "RetainUntilDate": datetime.datetime(2030, 1, 3, tzinfo=pytz.UTC),
    }
    client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention)
    retention = {
        "Mode": "GOVERNANCE",
        "RetainUntilDate": datetime.datetime(2030, 1, 1, tzinfo=pytz.UTC),
    }
    e = assert_raises(
        ClientError,
        client.put_object_retention,
        Bucket=bucket_name,
        Key=key,
        Retention=retention,
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == "AccessDenied"
    client.delete_object(
        Bucket=bucket_name,
        Key=key,
        VersionId=version_id,
        BypassGovernanceRetention=True,
    )


@pytest.mark.fails_on_dbstore
def test_object_lock_put_obj_retention_shorten_period_bypass():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    key = "file1"
    response = client.put_object(Bucket=bucket_name, Body="abc", Key=key)
    version_id = response["VersionId"]
    retention = {
        "Mode": "GOVERNANCE",
        "RetainUntilDate": datetime.datetime(2030, 1, 3, tzinfo=pytz.UTC),
    }
    client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention)
    retention = {
        "Mode": "GOVERNANCE",
        "RetainUntilDate": datetime.datetime(2030, 1, 1, tzinfo=pytz.UTC),
    }
    client.put_object_retention(
        Bucket=bucket_name, Key=key, Retention=retention, BypassGovernanceRetention=True
    )
    response = client.get_object_retention(Bucket=bucket_name, Key=key)
    assert response["Retention"] == retention
    client.delete_object(
        Bucket=bucket_name,
        Key=key,
        VersionId=version_id,
        BypassGovernanceRetention=True,
    )


@pytest.mark.fails_on_dbstore
def test_object_lock_delete_object_with_retention():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    key = "file1"

    response = client.put_object(Bucket=bucket_name, Body="abc", Key=key)
    retention = {
        "Mode": "GOVERNANCE",
        "RetainUntilDate": datetime.datetime(2030, 1, 1, tzinfo=pytz.UTC),
    }
    client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention)
    e = assert_raises(
        ClientError,
        client.delete_object,
        Bucket=bucket_name,
        Key=key,
        VersionId=response["VersionId"],
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == "AccessDenied"

    response = client.delete_object(
        Bucket=bucket_name,
        Key=key,
        VersionId=response["VersionId"],
        BypassGovernanceRetention=True,
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 204


@pytest.mark.fails_on_dbstore
def test_object_lock_delete_object_with_retention_and_marker():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    key = "file1"

    response = client.put_object(Bucket=bucket_name, Body="abc", Key=key)
    retention = {
        "Mode": "GOVERNANCE",
        "RetainUntilDate": datetime.datetime(2030, 1, 1, tzinfo=pytz.UTC),
    }
    client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention)
    del_response = client.delete_object(Bucket=bucket_name, Key=key)
    e = assert_raises(
        ClientError,
        client.delete_object,
        Bucket=bucket_name,
        Key=key,
        VersionId=response["VersionId"],
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == "AccessDenied"

    client.delete_object(
        Bucket=bucket_name, Key=key, VersionId=del_response["VersionId"]
    )
    e = assert_raises(
        ClientError,
        client.delete_object,
        Bucket=bucket_name,
        Key=key,
        VersionId=response["VersionId"],
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == "AccessDenied"

    response = client.delete_object(
        Bucket=bucket_name,
        Key=key,
        VersionId=response["VersionId"],
        BypassGovernanceRetention=True,
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 204


@pytest.mark.fails_on_dbstore
def test_object_lock_multi_delete_object_with_retention():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    key1 = "file1"
    key2 = "file2"

    response1 = client.put_object(Bucket=bucket_name, Body="abc", Key=key1)
    response2 = client.put_object(Bucket=bucket_name, Body="abc", Key=key2)

    versionId1 = response1["VersionId"]
    versionId2 = response2["VersionId"]

    # key1 is under retention, but key2 isn't.
    retention = {
        "Mode": "GOVERNANCE",
        "RetainUntilDate": datetime.datetime(2030, 1, 1, tzinfo=pytz.UTC),
    }
    client.put_object_retention(Bucket=bucket_name, Key=key1, Retention=retention)

    delete_response = client.delete_objects(
        Bucket=bucket_name,
        Delete={
            "Objects": [
                {"Key": key1, "VersionId": versionId1},
                {"Key": key2, "VersionId": versionId2},
            ]
        },
    )

    assert len(delete_response["Deleted"]) == 1
    assert len(delete_response["Errors"]) == 1

    failed_object = delete_response["Errors"][0]
    assert failed_object["Code"] == "AccessDenied"
    assert failed_object["Key"] == key1
    assert failed_object["VersionId"] == versionId1

    deleted_object = delete_response["Deleted"][0]
    assert deleted_object["Key"] == key2
    assert deleted_object["VersionId"] == versionId2

    delete_response = client.delete_objects(
        Bucket=bucket_name,
        Delete={"Objects": [{"Key": key1, "VersionId": versionId1}]},
        BypassGovernanceRetention=True,
    )

    assert ("Errors" not in delete_response) or (len(delete_response["Errors"]) == 0)
    assert len(delete_response["Deleted"]) == 1
    deleted_object = delete_response["Deleted"][0]
    assert deleted_object["Key"] == key1
    assert deleted_object["VersionId"] == versionId1


@pytest.mark.fails_on_dbstore
def test_object_lock_put_legal_hold():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    key = "file1"
    client.put_object(Bucket=bucket_name, Body="abc", Key=key)
    legal_hold = {"Status": "ON"}
    response = client.put_object_legal_hold(
        Bucket=bucket_name, Key=key, LegalHold=legal_hold
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    response = client.put_object_legal_hold(
        Bucket=bucket_name, Key=key, LegalHold={"Status": "OFF"}
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_object_lock_put_legal_hold_invalid_bucket():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name)
    key = "file1"
    client.put_object(Bucket=bucket_name, Body="abc", Key=key)
    legal_hold = {"Status": "ON"}
    e = assert_raises(
        ClientError,
        client.put_object_legal_hold,
        Bucket=bucket_name,
        Key=key,
        LegalHold=legal_hold,
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == "ObjectLockConfigurationNotFoundError"


@pytest.mark.fails_on_dbstore
def test_object_lock_put_legal_hold_invalid_status():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    key = "file1"
    client.put_object(Bucket=bucket_name, Body="abc", Key=key)
    legal_hold = {"Status": "abc"}
    e = assert_raises(
        ClientError,
        client.put_object_legal_hold,
        Bucket=bucket_name,
        Key=key,
        LegalHold=legal_hold,
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == "MalformedXML"


@pytest.mark.skip(reason="Not Implemented")
def test_object_lock_get_legal_hold():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    key = "file1"
    client.put_object(Bucket=bucket_name, Body="abc", Key=key)
    legal_hold = {"Status": "ON"}
    client.put_object_legal_hold(Bucket=bucket_name, Key=key, LegalHold=legal_hold)
    response = client.get_object_legal_hold(Bucket=bucket_name, Key=key)
    assert response["LegalHold"] == legal_hold
    legal_hold_off = {"Status": "OFF"}
    client.put_object_legal_hold(Bucket=bucket_name, Key=key, LegalHold=legal_hold_off)
    response = client.get_object_legal_hold(Bucket=bucket_name, Key=key)
    assert response["LegalHold"] == legal_hold_off


def test_object_lock_get_legal_hold_invalid_bucket():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name)
    key = "file1"
    client.put_object(Bucket=bucket_name, Body="abc", Key=key)
    e = assert_raises(
        ClientError, client.get_object_legal_hold, Bucket=bucket_name, Key=key
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == "ObjectLockConfigurationNotFoundError"


@pytest.mark.fails_on_dbstore
def test_object_lock_delete_object_with_legal_hold_on():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    key = "file1"
    response = client.put_object(Bucket=bucket_name, Body="abc", Key=key)
    client.put_object_legal_hold(
        Bucket=bucket_name, Key=key, LegalHold={"Status": "ON"}
    )
    e = assert_raises(
        ClientError,
        client.delete_object,
        Bucket=bucket_name,
        Key=key,
        VersionId=response["VersionId"],
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == "AccessDenied"
    client.put_object_legal_hold(
        Bucket=bucket_name, Key=key, LegalHold={"Status": "OFF"}
    )


@pytest.mark.fails_on_dbstore
def test_object_lock_delete_object_with_legal_hold_off():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    key = "file1"
    response = client.put_object(Bucket=bucket_name, Body="abc", Key=key)
    client.put_object_legal_hold(
        Bucket=bucket_name, Key=key, LegalHold={"Status": "OFF"}
    )
    response = client.delete_object(
        Bucket=bucket_name, Key=key, VersionId=response["VersionId"]
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 204


@pytest.mark.fails_on_dbstore
def test_object_lock_get_obj_metadata():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    key = "file1"
    client.put_object(Bucket=bucket_name, Body="abc", Key=key)
    legal_hold = {"Status": "ON"}
    client.put_object_legal_hold(Bucket=bucket_name, Key=key, LegalHold=legal_hold)
    retention = {
        "Mode": "GOVERNANCE",
        "RetainUntilDate": datetime.datetime(2030, 1, 1, tzinfo=pytz.UTC),
    }
    client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention)
    response = client.head_object(Bucket=bucket_name, Key=key)
    assert response["ObjectLockMode"] == retention["Mode"]
    assert response["ObjectLockRetainUntilDate"] == retention["RetainUntilDate"]
    assert response["ObjectLockLegalHoldStatus"] == legal_hold["Status"]

    client.put_object_legal_hold(
        Bucket=bucket_name, Key=key, LegalHold={"Status": "OFF"}
    )
    client.delete_object(
        Bucket=bucket_name,
        Key=key,
        VersionId=response["VersionId"],
        BypassGovernanceRetention=True,
    )


@pytest.mark.fails_on_dbstore
def test_object_lock_uploading_obj():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    key = "file1"
    client.put_object(
        Bucket=bucket_name,
        Body="abc",
        Key=key,
        ObjectLockMode="GOVERNANCE",
        ObjectLockRetainUntilDate=datetime.datetime(2030, 1, 1, tzinfo=pytz.UTC),
        ObjectLockLegalHoldStatus="ON",
    )

    response = client.head_object(Bucket=bucket_name, Key=key)
    assert response["ObjectLockMode"] == "GOVERNANCE"
    assert response["ObjectLockRetainUntilDate"] == datetime.datetime(
        2030, 1, 1, tzinfo=pytz.UTC
    )
    assert response["ObjectLockLegalHoldStatus"] == "ON"
    client.put_object_legal_hold(
        Bucket=bucket_name, Key=key, LegalHold={"Status": "OFF"}
    )
    client.delete_object(
        Bucket=bucket_name,
        Key=key,
        VersionId=response["VersionId"],
        BypassGovernanceRetention=True,
    )


@pytest.mark.fails_on_dbstore
def test_object_lock_changing_mode_from_governance_with_bypass():
    bucket_name = get_new_bucket_name()
    key = "file1"
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    # upload object with mode=GOVERNANCE
    retain_until = datetime.datetime.now(pytz.utc) + datetime.timedelta(seconds=10)
    client.put_object(
        Bucket=bucket_name,
        Body="abc",
        Key=key,
        ObjectLockMode="GOVERNANCE",
        ObjectLockRetainUntilDate=retain_until,
    )
    # change mode to COMPLIANCE
    retention = {"Mode": "COMPLIANCE", "RetainUntilDate": retain_until}
    client.put_object_retention(
        Bucket=bucket_name, Key=key, Retention=retention, BypassGovernanceRetention=True
    )


@pytest.mark.fails_on_dbstore
def test_object_lock_changing_mode_from_governance_without_bypass():
    bucket_name = get_new_bucket_name()
    key = "file1"
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    # upload object with mode=GOVERNANCE
    retain_until = datetime.datetime.now(pytz.utc) + datetime.timedelta(seconds=10)
    client.put_object(
        Bucket=bucket_name,
        Body="abc",
        Key=key,
        ObjectLockMode="GOVERNANCE",
        ObjectLockRetainUntilDate=retain_until,
    )
    # try to change mode to COMPLIANCE
    retention = {"Mode": "COMPLIANCE", "RetainUntilDate": retain_until}
    e = assert_raises(
        ClientError,
        client.put_object_retention,
        Bucket=bucket_name,
        Key=key,
        Retention=retention,
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == "AccessDenied"


@pytest.mark.fails_on_dbstore
def test_object_lock_changing_mode_from_compliance():
    bucket_name = get_new_bucket_name()
    key = "file1"
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    # upload object with mode=COMPLIANCE
    retain_until = datetime.datetime.now(pytz.utc) + datetime.timedelta(seconds=10)
    client.put_object(
        Bucket=bucket_name,
        Body="abc",
        Key=key,
        ObjectLockMode="COMPLIANCE",
        ObjectLockRetainUntilDate=retain_until,
    )
    # try to change mode to GOVERNANCE
    retention = {"Mode": "GOVERNANCE", "RetainUntilDate": retain_until}
    e = assert_raises(
        ClientError,
        client.put_object_retention,
        Bucket=bucket_name,
        Key=key,
        Retention=retention,
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == "AccessDenied"


@pytest.mark.fails_on_dbstore
def test_copy_object_ifmatch_good():
    bucket_name = get_new_bucket()
    client = get_client()
    resp = client.put_object(Bucket=bucket_name, Key="foo", Body="bar")

    client.copy_object(
        Bucket=bucket_name,
        CopySource=bucket_name + "/foo",
        CopySourceIfMatch=resp["ETag"],
        Key="bar",
    )
    response = client.get_object(Bucket=bucket_name, Key="bar")
    body = _get_body(response)
    assert body == "bar"


# TODO: remove fails_on_rgw when https://tracker.ceph.com/issues/40808 is resolved
@pytest.mark.fails_on_rgw
def test_copy_object_ifmatch_failed():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_object(Bucket=bucket_name, Key="foo", Body="bar")

    e = assert_raises(
        ClientError,
        client.copy_object,
        Bucket=bucket_name,
        CopySource=bucket_name + "/foo",
        CopySourceIfMatch="ABCORZ",
        Key="bar",
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 412
    assert error_code == "PreconditionFailed"


# TODO: remove fails_on_rgw when https://tracker.ceph.com/issues/40808 is resolved
@pytest.mark.fails_on_rgw
def test_copy_object_ifnonematch_good():
    bucket_name = get_new_bucket()
    client = get_client()
    resp = client.put_object(Bucket=bucket_name, Key="foo", Body="bar")

    e = assert_raises(
        ClientError,
        client.copy_object,
        Bucket=bucket_name,
        CopySource=bucket_name + "/foo",
        CopySourceIfNoneMatch=resp["ETag"],
        Key="bar",
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 412
    assert error_code == "PreconditionFailed"


@pytest.mark.fails_on_dbstore
def test_copy_object_ifnonematch_failed():
    bucket_name = get_new_bucket()
    client = get_client()
    resp = client.put_object(Bucket=bucket_name, Key="foo", Body="bar")

    client.copy_object(
        Bucket=bucket_name,
        CopySource=bucket_name + "/foo",
        CopySourceIfNoneMatch="ABCORZ",
        Key="bar",
    )
    response = client.get_object(Bucket=bucket_name, Key="bar")
    body = _get_body(response)
    assert body == "bar"


# TODO: results in a 404 instead of 400 on the RGW
@pytest.mark.fails_on_rgw
def test_object_read_unreadable():
    bucket_name = get_new_bucket()
    client = get_client()
    e = assert_raises(
        ClientError, client.get_object, Bucket=bucket_name, Key="\xae\x8a-"
    )
    status, _ = _get_status_and_error_code(e.response)
    assert status == 404


@pytest.mark.skip(reason="Not Implemented")
def test_get_bucket_policy_status():
    bucket_name = get_new_bucket()
    client = get_client()
    resp = client.get_bucket_policy_status(Bucket=bucket_name)
    assert resp["PolicyStatus"]["IsPublic"] == False


@pytest.mark.skip(reason="Not Implemented")
def test_get_public_acl_bucket_policy_status():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_bucket_acl(Bucket=bucket_name, ACL="public-read")
    resp = client.get_bucket_policy_status(Bucket=bucket_name)
    assert resp["PolicyStatus"]["IsPublic"] == True


@pytest.mark.skip(reason="Not Implemented")
def test_get_authpublic_acl_bucket_policy_status():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_bucket_acl(Bucket=bucket_name, ACL="authenticated-read")
    resp = client.get_bucket_policy_status(Bucket=bucket_name)
    assert resp["PolicyStatus"]["IsPublic"] == True


@pytest.mark.skip(reason="Not Implemented")
def test_get_publicpolicy_acl_bucket_policy_status():
    bucket_name = get_new_bucket()
    client = get_client()

    resp = client.get_bucket_policy_status(Bucket=bucket_name)
    assert resp["PolicyStatus"]["IsPublic"] == False

    resource1 = "arn:aws:s3:::" + bucket_name
    resource2 = "arn:aws:s3:::" + bucket_name + "/*"
    policy_document = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": "*"},
                    "Action": "s3:ListBucket",
                    "Resource": ["{}".format(resource1), "{}".format(resource2)],
                }
            ],
        }
    )

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)
    resp = client.get_bucket_policy_status(Bucket=bucket_name)
    assert resp["PolicyStatus"]["IsPublic"] == True


@pytest.mark.skip(reason="Not Implemented")
def test_get_nonpublicpolicy_acl_bucket_policy_status():
    bucket_name = get_new_bucket()
    client = get_client()

    resp = client.get_bucket_policy_status(Bucket=bucket_name)
    assert resp["PolicyStatus"]["IsPublic"] == False

    resource1 = "arn:aws:s3:::" + bucket_name
    resource2 = "arn:aws:s3:::" + bucket_name + "/*"
    policy_document = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": "*"},
                    "Action": "s3:ListBucket",
                    "Resource": ["{}".format(resource1), "{}".format(resource2)],
                    "Condition": {"IpAddress": {"aws:SourceIp": "10.0.0.0/32"}},
                }
            ],
        }
    )

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)
    resp = client.get_bucket_policy_status(Bucket=bucket_name)
    assert resp["PolicyStatus"]["IsPublic"] == False


@pytest.mark.skip(reason="Not Implemented")
def test_get_nonpublicpolicy_deny_bucket_policy_status():
    bucket_name = get_new_bucket()
    client = get_client()

    resp = client.get_bucket_policy_status(Bucket=bucket_name)
    assert resp["PolicyStatus"]["IsPublic"] == False

    resource1 = "arn:aws:s3:::" + bucket_name
    resource2 = "arn:aws:s3:::" + bucket_name + "/*"
    policy_document = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "NotPrincipal": {"AWS": "arn:aws:iam::s3tenant1:root"},
                    "Action": "s3:ListBucket",
                    "Resource": ["{}".format(resource1), "{}".format(resource2)],
                }
            ],
        }
    )

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)
    resp = client.get_bucket_policy_status(Bucket=bucket_name)
    assert resp["PolicyStatus"]["IsPublic"] == True


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/864")
def test_get_default_public_block():
    # client = get_svc_client(svc='s3control', client_config=Config(s3={'addressing_style': 'path'}))
    bucket_name = get_new_bucket()
    client = get_client()

    resp = client.get_public_access_block(Bucket=bucket_name)
    assert resp["PublicAccessBlockConfiguration"]["BlockPublicAcls"] == False
    assert resp["PublicAccessBlockConfiguration"]["BlockPublicPolicy"] == False
    assert resp["PublicAccessBlockConfiguration"]["IgnorePublicAcls"] == False
    assert resp["PublicAccessBlockConfiguration"]["RestrictPublicBuckets"] == False


@pytest.mark.skip(reason="Not Implemented")
def test_put_public_block():
    # client = get_svc_client(svc='s3control', client_config=Config(s3={'addressing_style': 'path'}))
    bucket_name = get_new_bucket()
    client = get_client()

    access_conf = {
        "BlockPublicAcls": True,
        "IgnorePublicAcls": True,
        "BlockPublicPolicy": True,
        "RestrictPublicBuckets": False,
    }

    client.put_public_access_block(
        Bucket=bucket_name, PublicAccessBlockConfiguration=access_conf
    )

    resp = client.get_public_access_block(Bucket=bucket_name)
    assert (
        resp["PublicAccessBlockConfiguration"]["BlockPublicAcls"]
        == access_conf["BlockPublicAcls"]
    )
    assert (
        resp["PublicAccessBlockConfiguration"]["BlockPublicPolicy"]
        == access_conf["BlockPublicPolicy"]
    )
    assert (
        resp["PublicAccessBlockConfiguration"]["IgnorePublicAcls"]
        == access_conf["IgnorePublicAcls"]
    )
    assert (
        resp["PublicAccessBlockConfiguration"]["RestrictPublicBuckets"]
        == access_conf["RestrictPublicBuckets"]
    )


@pytest.mark.skip(reason="Not Implemented")
def test_block_public_put_bucket_acls():
    # client = get_svc_client(svc='s3control', client_config=Config(s3={'addressing_style': 'path'}))
    bucket_name = get_new_bucket()
    client = get_client()

    access_conf = {
        "BlockPublicAcls": True,
        "IgnorePublicAcls": False,
        "BlockPublicPolicy": True,
        "RestrictPublicBuckets": False,
    }

    client.put_public_access_block(
        Bucket=bucket_name, PublicAccessBlockConfiguration=access_conf
    )

    resp = client.get_public_access_block(Bucket=bucket_name)
    assert (
        resp["PublicAccessBlockConfiguration"]["BlockPublicAcls"]
        == access_conf["BlockPublicAcls"]
    )
    assert (
        resp["PublicAccessBlockConfiguration"]["BlockPublicPolicy"]
        == access_conf["BlockPublicPolicy"]
    )

    e = assert_raises(
        ClientError, client.put_bucket_acl, Bucket=bucket_name, ACL="public-read"
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403

    e = assert_raises(
        ClientError, client.put_bucket_acl, Bucket=bucket_name, ACL="public-read-write"
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403

    e = assert_raises(
        ClientError, client.put_bucket_acl, Bucket=bucket_name, ACL="authenticated-read"
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403


@pytest.mark.skip(reason="Not Implemented")
def test_block_public_object_canned_acls():
    bucket_name = get_new_bucket()
    client = get_client()

    access_conf = {
        "BlockPublicAcls": True,
        "IgnorePublicAcls": False,
        "BlockPublicPolicy": False,
        "RestrictPublicBuckets": False,
    }

    client.put_public_access_block(
        Bucket=bucket_name, PublicAccessBlockConfiguration=access_conf
    )

    # resp = client.get_public_access_block(Bucket=bucket_name)
    # assert resp['PublicAccessBlockConfiguration']['BlockPublicAcls'] == access_conf['BlockPublicAcls']
    # assert resp['PublicAccessBlockConfiguration']['BlockPublicPolicy'] == access_conf['BlockPublicPolicy']

    # FIXME: use empty body until #42208
    e = assert_raises(
        ClientError,
        client.put_object,
        Bucket=bucket_name,
        Key="foo1",
        Body="",
        ACL="public-read",
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403

    e = assert_raises(
        ClientError,
        client.put_object,
        Bucket=bucket_name,
        Key="foo2",
        Body="",
        ACL="public-read",
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403

    e = assert_raises(
        ClientError,
        client.put_object,
        Bucket=bucket_name,
        Key="foo3",
        Body="",
        ACL="authenticated-read",
    )
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403


@pytest.mark.skip(reason="Not Implemented")
def test_block_public_policy():
    bucket_name = get_new_bucket()
    client = get_client()

    access_conf = {
        "BlockPublicAcls": False,
        "IgnorePublicAcls": False,
        "BlockPublicPolicy": True,
        "RestrictPublicBuckets": False,
    }

    client.put_public_access_block(
        Bucket=bucket_name, PublicAccessBlockConfiguration=access_conf
    )
    resource = _make_arn_resource("{}/{}".format(bucket_name, "*"))
    policy_document = make_json_policy("s3:GetObject", resource)

    check_access_denied(
        client.put_bucket_policy, Bucket=bucket_name, Policy=policy_document
    )


@pytest.mark.skip(reason="Not Implemented")
def test_ignore_public_acls():
    bucket_name = get_new_bucket()
    client = get_client()
    alt_client = get_alt_client()

    client.put_bucket_acl(Bucket=bucket_name, ACL="public-read")
    # Public bucket should be accessible
    alt_client.list_objects(Bucket=bucket_name)

    client.put_object(Bucket=bucket_name, Key="key1", Body="abcde", ACL="public-read")
    resp = alt_client.get_object(Bucket=bucket_name, Key="key1")
    assert _get_body(resp) == "abcde"

    access_conf = {
        "BlockPublicAcls": False,
        "IgnorePublicAcls": True,
        "BlockPublicPolicy": False,
        "RestrictPublicBuckets": False,
    }

    client.put_public_access_block(
        Bucket=bucket_name, PublicAccessBlockConfiguration=access_conf
    )
    resource = _make_arn_resource("{}/{}".format(bucket_name, "*"))

    client.put_bucket_acl(Bucket=bucket_name, ACL="public-read")
    # IgnorePublicACLs is true, so regardless this should behave as a private bucket
    check_access_denied(alt_client.list_objects, Bucket=bucket_name)
    check_access_denied(alt_client.get_object, Bucket=bucket_name, Key="key1")


@pytest.mark.skip(reason="https://github.com/nspcc-dev/neofs-s3-gw/issues/863")
def test_multipart_upload_on_a_bucket_with_policy():
    bucket_name = get_new_bucket()
    client = get_client()
    resource1 = "arn:aws:s3:::" + bucket_name
    resource2 = "arn:aws:s3:::" + bucket_name + "/*"
    policy_document = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "*",
                    "Resource": [resource1, resource2],
                }
            ],
        }
    )
    key = "foo"
    objlen = 50 * 1024 * 1024
    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)
    (upload_id, data, parts) = _multipart_upload(
        bucket_name=bucket_name, key=key, size=objlen, client=client
    )
    response = client.complete_multipart_upload(
        Bucket=bucket_name,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


def _put_bucket_encryption_s3(client, bucket_name):
    """
    enable a default encryption policy on the given bucket
    """
    server_side_encryption_conf = {
        "Rules": [
            {"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}},
        ]
    }
    response = client.put_bucket_encryption(
        Bucket=bucket_name,
        ServerSideEncryptionConfiguration=server_side_encryption_conf,
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


def _put_bucket_encryption_kms(client, bucket_name):
    """
    enable a default encryption policy on the given bucket
    """
    kms_keyid = get_main_kms_keyid()
    if kms_keyid is None:
        kms_keyid = "fool-me-again"
    server_side_encryption_conf = {
        "Rules": [
            {
                "ApplyServerSideEncryptionByDefault": {
                    "SSEAlgorithm": "aws:kms",
                    "KMSMasterKeyID": kms_keyid,
                }
            },
        ]
    }
    response = client.put_bucket_encryption(
        Bucket=bucket_name,
        ServerSideEncryptionConfiguration=server_side_encryption_conf,
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


@pytest.mark.sse_s3
@pytest.mark.skip(reason="Not Implemented")
def test_put_bucket_encryption_s3():
    bucket_name = get_new_bucket()
    client = get_client()
    _put_bucket_encryption_s3(client, bucket_name)


@pytest.mark.encryption
@pytest.mark.skip(reason="Not Implemented")
def test_put_bucket_encryption_kms():
    bucket_name = get_new_bucket()
    client = get_client()
    _put_bucket_encryption_kms(client, bucket_name)


@pytest.mark.sse_s3
@pytest.mark.skip(reason="Not Implemented")
def test_get_bucket_encryption_s3():
    bucket_name = get_new_bucket()
    client = get_client()

    response_code = ""
    try:
        client.get_bucket_encryption(Bucket=bucket_name)
    except ClientError as e:
        response_code = e.response["Error"]["Code"]

    assert response_code == "ServerSideEncryptionConfigurationNotFoundError"

    _put_bucket_encryption_s3(client, bucket_name)

    response = client.get_bucket_encryption(Bucket=bucket_name)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert (
        response["ServerSideEncryptionConfiguration"]["Rules"][0][
            "ApplyServerSideEncryptionByDefault"
        ]["SSEAlgorithm"]
        == "AES256"
    )


@pytest.mark.encryption
@pytest.mark.skip(reason="Not Implemented")
def test_get_bucket_encryption_kms():
    kms_keyid = get_main_kms_keyid()
    if kms_keyid is None:
        kms_keyid = "fool-me-again"
    bucket_name = get_new_bucket()
    client = get_client()

    response_code = ""
    try:
        client.get_bucket_encryption(Bucket=bucket_name)
    except ClientError as e:
        response_code = e.response["Error"]["Code"]

    assert response_code == "ServerSideEncryptionConfigurationNotFoundError"

    _put_bucket_encryption_kms(client, bucket_name)

    response = client.get_bucket_encryption(Bucket=bucket_name)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert (
        response["ServerSideEncryptionConfiguration"]["Rules"][0][
            "ApplyServerSideEncryptionByDefault"
        ]["SSEAlgorithm"]
        == "aws:kms"
    )
    assert (
        response["ServerSideEncryptionConfiguration"]["Rules"][0][
            "ApplyServerSideEncryptionByDefault"
        ]["KMSMasterKeyID"]
        == kms_keyid
    )


@pytest.mark.sse_s3
@pytest.mark.skip(reason="Not Implemented")
def test_delete_bucket_encryption_s3():
    bucket_name = get_new_bucket()
    client = get_client()

    response = client.delete_bucket_encryption(Bucket=bucket_name)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 204

    _put_bucket_encryption_s3(client, bucket_name)

    response = client.delete_bucket_encryption(Bucket=bucket_name)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 204

    response_code = ""
    try:
        client.get_bucket_encryption(Bucket=bucket_name)
    except ClientError as e:
        response_code = e.response["Error"]["Code"]

    assert response_code == "ServerSideEncryptionConfigurationNotFoundError"


@pytest.mark.encryption
@pytest.mark.skip(reason="Not Implemented")
def test_delete_bucket_encryption_kms():
    bucket_name = get_new_bucket()
    client = get_client()

    response = client.delete_bucket_encryption(Bucket=bucket_name)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 204

    _put_bucket_encryption_kms(client, bucket_name)

    response = client.delete_bucket_encryption(Bucket=bucket_name)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 204

    response_code = ""
    try:
        client.get_bucket_encryption(Bucket=bucket_name)
    except ClientError as e:
        response_code = e.response["Error"]["Code"]

    assert response_code == "ServerSideEncryptionConfigurationNotFoundError"


def _test_sse_s3_default_upload(file_size):
    """
    Test enables bucket encryption.
    Create a file of A's of certain size, and use it to set_contents_from_file.
    Re-read the contents, and confirm we get same content as input i.e., A's
    """
    bucket_name = get_new_bucket()
    client = get_client()
    _put_bucket_encryption_s3(client, bucket_name)

    data = "A" * file_size
    response = client.put_object(Bucket=bucket_name, Key="testobj", Body=data)
    assert (
        response["ResponseMetadata"]["HTTPHeaders"]["x-amz-server-side-encryption"]
        == "AES256"
    )

    response = client.get_object(Bucket=bucket_name, Key="testobj")
    assert (
        response["ResponseMetadata"]["HTTPHeaders"]["x-amz-server-side-encryption"]
        == "AES256"
    )
    body = _get_body(response)
    assert body == data


@pytest.mark.encryption
@pytest.mark.bucket_encryption
@pytest.mark.sse_s3
@pytest.mark.fails_on_dbstore
@pytest.mark.skip(reason="Not Implemented")
def test_sse_s3_default_upload_1b():
    _test_sse_s3_default_upload(1)


@pytest.mark.encryption
@pytest.mark.bucket_encryption
@pytest.mark.sse_s3
@pytest.mark.fails_on_dbstore
@pytest.mark.skip(reason="Not Implemented")
def test_sse_s3_default_upload_1kb():
    _test_sse_s3_default_upload(1024)


@pytest.mark.encryption
@pytest.mark.bucket_encryption
@pytest.mark.sse_s3
@pytest.mark.fails_on_dbstore
@pytest.mark.skip(reason="Not Implemented")
def test_sse_s3_default_upload_1mb():
    _test_sse_s3_default_upload(1024 * 1024)


@pytest.mark.encryption
@pytest.mark.bucket_encryption
@pytest.mark.sse_s3
@pytest.mark.fails_on_dbstore
@pytest.mark.skip(reason="Not Implemented")
def test_sse_s3_default_upload_8mb():
    _test_sse_s3_default_upload(8 * 1024 * 1024)


def _test_sse_kms_default_upload(file_size):
    """
    Test enables bucket encryption.
    Create a file of A's of certain size, and use it to set_contents_from_file.
    Re-read the contents, and confirm we get same content as input i.e., A's
    """
    kms_keyid = get_main_kms_keyid()
    if kms_keyid is None:
        pytest.skip("[s3 main] section missing kms_keyid")
    bucket_name = get_new_bucket()
    client = get_client()
    _put_bucket_encryption_kms(client, bucket_name)

    data = "A" * file_size
    response = client.put_object(Bucket=bucket_name, Key="testobj", Body=data)
    assert (
        response["ResponseMetadata"]["HTTPHeaders"]["x-amz-server-side-encryption"]
        == "aws:kms"
    )
    assert (
        response["ResponseMetadata"]["HTTPHeaders"][
            "x-amz-server-side-encryption-aws-kms-key-id"
        ]
        == kms_keyid
    )

    response = client.get_object(Bucket=bucket_name, Key="testobj")
    assert (
        response["ResponseMetadata"]["HTTPHeaders"]["x-amz-server-side-encryption"]
        == "aws:kms"
    )
    assert (
        response["ResponseMetadata"]["HTTPHeaders"][
            "x-amz-server-side-encryption-aws-kms-key-id"
        ]
        == kms_keyid
    )
    body = _get_body(response)
    assert body == data


@pytest.mark.encryption
@pytest.mark.bucket_encryption
@pytest.mark.sse_s3
@pytest.mark.fails_on_dbstore
@pytest.mark.skip(reason="Not Implemented")
def test_sse_kms_default_upload_1b():
    _test_sse_kms_default_upload(1)


@pytest.mark.encryption
@pytest.mark.bucket_encryption
@pytest.mark.sse_s3
@pytest.mark.fails_on_dbstore
@pytest.mark.skip(reason="Not Implemented")
def test_sse_kms_default_upload_1kb():
    _test_sse_kms_default_upload(1024)


@pytest.mark.encryption
@pytest.mark.bucket_encryption
@pytest.mark.sse_s3
@pytest.mark.fails_on_dbstore
@pytest.mark.skip(reason="Not Implemented")
def test_sse_kms_default_upload_1mb():
    _test_sse_kms_default_upload(1024 * 1024)


@pytest.mark.encryption
@pytest.mark.bucket_encryption
@pytest.mark.sse_s3
@pytest.mark.fails_on_dbstore
@pytest.mark.skip(reason="Not Implemented")
def test_sse_kms_default_upload_8mb():
    _test_sse_kms_default_upload(8 * 1024 * 1024)


@pytest.mark.encryption
@pytest.mark.bucket_encryption
@pytest.mark.sse_s3
@pytest.mark.fails_on_dbstore
@pytest.mark.skip(reason="Not Implemented")
def test_sse_s3_default_method_head():
    bucket_name = get_new_bucket()
    client = get_client()
    _put_bucket_encryption_s3(client, bucket_name)

    data = "A" * 1000
    key = "testobj"
    client.put_object(Bucket=bucket_name, Key=key, Body=data)

    response = client.head_object(Bucket=bucket_name, Key=key)
    assert (
        response["ResponseMetadata"]["HTTPHeaders"]["x-amz-server-side-encryption"]
        == "AES256"
    )

    sse_s3_headers = {
        "x-amz-server-side-encryption": "AES256",
    }
    lf = lambda **kwargs: kwargs["params"]["headers"].update(sse_s3_headers)
    client.meta.events.register("before-call.s3.HeadObject", lf)
    e = assert_raises(ClientError, client.head_object, Bucket=bucket_name, Key=key)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400


@pytest.mark.encryption
@pytest.mark.bucket_encryption
@pytest.mark.sse_s3
@pytest.mark.fails_on_dbstore
@pytest.mark.skip(reason="Not Implemented")
def test_sse_s3_default_multipart_upload():
    bucket_name = get_new_bucket()
    client = get_client()
    _put_bucket_encryption_s3(client, bucket_name)

    key = "multipart_enc"
    content_type = "text/plain"
    objlen = 30 * 1024 * 1024
    metadata = {"foo": "bar"}
    enc_headers = {"Content-Type": content_type}
    resend_parts = []

    (upload_id, data, parts) = _multipart_upload_enc(
        client,
        bucket_name,
        key,
        objlen,
        part_size=5 * 1024 * 1024,
        init_headers=enc_headers,
        part_headers=enc_headers,
        metadata=metadata,
        resend_parts=resend_parts,
    )

    lf = lambda **kwargs: kwargs["params"]["headers"].update(enc_headers)
    client.meta.events.register("before-call.s3.CompleteMultipartUpload", lf)
    client.complete_multipart_upload(
        Bucket=bucket_name,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )

    response = client.list_objects_v2(Bucket=bucket_name, Prefix=key)
    assert len(response["Contents"]) == 1
    assert response["Contents"][0]["Size"] == objlen

    lf = lambda **kwargs: kwargs["params"]["headers"].update(enc_headers)
    client.meta.events.register("before-call.s3.UploadPart", lf)

    response = client.get_object(Bucket=bucket_name, Key=key)

    assert response["Metadata"] == metadata
    assert response["ResponseMetadata"]["HTTPHeaders"]["content-type"] == content_type
    assert (
        response["ResponseMetadata"]["HTTPHeaders"]["x-amz-server-side-encryption"]
        == "AES256"
    )

    body = _get_body(response)
    assert body == data
    size = response["ContentLength"]
    assert len(body) == size

    _check_content_using_range(key, bucket_name, data, 1000000)
    _check_content_using_range(key, bucket_name, data, 10000000)


@pytest.mark.encryption
@pytest.mark.bucket_encryption
@pytest.mark.sse_s3
@pytest.mark.fails_on_dbstore
@pytest.mark.skip(reason="Not Implemented")
def test_sse_s3_default_post_object_authenticated_request():
    bucket_name = get_new_bucket()
    client = get_client()
    _put_bucket_encryption_s3(client, bucket_name)

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {
        "expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "conditions": [
            {"bucket": bucket_name},
            ["starts-with", "$key", "foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["starts-with", "$x-amz-server-side-encryption", ""],
            ["content-length-range", 0, 1024],
        ],
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, "utf-8")
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(
        hmac.new(bytes(aws_secret_access_key, "utf-8"), policy, hashlib.sha1).digest()
    )

    payload = OrderedDict(
        [
            ("key", "foo.txt"),
            ("AWSAccessKeyId", aws_access_key_id),
            ("acl", "private"),
            ("signature", signature),
            ("policy", policy),
            ("Content-Type", "text/plain"),
            ("file", ("bar")),
        ]
    )

    r = requests.post(url, files=payload)
    assert r.status_code == 204

    response = client.get_object(Bucket=bucket_name, Key="foo.txt")
    assert (
        response["ResponseMetadata"]["HTTPHeaders"]["x-amz-server-side-encryption"]
        == "AES256"
    )
    body = _get_body(response)
    assert body == "bar"


@pytest.mark.encryption
@pytest.mark.bucket_encryption
@pytest.mark.fails_on_dbstore
@pytest.mark.skip(reason="Not Implemented")
def test_sse_kms_default_post_object_authenticated_request():
    kms_keyid = get_main_kms_keyid()
    if kms_keyid is None:
        pytest.skip("[s3 main] section missing kms_keyid")
    bucket_name = get_new_bucket()
    client = get_client()
    _put_bucket_encryption_kms(client, bucket_name)

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {
        "expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "conditions": [
            {"bucket": bucket_name},
            ["starts-with", "$key", "foo"],
            {"acl": "private"},
            ["starts-with", "$Content-Type", "text/plain"],
            ["starts-with", "$x-amz-server-side-encryption", ""],
            ["content-length-range", 0, 1024],
        ],
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, "utf-8")
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(
        hmac.new(bytes(aws_secret_access_key, "utf-8"), policy, hashlib.sha1).digest()
    )

    payload = OrderedDict(
        [
            ("key", "foo.txt"),
            ("AWSAccessKeyId", aws_access_key_id),
            ("acl", "private"),
            ("signature", signature),
            ("policy", policy),
            ("Content-Type", "text/plain"),
            ("file", ("bar")),
        ]
    )

    r = requests.post(url, files=payload)
    assert r.status_code == 204

    response = client.get_object(Bucket=bucket_name, Key="foo.txt")
    assert (
        response["ResponseMetadata"]["HTTPHeaders"]["x-amz-server-side-encryption"]
        == "aws:kms"
    )
    assert (
        response["ResponseMetadata"]["HTTPHeaders"][
            "x-amz-server-side-encryption-aws-kms-key-id"
        ]
        == kms_keyid
    )
    body = _get_body(response)
    assert body == "bar"


def _test_sse_s3_encrypted_upload(file_size):
    """
    Test upload of the given size, specifically requesting sse-s3 encryption.
    """
    bucket_name = get_new_bucket()
    client = get_client()

    data = "A" * file_size
    response = client.put_object(
        Bucket=bucket_name, Key="testobj", Body=data, ServerSideEncryption="AES256"
    )
    assert (
        response["ResponseMetadata"]["HTTPHeaders"]["x-amz-server-side-encryption"]
        == "AES256"
    )

    response = client.get_object(Bucket=bucket_name, Key="testobj")
    assert (
        response["ResponseMetadata"]["HTTPHeaders"]["x-amz-server-side-encryption"]
        == "AES256"
    )
    body = _get_body(response)
    assert body == data


@pytest.mark.encryption
@pytest.mark.sse_s3
@pytest.mark.fails_on_dbstore
def test_sse_s3_encrypted_upload_1b():
    _test_sse_s3_encrypted_upload(1)


@pytest.mark.encryption
@pytest.mark.sse_s3
@pytest.mark.fails_on_dbstore
def test_sse_s3_encrypted_upload_1kb():
    _test_sse_s3_encrypted_upload(1024)


@pytest.mark.encryption
@pytest.mark.sse_s3
@pytest.mark.fails_on_dbstore
def test_sse_s3_encrypted_upload_1mb():
    _test_sse_s3_encrypted_upload(1024 * 1024)


@pytest.mark.encryption
@pytest.mark.sse_s3
@pytest.mark.fails_on_dbstore
def test_sse_s3_encrypted_upload_8mb():
    _test_sse_s3_encrypted_upload(8 * 1024 * 1024)


@pytest.mark.skip(reason="Not Implemented")
def test_get_object_torrent():
    client = get_client()
    bucket_name = get_new_bucket()
    key = "Avatar.mpg"

    file_size = 7 * 1024 * 1024
    data = "A" * file_size

    client.put_object(Bucket=bucket_name, Key=key, Body=data)

    response = None
    try:
        response = client.get_object_torrent(Bucket=bucket_name, Key=key)
        # if successful, verify the torrent contents are different from the body
        assert data != _get_body(response)
    except ClientError as e:
        # accept 404 errors - torrent support may not be configured
        status, error_code = _get_status_and_error_code(e.response)
        assert status == 404
        assert error_code == "NoSuchKey"
