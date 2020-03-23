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
from email.header import decode_header

from .utils import assert_raises
from .utils import generate_random
from .utils import _get_status_and_error_code
from .utils import _get_status

from .policy import Policy, Statement, make_json_policy

from . import (
    get_client,
    get_prefix,
    get_unauthenticated_client,
    get_bad_auth_client,
    get_v2_client,
    get_new_bucket,
    get_new_bucket_name,
    get_new_bucket_resource,
    get_config_is_secure,
    get_config_host,
    get_config_port,
    get_config_endpoint,
    get_main_aws_access_key,
    get_main_aws_secret_key,
    get_main_display_name,
    get_main_user_id,
    get_main_email,
    get_main_api_name,
    get_alt_aws_access_key,
    get_alt_aws_secret_key,
    get_alt_display_name,
    get_alt_user_id,
    get_alt_email,
    get_alt_client,
    get_tenant_client,
    get_tenant_iam_client,
    get_tenant_user_id,
    get_buckets_list,
    get_objects_list,
    get_main_kms_keyid,
    get_secondary_kms_keyid,
    nuke_prefixed_buckets,
    )

import boto
import boto.s3.connection
import sys
#import urlparse
import random
from botocore.client import Config

endpoint = 'http://localhost:8000'
access_key = 'b2345678901234567890'
secret_key = 'b234567890123456789012345678901234567890'
region_name = ''

def get_connection():
    conn = boto.connect_s3(
        aws_access_key_id = access_key,
        aws_secret_access_key = secret_key,
        host = 'localhost', port = 8000 ,
        is_secure=False,               # uncomment if you are not using ssl
        calling_format = boto.s3.connection.OrdinaryCallingFormat(),
        )

    return conn


def create_random_csv_object(rows,columns):
        result = ""
        for i in range(rows):
            row = "";
            for y in range(columns):
                row = row + "{},".format(random.randint(0,1000)); 
            result += row + "\n"

        return result

def upload_csv_object(bucket_name,new_key,obj):
        conn = get_connection()
        conn.create_bucket( bucket_name )
        bucket = conn.get_bucket( bucket_name )

        k1 = bucket.new_key( new_key );
        k1.set_contents_from_string( obj );
        
    
def run_s3select(bucket,key,query):
    s3 = boto3.client('s3',#'sns',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        region_name=region_name,
        aws_secret_access_key=secret_key)
        #config=Config(signature_version='v2'))


    r = s3.select_object_content(
        Bucket=bucket,
        Key=key,
        ExpressionType='SQL',
        InputSerialization = {"CSV": {}, "CompressionType": "NONE"},
        OutputSerialization = {"CSV": {}},
        Expression=query,)
    
    result = ""
    for event in r['Payload']:
        if 'Records' in event:
            records = event['Records']['Payload'].decode('utf-8')
            result += records
    
    return result

def remove_xml_tags_from_result(obj):
    result = ""
    for rec in obj.split("\n"):
        if(rec.find("Payload")>0 or rec.find("Records")>0):
            continue
        result += rec + "\n" # remove by split

    return result
   
def sum_column_x(column_pos,obj):
    res = 0 
    for rec in obj.split("\n"):
        col_num = 1
        if ( len(rec) == 0):
            continue;
        for col in rec.split(","):
            if (col_num == column_pos):
                res += int(col);
            col_num+=1; 

    return res

def test_count_operation():
    csv_obj_name = "csv_star_oper"
    bucket_name = "test"
    num_of_rows = 10
    obj_to_load = create_random_csv_object(num_of_rows,10)
    upload_csv_object(bucket_name,csv_obj_name,obj_to_load)
    res = remove_xml_tags_from_result( run_s3select(bucket_name,csv_obj_name,"select count(0) from stdin;") ).replace(",","")

    assert num_of_rows == int( res )

def test_column_sum():
    csv_obj = create_random_csv_object(10,10)

    csv_obj_name = "csv_10x10"
    bucket_name = "test"
    upload_csv_object(bucket_name,csv_obj_name,csv_obj)
    
    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select sum(int(_1)) from stdin;")  ).replace(",","")
    res_target = sum_column_x( 1 , csv_obj )

    assert int(res_s3select) == int(res_target) 





