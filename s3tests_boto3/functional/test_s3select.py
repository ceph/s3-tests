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
import random
from botocore.client import Config

region_name = ''

def get_connection():
    conn = boto.connect_s3(
        aws_access_key_id = get_main_aws_access_key(),
        aws_secret_access_key = get_main_aws_secret_key(),
        host = get_config_host(),
        port = get_config_port(),
        is_secure=False,               # uncomment if you are not using ssl
        calling_format = boto.s3.connection.OrdinaryCallingFormat(),
        )

    return conn

def create_csv_object_for_datetime(rows,columns):
        result = ""
        for _ in range(rows):
            row = ""
            for _ in range(columns):
                row = row + "{}{:02d}{:02d}-{:02d}{:02d}{:02d},".format(random.randint(0,100)+1900,random.randint(1,12),random.randint(1,28),random.randint(0,23),random.randint(0,59),random.randint(0,59),)
            result += row + "\n"

        return result

def create_random_csv_object(rows,columns,col_delim=",",record_delim="\n",csv_schema=""):
        result = ""
        if len(csv_schema)>0 :
            result = csv_schema + record_delim

        for _ in range(rows):
            row = ""
            for _ in range(columns):
                row = row + "{}{}".format(random.randint(0,1000),col_delim)
            result += row + record_delim

        return result


def upload_csv_object(bucket_name,new_key,obj):
        conn = get_connection()
        conn.create_bucket( bucket_name )
        bucket = conn.get_bucket( bucket_name )

        k1 = bucket.new_key( new_key )
        k1.set_contents_from_string( obj )
        
    
def run_s3select(bucket,key,query,column_delim=",",row_delim="\n",quot_char='"',esc_char='\\',csv_header_info="NONE"):

    s3 = boto3.client('s3',#'sns',
        endpoint_url=get_config_endpoint(),
        aws_access_key_id=get_main_aws_access_key(),
        region_name=region_name,
        aws_secret_access_key=get_main_aws_secret_key())
        #config=Config(signature_version='v2'))


    r = s3.select_object_content(
        Bucket=bucket,
        Key=key,
        ExpressionType='SQL',
        InputSerialization = {"CSV": {"RecordDelimiter" : row_delim, "FieldDelimiter" : column_delim,"QuoteEscapeCharacter": esc_char, "QuoteCharacter": quot_char, "FileHeaderInfo": csv_header_info}, "CompressionType": "NONE"},
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

def create_list_of_int(column_pos,obj,field_split=",",row_split="\n"):
    
    list_of_int = [] 
    for rec in obj.split(row_split):
        col_num = 1
        if ( len(rec) == 0):
            continue
        for col in rec.split(field_split):
            if (col_num == column_pos):
                list_of_int.append(int(col))
            col_num+=1

    return list_of_int
       
def test_count_operation():
    csv_obj_name = "csv_star_oper"
    bucket_name = "test"
    num_of_rows = 10
    obj_to_load = create_random_csv_object(num_of_rows,10)
    upload_csv_object(bucket_name,csv_obj_name,obj_to_load)
    res = remove_xml_tags_from_result( run_s3select(bucket_name,csv_obj_name,"select count(0) from stdin;") ).replace(",","")

    nose.tools.assert_equal( num_of_rows, int( res ))

def test_column_sum_min_max():
    csv_obj = create_random_csv_object(10000,10)

    csv_obj_name = "csv_10000x10"
    bucket_name = "test"
    upload_csv_object(bucket_name,csv_obj_name,csv_obj)
    
    csv_obj_name = "csv_10000x10"
    bucket_name_2 = "testbuck2"
    upload_csv_object(bucket_name_2,csv_obj_name,csv_obj)
    
    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select min(int(_1)) from stdin;")  ).replace(",","")
    list_int = create_list_of_int( 1 , csv_obj )
    res_target = min( list_int )

    nose.tools.assert_equal( int(res_s3select), int(res_target))

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select min(int(_4)) from stdin;")  ).replace(",","")
    list_int = create_list_of_int( 4 , csv_obj )
    res_target = min( list_int )

    nose.tools.assert_equal( int(res_s3select), int(res_target))
    
    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select max(int(_4)) from stdin;")  ).replace(",","")
    list_int = create_list_of_int( 4 , csv_obj )
    res_target = max( list_int )

    nose.tools.assert_equal( int(res_s3select), int(res_target))
    
    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select max(int(_7)) from stdin;")  ).replace(",","")
    list_int = create_list_of_int( 7 , csv_obj )
    res_target = max( list_int )

    nose.tools.assert_equal( int(res_s3select), int(res_target))
    
    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select sum(int(_4)) from stdin;")  ).replace(",","")
    list_int = create_list_of_int( 4 , csv_obj )
    res_target = sum( list_int )

    nose.tools.assert_equal( int(res_s3select), int(res_target))
    
    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select sum(int(_7)) from stdin;")  ).replace(",","")
    list_int = create_list_of_int( 7 , csv_obj )
    res_target = sum( list_int )

    nose.tools.assert_equal(  int(res_s3select) , int(res_target) )

    # the following queries, validates on *random* input an *accurate* relation between condition result,sum operation and count operation.
    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name_2,csv_obj_name,"select count(0),sum(int(_1)),sum(int(_2)) from stdin where (int(_1)-int(_2)) == 2;" ) )
    count,sum1,sum2,d = res_s3select.split(",")

    nose.tools.assert_equal( int(count)*2 , int(sum1)-int(sum2 ) )

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select count(0),sum(int(_1)),sum(int(_2)) from stdin where (int(_1)-int(_2)) == 4;" ) ) 
    count,sum1,sum2,d = res_s3select.split(",")

    nose.tools.assert_equal( int(count)*4 , int(sum1)-int(sum2) )

def test_complex_expressions():

    # purpose of test: engine is process correctly several projections containing aggregation-functions 
    csv_obj = create_random_csv_object(10000,10)

    csv_obj_name = "csv_100000x10"
    bucket_name = "test"
    upload_csv_object(bucket_name,csv_obj_name,csv_obj)

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select min(int(_1)),max(int(_2)),min(int(_3))+1 from stdin;")).replace("\n","")

    min_1 = min ( create_list_of_int( 1 , csv_obj ) )
    max_2 = max ( create_list_of_int( 2 , csv_obj ) )
    min_3 = min ( create_list_of_int( 3 , csv_obj ) ) + 1

    __res = "{},{},{},".format(min_1,max_2,min_3)
    
    # assert is according to radom-csv function 
    nose.tools.assert_equal( res_s3select, __res )

    # purpose of test that all where conditions create the same group of values, thus same result
    res_s3select_substr = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select min(int(_2)),max(int(_2)) from stdin where substr(_2,1,1) == "1"')).replace("\n","")

    res_s3select_between_numbers = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select min(int(_2)),max(int(_2)) from stdin where int(_2)>=100 and int(_2)<200')).replace("\n","")

    res_s3select_eq_modolu = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select min(int(_2)),max(int(_2)) from stdin where int(_2)/100 == 1 or int(_2)/10 == 1 or int(_2) == 1')).replace("\n","")

    nose.tools.assert_equal( res_s3select_substr, res_s3select_between_numbers)

    nose.tools.assert_equal( res_s3select_between_numbers, res_s3select_eq_modolu)
    
def test_alias():

    # purpose: test is comparing result of exactly the same queries , one with alias the other without.
    # this test is setting alias on 3 projections, the third projection is using other projection alias, also the where clause is using aliases
    # the test validate that where-clause and projections are executing aliases correctly, bare in mind that each alias has its own cache,
    # and that cache need to be invalidate per new row. 

    csv_obj = create_random_csv_object(10000,10)

    csv_obj_name = "csv_10000x10"
    bucket_name = "test"
    upload_csv_object(bucket_name,csv_obj_name,csv_obj)

    res_s3select_alias = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select int(_1) as a1, int(_2) as a2 , (a1+a2) as a3 from stdin where a3>100 and a3<300;")  ).replace(",","")

    res_s3select_no_alias = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select int(_1),int(_2),int(_1)+int(_2) from stdin where (int(_1)+int(_2))>100 and (int(_1)+int(_2))<300;")  ).replace(",","")

    nose.tools.assert_equal( res_s3select_alias, res_s3select_no_alias)


def test_alias_cyclic_refernce():

    number_of_rows = 10000
    
    # purpose of test is to validate the s3select-engine is able to detect a cyclic reference to alias.
    csv_obj = create_random_csv_object(number_of_rows,10)

    csv_obj_name = "csv_10000x10"
    bucket_name = "test"
    upload_csv_object(bucket_name,csv_obj_name,csv_obj)

    res_s3select_alias = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select int(_1) as a1,int(_2) as a2, a1+a4 as a3, a5+a1 as a4, int(_3)+a3 as a5 from stdin;")  )

    find_res = res_s3select_alias.find("number of calls exceed maximum size, probably a cyclic reference to alias")
    
    assert int(find_res) >= 0 

def test_datetime():

    # purpose of test is to validate date-time functionality is correct,
    # by creating same groups with different functions (nested-calls) ,which later produce the same result 

    csv_obj = create_csv_object_for_datetime(10000,1)

    csv_obj_name = "csv_datetime_10000x10"
    bucket_name = "test"

    upload_csv_object(bucket_name,csv_obj_name,csv_obj)

    res_s3select_date_time = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(0) from  stdin where extract("year",timestamp(_1)) > 1950 and extract("year",timestamp(_1)) < 1960;')  )

    res_s3select_substr = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(0) from  stdin where int(substr(_1,1,4))>1950 and int(substr(_1,1,4))<1960;')  )

    nose.tools.assert_equal( res_s3select_date_time, res_s3select_substr)

    res_s3select_date_time = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(0) from  stdin where  datediff("month",timestamp(_1),dateadd("month",2,timestamp(_1)) ) == 2;')  )

    res_s3select_count = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(0) from  stdin;')  )

    nose.tools.assert_equal( res_s3select_date_time, res_s3select_count)

    res_s3select_date_time = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(0) from  stdin where datediff("year",timestamp(_1),dateadd("day", 366 ,timestamp(_1))) == 1 ;')  )

    nose.tools.assert_equal( res_s3select_date_time, res_s3select_count)

    # validate that utcnow is integrate correctly with other date-time functions 
    res_s3select_date_time_utcnow = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(0) from  stdin where datediff("hours",utcnow(),dateadd("day",1,utcnow())) == 24 ;')  )

    nose.tools.assert_equal( res_s3select_date_time_utcnow, res_s3select_count)

def test_csv_parser():

    # purpuse: test default csv values(, \n " \ ), return value may contain meta-char 
    # NOTE: should note that default meta-char for s3select are also for python, thus for one example double \ is mandatory

    csv_obj = ',first,,,second,third="c31,c32,c33",forth="1,2,3,4",fifth="my_string=\\"any_value\\" , my_other_string=\\"aaaa,bbb\\" ",' + "\n"
    csv_obj_name = "csv_one_line"
    bucket_name = "test"

    upload_csv_object(bucket_name,csv_obj_name,csv_obj)

    # return value contain comma{,}
    res_s3select_alias = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select _6 from stdin;")  ).replace("\n","")
    nose.tools.assert_equal( res_s3select_alias, 'third="c31,c32,c33",')

    # return value contain comma{,}
    res_s3select_alias = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select _7 from stdin;")  ).replace("\n","")
    nose.tools.assert_equal( res_s3select_alias, 'forth="1,2,3,4",')

    # return value contain comma{,}{"}, escape-rule{\} by-pass quote{"} , the escape{\} is removed.
    res_s3select_alias = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select _8 from stdin;")  ).replace("\n","")
    nose.tools.assert_equal( res_s3select_alias, 'fifth="my_string="any_value" , my_other_string="aaaa,bbb" ",')

    # return NULL as first token
    res_s3select_alias = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select _1 from stdin;")  ).replace("\n","")
    nose.tools.assert_equal( res_s3select_alias, ',')

    # return NULL in the middle of line
    res_s3select_alias = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select _3 from stdin;")  ).replace("\n","")
    nose.tools.assert_equal( res_s3select_alias, ',')

    # return NULL in the middle of line (successive)
    res_s3select_alias = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select _4 from stdin;")  ).replace("\n","")
    nose.tools.assert_equal( res_s3select_alias, ',')

    # return NULL at the end line
    res_s3select_alias = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select _9 from stdin;")  ).replace("\n","")
    nose.tools.assert_equal( res_s3select_alias, ',')

def test_csv_definition():

    number_of_rows = 10000

    #create object with pipe-sign as field separator and tab as row delimiter.
    csv_obj = create_random_csv_object(number_of_rows,10,"|","\t")

    csv_obj_name = "csv_pipeSign_tab_eol"
    bucket_name = "test"

    upload_csv_object(bucket_name,csv_obj_name,csv_obj)
   
    # purpose of tests is to parse correctly input with different csv defintions  
    res = remove_xml_tags_from_result( run_s3select(bucket_name,csv_obj_name,"select count(0) from stdin;","|","\t") ).replace(",","")

    nose.tools.assert_equal( number_of_rows, int(res))
    
    # assert is according to radom-csv function 
    # purpose of test is validate that tokens are processed correctly
    res_s3select = remove_xml_tags_from_result( run_s3select(bucket_name,csv_obj_name,"select min(int(_1)),max(int(_2)),min(int(_3))+1 from stdin;","|","\t") ).replace("\n","")

    min_1 = min ( create_list_of_int( 1 , csv_obj , "|","\t") )
    max_2 = max ( create_list_of_int( 2 , csv_obj , "|","\t") )
    min_3 = min ( create_list_of_int( 3 , csv_obj , "|","\t") ) + 1

    __res = "{},{},{},".format(min_1,max_2,min_3)
    nose.tools.assert_equal( res_s3select, __res )


def test_schema_definition():

    number_of_rows = 10000

    # purpose of test is to validate functionality using csv header info
    csv_obj = create_random_csv_object(number_of_rows,10,csv_schema="c1,c2,c3,c4,c5,c6,c7,c8,c9,c10")

    csv_obj_name = "csv_with_header_info"
    bucket_name = "test"

    upload_csv_object(bucket_name,csv_obj_name,csv_obj)

    # ignoring the schema on first line and retrieve using generic column number
    res_ignore = remove_xml_tags_from_result( run_s3select(bucket_name,csv_obj_name,"select _1,_3 from stdin;",csv_header_info="IGNORE") ).replace("\n","")

    # using the scheme on first line, query is using the attach schema
    res_use = remove_xml_tags_from_result( run_s3select(bucket_name,csv_obj_name,"select c1,c3 from stdin;",csv_header_info="USE") ).replace("\n","")
    
    # result of both queries should be the same
    nose.tools.assert_equal( res_ignore, res_use)

    # using column-name not exist in schema
    res_multiple_defintion = remove_xml_tags_from_result( run_s3select(bucket_name,csv_obj_name,"select c1,c10,int(c11) from stdin;",csv_header_info="USE") ).replace("\n","")

    assert res_multiple_defintion.find("alias {c11} or column not exist in schema") > 0

    # alias-name is identical to column-name
    res_multiple_defintion = remove_xml_tags_from_result( run_s3select(bucket_name,csv_obj_name,"select int(c1)+int(c2) as c4,c4 from stdin;",csv_header_info="USE") ).replace("\n","")

    assert res_multiple_defintion.find("multiple definition of column {c4} as schema-column and alias") > 0
