========================
 S3 compatibility tests
========================

The tests use the Nose test framework.

python-virtualenv
PyYAML
nose >=1.0.0
boto >=2.6.0
bunch >=1.0.0
gevent ==0.13.6
isodate >=0.4.4
requests ==0.14.0
pytz >=2011k
ordereddict
httplib2
lxml

	sudo yum install python-virtualenv
	sudo ./bootstrap

Configuration:

[default]	
host = 
# port = 8080
is_secure = no

[fixtures]
bucket prefix = s3-{random}-

[s3 main]
#user_id = 

display_name = j

access_key = 
secret_key = 

[s3 alt]
user_id = 
display_name = 
email = 
access_key = 
secret_key = 

Once you have that, you can run the tests:

	S3TEST_CONF=your.conf ./virtualenv/bin/nosetests --with-xunit
	S3TEST_CONF=your.conf ./virtualenv/bin/nosetests -v 2>&1 | tee nosetestresults.csv
	
You can specify what test(s) to run:

	S3TEST_CONF=your.conf ./virtualenv/bin/nosetests s3tests.functional.test_s3:test_bucket_list_empty

Some tests have attributes set based on their current reliability and
things like AWS not enforcing their spec stricly. You can filter tests
based on their attributes:

	S3TEST_CONF=aws.conf ./virtualenv/bin/nosetests -a '!fails_on_aws'
