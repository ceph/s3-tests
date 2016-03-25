========================
 S3 compatibility tests
========================

The tests use the Nose test framework. First install::

	sudo yum install python-virtualenv

and then run ./bootstrap::

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

You will need to create a configuration file with the location of the
service and two different credentials, something like this::

	[DEFAULT]
	## this section is just used as default for all the "s3 *"
        ## sections, you can place these variables also directly there

	## replace with e.g. "localhost" to run against local software
	host = s3.amazonaws.com

	## uncomment the port to use something other than 80
	# port = 8080

	## say "no" to disable TLS
	is_secure = yes

	[fixtures]
	## all the buckets created will start with this prefix;
	## {random} will be filled with random characters to pad
	## the prefix to 30 characters long, and avoid collisions
	bucket prefix = YOURNAMEHERE-{random}-

	[s3 main]
	## the tests assume two accounts are defined, "main" and "alt".

	## user_id is a 64-character hexstring
	user_id = 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef

	## display name typically looks more like a unix login, "jdoe" etc
	display_name = youruseridhere

	## replace these with your access keys
	access_key = ABCDEFGHIJKLMNOPQRST
	secret_key = abcdefghijklmnopqrstuvwxyzabcdefghijklmn

	[s3 alt]
	## another user account, used for ACL-related tests
	user_id = 56789abcdef0123456789abcdef0123456789abcdef0123456789abcdef01234
	display_name = john.doe
	## the "alt" user needs to have email set, too
	email = john.doe@example.com
	access_key = NOPQRSTUVWXYZABCDEFG
	secret_key = nopqrstuvwxyzabcdefghijklmnabcdefghijklm

Once you have that, you can run the tests with::

	S3TEST_CONF=your.conf ./virtualenv/bin/nosetests --with-xunit
	S3TEST_CONF=your.conf ./virtualenv/bin/nosetests -v 2>&1 | tee nosetestresults.csv

You can specify what test(s) to run::

	S3TEST_CONF=your.conf ./virtualenv/bin/nosetests s3tests.functional.test_s3:test_bucket_list_empty

Some tests have attributes set based on their current reliability and
things like AWS not enforcing their spec stricly. You can filter tests
based on their attributes::

	S3TEST_CONF=aws.conf ./virtualenv/bin/nosetests -a '!fails_on_aws'

Configuration::

[default]	
host = 
# port = 8080
is_secure = no

[fixtures]
bucket prefix = s3-{random}-

[s3 main]
#user_id = 

display_name = 

access_key = 
secret_key = 

[s3 alt]
user_id = 
display_name = 
email = 
access_key = 
secret_key = 
