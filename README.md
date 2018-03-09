S3 compatibility checking tool
==============================
Purpose
---------
A tool for checking the compatibility of customer-specific object stores.

**Note:** The object store needs to be s3-compatible (i.e., provide s3-like APIs, etc.) in order for this tool to be useful.

Prerequisite
-------------

* Platform: Linux distribution(e.g. fedora, debian, centOS, etc.)
* Two s3-compatible object-store accounts

Setup and Usage
---------------
The compatibility tool is forked from s3tests open-sourced project https://github.com/ceph/s3-tests 

The tool is a set of unofficial Amazon AWS S3 compatibility tests, meant to be useful to people implementing software that exposes an S3-like API.

The tool is written in Python, using nose testing framework http://nose.readthedocs.io/en/latest/index.html

The tests only cover the REST interface.

The tests use the Boto library, so the tests will not be able to discover any HTTP-level differences that Boto conceals. Raw HTTP tests might be added later.

**Note:** This tutorial uses AWS S3 as the object store to test against. 

Follow these steps:

* **Download Tool Repository**

  Clone the tool from git hub repository (https://github.com/splunk/s3-tests).

* **Setup & Configuration**

  1. First, cd into the root directory, "s3-tests".
  
  2. Perform setup.

      a. Ensure you have the virtualenv software installed. For example, on Debian/Ubuntu:

      `sudo apt-get install python-virtualenv`

      b. In the s3-tests directory, run:

      `./bootstrap`

  3. Configure splunk.conf (s3tests/splunk.conf ).

     You will need to configure splunk.conf with the location of the service and two different credentials. 
     Usually you just need to configure it **ONCE**(unless the host changes, access key pairs rotates, etc). For example:
     ```
     [DEFAULT]
     ## This section serves as default for all the "s3 *" sections.
     ## You can also place the variables directly in the sections.

     ## Replace with "localhost" to run against local software.
     host = s3.amazonaws.com

     ## Uncomment the port to use something other than 80.
     # port = 8080

     ## Change to "no" to disable TLS.
     is_secure = yes

     [fixtures]
     ## All buckets created will start with this prefix.
     ## {random} will be filled with random characters to pad
     ## the prefix to 30 characters and avoid collisions.
     bucket prefix = YOURNAMEHERE-{random}-

     [s3 main]
     ## The tests assume two accounts are defined, "main" and "alt".

     ## user_id is a 64-character hexstring
     user_id = 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef

     ## display name typically looks like a unix login, for example, "jdoe".
     display_name = youruseridhere

     ## Replace these values with your access keys.
     access_key = ABCDEFGHIJKLMNOPQRST
     secret_key = abcdefghijklmnopqrstuvwxyzabcdefghijklmn

     [s3 alt]
     ## Another user account, used for ACL-related tests.
     user_id = 56789abcdef0123456789abcdef0123456789abcdef0123456789abcdef01234
     display_name = john.doe
     ## The "alt" user needs to have email set.
     email = john.doe@example.com
     access_key = NOPQRSTUVWXYZABCDEFG
     secret_key = nopqrstuvwxyzabcdefghijklmnabcdefghijklm
     ```
     Note: The user_id listed in the conf file is the unique canonical user id associated with your aws account. See https://docs.aws.amazon.com/general/latest/gr/acct-identifiers.html#FindingCanonicalId for further information on what it is and how to find it. If you delete kms_keyid in the conf file, all KMS-related tests will be skipped.

     You can use the aws cli tool to find the DisplayName(display_name in conf) and ID(user_id in conf) associated with your default profile. For example:

     <img width="716" alt="screen shot 2018-02-22 at 5 40 56 pm 1" src="https://user-images.githubusercontent.com/26911671/37020371-64180abc-20d0-11e8-9aeb-e1d752185816.png">

  4. Configure setup.cfg (s3tests/setup.cfg).

     Standard ini-style config files. Put your nose tests configuration in a [nosetests] section. Options are the same as on the command line, with the – prefix removed. For options that are simple switches, you must supply a value:

     <img width="302" alt="screen shot 2018-02-27 at 11 31 24 pm" src="https://user-images.githubusercontent.com/26911671/37020416-853f71b2-20d0-11e8-8fb5-fc7140ad3d3c.png">

     Unless you have some specific need, we recommend starting with the default setup.cfg included in the downloaded project repository. For detailed information on setup.cfg, refer to http://nose.readthedocs.io/en/latest/usage.html#extended-usage

     These are the default nose tests plugins enabled in setup.cfg:   
     *--with-xunit:* Provides test results in the standard XUnit XML format. For details and other options refer to http://nose.readthedocs.io/en/latest/plugins/attrib.html  
     *--failure-detail(optional):* Provides assert introspection. For details and other options, refer to http://nose.readthedocs.io/en/latest/plugins/failuredetail.html  
     *--verbose(optional):* Provides one line summaries of individual tests that get executed together with test result(success/failure).  
     *(--logging-level(optional):* Captures logging statements (with a specific log level or above) issued during test execution when an error or failure occurs. Depending on your specific need, you can disable log capturing with *--nologcapture* option, skip setting *--logging-level* option (in which case log statements of all levels will be captured), or set it to INFO, WARN, ERROR, etc. For details, refer to http://nose.readthedocs.io/en/latest/plugins/logcapture.html  

     **Note:** You can also overwrite the default behavior provided in setup.cfg on the command line.  For example, `S3TEST_CONF=splunk.conf ./virtualenv/bin/nosetests --logging-level=ERROR`

* **Test execution**

  Once you finish setup and configuration, assuming you are still in the s3-tests/ directory, you can run the tests with this command:

  `S3TEST_CONF=splunk.conf ./virtualenv/bin/nosetests -a '!skip_for_splunk' 2>&1 | tee -a splunk.log`

  This command enables additional plugin(s):

  -a - Select tests based on criteria rather then by filename. In the above example, we are selecting all test cases **NOT** tagged with 'skip_for_splunk'. For details and other options refer to http://nose.readthedocs.io/en/latest/plugins/attrib.html

  **Note:** Some options are already provided in setup.cfg, so essentially you are running:  `S3TEST_CONF=splunk.conf ./virtualenv/bin/nosetests --verbose --logging-level=DEBUG --with-xunit --failure-detail -a '!skip_for_splunk'  2>&1 | tee -a splunk.log`

  Other plugins are also available. For a list of all available plugins options supported by nose testing framework, run `nosetests --plugins -v` or refer to http://nose.readthedocs.io/en/latest/plugins/builtin.html

  To gather a list of tests being run, use the flags:

  `-v --collect-only`
  
  You can specify what test(s) to run. For example:

  `S3TEST_CONF=splunk.conf ./virtualenv/bin/nosetests s3tests.functional.test_s3:test_bucket_list_empty`

  Some tests have attributes set based on their current reliability and things like AWS not enforcing their spec strictly. You can filter tests based on their attributes:

  `S3TEST_CONF=aws.conf ./virtualenv/bin/nosetests -a '!fails_on_aws'`

Test Result
-----------
* **Success Scenario**

  A successful test usually takes 20~30 minutes to complete.
  <img width="933" alt="screen shot 2018-02-22 at 12 38 40 am" src="https://user-images.githubusercontent.com/26911671/37020437-9cf17b84-20d0-11e8-9ba9-9621e640bd34.png">
  
* **Failure Scenario**

  Depending on the failure type and the number of failed test units, test failure usually returns in 1 to 2 hours.
  <img width="724" alt="screen shot 2018-02-22 at 5 15 41 pm" src="https://user-images.githubusercontent.com/26911671/37020459-b2269980-20d0-11e8-8e50-d7b7d0044eb7.png">

  **Note:** The last line in the above picture is the brief summary of the test result (4 tests skipped, 16 failed tests with 13 errors, and 3 failures). You can also see it at the top of nosetests.xml file, as mentioned in "Debugging"  section below.

Debugging
---------
You can either send test-generated log files (splunk.log, nosetests.xml) to Splunk or do your own debugging. Skip this section if you send log files to Splunk. Zip or tar the files before sending them.

* **nosetests.xml( s3tests/nosetests.xml )**

  This is the detailed logging, with a summary (for example, how many tests gets run, how many get skipped, erorrs, failure) of the test execution. Also, it provides failed-tests-related stack-trace, assertion-error statements etc. It also includes how long each individual test unit takes to complete.

  If there are test faiulres during execution, open nosetests.xml in a text editor and search for the "error type" or "failure type" keywords for all failed tests with ERROR or FAILURE.

  <img width="726" alt="screen shot 2018-02-22 at 6 24 27 pm" src="https://user-images.githubusercontent.com/26911671/37020495-c9f98b9e-20d0-11e8-9c92-0ddb1f88a6ff.png">

* **splunk.log( s3tests/splunk.log )**

  You can also look at the splunk.log file generated during tests execution in your s3tests directory. This log file includes non-xml format stdout (for example, print statements) and logging statements (for example, boto debug logging) captured for failed tests, together with stacktrace and assertion introspection, etc. It looks similar to the success/failure scenario screenshots provided above in "Test Result" section.

  **Note:** Each time that you execute the test suite, splunk.log gets appended if there are any failed test.
    
Known issues
------------
* Don't try to clone two local copies on the same machine, because when you run ./bootstrap on the second cloned repo locally, if you have two or more pips installed in different places, it uninstalls the one that's first on the path and then reports that it has installed the version that is already installed. See https://github.com/pypa/pip/issues/3433 for more information. This can cause the bootstrapping process to fail. In short, just don't do it. If you really need an extra copy of this tool, set it up on a different machine.
* Bucket limitation: The test units in this tool are creating buckets at the beginning of the test unit and nuking them after finishing. Therefore, make sure that you have at least 20 bucket spots left for your object-store account, so that this tool can run properly.  For example, the default AWS s3 bucket limitation is 100. Therefore, in this case, make sure that you have at most 80 buckets in your s3 account when running this tool.
* In s3-tests/s3tests/functional/\_\_init\_\_.py, we deliberately commented out the print statements in the teardown methods( ( \_\_init\_\_.py:nuke_prefixed_buckets() and \_\_init\_\_.py:nuke_prefixed_buckets_on_conn()), where we clean up the generated testing buckets after completion of each test unit, to avoid log spamming. We decided to do a test-unit level setup/teardown, because of the bucket limitation (assuming a similar limitation exists for your s3-compatible object store) mentioned above.  Also, it is a good practice to clean up the testing state after each test unit completion to avoid inconsistency and confusion, especially given that test-execution-total-time is not a hard requirement. You don't usually need the commented print information for debugging purposes anyway. If you do, just uncomment those lines, and you will see those statements in nosetests.xml and splunk.log
