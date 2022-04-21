To start S3 compatibility tests do the following steps:
1. Make sure `python 3.6` is installed:
```bash
$ python3.6 --version 
Python 3.6.15
```
If not installed, e.g. in Debian/Ubuntu you will have to compile the appropriate 
version of `python`:
```bash 
$ wget https://www.python.org/ftp/python/3.6.15/Python-3.6.15.tar.xz
$ tar -xvf Python-3.6.15.tar.xz
$ sudo apt-get install build-essential zlib1g-dev libncurses5-dev libgdbm-dev 
libnss3-dev libssl-dev libsqlite3-dev libreadline-dev libffi-dev curl 
libbz2-dev -y
$ cd Python-3.6.15
$ ./configure --enable-optimizations --enable-shared
$ make -j 8 # -j indicates a number of threads
$ sudo make altinstall
```
In Arch/Manjaro you can install `python 3.6` from `AUR`, e.g. with `yay`:
```bash
$ git clone https://aur.archlinux.org/yay.git
$ cd yay
$ makepkg -si
$ yay -S python36
```

2. Run the script which will install required packages:
```bash
./bootstrap
```

3. Configure tests. `s3tests.conf.SAMPLE` is an example of configuration file. 
It is required to place your own values in variables:
    * host
    * port
    * access_key
    * secret_key

    The configuration file has multiple groups of settings:
    1. [s3 main]
    2. [s3 alt]
    3. [s3 tenant]
    
    It's preferred to use different wallets at least for [s3 main] and [s3 alt]. 

Example of configuration file for using with [neofs-dev-env](https://github.com/nspcc-dev/neofs-dev-env):
```
[DEFAULT]
## this section is just used for host, port and bucket_prefix

# host set for rgw in vstart.sh
host = s3.neofs.devenv

# port set for rgw in vstart.sh
port = 80

## say "False" to disable TLS
is_secure = False

[fixtures]
## all the buckets created will start with this prefix;
## {random} will be filled with random characters to pad
## the prefix to 30 characters long, and avoid collisions
bucket prefix = yournamehere-{random}-

[s3 main]
# main display_name set in vstart.sh
display_name = M. Tester

# main user_idname set in vstart.sh
user_id = testid

# main email set in vstart.sh
email = tester@ceph.com

# zonegroup api_name for bucket location
api_name = default

## main AWS access key
access_key = EhAVQAguHvFjALLprmUhK5U9GcMdmxdoiGaZew14Gy6Y0FaBHK7V6SmZqbtdbH1HMomy6t5gSpRiQnPVh3czT8DJ3

## main AWS secret key
secret_key = a7d8973be89494849c87b80d9345025d878067f9f4f3972d44025d0697f244e0

## replace with key id obtained when secret is created, or delete if KMS not tested
#kms_keyid = 01234567-89ab-cdef-0123-456789abcdef

[s3 alt]
# alt display_name set in vstart.sh
display_name = john.doe
## alt email set in vstart.sh
email = john.doe@example.com

# alt user_id set in vstart.sh
user_id = 56789abcdef0123456789abcdef0123456789abcdef0123456789abcdef01234

# alt AWS access key set in vstart.sh
access_key = 4CRZTKJ2ygRg1PLsYfvR62C8nGqoPaS6forhPzvqDsqB08iyzMAdAwtzuPFZKWqbnhC2B7wWMyMnfqiB6p9UwpLYE

# alt AWS secret key set in vstart.sh
secret_key = 8e4574f7028b21a1d79c7e8e84ac2872eebd529301b5fae8c0d68fb29fb64acd

[s3 tenant]
# tenant display_name set in vstart.sh
display_name = testx$tenanteduser

# tenant user_id set in vstart.sh
user_id = 9876543210abcdef0123456789abcdef0123456789abcdef0123456789abcdef

# tenant AWS secret key set in vstart.sh
access_key = 3wustVPEuxvZxsF7SFtzNUCBgnXDRHJ4EbqTcourb8Ak07Q9tKy2xnEgYGn37NbqyNbwc7LQzs8DHbyxNGQ5XE8nj

# tenant AWS secret key set in vstart.sh
secret_key = 2e48fd6184c558395a66e4efbf651451104ac34fc119c15eb243dd024000d060

# tenant email set in vstart.sh
email = tenanteduser@example.com

#following section needs to be added for all sts-tests
[iam]
#used for iam operations in sts-tests
#user_id from vstart.sh
user_id = 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef

#access_key from vstart.sh
access_key = GTFpVqCnqzRXy3BBjSwt3jAvPWEN6gJhWq5Aaw5zdwr1_5oxezqhbCgivkRZ4cz8ScQ7RrTetD2RYFQHxsTJjs19U

#secret_key vstart.sh
secret_key = 94b03c1fdab0e9b6b5de80e96f8ea49964877b8d1b7e2fc639c43865c7e2844a

#display_name from vstart.sh
display_name = youruseridhere

#following section needs to be added when you want to run Assume Role With Webidentity test
[webidentity]
#used for assume role with web identity test in sts-tests
#all parameters will be obtained from ceph/qa/tasks/keycloak.py
token=<access_token>

aud=<obtained after introspecting token>

thumbprint=<obtained from x509 certificate>

KC_REALM=<name of the realm>
```

4. Run tests:
    * All tests
    ```bash 
    $ S3TEST_CONF=s3tests.conf ./virtualenv/bin/nosetests -v -s s3tests_boto3.functional
    ```
    * Test with a specific attribute 
    ```bash
    $ S3TEST_CONF=s3tests.conf ./virtualenv/bin/nosetests -v -s  s3tests_boto3.functional.test_s3 -a 'multipart'
    ```
    * Specific test
    ```bash
    $ S3TEST_CONF=s3tests.conf ./virtualenv/bin/nosetests -v -s s3tests_boto3.functional.test_s3:test_bucket_policy_put_obj_request_obj_tag
    ```
    * Fixed to NeoFS S3 Gate tests
    ```bash
    S3TEST_CONF=s3tests.conf ./virtualenv/bin/nosetests -v -s  s3tests_boto3.functional.test_s3_neofs
    ```
Also, if you don't want to see all debug output you can append a parameter `--logging-level=ERROR`.  For more info see [here](https://nose.readthedocs.io/en/latest/usage.html).

