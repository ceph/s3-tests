import os
import random
import requests
import string
import time

from nose.plugins.skip import SkipTest
from nose.tools import eq_ as eq

def assert_raises(excClass, callableObj, *args, **kwargs):
    """
    Like unittest.TestCase.assertRaises, but returns the exception.
    """
    try:
        callableObj(*args, **kwargs)
    except excClass as e:
        return e
    else:
        if hasattr(excClass, '__name__'):
            excName = excClass.__name__
        else:
            excName = str(excClass)
        raise AssertionError("%s not raised" % excName)

def check_aws4_support():
    if 'S3_USE_SIGV4' not in os.environ:
       raise SkipTest

def check_aws2_support():
    if 'S3_USE_SIGV4' in os.environ:
       raise SkipTest

def generate_random(size, part_size=5*1024*1024):
    """
    Generate the specified number random data.
    (actually each MB is a repetition of the first KB)
    """
    chunk = 1024
    allowed = string.ascii_letters
    for x in range(0, size, part_size):
        strpart = ''.join([allowed[random.randint(0, len(allowed) - 1)] for _ in xrange(chunk)])
        s = ''
        left = size - x
        this_part_size = min(left, part_size)
        for y in range(this_part_size / chunk):
            s = s + strpart
        s = s + strpart[:(this_part_size % chunk)]
        yield s
        if (x == size):
            return

# syncs all the regions except for the one passed in
def region_sync_meta(targets, region):

    for (k, r) in targets.iteritems():
        if r == region:
            continue
        conf = r.conf
        if conf.sync_agent_addr:
            ret = requests.post('http://{addr}:{port}/metadata/incremental'.format(addr = conf.sync_agent_addr, port = conf.sync_agent_port))
            eq(ret.status_code, 200)
        if conf.sync_meta_wait:
            time.sleep(conf.sync_meta_wait)

