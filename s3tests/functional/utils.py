import requests
import time

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

