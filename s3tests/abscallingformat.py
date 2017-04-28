import boto
import boto.s3.connection
from boto.compat import six, urllib
from boto.s3.connection import SubdomainCallingFormat
from boto.connection import AWSAuthConnection

class AbsoluteCallingFormat(SubdomainCallingFormat):
    # Need this as we need to create a path, but we only pass on
    # the bucket and key, and we don't want to override make_request also
    def __init__(self,host,port,ssl):
        self.host = host
        self.port = port
        self.ssl = ssl


    def _make_endpoint(self,host,port,ssl):
        if ssl:
            return "https://" + host + ":" + str(port)
        else:
            return "http://" + host + ":" + str(port)

    def build_path_base(self,bucket,key=''):
        key = boto.utils.get_utf8_value(key)
        hoststr = self.build_host(self.host,bucket)
        endpoint = self._make_endpoint(hoststr, self.port, self.ssl)
        if endpoint[-1] != '/':
            endpoint += '/'
        return endpoint + urllib.parse.quote(key)

def abs_get_path(self, path='/'):
    return path
