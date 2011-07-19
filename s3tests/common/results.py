import bunch
import collections
import gevent
import time
import traceback
import yaml

from ..common import context

context.update(bunch.Bunch(
    result_queue = collections.deque(),
))


class TransferGreenletResult(object):
    """ Generic container object. Weeeeeeeeeeeeeee *short* """
    def __init__(self, type):
        # About the Greenlet
        self.type = type

        # About the key
        self.bucket = None
        self.key = None
        self.size = None

        # About the job
        self.success = False
        self.error = None

        self.start_time = None
        self.finish_time = None

        self.duration = None
        self.latency = None

        self.request_start = None
        self.request_finish = None

        self.chunks = None

    def markStarted(self):
        self.start_time = time.time()

    def markFinished(self):
        self.finish_time = time.time()
        self.duration = self.finish_time - self.start_time
        context.result_queue.append(self)

    def setKey(self, key):
        self.key = key.name
        self.bucket = key.bucket.name

    def setError(self, message='Unhandled Exception', show_traceback=False):
        """ Sets an error state in the result, and returns False... example usage:

        return self.result.setError('Something happened', traceback=True)
        """
        self.error = dict()
        self.error['msg'] = message
        if show_traceback:
            self.error['traceback'] = traceback.format_exc()
        return False

    @classmethod
    def repr_yaml(c, dumper, self):
        data = dict()
        for x in ('type', 'bucket', 'key', 'chunks'):
            data[x] = self.__dict__[x]

        # reader => r, writer => w
        data['type'] = data['type'][0]#chunks

        # the error key must be present ONLY on failure.
        assert not (self.success and self.error)
        if self.success:
            assert self.error == None
        else:
            assert self.error != None
            data['error'] = self.error

        data['start'] = self.request_start
        if self.request_finish:
            data['duration'] = 1000000000 * (self.request_finish - self.request_start)

        return dumper.represent_dict(data)

# And a representer for dumping a TransferGreenletResult as a YAML dict()
yaml.add_representer(TransferGreenletResult, TransferGreenletResult.repr_yaml)


class ResultsLogger(gevent.Greenlet):
    """ A quick little greenlet to always run and dump results. """
    def __init__(self):
        gevent.Greenlet.__init__(self)
        self.outfile = context.real_stdout

    def _run(self):
        while True:
            try:
                self._doit()
            except:
                print "An exception was encountered while dumping the results... this shouldn't happen!"
                traceback.print_exc()
            time.sleep(0.1)

    def _doit(self):
        while context.result_queue:
            result = context.result_queue.popleft()
            yrep = yaml.dump(result)
            self.outfile.write(yrep + "---\n")

