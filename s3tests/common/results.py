import bunch
import collections
import gevent
import time
import traceback
import yaml

from ..common import context

# Make sure context has somewhere to store what we need
context.update(bunch.Bunch(
    result_queue = collections.deque(),
))


class TransferGreenletResult(object):
    """ Generic container object. Weeeeeeeeeeeeeee *short* """
    def __init__(self, type):
        # About the key
        self.name = None
        self.size = None

        # About the job
        self.type = type
        self.success = False
        self.comment = None
        self.start_time = None
        self.finish_time = None

        self.latency = None
        self.duration = None

    def __repr__(self):
        d = self.__dict__
        d['success'] = d['success'] and 'ok' or 'FAILED'

        return self._format.format(**d)

    def queue_finished(self):
        context.result_queue.append(self)


# And a representer for dumping a TransferGreenletResult as a YAML dict()
yaml.add_representer(TransferGreenletResult, lambda dumper, data: dumper.represent_dict(data.__dict__) )


class ResultsLogger(gevent.Greenlet):
    """ A quick little greenlet to always run and dump results. """
    def __init__(self):
        gevent.Greenlet.__init__(self)
        self.outfile = None

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
            if self.outfile:
                self.outfile.write(yrep)
            print yrep, "\n"

