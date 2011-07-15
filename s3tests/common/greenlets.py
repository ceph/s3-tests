import bunch
import collections
import gevent
import random
import time
import traceback

from ..common import context, get_next_key
from ..common.results import TransferGreenletResult
from ..realistic import FileVerifier

# Make sure context has somewhere to store what we need
context.update(bunch.Bunch(
    neads_first_read = collections.deque(),
    all_keys = [],
    files_iter = None,
))

class SafeTransferGreenlet(gevent.Greenlet):
    def __init__(self, timeout=120):
        gevent.Greenlet.__init__(self)
        self.timeout = timeout
        self.result = None
        self.key = None # We store key in case we ned to retry due to gevent being a jerk

    def _run(self):
        """ A runner loop... using gevent creates a fun little bug where if two gevents try to
        do the same op (reading, for ex), it raises an AssertionError rather than just switching
        contexts again. Oh joy.

        To combat this, we've put the main work to do in _call_doit, which handles detecting the
        gevent quirk, and we'll retry as long as _call_doit requests that we retry, as indicated
         by _call_doit returning True.
        """
        while self._call_doit():
            time.sleep(0.1)

    def _call_doit(self):
        """ Return True if we need to retry, False otherwise. """
        result = self.result = TransferGreenletResult(self.type)
        result.start_time = time.time()
        
        try:
            with gevent.Timeout(self.timeout, False):
                result.success = self._doit()
        except gevent.GreenletExit:
            # We don't want to retry, as it's time to exit, but we also don't want to count
            # this as a failure.
            return False
        except AssertionError as e:
            # If we've raised this damn gevent error, we simply need to retry.
            if e.args[0].startswith('This event is already used by another greenlet'):
                return True # retry
            # Different assertion error, so fail normally.
            result.comment = traceback.format_exc()
        except Exception:
            result.comment = traceback.format_exc()

        result.finish_time = time.time()
        result.duration = result.finish_time - result.start_time
        result.queue_finished()
        return False # don't retry


class ReaderGreenlet(SafeTransferGreenlet):
    type = 'reader'

    def _doit(self):
        if self.key:
            key = self.key
        elif context.neads_first_read:
            key = context.neads_first_read.popleft()
        elif context.all_keys:
            key = random.choice(context.all_keys)
        else:
            time.sleep(1)
            self.result.comment = 'No available keys to test with reader. Try again later.'
            return False

        self.key = key
        fp = FileVerifier()
        self.result.name = key.name

        request_start = time.time()
        key.get_contents_to_file(fp)
        self.result.size = fp.size
        self.result.latency = fp.first_write - request_start

        if not fp.valid():
            self.result.comment = 'Failed to validate key {name!s}'.format(name=key.name)
            return False

        return True


class WriterGreenlet(SafeTransferGreenlet):
    type = 'writer'

    def _doit(self):
        if self.key:
            key = self.key
        else:
            key = self.key = get_next_key(context.bucket)

        fp = next(context.files_iter)
        self.result.name = key.name
        self.result.size = fp.size

        key.set_contents_from_file(fp)
        self.result.latency = time.time() - fp.last_read

        # And at the end, add to neads_first_read and shuffle
        context.neads_first_read.append(key)
        context.all_keys.append(key)

        return True
