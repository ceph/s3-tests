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

        To combat this, we've put the main work to do in _real_run, which handles detecting the
        gevent quirk, and we'll retry as long as _real_run requests that we retry, as indicated
         by _real_run returning True.
        """
        while self._real_run():
            time.sleep(0.1)

    def _real_run(self):
        """ Return True if we need to retry, False otherwise. """
        result = self.result = TransferGreenletResult(self.type)
        result.markStarted()

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
            result.setError(show_traceback=True)
        except Exception:
            result.setError(show_traceback=True)

        result.markFinished()
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
            return self.result.setError('No available keys to test with reader. Try again later.')

        self.key = key
        self.result.setKey(key)

        fp = FileVerifier()

        key.get_contents_to_file(fp)

        self.result.request_finish = time.time()
        self.result.request_start = fp.created_at
        self.result.chunks = fp.chunks
        self.result.size = fp.size

        if not fp.valid():
            return self.result.setError('Failed to validate key {name!s}'.format(name=key.name))

        return True


class WriterGreenlet(SafeTransferGreenlet):
    type = 'writer'

    def _doit(self):
        if self.key:
            key = self.key
        else:
            key = get_next_key(context.bucket)

        self.key = key
        self.result.setKey(key)

        fp = next(context.files_iter)
        self.result.size = fp.size

        key.set_contents_from_file(fp)

        self.result.request_finish = time.time()
        self.result.request_start = fp.start_time
        self.result.chunks = fp.last_chunks

        # And at the end, add to neads_first_read and shuffle
        context.neads_first_read.append(key)
        context.all_keys.append(key)

        return True
