import bunch
import collections
import gevent
import random
import time

from ..common import context, get_next_key
from ..common.results import TransferGreenletResult
from ..realistic import FileVerifier


# Make sure context has somewhere to store what we need
context.update(bunch.Bunch(
    needs_first_read=collections.deque(),
    all_keys=[],
    files_iter=None,
))


class SafeTransferGreenlet(gevent.Greenlet):
    def __init__(self, timeout=120):
        gevent.Greenlet.__init__(self)
        self.timeout = timeout
        self.result = None

    def _run(self):
        result = self.result = TransferGreenletResult(self.type)
        result.markStarted()

        try:
            with gevent.Timeout(self.timeout, False):
                result.success = self._doit()
        except gevent.GreenletExit:
            return
        except:
            result.setError(show_traceback=True)

        result.markFinished()


class ReaderGreenlet(SafeTransferGreenlet):
    type = 'reader'

    def _doit(self):
        if context.needs_first_read:
            key = context.needs_first_read.popleft()
        elif context.all_keys:
            key = random.choice(context.all_keys)
        else:
            time.sleep(1)
            return self.result.setError('No available keys to test with reader. Try again later.')

        # Copynew the key object
        key = key.bucket.new_key(key.name)
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
        key = get_next_key(context.bucket)
        self.result.setKey(key)

        fp = next(context.files_iter)
        self.result.size = fp.size

        key.set_contents_from_file(fp)

        self.result.request_finish = time.time()
        self.result.request_start = fp.start_time
        self.result.chunks = fp.last_chunks

        # And at the end, add to needs_first_read and shuffle
        context.needs_first_read.append(key)
        context.all_keys.append(key)

        return True
