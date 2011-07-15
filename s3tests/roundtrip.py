import gevent.monkey
gevent.monkey.patch_all()

import bunch
import collections
import gevent
import gevent.pool
import itertools
import random
import realistic
import time
import traceback

import common
from common import context, config
from common.greenlets import ReaderGreenlet, WriterGreenlet
from common.results import ResultsLogger

# Set up the common context to use our information. Wee.
context.update(bunch.Bunch(
    # Set to False when it's time to exit main loop.
    running = True,

    # The pools our tasks run in.
    greenlet_pools = bunch.Bunch(
        writer=None,
        reader=None,
        ),

    # The greenlet that keeps logs going.
    results_logger = None,
))


def setup():
    config_rt = config.roundtrip

    context.bucket = common.get_new_bucket()
    print "Using bucket: {name}".format(name=context.bucket.name)

    context.greenlet_pools.reader = gevent.pool.Pool(config_rt.pool_sizes.reader, ReaderGreenlet)
    context.greenlet_pools.writer = gevent.pool.Pool(config_rt.pool_sizes.writer, WriterGreenlet)

    context.key_iter = itertools.count(1)
    context.files_iter = realistic.files_varied(config_rt.create_objects)


def _main():
    def _stop_running():
        """ Since we can't do assignment in a lambda, we have this little stub """
        context.running = False

    grace_period = config.roundtrip.grace_wait

    print "Launching/Scheduling essential services..."
    gevent.spawn_later(config.roundtrip.duration + grace_period, _stop_running)
    context.results_logger = ResultsLogger.spawn()

    print "Launching the pool of writers, and giving them {grace} seconds to get ahead of us!".format(grace=grace_period)
    writers_start_time = time.time()
    while time.time() - writers_start_time < grace_period:
        common.fill_pools(context.greenlet_pools.writer)
        time.sleep(0.1)

    # Main work loop.
    print "Starting main work loop..."
    while context.running:
        common.fill_pools(*context.greenlet_pools.values())
        time.sleep(0.1)

    print "We've hit duration. Time to stop!"
    print "Waiting {grace} seconds for jobs to finish normally.".format(grace=grace_period)
    time.sleep(grace_period)

    print "Killing off any remaining jobs."
    context.greenlet_pools.reader.kill()
    context.greenlet_pools.writer.kill()

    print "Waiting 10 seconds for them to finish dying off and collections to complete!"
    time.sleep(10)

    print "Killing essential services..."
    context.results_logger.kill()

    print "Done!"


def main():
    common.setup()
    setup()

    # Normal
    try:
        _main()
    except:
        traceback.print_exc()
    common.teardown()
