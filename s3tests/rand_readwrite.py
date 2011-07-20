#!/usr/bin/python

import gevent
import gevent.pool
import gevent.queue
import gevent.monkey; gevent.monkey.patch_all()
import optparse
import sys
import time
import random
import yaml

import generate_objects
import realistic
import common

NANOSECOND = int(1e9)

def reader(bucket, name, queue):
    while True:
        count = 0
        for key in bucket.list():
            fp = realistic.FileVerifier()
            start = time.time()
            key.get_contents_to_file(fp)
            end = time.time()
            elapsed = end - start
            queue.put(
                dict(
                    type='r',
                    bucket=bucket.name,
                    key=key.name,
                    #TODO chunks
                    start=start,
                    duration=int(round(elapsed * NANOSECOND)),
                    #TODO error, fp.valid()
                    #TODO name
                    ),
                )
            count += 1
        if count == 0:
            gevent.sleep(1)

def writer(bucket, name, queue, quantity=1, file_size=1, file_stddev=0, file_name_seed=None):
    while True:
        r = random.randint(0, 65535)
        r2 = r
        if file_name_seed != None:
            r2 = file_name_seed

        files = generate_objects.get_random_files(
            quantity=quantity,
            mean=1024 * file_size,
            stddev=1024 * file_stddev,
            seed=r,
            )

        start = time.time()
        generate_objects.upload_objects(bucket, files, r2)
        end = time.time()
        elapsed = end - start

        queue.put(
            dict(
                type='w',
                bucket=bucket.name,
                #TODO this current combines stats for multiple files? key=key.name,
                #TODO chunks
                start=start,
                duration=int(round(elapsed * NANOSECOND)),
                #TODO error
                ),
            )

def parse_options():
    parser = optparse.OptionParser()
    parser.add_option("-t", "--time", dest="duration", type="float",
        help="duration to run tests (seconds)", default=5, metavar="SECS")
    parser.add_option("-r", "--read", dest="num_readers", type="int",
        help="number of reader threads", default=0, metavar="NUM")
    parser.add_option("-w", "--write", dest="num_writers", type="int",
        help="number of writer threads", default=2, metavar="NUM")
    parser.add_option("-s", "--size", dest="file_size", type="float",
        help="file size to use, in kb", default=1024, metavar="KB")
    parser.add_option("-q", "--quantity", dest="quantity", type="int",
        help="number of files per batch", default=1, metavar="NUM")
    parser.add_option("-d", "--stddev", dest="stddev", type="float",
        help="stddev of file size", default=0, metavar="KB")
    parser.add_option("-W", "--rewrite", dest="rewrite", action="store_true",
        help="rewrite the same files (total=quantity)")
    parser.add_option("--no-cleanup", dest="cleanup", action="store_false",
        help="skip cleaning up all created buckets", default=True)

    return parser.parse_args()

def main():
    # parse options
    (options, args) = parse_options()

    try:
        # setup
        common.setup()
        bucket = common.get_new_bucket()
        print "Created bucket: {name}".format(name=bucket.name)
        r = None
        if (options.rewrite):
            r = random.randint(0, 65535)
        q = gevent.queue.Queue()

        # main work
        print "Using file size: {size} +- {stddev}".format(size=options.file_size, stddev=options.stddev)
        print "Spawning {r} readers and {w} writers...".format(r=options.num_readers, w=options.num_writers)
        group = gevent.pool.Group()
        for x in xrange(options.num_writers):
            group.spawn(writer, bucket,
                        name=x,
                        queue=q,
                        file_size=options.file_size,
                        file_stddev=options.stddev,
                        quantity=options.quantity,
                        file_name_seed=r,
                        )
        for x in xrange(options.num_readers):
            group.spawn(reader, bucket,
                        name=x,
                        queue=q,
                        )
        def stop():
            group.kill(block=True)
            q.put(StopIteration)
        gevent.spawn_later(options.duration, stop)

        yaml.safe_dump_all(q, stream=sys.stdout, default_flow_style=False)

    finally:
        # cleanup
        if options.cleanup:
            common.teardown()
