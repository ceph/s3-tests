#!/usr/bin/python

import gevent
import gevent.pool
import gevent.queue
import gevent.monkey; gevent.monkey.patch_all()
import optparse
import sys
import time
import traceback
import random
import yaml

import generate_objects
import realistic
import common

NANOSECOND = int(1e9)

def reader(bucket, worker_id, file_names, queue):
    while True:
        objname = random.choice(file_names)
        key = bucket.new_key(objname)

        fp = realistic.FileVerifier()
        result = dict(
                type='r',
                bucket=bucket.name,
                key=key.name,
                worker=worker_id,
                )

        start = time.time()
        try:
            key.get_contents_to_file(fp)
        except gevent.GreenletExit:
            raise
        except Exception as e:
            # stop timer ASAP, even on errors
            end = time.time()
            result.update(
                error=dict(
                    msg=str(e),
                    traceback=traceback.format_exc(),
                    ),
                )
            # certain kinds of programmer errors make this a busy
            # loop; let parent greenlet get some time too
            time.sleep(0)
        else:
            end = time.time()

            if not fp.valid():
                result.update(
                    error=dict(
                        msg='md5sum check failed',
                        ),
                    )

        elapsed = end - start
        result.update(
            start=start,
            duration=int(round(elapsed * NANOSECOND)),
            chunks=fp.chunks,
            )
        queue.put(result)

def writer(bucket, worker_id, file_names, files, queue):
    while True:
        fp = next(files)
        objname = random.choice(file_names)
        key = bucket.new_key(objname)

        result = dict(
            type='w',
            bucket=bucket.name,
            key=key.name,
            #TODO chunks
            worker=worker_id,
            )

        start = time.time()
        try:
            key.set_contents_from_file(fp)
        except gevent.GreenletExit:
            raise
        except Exception as e:
            # stop timer ASAP, even on errors
            end = time.time()
            result.update(
                error=dict(
                    msg=str(e),
                    traceback=traceback.format_exc(),
                    ),
                )
            # certain kinds of programmer errors make this a busy
            # loop; let parent greenlet get some time too
            time.sleep(0)
        else:
            end = time.time()

        elapsed = end - start
        result.update(
            start=start,
            duration=int(round(elapsed * NANOSECOND)),
            chunks=fp.last_chunks,
            )
        queue.put(result)

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
    parser.add_option("-d", "--stddev", dest="stddev", type="float",
        help="stddev of file size", default=0, metavar="KB")
    parser.add_option("-n", "--numfiles", dest="num_files", type="int",
        help="total number of files to write", default=1, metavar="NUM")
    parser.add_option("--seed", dest="seed", type="int",
        help="seed to use for random number generator", metavar="NUM")
    parser.add_option("--no-cleanup", dest="cleanup", action="store_false",
        help="skip cleaning up all created buckets", default=True)

    return parser.parse_args()

def write_file(bucket, file_name, file):
    """
    Write a single file to the bucket using the file_name.
    This is used during the warmup to initialize the files.
    """
    key = bucket.new_key(file_name)
    key.set_contents_from_file(file)

def main():
    # parse options
    (options, args) = parse_options()

    try:
        # setup
        common.setup()
        bucket = common.get_new_bucket()
        print "Created bucket: {name}".format(name=bucket.name)
        file_names = list(realistic.names(
            mean=15,
            stddev=4,
            seed=options.seed,
            max_amount=options.num_files
            ))
        files = realistic.files(
            mean=1024 * options.file_size,
            stddev=1024 * options.stddev,
            seed=options.seed,
            )
        q = gevent.queue.Queue()

        # warmup - get initial set of files uploaded
        print "Uploading initial set of {num} files".format(num=options.num_files)
        warmup_pool = gevent.pool.Pool(size=100)
        for file_name in file_names:
            file = next(files)
            warmup_pool.spawn_link_exception(
                write_file,
                bucket=bucket,
                file_name=file_name,
                file=file,
                )
        warmup_pool.join()

        # main work
        print "Starting main worker loop."
        print "Using file size: {size} +- {stddev}".format(size=options.file_size, stddev=options.stddev)
        print "Spawning {w} writers and {r} readers...".format(r=options.num_readers, w=options.num_writers)
        group = gevent.pool.Group()
        for x in xrange(options.num_writers):
            group.spawn_link_exception(
                writer,
                bucket=bucket,
                worker_id=x,
                file_names=file_names,
                files=files,
                queue=q,
                )
        for x in xrange(options.num_readers):
            group.spawn_link_exception(
                reader,
                bucket=bucket,
                worker_id=x,
                file_names=file_names,
                queue=q,
                )
        def stop():
            group.kill(block=True)
            q.put(StopIteration)
        gevent.spawn_later(options.duration, stop)

        yaml.safe_dump_all(q, stream=sys.stdout)

    finally:
        # cleanup
        if options.cleanup:
            common.teardown()
