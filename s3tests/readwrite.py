import gevent
import gevent.pool
import gevent.queue
import gevent.monkey; gevent.monkey.patch_all()
import itertools
import optparse
import os
import sys
import time
import traceback
import random
import yaml

import realistic
import common

NANOSECOND = int(1e9)

def reader(bucket, worker_id, file_names, queue, rand):
    while True:
        objname = rand.choice(file_names)
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

def writer(bucket, worker_id, file_names, files, queue, rand):
    while True:
        fp = next(files)
        objname = rand.choice(file_names)
        key = bucket.new_key(objname)

        result = dict(
            type='w',
            bucket=bucket.name,
            key=key.name,
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
    parser = optparse.OptionParser(
        usage='%prog [OPTS] <CONFIG_YAML',
        )
    parser.add_option("--no-cleanup", dest="cleanup", action="store_false",
        help="skip cleaning up all created buckets", default=True)

    return parser.parse_args()

def write_file(bucket, file_name, fp):
    """
    Write a single file to the bucket using the file_name.
    This is used during the warmup to initialize the files.
    """
    key = bucket.new_key(file_name)
    key.set_contents_from_file(fp)

def main():
    # parse options
    (options, args) = parse_options()

    common.setup()
    config = common.config
    bucket = None

    try:
        # setup
        real_stdout = sys.stdout
        sys.stdout = sys.stderr

        # verify all required config items are present
        if 'readwrite' not in config:
            raise RuntimeError('readwrite section not found in config')
        for item in ['readers', 'writers', 'duration', 'files']:
            if item not in config.readwrite:
                raise RuntimeError("Missing readwrite config item: {item}".format(item=item))
        for item in ['num', 'size', 'stddev']:
            if item not in config.readwrite.files:
                raise RuntimeError("Missing readwrite config item: files.{item}".format(item=item))

        seeds = dict(config.readwrite.get('random_seed', {}))
        seeds.setdefault('main', random.randrange(2**32))

        rand = random.Random(seeds['main'])

        for name in ['names', 'contents', 'writer', 'reader']:
            seeds.setdefault(name, rand.randrange(2**32))

        print 'Using random seeds: {seeds}'.format(seeds=seeds)

        # setup bucket and other objects
        bucket = common.get_new_bucket(common.s3.main)
        print "Created bucket: {name}".format(name=bucket.name)
        file_names = realistic.names(
            mean=15,
            stddev=4,
            seed=seeds['names'],
            )
        file_names = itertools.islice(file_names, config.readwrite.files.num)
        file_names = list(file_names)
        files = realistic.files2(
            mean=1024 * config.readwrite.files.size,
            stddev=1024 * config.readwrite.files.stddev,
            seed=seeds['contents'],
            )
        q = gevent.queue.Queue()

        # warmup - get initial set of files uploaded
        print "Uploading initial set of {num} files".format(num=config.readwrite.files.num)
        warmup_pool = gevent.pool.Pool(size=100)
        for file_name in file_names:
            fp = next(files)
            warmup_pool.spawn_link_exception(
                write_file,
                bucket=bucket,
                file_name=file_name,
                fp=fp,
                )
        warmup_pool.join()

        # main work
        print "Starting main worker loop."
        print "Using file size: {size} +- {stddev}".format(size=config.readwrite.files.size, stddev=config.readwrite.files.stddev)
        print "Spawning {w} writers and {r} readers...".format(w=config.readwrite.writers, r=config.readwrite.readers)
        group = gevent.pool.Group()
        rand_writer = random.Random(seeds['writer'])
        for x in xrange(config.readwrite.writers):
            this_rand = random.Random(rand_writer.randrange(2**32))
            group.spawn_link_exception(
                writer,
                bucket=bucket,
                worker_id=x,
                file_names=file_names,
                files=files,
                queue=q,
                rand=this_rand,
                )
        rand_reader = random.Random(seeds['reader'])
        for x in xrange(config.readwrite.readers):
            this_rand = random.Random(rand_reader.randrange(2**32))
            group.spawn_link_exception(
                reader,
                bucket=bucket,
                worker_id=x,
                file_names=file_names,
                queue=q,
                rand=this_rand,
                )
        def stop():
            group.kill(block=True)
            q.put(StopIteration)
        gevent.spawn_later(config.readwrite.duration, stop)

        yaml.safe_dump_all(q, stream=real_stdout)

    finally:
        # cleanup
        if options.cleanup:
            if bucket is not None:
                common.nuke_bucket(bucket)
