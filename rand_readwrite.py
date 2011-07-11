#!/usr/bin/python

import gevent
import gevent.queue
import gevent.monkey; gevent.monkey.patch_all()
import optparse
import time
import random

import generate_objects
import realistic
import common

class Result:
    TYPE_NONE = 0
    TYPE_READER = 1
    TYPE_WRITER = 2

    def __init__(self, name, type=TYPE_NONE, time=0, success=True, size=0, details=''):
        self.name = name
        self.type = type
        self.time = time
        self.success = success
        self.size = size
        self.details = details

    def __repr__(self):
        type_dict = {Result.TYPE_NONE: 'None', Result.TYPE_READER: 'Reader', Result.TYPE_WRITER: 'Writer'}
        type_s = type_dict[self.type]
        if self.success:
            status = 'Success'
        else:
            status = 'FAILURE'

        return "<Result: [{success}] {type}{name} -- {size} KB in {time}s = {mbps} MB/s {details}>".format(
            success=status,
            type=type_s,
            name=self.name,
            size=self.size,
            time=self.time,
            mbps=self.size / self.time / 1024.0,
            details=self.details
            )

def reader(seconds, bucket, name=None, queue=None):
    with gevent.Timeout(seconds, False):
        while (1):
            count = 0
            for key in bucket.list():
                fp = realistic.FileVerifier()
                start = time.clock()
                key.get_contents_to_file(fp)
                end = time.clock()
                elapsed = end - start
                if queue:
                    queue.put(
                        Result(
                            name,
                            type=Result.TYPE_READER,
                            time=elapsed,
                            success=fp.valid(),
                            size=fp.size / 1024,
                            ),
                        )
                count += 1
            if count == 0:
                gevent.sleep(1)

def writer(seconds, bucket, name=None, queue=None, quantity=1, file_size=1, file_stddev=0, file_name_seed=None):
    with gevent.Timeout(seconds, False):
        while (1):
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

            start = time.clock()
            generate_objects.upload_objects(bucket, files, r2)
            end = time.clock()
            elapsed = end - start

            if queue:
                queue.put(Result(name,
                    type=Result.TYPE_WRITER,
                    time=elapsed,
                    size=sum(f.size/1024 for f in files),
                    )
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
        greenlets = []
        greenlets += [gevent.spawn(writer, options.duration, bucket,
            name=x,
            queue=q,
            file_size=options.file_size,
            file_stddev=options.stddev,
            quantity=options.quantity,
            file_name_seed=r
            ) for x in xrange(options.num_writers)]
        greenlets += [gevent.spawn(reader, options.duration, bucket,
                name=x,
                queue=q
                ) for x in xrange(options.num_readers)]
        gevent.spawn_later(options.duration, lambda: q.put(StopIteration))

        total_read = 0
        total_write = 0
        read_success = 0
        read_failure = 0
        write_success = 0
        write_failure = 0
        for item in q:
            print item
            if item.type == Result.TYPE_READER:
                if item.success:
                    read_success += 1
                    total_read += item.size
                else:
                    read_failure += 1
            elif item.type == Result.TYPE_WRITER:
                if item.success:
                    write_success += 1
                    total_write += item.size
                else:
                    write_failure += 1

        # overall stats
        print "--- Stats ---"
        print "Total Read:  {read} MB ({mbps} MB/s)".format(
            read=(total_read/1024.0),
            mbps=(total_read/1024.0/options.duration)
            )
        print "Total Write: {write} MB ({mbps} MB/s)".format(
            write=(total_write/1024.0),
            mbps=(total_write/1024.0/options.duration)
            )
        print "Read filures: {num} ({percent}%)".format(
            num=read_failure,
            percent=(100.0*read_failure/max(read_failure+read_success, 1))
            )
        print "Write failures: {num} ({percent}%)".format(
            num=write_failure,
            percent=(100.0*write_failure/max(write_failure+write_success, 1))
            )

        gevent.joinall(greenlets, timeout=1)
    except Exception as e:
        print e
    finally:
        # cleanup
        if options.cleanup:
            common.teardown()

if __name__ == "__main__":
    main()
