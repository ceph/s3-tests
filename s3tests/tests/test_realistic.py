import realistic
import shutil
import tempfile


# XXX not used for now
def create_files(mean=2000):
    return realistic.files2(
        mean=1024 * mean,
        stddev=1024 * 500,
        seed=1256193726,
        numfiles=4,
    )


class TestFiles(object):
    # the size and seed is what we can get when generating a bunch of files
    # with pseudo random numbers based on sttdev, seed, and mean.

    # this fails, demonstrating the problem
    def test_random_file_invalid(self):
        size = 2506764
        seed = 3391518755
        source = realistic.RandomContentFile(size=size, seed=seed)
        t = tempfile.SpooledTemporaryFile()
        shutil.copyfileobj(source, t)
        precomputed = realistic.PrecomputedContentFile(t)

        verifier = realistic.FileVerifier()
        shutil.copyfileobj(precomputed, verifier)

        assert verifier.valid()

    # this passes
    def test_random_file_valid(self):
        size = 2506001
        seed = 3391518755
        source = realistic.RandomContentFile(size=size, seed=seed)
        t = tempfile.SpooledTemporaryFile()
        shutil.copyfileobj(source, t)
        precomputed = realistic.PrecomputedContentFile(t)

        verifier = realistic.FileVerifier()
        shutil.copyfileobj(precomputed, verifier)

        assert verifier.valid()

