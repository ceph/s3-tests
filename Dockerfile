FROM ubuntu:14.04

RUN DEBIAN_FRONTEND=noninteractive apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get upgrade -y && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        libevent-dev \
        libxml2-dev \
        libxslt-dev \
        python-dev \
        python-pip \
        python-virtualenv \
        zlib1g-dev && \
    DEBIAN_FRONTEND=noninteractive apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY \
    bootstrap \
    requirements.txt \
    run-docker.sh \
    s3tests \
    setup.py \
    /opt/s3-tests/

WORKDIR /opt/s3-tests
RUN ./bootstrap

ENV \
    NOSETESTS_ATTR="" \
    S3TEST_CONF="/s3test.conf"

ENTRYPOINT ["/opt/s3-tests/run-docker.sh"]
