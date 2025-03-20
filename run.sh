#!/bin/bash

set -euo pipefail

readonly ENV="stage"
readonly LOG_DIR="/tmp"
readonly LOG="${LOG_DIR}/ceph-test-$(date +%d-%m-%H-%M-%S).log"
readonly NODE_EXPORTER_PATH="/mnt/node_exporter/data/ceph_test_status_${ENV}.prom"
readonly SELECTION_FILE=$(mktemp)

trap 'rm -f "${SELECTION_FILE}"' EXIT

###############################################
# Временнo для тестирования
if [ -d ~/venv/ceph-test ]; then
    source ~/venv/ceph-test/bin/activate
else
    echo "ERROR: Virtual environment not found. Please install tox in ~/venv/ceph-test"
    exit 1
fi
###############################################

cat << EOF > "${SELECTION_FILE}"
s3tests_boto3/functional/test_s3.py::test_object_write_read_update_read_delete
s3tests_boto3/functional/test_s3.py::test_multipart_upload_overwrite_existing_object
EOF

S3TEST_CONF=s3tests.conf S3_USE_SIGV4=1 tox -- --select-from-file "${SELECTION_FILE}" > "${LOG}" 2>&1

echo "s3_ceph_test{exitcode=\"${?}\", instance=\"$(hostname)\", environment=\"${ENV}\"} $(date +%s)" > "${NODE_EXPORTER_PATH}"

find "${LOG_DIR}" -type f -name "ceph-test*.log" -mtime +1 -delete 2>/dev/null