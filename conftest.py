import configparser

import pytest

from s3tests_boto3.functional import setup


def read_config(config_file):
    config = configparser.ConfigParser()
    config.read(config_file)
    return config


@pytest.fixture(scope="session", autouse=True)
def nose_setup():
    setup()
