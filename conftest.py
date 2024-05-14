import configparser
import json
import logging
import os
import re
import shutil
import sys
import time
from pathlib import Path

import allure
import jinja2
import pexpect
import pytest
from helpers.common import ASSETS_DIR
from helpers.wallet_helpers import create_wallet
from neofs_testlib.env.env import NeoFSEnv, NodeWallet
from neofs_testlib.reporter import AllureHandler, get_reporter
from neofs_testlib.utils.wallet import (
    get_last_address_from_wallet,
    get_last_public_key_from_wallet,
)

from s3tests_boto3.functional import setup

get_reporter().register_handler(AllureHandler())
logger = logging.getLogger("NeoLogger")


def pytest_addoption(parser):
    parser.addoption(
        "--persist-env", action="store_true", default=False, help="persist deployed env"
    )
    parser.addoption("--load-env", action="store", help="load persisted env from file")


def _run_with_passwd(cmd: str, password: str) -> str:
    child = pexpect.spawn(cmd)
    child.delaybeforesend = 1
    child.expect(".*")
    child.sendline(f"{password}\r")
    if sys.platform == "darwin":
        child.expect(pexpect.EOF)
        cmd = child.before
    else:
        child.wait()
        cmd = child.read()
    return cmd.decode()


def read_config(config_file):
    config = configparser.ConfigParser()
    config.read(config_file)
    return config


@allure.step("Init S3 Credentials")
def init_s3_credentials(
    wallet: NodeWallet,
    neofs_env: NeoFSEnv,
) -> tuple:
    gate_public_key = get_last_public_key_from_wallet(
        neofs_env.s3_gw.wallet.path, neofs_env.s3_gw.wallet.password
    )
    
    cmd = (
        f"{neofs_env.neofs_s3_authmate_path} --debug --with-log --timeout 1m "
        f"issue-secret --wallet {wallet.path} --gate-public-key={gate_public_key} "
        f"--peer {neofs_env.storage_nodes[0].endpoint} "
        f"--bearer-rules {os.getcwd()}/bearer_rules.json --container-placement-policy 'REP 1' "
        f"--container-policy {os.getcwd()}/container_policy.json"
    )
    
    logger.info(f"Executing command: {cmd}")

    try:
        output = _run_with_passwd(cmd, wallet.password)
        
        logger.info(f"output: {output}")

        # output contains some debug info and then several JSON structures, so we find each
        # JSON structure by curly brackets (naive approach, but works while JSON is not nested)
        # and then we take JSON containing secret_access_key
        json_blocks = re.findall(r"\{.*?\}", output, re.DOTALL)
        for json_block in json_blocks:
            try:
                parsed_json_block = json.loads(json_block)
                if "secret_access_key" in parsed_json_block:
                    return (
                        parsed_json_block["access_key_id"],
                        parsed_json_block["secret_access_key"],
                        parsed_json_block["wallet_public_key"],
                        get_last_address_from_wallet(wallet.path, wallet.password),
                    )
            except json.JSONDecodeError:
                raise AssertionError(f"Could not parse info from output\n{output}")
        raise AssertionError(f"Could not find AWS credentials in output:\n{output}")
    except Exception as exc:
        raise RuntimeError(
            f"Failed to init s3 credentials because of error\n{exc}"
        ) from exc


def create_dir(dir_path: str) -> None:
    with allure.step("Create directory"):
        remove_dir(dir_path)
        os.mkdir(dir_path)


def remove_dir(dir_path: str) -> None:
    with allure.step("Remove directory"):
        shutil.rmtree(dir_path, ignore_errors=True)


@pytest.fixture(scope="session")
@allure.title("Prepare tmp directory")
def temp_directory() -> str:
    with allure.step("Prepare tmp directory"):
        full_path = os.path.join(os.getcwd(), ASSETS_DIR)
        create_dir(full_path)

    yield full_path

    with allure.step("Remove tmp directory"):
        remove_dir(full_path)


@pytest.fixture(scope="session", autouse=True)
def neofs_setup(request, temp_directory):
    if request.config.getoption("--load-env"):
        neofs_env = NeoFSEnv.load(request.config.getoption("--load-env"))
    else:
        neofs_env = NeoFSEnv.simple()

    neofs_env.neofs_adm().morph.set_config(
        rpc_endpoint=f"http://{neofs_env.morph_rpc}",
        alphabet_wallets=neofs_env.alphabet_wallets_dir,
        post_data="ContainerFee=0 ContainerAliasFee=0 MaxObjectSize=524288",
    )
    time.sleep(30)

    main_wallet = create_wallet()

    (
        main_access_key_id,
        main_secret_access_key,
        main_wallet_public_key,
        main_wallet_address,
    ) = init_s3_credentials(main_wallet, neofs_env)

    alt_wallet = create_wallet()

    (alt_access_key_id, alt_secret_access_key, alt_wallet_public_key, _) = (
        init_s3_credentials(alt_wallet, neofs_env)
    )

    jinja_env = jinja2.Environment()
    config_template = Path("s3tests.conf.SAMPLE").read_text()
    jinja_template = jinja_env.from_string(config_template)
    rendered_config = jinja_template.render(
        S3_HOST=neofs_env.s3_gw.address.split(":")[0],
        S3_PORT=neofs_env.s3_gw.address.split(":")[1],
        S3_TLS=True,
        S3_MAIN_DISPLAY_NAME=main_wallet_address,
        S3_MAIN_USER_ID=main_wallet_public_key,
        S3_MAIN_ACCESS_KEY=main_access_key_id,
        S3_MAIN_SECRET_KEY=main_secret_access_key,
        S3_ALT_USER_ID=alt_wallet_public_key,
        S3_ALT_ACCESS_KEY=alt_access_key_id,
        S3_ALT_SECRET_KEY=alt_secret_access_key,
        S3_TENANT_USER_ID=alt_wallet_public_key,
        S3_TENANT_ACCESS_KEY=alt_access_key_id,
        S3_TENANT_SECRET_KEY=alt_secret_access_key,
        S3_IAM_USER_ID=alt_wallet_public_key,
        S3_IAM_ACCESS_KEY=alt_access_key_id,
        S3_IAM_SECRET_KEY=alt_secret_access_key,
    )
    with open("s3tests.conf", mode="w") as fp:
        fp.write(rendered_config)

    os.environ["S3TEST_CONF"] = "s3tests.conf"

    yield neofs_env

    if request.config.getoption("--persist-env"):
        neofs_env.persist()
    else:
        if not request.config.getoption("--load-env"):
            neofs_env.kill()

    logs_path = os.path.join(os.getcwd(), ASSETS_DIR, "logs")
    os.makedirs(logs_path, exist_ok=True)

    shutil.copyfile(neofs_env.s3_gw.stderr, f"{logs_path}/s3_gw_log.txt")
    for idx, ir in enumerate(neofs_env.inner_ring_nodes):
        shutil.copyfile(ir.stderr, f"{logs_path}/ir_{idx}_log.txt")
    for idx, sn in enumerate(neofs_env.storage_nodes):
        shutil.copyfile(sn.stderr, f"{logs_path}/sn_{idx}_log.txt")

    logs_zip_file_path = shutil.make_archive("neofs_logs", "zip", logs_path)
    allure.attach.file(logs_zip_file_path, name="neofs logs", extension="zip")


@pytest.fixture(scope="session", autouse=True)
def nose_setup(neofs_setup):
    setup()
