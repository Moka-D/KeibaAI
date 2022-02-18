#!/usr/bin/python

import os
import argparse
from typing import List
from logging import Logger
from common.db_register import DBRegistar
from common.log_api import get_module_logger
from config.db_config import db_config


ERROR_HORSE_LIST_PATH = './error_horses.txt'


def get_error_horse_id_list(logger: Logger = None):
    if os.path.exists(ERROR_HORSE_LIST_PATH):
        with open(ERROR_HORSE_LIST_PATH, 'r') as f:
            horse_id_list = f.read().splitlines()
    else:
        if logger is not None:
            logger.warning("Not exists 'error_horses'.")
        horse_id_list = []

    return horse_id_list


def main(horse_id_list: List[str] = None, is_local: bool = False):
    # Logging
    if is_local:
        log_dir = './data/logs'
        verbose = True
    else:
        log_dir = '/app/logs'
        verbose = False
    logger = get_module_logger(__name__, log_dir, verbose=verbose)
    logger.info("Start Scraping errored horse results.")

    if horse_id_list is None:
        horse_id_list = get_error_horse_id_list(logger)

    try:
        if is_local:
            logger.debug("Use local database config.")
            dbr = DBRegistar(db_config['local'], logger)
        else:
            logger.debug("Use server database config.")
            dbr = DBRegistar(db_config['write'], logger)

        dbr.regist_horse_results(horse_id_list, with_jocky_id=False)
    except Exception:
        logger.exception("Unexpected error has been occurred.")
        return

    logger.info("All horse data has been registered.")


if __name__ == '__main__':
    # 引数処理
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--horses",
        help="scraping target 'horse_id'",
        type=str,
        nargs='*'
    )
    parser.add_argument(
        "-l", "--local",
        action="store_true",
        help="use local database config"
    )
    args = parser.parse_args()

    main(args.horses, args.local)
