#!/usr/bin/python

import argparse
import datetime as dt
import time
from typing import List

from common.db_register import DBRegistar
from common.log_api import get_module_logger
from common.scrape import scrape_period_race_id_list
from common.utils import InvalidArgument, send_line_notify
from config.db_config import db_config


def main(year_list: List[int], is_local: bool = False):
    # 引数処理
    if not year_list:
        raise InvalidArgument('Arguments are too short. It needs 2 argumens at least.')

    msg = "Start scraping race data of {}.".format(year_list)
    send_line_notify("[INFO] " + msg)
    start_time = time.time()

    # Logging
    if is_local:
        log_dir = './data/logs'
        verbose = True
    else:
        log_dir = '/app/logs'
        verbose = False
    logger = get_module_logger(__name__, log_dir, verbose=verbose)
    logger.info(msg)

    error_horses = set()
    try:
        this_year = dt.date.today().year

        if is_local:
            logger.debug("Use local database config.")
            dbr = DBRegistar(db_config['local'], logger)
        else:
            logger.debug("Use server database config.")
            dbr = DBRegistar(db_config['write'], logger)

        for year in year_list:
            if year < 1975 or year > this_year:
                logger.error("Argument 'year' must be from 1975 to {}. (Actual:{})".format(this_year, year))
                continue

            logger.debug("Start scraping 'race_id_list' data of {}'s.".format(year))
            race_id_list = scrape_period_race_id_list(year)
            logger.debug("Start scraping race data of {}'s.".format(year))
            error_horses |= dbr.regist_race_results(race_id_list)
    except Exception:
        logger.exception("Unexpected error has been occurred.")
        send_line_notify("[Error] Unexpected error has been occurred. Stop scraping.")
        return

    proc_time = time.time() - start_time
    msg = "Race data of {} has been inserted successfully. Total time: {:.1f}[sec]".format(year_list, proc_time)
    send_line_notify("[INFO] " + msg)
    logger.info(msg)

    if error_horses:
        logger.warning("There is error_horse_list.")
        ids = "\n".join(error_horses)
        with open('./error_horses.txt', 'w') as f:
            f.write(ids)


if __name__ == '__main__':
    # 引数処理
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "years",
        help="scraping target years",
        nargs='*',
        type=int
    )
    parser.add_argument(
        "-l", "--local",
        action="store_true",
        help="use local database config"
    )
    args = parser.parse_args()

    main(args.years, args.local)
