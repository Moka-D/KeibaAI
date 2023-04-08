#!/usr/bin/python

import argparse
import datetime as dt
import time

from common.db_register import DBRegistar
from common.log_api import get_module_logger
from common.scrape import scrape_period_race_id_list
from common.utils import InvalidArgument, send_line_notify
from config.db_config import db_config


def main(year: int, month: int, is_local: bool = False, with_horse_results: bool = False):
    # 引数処理
    this_year = dt.datetime.today().year
    if year < 1975 or year > this_year:
        raise InvalidArgument("Argument 'year' must be from 1975 to {}.".format(this_year))
    if month < 1 or month > 12:
        raise InvalidArgument("Argument 'month' must be from 1 to 12.")

    msg = "Start scraping monthly race data of {}/{}.".format(year, month)
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

    try:
        logger.debug("Start scraping 'race_id_list' data of {}/{}.".format(year, month))
        race_id_list = scrape_period_race_id_list(year=year,
                                                  start_month=month,
                                                  end_month=month)

        if is_local:
            logger.debug("Use local database config.")
            dbr = DBRegistar(db_config['local'], logger)
        else:
            logger.debug("Use server database config.")
            dbr = DBRegistar(db_config['write'], logger)

        logger.debug("Start scraping race data of {}/{}.".format(year, month))
        error_horses = dbr.regist_race_results(race_id_list, with_horse_result=with_horse_results)
    except Exception:
        logger.exception("Unexpected error has been occurred.")
        send_line_notify("[Error] Unexpected error has been occurred. Stop scraping.")
        return

    proc_time = time.time() - start_time
    msg = "Monthly race data of {}/{} has been inserted successfully. Total time: {:.1f}[sec]".format(
        year, month, proc_time)
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
        "year",
        help="scraping target year",
        type=int
    )
    parser.add_argument(
        "month",
        help="scraping target month",
        type=int
    )
    parser.add_argument(
        "-r", "--results",
        action="store_true",
        help="with horse old results"
    )
    parser.add_argument(
        "-l", "--local",
        action="store_true",
        help="use local database config"
    )
    args = parser.parse_args()

    main(args.year, args.month, args.local, args.results)
