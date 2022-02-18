#!/usr/bin/python

import datetime as dt
import time
import re
from common.db_register import DBRegistar
from common.log_api import get_module_logger
from config.db_config import db_config
from common.utils import InvalidArgument, send_line_notify
from common.scrape import scrape_period_race_id_list, DATE_PATTERN, scrape_race_card_id_list
import argparse


def main(race_date: str, is_local: bool = False):
    # 引数処理
    if re.fullmatch(DATE_PATTERN, race_date) is None:
        raise InvalidArgument("Argument Format -> 'yyyy/mm/dd'")

    msg = "Start scraping race data of {}.".format(race_date)
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
        logger.debug("Start scraping 'race_id_list' of {}.".format(race_date))
        race_date_i = dt.datetime.strptime(race_date, '%Y/%m/%d').date().strftime('%Y%m%d')
        try:
            race_id_list = scrape_race_card_id_list(race_date_i, is_past=True)
        except AttributeError:
            raise InvalidArgument("Invalid race date.")

        if not race_id_list:
            logger.error("'race_id_list' is null.")
            send_line_notify("[Error] Unexpected error has been occurred. Stop scraping.")
            return

        if is_local:
            logger.debug("Use local database config.")
            dbr = DBRegistar(db_config['local'], logger)
        else:
            logger.debug("Use server database config.")
            dbr = DBRegistar(db_config['write'], logger)

        logger.debug("Start scraping race data of {}.".format(race_date))
        error_horses = dbr.regist_race_results(race_id_list, with_horse_result=False)
    except Exception:
        logger.exception("Unexpected error has been occurred.")
        send_line_notify("[Error] Unexpected error has been occurred. Stop scraping.")
        return

    proc_time = time.time() - start_time
    msg = "Race data of {} has been inserted successfully. Total time: {:.1f}[sec]".format(race_date, proc_time)
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
        "race_date",
        help="the day that races take place (Format:'yyyy/mm/dd')"
    )
    parser.add_argument(
        "-l", "--local",
        action="store_true",
        help="use local database config"
    )
    args = parser.parse_args()

    main(args.race_date, args.local)