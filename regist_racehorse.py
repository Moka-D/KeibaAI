#!/usr/bin/python

import re
import time
import datetime as dt
from common.db_register import DBRegistar
from common.log_api import get_module_logger
from config.db_config import db_config
from common.scrape import DATE_PATTERN, scrape_race_card, scrape_race_card_id_list
from common.utils import InvalidArgument, send_line_notify
import argparse


def main(race_date: str, is_local: bool = False):
    # 引数処理
    if re.fullmatch(DATE_PATTERN, race_date) is None:
        raise InvalidArgument("Argument Format -> 'yyyy/mm/dd'")

    msg = "Start scraping horse data of {}.".format(race_date)
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

    error_horse_list = []
    try:
        logger.debug("Start scraping 'race_id_list' of {}.".format(race_date))
        race_date_d = dt.datetime.strptime(race_date, '%Y/%m/%d').date()
        today = dt.datetime.today().date()

        if today == race_date_d:
            t_now = dt.datetime.now().time()
            t_limit = dt.datetime.strptime('8:00', '%H:%M').time()
            is_past = t_now > t_limit
        else:
            is_past = today > race_date_d

        try:
            race_id_list = scrape_race_card_id_list(race_date_d, is_past=is_past)
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

        for race_id in race_id_list:
            logger.debug("Start scraping horse data of race_id:{}.".format(race_id))
            try:
                race_card = scrape_race_card(race_id, race_date_d)
                error_horse_list += dbr.regist_horse(dict(zip(race_card['horse_id'], race_card['馬名'])),
                                                     with_horse_result=True)
            except AttributeError:
                logger.exception("'AttributeError' has been raised with race_id:{}".format(race_id))
    except Exception:
        logger.exception("Unexpected error has been occurred.")
        send_line_notify("[Error] Unexpected error has been occurred. Stop scraping.")
        return

    proc_time = time.time() - start_time
    msg = "Horse data of {} has been inserted successfully. Total time: {:.1f}[sec]".format(race_date, proc_time)
    send_line_notify("[INFO] " + msg)
    logger.info(msg)

    if error_horse_list:
        logger.warning("There is error_horse_list.")
        ids = "\n".join(error_horse_list)
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
