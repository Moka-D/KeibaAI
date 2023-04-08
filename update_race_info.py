#!/usr/bin/python

import sys
import time

from common.db_api import DBManager
from common.scrape import scrape_race_info
from common.utils import send_line_notify
from config.db_config import db_config


def main():
    send_line_notify("Start updating race_info table.")
    start_time = time.time()

    dbm = DBManager(db_config['write'])
    race_id_list = dbm.get_race_id_list()
    query = "UPDATE race_info SET grade=%s, age_limit=%s, classification=%s, sex_limit=%s WHERE race_id=%s"

    for race_id in race_id_list:
        race_info, _, _ = scrape_race_info(str(race_id))
        data = (race_info['grade'], race_info['age_limit'],
                race_info['classification'], race_info['sex_limit'], race_id,)
        if not dbm.insert_data(query, data):
            print("Error occurred at race_id:'{}'".format(race_id), file=sys.stderr)

    proc_time = time.time() - start_time
    send_line_notify("Finished updating race_info table. Total time: {:.1f}[sec]".format(proc_time))


if __name__ == '__main__':
    main()
