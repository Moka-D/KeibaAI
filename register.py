#!/usr/bin/python
# -*- coding: utf-8 -*-
from datetime import time
import sys
import pandas as pd
from scraping import scrape_race_info, scrape_horse_peds
from dbapi import DBManager
from common import InvalidArgument
from utils import create_race_id_list


DB_FILEPATH = 'Z:\db\keiba.db'


def registe_race_results(year_list: list[int]) -> None:
    """
    レース結果をDBに登録する関数

    Parameters
    ----------
    year_list : list[int]
        年一覧
    """
    # データベース接続
    dbm = DBManager.open(DB_FILEPATH)

    for year in year_list:
        try:
            race_id_list = create_race_id_list(year)
        except InvalidArgument as e:
            print(e)
            continue

        for race_id in race_id_list:
            time.sleep(1)
            try:
                info, result, payoff = scrape_race_info(race_id)
            except IndexError:
                continue
            except Exception as e:
                print(e)
                print('race_id: '+ race_id)
                dbm.close()
                return

    dbm.close()


if __name__ == '__main__':
    year_list = []
    args = sys.argv
    n_args = len(args)
    if n_args >= 2:
        for i in range(1, n_args):
            if args[i].isdigit():
                year_list.append(int(args[i]))
            else:
                raise InvalidArgument("Argument is not digit.")
    else:
        raise InvalidArgument("Argumens are too short.")

    print(year_list)
