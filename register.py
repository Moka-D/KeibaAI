#!/usr/bin/python
# -*- coding: utf-8 -*-
from typing import List
from common import InvalidArgument, get_environment
from dbapi import DBManager
from utils import create_race_id_list
if get_environment() == 'Jupyter':
    from tqdm.notebook import tqdm
else:
    from tqdm import tqdm


DB_FILEPATH: str = "D:\\Masatoshi\\Work\\db\\keiba.db"


def insert_race_results(year_list: List[int], db_path: str = DB_FILEPATH) -> None:
    """
    レース結果をDBに登録する関数

    Parameters
    ----------
    year_list : list[int]
        年一覧
    db_path : str
        データベースファイルのパス
    """
    dbm = DBManager(db_path)

    for year in year_list:
        try:
            race_id_list = create_race_id_list(year)
        except InvalidArgument as e:
            print(e)
            continue

        for race_id in tqdm(race_id_list):
            dbm.insert_race_data_with_scrape(race_id)

        print("Race data of {} has been inserted successfully.".format(year))
