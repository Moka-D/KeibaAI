#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import datetime as dt
import pandas as pd
import re
from typing import Dict, List, Set
from common import InvalidArgument, get_environment
from dbapi import DBManager
from utils import create_race_id_list, get_all_race_id, judge_region
from scraping import scrape_horse_peds, scrape_race_info
if get_environment() == 'Jupyter':
    from tqdm.notebook import tqdm
else:
    from tqdm import tqdm


DB_FILEPATH: str = "D:\\Masatoshi\\Work\\db\\keiba.db"


class Registar:
    def __init__(self, db_path: str) -> None:
        self._dbm = DBManager(db_path)

    def regist_race_results(self, race_id_list: List[str]) -> Set[str]:
        """
        レース結果をDBに登録する関数

        Parameters
        ----------
        race_id_list : list[str]
            レースIDのリスト
        """
        horse_id_set = set()

        for race_id in tqdm(race_id_list):
            if judge_region(race_id) == 'Harnes':
                continue

            if not self._dbm.is_id_inserted('race_info', race_id):
                try:
                    race_info, results, payoff_table = scrape_race_info(race_id)
                except ValueError:
                    continue

                self.regist_race_info(race_id, race_info)
                self.regist_horse(dict(zip(results['horse_id'], results['馬名'])))
                self.regist_jockey(dict(zip(results['jockey_id'], results['騎手'])))
                self.regist_trainer(dict(zip(results['trainer_id'], results['調教師'])))
                self.regist_result(race_id, results)
                self.regist_payoff(race_id, payoff_table)

                horse_id_set |= set(results['horse_id'].to_list())

        return horse_id_set

    def regist_per_year(self, year_list: List[int]) -> Set[str]:
        horse_id_set = set()
        for year in year_list:
            try:
                race_id_list = create_race_id_list(year)
            except InvalidArgument as e:
                print(e)
                break

            horse_id_set |= self.regist_race_results(race_id_list)
            print("Race data of {} has been inserted successfully.".format(year))

        return horse_id_set

    def regist_horse(self, horse_dict: Dict[str, str]):
        for id, name in horse_dict.items():
            if not self._dbm.is_id_inserted('horse', id):
                sql = 'INSERT INTO horse VALUES (?,?,?,?,?,?,?,?)'
                try:
                    peds = scrape_horse_peds(id)
                    data = (id, name, peds[0], peds[1], peds[2], peds[3], peds[4], peds[5])
                except Exception as e:
                    print(e + 'at horse_id:' + id)
                    data = (id, name, None, None, None, None, None, None)

                self._dbm.insert_data(sql, data)

    def regist_jockey(self, jockey_dict: Dict[str, str]):
        for id, name in jockey_dict.items():
            if not self._dbm.is_id_inserted('jockey', id):
                sql = 'INSERT INTO jockey VALUES (?,?)'
                data = (id, name)
                self._dbm.insert_data(sql, data)

    def regist_trainer(self, trainer_dict: Dict[str, str]):
        for id, name in trainer_dict.items():
            if not self._dbm.is_id_inserted('trainer', id):
                sql = 'INSERT INTO trainer VALUES (?,?)'
                data = (id, name)
                self._dbm.insert_data(sql, data)

    def regist_race_info(self, race_id:str, race_info: Dict[str, str]):
        sql = 'INSERT INTO race_info VALUES (?,?,?,?,?,?,?,?,?,?,?,?)'
        race_date = int(dt.datetime.strptime(race_info['date'], '%Y/%m/%d').date().strftime('%Y%m%d'))
        hold_no = pd.to_numeric(race_id[6:8], errors='coerce')
        hold_day = pd.to_numeric(race_id[8:10], errors='coerce')
        race_no = pd.to_numeric(race_id[10:12], errors='coerce')
        data = (race_id, race_info['title'], race_date, race_id[4:6], hold_no,
                hold_day, race_no, int(race_info['course_dist']),
                race_info['race_type'], race_info['turn'],
                race_info['ground_state'], race_info['weather'])
        self._dbm.insert_data(sql, data)

    def regist_result(self, race_id: str, results: pd.DataFrame):
        sql = 'INSERT INTO results VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
        for _, row in results.iterrows():
            data = (race_id, row['馬番'], row['枠番'], row['着順'], row['horse_id'],
                    row['性齢'], row['斤量'], row['jockey_id'], row['タイム'], row['着差'],
                    row['通過'], row['上り'], row['単勝'], row['人気'], row['馬体重'],
                    row['trainer_id'], row['馬主'], row['賞金（万円）'])
            self._dbm.insert_data(sql, data)

    def regist_payoff(self, race_id: str, payoff: pd.DataFrame):
        payoff_tmp = payoff.copy()
        payoff_tmp[2] = payoff_tmp[2].map(lambda x: re.findall(r'\d+,*\d+', x)[0]).str.replace(',', '').astype(int)
        payoff_tmp[3] = payoff_tmp[3].map(lambda x: re.findall(r'\d+', x)[0]).astype(int)
        sql = 'INSERT INTO race_payoff VALUES (?,?,?,?,?)'
        for _, row in payoff_tmp.iterrows():
            data = (race_id, row[0], row[1], row[2], row[3])
            self._dbm.insert_data(sql, data)


def main(args):
    year_list = []
    if len(args) >= 2:
        for i in range(1, len(args)):
            if args[i].isdigit():
                year_list.append(int(args[i]))
            else:
                print('Argument is not digit')
                return
        reg = Registar("\\\\mokad-pi-omv\\public\\99_work\\keiba.db")
        horse_id_set = reg.regist_per_year(year_list)
        print(horse_id_set)
    else:
        print('Arguments are too short')


if __name__ == '__main__':
    main(sys.argv)
