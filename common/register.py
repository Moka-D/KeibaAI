# -*- coding: utf-8 -*-
import sys
import datetime as dt
import numpy as np
import pandas as pd
import re
from typing import Dict, List
from common import InvalidArgument, get_environment
from dbapi import DBManager
from utils import create_race_id_list, judge_region
from scrape import scrape_horse_peds, scrape_horse_results, scrape_race_info
if get_environment() == 'Jupyter':
    from tqdm.notebook import tqdm
else:
    from tqdm import tqdm
from db_config import db_config


class Registar:
    def __init__(self, db_path: str) -> None:
        self._dbm = DBManager(db_path)

    def regist_race_results(self, race_id_list: List[str]):
        """
        レース結果をDBに登録する関数

        Parameters
        ----------
        race_id_list : list[str]
            レースIDのリスト
        """
        for race_id in tqdm(race_id_list):
            if not self._dbm.is_id_inserted('race_info', race_id):
                try:
                    race_info, results, payoff_table = scrape_race_info(race_id)
                except ValueError:
                    continue
                except AttributeError:
                    continue

                self.regist_horse_peds(dict(zip(results['horse_id'], results['馬名'])))
                self.regist_jockey(dict(zip(results['jockey_id'], results['騎手'])))
                self.regist_trainer(dict(zip(results['trainer_id'], results['調教師'])))
                self.regist_race_info(race_id, race_info)
                self.regist_result(race_id, results)
                self.regist_payoff(race_id, payoff_table)
            else:
                print('race_id:{} has been inserted.'.format(race_id))

    def regist_per_year(self, year_list: List[int]):
        for year in year_list:
            try:
                race_id_list = create_race_id_list(year)
            except InvalidArgument as e:
                print(e)
                break

            self.regist_race_results(race_id_list)
            print("Race data of {} has been inserted successfully.".format(year))

    def regist_horse_results(self, horse_id_list: List[str] = None, with_jockey_id: bool = True):
        if horse_id_list is None:
            horse_id_list = self._dbm.get_horse_id_list()

        ng_id_list = []
        for horse_id in tqdm(horse_id_list):
            try:
                df = scrape_horse_results(horse_id, with_jockey_id)

                natinal_idx = df['race_id'].map(lambda x: judge_region(x) != 'Overseas')
                df.loc[natinal_idx, '賞金'] = df.loc[natinal_idx, '賞金'].fillna(0)

                for row in df.itertuples(name=None):
                    race_id = row[29]

                    if self._dbm.is_horse_results_inserted(horse_id=horse_id, race_id=race_id):
                        # 処理時間短縮のため、登録済みならスキップ
                        break

                    if with_jockey_id:
                        jockey_id = row[30]
                        if pd.notna(row[13]) and pd.notna(jockey_id):
                            jockey_dict = {}
                            jockey_dict[jockey_id] = row[13]
                            self.regist_jockey(jockey_dict)
                    else:
                        if pd.notna(row[13]):
                            jockey_id = self._dbm.get_jockey_id(row[13])
                        else:
                            jockey_id = np.nan

                    is_overseas = (judge_region(race_id) == 'Overseas')

                    if not self._dbm.is_id_inserted('race_info', race_id):
                        info = {
                            'date': int(dt.datetime.strptime(row[1], '%Y/%m/%d').date().strftime('%Y%m%d')),
                            'title': row[5],
                            'distance': int(re.findall(r'\d+', row[15])[0]),
                            'race_type': re.findall(r'\D+', row[15])[0],
                            'turn': np.nan,
                            'ground_state': row[16],
                            'weather': row[3],
                            'horse_num': row[7]
                        }

                        if is_overseas:
                            info['place_id'] = race_id[4:6]
                            info['hold_no'] = np.nan
                            info['hold_day'] = np.nan
                            info['race_no'] = int(race_id[10:12])
                        else:
                            info['place_id'] = str(int(race_id[4:6]))
                            info['hold_no'] = int(race_id[6:8])
                            info['hold_day'] = int(race_id[8:10])
                            info['race_no'] = row[4]

                        sql = 'INSERT INTO race_info VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)'
                        data = (race_id, info['title'], info['date'],
                                info['place_id'], info['hold_no'],
                                info['hold_day'], info['race_no'],
                                info['distance'], info['race_type'],
                                info['turn'], info['ground_state'],
                                info['weather'], info['horse_num'])
                        self._dbm.insert_data(sql, data)

                    sql = 'INSERT INTO horse_results VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
                    data = (horse_id, race_id, row[8], row[9], row[10],
                            row[11], row[12], jockey_id, row[14], row[18],
                            row[19], row[21], row[22], row[23], row[24],
                            row[28])
                    self._dbm.insert_data(sql, data)
            except Exception as e:
                print("'{}' has been raised with horse_id:'{}' ({})".format(e.__class__.__name__, horse_id, e.args[0]))
                ng_id_list.append(horse_id)

        return ng_id_list

    def regist_horse_peds(self, horse_dict: Dict[str, str]):
        for id, name in horse_dict.items():
            if not self._dbm.is_id_inserted('horse', id):
                sql = 'INSERT INTO horse VALUES (?,?,?,?,?,?,?,?)'
                try:
                    peds = scrape_horse_peds(id)
                    data = (id, name, peds[0], peds[1], peds[2], peds[3], peds[4], peds[5])
                except Exception as e:
                    print("'{}' has been raised while scraping peds of horse_id:'{}' ({})".format(e.__class__.__name__, id))
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

    def regist_race_info(self, race_id: str, race_info: Dict[str, str]):
        sql = 'INSERT INTO race_info VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)'
        race_date = int(dt.datetime.strptime(race_info['date'], '%Y/%m/%d').date().strftime('%Y%m%d'))
        data = (race_id, race_info['title'], race_date, str(int(race_id[4:6])), race_id[6:8],
                race_id[8:10], race_id[10:12], int(race_info['course_dist']),
                race_info['race_type'], race_info['turn'],
                race_info['ground_state'], race_info['weather'], race_info['horse_num'])
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
        for row in payoff_tmp.itertuples(name=None):
            data = (race_id, row[1], row[2], row[3], row[4])
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
        reg = Registar(db_config['path'])
        reg.regist_per_year(year_list)
    else:
        print('Arguments are too short')


if __name__ == '__main__':
    main(sys.argv)
