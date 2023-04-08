#!/usr/bin/python
"""DB登録処理モジュール
"""

import os
import sys

sys.path.append(os.pardir)
import datetime as dt
import re
from logging import Logger
from typing import Dict, List, Union

import pandas as pd

from common.db_api import DBManager
from common.scrape import (scrape_horse_peds, scrape_horse_results,
                           scrape_race_info)
from common.utils import PAYOFF_KIND_TO_ID, judge_region


class DBRegistar:
    """DB登録処理クラス

    Parameters
    ----------
    db_config : dict[str, str]
        DB設定
    """

    def __init__(self, db_config: Dict[str, str], logger: Logger) -> None:
        self._dbm = DBManager(db_config, logger)
        self._logger = logger

    def regist_race_results(self, race_id_list: List[str], with_horse_result: bool = False):
        """
        レース結果をDBに登録する関数

        Parameters
        ----------
        race_id_list : list[str]
            レースIDのリスト
        with_horse_result : bool, default False
            馬テーブルに登録済みの馬についても、過去結果の更新を行うかどうか

        Returns
        -------
        set[str]
            例外が発生した馬IDの一覧
        """
        error_horse_list = []
        for race_id in race_id_list:
            if self._dbm.is_id_inserted('race_info', race_id):
                self._logger.warning('race_id:{} has been inserted.'.format(race_id))
                continue

            try:
                race_info, results, payoff_table = scrape_race_info(race_id)
            except AttributeError:
                self._logger.exception("'AttributeError' has been raised with race_id:{}".format(race_id))
                continue

            error_horse_list += self.regist_horse(dict(zip(results['horse_id'], results['馬名'])), with_horse_result)
            self._insert_jockey(dict(zip(results['jockey_id'], results['騎手'])))
            self._insert_trainer(dict(zip(results['trainer_id'], results['調教師'])))
            self._insert_race_info(race_id, race_info)
            self._insert_result(race_id, results)
            self._insert_payoff(race_id, payoff_table)

        return set(error_horse_list)

    def regist_horse(
        self,
        horse_dict: Dict[str, str],
        with_horse_result: bool = False,
        with_jocky_id: bool = True
    ) -> List[str]:
        """馬の情報を登録

        Parameters
        ----------
        horse_dict : dict[str, str]
            馬IDと馬名の辞書
        with_horse_result : bool
            馬テーブルに登録済みの馬についても、過去結果の更新を行うかどうか
        with_jocky_id : bool, default True
            馬の過去結果に騎手IDを含めるかどうか

        Returns
        -------
        list[str]
            例外が発生した馬IDのリスト
        """

        error_horse_list = []

        peds_query = "INSERT INTO horse VALUES %s"
        peds_data_list = []

        results_query = "INSERT INTO horse_results VALUES %s"
        results_data_list = []

        for horse_id, horse_name in horse_dict.items():
            if not self._dbm.is_id_inserted('horse', horse_id):
                # DB未登録の馬の血統情報を登録
                is_new_horse = True
                try:
                    peds = scrape_horse_peds(horse_id)
                    peds_data_list += [(horse_id, horse_name, peds[0], peds[1], peds[2], peds[3], peds[4], peds[5],)]
                except IndexError:
                    self._logger.exception(
                        "'IndexError' has been raised while scraping peds of horse_id:{}".format(horse_id))
                    peds_data_list += [(horse_id, horse_name, None, None, None, None, None, None)]
                    error_horse_list.append(horse_id)
            else:
                is_new_horse = False

            if is_new_horse or with_horse_result:
                try:
                    results_data_list += self._create_horse_results_data(horse_id, with_jocky_id)
                except Exception as e:
                    self._logger.exception("'{}' has been raised with horse_id:{}".format(
                        e.__class__.__name__, horse_id))
                    error_horse_list.append(horse_id)

        # 外部キー例外が出るので、必ず血統->過去結果の順に登録
        self._dbm.insert_fast(peds_query, peds_data_list)
        self._dbm.insert_fast(results_query, results_data_list)

        return error_horse_list

    def regist_horse_results(
        self,
        horse_id_list: List[str],
        with_jocky_id: bool = True
    ):

        error_horse_list = []
        data_list = []
        query = "INSERT INTO horse_results VALUES %s"

        for horse_id in horse_id_list:
            try:
                data_list += self._create_horse_results_data(horse_id, with_jocky_id)
            except Exception as e:
                self._logger.exception("'{}' has been raised with horse_id:{}".format(e.__class__.__name__, horse_id))
                error_horse_list.append(horse_id)

        self._dbm.insert_fast(query, data_list)
        return error_horse_list

    def _create_horse_results_data(
        self,
        horse_id: str,
        with_jockey_id: bool = True
    ):

        data_list = []
        result_df = scrape_horse_results(horse_id, with_jockey_id)
        domestic_idx = result_df['race_id'].map(lambda x: judge_region(x) != 'Overseas')
        result_df.loc[domestic_idx, '賞金'] = result_df.loc[domestic_idx, '賞金'].fillna(0)

        for row in result_df.itertuples(name=None):
            race_id = row[29]

            race_date = int(dt.datetime.strptime(row[1], '%Y/%m/%d').date().strftime('%Y%m%d'))
            if self._dbm.is_horse_results_inserted(horse_id, race_date):
                # 処理時間短縮のため、登録済みならスキップ
                break

            if with_jockey_id:
                jockey_id = row[30]
                if pd.notna(row[13]) and pd.notna(jockey_id):
                    jockey_dict = {}
                    jockey_dict[jockey_id] = row[13]
                    self._insert_jockey(jockey_dict)
            else:
                if pd.notna(row[13]):
                    jockey_id = self._dbm.get_jockey_id(row[13])
                else:
                    jockey_id = None

            is_overseas = (judge_region(race_id) == 'Overseas')

            info = {
                'race_type': re.findall(r'\D+', row[15])[0],
                'distance': int(re.findall(r'\d+', row[15])[0])
            }

            if is_overseas:
                info['place_id'] = race_id[4:6]
                info['race_no'] = int(race_id[10:12])
            else:
                info['place_id'] = str(int(race_id[4:6]))
                info['race_no'] = row[4]

            data_list += [(horse_id, race_date, info['place_id'], row[3],
                           info['race_no'], row[7], row[8], row[9], row[10],
                           row[11], row[12], jockey_id, row[14],
                           info['race_type'], info['distance'], row[16],
                           row[18], row[19], row[21], row[22], row[23],
                           row[24], row[28],)]

        return data_list

    def _insert_jockey(self, jockey_dict: Dict[str, str]):
        query = "INSERT INTO jockey VALUES %s"
        data_list = []
        for id, name in jockey_dict.items():
            if not self._dbm.is_id_inserted('jockey', id):
                data_list += [(id, name,)]
        self._dbm.insert_fast(query, data_list)

    def _insert_trainer(self, trainer_dict: Dict[str, str]):
        query = "INSERT INTO trainer VALUES %s"
        data_list = []
        for id, name in trainer_dict.items():
            if not self._dbm.is_id_inserted('trainer', id):
                data_list += [(id, name,)]
        self._dbm.insert_fast(query, data_list)

    def _insert_race_info(self, race_id: str, race_info: Dict[str, Union[str, int]]):
        query = "INSERT INTO race_info VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        race_date = int(dt.datetime.strptime(race_info['date'], '%Y/%m/%d').date().strftime('%Y%m%d'))
        data = (race_id, race_info['title'], race_date, str(int(race_id[4:6])),
                race_id[6:8], race_id[8:10], race_id[10:12],
                int(race_info['course_dist']), race_info['race_type'],
                race_info['turn'], race_info['ground_state'],
                race_info['weather'], race_info['grade'],
                race_info['age_limit'], race_info['classification'],
                race_info['sex_limit'],)
        self._dbm.insert_data(query, data)

    def _insert_result(self, race_id: str, results: pd.DataFrame):
        query = "INSERT INTO results VALUES %s"
        data_list = []
        for _, row in results.iterrows():
            data_list += [(race_id, row['馬番'], row['枠番'], row['着順'], row['horse_id'],
                           row['性齢'], row['斤量'], row['jockey_id'], row['タイム'],
                           row['着差'], row['通過'], row['上り'], row['単勝'], row['人気'],
                           row['馬体重'], row['trainer_id'], row['馬主'], row['賞金（万円）'],)]
        self._dbm.insert_fast(query, data_list)

    def _insert_payoff(self, race_id: str, payoff: pd.DataFrame):
        payoff_tmp = payoff.copy()
        payoff_tmp[0] = payoff_tmp[0].map(PAYOFF_KIND_TO_ID)
        payoff_tmp[2] = payoff_tmp[2].map(lambda x: re.findall(r'[\d|,]+', x)[0]).str.replace(',', '').astype(int)
        payoff_tmp[3] = payoff_tmp[3].map(lambda x: re.findall(r'\d+', x)[0]).astype(int)
        query = "INSERT INTO race_payoff VALUES %s"
        data_list = []
        for row in payoff_tmp.itertuples(name=None):
            data_list += [(int(race_id), row[1], row[2], row[3], row[4],)]
        self._dbm.insert_fast(query, data_list)
