#!/usr/bin/python
"""DB APIモジュール
"""

import os
import sys

sys.path.append(os.pardir)
from logging import Logger
from typing import Any, Dict, List, Set, Tuple, Union

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from common.utils import InvalidArgument

DB_URL_BASE = 'postgresql://{user}:{password}@{host}:{port}/{dbname}'


class DBManager:
    """データベース管理クラス

    Parameters
    ----------
    db_config : Dict[str, str]
        DB設定
    """

    def __init__(self, db_config: Dict[str, str], logger: Logger = None) -> None:
        self._path = DB_URL_BASE.format(user=db_config['user'],
                                        password=db_config['pass'],
                                        host=db_config['host'],
                                        port=db_config['port'],
                                        dbname=db_config['name'])
        self._logger = logger

    def is_id_inserted(self, table_name: str, id: Union[int, str]) -> bool:
        """IDがテーブルに登録済みのものか判定

        Parameters
        ----------
        table_name : str ('race_info' | 'horse' | 'jockey' | 'trainer')
            テーブル名
        id : str
            ID

        Returns
        -------
        bool
            登録済みかどうか
        """
        # 引数チェック
        if table_name not in ['race_info', 'horse', 'jockey', 'trainer']:
            raise InvalidArgument("invalid argument of table_name: '{}'".format(table_name))

        # テーブル名でクエリを切り替え
        if table_name == 'race_info':
            query = "SELECT * FROM {} WHERE race_id={}".format(table_name, id)
        else:
            query = "SELECT * FROM {} WHERE id='{}'".format(table_name, id)

        # DBに接続
        with psycopg2.connect(self._path) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                if cur.fetchall():
                    ret = True
                else:
                    ret = False
        return ret

    def select_resutls(self) -> Union[pd.DataFrame, None]:
        query = 'SELECT * FROM results INNER JOIN race_info USING(race_id)'
        return self._read_df(query)

    def select_payoffs(self) -> pd.DataFrame:
        query = 'SELECT * FROM race_payoff'
        return self._read_df(query)

    def select_race_infos(self) -> pd.DataFrame:
        query = 'SELECT * FROM race_info'
        return self._read_df(query)

    def select_horse_peds(self) -> pd.DataFrame:
        query = 'SELECT id, father, mother, fathers_father, fathers_mother, mothers_father, mothers_mother FROM horse'
        df = self._read_df(query)
        return df.set_index('id')

    def select_horse_results(self):
        query = 'SELECT * FROM horse_results'
        return self._read_df(query)

    def select_horse_reuslts_with_list(self, horse_id_list: Union[List[str], Set[str]]):
        query = 'SELECT * FROM horse_results WHERE horse_id IN %(horse_id_list)s'
        params = {
            'horse_id_list': tuple(horse_id_list)
        }
        return self._read_df(query, params)

    def _read_df(self, query: str, params: Dict[str, Any] = None):
        conn = psycopg2.connect(self._path)
        try:
            with conn.cursor() as cur:
                if params is None:
                    cur.execute(query)
                else:
                    cur.execute(query, params)
                column_list = [d.name for d in cur.description]
                df = pd.DataFrame(cur.fetchall(), columns=column_list)
                dtype_dict = {}
                for d in cur.description:
                    if d.type_code == 1700:
                        dtype_dict[d.name] = 'float64'
                    if d.type_code == 1082:
                        dtype_dict[d.name] = 'datetime64'
                if len(dtype_dict) > 0:
                    df = df.astype(dtype_dict)
        except psycopg2.Error as e:
            self._handle_error_message("psycopg2.Error has been occurred.", e)
            df = pd.DataFrame()
        finally:
            conn.close()

        return df

    def get_horse_id_list(self) -> List[str]:
        query = 'SELECT id FROM horse'
        with psycopg2.connect(self._path) as conn:
            df = pd.read_sql(query, conn)
        return df['id'].values.tolist()

    def insert_data(self, query: str, data: Tuple[Any]) -> bool:
        if not data:
            return False

        with psycopg2.connect(self._path) as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(query, data)
                    conn.commit()
                    ret = True
                except psycopg2.Error as e:
                    self._handle_error_message("psycopg2.Error has been occurred.", e)
                    conn.rollback()
                    ret = False
        return ret

    def insert_fast(self, query: str, data: List[Tuple[Any]]) -> bool:
        if not data:
            return False

        with psycopg2.connect(self._path) as conn:
            with conn.cursor() as cur:
                try:
                    execute_values(cur, query, data)
                    conn.commit()
                    ret = True
                except psycopg2.Error as e:
                    self._handle_error_message("psycopg2.Error has been occurred.", e)
                    conn.rollback()
                    ret = False
        return ret

    def is_horse_results_inserted(self, horse_id: str, date: int = None) -> bool:
        if date is None:
            query = "SELECT * FROM horse_results WHERE horse_id='{}'".format(horse_id)
        else:
            query = "SELECT * FROM horse_results WHERE horse_id='{}' and race_date={}".format(horse_id, date)

        with psycopg2.connect(self._path) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                if cur.fetchall():
                    ret = True
                else:
                    ret = False
        return ret

    def get_race_id_list(self) -> List[int]:
        query = "SELECT race_id FROM race_info"
        with psycopg2.connect(self._path) as conn:
            df = pd.read_sql(query, conn)
        return df['race_id'].values.tolist()

    def get_jockey_id(self, jockey_name: str) -> Union[float, Any]:
        query = "SELECT id FROM jockey WHERE jockey_name='{}'".format(jockey_name)
        jockey_id = None
        with psycopg2.connect(self._path) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                jockey_id_list = cur.fetchall()
                if len(jockey_id_list) == 0:
                    self._logger.warning("jockey_name:'{}' does not exists. Retun 'None'.".format(jockey_name))
                elif len(jockey_id_list) > 1:
                    self._logger.warning("Multi records of jockey_name:'{}' exist. Return 'None'.".format(jockey_name))
                else:
                    jockey_id = jockey_id_list[0][0]
        return jockey_id

    def _handle_error_message(self, msg: str, e: Exception):
        if self._logger is None:
            print(msg, e.args[0], file=sys.stderr)
        else:
            self._logger.exception(msg)
