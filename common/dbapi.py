# -*- coding: utf-8 -*-
import os
import glob
import sqlite3
import numpy as np
from typing import Any, List, Tuple, Union
import pandas as pd
from utils import InvalidArgument
import warnings


class DBManager:
    """データベース管理クラス

    Parameters
    ----------
    db_filepath : str
        dbファイルへのパス
    """

    def __init__(self, db_filepath: str) -> None:
        # 存在しないdbファイルの場合は、各tableを作成する
        if not os.path.exists(db_filepath):
            print('Creating tables...')
            self._conn = sqlite3.connect(db_filepath)
            cur = self._conn.cursor()
            paths = map(os.path.abspath, glob.glob('./sql/*.sql', recursive=False))
            paths = filter(os.path.isfile, paths)
            paths = sorted(paths)
            for _, p in enumerate(paths):
                with open(p) as f:
                    sql = f.read()
                    cur.execute(sql)
            cur.close()
        else:
            self._conn = sqlite3.connect(db_filepath)

    def __del__(self) -> None:
        self._conn.close()

    def is_id_inserted(self, table_name: str, id: str) -> bool:
        if table_name not in ['race_info', 'horse', 'jockey', 'trainer']:
            raise InvalidArgument("invalid argument of table_name: '{}'".format(table_name))

        if table_name == 'race_info':
            sql = 'SELECT * FROM {} WHERE race_id="{}"'.format(table_name, id)
        else:
            sql = 'SELECT * FROM {} WHERE id="{}"'.format(table_name, id)

        cur = self._conn.cursor()
        try:
            cur.execute(sql)
            if cur.fetchall():
                ret = True
            else:
                ret = False
        except sqlite3.Error as e:
            print("sqlite3.Error occurred:", e.args[0])
            ret = False
        finally:
            cur.close()

        return ret

    def select_resutls(self, where_list: List[str] = []) -> Union[pd.DataFrame, None]:
        sql = 'SELECT * FROM results INNER JOIN race_info USING(race_id)'
        if where_list:
            sql += ' WHERE '
        try:
            return pd.read_sql(sql, self._conn)
        except sqlite3.Error as e:
            print("sqlite3.Error occurred:", e.args[0])

    def select_payoffs(self) -> pd.DataFrame:
        sql = 'SELECT * FROM race_payoff'
        return pd.read_sql(sql, self._conn)

    def select_race_infos(self) -> pd.DataFrame:
        sql = 'SELECT * FROM race_info'
        return pd.read_sql(sql, self._conn)

    def select_horse_peds(self) -> pd.DataFrame:
        sql = 'SELECT id, father, mother, fathers_father, fathers_mother, mothers_father, mothers_mother FROM horse'
        df = pd.read_sql(sql, self._conn)
        return df.set_index('id')

    def get_horse_id_list(self) -> List[str]:
        sql = 'SELECT id FROM horse'
        df = pd.read_sql(sql, self._conn)
        return df['id'].values.tolist()

    def insert_data(self, sql: str, data: Tuple[Any]) -> None:
        cur = self._conn.cursor()
        try:
            cur.execute(sql, data)
            self._conn.commit()
        except sqlite3.Error as e:
            print("sqlite3.Error occurred:", e.args[0])
            self._conn.rollback()
        finally:
            cur.close()

    def update_data(self, sql: str):
        cur = self._conn.cursor()
        try:
            cur.execute(sql)
            self._conn.commit()
        except sqlite3.Error as e:
            print("sqlite3.Error occurred:", e.args[0])
            self._conn.rollback()
        finally:
            cur.close()

    def select_data(self, sql: str):
        try:
            df = pd.read_sql(sql, self._conn)
        except sqlite3.Error as e:
            print("sqlite3.Error occurred:", e.args[0])
            df = pd.DataFrame()
        return df

    def is_horse_results_inserted(self, horse_id: str, race_id: str) -> bool:
        if race_id is None:
            sql = 'SELECT * FROM horse_results WHERE horse_id="{}"'.format(horse_id)
        else:
            sql = 'SELECT * FROM horse_results WHERE horse_id="{}" and race_id="{}"'.format(horse_id, race_id)
        cur = self._conn.cursor()

        try:
            cur.execute(sql)
            if cur.fetchall():
                ret = True
            else:
                ret = False
        except sqlite3.Error as e:
            print("sqlite3.Error occurred:", e.args[0])
            ret = False
        finally:
            cur.close()

        return ret

    def get_race_id_list(self):
        sql = 'SELECT race_id FROM race_info'
        df = pd.read_sql(sql, self._conn)
        return df['race_id'].values.tolist()

    def get_jockey_id(self, jockey_name: str):
        sql = 'SELECT id FROM jockey WHERE name="{}"'.format(jockey_name)
        cur = self._conn.cursor()

        try:
            cur.execute(sql)
            jockey_id_list = cur.fetchall()
            if len(jockey_id_list) == 0:
                warnings.warn("jockey_name:'{}' does not exists. Retun NaN.".format(jockey_name))
                jockey_id = np.nan
            elif len(jockey_id_list) > 1:
                warnings.warn("Multi records of jockey_name:'{}' exist. Return NaN.".format(jockey_name))
                jockey_id = np.nan
            else:
                jockey_id = jockey_id_list[0][0]
        except sqlite3.Error as e:
            print("sqlite3.Error occurred:", e.args[0])
        finally:
            cur.close()

        return jockey_id
