#!/usr/bin/python
# -*- coding: utf-8 -*-
import os
import glob
import sqlite3
from typing import Any, List, Tuple
import pandas as pd
from common import InvalidArgument


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

        ret = True
        cur = self._conn.cursor()
        if table_name == 'race_info':
            sql = 'SELECT * FROM {} WHERE race_id = "{}"'.format(table_name, id)
        else:
            sql = 'SELECT * FROM {} WHERE id = "{}"'.format(table_name, id)
        cur.execute(sql)
        if not cur.fetchall():
            ret = False

        cur.close()
        return ret

    def select_all_resutls(self) -> pd.DataFrame:
        sql = 'SELECT * FROM results INNER JOIN race_info USING(race_id)'
        return pd.read_sql(sql, self._conn)

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
            print("slite3.Error occurred:", e.args[0])
            self._conn.rollback()
        finally:
            cur.close()
