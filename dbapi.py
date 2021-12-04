#!/usr/bin/python
# -*- coding: utf-8 -*-
import os
import glob
import re
import sqlite3
from sqlite3.dbapi2 import SQLITE_PRAGMA
import pandas as pd
from common import InvalidArgument
from scraping import scrape_horse_peds, scrape_race_info
import datetime as dt


class DBManager:
    """
    データベース管理クラス

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

    def __del__(self):
        self._conn.close()

    def insert_race_data_with_scrape(self, race_id: str) -> None:
        """
        レースデータをスクレイピングしてDBに登録する関数

        Parameters
        ----------
        race_id : str
            レースID
        """
        race_id_i = int(race_id)
        cur = self._conn.cursor()
        try:
            if self.is_race_inserted('race_info', race_id_i, cur):
                raise Exception("race_id:{} already exists in results table".format(race_id_i))

            info, result, payoff = scrape_race_info(race_id)

            # レース情報
            sql = 'INSERT INTO race_info VALUES (?,?,?,?,?,?,?,?,?,?,?,?)'
            race_date = int(dt.datetime.strptime(info['date'], '%Y/%m/%d').date().strftime('%Y%m%d'))
            data = (race_id_i, info['title'], race_date, int(race_id[4:6]), int(race_id[6:8]), \
                    int(race_id[8:10]), int(race_id[10:12]), int(info['course_dist']), \
                    info['race_type'], info['turn'], info['ground_state'], info['weather'])
            cur.execute(sql, data)

            # 馬、騎手、調教師の登録
            self._insert_unduplicated('horse', dict(zip(result['horse_id'], result['馬名'])), cur)
            self._insert_unduplicated('jockey', dict(zip(result['jockey_id'], result['騎手'])), cur)
            self._insert_unduplicated('trainer', dict(zip(result['trainer_id'], result['調教師'])), cur)

            # レース結果
            sql = 'INSERT INTO results VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
            for _, row in result.iterrows():
                data = (race_id_i, row['馬番'], row['枠番'], row['着順'], row['horse_id'], \
                        row['性齢'], row['斤量'], row['jockey_id'], row['タイム'], row['着差'], \
                        row['通過'], row['上り'], row['単勝'], row['人気'], row['馬体重'], \
                        row['trainer_id'], row['馬主'], row['賞金（万円）'])
                cur.execute(sql, data)

            # 払い戻し表
            payoff_tmp = payoff.copy()
            payoff_tmp[2] = payoff_tmp[2].map(lambda x: re.findall(r'\d+,*\d+', x)[0]).str.replace(',', '').astype(int)
            payoff_tmp[3] = payoff_tmp[3].map(lambda x: re.findall(r'\d+', x)[0]).astype(int)
            sql = 'INSERT INTO race_payoff VALUES (?,?,?,?,?)'
            for _, row in payoff_tmp.iterrows():
                data = (race_id_i, row[0], row[1], row[2], row[3])
                cur.execute(sql, data)

            self._conn.commit()
        except ValueError:
            pass
        except Exception as e:
            print(e)
            print('race_id:' + race_id)
            self._conn.rollback()
        finally:
            cur.close()

    def _insert_unduplicated(self, table_name: str, data: dict[str, str], cur: sqlite3.Cursor) -> None:
        if table_name not in ['horse', 'jockey', 'trainer']:
            raise InvalidArgument("invalid argument of table_name: '{}'".format(table_name))

        for id, name in data.items():
            if not self._is_name_inserted(table_name, id, cur):
                if table_name == 'horse':
                    sql = 'INSERT INTO horse VALUES (?,?,?,?,?,?,?,?)'
                    try:
                        peds = scrape_horse_peds(id)
                        data = (id, name, peds[0], peds[1], peds[2], peds[3], peds[4], peds[5])
                    except Exception as e:
                        print(e + 'at horse_id:' + id)
                        data = (id, name, None, None, None, None, None, None)
                else:
                    sql = 'INSERT INTO {} VALUES (?,?)'.format(table_name)
                    data = (id, name)
                cur.execute(sql, data)

    def _is_name_inserted(self, table_name: str, id: str, cur: sqlite3.Cursor) -> bool:
        if table_name not in ['horse', 'jockey', 'trainer']:
            raise InvalidArgument("invalid argument of table_name: '{}'".format(table_name))

        sql = 'SELECT * FROM {} WHERE id = "{}"'.format(table_name, id)
        cur.execute(sql)
        if not cur.fetchall():
            return False
        else:
            return True

    def is_race_inserted(self, table_name: str, race_id: int, cur: sqlite3.Cursor) -> bool:
        if table_name not in ['results', 'race_info', 'race_payoff']:
            raise InvalidArgument("invalid argument of table_name: '{}'".format(table_name))

        sql = 'SELECT * FROM {} WHERE race_id = {}'.format(table_name, race_id)
        cur.execute(sql)
        if not cur.fetchall():
            return False
        else:
            return True
