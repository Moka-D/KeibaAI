#!/usr/bin/python
# -*- coding: utf-8 -*-
import re
import sqlite3
from typing import final
import pandas as pd
from common import InvalidArgument
from scraping import scrape_horse_peds


class DBManager:
    def __init__(self, db_filepath: str) -> None:
        self.path = db_filepath

    def insert_race_data(self, race_id: str, info: dict[str, str], result: pd.DataFrame, payoff: pd.DataFrame) -> None:
        race_id_i = int(race_id)
        conn = sqlite3.connect(self.path)
        cur = conn.cursor()
        try:
            if self._is_race_inserted('race_info', race_id_i, cur):
                raise Exception("race_id:{} already exists in results table".format(race_id_i))

            # レース情報
            sql = 'INSERT INTO race_info VALUES (?,?,?,?,?,?,?,?,?,?,?,?)'
            data = (race_id_i, info['title'], info['date'], int(race_id[4:6]), int(race_id[6:8]), \
                    int(race_id[8:10]), int(race_id[10:12]), int(info['course_dist']), \
                    info['race_type'], info['turn'], info['ground_state'], info['weather'])
            cur.execute(sql, data)

            # 馬、騎手、調教師の登録
            self._insert_unduplicated('horse', dict(zip(result['horse_id'], result['馬名'])), cur)
            self._insert_unduplicated('jockey', dict(zip(result['jockey_id'], result['騎手'])), cur)
            self._insert_unduplicated('trainer', dict(zip(result['trainer_id'], result['調教師'])), cur)

            # レース結果
            sql = 'INSERT INTO results VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
            for _, row in result.iterrows():
                data = (race_id_i, row['馬番'], row['枠番'], row['着順'], row['horse_id'], \
                        row['年齢'], row['性'], row['斤量'], row['jockey_id'], row['タイム'], \
                        row['着差'], row['通過'], row['上り'], row['単勝'], row['人気'], \
                        row['体重'], row['体重変化'], row['trainer_id'], row['馬主'], row['賞金（万円）'])
                cur.execute(sql, data)

            # 払い戻し表
            payoff_tmp = payoff.copy()
            payoff_tmp[2] = payoff_tmp[2].map(lambda x: re.findall(r'\d+,*\d+', x)[0]).str.replace(',', '').astype(int)
            payoff_tmp[3] = payoff_tmp[3].map(lambda x: re.findall(r'\d+', x)[0]).astype(int)
            sql = 'INSERT INTO race_payoff VALUES (?,?,?,?,?)'
            for _, row in payoff_tmp.iterrows():
                data = (race_id_i, row[0], row[1], row[2], row[3])
                cur.execute(sql, data)

            conn.commit()
            print("All data of race_id:{} has been successfully committed.".format(race_id))
        except Exception as e:
            print(e)
            conn.rollback()
        finally:
            cur.close()
            conn.close()

    def _insert_unduplicated(self, table_name: str, data: dict[str, str], cur: sqlite3.Cursor) -> None:
        if table_name not in ['horse', 'jockey', 'trainer']:
            raise InvalidArgument("invalid argument of table_name: '{}'".format(table_name))

        for key, value in data.items():
            if not self._is_name_inserted(table_name, key, cur):
                if table_name == 'horse':
                    self._insert_horse_data(key, value, cur)
                else:
                    self._insert_name(table_name, key, value, cur)
            else:
                print("ID:{} already exists in {} table.".format(key, table_name))

    def _insert_horse_data(self, horse_id: str, horse_name: str, cur: sqlite3.Cursor) -> None:
        sql = 'INSERT INTO horse VALUES (?,?,?,?,?,?,?,?)'
        try:
            peds = scrape_horse_peds(horse_id)
            data = (horse_id, horse_name, peds[0], peds[1], peds[2], peds[3], peds[4], peds[5])
        except Exception as e:
            print(e + 'at horse_id:' + horse_id)
            data = (horse_id, horse_name, None, None, None, None, None, None)
        cur.execute(sql, data)

    def _insert_name(self, table_name: str, id: str, name: str, cur: sqlite3.Cursor) -> None:
        if table_name not in ['horse', 'jockey', 'trainer']:
            raise InvalidArgument("invalid argument of table_name: '{}'".format(table_name))

        sql = 'INSERT INTO {} VALUES (?,?)'.format(table_name)
        data = (id, name)
        cur.execute(sql, data)

    def _is_name_inserted(self, table_name: str, id: str, cur: sqlite3.Cursor) -> bool:
        if table_name not in ['horse', 'jockey', 'trainer']:
            raise InvalidArgument("invalid argument of table_name: '{}'".format(table_name))

        sql = 'SELECT * FROM {} WHERE {}_id = "{}"'.format(table_name, table_name, id)
        cur.execute(sql)
        if not cur.fetchall():
            return False
        else:
            return True

    def _is_race_inserted(self, table_name: str, race_id: int, cur: sqlite3.Cursor) -> bool:
        if table_name not in ['results', 'race_info', 'race_payoff']:
            raise InvalidArgument("invalid argument of table_name: '{}'".format(table_name))

        sql = 'SELECT * FROM {} WHERE race_id = {}'.format(table_name, race_id)
        cur.execute(sql)
        if not cur.fetchall():
            return False
        else:
            return True
