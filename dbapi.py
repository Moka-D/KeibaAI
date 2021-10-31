#!/usr/bin/python
# -*- coding: utf-8 -*-
import sqlite3
import pandas as pd


class DBManager:
    def __init__(self, db_filepath: str) -> None:
        self.path = db_filepath
        self.conn = sqlite3.connect(self.path)

    @classmethod
    def open(cls, db_filepath: str):
        return cls(db_filepath)

    def close(self) -> None:
        self.conn.close()

    def insert_result_table(self, race_id: str, result_df: pd.DataFrame) -> None:
        race_id_i = int(race_id)
        cur = self.conn.cursor()

        # horse_id未登録チェック
        horse_id_list = dict(zip(result_df['horse_id'], result_df['horse']))

        for _, row in result_df.iterrows():
            cur.execute('INSERT INTO result(race_id, horse_no, frame_no, ' \
                        'arriving_order, horse_id, horse_age, horse_sex, impost, ' \
                        'jockey_id, goal_time, margin_length, corner_pass, ' \
                        'last_three_furlong, win_odds, popular_order, horse_weight, ' \
                        'horse_weight_change, trainer_id, owner_name, prise) ' \
                        'VALUES(%d, )')

        self.conn.commit()
        cur.close()

    def insert_race_info(self, race_id: str, race_info: dict[str, str]) -> None:
        pass

    def insert_horse_table(self, horse_id: str, horse_df: pd.DataFrame) -> None:
        pass

    def is_horse_exist(self, horse_id: str):
        pass
