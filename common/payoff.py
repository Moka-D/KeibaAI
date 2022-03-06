#!/usr/bin/python
"""払い戻しテーブルモジュール
"""

import os
import sys
sys.path.append(os.pardir)
from logging import Logger
from typing import Dict
import pandas as pd
from common.db_api import DBManager
from common.utils import str_list_to_int, PAYOFF_KIND_TO_ID


class Payoff:
    def __init__(self, payoff_table: pd.DataFrame) -> None:
        self.table = payoff_table

    @classmethod
    def read_db(cls, db_config: Dict[str, str], logger: Logger = None) -> 'Payoff':
        dbm = DBManager(db_config, logger)
        df = dbm.select_payoffs()
        return cls(df)

    @property
    def tansho(self) -> pd.DataFrame:
        df = self.table[self.table['ticket_type'] == PAYOFF_KIND_TO_ID.get('単勝')][['race_id', 'pattern', 'payoff', 'popularity']]
        df['pattern'] = df['pattern'].astype(int)
        return df

    @property
    def fukusho(self) -> pd.DataFrame:
        df = self.table[self.table['ticket_type'] == PAYOFF_KIND_TO_ID.get('複勝')][['race_id', 'pattern', 'payoff', 'popularity']]
        df['pattern'] = df['pattern'].astype(int)
        return df

    @property
    def umaren(self) -> pd.DataFrame:
        df = self.table[self.table['ticket_type'] == PAYOFF_KIND_TO_ID.get('馬連')][['race_id', 'pattern', 'payoff', 'popularity']]
        df['pattern'] = df['pattern'].str.split('-').map(str_list_to_int)
        return df

    @property
    def sanrenfuku(self) -> pd.DataFrame:
        df = self.table[self.table['ticket_type'] == PAYOFF_KIND_TO_ID.get('3連複')][['race_id', 'pattern', 'payoff', 'popularity']]
        df['pattern'] = df['pattern'].str.split('-').map(str_list_to_int)
        return df

    @property
    def wakuren(self) -> pd.DataFrame:
        df = self.table[self.table['ticket_type'] == PAYOFF_KIND_TO_ID.get('枠連')][['race_id', 'pattern', 'payoff', 'popularity']]
        df['pattern'] = df['pattern'].str.split('-').map(str_list_to_int)
        return df

    @property
    def wide(self) -> pd.DataFrame:
        df = self.table[self.table['ticket_type'] == PAYOFF_KIND_TO_ID.get('ワイド')][['race_id', 'pattern', 'payoff', 'popularity']]
        df['pattern'] = df['pattern'].str.split('-').map(str_list_to_int)
        return df

    @property
    def umatan(self) -> pd.DataFrame:
        df = self.table[self.table['ticket_type'] == PAYOFF_KIND_TO_ID.get('馬単')][['race_id', 'pattern', 'payoff', 'popularity']]
        df['pattern'] = df['pattern'].str.split('→').map(str_list_to_int)
        return df

    @property
    def sanrentan(self) -> pd.DataFrame:
        df = self.table[self.table['ticket_type'] == PAYOFF_KIND_TO_ID.get('3連単')][['race_id', 'pattern', 'payoff', 'popularity']]
        df['pattern'] = df['pattern'].str.split('→').map(str_list_to_int)
        return df
