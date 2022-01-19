# -*- coding: utf-8 -*-
import os
import sys
sys.path.append(os.pardir)
from typing import List
import pandas as pd
from common.dbapi import DBManager


def str_list_to_int(x: List[str]) -> List[int]:
    return [int(n) for n in x]


class Payoff:
    def __init__(self, payoff_table: pd.DataFrame) -> None:
        self.table = payoff_table

    @classmethod
    def read_db(cls, db_path: str) -> 'Payoff':
        dbm = DBManager(db_path)
        df = dbm.select_payoffs()
        return cls(df.set_index('race_id'))

    @property
    def tansho(self) -> pd.DataFrame:
        df = self.table[self.table['ticket_type']=='単勝'][['pattern', 'payoff']]
        df['pattern'] = df['pattern'].astype(int)
        return df

    @property
    def fukusho(self) -> pd.DataFrame:
        df = self.table[self.table['ticket_type']=='複勝'][['pattern', 'payoff']]
        df['pattern'] = df['pattern'].astype(int)
        return df

    @property
    def umaren(self) -> pd.DataFrame:
        df = self.table[self.table['ticket_type']=='馬連'][['pattern', 'payoff']]
        df['pattern'] = df['pattern'].str.split('-').map(str_list_to_int)
        return df

    @property
    def sanrenfuku(self) -> pd.DataFrame:
        df = self.table[self.table['ticket_type']=='3連複'][['pattern', 'payoff']]
        df['pattern'] = df['pattern'].str.split('-').map(str_list_to_int)
        return df

    @property
    def wakuren(self) -> pd.DataFrame:
        df = self.table[self.table['ticket_type']=='枠連'][['pattern', 'payoff']]
        df['pattern'] = df['pattern'].str.split('-').map(str_list_to_int)
        return df

    @property
    def wide(self) -> pd.DataFrame:
        df = self.table[self.table['ticket_type']=='ワイド'][['pattern', 'payoff']]
        df['pattern'] = df['pattern'].str.split('-').map(str_list_to_int)
        return df

    @property
    def umatan(self) -> pd.DataFrame:
        df = self.table[self.table['ticket_type']=='馬単'][['pattern', 'payoff']]
        df['pattern'] = df['pattern'].str.split('→').map(str_list_to_int)
        return df

    @property
    def sanrentan(self) -> pd.DataFrame:
        df = self.table[self.table['ticket_type']=='3連単'][['pattern', 'payoff']]
        df['pattern'] = df['pattern'].str.split('→').map(str_list_to_int)
        return df
