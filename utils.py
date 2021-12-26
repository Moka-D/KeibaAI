#!/usr/bin/python
# -*- coding: utf-8 -*-
import datetime as dt
from typing import List, Tuple
import pandas as pd
from common import InvalidArgument


MAX_PLACE_NUM = 11
MAX_HOLD_NUM = 6
MAX_DAY_NUM = 13
MAX_RACE_NUM = 13


def create_race_id_list(year: int) -> List[str]:
    """
    指定した年の全レースIDを生成する関数

    Parameters
    ----------
    year : int
        年 (>= 1975 and <= 今年)

    Returns
    -------
    race_id_list : list[str]
        レースIDのリスト
    """
    # 引数チェック
    if year < 1975:
        raise InvalidArgument("引数 year は 1975 以上の整数を指定してください。")
    if year > dt.date.today().year:
        raise InvalidArgument("引数 year は %d 以下の整数を指定してください。" % dt.date.today().year)

    race_id_list = []
    for place in range(1, MAX_PLACE_NUM):
        for hold in range(1,MAX_HOLD_NUM):
            for day in range(1, MAX_DAY_NUM):
                for race in range(1, MAX_RACE_NUM):
                    race_id = "{:4d}{:0>2d}{:0>2d}{:0>2d}{:0>2d}".format(year, place, hold, day, race)
                    race_id_list.append(race_id)

    return race_id_list


def split_data(df: pd.DataFrame, test_size: float = 0.3) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    学習データとテストデータに分割する関数

    Parameters
    ----------
    df : pandas.DataFrame
        入力データ
    test_size : float, default 0.3
        テストデータサイズの割合

    Returns
    -------
    train : pandas.DataFrame
        学習データ
    test : pandas.DataFrame
        テストデータ
    """
    sorted_id_list = df.sort_values('date').index.unique()
    drop_threshold = round(len(sorted_id_list) * (1 - test_size))
    train_id_list = sorted_id_list[:drop_threshold]
    test_id_list = sorted_id_list[drop_threshold:]
    train = df.loc[train_id_list]
    test = df.loc[test_id_list]
    return train, test
