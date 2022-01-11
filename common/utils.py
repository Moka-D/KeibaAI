# -*- coding: utf-8 -*-
import datetime as dt
from typing import List, Tuple
import pandas as pd
import re


MAX_PLACE_NUM = 11
MAX_HOLD_NUM = 6
MAX_DAY_NUM = 13
MAX_RACE_NUM = 13

DATE_PATTERN = re.compile('\d{4}/\d{1,2}/\d{1,2}')

ID_TO_PLACE = {
    '01': '札幌',
    '02': '函館',
    '03': '福島',
    '04': '新潟',
    '05': '東京',
    '06': '中山',
    '07': '中京',
    '08': '京都',
    '09': '阪神',
    '10': '小倉',

    '30': '門別',
    '35': '盛岡',
    '36': '水沢',
    '42': '浦和',
    '43': '船橋',
    '44': '大井',
    '45': '川崎',
    '46': '金沢',
    '47': '笠松',
    '48': '名古屋',
    '50': '園田',
    '51': '姫路',
    '54': '高知',
    '55': '佐賀',
    '65': '帯広(ばんえい)',

    'A4': 'アメリカ',
    'A6': 'イギリス',
    'A8': 'フランス',
    'B4': 'ニュージーランド',
    'C5': 'シャンティイ',
    'C8': 'ロンシャン',
    'E2': 'アルゼンチン',
    'F3': 'サンタアニタパーク',
    'F4': 'チャーチルダウンズ',
    'G0': '香港',
    'H1': 'シャティン',
}

PAYOFF_KIND_TO_ID = {
    '単勝': 1,
    '複勝': 2,
    '枠連': 3,
    '馬連': 4,
    'ワイド': 5,
    '枠単': 6,
    '馬単': 7,
    '三連複': 8,
    '三連単': 9
}


class InvalidArgument(Exception):
    """不正引数例外クラス"""
    pass


def get_environment():
    try:
        env = get_ipython().__class__.__name__
        if env == 'ZMQInteractiveShell':
            return 'Jupyter'
        elif env == 'TerminalInteractiveShell':
            return 'IPython'
        else:
            return 'OtherShell'
    except NameError:
        return 'Interpreter'


def create_race_id_list(year: int) -> List[str]:
    """
    指定した年の全レースIDを生成する関数

    Parameters
    ----------
    year : int
        年 (>= 1975 and <= 今年)

    Returns
    -------
    list[str]
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


def judge_region(race_id: str):
    place = race_id[4:6]
    try:
        place_id = int(place)
    except ValueError:
        return 'Overseas'

    if place_id >= 1 and place_id <= 10:
        return 'JRA'
    elif place_id == 65:
        return 'Harnes'
    else:
        return 'NRA'
