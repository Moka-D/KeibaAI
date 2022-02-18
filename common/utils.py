#!/usr/bin/python
"""ユーティリティメソッドモジュール
"""

import os
import datetime as dt
from typing import List, Tuple, Union
import pandas as pd
import re
import requests
from enum import Enum


MAX_PLACE_NUM = 10
MAX_HOLD_NUM = 6
MAX_DAY_NUM = 12
MAX_RACE_NUM = 12

DATE_PATTERN = re.compile('\d{4}/\d{1,2}/\d{1,2}')

PAYOFF_KIND_TO_ID = {
    '単勝': 1,
    '複勝': 2,
    '枠連': 3,
    '馬連': 4,
    'ワイド': 5,
    '馬単': 6,
    '3連複': 7,
    '3連単': 8,
    '枠単': 9
}
GRADE_KIND_TO_ID = {
    'GI': 1,
    'JGI': 1,
    'GII': 2,
    'JGII': 2,
    'GIII': 3,
    'JGIII': 3,
    '重賞': 4,
    'L': 5
}
AGE_LIMIT_TO_ID = {
    '4歳以上': 1,
    '3歳以上': 2,
    '3歳': 3,
    '2歳': 4
}
CLASSIFICATION_TO_ID = {
    'オープン': 1,
    '1600万下': 2,
    '1000万下': 3,
    '500万下': 4,
    '未勝利': 5,
    '新馬': 6
}
SEX_LIMIT_TO_ID = {
    '牡・牝': 1,
    '牝': 2
}

SEX_LIST = ['牡', '牝', 'セ']
PLACE_ID_LIST = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
RACE_TYPE_LIST = ['芝', 'ダ', '障']
TURN_LIST = ['右', '左', '他']
GROUND_STATE_LIST = ['良', '稍', '重', '不']
WEATHER_LIST = ['曇', '晴', '雨', '小雨', '小雪', '雪']


class Racecourse(Enum):
    """競馬場定義クラス
    """

    SAPPORO   = (1, '札幌', '右')
    HAKODATE  = (2, '函館', '右')
    FUKUSHIMA = (3, '福島', '右')
    NIIGATA   = (4, '新潟', '左')
    TOKYO     = (5, '東京', '左')
    NAKAYAMA  = (6, '中山', '右')
    CHUKYO    = (7, '中京', '左')
    KYOTO     = (8, '京都', '右')
    HANSHIN   = (9, '阪神', '右')
    KOKURA    = (10, '小倉', '右')

    MONBETSU  = (30, '門別', '右')
    MORIOKA   = (35, '盛岡', '左')
    MIZUSAWA  = (36, '水沢', '右')
    URAWA     = (42, '浦和', '左')
    FUNABASHI = (43, '船橋', '左')
    OI        = (44, '大井', '右')
    KAWASAKI  = (45, '川崎', '左')
    KANAZAWA  = (46, '金沢', '右')
    KASAMATSU = (47, '笠松', '右')
    NAGOYA    = (48, '名古屋', '右')
    SONODA    = (50, '園田', '右')
    HIMEJI    = (51, '姫路', '右')
    FUKUYAMA  = (53, '福山', '右')
    KOCHI     = (54, '高知', '右')
    SAGA      = (55, '佐賀', '右')

    OBIHIRO = (65, '帯広(ばんえい)', '他')

    def __init__(self, id: int, ja: str, turn: str) -> None:
        self.id = id
        self.ja = ja
        self.turn = turn

    @classmethod
    def _member_as_list(cls) -> List['Racecourse']:
        return [*cls.__members__.values()]

    @classmethod
    def value_of(cls, id: int) -> 'Racecourse':
        """IDから競馬場定義クラスを取得

        Parameters
        ----------
        id : int
            競馬場ID

        Returns
        -------
        Racecource
            競馬場定義クラス
        """
        for c in cls._member_as_list():
            if id == c.id:
                return c


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
    for place in range(1, MAX_PLACE_NUM + 1):
        for hold in range(1, MAX_HOLD_NUM + 1):
            for day in range(1, MAX_DAY_NUM + 1):
                for race in range(1, MAX_RACE_NUM + 1):
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
    sorted_id_list = df.sort_values('race_date').index.unique()
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


def str_list_to_int(x: List[str]) -> List[int]:
    return [int(n) for n in x]


def judge_turn(place: str, distance: int) -> Union[str, None]:
    try:
        place_id = int(place)
    except ValueError:
        return None

    racecource = Racecourse.value_of(place_id)
    if racecource.ja == '新潟' and distance == 1000:
        return '他'
    elif racecource.ja == '大井' and distance == 1650:
        return '左'
    else:
        return racecource.turn


def send_line_notify(message):
    api = 'https://notify-api.line.me/api/notify'
    headers = {'Authorization': f'Bearer {os.environ["LINE_API_TOKEN"]}'}
    data = {'message': f'message: {message}'}
    requests.post(api, headers=headers, data=data)
