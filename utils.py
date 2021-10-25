#!/usr/bin/python
# -*- coding: utf-8 -*-
import os
import datetime


MAX_PLACE_NUM = 11
MAX_HOLD_NUM = 6
MAX_DAY_NUM = 13
MAX_RACE_NUM = 13


def create_race_id_list(start_year: int, last_year: int) -> list[str]:
    """
    指定した年範囲のレースIDを生成する関数

    Parameters
    ----------
    start_year : int
        開始年 (>= 1975)
    last_year : int
        終了年 (<= 今年 and >= start_year)

    Returns
    -------
    race_id_list : list[str]
        レースIDのリスト
    """
    # 引数チェック
    if start_year > last_year:
        raise ValueError("start_year は last_year 以下の値を設定してください。")
    if start_year < 1975:
        raise ValueError("start_year は 1975 以上の整数を指定してください。")
    if last_year > datetime.date.today().year:
        raise ValueError("last_year は %d 以下の整数を指定してください。" % datetime.date.today().year)

    race_id_list = []
    for year in range(start_year, last_year + 1):
        for place in range(1, MAX_PLACE_NUM):
            for hold in range(1,MAX_HOLD_NUM):
                for day in range(1, MAX_DAY_NUM):
                    for race in range(1, MAX_RACE_NUM):
                        race_id = "{:4d}{:0>2d}{:0>2d}{:0>2d}{:0>2d}".format(year, place, hold, day, race)
                        race_id_list.append(race_id)

    return race_id_list
