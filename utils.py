#!/usr/bin/python
# -*- coding: utf-8 -*-
import os


MAX_PLACE_NUM = 11
MAX_HOLD_NUM = 6
MAX_DAY_NUM = 13
MAX_RACE_NUM = 13


def create_race_id_list(start_year, last_year):
    race_id_list = []
    if start_year <= last_year and start_year >= 1975 and last_year <= datetime.date.today().year:
        for year in range(start_year, last_year + 1):
            for place in range(1, MAX_PLACE_NUM):
                for hold in range(1,MAX_HOLD_NUM):
                    for day in range(1, MAX_DAY_NUM):
                        for race in range(1, MAX_RACE_NUM):
                            race_id = "{:4d}{:0>2d}{:0>2d}{:0>2d}{:0>2d}".format(year, place, hold, day, race)
                            race_id_list.append(race_id)
    return race_id_list
