#!/usr/bin/python
# -*- coding: utf-8 -*-
import os
import datetime
import pandas as pd
import time
from tqdm.notebook import tqdm
import requests
import re
from bs4 import BeautifulSoup


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


def regist_result(race_id_list, pre_race_results={}, pre_race_infos={}):
    race_results = pre_race_results
    race_infos = pre_race_infos
    for race_id in tqdm(race_id_list):
        time.sleep(1)
        try:
            title, info1, info2, df = scrape_race_info(race_id)
            race_results[race_id] = df

            info_dict={}
            info_dict['title'] = title
            info_dict
        except IndexError:
            continue
        except:
            print("Error!: Unexpected error occurred at race_id=%d" % race_id)
            break
    return race_results, race_infos


def scrape_race_info(race_id):
    url = 'https://db.netkeiba.com/race/' + race_id

    html = requests.get(url)
    html.encoding = 'EUC-JP'
    soup = BeautifulSoup(html.text, 'html.parser')
    result_soup = soup.find('table', attrs={'summary': 'レース結果'})
    data_intro = soup.find('div', attrs={'class': 'data_intro'})

    # race_info
    title = data_intro.find_all('h1')[0].text
    race_info = data_intro.find_all('p')
    info1 = re.findall(r'\w+', race_info[0].text)
    info2 = re.findall(r'\w+', race_info[1].text)

    # result df
    df = pd.read_html(url)[0]

    # horse_id
    horse_id_list = []
    horse_a_list = result_soup.find_all('a', attrs={'href': re.compile('^/horse')})
    for a in horse_a_list:
        horse_id = re.findall(r'\d+', a['href'])
        horse_id_list.append(horse_id[0])

    # jockey_id
    jockey_id_list = []
    jockey_a_list = result_soup.find_all('a', attrs={'href': re.compile('^/jockey')})
    for a in jockey_a_list:
        jockey_id = re.findall(r'\d+', a['href'])
        jockey_id_list.append(jockey_id[0])

    df['horse_id'] = horse_id_list
    df['jockey_id'] = jockey_id_list

    return title, info1, info2, df
