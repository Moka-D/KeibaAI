#!/usr/bin/python
# -*- coding: utf-8 -*-
import pandas as pd
import time
import requests
import re
from bs4 import BeautifulSoup


class UnMatchExpectedData(Exception):
    """
    期待するデータが見つからなかったときの例外クラス
    """
    pass


def scrape_race_info(race_id: str) -> tuple[pd.DataFrame, dict]:
    url = 'https://db.netkeiba.com/race/' + race_id

    time.sleep(1)
    html = requests.get(url)
    html.encoding = 'EUC-JP'
    soup = BeautifulSoup(html.text, 'html.parser')
    result_soup = soup.find('table', attrs={'summary': 'レース結果'})
    data_intro = soup.find('div', attrs={'class': 'data_intro'})

    # race_info
    info_dict = {}
    info_dict['title'] = data_intro.find_all('h1')[0].text
    p_texts = data_intro.find_all('p')
    text1 = p_texts[0].text
    text2 = p_texts[1].text

    # race_type
    if '障' in text1:
        info_dict['race_type'] = '障害'
    elif '芝' in text1:
        info_dict['race_type'] = '芝'
    elif 'ダ' in text1:
        info_dict['race_type'] = 'ダート'
    else:
        raise UnMatchExpectedData("期待するrace_typeが見つかりませんでした。")

    # date
    info_dict['date'] = re.findall(r'\w+', text2)[0]

    # others info
    text_list = re.findall(r'\w+', text1)
    for text in text_list:
        if 'm' in text:
            info_dict['course_dist'] = re.findall(r'\d+', text)[0]
        if text in ['良', '稍重', '重', '不良']:
            info_dict['ground_state'] = text
        if text in ['曇', '晴', '雨', '小雨', '小雪', '雪']:
            info_dict['weather'] = text

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

    # trainer_id
    trainer_id_list = []
    trainer_a_list = result_soup.find_all('a', attrs={'href': re.compile('^/trainer')})
    for a in trainer_a_list:
        trainer_id = re.findall(r'\d+', a['href'])
        trainer_id_list.append(trainer_id[0])

    # result df
    result_df = pd.read_html(url)[0]

    result_df['horse_id'] = horse_id_list
    result_df['jockey_id'] = jockey_id_list
    result_df['trainer_id'] = trainer_id_list

    return result_df, info_dict
