#!/usr/bin/python
# -*- coding: utf-8 -*-
import pandas as pd
import time
import requests
import re
from bs4 import BeautifulSoup
from urllib.request import urlopen


class UnMatchExpectedData(Exception):
    """
    期待するデータが見つからなかったときの例外クラス
    """
    pass


def scrape_race_info(race_id: str) -> tuple[pd.DataFrame, dict[str, str], pd.DataFrame]:
    """
    レース結果のスクレイピングを行う関数

    Parameters
    ----------
    race_id : str
        レース結果

    Returns
    -------
    result_df : pandas.DataFrame
        レース結果
    info_dict : dict[str, str]
        レース情報
    payoff_table : pandas.DataFrame
        払い戻し表
    """
    url = 'https://db.netkeiba.com/race/' + race_id
    result_df = pd.read_html(url)[0]

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

    result_df['horse_id'] = horse_id_list
    result_df['jockey_id'] = jockey_id_list
    result_df['trainer_id'] = trainer_id_list

    # payoff
    f = urlopen(url)
    html_p = f.read()
    html_p = html_p.replace(b'<br />', b'|')
    dfs = pd.read_html(html_p)
    payoff_table = pd.concat([dfs[1], dfs[2]], ignore_index=True)

    return result_df, info_dict, payoff_table


def scrape_horse_peds(horse_id: str) -> pd.DataFrame:
    """
    馬の血統(2世代前まで)をスクレイピングで取得する関数

    Parameters
    ----------
    horse_id : str
        馬ID

    Returns
    -------
    peds_df : pandas.DataFrame
        馬の血統表 (2世代前まで)
    """
    url = 'https://db.netkeiba.com/horse/' + horse_id
    df = pd.read_html(url)[2]

    generations = {}
    columns_num = len(df.columns)
    for i in reversed(range(columns_num)):
        generations[i] = df[i]
        df.drop([i], axis=1, inplace=True)
        df = df.drop_duplicates()

    peds_df = pd.concat([generations[i] for i in range(columns_num)], ignore_index=True).rename(horse_id)
    return peds_df
