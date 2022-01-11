# -*- coding: utf-8 -*-
from typing import Dict, Tuple, Union
import pandas as pd
import requests
import re
from bs4 import BeautifulSoup
import time


DATE_PATTERN = re.compile('\d{4}/\d{1,2}/\d{1,2}')
GROUND_STATE_LIST = ['良', '稍', '重', '不']
WEATHER_LIST = ['曇', '晴', '雨', '小雨', '小雪', '雪']


def scrape_race_info(race_id: str) -> Tuple[Dict[str, Union[str, int]], pd.DataFrame, pd.DataFrame]:
    """レース結果をスクレイピングする関数

    Parameters
    ----------
    race_id : str
        レース結果

    Returns
    -------
    info_dict : dict[str, str or int]
        レース情報
    result_df : pandas.DataFrame
        出走馬一覧とレースの結果
    payoff_table : pandas.DataFrame
        払い戻し表
    """
    time.sleep(1)
    url = 'https://db.sp.netkeiba.com/race/' + race_id

    html = requests.get(url)
    html.encoding = 'EUC-JP'
    soup = BeautifulSoup(html.text, 'html.parser')
    result_table = soup.find('table', attrs={'class': 'table_slide_body ResultsByRaceDetail'})

    # race_info
    info_dict = {}
    info_dict['title'] = soup.find('span', attrs={'class': 'RaceName_main'}).text
    info_text = soup.find('div', attrs={'class': 'RaceData'}).text

    # race_type, turn
    if '障' in info_text:
        info_dict['race_type'] = '障害'
    elif '芝' in info_text:
        info_dict['race_type'] = '芝'
    elif 'ダ' in info_text:
        info_dict['race_type'] = 'ダート'
    else:
        info_dict['race_type'] = '他'

    if '右' in info_text:
        info_dict['turn'] = '右'
    elif '左' in info_text:
        info_dict['turn'] = '左'
    else:
        info_dict['turn'] = '他'

    # others info
    text_list = re.findall(r'\w+', info_text)
    for text in reversed(text_list):
        if 'm' in text:
            info_dict['course_dist'] = re.findall(r'\d+', text)[0]
        if text in GROUND_STATE_LIST:
            info_dict['ground_state'] = text
        if text in WEATHER_LIST:
            info_dict['weather'] = text

    if 'course_dist' not in info_dict:
        info_dict['course_dist'] = None
    if 'ground_state' not in info_dict:
        info_dict['ground_state'] = None
    if 'weather' not in info_dict:
        info_dict['weather'] = None

    # date
    date_text = soup.find('span', attrs={'class': 'Race_Date'}).text
    info_dict['date'] = re.search(DATE_PATTERN, date_text).group()

    # horse_id
    horse_id_list = []
    horse_a_list = result_table.find_all('a', attrs={'href': re.compile('/horse/.*/')})
    for a in horse_a_list:
        horse_id = a['href'].removeprefix('https://db.sp.netkeiba.com/horse/').removesuffix('/')
        horse_id_list.append(horse_id)

    # jockey_id
    jockey_id_list = []
    jockey_a_list = result_table.find_all('a', attrs={'href': re.compile('/jockey/.*/')})
    for a in jockey_a_list:
        jockey_id = a['href'].removeprefix('https://db.sp.netkeiba.com/jockey/').removesuffix('/')
        jockey_id_list.append(jockey_id)

    # trainer_id
    trainer_id_list = []
    trainer_a_list = result_table.find_all('a', attrs={'href': re.compile('/trainer/.*/')})
    for a in trainer_a_list:
        trainer_id = a['href'].removeprefix('https://db.sp.netkeiba.com/trainer/').removesuffix('/')
        trainer_id_list.append(trainer_id)

    df_list = pd.read_html(url)
    df = df_list[0]
    df['horse_id'] = horse_id_list
    df['jockey_id'] = jockey_id_list
    df['trainer_id'] = trainer_id_list

    df['賞金（万円）'].fillna(0, inplace=True)

    info_dict['horse_num'] = len(df)

    # payoff
    if len(df_list) >= 2:
        payoff_table = df_list[1]
    else:
        payoff_table = pd.DataFrame()

    return info_dict, df, payoff_table


def scrape_horse_peds(horse_id: str) -> pd.DataFrame:
    """馬の血統(2世代前まで)をスクレイピングする関数

    Parameters
    ----------
    horse_id : str
        馬ID

    Returns
    -------
    peds_df : pandas.DataFrame
        馬の血統表 (2世代前まで)
    """
    time.sleep(1)
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


def scrape_race_card(race_id: str, date: int) -> pd.DataFrame:
    """出馬表をスクレイピングする関数

    Parameters
    ----------
    race_id : str
        レースID
    date : str
        レースの日付

    Returns
    -------
    race_card_df : pd.DataFrame
        出馬表
    """
    time.sleep(1)
    url = 'https://race.netkeiba.com/race/shutuba.html?race_id=' + race_id
    df = pd.read_html(url)[0]
    df = df.T.reset_index(level=0, drop=True).T

    html = requests.get(url)
    html.encoding = 'EUC-JP'
    soup = BeautifulSoup(html.text, 'html.parser')

    # レース情報
    info_texts = soup.find('div', attrs={'class': 'RaceData01'}).text
    info = re.findall(r'\w+', info_texts)

    if '障' in info_texts:
        df['race_type'] = ['障害'] * len(df)
    elif '芝' in info_texts:
        df['race_type'] = ['芝'] * len(df)
    elif 'ダ' in info_texts:
        df['race_type'] = ['ダート'] * len(df)

    if '右' in info_texts:
        df['turn'] = ['右'] * len(df)
    elif '左' in info_texts:
        df['turn'] = ['左'] * len(df)
    else:
        df['turn'] = ['その他'] * len(df)

    for text in info:
        if 'm' in text:
            df['distance'] = [int(re.findall(r'\d+', text)[0])] * len(df)
        if text in GROUND_STATE_LIST:
            df['ground'] = [text] * len(df)
        if text in WEATHER_LIST:
            df['weather'] = [text] * len(df)

    df['date'] = [date] * len(df)
    df['race_id'] = [race_id] * len(df)
    df['horse_num'] = [len(df)] * len(df)

    # 優勝賞金
    prise_text = soup.find('div', attrs={'class': 'RaceList_Item02'}).text
    prise_text = re.findall(r'本賞金:\d*', prise_text)[0]
    prise = int(re.findall(r'\d+', prise_text)[0])
    df['win_prise'] = [prise] * len(df)

    # horse_id
    horse_id_list = []
    horse_td_list = soup.find_all('td', attrs={'class': 'HorseInfo'})
    for td in horse_td_list:
        horse_id = re.findall(r'\d+', td.find('a')['href'])
        horse_id_list.append(horse_id[0])

    # jockey_id
    jockey_id_list = []
    jockey_td_list = soup.find_all('td', attrs={'class': 'Jockey'})
    for td in jockey_td_list:
        jockey_id = re.findall(r'\d+', td.find('a')['href'])
        jockey_id_list.append(jockey_id[0])

    # trainer_id
    trainer_id_list = []
    trainer_td_list = soup.find_all('td', attrs={'class': 'Trainer'})
    for td in trainer_td_list:
        trainer_id = re.findall(r'\d+', td.find('a')['href'])
        trainer_id_list.append(trainer_id[0])

    df['horse_id'] = horse_id_list
    df['jockey_id'] = jockey_id_list
    df['trainer_id'] = trainer_id_list

    df.drop(['印', 'Unnamed: 9_level_1', '登録', 'メモ'], axis=1, inplace=True)
    return df


def scrape_horse_results(horse_id: str, with_jockey_id: bool = True) -> pd.DataFrame:
    """馬の過去結果をスクレイピング

    Parameters
    ----------
    horse_id : str
        馬ID

    Returns
    -------
    pd.DataFrame
        結果df
    """
    time.sleep(1)
    url = 'https://db.netkeiba.com/horse/result/' + horse_id

    html = requests.get(url)
    html.encoding = 'EUC-JP'
    soup = BeautifulSoup(html.text, 'html.parser')
    result_table = soup.find('table', attrs={'class': 'db_h_race_results nk_tb_common'})

    race_a_list = result_table.find_all('a', attrs={'href': re.compile('^/race')})
    race_id_list = []
    for a in race_a_list:
        href = a['href']
        if not ('list' in href or 'sum' in href or 'movie' in href):
            race_id_list.append(href.removeprefix('/race/').removesuffix('/'))

    if with_jockey_id:
        jockey_a_list = result_table.find_all('a', attrs={'href': re.compile('^/jockey')})
        jockey_id_list = []
        for a in jockey_a_list:
            jockey_id = a['href'].removeprefix('/jockey/').removesuffix('/')
            jockey_id_list.append(jockey_id)

    df = pd.read_html(url)[0]

    df.loc[df['レース名'].notna(), 'race_id'] = race_id_list
    if with_jockey_id:
        df.loc[df['騎手'].notna(), 'jockey_id'] = jockey_id_list

    return df
