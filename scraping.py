#!/usr/bin/python
# -*- coding: utf-8 -*-
import pandas as pd
import requests
import re
from bs4 import BeautifulSoup
from common import INVALID_VALUE


DATE_PATTERN = re.compile('\d{4}/\d{1,2}/\d{1,2}')
GROUND_STATE_LIST = ['良', '稍', '重', '不']
WEATHER_LIST = ['曇', '晴', '雨', '小雨', '小雪', '雪']


def scrape_race_info(race_id: str) -> tuple[dict[str, str], pd.DataFrame, pd.DataFrame]:
    """
    レース結果をスクレイピングする関数

    Parameters
    ----------
    race_id : str
        レース結果

    Returns
    -------
    info_dict : dict[str, str]
        レース情報
    result_df : pandas.DataFrame
        出走馬一覧とレースの結果
    payoff_table : pandas.DataFrame
        払い戻し表
    """
    url = 'https://db.sp.netkeiba.com/race/' + race_id
    df_list = pd.read_html(url)
    df = df_list[0]

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

    # date
    date_text = soup.find('span', attrs={'class': 'Race_Date'}).text
    info_dict['date'] = re.search(DATE_PATTERN, date_text).group()

    # horse_id
    horse_id_list = []
    horse_a_list = result_table.find_all('a', attrs={'href': re.compile('/horse/\d+/')})
    for a in horse_a_list:
        horse_id = re.findall(r'\d+', a['href'])
        horse_id_list.append(horse_id[0])

    # jockey_id
    jockey_id_list = []
    jockey_a_list = result_table.find_all('a', attrs={'href': re.compile('/jockey/\d+/')})
    for a in jockey_a_list:
        jockey_id = re.findall(r'\d+', a['href'])
        jockey_id_list.append(jockey_id[0])

    # trainer_id
    trainer_id_list = []
    trainer_a_list = result_table.find_all('a', attrs={'href': re.compile('/trainer/\d+/')})
    for a in trainer_a_list:
        trainer_id = re.findall(r'\d+', a['href'])
        trainer_id_list.append(trainer_id[0])

    df['horse_id'] = horse_id_list
    df['jockey_id'] = jockey_id_list
    df['trainer_id'] = trainer_id_list

    df.drop(['タイム指数', '調教タイム', '厩舎コメント', '備考'], axis=1, inplace=True)
    result_df = result_preprocess(df)

    # payoff
    payoff_table = df_list[1]

    return info_dict, result_df, payoff_table


def scrape_horse_peds(horse_id: str) -> pd.DataFrame:
    """
    馬の血統(2世代前まで)をスクレイピングする関数

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


def scrape_race_card(race_id: str, date: str) -> pd.DataFrame:
    """
    出馬表をスクレイピングする関数

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
            df['course_dist'] = [int(re.findall(r'\d+', text)[0])] * len(df)
        if text in GROUND_STATE_LIST:
            df['ground_state'] = [text] * len(df)
        if text in WEATHER_LIST:
            df['weather'] = [text] * len(df)

    df['date'] = [date] * len(df)

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
    race_card_df = race_card_preprocess(df)
    return race_card_df


def result_preprocess(result_df: pd.DataFrame) -> pd.DataFrame:
    """
    スクレイプしたレース結果データを処理する関数

    Parameters
    ----------
    result_df : pandas.DataFrame
        レース結果DataFrame

    Returns
    -------
    df : pandas.DataFrame
        処理後レース結果DataFrame
    """
    df = result_df.copy()

    # 着順
    df['着順'] = pd.to_numeric(df['着順'], errors='coerce')
    df['着順'].fillna(INVALID_VALUE, inplace=True)
    df['着順'] = df['着順'].astype(int)

    # 性齢
    df['性'] = df['性齢'].map(lambda x: str(x)[0])
    df['年齢'] = df['性齢'].map(lambda x: str(x)[1]).astype(int)

    # 馬体重
    df['体重'] = df['馬体重'].str.split('(', expand=True)[0].astype(int)
    df['体重変化'] = df['馬体重'].str.split('(', expand=True)[1].str[:-1].astype(int)

    # 賞金
    df['賞金（万円）'].fillna(0, inplace=True)

    # 着差
    df['着差'].fillna(0, inplace=True)

    # 型変換
    df['horse_id'] = df['horse_id'].astype(int)
    df['jockey_id'] = df['jockey_id'].astype(int)
    df['trainer_id'] = df['trainer_id'].astype(int)

    return df.drop(['性齢', '馬体重'], axis=1)


def race_card_preprocess(result_df: pd.DataFrame) -> pd.DataFrame:
    """
    スクレイプした出馬表データを処理する関数

    Parameters
    ----------
    result_df : pandas.DataFrame
        出馬表DataFrame

    Returns
    -------
    df : pandas.DataFrame
        処理後出馬表DataFrame
    """
    df = result_df.copy()

    # 性齢
    df['性'] = df['性齢'].map(lambda x: str(x)[0])
    df['年齢'] = df['性齢'].map(lambda x: str(x)[1]).astype(int)

    # 馬体重
    df['体重'] = df['馬体重(増減)'].str.split('(', expand=True)[0].astype(int)
    df['体重変化'] = df['馬体重(増減)'].str.split('(', expand=True)[1].str[:-1].astype(int)

    # 型変換
    df['horse_id'] = df['horse_id'].astype(int)
    df['jockey_id'] = df['jockey_id'].astype(int)
    df['trainer_id'] = df['trainer_id'].astype(int)

    return df.drop(['性齢', '馬体重(増減)', '人気'], axis=1)
