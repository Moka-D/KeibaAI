#!/usr/bin/python
"""スクレイピング処理モジュール

Webからスクレイピングしてデータを取得するメソッド郡

"""

import sys
import os
sys.path.append(os.pardir)
from typing import Dict, Tuple, Union, List
import datetime as dt
import numpy as np
import pandas as pd
import requests
import re
from bs4 import BeautifulSoup
import time
from selenium import webdriver
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from common.utils import (
    DATE_PATTERN,
    GRADE_KIND_TO_ID,
    GROUND_STATE_LIST,
    WEATHER_LIST,
    AGE_LIMIT_TO_ID,
    CLASSIFICATION_TO_ID,
    SEX_LIMIT_TO_ID,
    InvalidArgument
)


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

    url = 'https://db.sp.netkeiba.com/race/' + race_id

    time.sleep(1)
    html = requests.get(url)
    html.encoding = 'EUC-JP'
    soup = BeautifulSoup(html.text, 'html.parser')
    result_table = soup.find('table', attrs={'class': 'table_slide_body ResultsByRaceDetail'})
    others_info = soup.find('div', attrs={'class': 'RaceHeader_Value_Others'}).text

    # race_info
    info_dict = {}
    info_dict['title'] = soup.find('span', attrs={'class': 'RaceName_main'}).text
    info_text = soup.find('div', attrs={'class': 'RaceData'}).text

    # race_type, turn
    if '障' in info_text:
        info_dict['race_type'] = '障'
    elif '芝' in info_text:
        info_dict['race_type'] = '芝'
    elif 'ダ' in info_text:
        info_dict['race_type'] = 'ダ'
    else:
        info_dict['race_type'] = None

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

    # grade
    try:
        grade = soup.find('span', attrs={'class': 'Icon_GradeType'}).text
        info_dict['grade'] = GRADE_KIND_TO_ID.get(grade)
    except AttributeError:
        info_dict['grade'] = None

    # age_limit
    if '４歳以上' in others_info:
        info_dict['age_limit'] = AGE_LIMIT_TO_ID.get('4歳以上')
    elif '３歳以上' in others_info:
        info_dict['age_limit'] = AGE_LIMIT_TO_ID.get('3歳以上')
    elif '３歳' in others_info:
        info_dict['age_limit'] = AGE_LIMIT_TO_ID.get('3歳')
    elif '２歳' in others_info:
        info_dict['age_limit'] = AGE_LIMIT_TO_ID.get('2歳')
    else:
        info_dict['age_limit'] = None

    # classification
    if 'オープン' in others_info:
        info_dict['classification'] = CLASSIFICATION_TO_ID.get('オープン')
    elif '未勝利' in others_info:
        info_dict['classification'] = CLASSIFICATION_TO_ID.get('未勝利')
    elif '新馬' in others_info:
        info_dict['classification'] = CLASSIFICATION_TO_ID.get('新馬')
    elif '３勝' in others_info or '１６００万' in others_info:
        info_dict['classification'] = CLASSIFICATION_TO_ID.get('1600万下')
    elif '２勝' in others_info or '１０００万' in others_info:
        info_dict['classification'] = CLASSIFICATION_TO_ID.get('1000万下')
    elif '１勝' in others_info or '５００万' in others_info:
        info_dict['classification'] = CLASSIFICATION_TO_ID.get('500万下')
    else:
        info_dict['classification'] = None

    # sex_limit
    if '牡・牝' in others_info:
        info_dict['sex_limit'] = SEX_LIMIT_TO_ID.get('牡・牝')
    elif '牝' in others_info:
        info_dict['sex_limit'] = SEX_LIMIT_TO_ID.get('牝')
    else:
        info_dict['sex_limit'] = None

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

    time.sleep(1)
    df_list = pd.read_html(url)
    df = df_list[0]
    df['horse_id'] = horse_id_list
    df['jockey_id'] = jockey_id_list
    df['trainer_id'] = trainer_id_list

    df['賞金（万円）'].fillna(0, inplace=True)
    df['単勝'] = pd.to_numeric(df['単勝'], errors='coerce')
    df.replace({np.nan: None}, inplace=True)

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

    url = 'https://db.netkeiba.com/horse/' + horse_id

    time.sleep(1)
    df = pd.read_html(url)[2]

    generations = {}
    columns_num = len(df.columns)
    for i in reversed(range(columns_num)):
        generations[i] = df[i]
        df.drop([i], axis=1, inplace=True)
        df = df.drop_duplicates()

    peds_df = pd.concat([generations[i] for i in range(columns_num)], ignore_index=True).rename(horse_id)
    return peds_df


def scrape_race_card(race_id: str, date: dt.date) -> pd.DataFrame:
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

    url = 'https://race.netkeiba.com/race/shutuba.html?race_id=' + race_id

    time.sleep(1)
    df = pd.read_html(url)[0]
    df = df.T.reset_index(level=0, drop=True).T

    html = requests.get(url)
    html.encoding = 'EUC-JP'
    soup = BeautifulSoup(html.text, 'html.parser')

    # レース情報
    info_texts = soup.find('div', attrs={'class': 'RaceData01'}).text
    info = re.findall(r'\w+', info_texts)

    if '障' in info_texts:
        df['race_type'] = ['障'] * len(df)
    elif '芝' in info_texts:
        df['race_type'] = ['芝'] * len(df)
    elif 'ダ' in info_texts:
        df['race_type'] = ['ダ'] * len(df)

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

    date_i = int(date.strftime('%Y%m%d'))
    df['race_date'] = [date_i] * len(df)
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

    url = 'https://db.netkeiba.com/horse/result/' + horse_id

    time.sleep(1)
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

    time.sleep(1)
    df = pd.read_html(url)[0]

    df.loc[df['レース名'].notna(), 'race_id'] = race_id_list
    if with_jockey_id:
        df.loc[df['騎手'].notna(), 'jockey_id'] = jockey_id_list

    df.replace({np.nan: None}, inplace=True)

    return df


def get_driver():
    """ドライバーの生成
    """
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--disable-features=VizDisplayCompositor')

    driver = webdriver.Remote(
        command_executor=os.environ["SELENIUM_URL"],
        desired_capabilities=options.to_capabilities(),
        options=options
    )
    return driver


def scrape_period_race_id_list(
        year: int,
        start_month: int = 1,
        end_month: int = 12,
        only_jra: bool = True
    ) -> List[str]:
    """指定期間に開催されたレースIDの一覧を取得

    Parameters
    ----------
    year : int
        年 (1975～今年)
    start_month : int, default 1
        開始月
    end_month : int, default 12
        終了月 (開始月よりも前だとエラー)
    only_jra : bool, default True
        JRAのレースに絞るかどうか

    Returns
    -------
    list[str]
        レースIDのリスト

    Raises
    ------
    InvalidArgument
        不正引数例外
    """
    # 引数処理
    this_year = dt.datetime.today().year
    if year < 1975 or year > this_year:
        raise InvalidArgument("Argument 'year' must be from 1975 to {}.".format(this_year))
    if start_month < 1 or start_month > 12:
        raise InvalidArgument("Argument 'start_month' must be from 1 to 12.")
    if end_month < 1 or end_month > 12:
        raise InvalidArgument("Argument 'end_month' must be from 1 to 12.")
    if end_month < start_month:
        raise InvalidArgument("Argument 'end_month' must be more than argument 'start_month'.")

    race_id_list = []

    driver = get_driver()

    try:
        wait = WebDriverWait(driver, 10)
        driver.get("https://db.netkeiba.com/?pid=race_search_detail")
        time.sleep(1)
        wait.until(EC.presence_of_all_elements_located)

        # 期間を選択
        start_year_element = driver.find_element(by=By.NAME, value='start_year')
        start_year_select = Select(start_year_element)
        start_year_select.select_by_value(str(year))
        start_mon_element = driver.find_element(by=By.NAME, value='start_mon')
        start_mon_select = Select(start_mon_element)
        start_mon_select.select_by_value(str(start_month))
        end_year_element = driver.find_element(by=By.NAME, value='end_year')
        end_year_select = Select(end_year_element)
        end_year_select.select_by_value(str(year))
        end_mon_element = driver.find_element(by=By.NAME, value='end_mon')
        end_mon_select = Select(end_mon_element)
        end_mon_select.select_by_value(str(end_month))

        if only_jra:
            # 中央競馬をチェック
            for i in range(1, 11):
                terms = driver.find_element(by=By.ID, value=("check_Jyo_" + str(i).zfill(2)))
                terms.click()

        # 表示件数を100件に変更
        list_element = driver.find_element(by=By.NAME, value='list')
        list_select = Select(list_element)
        list_select.select_by_value('100')

        # フォームを送信
        frm = driver.find_element(by=By.CSS_SELECTOR, value="#db_search_detail_form > form")
        frm.submit()
        time.sleep(5)
        wait.until(EC.presence_of_all_elements_located)

        while True:
            time.sleep(5)
            wait.until(EC.presence_of_all_elements_located)
            all_rows = driver.find_element(by=By.CLASS_NAME, value='race_table_01').find_elements(by=By.TAG_NAME, value='tr')

            for row in range(1, len(all_rows)):
                race_href = all_rows[row].find_elements(by=By.TAG_NAME, value='td')[4].find_element(by=By.TAG_NAME, value='a').get_attribute('href')
                race_id = race_href.removeprefix('https://db.netkeiba.com/race/').removesuffix('/')
                race_id_list.append(race_id)

            try:
                target = driver.find_elements(by=By.LINK_TEXT, value='次')[0]
                driver.execute_script("arguments[0].click();", target)
            except IndexError:
                break
    finally:
        driver.quit()

    return race_id_list


def scrape_race_card_id_list(
    race_date: str,
    is_past: bool = False
) -> List[str]:
    """指定日に開催のレースID一覧を取得

    Parameters
    ----------
    race_date : str
        レースの開催日 (フォーマット:'20xx/yy/zz')
    is_past : bool, default False
        過去の日付かどうか

    Returns
    -------
    list[str]
        レースID一覧
    """

    race_id_list = []
    url = "https://race.netkeiba.com/top/race_list.html?kaisai_date=" + race_date

    driver = get_driver()

    try:
        wait = WebDriverWait(driver, 10)
        driver.get(url)
        time.sleep(1)
        wait.until(EC.presence_of_all_elements_located)

        # レースIDの取得
        soup = BeautifulSoup(driver.page_source, features='lxml')
        elem_base = soup.find(id="RaceTopRace")
        elems = elem_base.find_all("li", attrs={'class': 'RaceList_DataItem'})
        for elem in elems:
            a_tag = elem.find("a")
            if a_tag:
                href = a_tag.get('href')
                if is_past:
                    match = re.findall("\/race\/result.html\?race_id=(.*)&rf=race_list", href)
                else:
                    match = re.findall("\/race\/shutuba.html\?race_id=(.*)&rf=race_list", href)
                if len(match) > 0:
                    race_id = match[0]
                    race_id_list.append(race_id)
    finally:
        driver.quit()

    return race_id_list
