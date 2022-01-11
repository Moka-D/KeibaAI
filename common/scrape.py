# -*- coding: utf-8 -*-
import sys
import os
sys.path.append(os.pardir)
from typing import Dict, Tuple, Union, List
import pandas as pd
import requests
import re
from bs4 import BeautifulSoup
import time
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from common.utils import DATE_PATTERN


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


def scrape_period_race_id_list(
        start_year: int,
        end_year: int,
        start_month: int = 1,
        end_month: int = 12,
        only_jra: bool = True
    ) -> List[str]:

    # ドライバーの生成
    options = Options()
    options.add_argument('--headless')
    options.add_argument('log-level=3')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    wait = WebDriverWait(driver, 10)

    driver.get("https://db.netkeiba.com/?pid=race_search_detail")
    time.sleep(1)
    wait.until(EC.presence_of_all_elements_located)

    # 期間を選択
    start_year_element = driver.find_element(by=By.NAME, value='start_year')
    start_year_select = Select(start_year_element)
    start_year_select.select_by_value(str(start_year))
    start_mon_element = driver.find_element(by=By.NAME, value='start_mon')
    start_mon_select = Select(start_mon_element)
    start_mon_select.select_by_value(str(start_month))
    end_year_element = driver.find_element(by=By.NAME, value='end_year')
    end_year_select = Select(end_year_element)
    end_year_select.select_by_value(str(end_year))
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

    race_id_list = []

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

    # ドライバーの終了
    driver.close()
    driver.quit()

    return race_id_list


def scrape_race_card_id_list(race_date: str) -> List[str]:

    url = "https://race.netkeiba.com/top/race_list.html?kaisai_date=" + race_date

    # ドライバーの生成
    options = Options()
    options.add_argument('--headless')
    options.add_argument('log-level=3')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    wait = WebDriverWait(driver, 10)

    driver.get(url)
    time.sleep(1)
    wait.until(EC.presence_of_all_elements_located)

    # レースIDの取得
    soup = BeautifulSoup(driver.page_source, features='lxml')
    elem_base = soup.find(id="RaceTopRace")
    elems = elem_base.find_all("li", attrs={'class': 'RaceList_DataItem'})
    race_id_list = []
    for elem in elems:
        a_tag = elem.find("a")
        if a_tag:
            href = a_tag.get('href')
            match = re.findall("\/race\/result.html\?race_id=(.*)&rf=race_list", href)
            if len(match) > 0:
                race_id = match[0]
                race_id_list.append(race_id)

    # ドライバーの終了
    driver.close()
    driver.quit()

    return race_id_list
