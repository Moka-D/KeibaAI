#!/usr/bin/python
# -*- coding: utf-8 -*-
import datetime as dt
from typing import List, Tuple
import pandas as pd
import time
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from common import InvalidArgument


MAX_PLACE_NUM = 11
MAX_HOLD_NUM = 6
MAX_DAY_NUM = 13
MAX_RACE_NUM = 13


def create_race_id_list(year: int) -> List[str]:
    """
    指定した年の全レースIDを生成する関数

    Parameters
    ----------
    year : int
        年 (>= 1975 and <= 今年)

    Returns
    -------
    List[str]
        レースIDのリスト
    """
    # 引数チェック
    if year < 1975:
        raise InvalidArgument("引数 year は 1975 以上の整数を指定してください。")
    if year > dt.date.today().year:
        raise InvalidArgument("引数 year は %d 以下の整数を指定してください。" % dt.date.today().year)

    race_id_list = []
    for place in range(1, MAX_PLACE_NUM):
        for hold in range(1,MAX_HOLD_NUM):
            for day in range(1, MAX_DAY_NUM):
                for race in range(1, MAX_RACE_NUM):
                    race_id = "{:4d}{:0>2d}{:0>2d}{:0>2d}{:0>2d}".format(year, place, hold, day, race)
                    race_id_list.append(race_id)

    return race_id_list


def get_all_race_id(
        start_year: int,
        end_year: int,
        start_month: int = 1,
        end_month: int = 12,
        only_jra: bool = False
    ) -> List[str]:

    URL = "https://db.netkeiba.com/?pid=race_search_detail"

    options = Options()
    options.add_argument('--headless')
    options.add_argument('log-level=3')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    wait = WebDriverWait(driver, 10)

    driver.get(URL)
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
    sorted_id_list = df.sort_values('date').index.unique()
    drop_threshold = round(len(sorted_id_list) * (1 - test_size))
    train_id_list = sorted_id_list[:drop_threshold]
    test_id_list = sorted_id_list[drop_threshold:]
    train = df.loc[train_id_list]
    test = df.loc[test_id_list]
    return train, test
