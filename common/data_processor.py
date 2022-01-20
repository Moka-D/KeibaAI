# -*- coding: utf-8 -*-
from distutils.log import error
import os
import sys
from tkinter.messagebox import NO
import weakref
sys.path.append(os.pardir)
import datetime as dt
import re
from typing import List, Tuple, Union
import warnings
import pandas as pd
import itertools
from common.utils import InvalidArgument, get_environment
from sklearn.preprocessing import LabelEncoder
import numpy as np
if get_environment() == 'Jupyter':
    from tqdm.notebook import tqdm
else:
    from tqdm import tqdm
from common.dbapi import DBManager
from common.scrape import scrape_race_card


def split_data(df: pd.DataFrame, test_size: float = 0.3) -> Tuple[pd.DataFrame, pd.DataFrame]:
    sorted_id_list = df.sort_values('date').index.unique()
    drop_threshold = round(len(sorted_id_list) * (1 - test_size))
    train_id_list = sorted_id_list[:drop_threshold]
    test_id_list = sorted_id_list[drop_threshold:]
    train = df.loc[train_id_list]#.drop(['date'], axis=1)
    test = df.loc[test_id_list]#.drop(['date'], axis=1)
    return train, test


def encode_weather(weather: str) -> int:
    if weather == '晴':
        return 6
    elif weather == '雨':
        return 1
    elif weather == '小雨':
        return 2
    elif weather == '小雪':
        return 3
    elif weather == '曇':
        return 4
    elif weather == '雪':
        return 5
    else:
        return 0


def encode_ground(ground: str) -> int:
    if ground == '良':
        return 1
    elif ground == '稍':
        return 2
    elif ground == '重':
        return 3
    elif ground == '不':
        return 4
    else:
        return 0


def encode_race_type(race_type: str) -> int:
    if race_type == '芝':
        return 1
    elif race_type == 'ダート':
        return 2
    elif race_type == '障害':
        return 3
    else:
        return 0


def encode_turn(turn: str) -> int:
    if turn == '左':
        return 1
    elif turn == '右':
        return 2
    elif turn == '他':
        return 3
    else:
        return 0


def encode_sex(sex: str) -> int:
    if sex == '牡':
        return 1
    elif sex == '牝':
        return 2
    elif sex == 'セ':
        return 3
    else:
        return 0


class Peds:
    def __init__(self, peds: pd.DataFrame) -> None:
        self.data = peds
        self.data_e = pd.DataFrame()
        self.encode()

    @classmethod
    def read_db(cls, db_path: str):
        dbm = DBManager(db_path)
        df = dbm.select_horse_peds()
        return cls(df)

    def encode(self):
        df = self.data.copy()
        le = LabelEncoder().fit(list(set(itertools.chain.from_iterable(df.fillna('Na').values))))
        for col in df.columns:
            df[col] = le.transform(df[col].fillna('Na'))
        self.data_e = df


class HorseResults:
    def __init__(self, result_df: pd.DataFrame) -> None:
        self.data = result_df[['race_id', 'horse_id', 'date', 'place_id',
                               'weather', 'race_no', 'horse_no',
                               'win_odds', 'popularity',
                               'arriving_order', 'ground', 'goal_time',
                               'race_type', 'distance', 'ground', 'time_diff',
                               'corner_pass', 'last_three_furlong', 'prise',
                               'horse_num']]
        self.data_p = pd.DataFrame()
        self.preprocesing()

    @classmethod
    def read_db(cls, db_path: str) -> 'Results':
        dbm = DBManager(db_path)
        df = dbm.select_horse_results()
        return cls(df)

    def preprocesing(self) -> None:
        df = self.data.copy()

        def convert_time(x):
            try:
                return float(x.split(':')[0]) * 60.0 + float(x.split(':')[1])
            except:
                return np.nan

        # 型変換
        df['arriving_order'] = pd.to_numeric(df['arriving_order'], errors='coerce')
        df[df['arriving_order']==1]['time_diff'].fillna(0, inplace=True)
        df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
        df['goal_time'] = df['goal_time'].map(convert_time, na_action='ignore')

        def corner(x, n):
            if type(x) != str:
                return x
            elif n == 4:
                return int(re.findall(r'\d+', x)[-1])
            elif n == 1:
                return int(re.findall(r'\d+', x)[0])

        # 通過順
        df['first_corner'] = df['corner_pass'].map(lambda x: corner(x, 1))
        df['last_corner'] = df['corner_pass'].map(lambda x: corner(x, 4))

        # 頭数で割る
        df['arriving_order'] = df['arriving_order'] / df['horse_num']
        df['popularity'] = df['popularity'] / df['horse_num']
        df['first_corner'] = df['first_corner'] / df['horse_num']
        df['last_corner'] = df['last_corner'] / df['horse_num']

        # 距離で割る
        df['goal_time'] = df['goal_time'] / df['distance'] * 100

        self.data_p = df.set_index('horse_id')

    def _get_l_days(self, target_df: pd.DataFrame, date: dt.datetime):
        filtered_df = target_df.groupby(level=0).head(1)
        td = date - filtered_df['date']
        td.rename('l_days', inplace=True)
        return td.map(lambda x: x.days)

    def _get_average(self, target_df: pd.DataFrame, n_samples: Union[int, str] = 'all'):
        ave_target_cols = [
            'arriving_order', 'popularity', 'distance', 'goal_time', 'time_diff', 'last_three_furlong',
            'first_corner', 'last_corner', 'prise'
        ]

        if n_samples == 'all':
            filtered_df = target_df
        elif n_samples > 0:
            filtered_df = target_df.groupby(level=0).head(n_samples)
        else:
            raise InvalidArgument("'n_samples' must be >0")

        average = filtered_df.groupby(level=0)[ave_target_cols].mean()
        return pd.DataFrame(average).add_suffix('_{}R'.format(n_samples))

    def _merge_per_date(self, results: pd.DataFrame, date: dt.datetime, ave_samples_list: List[Union[int, str]] = [5, 9, 'all']) -> pd.DataFrame:
        df = results[results['date'] == date]
        horse_id_list = df['horse_id']
        target_df = self.data_p.query('index in @horse_id_list')
        target_df = target_df[target_df['date'] < date].sort_values('date', ascending=False)

        merged_df = df.merge(self._get_l_days(target_df, date), left_on='horse_id', right_index=True, how='left')
        for ave_samples in ave_samples_list:
            merged_df = merged_df.merge(self._get_average(target_df, ave_samples), left_on='horse_id', right_index=True, how='left')
        return merged_df

    def merge_all(self, results: pd.DataFrame, ave_samples_list: List[Union[int, str]] = [5, 9, 'all']) -> pd.DataFrame:
        date_list = results['date'].unique()
        merged_df = pd.concat([self._merge_per_date(results, date, ave_samples_list) for date in tqdm(date_list)])
        return merged_df


class DataProcessor:
    def __init__(self) -> None:
        self.data = pd.DataFrame()
        self.data_p = pd.DataFrame()
        self.data_m = pd.DataFrame()
        self.data_pe = pd.DataFrame()
        self.data_c = pd.DataFrame()

    def merge_horse_results(self, hr: HorseResults, ave_samples_list: List[Union[int, str]] = [5, 9, 'all']) -> None:
        df = self.data_p.copy()
        df = hr.merge_all(df, ave_samples_list)
        self.data_m = df

    def merge_peds(self, peds: Peds):
        self.data_pe = self.data_m.merge(peds.data_e, left_on='horse_id', right_index=True, how='left')

        self.no_peds = self.data_pe[self.data_pe['father'].isnull()]['horse_id'].unique()
        if len(self.no_peds) > 0:
            warnings.warn('WARNING: scrape peds at horse_id_list "no_peds"')

    def process_categorical(
            self,
            le_horse: LabelEncoder,
            le_jockey: LabelEncoder,
            le_trainer: LabelEncoder
        ) -> None:

        df = self.data_pe.copy()

        mask_horse = df['horse_id'].isin(le_horse.classes_)
        new_horse_id = df['horse_id'].mask(mask_horse).dropna().unique()
        le_horse.classes_ = np.concatenate([le_horse.classes_, new_horse_id])
        df['horse_id'] = le_horse.transform(df['horse_id'])

        mask_jockey = df['jockey_id'].isin(le_jockey.classes_)
        new_jockey_id = df['jockey_id'].mask(mask_jockey).dropna().unique()
        le_jockey.classes_ = np.concatenate([le_jockey.classes_, new_jockey_id])
        df['jockey_id'] = le_jockey.transform(df['jockey_id'])

        mask_trainer = df['trainer_id'].isin(le_trainer.classes_)
        new_trainer_id = df['trainer_id'].mask(mask_trainer).dropna().unique()
        le_trainer.classes_ = np.concatenate([le_trainer.classes_, new_trainer_id])
        df['trainer_id'] = le_trainer.transform(df['trainer_id'])

        df['weather'] = df['weather'].map(encode_weather)
        df['ground'] = df['ground'].map(encode_ground)
        df['race_type'] = df['race_type'].map(encode_race_type)
        df['turn'] = df['turn'].map(encode_turn)
        df['sex'] = df['sex'].map(encode_sex)

        self.data_c = df

    def get_final_data(self, drop_nan: bool = False):
        df = self.data_c.copy()
        if drop_nan:
            df = df.dropna()
        return df.drop(['horse_id'], axis=1)


class Results(DataProcessor):
    def __init__(self, result_df: pd.DataFrame, is_merged: bool = False) -> None:
        super().__init__()
        if is_merged:
            self.data_m = result_df
        else:
            self.data = result_df
            self.preprocesing()
        self.le_horse = None
        self.le_jockey = None
        self.le_trainer = None

    @classmethod
    def read_db(
            cls,
            db_path: str,
            begin_date: int = None,
            end_date: int = None,
            flat_only: bool = False
        ) -> 'Results':

        dbm = DBManager(db_path)

        # 条件文の生成
        where = ''
        if begin_date is not None:
            where += 'date>={}'.format(begin_date)
        if end_date is not None:
            if where != '':
                where += ' and '
            where += 'date<={}'.format(end_date)
        if flat_only:
            if where != '':
                where += ' and '
            where += 'race_type IN ("芝", "ダート")'

        df = dbm.select_resutls(where)
        return cls(df)

    @classmethod
    def read_pickle(cls, filepath):
        df = pd.read_pickle(filepath)
        return cls(df, True)

    def preprocesing(self) -> None:
        df = self.data.copy()

        df.set_index('race_id', inplace=True)

        # 何月開催か
        df['month'] = df['date'].map(lambda x: (x % 10000) // 100)

        # 着順
        df['arriving_order'] = pd.to_numeric(df['arriving_order'], errors='coerce')
        df.dropna(subset=['arriving_order'], inplace=True)
        df['arriving_order'] = df['arriving_order'].astype(int)
        #df['rank'] = df['arriving_order'].map(lambda x: 1 if x < 4 else 0)

        # 性齢
        df['sex'] = df['sex_age'].map(lambda x: str(x)[0])
        df['age'] = df['sex_age'].map(lambda x: re.findall(r'\d+', x)[0]).astype(int)

        # 馬体重
        df['weight'] = df['horse_weight'].str.split('(', expand=True)[0].astype(int)
        df['weight_change'] = df['horse_weight'].str.split('(', expand=True)[1].str[:-1].astype(int)

        # 型変換
        df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
        df['place_id'] = df['place_id'].astype(int)

        # 賞金
        df['prise'].fillna(0, inplace=True)
        df = df.merge(df.groupby(level=0)['prise'].max().rename('win_prise'),
                      left_index=True, right_index=True, how='left')

        # 不要列削除
        df.drop(['sex_age', 'horse_weight', 'win_odds', 'popularity',
                 'corner_pass', 'owner_name', 'margin_length', 'race_title',
                 'goal_time', 'last_three_furlong', 'prise'],
                axis=1, inplace=True)

        self.data_p = df

    def process_categorical(self) -> None:
        self.le_horse = LabelEncoder().fit(self.data_pe['horse_id'])
        self.le_jockey = LabelEncoder().fit(self.data_pe['jockey_id'])
        self.le_trainer = LabelEncoder().fit(self.data_pe['trainer_id'])
        super().process_categorical(self.le_horse, self.le_jockey, self.le_jockey)

    def target_binary(self, drop_nan: bool = False):
        df = self.get_final_data(drop_nan)
        df['l_days'].dropna(inplace=True)
        df['rank']  = df['arriving_order'].map(lambda x: 0 if x < 4 else 1)
        return df.drop(['arriving_order'], axis=1)

    def target_multiclass(self, drop_nan: bool = False):
        df = self.get_final_data(drop_nan)
        df['l_days'].dropna(inplace=True)

        def get_rank(n):
            if n < 4:
                return 0
            elif n >= 4 and n < 9:
                return 1
            else:
                return 2

        df['rank'] = df['arriving_order'].map(get_rank)
        return df.drop(['arriving_order'], axis=1)


class RaceCard(DataProcessor):
    def __init__(self, df: pd.DataFrame) -> None:
        super().__init__()
        self.data = df
        self.preprocess()

    @classmethod
    def scrape(cls, race_id_list: List[str], date: int) -> 'RaceCard':
        data = pd.DataFrame()
        for race_id in tqdm(race_id_list):
            race_df = scrape_race_card(race_id, date)
            data = data.append(race_df)

        return cls(data)

    def preprocess(self) -> None:
        df = self.data.copy()

        # 何月開催か
        df['month'] = df['date'].map(lambda x: (x % 100000000) // 1000000)

        # 性齢
        df['sex'] = df['性齢'].map(lambda x: str(x)[0])
        df['age'] = df['性齢'].map(lambda x: str(x)[1]).astype(int)

        # 馬体重
        df['weight'] = df['馬体重(増減)'].str.split('(', expand=True)[0].astype(int)
        df['weight_change'] = df['馬体重(増減)'].str.split('(', expand=True)[1].str[:-1].astype(int)

        df['place_id'] = df['race_id'].map(lambda x: int(x[4:6]))
        df['hold_no'] = df['race_id'].map(lambda x: int(x[6:8]))
        df['hold_day'] = df['race_id'].map(lambda x: int(x[8:10]))
        df['race_no'] = df['race_id'].map(lambda x: int(x[10:12]))

        # 型変更
        df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
        df['枠'] = df['枠'].astype(int)
        df['馬番'] = df['馬番'].astype(int)
        df['斤量'] = df['斤量'].astype(float)

        df.set_index('race_id', inplace=True)

        # 列名変更
        df.rename(columns={'枠':'frame_no', '馬番':'horse_no', '斤量':'impost'}, inplace=True)

        self.data_p = df[['horse_no', 'frame_no', 'horse_id', 'impost',
                          'jockey_id', 'trainer_id', 'date', 'place_id',
                          'hold_no', 'hold_day', 'race_no', 'distance',
                          'race_type', 'turn', 'ground', 'weather',
                          'horse_num', 'month', 'sex', 'age', 'weight',
                          'weight_change', 'win_prise']]

    def process_categorical(self, results: Results) -> None:
        super().process_categorical(results.le_horse, results.le_jockey, results.le_trainer)
