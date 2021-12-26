#!/usr/bin/python
# -*- coding: utf-8 -*-
import datetime as dt
import re
from typing import List, Tuple, Union
import warnings
import pandas as pd
from common import get_environment
if get_environment() == 'Jupyter':
    from tqdm.notebook import tqdm
else:
    from tqdm import tqdm
from sklearn.preprocessing import LabelEncoder
import numpy as np
from dbapi import DBManager
from scraping import scrape_race_card
import itertools


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


class Peds:
    def __init__(self, peds: pd.DataFrame) -> None:
        self.data = peds
        self.data_e = pd.DataFrame()

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
        self.data_e = df.astype('category')


class DataProcessor:
    def __init__(self) -> None:
        self.data = pd.DataFrame()
        self.data_p = pd.DataFrame()
        self.data_m = pd.DataFrame()
        self.data_pe = pd.DataFrame()
        self.data_c = pd.DataFrame()
        self.n_samples = 0

    def average(self, horse_id_list: List[str], date: dt.datetime, n_samples: Union[int, str] = 'all'):
        ave_target_cols = [
            'arriving_order', 'distance', 'time_diff', 'last_three_furlong',
            'first_corner', 'last_corner', 'prise'
        ]

        target_df = self.data_p.query('horse_id in @horse_id_list')
        target_df = target_df.set_index('horse_id')
        filtered_df = target_df[target_df['date'] < date].sort_values('date', ascending=False).groupby(level=0).head(n_samples)

        if n_samples == 'all':
            filtered_df = target_df[target_df['date'] < date].sort_values('date', ascending=False).groupby(level=0)
        elif n_samples > 0:
            filtered_df = target_df[target_df['date'] < date].sort_values('date', ascending=False).groupby(level=0).head(n_samples)

        average = filtered_df.groupby(level=0)[ave_target_cols].mean()
        return pd.DataFrame(average).add_suffix('_{}R'.format(n_samples))

    def concat_old_results(self, horse_id_list: List[str], date: dt.datetime, n_samples: int = 3) -> pd.DataFrame:
        if n_samples <= 0:
            raise Exception('n_samples must be >0')

        target_df = self.data_p.query('horse_id in @horse_id_list')
        target_df = target_df.set_index('horse_id')
        filtered_df = target_df[target_df['date'] < date].sort_values('date', ascending=False).groupby(level=0).head(n_samples)

        #average_5R = self.average(target_df, date, 5)
        #average_9R = self.average(target_df, date, 9)
        #average_df = pd.concat([average_5R, average_9R], axis=1)

        pre_target_cols = [
            'place_id', 'weather', 'race_no', 'horse_num', 'horse_no',
            'arriving_order', 'race_type','distance', 'ground', 'goal_time',
            'time_diff', 'last_three_furlong', 'first_corner', 'last_corner',
            'prise'
        ]

        p_grouped_data = filtered_df.groupby(level=0)[pre_target_cols]
        #p_data = average_df.copy()
        p_data = pd.DataFrame()
        for n in range(n_samples):
            pn_data = p_grouped_data.nth(n).add_prefix('p{}_'.format(n + 1))
            p_data = pd.concat([p_data, pn_data], axis=1)

        return p_data

    def merge(self, results: pd.DataFrame, date: dt.datetime, n_samples: Union[int, str] = 'all') -> pd.DataFrame:
        df = results[results['date'] == date]
        horse_id_list = df['horse_id']
        merged_df = df.merge(self.average(horse_id_list, date, n_samples), left_on='horse_id', right_index=True, how='left')
        return merged_df

    def merge_all(self, results: pd.DataFrame, n_samples: Union[int, str] = 'all') -> pd.DataFrame:
        date_list = results['date'].unique()
        merged_df = pd.concat([self.merge(results, date, n_samples) for date in tqdm(date_list)])
        return merged_df

    def merge_peds(self, peds: Peds):
        self.data_pe = self.data_m.merge(peds.data_e, left_on='horse_id', right_index=True, how='left')

        self.no_peds = self.data_pe[self.data_pe['father'].isnull()]['horse_id'].unique()
        if len(self.no_peds) > 0:
            warnings.warn('WARNING: scrape peds at horse_id_list "no_peds"')

    def process_categorical(self, le_horse: LabelEncoder, le_jockey: LabelEncoder, results_m: pd.DataFrame):
        df = self.data_pe.copy()

        mask_horse = df['horse_id'].isin(le_horse.classes_)
        new_horse_id = df['horse_id'].mask(mask_horse).dropna().unique()
        le_horse.classes_ = np.concatenate([le_horse.classes_, new_horse_id])
        df['horse_id'] = le_horse.transform(df['horse_id'])
        df['horse_id'] = df['horse_id'].astype('category')

        mask_jockey = df['jockey_id'].isin(le_jockey.classes_)
        new_jockey_id = df['jockey_id'].mask(mask_jockey).dropna().unique()
        le_jockey.classes_ = np.concatenate([le_jockey.classes_, new_jockey_id])
        df['jockey_id'] = le_jockey.transform(df['jockey_id'])
        df['jockey_id'] = df['jockey_id'].astype('category')

        """
        category_cols = ['weather', 'ground', 'race_type']
        for c_col in category_cols:
            target_cols = [c_col]
            target_cols.extend(['p{}_{}'.format(n, c_col) for n in range(1, self.n_samples + 1)])
            values_list = list(set(itertools.chain.from_iterable(results_m[target_cols].fillna('Na').values)))
            le = LabelEncoder().fit(values_list)
            for t_col in target_cols:
                df[t_col] = le.transform(df[t_col].fillna('Na'))
                df[t_col] = df[t_col].astype('category')
        """

        df['weather'] = results_m

        weather = results_m['weather'].unique()
        ground = results_m['ground'].unique()
        race_type = results_m['race_type'].unique()
        sexes = results_m['sex'].unique()
        turns = results_m['turn'].unique()
        df['weather'] = pd.Categorical(df['weather'], weather)
        df['ground'] = pd.Categorical(df['ground'], ground)
        df['race_type'] = pd.Categorical(df['race_type'], race_type)
        df['sex'] = pd.Categorical(df['sex'], sexes)
        df['turn'] = pd.Categorical(df['turn'], turns)
        df = pd.get_dummies(df, columns=['weather', 'ground', 'race_type', 'sex', 'turn'])

        self.data_c = df

    def get_final_data(self, drop_nan: bool = True):
        df = self.data_c.copy()
        if drop_nan:
            df = df.dropna()
        return df.drop(['horse_id', 'arriving_order', 'goal_time', 'last_three_furlong', 'time_diff', 'first_corner', 'last_corner', 'prise'], axis=1)


class Results(DataProcessor):
    def __init__(self, result_df: pd.DataFrame) -> None:
        super().__init__()
        self.data = result_df

    @classmethod
    def read_db(cls, db_path: str):
        dbm = DBManager(db_path)
        df = dbm.select_all_resutls()
        return cls(df)

    def preprocesing(self, flat_only: bool = True):
        """スクレイプしたレース結果データの前処理
        """
        df = self.data.copy()

        df.set_index('race_id', inplace=True)

        if flat_only:
            # 障害競走を除外
            df = df[df['race_type'] != '障害']

        # 何頭立てか
        df = df.merge(df.groupby(level=0).size().rename('horse_num'), left_index=True, right_index=True, how='left')

        def get_rank(x):
            if x <= 3:
                return 0
            elif x >= 4 and x <= 8:
                return 1
            else:
                return 2

        # 着順
        df['arriving_order'] = pd.to_numeric(df['arriving_order'], errors='coerce')
        df.dropna(subset=['arriving_order'], inplace=True)
        df['arriving_order'] = df['arriving_order'].astype(int)
        df['rank'] = df['arriving_order'].map(get_rank)

        # 性齢
        df['sex'] = df['sex_age'].map(lambda x: str(x)[0])
        df['age'] = df['sex_age'].map(lambda x: str(x)[1]).astype(int)

        # 馬体重
        df['weigth'] = df['horse_weight'].str.split('(', expand=True)[0].astype(int)
        df['weight_change'] = df['horse_weight'].str.split('(', expand=True)[1].str[:-1].astype(int)

        # 型変換
        df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
        df['goal_time'] = df['goal_time'].map(lambda x: float(x.split(':')[0]) * 60.0 + float(x.split(':')[1]))

        # 優勝賞金
        df = df.merge(df.groupby(level=0)['prise'].max().rename('win_prise'), left_index=True, right_index=True, how='left')

        # トップとのタイム差
        df = df.merge(df.groupby(level=0)['goal_time'].min().rename('top_time'), left_index=True, right_index=True, how='left')
        df['time_diff'] = df['goal_time'] - df['top_time']

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

        # 不要列削除
        df.drop(['sex_age', 'horse_weight', 'win_odds', 'popular_order', 'corner_pass', 'owner_name', 'margin_length', 'race_title', 'top_time', 'trainer_id'], axis=1, inplace=True)

        self.data_p = df

    def merge_old_results(self, start_date: int = 20180101, n_samples: int = 3) -> None:
        df = self.data_p.copy()
        df = df[df['date'] >= pd.to_datetime(start_date, format='%Y%m%d')]
        self.data_m = self.merge_all(df, n_samples)

    def process_categorical(self):
        self.le_horse = LabelEncoder().fit(self.data_pe['horse_id'])
        self.le_jockey = LabelEncoder().fit(self.data_pe['jockey_id'])
        super().process_categorical(self.le_horse, self.le_jockey, self.data_pe)

    def get_final_data(self, drop_nan: bool = True):
        df = self.data_c.copy()
        if drop_nan:
            df = df.dropna()
        return df.drop(['arriving_order', 'goal_time', 'last_three_furlong', 'time_diff', 'first_corner', 'last_corner', 'prise'], axis=1)


class RaceCard(DataProcessor):
    def __init__(self, df: pd.DataFrame) -> None:
        super().__init__()
        self.data = df

    @classmethod
    def scrape(cls, race_id_list: List[str], date: int):
        data = pd.DataFrame()
        for race_id in race_id_list:
            try:
                race_df = scrape_race_card(race_id, date)
                data = data.append(race_df)
            except IndexError:
                continue
            except Exception as e:
                print(e)
                print('race_id: '+ race_id)
                break

        return cls(data)

    def preprocess(self):
        """スクレイプした出馬表データを処理する関数

        Parameters
        ----------
        result_df : pandas.DataFrame
            出馬表DataFrame
        race_id : str
            レースID

        Returns
        -------
        df : pandas.DataFrame
            処理後出馬表DataFrame
        """
        df = self.data.copy()


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

        df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')

        df.set_index('race_id', inplace=True)

        # 列名変更
        df.rename(columns={'枠':'frame_no', '馬番':'horse_no', '斤量':'impost'}, inplace=True)

        self.data_p = df[['horse_no', 'frame_no', 'horse_id', 'impost',
                          'jockey_id','date', 'place_id', 'hold_no',
                          'hold_day', 'race_no', 'distance', 'race_type',
                          'turn', 'ground', 'weather', 'horse_num', 'sex',
                          'age', 'weight', 'weight_change', 'win_prise']]

    def merge_results(self, results: Results, n_samples: int = 2):
        df = self.data_p.copy()

        self.data_m = results.merge_all(df, n_samples)
