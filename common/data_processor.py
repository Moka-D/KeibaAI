#!/usr/bin/python
"""データ前処理モジュール
"""

import os
import sys
sys.path.append(os.pardir)
import datetime as dt
from logging import Logger
import re
from typing import List, Set, Tuple, Union, Dict, Any
import pandas as pd
from common.utils import (
    InvalidArgument,
    get_environment
)
import numpy as np
if get_environment() == 'Jupyter':
    from tqdm.notebook import tqdm
else:
    from tqdm import tqdm
from common.db_api import DBManager
from common.scrape import scrape_race_card
from sklearn.preprocessing import StandardScaler


def _convert_time(x):
    try:
        return float(x.split(':')[0]) * 60.0 + float(x.split(':')[1])
    except:
        return np.nan


def proc_dummies_and_std(
    df: pd.DataFrame,
    dummies_dict: Dict[str, List[str]],
    sc: Any = None
) -> Tuple[pd.DataFrame, Any]:

    df_tmp = df.copy()
    dummy_cols = dummies_dict.keys()
    std_cols = list(set(df_tmp.columns.tolist()) - set(dummy_cols))

    for col in dummy_cols:
        df_tmp[col] = pd.Categorical(df_tmp[col], dummies_dict[col])
    df_tmp = pd.get_dummies(df_tmp, columns=list(dummy_cols))

    if sc is None:
        sc = StandardScaler().fit(df_tmp[std_cols])

    scaled = pd.DataFrame(sc.transform(df_tmp[std_cols]), columns=std_cols, index=df_tmp.index)
    df_tmp.update(scaled)

    return df_tmp, sc


class HorseResults:
    def __init__(self, result_df: pd.DataFrame) -> None:
        if len(result_df.index) == 0:
            raise InvalidArgument("'result_df' is empty")

        self.data = result_df[['horse_id', 'race_date', 'place_id',
                               'weather', 'race_no', 'horse_no',
                               'win_odds', 'popularity',
                               'arriving_order', 'ground', 'goal_time',
                               'race_type', 'distance', 'ground', 'time_diff',
                               'corner_pass', 'last_three_furlong', 'prise',
                               'horse_num']]
        self.data_p = pd.DataFrame()
        self.preprocesing()

    @classmethod
    def read_db(cls, db_path: str, horse_id_list: Union[List[str], Set[str]] = None, logger: Logger = None) -> 'Results':
        dbm = DBManager(db_path, logger)
        if horse_id_list is None:
            df = dbm.select_horse_results()
        else:
            df = dbm.select_horse_reuslts_with_list(horse_id_list)
        return cls(df)

    def preprocesing(self) -> None:
        df = self.data.copy()

        # 型変換
        df['arriving_order'] = pd.to_numeric(df['arriving_order'], errors='coerce')
        df['race_date'] = pd.to_datetime(df['race_date'], format='%Y%m%d')
        df['goal_time'] = df['goal_time'].map(_convert_time, na_action='ignore')

        # 1着の着差を0にする
        df['time_diff'] = df['time_diff'].map(lambda x: 0 if x < 0 else x)
        df[df['arriving_order']==1]['time_diff'].fillna(0, inplace=True)

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
        df['d_arriving_order'] = df['arriving_order'] / df['horse_num']
        df['d_popularity'] = df['popularity'] / df['horse_num']
        df['d_first_corner'] = df['first_corner'] / df['horse_num']
        df['d_last_corner'] = df['last_corner'] / df['horse_num']

        # 距離で割る
        df['d_goal_time'] = df['goal_time'] / df['distance'] * 100

        self.data_p = df.set_index('horse_id')

    def _get_l_days(self, target_df: pd.DataFrame, date: np.datetime64):
        filtered_df = target_df.groupby(level=0).head(1)
        td = date - filtered_df['race_date']
        td.rename('l_days', inplace=True)
        return td.map(lambda x: x.days)

    def _get_old_results(self, target_df: pd.DataFrame, n_races: int = 1):
        if n_races < 1:
            raise Exception("n_races must be >= 1")

        pre_target_cols = ['distance', 'time_diff', 'last_three_furlong']

        # Nanの平均埋め
        f = lambda x: x.fillna(x.mean())
        transformed = target_df.copy()
        transformed[pre_target_cols] = target_df.groupby(level=0)[pre_target_cols].transform(f)

        # n走前のデータを取得
        p_grouped_data = transformed.sort_values('race_date', ascending=False).groupby(level=0)[pre_target_cols]
        p_data = pd.DataFrame()
        for n in range(n_races):
            pn_data = p_grouped_data.nth(n).add_prefix('p{}_'.format(n + 1))
            p_data = pd.concat([p_data, pn_data], axis=1)

        return p_data

    def _get_average(self, target_df: pd.DataFrame):
        ave_target_cols = ['d_first_corner', 'd_last_corner']
        return target_df.groupby(level=0)[ave_target_cols].mean()

    def _merge_per_date(
        self,
        results: pd.DataFrame,
        date: np.datetime64,
        base_df: pd.DataFrame,
        n_races: int = 1
    ) -> pd.DataFrame:

        df = results[results['race_date']==date].copy()
        one_month_ago = np.datetime64(pd.Timestamp(date) - pd.DateOffset(months=1))

        # 馬の過去複勝率を追加
        horse_id_list = df['horse_id'].unique()
        horse_df = self.data_p.query('index in @horse_id_list')
        horse_df = horse_df[horse_df['race_date'] < date].sort_values('race_date', ascending=False)
        horse_place_ratio = {}
        for horse_id in horse_id_list:
            horse_result = horse_df[horse_df.index==horse_id]['arriving_order']
            if len(horse_result) > 0:
                horse_place_ratio[horse_id] = len(horse_result[horse_result < 4]) / len(horse_result)
            else:
                horse_place_ratio[horse_id] = 0
        df['horse_place_ratio'] = df['horse_id'].map(horse_place_ratio)

        # 騎手の過去1ヶ月間の複勝率を追加
        jockey_id_list = df['jockey_id'].unique()
        jockey_df = base_df.query('race_date >= @one_month_ago and race_date < @date')
        jockey_place_ratio = {}
        for jockey_id in jockey_id_list:
            jockey_result = jockey_df[jockey_df['jockey_id']==jockey_id]['arriving_order']
            if len(jockey_result) > 0:
                jockey_place_ratio[jockey_id] = len(jockey_result[jockey_result < 4]) / len(jockey_result)
            else:
                jockey_place_ratio[jockey_id] = 0
        df['jockey_place_ratio'] = df['jockey_id'].map(jockey_place_ratio)

        # 平均情報の追加
        df = df.merge(self._get_average(horse_df),
                      left_on='horse_id', right_index=True, how='left')

        # 前走結果の追加
        df = df.merge(self._get_old_results(horse_df, n_races),
                      left_on='horse_id', right_index=True, how='left')

        # 前走からの日数を追加
        df = df.merge(self._get_l_days(horse_df, date),
                      left_on='horse_id', right_index=True, how='left')

        return df

    def merge_all(
        self,
        results: pd.DataFrame,
        base_df: pd.DataFrame,
        n_races: int = 1
    ) -> pd.DataFrame:
        date_list = results['race_date'].unique()
        merged_df = pd.concat([self._merge_per_date(results, date, base_df, n_races) for date in tqdm(date_list)])
        return merged_df


class DataProcessor:
    def __init__(self) -> None:
        self.data = pd.DataFrame()
        self.data_p = pd.DataFrame()
        self.data_t = pd.DataFrame()
        self.data_m = pd.DataFrame()


class Results(DataProcessor):
    def __init__(self, result_df: pd.DataFrame) -> None:
        if len(result_df.index) == 0:
            raise InvalidArgument("'result_df' is empty")

        super().__init__()
        self.data = result_df
        self.preprocesing()
        self.le_horse = None
        self.le_jockey = None
        self.le_trainer = None

    @classmethod
    def read_db(
            cls,
            db_path: str,
            logger: Logger = None
        ) -> 'Results':

        dbm = DBManager(db_path, logger)
        df = dbm.select_resutls()
        return cls(df)

    @classmethod
    def read_pickle(cls, filepath):
        df = pd.read_pickle(filepath)
        return cls(df, True)

    def preprocesing(self) -> None:
        df = self.data.copy()

        df.sort_values(['race_id', 'horse_no'], inplace=True)

        df.set_index('race_id', inplace=True)

        # 何頭立てか
        df = df.merge(df.groupby('race_id').size().rename('horse_num'), left_index=True, right_index=True, how='left')

        # 何月開催か
        df['month'] = df['race_date'].map(lambda x: (x % 10000) // 100)

        # 着順
        df['arriving_order'] = pd.to_numeric(df['arriving_order'], errors='coerce')
        df.dropna(subset=['arriving_order'], inplace=True)
        df['arriving_order'] = df['arriving_order'].astype(int)
        #df['rank'] = df['arriving_order'].map(lambda x: 1 if x < 4 else 0)

        # 性齢
        df['sex'] = df['sex_age'].map(lambda x: str(x)[0])
        df['age'] = df['sex_age'].map(lambda x: re.findall(r'\d+', x)[0]).astype(int)

        # 馬体重
        df = df[df['horse_weight']!='計不']
        df['weight'] = df['horse_weight'].str.split('(', expand=True)[0].astype(int)
        df['weight_change'] = df['horse_weight'].str.split('(', expand=True)[1].str[:-1].astype(int)

        # 型変換
        df['race_date'] = pd.to_datetime(df['race_date'], format='%Y%m%d')
        df['place_id'] = df['place_id'].astype(int)
        df['goal_time'] = df['goal_time'].map(_convert_time, na_action='ignore')

        # 賞金
        df['prise'].fillna(0, inplace=True)
        df = df.merge(df.groupby(level=0)['prise'].max().rename('win_prise'),
                      left_index=True, right_index=True, how='left')

        # 不要列削除
        df.drop(['sex_age', 'horse_weight', 'corner_pass', 'owner_name',
                 'margin_length', 'race_title', 'last_three_furlong'],
                axis=1, inplace=True)

        self.data_p = df

    def create_target_df(
        self,
        begin_date: int,
        end_date: int,
        place_id: int,
        race_type: str,
        distance: int,
        option: str = ""
    ):
        begin_date = pd.to_datetime(begin_date, format='%Y%m%d')
        end_date = pd.to_datetime(end_date, format='%Y%m%d')

        df = self.data_p.copy()
        df = df.query("race_date >= @begin_date and race_date <= @end_date and place_id == @place_id and race_type == @race_type and distance == @distance"
                      + option)
        self.data_t = df[['horse_no', 'frame_no', 'arriving_order', 'horse_id',
                          'impost', 'jockey_id', 'goal_time', 'popularity',
                          'prise', 'race_date', 'horse_num', 'sex', 'age',
                          'weight', 'weight_change', 'win_prise']]

    def merge_horse_results(self, hr: HorseResults, n_races: int = 1) -> None:
        df = self.data_t.copy()
        base_df = self.data_p.copy()
        df = hr.merge_all(df, base_df, n_races)
        self.data_m = df

    def target_binary(self, with_horse_no: bool = False) -> pd.DataFrame:
        if with_horse_no:
            drop_cols = ['arriving_order', 'horse_id', 'jockey_id',
                         'goal_time', 'popularity', 'prise']
        else:
            drop_cols = ['horse_no', 'arriving_order', 'horse_id', 'jockey_id',
                         'goal_time', 'popularity', 'prise']

        df = self.data_m.copy()
        df['rank']  = df['arriving_order'].map(lambda x: 1 if x < 4 else 0)
        return df.drop(drop_cols, axis=1)

    def target_multiclass(self) -> pd.DataFrame:
        df = self.data_m.copy()

        def get_rank(n):
            if n < 4:
                return 2
            elif n >= 4 and n < 9:
                return 1
            else:
                return 0

        df['rank'] = df['arriving_order'].map(get_rank)
        return df.drop(['horse_no', 'arriving_order', 'horse_id', 'jockey_id',
                        'goal_time', 'popularity', 'prise'], axis=1)

    def target_goal_time(self, with_horse_no: bool = False):
        if with_horse_no:
            drop_cols = ['arriving_order', 'horse_id', 'jockey_id',
                         'popularity', 'prise']
        else:
            drop_cols = ['horse_no', 'arriving_order', 'horse_id', 'jockey_id',
                         'popularity', 'prise']

        df = self.data_m.copy()
        return df.drop(drop_cols, axis=1)


class RaceCard(DataProcessor):
    def __init__(self, df: pd.DataFrame) -> None:
        if len(df.index) == 0:
            raise InvalidArgument("'df' is empty")

        super().__init__()
        self.data = df
        self.preprocess()

    @classmethod
    def scrape(cls, race_id: str, date: dt.date) -> 'RaceCard':
        df = scrape_race_card(race_id, date)
        return cls(df)

    def preprocess(self) -> None:
        df = self.data.copy()

        # 何月開催か
        df['month'] = df['race_date'].map(lambda x: (x % 100000000) // 1000000)

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
        df['race_date'] = pd.to_datetime(df['race_date'], format='%Y%m%d')
        df['枠'] = df['枠'].astype(int)
        df['馬番'] = df['馬番'].astype(int)
        df['斤量'] = df['斤量'].astype(float)

        df.set_index('race_id', inplace=True)

        # 列名変更
        df.rename(columns={'枠':'frame_no', '馬番':'horse_no', '斤量':'impost'}, inplace=True)

        self.data_p = df[['horse_no', 'frame_no', 'horse_id', 'impost',
                          'jockey_id', 'trainer_id', 'race_date', 'place_id',
                          'hold_no', 'hold_day', 'race_no', 'distance',
                          'race_type', 'turn', 'ground', 'weather',
                          'horse_num', 'month', 'sex', 'age', 'weight',
                          'weight_change', 'win_prise']]

    def create_target_df(self):
        df = self.data_p.copy()
        self.data_t = df[['horse_no', 'frame_no', 'horse_id', 'impost',
                          'jockey_id', 'race_date', 'horse_num', 'sex',
                          'age', 'weight', 'weight_change', 'win_prise']]

    def merge_horse_results(self, r: Results, hr: HorseResults, n_races: int = 1) -> None:
        df = self.data_t.copy()
        base_df = r.data_p.copy()
        df = hr.merge_all(df, base_df, n_races)
        self.data_m = df

    def get_test_df(self, train_df: pd.DataFrame):
        df = self.data_m.copy()
        return df[train_df.columns]
