#!/usr/bin/python
"""モデル評価モジュール
"""

import os
import sys

sys.path.append(os.pardir)
import itertools
from typing import Any, Tuple, Union

import numpy as np
import pandas as pd
from scipy.special import comb, perm

from common.payoff import Payoff
from common.utils import InvalidArgument

TICKET_UNIT_PRICE: int = 100


class ModelEvalator:
    def __init__(self, model: Any) -> None:
        self.model = model

    def pred_table(
        self,
        X: pd.DataFrame,
        target: str,
        with_sort: bool = True
    ) -> Union[pd.DataFrame, pd.Series]:

        pred_table = X.copy()[['horse_no']]
        if target in ['binary', 'regression']:
            pred_table['pred'] = self.model.predict(X, num_iteration=self.model.best_iteration)
        elif target == 'multiclass':
            pred_table['pred'] = self.model.predict(X, num_iteration=self.model.best_iteration)[..., 2]
        else:
            raise InvalidArgument("Invalid 'target'.")

        if with_sort:
            if target == 'regression':
                pred_table.sort_values('pred', ascending=True, inplace=True)
            else:
                pred_table.sort_values('pred', ascending=False, inplace=True)

        return pred_table.reset_index()

    def feature_importance(self, X, n_display=20):
        importances = pd.DataFrame({'features': X.columns, 'importance': self.model.feature_importances_})
        return importances.sort_values('importance', ascending=False)[:n_display]


def get_wins_single(
    pred_table: pd.DataFrame,
    payoff_table: pd.DataFrame
) -> Tuple[float, float, int]:

    win_money_list = []
    n_hits = 0

    for race_id in pred_table['race_id'].unique():
        horse_list = pred_table[pred_table['race_id'] == race_id]['horse_no'].tolist()
        result = payoff_table[payoff_table['race_id'] == race_id]
        win_money = 0
        for horse_no in horse_list:
            for row in result.itertuples():
                if horse_no == row.pattern:
                    win_money += (row.payoff)
                    n_hits += 1

        win_money_list.append(win_money)

    return n_hits, win_money_list


def get_wins_combination(
    pred_table: pd.DataFrame,
    payoff_table: pd.DataFrame,
    required_horse_num: int
) -> Tuple[float, float, int]:

    win_money_list = []
    n_hits = 0

    for race_id in pred_table['race_id'].unique():
        horse_list = pred_table[pred_table['race_id'] == race_id]['horse_no'].tolist()
        result = payoff_table[payoff_table['race_id'] == race_id]
        win_money = 0
        for horse_comb in itertools.combinations(horse_list, required_horse_num):
            for row in result.itertuples():
                if set(horse_comb) == set(row.pattern):
                    win_money += row.payoff
                    n_hits += 1

        win_money_list.append(win_money)

    return n_hits, win_money_list


def get_wins_permutation(
    pred_table: pd.DataFrame,
    payoff_table: pd.DataFrame,
    required_horse_num: int
) -> Tuple[float, float, int]:

    win_money_list = []
    n_hits = 0

    for race_id in pred_table['race_id'].unique():
        horse_list = pred_table[pred_table['race_id'] == race_id]['horse_no'].tolist()
        result = payoff_table[payoff_table['race_id'] == race_id]
        win_money = 0
        for horse_comb in itertools.permutations(horse_list, required_horse_num):
            for row in result.itertuples():
                if list(horse_comb) == row.pattern:
                    win_money += row.payoff
                    n_hits += 1

        win_money_list.append(win_money)

    return n_hits, win_money_list


def disp_top_n_box(
    df: pd.DataFrame,
    pt: Payoff,
    max_n: int = 3
) -> None:

    for n in range(1, max_n + 1):
        print('---- Top-{}'.format(n))
        print('                hit    ret    std_ret')

        pred_table = df.groupby('race_id').head(n)[['race_id', 'horse_no']]
        _disp_result('win', pred_table, pt, n)
        _disp_result('place', pred_table, pt, n)
        if n >= 2:
            _disp_result('quinella place', pred_table, pt, n)
            _disp_result('quinella', pred_table, pt, n)
            _disp_result('exacta', pred_table, pt, n)
        if n >= 3:
            _disp_result('trio', pred_table, pt, n)
            _disp_result('trifecta', pred_table, pt, n)

        print('')


def _disp_result(
    type: str,
    pred_table: pd.DataFrame,
    pt: Payoff,
    n: int
) -> None:

    if type == 'win':   # 単勝
        n_bets_per_race = n
        n_hits, win_money_list = get_wins_single(pred_table, pt.tansho)
    elif type == 'place':   # 複勝
        n_bets_per_race = n
        n_hits, win_money_list = get_wins_single(pred_table, pt.fukusho)
    elif type == 'quinella place':  # ワイド
        n_bets_per_race = comb(n, 2, exact=True)
        n_hits, win_money_list = get_wins_combination(pred_table, pt.wide, 2)
    elif type == 'quinella':    # 馬連
        n_bets_per_race = comb(n, 2, exact=True)
        n_hits, win_money_list = get_wins_combination(pred_table, pt.umaren, 2)
    elif type == 'exacta':  # 馬単
        n_bets_per_race = perm(n, 2, exact=True)
        n_hits, win_money_list = get_wins_permutation(pred_table, pt.umatan, 2)
    elif type == 'trio':    # 3連複
        n_bets_per_race = comb(n, 3, exact=True)
        n_hits, win_money_list = get_wins_combination(pred_table, pt.sanrenfuku, 3)
    elif type == 'trifecta':    # 3連単
        n_bets_per_race = perm(n, 3, exact=True)
        n_hits, win_money_list = get_wins_permutation(pred_table, pt.sanrentan, 3)
    else:
        raise InvalidArgument("Invalid 'type'.")

    n_bets = len(pred_table['race_id'].unique()) * n_bets_per_race
    hit = n_hits / n_bets
    ret = sum(win_money_list) / (n_bets * 100)
    std_ret = np.std(list(map(lambda x: x / (n_bets_per_race * 100), win_money_list)))
    print('{:<14}  {:.3f}  {:.3f}  {:.3f}'.format(type, hit, ret, std_ret))
