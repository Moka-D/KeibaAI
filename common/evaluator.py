# -*- coding: utf-8 -*-
import os
import sys
sys.path.append(os.pardir)
from typing import Any, Tuple, Union
import numpy as np
import pandas as pd
from common.payoff import Payoff


TICKET_UNIT_PRICE: int = 100


class ModelEvalator:
    def __init__(self, model: Any, db_path: str) -> None:
        self.model = model
        self.pt = Payoff.read_db(db_path)

    def pred_table(self, X: pd.DataFrame) -> Union[pd.DataFrame, pd.Series]:
        pred_table = X.copy()[['horse_no']]
        pred_table['pred'] = self.model.predict_proba(X)[:, 0]
        return pred_table

    def feature_importance(self, X, n_display=20):
        importances = pd.DataFrame({'features': X.columns, 'importance': self.model.feature_importance})
        return importances.sort_values('importance', ascending=False)[:n_display]

    def tansho_return(
            self,
            X: pd.DataFrame,
            threshold: float = 0.5,
            horse_num: Union[int, None] = 1
        ) -> Tuple[float, float]:

        pred_table = self.pred_table(X)
        if horse_num is None:
            pred_table = pred_table[pred_table['pred'] > threshold].sort_values('pred', ascending=False).groupby(level=0).head()
        else:
            pred_table = pred_table[pred_table['pred'] > threshold].sort_values('pred', ascending=False).groupby(level=0).head(horse_num)

        n_bets = len(pred_table)
        win_money = 0
        n_hits = 0
        tansho = self.pt.tansho.copy()

        for race_id, row_p in pred_table.iterrows():
            horse_no = row_p['horse_no']
            race_payoff = tansho.loc[race_id]
            if isinstance(race_payoff, pd.DataFrame):
                for row_r in race_payoff.itertuples():
                    if horse_no == row_r.pattern:
                        win_money += row_r.payoff
                        n_hits += 1
            elif horse_no == race_payoff['pattern']:
                win_money += race_payoff['payoff']
                n_hits += 1

        return_rate = win_money / (n_bets * TICKET_UNIT_PRICE)
        hit_rate = n_hits / n_bets

        return return_rate, hit_rate
