# -*- coding: utf-8 -*-
from ast import arg
import sys
import pandas as pd
import numpy as np
import datetime as dt
import lightgbm as lgb
from common.data_processor import (
    RaceCard,
    Results,
    HorseResults,
    Peds
)
from common.db_config import db_config
from common.utils import InvalidArgument


RESULTS_M_PKL_PATH = "./results_m_2015_2021.pickle"


def main(args):
    if len(args) < 2:
        raise InvalidArgument("Arguments are too short. It needs 3 argumens at least.")

    race_id = args[1]

    today = int(dt.datetime.today().strftime('%Y%m%d'))
    rc = RaceCard.scrape([race_id], today)

    #r = Results.read_db(db_config['main'], begin_date=20150101, end_date=20211231, flat_only=True)
    r = Results.read_pickle(RESULTS_M_PKL_PATH)
    hr = HorseResults.read_db(db_config['main'])
    #r.merge_horse_results(hr, ave_samples=5)

    p = Peds.read_db(db_config['main'])
    r.merge_peds(p)

    rc.merge_horse_results(hr, ave_samples=5)
    rc.merge_peds(p)

    r.process_categorical()
    rc.process_categorical(r)

    #train, valid = split_data(r.data_c)
    train = r.get_final_data(drop_nan=True)
    X_train = train.drop(['rank', 'date'], axis=1)
    y_train = train['rank']
    #X_valid = valid.drop(['horse_id', 'rank', 'date'], axis=1)
    #y_valid = valid['rank']

    lgb_train = lgb.Dataset(X_train, y_train)
    #lgb_valid = lgb.Dataset(X_valid, y_valid, reference=lgb_train)

    params = {
        'objective': 'binary',
        'random_state': 100,
        'feature_pre_filter': False,
        'lambda_l1': 9.927931426029202,
        'lambda_l2': 2.2847450729790525e-08,
        'num_leaves': 5,
        'feature_fraction': 0.4,
        'bagging_fraction': 1.0,
        'bagging_freq': 0,
        'min_child_samples': 20,
        'num_iterations': 1000,
        'categorical_column': [3, 4, 5, 10, 11, 12, 13, 16, 30, 31, 32, 33, 34, 35]
    }

    model = lgb.train(
                params,
                lgb_train,
                verbose_eval=50
            )

    X_test = rc.data_c.drop(['horse_id', 'date'], axis=1)
    y_pred_proba = model.predict(X_test, num_iteration=model.best_iteration)

    y_pred = (y_pred_proba - np.mean(y_pred_proba)) / np.std(y_pred_proba)
    y_pred_std = (y_pred - np.min(y_pred)) / (np.max(y_pred) - np.min(y_pred))

    df = rc.data.copy()
    df['y_pred'] = y_pred_proba
    df['y_pred_std'] = y_pred_std
    print(df[['枠', '馬番', '馬名', 'y_pred', 'y_pred_std']].sort_values('y_pred', ascending=False))


if __name__ == '__main__':
    main(sys.argv)
