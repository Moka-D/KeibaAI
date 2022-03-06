#!/usr/bin/python

import os
import sys
sys.path.append(os.pardir)
import pandas as pd
import re
import datetime as dt
from dateutil.relativedelta import relativedelta
from common.data_processor import RaceCard, Results, HorseResults
from config.db_config import db_config
from common.utils import DATE_PATTERN, InvalidArgument, Racecourse
import lightgbm as lgb


params = {
    'binary': {
        'task': 'train',
        'boosting_type': 'gbdt',
        'objective': 'binary',
        'metric': 'auc'
    },
    'regression': {
        'task': 'train',
        'boosting_type': 'gbdt',
        'objective': 'regression',
        'metric': 'rmse'
    }
}


def predict_by_lgb(
    race_id: str,
    target: str,
    race_date: str = None
):
    if target not in ['binary', 'regression']:
        raise InvalidArgument("Argument 'target' mest be 'binary' or 'regression'")

    if race_date is None:
        race_date_d = dt.datetime.today().date()
    else:
        if re.fullmatch(DATE_PATTERN, race_date) is None:
            raise InvalidArgument("Argument 'race_date' Format -> 'yyyy/mm/dd'")

        race_date_d = dt.datetime.strptime(race_date, '%Y/%m/%d').date()

    end_date_d = race_date_d - relativedelta(years=1, month=12, day=31)
    end_date = int(end_date_d.strftime('%Y%m%d'))
    begin_date_d = dt.date(year=2014, month=1, day=1)
    begin_date = int(begin_date_d.strftime('%Y%m%d'))

    print("Scraping race card...")
    rc = RaceCard.scrape(race_id, race_date_d)
    place_id = rc.data_p['place_id'][0]
    race_type = rc.data_p['race_type'][0]
    distance = rc.data_p['distance'][0]

    print("Race info:")
    print("  race course :", Racecourse.value_of(place_id).ja)
    print("  race type   :", race_type)
    print("  distance    :", distance)

    rc.create_target_df()

    print("Loading data from DB...")
    r = Results.read_db(db_config['read'])
    r.create_target_df(begin_date=begin_date,
                       end_date=end_date,
                       place_id=place_id,
                       race_type=race_type,
                       distance=distance,
                       option=" and age_limit <= 2")
                       #option=" and classification != 6")

    print("Race num of training data from {} to {} : {}".format(begin_date_d.strftime('%Y/%m/%d'),
                                                                end_date_d.strftime('%Y/%m/%d'),
                                                                len(r.data_t.index.unique())))

    horse_id_list = set(r.data_t['horse_id'].unique())
    horse_id_list |= set(rc.data_t['horse_id'].unique())

    hr = HorseResults.read_db(db_config['read'], horse_id_list)
    r.merge_horse_results(hr)
    rc.merge_horse_results(r, hr)

    if target == 'binary':
        train = r.target_binary()
        X_train = train.drop(['race_date', 'rank'], axis=1)
        y_train = train['rank']
    elif target == 'regression':
        train = r.target_goal_time()
        X_train = train.drop(['race_date', 'goal_time'], axis=1)
        y_train = train['goal_time']

    X_test = rc.get_test_df(X_train)
    X_train['sex'] = pd.Categorical(X_train['sex'], ['牡', '牝'])
    X_train = pd.get_dummies(X_train, columns=['sex'])
    X_test['sex'] = pd.Categorical(X_test['sex'], ['牡', '牝'])
    X_test = pd.get_dummies(X_test, columns=['sex'])

    print("Training...")
    lgb_train = lgb.Dataset(X_train, y_train)
    model = lgb.train(params=params[target], train_set=lgb_train)

    print("=" * 50)
    print("Predict Result")
    race_card = rc.data[['馬番', '馬名']].copy()
    race_card['pred'] = model.predict(X_test, num_iteration=model.best_iteration)
    if target == 'binary':
        race_card.sort_values('pred', ascending=False, inplace=True)
    elif target == 'regression':
        race_card.sort_values('pred', ascending=True, inplace=True)
    print(race_card)
