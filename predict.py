#!/usr/bin/python

import sys
import datetime as dt
import lightgbm as lgb
from common.data_processor import (
    RaceCard,
    Results,
    HorseResults,
    Peds,
    split_data
)
from config.db_config import db_config
from common.utils import InvalidArgument
from sklearn.model_selection import train_test_split
from model.model_config import model_params


RESULTS_M_PKL_PATH = "./results_m_2015_2021.pickle"


def main(args):
    if len(args) < 2:
        raise InvalidArgument("Arguments are too short. It needs 3 argumens at least.")

    race_id = args[1]

    today = int(dt.datetime.today().strftime('%Y%m%d'))
    rc = RaceCard.scrape([race_id], today)
    race_type = rc.data_p['race_type'][0]

    #r = Results.read_db(db_config['read'], begin_date=20150101, end_date=20211231)
    r = Results.read_pickle(RESULTS_M_PKL_PATH)
    hr = HorseResults.read_db(db_config['read'])
    #r.merge_horse_results(hr)

    p = Peds.read_db(db_config['read'])
    r.merge_peds(p)

    rc.merge_horse_results(hr)
    rc.merge_peds(p)

    r.process_categorical()
    rc.process_categorical(r)

    train, valid = split_data(r.target_binary(race_type))
    #train, valid = train_test_split(r.target_binary(), test_size=0.2, random_state=0)
    X_train = train.drop(['rank', 'race_date'], axis=1)
    y_train = train['rank']
    X_valid = valid.drop(['rank', 'race_date'], axis=1)
    y_valid = valid['rank']

    lgb_train = lgb.Dataset(X_train, y_train)
    lgb_valid = lgb.Dataset(X_valid, y_valid, reference=lgb_train)

    model = lgb.train(params=model_params[race_type],
                      train_set=lgb_train,
                      valid_sets=[lgb_train, lgb_valid],
                      num_boost_round=1000,
                      verbose_eval=100)

    X_test = rc.data_c.drop(['horse_id', 'race_date', 'race_type'], axis=1)
    y_pred = model.predict(X_test, num_iteration=model.best_iteration)

    df = rc.data.copy()
    df['pred'] = y_pred
    print(df[['枠', '馬番', '馬名', 'pred']].sort_values('pred', ascending=False))


if __name__ == '__main__':
    main(sys.argv)
