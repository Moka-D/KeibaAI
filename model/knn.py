#!/usr/bin/python

import os
import sys

sys.path.append(os.pardir)
import datetime as dt
import re

from dateutil.relativedelta import relativedelta
from sklearn import metrics
from sklearn.neighbors import KNeighborsClassifier

from common.data_processor import (HorseResults, RaceCard, Results,
                                   proc_dummies_and_std)
from common.utils import DATE_PATTERN, InvalidArgument, Racecourse
from config.db_config import db_config


def predict_by_knn(
    race_id: str,
    race_date: str = None
):
    # end_date = (race_date // 10000 - 1) * 10000 + 101
    # begin_date = end_date - 50000

    if race_date is None:
        race_date_d = dt.datetime.today().date()
    else:
        if re.fullmatch(DATE_PATTERN, race_date) is None:
            raise InvalidArgument("Argument Format -> 'yyyy/mm/dd'")

        race_date_d = dt.datetime.strptime(race_date, '%Y/%m/%d').date()

    end_date_d = race_date_d - relativedelta(years=1, month=12, day=31)
    end_date = int(end_date_d.strftime('%Y%m%d'))
    begin_date_d = dt.date(year=2015, month=1, day=1)
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

    print("Race num of training data from {} to {} : {}".format(begin_date_d.strftime('%Y/%m/%d'),
                                                                end_date_d.strftime('%Y/%m/%d'),
                                                                len(r.data_t.index.unique())))

    horse_id_list = set(r.data_t['horse_id'].unique())
    horse_id_list |= set(rc.data_t['horse_id'].unique())

    hr = HorseResults.read_db(db_config['read'], horse_id_list)
    r.merge_horse_results(hr)
    rc.merge_horse_results(r, hr)

    train = r.target_binary()
    X_train = train.drop(['race_date', 'rank'], axis=1)
    y_train = train['rank']
    X_test = rc.get_test_df(X_train)
    print(X_test)

    X_train, sc = proc_dummies_and_std(X_train, dummies_dict={'sex': ['牝', '牡']})
    X_test, _ = proc_dummies_and_std(X_test, dummies_dict={'sex': ['牝', '牡']}, sc=sc)

    print("Training...")
    model = KNeighborsClassifier(n_neighbors=100)
    model.fit(X_train.values, y_train)
    print("Train Accuracy :", metrics.accuracy_score(y_train, model.predict(X_train.values)))

    print("=" * 50)
    print("Predict Result")
    race_card = rc.data[['馬番', '馬名']].copy()
    race_card['pred'] = model.predict_proba(X_test.values)[:, 1]
    race_card.sort_values('pred', ascending=False, inplace=True)
    print(race_card)


if __name__ == '__main__':
    predict_by_knn('202205010811', '2022/02/20')
