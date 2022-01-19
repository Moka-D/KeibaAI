# -*- coding: utf-8 -*-
import sys
import re
import datetime as dt
from common.register import Registar
from common.db_config import db_config
from common.scrape import DATE_PATTERN, scrape_race_card, scrape_race_card_id_list
from common.utils import InvalidArgument
from tqdm import tqdm
import joblib


def main(args):
    # 引数処理
    if len(args) != 2:
        raise InvalidArgument('It needs 1 argument.')
    if re.fullmatch(DATE_PATTERN, args[1]) is None:
        raise InvalidArgument("Argument Format -> 'yyyy/mm/dd'")

    race_date = dt.datetime.strptime(args[1], '%Y/%m/%d').date().strftime('%Y%m%d')
    print('Creating race id list...')
    try:
        race_id_list = scrape_race_card_id_list(race_date)
    except AttributeError:
        raise InvalidArgument("Invalid race date.")

    if not race_id_list:
        print('"race_id_list" is null.')
        return

    reg = Registar(db_config['main'])
    ng_horse_id_list = []
    for race_id in tqdm(race_id_list):
        try:
            race_card = scrape_race_card(race_id, int(race_date))
            reg.regist_horse_peds(dict(zip(race_card['horse_id'], race_card['馬名'])))
            ng_horse_id_list += reg.regist_horse_results(race_card['horse_id'].to_list(), tqdm_leave=False)
        except Exception as e:
            print("'{}' has been raised with race_id:'{}' ({})".format(e.__class__.__name__, race_id, e.args[0]))

    if ng_horse_id_list:
        print("There is ng_horse_id_list.")
        joblib.dump(ng_horse_id_list, 'error_id_list.pkl')

    print('Finished')


if __name__ == '__main__':
    main(sys.argv)
