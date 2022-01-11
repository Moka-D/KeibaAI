# -*- coding: utf-8 -*-
import re
from sre_constants import IN
import sys
import re
import datetime as dt
from common.register import Registar
from common.db_config import db_config
from common.scrape import DATE_PATTERN, scrape_race_card, scrape_race_card_id_list
from common.utils import InvalidArgument


def main(args):
    # 引数処理
    if len(args) != 2:
        raise InvalidArgument('It needs 1 argument.')
    if re.fullmatch(DATE_PATTERN, args[1]) is None:
        raise InvalidArgument("Argument Format -> 'xxxx/yy/zz'")

    race_date = dt.datetime.strptime(args[1], '%Y/%m/%d').date().strftime('%Y%m%d')
    print('Creating race id list...')
    try:
        race_id_list = scrape_race_card_id_list(race_date)
    except AttributeError:
        raise InvalidArgument("Invalid race date.")

    reg = Registar(db_config['path'])
    ng_id_list = []
    for race_id in race_id_list:
        race_card = scrape_race_card(race_id, int(race_date))
        reg.regist_horse_peds(dict(zip(race_card['horse_id'], race_card['馬名'])))
        ng_id_list += reg.regist_horse_results(race_card['horse_id'].to_list(), tqdm_leave=False)

    print(ng_id_list)


if __name__ == '__main__':
    main(sys.argv)
