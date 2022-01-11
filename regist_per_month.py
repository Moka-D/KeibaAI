# -*- coding: utf-8 -*-
import sys
import datetime as dt
from common.register import Registar
from common.db_config import db_config
from common.utils import InvalidArgument
from common.scrape import scrape_period_race_id_list


def main(args):
    # 引数処理
    if len(args) != 3:
        raise InvalidArgument('It needs 2 arguments.')
    if not args[1].isdigit() or not args[2].isdigit():
        raise InvalidArgument('Arguments must be numeric.')

    year = int(args[1])
    this_year = dt.datetime.today().year
    if year < 1975 or year > this_year:
        raise InvalidArgument("First Argument (year) must be 1975~{}".format(this_year))

    month = int(args[2])
    if month < 1 or month > 12:
        raise InvalidArgument("Second Argument (month) must be 1~12")

    print('Creating race id list...')
    race_id_list = scrape_period_race_id_list(start_year=year,
                                   end_year=year,
                                   start_month=month,
                                   end_month=month)

    #reg = Registar(db_config['path'])
    #reg.regist_race_results(race_id_list)
    #print('Finished.')
    print(race_id_list)


if __name__ == '__main__':
    main(sys.argv)
