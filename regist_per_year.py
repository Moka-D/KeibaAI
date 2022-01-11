# -*- coding: utf-8 -*-
import sys
from common.utils import InvalidArgument, create_race_id_list
from common.register import Registar
from common.db_config import db_config


def regist_per_year(register, year_list):
    for year in year_list:
        try:
            race_id_list = create_race_id_list(year)
        except InvalidArgument as e:
            print(e)
            break

        register.regist_race_results(race_id_list)
        print("Race data of {} has been inserted successfully.".format(year))


def main(args):
    # 引数処理
    if len(args) < 2:
        raise InvalidArgument('Arguments are too short. It needs 2 argumens at least.')

    year_list = []
    for i in range(1, len(args)):
        if args[i].isdigit():
            year_list.append(int(args[i]))
        else:
            raise InvalidArgument('Arguments must be numeric.')

    reg = Registar(db_config['path'])
    for year in year_list:
        try:
            race_id_list = create_race_id_list(year)
        except InvalidArgument as e:
            print(e)
            continue

        reg.regist_race_results(race_id_list)

    print("Race data of {} has been inserted successfully.".format(year))


if __name__ == '__main__':
    main(sys.argv)
