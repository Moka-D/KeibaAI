#!/usr/bin/python

import argparse
import os
import re

from parso import parse
from common.utils import DATE_PATTERN, InvalidArgument
from model.knn import predict_by_knn
from model.lgb import predict_by_lgb


def main(
    race_id: str,
    race_date: str = None
):
    if race_date is not None and re.fullmatch(DATE_PATTERN, race_date) is None:
        raise InvalidArgument("Argument Format -> 'yyyy/mm/dd'")

    model_no = ""
    while True:
        os.system('cls')
        print("予測に使用するモデルを選択")
        print("  1: k-NN (3着内率予測)")
        print("  2: LightGBM (3着内率予測)")
        print("  3: LightGBM (走破タイム予測)")
        print("  0: 終了")
        model_no = input("> ")

        if model_no == '0':
            print("終了します。")
            return
        elif model_no == '1':
            predict_by_knn(race_id, race_date)
            return
        elif model_no == '2':
            predict_by_lgb(race_id, 'binary', race_date)
            return
        elif model_no == '3':
            predict_by_lgb(race_id, 'regression', race_date)
            return
        else:
            print("不正な入力です。")
            _ = input("何かキーを押してください。")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('race_id', help="ID of race to predict")
    parser.add_argument('--date', '-d', help="Race date (Format:'yyyy/mm/dd')")
    args = parser.parse_args()

    main(args.race_id, args.date)
