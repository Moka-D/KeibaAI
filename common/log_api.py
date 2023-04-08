#!/usr/bin/python

import datetime as dt
import json
import os
from logging import Logger, config, getLogger


def get_module_logger(
    module: str,
    log_dir: str,
    config_path: str = './config/log_config.json',
    verbose: bool = False
) -> Logger:
    """ログハンドラ取得API

    Parameters
    ----------
    module : str
        モジュール名
    log_dir : str
        ログ保存先ディレクトリ
    config_path : str, default './config/log_config.json'
        ログ設定ファイルパス
    verbose : bool, default False
        Debug表示を行うかどうか

    Returns
    -------
    Logger
        ログハンドラ
    """

    with open(config_path, 'r') as f:
        log_conf = json.load(f)

    # ログファイルのパスを変更
    log_filename = '{}.log'.format(dt.datetime.now().strftime("%Y%m%d_%H%M%S"))
    log_filepath = os.path.join(log_dir, log_filename)
    log_conf['handlers']['fileHandler']['filename'] = log_filepath

    # Debug表示を行うかどうか
    if verbose:
        log_conf['handlers']['fileHandler']['level'] = "DEBUG"

    config.dictConfig(log_conf)
    return getLogger(module)
