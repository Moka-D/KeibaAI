#!/usr/bin/python
"""モデルパラメータ設定ファイル
"""

model_params = {
    '芝': {
        'objective': 'binary',
        'metric': 'auc',
        'feature_pre_filter': False,
        'lambda_l1': 3.9527246742486897,
        'lambda_l2': 1.5543552961698833e-07,
        'num_leaves': 111,
        'feature_fraction': 0.4,
        'bagging_fraction': 0.4159527509593907,
        'bagging_freq': 2,
        'min_child_samples': 25,
        'num_iterations': 1000,
        'early_stopping_round': 20,
        'categorical_column': [3, 4, 44, 45, 46, 47, 48, 49]
    },
    'ダ': {
        'objective': 'binary',
        'metric': 'auc',
        'feature_pre_filter': False,
        'lambda_l1': 9.08546738937348,
        'lambda_l2': 2.5108458144726414e-08,
        'num_leaves': 5,
        'feature_fraction': 0.4,
        'bagging_fraction': 1.0,
        'bagging_freq': 0,
        'min_child_samples': 20,
        'num_iterations': 1000,
        'early_stopping_round': 20,
        'categorical_column': [3, 4, 44, 45, 46, 47, 48, 49]
    },
    '障': {
        'objective': 'binary',
        'metric': 'auc',
        'feature_pre_filter': False,
        'lambda_l1': 5.924465735059502e-05,
        'lambda_l2': 0.0004516184119572766,
        'num_leaves': 31,
        'feature_fraction': 0.6839999999999999,
        'bagging_fraction': 0.8056394440107637,
        'bagging_freq': 3,
        'min_child_samples': 20,
        'num_iterations': 1000,
        'early_stopping_round': 20,
        'categorical_column': [3, 4, 44, 45, 46, 47, 48, 49]
    }
}
