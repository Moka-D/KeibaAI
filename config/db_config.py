#!/usr/bin/python
"""DB設定ファイル
"""

db_config = {
    'read': {
        'host': 'moka-server',
        'port': '5432',
        'name': 'keiba',
        'user': 'predict',
        'pass': 'predict'
    },
    'write': {
        'host': 'db-main',
        'port': '5432',
        'name': 'keiba',
        'user': 'scraping',
        'pass': 'scraping'
    },
    'local': {
        'host': 'localhost',
        'port': '5432',
        'name': 'test',
        'user': 'test',
        'pass': 'testuser'
    }
}
