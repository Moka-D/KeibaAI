#!/usr/bin/python
# -*- coding: utf-8 -*-

class InvalidArgument(Exception):
    """不正引数例外クラス"""
    pass


def get_environment():
    try:
        env = get_ipython().__class__.__name__
        if env == 'ZMQInteractiveShell':
            return 'Jupyter'
        elif env == 'TerminalInteractiveShell':
            return 'IPython'
        else:
            return 'OtherShell'
    except NameError:
        return 'Interpreter'
