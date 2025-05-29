from datetime import datetime

import allure
import pytest

# from common.data import clear_table_bak, clear_table_snapshot


@pytest.fixture(scope='session')
def debug(request):
    return request.config.getoption("--debug")


@pytest.fixture(scope='function', autouse=True)
def print_time():
    allure.dynamic.parameter('执行时间', f'{datetime.now():%Y-%m-%d %H:%M:%S}')


# @pytest.fixture(scope='session', autouse=True)
# def init_db():
#     clear_table_bak('__all__')
#     clear_table_snapshot()
#     yield
#     # 后置操作
