import os
import string
from datetime import datetime, timedelta
from pathlib import Path
from random import sample

# 项目目录
BASE_DIR = Path(__file__).parent.resolve()
# 测试数据文件夹
TEST_DATA_DIR = Path(BASE_DIR, 'data')
# 认证信息文件夹
AUTH_DIR = Path(BASE_DIR, 'auth')
# 临时信息文件夹，存放运行脚本时产生的临时信息
TMP_DIR = Path(TEST_DATA_DIR, 'tmp')
# 接口session过期时间，单位是分
SESSION_TIMEOUT_M = 60
# 接口基础url地址
BASE_URL = os.getenv(
    'BASE_URL') or 'http://tjshopgateway.rancher.gd'

# ------自定义信息------
DRAW_YEAR = (datetime.now() + timedelta(days=8)).year
# 默认奖期号，奖期年后两位+107
DRAW_NO = f'{DRAW_YEAR%100:02d}107'
# 脚本运行用户，用于tmp_use表，保证同一时间只能有一个脚本在运行
RUNNING_USER = ''.join(sample(string.ascii_letters, 5))
# 默认验证码，用于模拟登录
CAPTCHA = '8888'
# 验证码图片保存路径
CAPTCHA_IMAGE_PATH = Path(TMP_DIR, 'captcha',  'captcha.png')

# 数据库信息，在无k8s环境下使用
# DB_HOST = os.getenv('DB_HOST') or '192.168.12.90'  # test
DB_HOST = os.getenv('DB_HOST') or '192.168.22.47'  # dev
DB_PORT = os.getenv('DB_PORT') or 3306
DB_USER = os.getenv('DB_USER') or 'tjshop'
# DB_PASSWORD = os.getenv('DB_PASSWORD') or 'tjshop888'  # test
DB_PASSWORD = os.getenv('DB_PASSWORD') or 'Tjshop888!'  # dev
DB_NAME = os.getenv('DB_NAME') or 'tjshop'
#  监控库信息，在无k8s环境下使用
# MONITOR_NAME = os.getenv('MONITOR_NAME') or 'tjshop'
# MONITOR_USER = os.getenv('MONITOR_USER') or 'tjshop'
# MONITOR_HOST = os.getenv('MONITOR_HOST') or '192.168.12.90'
# MONITOR_PORT = os.getenv('MONITOR_PORT') or 3306
# MONITOR_PASSWORD = os.getenv('MONITOR_PASSWORD') or 'tjshop888'

# 缓存库信息，在无k8s环境下使用
REALTIME_REDIS_NODE = os.getenv(
    'REALTIME_REDIS_NODE') or '192.168.24.213:30033'
REALTIME_REDIS_PASSWORD = os.getenv('REALTIME_REDIS_PASSWORD') or 'Vegas2.0'
REALTIME_REDIS_MASTER = os.getenv('REALTIME_REDIS_MASTER') or 'mymaster'
SP_LOCK_REDIS_NODE = os.getenv('SP_LOCK_REDIS_NODE') or '192.168.24.213:30033'
SP_LOCK_REDIS_PASSWORD = os.getenv('SP_LOCK_REDIS_PASSWORD') or 'Vegas2.0'
SP_LOCK_REDIS_MASTER = os.getenv('SP_LOCK_REDIS_MASTER') or 'mymaster'
