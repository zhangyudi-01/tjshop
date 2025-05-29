import pytest
from apis.manage import Manage
from common.captcha import CaptchaHandler
from settings import CAPTCHA_IMAGE_PATH

@pytest.fixture(scope="session")
def manage():
    return Manage()

@pytest.fixture(scope="function")
def captcha_handler(manage):
    """
    验证码处理夹具（函数级）

    该夹具用于在执行每条测试用例期间处理验证码的获取与解析，通过yield返回验证码相关数据，
    保持会话期间验证码处理状态

    :param manage: pytest夹具依赖，用于获取管理对象以访问系统接口
    :return: 包含验证码文本和验证码ID的元组 (captcha: str, captcha_id: str)
             captcha: 通过图像解析得到的验证码文本
             captcha_id: 从管理系统获取的验证码唯一标识

    处理流程：
    1. 初始化验证码处理器
    2. 从管理系统获取验证码ID
    3. 解析指定路径的验证码图片获取文本
    """
    captcha_handler = CaptchaHandler()
    captcha_id = manage.get_captcha()
    captcha = captcha_handler.parse_image_captcha(CAPTCHA_IMAGE_PATH)
    
    yield captcha, captcha_id

