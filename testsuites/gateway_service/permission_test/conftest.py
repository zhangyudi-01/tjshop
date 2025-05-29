import pytest
from apis.manage import Manage
from common.captcha import CaptchaHandler
from settings import CAPTCHA_IMAGE_PATH

@pytest.fixture(scope="module")
def no_perm_api():
    """提供一个已登录的无权限用户API接口实例。"""
    manage = Manage()
    manage.login('17862721193', 'Tjtc@111',ScreenType='pc')
    return manage

@pytest.fixture(scope="module")
def all_perm_api():
    """提供一个已登录的全权限用户API接口实例。"""
    manage = Manage()
    manage.login('17862721193', 'Tjtc@111',ScreenType='pc')
    return manage

@pytest.fixture(scope="module")
def pc_perm_api():
    """提供一个已登录的PC端权限用户API接口实例。"""
    manage = Manage()
    manage.login(ScreenType='pc')
    return manage

@pytest.fixture(scope="module")
def app_perm_api():
    """提供一个已登录的App端权限用户API接口实例。"""
    manage = Manage()
    manage.login(ScreenType=None)
    return manage