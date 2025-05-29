from common.api import API
from pathlib import Path
from settings import BASE_URL, BASE_DIR, CAPTCHA_IMAGE_PATH
import logging
from typing import Any, Dict
from common.regex_utils import RegexHelper
from common.captcha import CaptchaHandler

logger = logging.getLogger(__name__)

class Manage(API):
    """管理端接口"""
    base_url = BASE_URL
    name = "管理端接口"

    def get_captcha(self) -> str:
        """获取验证码"""
        try:
            tmp_dir = Path(BASE_DIR) / 'data/tmp/captcha'
            filename = tmp_dir / 'captcha.png'

            response = self.request(
                '/admin/GetVerifyCodeImg',
                method='POST',
                request_param={}
            )

            if response.status_code == 200:
                with open(filename, 'wb') as f:
                    f.write(response.content)

                uuid = RegexHelper.extract_uuid(
                    response.headers.get('content-disposition', '')
                )
                return uuid
            else:
                logger.error(f'获取验证码失败，状态码：{response.status_code}')
        except Exception as e:
            logger.error(f'验证码获取异常：{str(e)}')

    def login(
        self, 
        username="17862721193",
        password="Tjtc@111",
        captcha='',
        captcha_id='',
        ScreenType='pc',
    ) -> Dict[str, Any]:
        """
        用户登录方法

        :param username: 登录账号
        :param password: 登录密码
        :param captcha: 验证码
        :param captcha_id: 验证码ID
        :param ScreenType: 设备类型 'pc'(默认) 或 None(移动端)
        """

        captcha_id = captcha_id or self.get_captcha()
        captcha = captcha or CaptchaHandler().parse_image_captcha(CAPTCHA_IMAGE_PATH)

        response = self.request(
            '/admin/login',
            method='POST',
            request_param={
                'loginAcct': username,
                'passwd': password,
                'verifyCode': captcha,
                'verifyCode_Id': captcha_id
            },
            content_type='application/json'
        ).json()

        assert response['code'] == 0, f'登录失败，用户名{username}，密码{password}，返回内容：{response}'

        token = response['data'].get('token')  # 使用get方法避免KeyError
        if token:
            headers = {'token': token}
            # 修改条件判断逻辑，当ScreenType为None时不添加header
            if ScreenType and str(ScreenType).lower() == 'pc':
                headers.update({
                    'ScreenType': 'pc',
                    'route': '/home'
                })
            self.set_options(headers=headers)
        assert token, '登录成功，但返回的token为空'
        return response

# {"msg":"验证码错误，请重新输入!","code":-1}