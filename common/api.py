import logging
from datetime import datetime
from pathlib import Path
from time import perf_counter
from typing import Generator, Literal

import allure
import httpx
import json_tools
import pytest
from wcwidth import wcswidth

from common.data import CaseDict, json_dumps
from settings import TEST_DATA_DIR

logging.getLogger("httpx").setLevel(logging.ERROR)
logger = logging.getLogger()
CONTENT_TYPE_DICT = {
    '.aac': 'audio/aac',
    '.avif': 'image/avif',
    '.avi': 'video/x-msvideo',
    '.bin': 'application/octet-stream',
    '.bmp': 'image/bmp',
    '.bz': 'application/x-bzip',
    '.bz2': 'application/x-bzip2',
    '.css': 'text/css',
    '.csv': 'text/csv',
    '.doc': 'application/msword',
    '.docx':
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    '.gz': 'application/gzip',
    '.gif': 'image/gif',
    '.html': 'text/html',
    '.htm,': 'text/html',
    '.ico': 'image/vnd.microsoft.icon',
    '.ics': 'text/calendar',
    '.jar': 'application/java-archive',
    '.jpg': 'image/jpeg',
    '.jpeg': 'image/jpeg',
    '.js': 'text/javascript',
    '.json': 'application/json',
    '.jsonld': 'application/ld+json',
    '.mp3': 'audio/mpeg',
    '.mp4': 'video/mp4',
    '.mpeg': 'video/mpeg',
    '.mpkg': 'application/vnd.apple.installer+xml',
    '.png': 'image/png',
    '.pdf': 'application/pdf',
    '.ppt': 'application/vnd.ms-powerpoint',
    '.pptx':
    'application/vnd.openxmlformats-officedocument.presentationml.presentation',
    '.rar': 'application/vnd.rar',
    '.rtf': 'application/rtf',
    '.sh': 'application/x-sh',
    '.svg': 'image/svg+xml',
    '.tar': 'application/x-tar',
    '.ts': 'video/mp2t',
    '.txt': 'text/plain',
    '.vsd': 'application/vnd.visio',
    '.wav': 'audio/wav',
    '.weba': 'audio/webm',
    '.webm': 'video/webm',
    '.webp': 'image/webp',
    '.xhtml': 'application/xhtml+xml',
    '.xls': 'application/vnd.ms-excel',
    '.xlsx':
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    '.xml': 'application/xml',
    '.xul': 'application/vnd.mozilla.xul+xml',
    '.zip': 'application/zip',
    '.7z': 'application/x-7z-compressed'
}

API_METHOD = Literal['PUT', 'GET', 'POST', 'PATCH', 'DELETE', 'UPLOAD']
CONTENT_TYPE_ENUM = Literal['application/json',
                            'application/x-www-form-urlencoded',
                            'multipart/form-data', 'text/plain']


class ApiError(Exception):
    """接口相关异常"""

    def __init__(self, code: int, message: str):
        self.code = code
        self.message = message
        super().__init__(f'接口调用失败，错误码：{code}，{message}')


class API:
    """api基类
    """
    # 接口的根url
    base_url = ''
    name = ''
    # 成功返回包含的结果，用于判断接口操作是否成功。
    success_response = {'code': 0}

    def __init__(self, name: str = '', base_url: str = None) -> None:
        """初始化

        Args:
            name (str): 接口名称
            base_url (str): 接口url
        """
        self.session = httpx.Client(timeout=300, verify=False)
        if name:
            self.name = name
        if base_url:
            self.base_url = base_url
        self.options = {}

    def set_options(self, **kwargs):
        """设置请求选项，如headers、cookies等，参考httpx.Client说明
        """
        self.options = kwargs or {}

    @allure.step('调用接口{route}')
    def request(self,
                route: str,
                method: API_METHOD,
                request_param: dict | str = {},
                expect_result: dict = None,
                generator=None,
                content_type: CONTENT_TYPE_ENUM = None,
                expect_http_code: int = None,
                validate_http_status_code: bool =True):
        """请求，并对返回结果进行验证

        Args:
            route (str): 接口相对于根url的路径，最终接口地址为self.base_url+route
            method (Literal['PUT', 'GET', 'POST', 'PATCH', 'DELETE', 'UPLOAD']): 接口请求方法，其中UPLOAD为上传文件，其他为HTTP请求方法
            request_param (dict, str): 请求参数，上传文件时，value为相对于TEST_DATA_DIR的相对路径，或者绝对路径
            expect_result (dict, optional): 预期结果. Defaults to None.如果有值，则进行验证。
            func (generator, optional): 生成器对象，yield前面为前置数据处理，后面为后置数据处理。此处开始执行yield后面的部分。 Defaults to None.
            content_type (str, optional): 表单请求时，请求的内容类型，不写时为application/json
            expect_http_code (int, optional): 预期的HTTP状态码，默认200
            validate_http_status_code (bool): 是否验证HTTP状态码，默认为True
        """
        content_type = content_type or 'application/json'
        expect_http_code = expect_http_code or 200
        method = method.upper()
        if method not in API_METHOD.__args__:
            raise ValueError(f"请求方式错误，只能为{API_METHOD.__args__}之一，实际为{method}")
        request_method = 'POST' if method == 'UPLOAD' else method
        request_dict = {}
        request_dict['url'] = self.base_url + route
        if request_param:
            match method:
                case 'GET':
                    request_dict['params'] = request_param
                case 'DELETE' | 'PATCH' | 'PUT' | 'POST':
                    if content_type != 'application/json':
                        request_dict['data'] = request_param
                    else:
                        request_dict['json'] = request_param
                case 'UPLOAD':
                    if isinstance(request_param, dict):
                        for key, value in request_param.items():
                            file = Path(TEST_DATA_DIR, value)
                            request_param[key] = str(value)
                            f = open(file, 'rb')
                            request_dict['files'] = {
                                key:
                                (file.name, f, CONTENT_TYPE_DICT[file.suffix])
                            }
                    else:
                        raise ValueError(
                            f"上传文件时，request_param必须为dict，实际为{type(request_param)}"
                        )
        try:
            self.call_info = {
                '接口URL': request_dict['url'],
                '请求方式': request_method,
                '请求参数': request_param
            }
            try:
                request_start = perf_counter()
                response: httpx.Response = self.session.request(
                    request_method, **request_dict, **self.options)
                logger.info(
                    f'{route}用时:{perf_counter()-request_start:.2f}秒, X-Request-ID:{response.headers.get("X-Request-ID")}'
                )
            finally:
                if method == 'UPLOAD':
                    f.close()
                    allure.attach.file(file,
                                       name=file.name,
                                       extension=file.suffix.removeprefix('.'))
            try:
                self.actual_result = response.json()
            except:
                self.actual_result = {'返回内容': response.text}
        finally:
            try:
                if isinstance(generator, Generator):
                    with allure.step("完成后数据处理"):
                        try:
                            generator.send(self.actual_result)
                        except StopIteration:
                            pass
            finally:
                if validate_http_status_code:
                    assert response.status_code == expect_http_code, f'{request_dict["url"]} 访问错误，返回码：{response.status_code}。'
                if expect_result:
                    self.verify_response(expect_result)
                else:
                    content_type = response.headers.get('content-type', '')
                    #  非图片类型响应进行success_response校验
                    if not content_type.startswith('image/'):
                        for k, v in self.success_response.items():
                            if self.actual_result.get(k) != v:
                                print(
                                    f"请求内容：{self.call_info}\n返回内容：{response.text}")
                                break
        return response

    def verify_response(self, expect_result: dict):
        """返回expect_result和self.actual_result比较结果

        Args:
            expect_result (dict): _预期信息
        """
        # 如果实际结果中存在token，需要剔除掉，因为token会变化
        data = self.actual_result.get('data')
        if isinstance(data, dict) and 'token' in data:
            data.pop('token')
        # 返回expect_result和self.actual_result比较结果
        difflist = json_tools.diff(expect_result, self.actual_result)
        if difflist != []:
            logger.info(f'接口返回：\n{json_dumps(self.actual_result)}')
            raise AssertionError(
                f'{json_dumps(self.call_info)}\n与预期差异：\n{json_dumps(difflist)}'
            )

    def login(self, username='', password=''):
        pass

    def logout(self):
        pass

    def test(self, case_dict: CaseDict, generator=None):
        """测试接口

        Args:
            case_dict (CaseDict): _用例字典，由common.data.api_test_data读取文件生成
            generator (Generator, optional): _生成器对象，yield前面为前置数据处理，后面为后置数据处理. Defaults to None.
        """
        print(
            f"\n测试内容: {case_dict['测试内容'].ljust(46-wcswidth(case_dict['测试内容']))} {datetime.now():%Y-%m-%d %H:%M:%S}"
        )
        # suites显示内容
        allure.dynamic.title(f"{case_dict['测试内容']}")
        # allure.dynamic.suite(f"接口:{case_dict['接口名']}-{case_dict['路径']}")
        allure.dynamic.sub_suite(f"接口:{case_dict['接口名']}-{case_dict['路径']}")


        # behaviors显示内容
        allure.dynamic.feature(f"{self.name} {self.base_url}")
        allure.dynamic.story(f"{case_dict['接口名']}-{case_dict['路径']}")
        if case_dict.get('描述'):
            allure.dynamic.description(case_dict['描述'])
        if skip := case_dict.get('跳过'):
            pytest.skip(skip)
        if isinstance(generator, Generator):
            with allure.step("数据预处理"):
                next(generator)
        else:
            generator = None
        try:
            for key, value in case_dict.items():
                if isinstance(value, (dict, list)):
                    value = json_dumps(value)
                allure.dynamic.parameter(key, value)
            if case_dict.get('是否登录') == '是':
                if case_dict.get('用户名') and case_dict.get('密码'):
                    self.login(case_dict.get('用户名'), case_dict.get('密码'))
                else:
                    self.login()
            elif case_dict.get('是否登录') == '否':
                self.logout()
        except Exception as e:
            if isinstance(generator, Generator):
                next(generator, None)
            raise e
        return self.request(case_dict['路径'],
                            case_dict['请求方式'],
                            case_dict['参数'],
                            case_dict.get('预期'),
                            generator,
                            case_dict.get('内容类型'),
                            expect_http_code=case_dict.get(
                                'HTTP_STATUS_CODE', 200),
                            validate_http_status_code=case_dict.get('VALIDATE_HTTP_STATUS_CODE', True))
