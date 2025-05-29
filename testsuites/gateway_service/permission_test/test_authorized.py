import copy
from pathlib import Path

import allure
import pytest
from apis.manage import Manage
from common.api import CaseDict
from common.data import DB_CONN, api_test_data
from settings import TEST_DATA_DIR

# 定义预期的越权错误响应
EXPECTED_UNAUTHORIZED_RESPONSE = {
    "code": -105,
    "msg": "无权访问该接口！"
}

def check_unauthorized_access(case_dict: CaseDict):
    """
    越权测试专用的处理数据生成器。
    前置处理 (yield 之前): 可用于测试环境准备，例如确保用户有权访问或切换用户等。
    后置处理 (yield 之后): 接收实际结果 actual_result，并执行越权测试的断言。
    """

    # request 方法会把 actual_result 发送回这里
    actual_result = yield


    #  有所有权限的用户访问接口的实际返回结果不应该等于EXPECTED_UNAUTHORIZED_RESPONSE
    assert actual_result != EXPECTED_UNAUTHORIZED_RESPONSE, \
        f"越权测试失败：预期有权限访问该接口，但实际返回越权错误。接口: {case_dict['路径']}, 返回: {actual_result}"


def get_api_path(path: Path) -> list[CaseDict]:
    """返回接口测试用例。
    Args:
        path(Path): 用例文件地址，模板见/data/template/接口测试用例.xlsx
    Returns:
        list: 一个CaseDict列表，每个元素为一条用例.
    """
    case_dict = api_test_data(path)
    sql_result = DB_CONN.get_asdict("""
    SELECT
        '越权测试' as api_name,
        (select p2.perm_name from permission p2 where p2.perm_id=p.up_perm_id) as father_perm_name,
        p.perm_name,
        pif.interface_name ,
        CASE 
            p.perm_type 
            when 0 then 'app'
            when 1 then '管理平台'
        END as 权限类别
    from
        permission_interface pif
    left join permission p on
        p.perm_id = pif.permission_id""")
    case_list = []
    for interface_info  in sql_result:
        for base_template  in case_dict:
            new_case_dict = copy.deepcopy(base_template)
            new_case_dict['路径'] = interface_info.get('interface_name')
            new_case_dict['接口名'] = interface_info.get('api_name')
            new_case_dict['描述'] = (
                f"{interface_info['权限类别']} -> "
                f"{interface_info['father_perm_name']} -> "
                f"{interface_info['perm_name']} -> "
                f"{interface_info['interface_name']}"
            )
            new_case_dict['VALIDATE_HTTP_STATUS_CODE'] = False
            case_list.append(new_case_dict)
    return case_list


@pytest.mark.parametrize("case_dict",get_api_path(Path(TEST_DATA_DIR,  'gateway_service', '接口测试用例_authorized.xlsx')))
def test_normal(all_perm_api: Manage, case_dict: CaseDict):
    all_perm_api.test(case_dict, eval(case_dict['处理数据'] or 'None'))

