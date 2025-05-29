import copy
from pathlib import Path
import pytest
from apis.manage import Manage
from common.api import CaseDict
from common.data import DB_CONN, api_test_data
from settings import TEST_DATA_DIR

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
            case_list.append(new_case_dict)
    return case_list


@pytest.mark.parametrize("case_dict",get_api_path(Path(TEST_DATA_DIR,  'gateway_service', '接口测试用例_unauthorized.xlsx')))
def test_normal(no_perm_api: Manage, case_dict: CaseDict):
    no_perm_api.test(case_dict, eval(case_dict['处理数据'] or 'None'))

