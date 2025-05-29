from pathlib import Path
import pytest
from apis.manage import Manage
from common.api import CaseDict
from common.data import DB_CONN, api_test_data
from settings import TEST_DATA_DIR


def get_menu(case_dict:  CaseDict):
    """根据用户角色和权限获取菜单信息。"""
    sql_result = DB_CONN.get_aslist("""
        SELECT
        rp.perm_id as permId,
        p.perm_name as permName,
        p.perm_type as permType,
        p.up_perm_id as upPermId,
        DATEDIFF(NOW(), tu.pw_last_update_time) as diffdays,
        case
            when DATEDIFF(NOW(), tu.pw_last_update_time) > (select tc.config_value from toto_config tc WHERE tc.config_key = 'pw_update_limit') then 1
            else 0
        end as ifModifyPWD,
        case
            when DATEDIFF(NOW(), tu.pw_last_update_time) > (select tc.config_value from toto_config tc WHERE tc.config_key = 'pw_update_limit') then 'true'
            else 'false'
        end as pwmodify
    from
        user_role ur
    left join role_perm rp on rp.role_id = ur.role_id 
    left join permission p on p.perm_id = rp.perm_id 
    left join toto_user tu on tu.user_id = ur.user_id 
    where tu.mobile = TO_BASE64(AES_ENCRYPT('17862721193', 'MvQi-s`7jX-d9ndf'))""")
    # 提取并排序权限标识，构建预期数据
    perms = []
    for row in sql_result:
        perm = {
            "permId": row[0],
            "permName": row[1],
            "permType": row[2],
            "upPermId": row[3]
        }
        perms.append(perm)

    # 设置用例预期输出结构
    if 'data' in case_dict["预期"]:
        case_dict['预期']['data']['menu'] = perms
        case_dict['预期']['data']['diffdays'] = sql_result[0][4]
        case_dict['预期']['data']['ifModifyPWD'] = sql_result[0][5]
        case_dict['预期']['data']['pwmodify'] = sql_result[0][6]


    # 挂起生成器，返回接口调用结果
    actual_result = yield  # 此处实际调用被测接口

    # 标准化处理接口返回数据
    # actual_result['data']['menu'].sort()  # 权限列表排序以便比较

    # 预留位置：后续应添加数据验证逻辑
    # 设计要点：需还原预处理修改，验证数据库持久化数据
    pass

@pytest.mark.parametrize("case_dict", api_test_data(Path(TEST_DATA_DIR,'manage_service','接口测试用例_login.xlsx')))
def test_normal(manage:Manage,case_dict:CaseDict, captcha_handler):
    captcha, captcha_id = captcha_handler
    # 如果参数中未指定验证码，则使用提前获取的captcha
    if not case_dict['参数']['verifyCode']:
        case_dict['参数']['verifyCode'] = captcha
    case_dict['参数']['verifyCode_Id'] = captcha_id
    # 使用提前获取的captcha_id参数
    manage.test(case_dict, eval(case_dict['处理数据'] or 'None'))
    