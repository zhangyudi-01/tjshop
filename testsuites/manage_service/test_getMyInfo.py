from pathlib import Path
import pytest
from apis.manage import Manage
from common.data import CaseDict, DB_CONN, api_test_data
from settings import TEST_DATA_DIR


def get_myinfo(case_dict:  CaseDict):
    sql_result = DB_CONN.get_asdict("""
    SELECT
    tu.user_name as userName,
    tu.login_name as loginName,
    AES_DECRYPT(FROM_BASE64(tu.mobile),'MvQi-s`7jX-d9ndf') as mobile,
    tu.person_sign as personSign,
    GROUP_CONCAT(DISTINCT dj.job_name)  as jobName,
    GROUP_CONCAT(DISTINCT d.department_name) as departmentName,
    GROUP_CONCAT(DISTINCT r.role_name) as roleName
    from
    toto_user tu
    left join user_job uj on uj.user_id = tu.user_id 
    left join department_job dj on uj.job_id = dj.job_id 
    left join department d on d.department_id = dj.department_id 
    left join user_role ur on ur.user_id = tu.user_id 
    left join `role` r on r.role_id = ur.role_id  
    where tu.user_id = 68
    group by tu.user_id 
    """)
    case_dict["预期"]["data"]["userName"] = sql_result[0]["userName"]
    case_dict["预期"]["data"]["loginName"] = sql_result[0]["loginName"]
    case_dict["预期"]["data"]["mobile"] = sql_result[0]["mobile"].decode('utf-8')  # 新增解码处理
    case_dict["预期"]["data"]["personSign"] = sql_result[0]["personSign"]
    case_dict["预期"]["data"]["jobName"] = sql_result[0]["jobName"]
    case_dict["预期"]["data"]["departmentName"] = sql_result[0]["departmentName"]
    case_dict["预期"]["data"]["roleName"] = sql_result[0]["roleName"]

    actual_result = yield

@pytest.mark.parametrize("case_dict", api_test_data(Path(TEST_DATA_DIR,'manage_service','接口测试用例_查询个人信息.xlsx')))
def test_getMyInfo(manage:Manage,case_dict:CaseDict):
    manage.test(case_dict, eval(case_dict['处理数据'] or 'None'))
