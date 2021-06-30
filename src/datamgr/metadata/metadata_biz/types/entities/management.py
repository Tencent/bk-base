# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
License for BK-BASE 蓝鲸基础平台:
--------------------------------------------------------------------
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the Software without restriction, including without limitation
the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all copies or substantial
portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

from datetime import datetime

import attr
from attr.validators import in_

from metadata.type_system.basic_type import Entity, as_metadata


@as_metadata
@attr.s
class Staff(Entity):
    id = attr.ib(type=int)
    wei_xin_user_name = attr.ib(type=str)
    work_qq_number = attr.ib(type=str)
    status_name = attr.ib(type=str)
    status_id = attr.ib(type=int)
    type_id = attr.ib(type=int)
    post_name = attr.ib(type=str)
    work_dept_id = attr.ib(type=int)
    official_id = attr.ib(type=int)
    type_name = attr.ib(type=str)
    work_dept_name = attr.ib(type=str)
    gender = attr.ib(type=str)
    ex_properties = attr.ib(type=str)
    official_name = attr.ib(type=str)
    rtx = attr.ib(type=str)
    login_name = attr.ib(type=str, metadata={'identifier': True, 'dgraph': {'index': ['exact', 'trigram']}})
    department_name = attr.ib(type=str)
    enabled = attr.ib(type=bool)
    group_name = attr.ib(type=str)
    department_id = attr.ib(type=int)
    english_name = attr.ib(type=str)
    qq = attr.ib(type=str)
    chinese_name = attr.ib(type=str)
    birthday = attr.ib(type=str)
    full_name = attr.ib(type=str)
    post_id = attr.ib(type=int)
    mobile_phone_number = attr.ib(type=str)
    group_id = attr.ib(type=int)
    branch_phone_number = attr.ib(type=str)
    synced_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class ProjectInfo(Entity):
    project_id = attr.ib(type=int, metadata={'identifier': True, 'dgraph': {'index': ['int']}})
    project_name = attr.ib(type=str, metadata={'dgraph': {'index': ['exact', 'trigram']}})
    bk_app_code = attr.ib(type=str)
    description = attr.ib(type=str)
    created_by = attr.ib(type=str)
    updated_by = attr.ib(type=str)
    deleted_by = attr.ib(type=str, default='')
    active = attr.ib(type=bool, default=True)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    deleted_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class Biz(Entity):
    __abstract__ = True
    __definable__ = True


@as_metadata
@attr.s
class BKBiz(Biz):
    id = attr.ib(type=int, metadata={'identifier': True, 'dgraph': {'index': ['int']}})
    name = attr.ib(type=str, metadata={'dgraph': {'index': ['trigram']}})
    maintainer = attr.ib(type=str)
    developer = attr.ib(type=str)
    planner = attr.ib(type=str)
    biz_type = attr.ib(type=str, validator=in_('v1,v3'))
    biz = attr.ib(type=Biz)
    app_type = attr.ib(type=str, default='')
    game_type = attr.ib(type=str, default='')
    source = attr.ib(type=str, default='')
    operate_state = attr.ib(type=str, default='')
    bip_grade = attr.ib(type=str, default='')


@as_metadata
@attr.s
class BizV1(Biz):
    ApplicationID = attr.ib(type=int, default='', metadata={'identifier': True, 'dgraph': {'index': ['int']}})
    PmpSafeMan = attr.ib(type=str, default='')
    PmpType = attr.ib(type=str, default='')
    OperState = attr.ib(type=str, default='')
    AppType = attr.ib(type=str, default='')
    SourceID = attr.ib(type=str, default='')
    AppDevMan = attr.ib(type=str, default='')
    BipGradeId = attr.ib(type=str, default='')
    PmpSpecialTarget = attr.ib(type=str, default='')
    VipdlID = attr.ib(type=str, default='')
    PmpSA = attr.ib(type=str, default='')
    VaskeyID = attr.ib(type=str, default='')
    BipAppName = attr.ib(type=str, default='')
    MobileQQAppID = attr.ib(type=str, default='')
    OperationPlanning = attr.ib(type=str, default='')
    WechatAppID = attr.ib(type=str, default='')
    BetaTime = attr.ib(type=str, default='')
    TcmID = attr.ib(type=str, default='')
    PmpDBABackup = attr.ib(type=str, default='')
    BipDevUnit = attr.ib(type=str, default='')
    BipGR3TDRStart = attr.ib(type=str, default='')
    PmpCmMan = attr.ib(type=str, default='')
    BipGR2TDREnd = attr.ib(type=str, default='')
    PmpState = attr.ib(type=str, default='')
    Abbreviation = attr.ib(type=str, default='')
    BipGR1 = attr.ib(type=str, default='')
    BipGR2 = attr.ib(type=str, default='')
    BipGR3 = attr.ib(type=str, default='')
    BipGR4 = attr.ib(type=str, default='')
    PmpID = attr.ib(type=str, default='')
    BipGR6 = attr.ib(type=str, default='')
    PmpLogo = attr.ib(type=str, default='')
    Operator = attr.ib(type=str, default='')
    GameTypeName = attr.ib(type=str, default='')
    BipGR4TDRStart = attr.ib(type=str, default='')
    BipGR3TDREnd = attr.ib(type=str, default='')
    ApplicationName = attr.ib(type=str, default='')
    DisplayName = attr.ib(type=str, default='')
    AppDevBackup = attr.ib(type=str, default='')
    PmpOpeManMajor = attr.ib(type=str, default='')
    PmpStandard = attr.ib(type=str, default='')
    BipGradeName = attr.ib(type=str, default='')
    AppUserManual = attr.ib(type=str, default='')
    Maintainers = attr.ib(type=str, default='')
    AppSummary = attr.ib(type=str, default='')
    BipGR5 = attr.ib(type=str, default='')
    BipState = attr.ib(type=str, default='')
    PmpOpePM = attr.ib(type=str, default='')
    PmpOssMan = attr.ib(type=str, default='')
    PmpServicePM = attr.ib(type=str, default='')
    BipManager = attr.ib(type=str, default='')
    PmpSensCol = attr.ib(type=str, default='')
    PmpGroupUser = attr.ib(type=str, default='')
    PmpCmReqMan = attr.ib(type=str, default='')
    ProductId = attr.ib(type=str, default='')
    AppDevTeam = attr.ib(type=str, default='')
    AppDirector = attr.ib(type=str, default='')
    ProductName = attr.ib(type=str, default='')
    OperStateName = attr.ib(type=str, default='')
    PmpNoDelFileTime = attr.ib(type=str, default='')
    Owner = attr.ib(type=str, default='')
    BipID = attr.ib(type=str, default='')
    BipResource = attr.ib(type=str, default='')
    BipGR2TDRStart = attr.ib(type=str, default='')
    PmpProductMan = attr.ib(type=str, default='')
    PmpGroup = attr.ib(type=str, default='')
    AppImportantLevel = attr.ib(type=str, default='')
    AppForumUrl = attr.ib(type=str, default='')
    PmpIdipMan = attr.ib(type=str, default='')
    PmpOpeManager = attr.ib(type=str, default='')
    Star = attr.ib(type=str, default='')
    SourceName = attr.ib(type=str, default='')
    PmpBetaTime = attr.ib(type=str, default='')
    AppTypeName = attr.ib(type=str, default='')
    CmdbID = attr.ib(type=str, default='')
    TclsID = attr.ib(type=str, default='')
    BipAppCode = attr.ib(type=str, default='')
    PmpPtCapacity = attr.ib(type=str, default='')
    VisitorAppID = attr.ib(type=str, default='')
    PmpOpeExpert = attr.ib(type=str, default='')
    PmpTestTM = attr.ib(type=str, default='')
    DeptNameID = attr.ib(type=str, default='')
    AppDoc = attr.ib(type=str, default='')
    PmpPortalMan = attr.ib(type=str, default='')
    BipOB = attr.ib(type=str, default='')
    PmpOpeManBackup = attr.ib(type=str, default='')
    AppUrl = attr.ib(type=str, default='')
    TestResource = attr.ib(type=str, default='')
    PmpComPlot = attr.ib(type=str, default='')
    IsBip = attr.ib(type=str, default='')
    PmpTlogMan = attr.ib(type=str, default='')
    PmpQA = attr.ib(type=str, default='')
    DeptName = attr.ib(type=str, default='')
    PmpQC = attr.ib(type=str, default='')
    PmpTechBetaTime = attr.ib(type=str, default='')
    PmpDBAMajor = attr.ib(type=str, default='')
    OperationTime = attr.ib(type=str, default='')
    BipDept = attr.ib(type=str, default='')
    AlamRvcMan = attr.ib(type=str, default='')
    IdipID = attr.ib(type=str, default='')
    GroupNameID = attr.ib(type=str, default='')
    AppImportantLevelName = attr.ib(type=str, default='')
    PmpCloseBetaTime = attr.ib(type=str, default='')
    GroupName = attr.ib(type=str, default='')
    PmpOpeDevMan = attr.ib(type=str, default='')
    AppGameTypeID = attr.ib(type=str, default='')
    AppOpeManual = attr.ib(type=str, default='')
    BipGR4TDREnd = attr.ib(type=str, default='')


@as_metadata
@attr.s
class BizV3(Biz):
    bk_biz_id = attr.ib(type=int, metadata={'identifier': True, 'dgraph': {'index': ['int']}})
    last_time = attr.ib(type=str, default=None)
    language = attr.ib(type=str, default=None)
    life_cycle = attr.ib(type=str, default=None)
    default = attr.ib(type=int, default=None)
    bk_biz_maintainer = attr.ib(type=str, default=None)
    bk_biz_tester = attr.ib(type=str, default=None)
    is_sync_topo = attr.ib(type=bool, default=None)
    bk_biz_developer = attr.ib(type=str, default=None)
    create_time = attr.ib(type=str, default=None)
    bk_biz_productor = attr.ib(type=str, default=None)
    bk_oper_plan = attr.ib(type=str, default=None)
    business_dept_name = attr.ib(type=str, default=None)
    bk_supplier_account = attr.ib(type=str, default=None)
    operator = attr.ib(type=str, default=None)
    time_zone = attr.ib(type=str, default=None)
    is_sync_host_app = attr.ib(type=bool, default=None)
    bk_biz_name = attr.ib(type=str, default=None)
    business_dept_id = attr.ib(type=int, default=None)
    bk_supplier_id = attr.ib(type=int, default=None)
    bs1_name = attr.ib(type=str, default=None)
    bs1_name_id = attr.ib(type=int, default=None)
    bs3_name = attr.ib(type=str, default=None)
    bs3_name_id = attr.ib(type=int, default=None)
    bs2_name = attr.ib(type=str, default=None)
    bs2_name_id = attr.ib(type=int, default=None)
    extra_ = attr.ib(type=str, default=None)


@as_metadata
@attr.s
class BizCloud(BizV1):
    pass
