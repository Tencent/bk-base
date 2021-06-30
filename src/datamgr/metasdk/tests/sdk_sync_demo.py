#! -*- coding=utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""

from tests.conftest import *

Base = declarative_base()


class SampleResultTable(Base):
    __tablename__ = "sample_result_table"

    id = Column(Integer, primary_key=True)
    result_table_id = Column(String(255), nullable=False)
    sample_set_id = Column(Integer, nullable=False)
    fields_mapping = Column(Text, comment="字段映射信息,json结构")
    bk_biz_id = Column(Integer, nullable=False, comment="样本集业务")
    created_by = Column(String(50), nullable=False, comment="创建者")
    created_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"), comment="创建时间")
    updated_by = Column(String(50), nullable=False, comment="更新者")
    updated_at = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"))


# 业务数据库
engine = create_engine("sqlite:///:memory:", echo=True)

Base.metadata.create_all(engine)
session_factory = sessionmaker()
session_ins = session_factory(bind=engine)
session_ins_autocommit = session_factory(bind=engine, autocommit=True)

# early_patch
RPCMixIn.bridge_sync = batch_bridge_sync
MetaSync.db_log_prepare = batch_db_log_prepare

# sync_hook客户端配置
settings = DEFAULT_SETTINGS.copy()


# 实例化meta_client类
metadata_client = metadata_client()
# 注册监听的表名
metadata_client.register_sync_model([SampleResultTable.__tablename__])
# 开启sync_hook开关
metadata_client.enable_meta_sync_hook()


@meta_sync(session_ins, "instance")
def run_demo():
    ed_user = SampleResultTable(
        result_table_id="123",
        sample_set_id=123,
        fields_mapping="{a:1, b:2}",
        bk_biz_id=12,
        created_by="admin",
        updated_by="admin",
    )
    session_ins.add(ed_user)
    new_user = SampleResultTable(
        result_table_id="124",
        sample_set_id=124,
        fields_mapping="{a:1, b:2, c:3}",
        bk_biz_id=22,
        created_by="admin",
        updated_by="admin",
    )
    session_ins.add(new_user)
    session_ins.commit()
    ed_user.sample_set_id = 112233
    ed_user.bk_biz_id = 112233
    new_user_modify = SampleResultTable(id=2, sample_set_id=112244)
    session_ins.merge(new_user_modify)

    # session_ins.delete(new_user)

    # 可有可无，autocommit=False时，sync_hook最后会自动提交一次
    session_ins.commit()
    for instance in session_ins.query(SampleResultTable).all():
        print("[db state after run_demo]:")
        print(instance.__dict__)


@meta_sync(session_ins_autocommit, "instance")
def run_demo_autocommit():
    # 保证 begin 和 commit 成对
    session_ins_autocommit.begin()
    ed_user = SampleResultTable(
        result_table_id="125",
        sample_set_id=125,
        fields_mapping="{a:1, b:2}",
        bk_biz_id=12,
        created_by="admin",
        updated_by="admin",
    )
    session_ins_autocommit.add(ed_user)
    new_rt = SampleResultTable(
        result_table_id="126",
        sample_set_id=126,
        fields_mapping="{a:1, b:2, c:3}",
        bk_biz_id=22,
        created_by="admin",
        updated_by="admin",
    )
    session_ins_autocommit.add(new_rt)
    session_ins_autocommit.commit()
    session_ins_autocommit.begin()
    ed_user.sample_set_id = 1125
    ed_user.bk_biz_id = 1125
    session_ins_autocommit.commit()

    for instance in session_ins_autocommit.query(SampleResultTable).all():
        print("[db state after run_demo_autocommit]:")
        print(instance.__dict__)


@meta_sync(session_factory, "factory", bind=engine)
def run_demo_factory():
    ed_user = SampleResultTable(
        result_table_id="127",
        sample_set_id=127,
        fields_mapping="{a:1, b:2}",
        bk_biz_id=12,
        created_by="admin",
        updated_by="admin",
    )
    session_ins.add(ed_user)
    new_user = SampleResultTable(
        result_table_id="128",
        sample_set_id=128,
        fields_mapping="{a:1, b:2, c:3}",
        bk_biz_id=22,
        created_by="admin",
        updated_by="admin",
    )
    session_ins.add(new_user)
    session_ins.commit()
    ed_user.sample_set_id = 1127
    ed_user.bk_biz_id = 1127

    session_ins.delete(new_user)

    # 可有可无，autocommit=False时，sync_hook最后会自动提交一次
    session_ins.commit()

    for instance in session_ins.query(SampleResultTable).all():
        print("[db state after run_demo]:")
        print(instance.__dict__)


@meta_sync(session_factory, "factory", bind=engine, autocommit=True)
def run_demo_factory_autocommit():
    # 保证 begin 和 commit 成对
    session_ins_autocommit.begin()
    ed_user = SampleResultTable(
        result_table_id="129",
        sample_set_id=129,
        fields_mapping="{a:1, b:2}",
        bk_biz_id=12,
        created_by="admin",
        updated_by="admin",
    )
    session_ins_autocommit.add(ed_user)
    new_rt = SampleResultTable(
        result_table_id="130",
        sample_set_id=130,
        fields_mapping="{a:1, b:2, c:3}",
        bk_biz_id=22,
        created_by="admin",
        updated_by="admin",
    )
    session_ins_autocommit.add(new_rt)
    session_ins_autocommit.commit()

    session_ins_autocommit.begin()
    ed_user.sample_set_id = 1129
    ed_user.bk_biz_id = 1129
    session_ins_autocommit.commit()

    for instance in session_ins_autocommit.query(SampleResultTable).all():
        print("[db state after run_demo]:")
        print(instance.__dict__)
