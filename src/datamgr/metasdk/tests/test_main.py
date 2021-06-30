# coding=utf-8
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""

import json
import sys

from tests.conftest import *

Base = declarative_base()


@as_local_metadata
class ResultTable(LocalMetaData):
    bk_biz_id = attr.ib(type=int, metadata={"dgraph": {"count": True, "index": ["int"]}})
    project_id = attr.ib(type=int, metadata={"dgraph": {"count": True, "index": ["int"]}})
    result_table_id = attr.ib(
        type=str, metadata={"identifier": True, "dgraph": {"count": True, "index": ["exact", "trigram"]}}
    )
    result_table_name = attr.ib(
        type=str,
    )
    result_table_name_alias = attr.ib(type=str, metadata={"dgraph": {"count": True, "index": ["fulltext", "trigram"]}})
    processing_type = attr.ib(type=str)
    created_by = attr.ib(type=str, metadata={"dgraph": {"count": True, "index": ["exact", "trigram"]}})
    updated_by = attr.ib(type=str, metadata={"dgraph": {"count": True, "index": ["exact", "trigram"]}})
    description = attr.ib(type=str, metadata={"dgraph": {"count": True, "index": ["fulltext", "trigram"]}})
    result_table_type = attr.ib(type=str, default=None)
    generate_type = attr.ib(
        type=str,
        default="user",
        metadata={
            "dgraph": {
                "count": True,
                "index": [
                    "exact",
                ],
            }
        },
    )
    sensitivity = attr.ib(type=str, default="public")
    count_freq = attr.ib(type=int, default=0)
    count_freq_unit = attr.ib(type=str, default="s")
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    is_managed = attr.ib(type=bool, default=True)
    platform = attr.ib(type=str, default="bkdata")
    data_category = attr.ib(type=str, default="")


@as_local_metadata
class LifeCycle(LocalMetaData):
    id = attr.ib(type=str, metadata={"identifier": True, "dgraph": {"count": True, "index": ["exact"]}})
    target_id = attr.ib(
        type=str,
    )
    target_type = attr.ib(type=str)
    range_id = attr.ib(type=str)
    heat_id = attr.ib(type=str)


@as_local_metadata
class Range(LocalMetaData):
    id = attr.ib(type=str, metadata={"identifier": True, "dgraph": {"count": True, "index": ["exact"]}})
    project_count = attr.ib(type=int)
    biz_count = attr.ib(type=int)
    depth = attr.ib(type=int)
    node_count_list = attr.ib(type=str)
    node_count = attr.ib(type=int)
    weighted_node_count = attr.ib(type=float)
    app_code_count = attr.ib(type=int)
    range_score = attr.ib(type=float)
    normalized_range_score = attr.ib(type=float, metadata={"dgraph": {"count": True, "index": ["float"]}})


class SampleResultTable(Base):
    __tablename__ = "sample_result_table"

    id = Column(Integer, primary_key=True)
    result_table_id = Column(String(255), nullable=False)
    sample_set_id = Column(Integer, nullable=False)
    fields_mapping = Column(Text, comment="字段映射信息,json结构")
    bk_biz_id = Column(Integer, nullable=False, comment="样本集业务")
    created_by = Column(String(50), nullable=False, comment="创建者")
    updated_by = Column(String(50), nullable=False, comment="更新者")


def test_erp_query(metadata_client):
    ret = metadata_client.query_via_erp(
        {"Range": {"starts": ["591_hugo_large_result_table"], "expression": {"*": True}}}
    )
    assert isinstance(ret, dict)


def test_graphql_query(metadata_client):
    ret = metadata_client.query("{info(func: has(ResultTable.typed)) {cnt:count(uid)}}")
    assert isinstance(ret, dict)


def test_edit(metadata_client):
    try:
        with metadata_client.session as se:
            se.create(
                ResultTable(
                    **{
                        u"bk_biz_id": 5,
                        u"result_table_name_alias": u"ok",
                        u"processing_type": u"stream",
                        u"count_freq": 1,
                        u"updated_by": u"admin",
                        u"platform": u"bkdata",
                        u"created_at": u"2018-11-18 19:13:20",
                        u"updated_at": u"2018-11-18 19:13:20",
                        u"created_by": u"admin",
                        u"result_table_name": u"ok",
                        u"project_id": 2,
                        u"result_table_id": 20,
                        u"description": u"ok",
                    }
                )
            )
            se.create(
                Range(
                    id="test_id",
                    project_count=1,
                    biz_count=2,
                    depth=3,
                    node_count_list="",
                    node_count=10,
                    weighted_node_count=0.011,
                    app_code_count=10,
                    range_score=6,
                    normalized_range_score=8,
                )
            )
            se.create(
                LifeCycle(
                    id="16356_raw_data",
                    target_id="16356",
                    target_type="access_raw_data",
                    range_id="16356_raw_data",
                    heat_id="16356_raw_data",
                )
            )
            se.commit()
    except Exception:
        raise
    assert True


def test_event_subscribe(metadata_client):
    test_config = dict(name="neo_test_1", key="test", refer="test", callback=lambda x: json.dumps(x))
    subscriber = metadata_client.event_subscriber(test_config)
    print("==register first time==")
    set_orders = subscriber.start_to_listening()
    assert set_orders == subscriber.subscribe_orders


def test_subscriber_register_twice(metadata_client):
    test_config = dict(name="neo_test_2", key="test", refer="test", callback=lambda x: json.dumps(x))
    subscriber = metadata_client.event_subscriber(test_config)
    print("==register second time==")
    set_orders = subscriber.start_to_listening()
    assert set_orders is None


def test_meta_sync(metadata_client):
    global_settings.SYNC_CONTENT_TYPE = "content"
    engine = create_engine("sqlite:///:memory:", echo=True)
    Base.metadata.create_all(engine)
    session_factory = sessionmaker()
    session_ins = session_factory(bind=engine)
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
        session_ins.delete(new_user)
        # 可有可无，autocommit=False时，sync_hook最后会自动提交一次
        session_ins.commit()
        for instance in session_ins.query(SampleResultTable).all():
            pass

    try:
        # 实例demo
        run_demo()
    except Exception:
        raise
    assert True
