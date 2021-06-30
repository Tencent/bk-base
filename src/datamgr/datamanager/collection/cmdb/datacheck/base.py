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
"""
cmdb 数据对账 Mixin 基类
"""
import logging
from concurrent.futures import as_completed
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Set

from api import dataquery_api
from collection.conf.constants import DATACHECK_MEASUREMENT, POOL_SIZE
from common.metrics import report_metrics

logger = logging.getLogger(__name__)


class CMDBBaseCheckerMixin:
    """
    数据对账基类，需要跟 CMDBBaseCollector 结合和使用
    """

    def check_all_biz(self):
        """
        对全部业务依次进行对账
        """
        bk_biz_ids = self.bk_biz_ids
        logger.info(
            f"Start check cmdb {self.key_name} with rt {self.rt_id}, business count is {len(bk_biz_ids)}"
        )

        same_biz_ids = []
        diff_biz_ids = []
        error_biz_ids = []

        pool = ThreadPoolExecutor(POOL_SIZE)
        future_to_bk_biz_id = {
            pool.submit(self.check_biz, bk_biz_id): bk_biz_id
            for bk_biz_id in bk_biz_ids
        }
        for future in as_completed(future_to_bk_biz_id):
            bk_biz_id = future_to_bk_biz_id[future]
            try:
                if future.result():
                    same_biz_ids.append(bk_biz_id)
                else:
                    diff_biz_ids.append(bk_biz_id)
            except Exception as err:
                logger.exception(f"[BIZ({bk_biz_id})] Fail to check biz content, {err}")
                error_biz_ids.append(bk_biz_id)

        result = {
            "same_count": len(same_biz_ids),
            "diff_count": len(diff_biz_ids),
            "error_count": len(error_biz_ids),
        }

        logger.info(
            f"Check finished. Same biz count: {result['same_count']}; "
            f"Diff biz count: {result['diff_count']}; Diff biz ids: {diff_biz_ids}; "
            f"Error biz count: {result['error_count']}; Error biz ids: {error_biz_ids}."
        )

        return result

    def check_biz(self, bk_biz_id: int) -> bool:
        """
        对指定的业务下数据进行对账

        :param bk_biz_id: 业务 ID
        """
        # 第一步，检查内容，目前仅支持新增和删除
        content_result = self.check_biz_content(self.rt_id, bk_biz_id)
        logger.info(f"[BIZ({bk_biz_id})] Content diff result: {content_result}")
        self.report_metrics(
            object_type=self.object_type,
            bk_biz_id=bk_biz_id,
            cmdb_count=content_result["cmdb_count"],
            pub_count=content_result["rt_count"],
            create_count=content_result["need_create_count"],
            update_count=content_result["need_update_count"],
            delete_count=content_result["need_delete_count"],
        )

        # 第二步，修复数据，新增和更新类的问题，有周期批量同步的方式进行修复，无需处理
        # 删除问题待解决，可以考虑重新上报一条 event_type=delete 的数据
        pass

        return content_result["result"]

    def check_biz_content(self, rt_id, bk_biz_id):
        """
        检查该业务下对象 KEY，返回差异结论
        """
        rt_keys = self.query_biz_rt_keys(rt_id, bk_biz_id)
        cmdb_keys = set(self.collect_biz_content(bk_biz_id, only_pk_key=True))

        need_create_keys = cmdb_keys.difference(rt_keys)
        need_delete_keys = rt_keys.difference(cmdb_keys)

        diff_result = not bool(len(need_create_keys) + len(need_delete_keys))

        # 目前在数量上和主键上对齐
        result = {
            "result": diff_result,
            "rt_count": len(rt_keys),
            "cmdb_count": len(cmdb_keys),
            "bk_biz_id": bk_biz_id,
            "diff_content": {
                "need_create_keys": need_create_keys,
                "need_delete_keys": need_delete_keys,
            },
            "need_create_count": len(need_create_keys),
            "need_delete_count": len(need_delete_keys),
            "need_update_count": 0,
        }

        return result

    def query_biz_rt_keys(self, rt_id: str, bk_biz_id: int) -> Set:
        """
        获取 rt 内存储的数据详情
        """
        # 这里有局限性，单个业务超过 100 万主机时，这里的查询就会出问题
        content_query = (
            f"SELECT {self.key_name} FROM {rt_id} "
            f"WHERE bk_biz_id={bk_biz_id} "
            f'AND bkpub_event_type != "delete" LIMIT 1000000'
        )

        content_data = dataquery_api.query_sql(
            sql=content_query, prefer_storage="ignite"
        )
        rt_keys = {d[self.key_name] for d in content_data}

        return rt_keys

    @staticmethod
    def report_metrics(
        object_type: str,
        bk_biz_id: int,
        cmdb_count: int = 0,
        pub_count: int = 0,
        create_count: int = 0,
        update_count: int = 0,
        delete_count: int = 0,
    ):
        """
        上报对账的结论

        :param object_type: 对象类型，目前检测的有 host/set/module/relation
        :param bk_biz_id: 业务ID
        :param cmdb_count: CMDB数量
        :param pub_count: 平台公共数据数量
        :param create_count: 新建数量
        :param update_count: 更新数量
        :param delete_count: 删除数量
        """
        tags_mapping = {"bk_biz_id": bk_biz_id, "object_type": object_type}
        fields_mapping = {
            "cmdb_count": cmdb_count,
            "pub_count": pub_count,
            "create_count": create_count,
            "update_count": update_count,
            "delete_count": delete_count,
        }

        report_metrics(DATACHECK_MEASUREMENT, fields_mapping, tags_mapping)
