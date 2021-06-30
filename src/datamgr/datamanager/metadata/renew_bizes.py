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
import copy
import json
import logging
import operator
import sys
import time

import attr
import requests
from metadata_client import DEFAULT_SETTINGS, MetadataClient
from more_itertools import chunked

from conf.settings import metadata_settings
from metadata.exc import MetaDataTaskError
from metadata.local_metadata import BizV1, BizV3, BKBiz

logger = logging.getLogger(__name__)


class RenewBizes(object):
    def __init__(self):
        self.settings = DEFAULT_SETTINGS.copy()
        self.settings.update(metadata_settings)
        self.metadata_client = MetadataClient(self.settings)
        self.bk_app_code = self.settings.BK_APP_CODE
        self.bk_app_secret = self.settings.BK_APP_SECRET
        self.cc_v1_lst = None
        self.cc_v3_lst = None
        self.cc_v3_id_set = set()
        self.biz_metadata_buffer = {}
        self.biz_mutate_lst = []

    @staticmethod
    def replace_user_separator(user_ids):
        """
        CC1.0中的成员用";"分隔，CC3.0用的是","，统一为CC1.0的输出
        @param user_ids: 用户列表  "aaa,bbb,ccc"
        @return:  "aaa;bbb;ccc"
        """
        user_ids = str(user_ids) if user_ids else ""
        return user_ids.replace(",", ";")

    def get_cc_v1_lst(self):
        if not self.settings.CC_V1_URL:
            return []
        r = requests.get(
            self.settings.CC_V1_URL,
            params={
                "bk_app_code": self.bk_app_code,
                "bk_supplier_account": "tencent",
                "bk_app_secret": self.bk_app_secret,
            },
        )
        r.raise_for_status()
        info = r.json()
        if not info["result"]:
            raise MetaDataTaskError("Fail to get cc list.")
        return r.json()["data"]

    def get_cc_v3_lst(self):
        if not self.settings.CC_V3_URL:
            return []
        r = requests.get(
            self.settings.CC_V3_URL,
            params={
                "bk_app_code": self.bk_app_code,
                "bk_supplier_account": "tencent",
                "bk_app_secret": self.bk_app_secret,
                "bk_username": "admin",
            },
        )
        r.raise_for_status()
        info = r.json()
        if not info["result"]:
            raise MetaDataTaskError("Fail to get cc v3 list.")
        return r.json()["data"]["info"]

    def create_v1_bizes(self):
        for item in self.cc_v1_lst:
            item["ApplicationID"] = int(item["ApplicationID"])
            # 检测业务是否已经迁移到v3
            if item["ApplicationID"] in self.cc_v3_id_set:
                continue
            if "BizV1" not in self.biz_metadata_buffer:
                self.biz_metadata_buffer["BizV1"] = {}
            self.biz_metadata_buffer["BizV1"][str(item["ApplicationID"])] = BizV1(
                **item
            )
            bk_biz_kwargs = {
                "id": item["ApplicationID"],
                "name": item["ApplicationName"],
                "maintainer": item["Maintainers"],
                "developer": item["AppDevMan"],
                "planner": item["OperationPlanning"],
                "biz_type": "v1",
                "app_type": item["AppTypeName"],
                "game_type": item["GameTypeName"],
                "source": item["SourceName"],
                "operate_state": item["OperStateName"],
                "bip_grade": item["BipGradeName"],
            }
            if "BKBiz" not in self.biz_metadata_buffer:
                self.biz_metadata_buffer["BKBiz"] = {}
            self.biz_metadata_buffer["BKBiz"][str(item["ApplicationID"])] = BKBiz(
                **bk_biz_kwargs
            )

    def create_v3_bizes(self):
        for item in self.cc_v3_lst:
            item["bk_biz_id"] = int(item["bk_biz_id"])
            if not item.get("bk_biz_tester", ""):
                item["bk_biz_tester"] = (
                    str(item.get("bk_pmp_test_tm", "")).replace(";", ",")
                    if item.get("bk_pmp_test_tm", "")
                    else ""
                )
            attr_fields = attr.fields_dict(BizV3)
            kwargs = {}
            for k, v in item.items():
                if k in attr_fields:
                    if (
                        v is not None
                        and attr_fields[k].type == "int"
                        and not isinstance(v, int)
                    ):
                        logger.warning(
                            "value {} of {} is not instance of {} , filter this dirty item ".format(
                                v, k, attr_fields[k].type
                            )
                        )
                        continue
                    kwargs[k] = v
            kwargs["extra_"] = json.dumps(
                {k: v for k, v in item.items() if k not in attr.fields_dict(BizV3)}
            )
            if "BizV3" not in self.biz_metadata_buffer:
                self.biz_metadata_buffer["BizV3"] = {}
            self.biz_metadata_buffer["BizV3"][str(item["bk_biz_id"])] = BizV3(**kwargs)
            bk_biz_kwargs = {
                "id": item["bk_biz_id"],
                "name": item["bk_biz_name"],
                "maintainer": self.replace_user_separator(
                    item.get("bk_biz_maintainer", "")
                ),
                "developer": self.replace_user_separator(
                    item.get("bk_biz_developer", "")
                ),
                "planner": self.replace_user_separator(item.get("bk_oper_plan", "")),
                "biz_type": "v3",
            }
            if "BKBiz" not in self.biz_metadata_buffer:
                self.biz_metadata_buffer["BKBiz"] = {}
            self.biz_metadata_buffer["BKBiz"][str(item["bk_biz_id"])] = BKBiz(
                **bk_biz_kwargs
            )
            # 记录v3已经构建的业务id,避免v1旧版本重复构建
            self.cc_v3_id_set.add(int(bk_biz_kwargs["id"]))

    @staticmethod
    def std_biz_item(item, biz_cls):
        """
        标准化biz数据
        :param item: biz数据信息
        :param biz_type: biz类定义
        :return: dict 标准化之后的biz数据
        """
        std_item = {}
        managed_attr_list = [attr_def.name for attr_def in attr.fields(biz_cls)]
        for k, v in item.items():
            k = k.replace("{}.".format(biz_cls.__name__), "")
            if k not in managed_attr_list:
                continue
            if k == "typed":
                continue
            if sys.version_info[0] == 3 and not isinstance(v, (int, str, float, bool)):
                v = ""
            if sys.version_info[0] == 2 and not isinstance(
                v, (int, str, unicode, float, bool)
            ):  # noqa
                v = ""
            std_item[k] = "" if not v else v
        return std_item

    def get_biz_diff(self, biz_to_check, biz_cls, biz_pri_key):
        """
        检查配置库拉取的biz信息和元数据biz信息的差异, 获取biz更新和新增进行变更
        :param biz_to_check: 待检查的配置库biz信息
        :param biz_cls: biz类定义
        :param biz_pri_key: biz信息的主键名称
        :return: None
        """
        biz_id_lst = copy.copy(list(biz_to_check.keys()))
        biz_id_chunked_lst = chunked(biz_id_lst, 50)
        for biz_id_chunk in biz_id_chunked_lst:
            statement = "{{ret_biz_lst(func: eq({}.{}, [{}])){{ {{{}}} }}}}".format(
                biz_cls.__name__,
                biz_pri_key,
                ",".join(biz_id_chunk),
                " ".join(
                    [
                        "{}.{}".format(biz_cls.__name__, attr_def.name)
                        for attr_def in attr.fields(biz_cls)
                    ]
                ),
            )
            local_biz_lst = self.metadata_client.query(statement)
            turn_buffer = dict()
            # 检查biz更新
            for local_biz in local_biz_lst.get("data", {}).get("ret_biz_lst", []):
                local_std_item = self.std_biz_item(local_biz, biz_cls)
                cc_obj = biz_to_check.get(str(local_std_item[biz_pri_key]), {})
                cc_std_item = self.std_biz_item(attr.asdict(cc_obj), biz_cls)
                if operator.ne(cc_std_item, local_std_item):
                    turn_buffer[str(local_std_item[biz_pri_key])] = dict(
                        change=True,
                        biz_obj=cc_obj,
                        BKBiz=self.biz_metadata_buffer["BKBiz"][
                            str(local_std_item[biz_pri_key])
                        ],
                    )
                else:
                    turn_buffer[str(local_std_item[biz_pri_key])] = dict(change=False)

            for local_biz_id, buffer_info in turn_buffer.items():
                if buffer_info["change"]:
                    self.biz_mutate_lst.append(buffer_info["biz_obj"])
                    self.biz_mutate_lst.append(buffer_info["BKBiz"])
                del biz_to_check[str(local_biz_id)]

            time.sleep(0.3)

        # 检查biz新增
        if biz_to_check:
            for new_biz_id, new_biz_obj in biz_to_check.items():
                self.biz_mutate_lst.append(new_biz_obj)
                self.biz_mutate_lst.append(
                    self.biz_metadata_buffer["BKBiz"][str(new_biz_id)]
                )

    def renew_bizes(self):
        for biz_cls, biz_pri_key in {
            BizV1: "ApplicationID",
            BizV3: "bk_biz_id",
        }.items():
            biz_to_check = self.biz_metadata_buffer.get(biz_cls.__name__, {})
            self.get_biz_diff(biz_to_check, biz_cls, biz_pri_key)

        mutate_chunked_bizes = chunked(self.biz_mutate_lst, 200)
        for chuck in mutate_chunked_bizes:
            with self.metadata_client.session as se:
                se.create(chuck)
                se.commit()

    def run(self):
        # self.cc_v1_lst = self.get_cc_v1_lst()
        # logger.info('[Metadata] Got cc v1 list.')
        self.cc_v3_lst = self.get_cc_v3_lst()
        logger.info("[Metadata] Got cc v3 list.")
        self.create_v3_bizes()
        # self.create_v1_bizes()
        logger.info("[Metadata] Created Biz metadata list. Start to renew Biz.")
        self.renew_bizes()
        logger.info(
            "[Metadata] {} Biz has been updated".format(len(self.biz_mutate_lst))
        )


def renew_bizes():
    logger.info("[Metadata] Start to renew businesses")
    RenewBizes().run()
    logger.info("[Metadata] Succeed to renew businesses")
