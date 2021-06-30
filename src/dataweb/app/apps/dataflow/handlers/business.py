# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""
from django.conf import settings
from django.core.cache import cache
from django.core.exceptions import ValidationError
from django.core.validators import validate_ipv4_address
from django.utils.translation import ugettext as _
from django.utils.translation import ugettext_noop

from apps.api import AuthApi, CCApi, GseApi
from apps.common.log import logger
from apps.exceptions import ComponentCallError, FormError
from apps.utils import APIModel

# 内部版 v1 云区域 id 为 1
IEOD_STATIC_BK_CLOUD_ID = 1


class Business(APIModel):
    """
    从 CC 读取业务数据，本地缓存两份数据，一份缓存，一份备份

    cache
        | -- cc_biz_list 缓存业务列表，过期时间 1 分钟
        | -- cc_biz_list_backup 备份业务列表，当 CC 服务不可用时，从此读取
    """

    AUTH_ROLES = ["Maintainers"]
    MAX_LIMIT = 200

    def __init__(self, biz_id):
        super(Business, self).__init__()
        self.biz_id = biz_id

    @classmethod
    def list(cls):
        biz_list = cache.get("cc_biz_list")
        if biz_list is None:
            try:
                biz_list = CCApi.get_app_list()
            except ComponentCallError as e:
                logger.exception(_("获取业务列表异常，args=%s") % (e.args,))

                # 备份拯救世界
                return cls._get_backup_biz_list()

            cache.set("cc_biz_list", biz_list, 60)
            # 永不过期
            cache.set("cc_biz_list_backup", biz_list, None)

        return biz_list

    @classmethod
    def get_name_dict(cls):
        biz_list = cls.list()
        return {int(_biz["bk_biz_id"]): _biz["bk_biz_name"] for _biz in biz_list}

    @classmethod
    def get_app_by_user(cls, user):
        """
        获取用户有权限的业务列表
        """
        return AuthApi.list_user_biz({"user_id": user})

    @classmethod
    def get_app_ids_by_user(cls, user):
        data = cls.get_app_by_user(user)
        return [int(_d["bk_biz_id"]) for _d in data]

    @classmethod
    def wrap_biz_name(cls, data, val_key="bk_biz_id", display_key="bk_biz_name", key_path=None):
        """
        数据列表，统一加上业务名称
        @param {Int} val_key 业务 ID 的 KEY 值
        @param {String} display_key 业务名称的 KEY 值
        @param {List} key_path val_key在data中所在的字典路径
        @paramExample 参数样例
        {
            "data": [
                {
                    "target_details": {
                        "xxx": {
                            "bk_biz_id": 3
                        }
                    }
                }
            ],
            "val_key": "bk_biz_id",
            "display_key": "bk_biz_name",
            "key_path": ["target_details", "xxx"]
        }
        @successExample 成功返回
        [
            {
                "target_details": {
                    "xxx": {
                        "bk_biz_id": 3
                        "bk_biz_name": u"蓝鲸",
                    }
                }
            }
        ]
        """
        if not key_path:
            key_path = []
        _name_dict = cls.get_name_dict()

        for _d in data:
            _temp_data = _d
            for _key_path in key_path:
                _temp_data = _temp_data[_key_path]
            biz_id = int(_temp_data[val_key])
            _temp_data[display_key] = _name_dict[biz_id] if biz_id in _name_dict else ""
        return data

    @classmethod
    def list_cloud_area(cls):
        """
        查询给定模型的实例信息
        """
        cloud_areas = cls.puller_by_pagination(CCApi.search_cloud_area, limit=200)
        cloud_areas = sorted(cloud_areas, key=lambda a: a["bk_cloud_id"])

        # 定制化代码
        if settings.RUN_VER == "ieod":
            for cloud in cloud_areas:
                if int(cloud["bk_cloud_id"]) == 0:
                    cloud["bk_cloud_name"] = "CC3.0默认云区域"

            # 内部版1.0 补充 bk_cloud_id=1
            cloud_areas.insert(0, {"bk_cloud_name": "CC1.0默认云区域", "bk_cloud_id": 1})

        return cloud_areas

    @staticmethod
    def puller_by_pagination(api, limit=200):
        """
        通过分页拉取所有数据，目前仅对 CMDB 拉取云区域数据有效，对 API 格式要求

        - paramaters
            {
                'page': {
                    'start': 0,
                    'limit': 2000
                }
            }
        - response
            {
                'count': 20,
                'info': [
                    {}
                ]
            }

        """
        start = 0

        data = []
        while 1:
            params = {"page": {"start": start, "limit": limit}}
            content = api(params)
            data.extend(content["info"])
            # 如果当前页还有内容，则继续往下一页拉取
            if len(content["info"]) > 0:
                start += limit
            else:
                break

        return data

    @classmethod
    def get_cloud_area_map(cls):
        """
        获取云区域映射
        """
        cloud_area = cls.list_cloud_area()
        return {_cloud["bk_cloud_id"]: _cloud["bk_cloud_name"] for _cloud in cloud_area}

    @classmethod
    def generate_inst_uniq_key(cls, bk_inst_id, bk_obj_id):
        """
        生成唯一键，便于寻找
        """
        return "{}-{}".format(bk_inst_id, bk_obj_id)

    def wrap_agent_status(self, hosts):
        """
        主机列表补充 agent 状态
        """
        if not hosts:
            return []

        status = self._get_agent_status(hosts, bk_biz_id=self.biz_id)
        for _h in hosts:
            _key = "{}:{}".format(_h["bk_cloud_id"], _h["ip"])
            if _key in list(status.keys()):
                if status[_key]["bk_agent_alive"] == 1:
                    _h["status"] = 1
                    _h["status_display"] = _("已安装")
                else:
                    _h["status"] = 0
                    _h["status_display"] = _("异常")
            else:
                _h["status"] = -1
                _h["status_display"] = _("未安装")
        return hosts

    def list_role_member(self):
        roles = [ugettext_noop("bk_biz_maintainer")]

        api_params = {"fields": roles, "condition": {"bk_biz_id": self.biz_id}}

        data = CCApi.get_biz_user(api_params)
        if data["count"] == 0:
            return []

        members = []
        for _role in roles:
            _role_members_str = data["info"][0][_role]

            members.append(
                {
                    "role": _role,
                    "role_display": _(_role),
                    "members": _role_members_str.split(",") if _role_members_str else [],
                }
            )

        return members

    def get_instance_topo(self):
        """
        获取CC各个层级构成TOPO，不仅仅支持 set、moudle
        """
        biz_inst_topo = cache.get("biz_inst_topo_%s" % self.biz_id)
        if biz_inst_topo is None:
            biz_inst_topo = CCApi.search_biz_inst_topo({"bk_biz_id": self.biz_id, "level": -1})
            cache.set("biz_inst_topo_%s" % self.biz_id, biz_inst_topo, 60)
        return biz_inst_topo

    def recursion_topo_children(self, topo_dict, children=None, current_path=""):
        """
        递归获取bk_inst_id+bk_obj_id 对应的实例名称
        @param {Dict} topo_dict 传入一个空字典用于递归存储
        @paramExample 参数样例
        {
            "topo_dict": {},
            "children": {
                "bk_inst_id": 2,
                "bk_obj_id": "biz",
                "bk_inst_name": u"蓝鲸",
                "child": []
                }
        }
        @reutrn 得到的topo_dict样例
        {
            "2-biz": {
                "bk_inst_name": u"蓝鲸",
                "bk_inst_path": "/蓝鲸/xxxxx/数据服务模块/dataapi"
                }
        }
        """
        if children is None:
            children = self.get_instance_topo()
        if not len(children):
            return topo_dict

        for child in children:
            bk_inst_path = "{}/{}".format(current_path, child["bk_inst_name"])
            uniq_key = self.generate_inst_uniq_key(child["bk_inst_id"], child["bk_obj_id"])
            topo_dict.update({uniq_key: {"bk_inst_name": child["bk_inst_name"], "bk_inst_path": bk_inst_path}})
            self.recursion_topo_children(topo_dict, children=child.get("child", []), current_path=bk_inst_path)

    def get_cc_topo_instance_map(self):
        """
        获取cc拓扑实例映射
        """
        cc_topo_instance_map = {}
        self.recursion_topo_children(cc_topo_instance_map)
        return cc_topo_instance_map

    def filter_valid_hosts(self, hosts):
        """
        过滤在业务底下的主机列表，返回合法主机列表，非法主机列表

        @note 因为太方便，顺便在这里补充下云区域名称，后续如果不合理，再去掉这块耦合的逻辑
        @param {[Dict]} hosts，示例
            [
                {'ip': '127.0.0.1', 'bk_cloud_id': 1}
            ]
        """
        # 获取云区字典映射
        m_cloud_areas = self.get_cloud_area_map()

        # 检查用户是否传入了多个云区域
        if not self._check_bk_cloud_id(hosts):
            raise FormError("云区域一次只能选一个！")

        bk_cloud_id = hosts[0]["bk_cloud_id"]
        # 如果用户选择的云区域不等于1，则走v3的接口查询
        if bk_cloud_id != IEOD_STATIC_BK_CLOUD_ID:
            force_v3 = True
        else:
            force_v3 = False

        # 正则不匹配的ip
        irregular_hosts = []
        m_host = {}
        for _h in hosts:
            try:
                validate_ipv4_address(_h["ip"])
            except ValidationError:
                irregular_hosts.append({"ip": _h["ip"], "bk_cloud_id": _h["bk_cloud_id"], "error": _("非法IP地址")})
            else:
                m_host.update({"{}:{}".format(_h["bk_cloud_id"], _h["ip"]): _h})

        # 每页拉取的数量
        limit = 500
        # 开始的位置
        start = 0
        # 总记录数
        count = 1
        data = []
        while start < count:
            api_params = {
                "page": {"start": start, "limit": limit, "sort": "bk_host_id"},
                "bk_biz_id": self.biz_id,
                "fields": ["bk_host_id", "bk_cloud_id", "bk_host_innerip"],
                "force_v3": force_v3,
            }
            response_data = CCApi.search_host(api_params)
            data_info = response_data.get("info", [])
            if data_info is None:
                data_info = []
            data.extend(data_info)
            count = response_data.get("count", 0)
            start += len(data_info)

        valid_hosts = []
        valid_keys = []
        for _d in data:
            _inner_ip, _cloud_id = self._parse_host_attribute(_d)
            _key = "{}:{}".format(_cloud_id, _inner_ip)
            if _key in m_host:
                valid_hosts.append(
                    {"ip": _inner_ip, "bk_cloud_id": _cloud_id, "bk_cloud_name": m_cloud_areas.get(_cloud_id, "暂无信息")}
                )
                valid_keys.append(_key)

            # 特殊逻辑，对于云区域为0的主机，同样可以匹配云区域1
            if _cloud_id == 0:
                _key = "1:{}".format(_inner_ip)
                if _key in m_host:
                    valid_hosts.append(
                        {"ip": _inner_ip, "bk_cloud_id": 1, "bk_cloud_name": m_cloud_areas.get(1, "暂无信息")}
                    )
                    valid_keys.append(_key)


        invalid_hosts = []
        for _key, _host in list(m_host.items()):
            if _key not in valid_keys:
                _bk_cloud_id = _host["bk_cloud_id"]
                _bk_cloud_name = m_cloud_areas[_bk_cloud_id] if _bk_cloud_id in m_cloud_areas else _bk_cloud_id

                invalid_hosts.append(
                    {
                        "ip": _host["ip"],
                        "bk_cloud_id": _host["bk_cloud_id"],
                        "error": _("该业务在云区域【{}】下不存在以下主机").format(_bk_cloud_name),
                    }
                )
        invalid_hosts.extend(irregular_hosts)
        return valid_hosts, invalid_hosts

    def _check_bk_cloud_id(self, hosts):
        bk_cloud_id_set = {h["bk_cloud_id"] for h in hosts}
        if len(bk_cloud_id_set) > 1:
            return False
        return True

    def _parse_host_attribute(self, data):
        return data["bk_host_innerip"], data["bk_cloud_id"]

    def get_biz_name(self):
        """
        获取业务名称
        """
        _name_dict = self.get_name_dict()
        _bk_biz_id = int(self.biz_id)
        if _bk_biz_id in _name_dict:
            return _name_dict[_bk_biz_id]

        return _("未知业务")

    @staticmethod
    def _get_backup_biz_list():
        """
        从备份中获取业务列表
        """
        backup_biz_list = cache.get("cc_biz_list_backup")
        if backup_biz_list is not None:
            return backup_biz_list
        else:
            return []

    @staticmethod
    def _get_agent_status(hosts, bk_biz_id=0):
        """
        查询主机 Agent 状态
        @param hosts {array.<dict>} 示例
            [
                {'ip': '127.0.0.1', 'plat_id': 1}
            ]
        @result {Dict}
            {
                "1:127.0.0.1": {
                    "status": 1
                }
            }
        """
        bk_cloud_id = hosts[0]["bk_cloud_id"]
        force_v3 = False
        if bk_cloud_id != IEOD_STATIC_BK_CLOUD_ID:
            force_v3 = True

        api_params = {"bk_biz_id": bk_biz_id, "bk_supplier_id": 0, "hosts": hosts, "force_v3": force_v3}
        return GseApi.get_agent_status(api_params)
