# -- coding: utf-8 --
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
import logging
import math

import attr
from django.db.models import Q
from iam import IAM, DjangoQuerySetConverter
from iam.auth.models import (
    Action,
    ApiBatchAuthRequest,
    ApiBatchAuthResourceWithId,
    ApiBatchAuthResourceWithPath,
    Subject,
)
from more_itertools import chunked

from auth.bkiam import constant
from auth.bkiam.constant import RESOURCE_NUMBER
from auth.bkiam.convert import Convert
from auth.bkiam.data_config import action_maps
from auth.bkiam.manager import Manager
from auth.models import Resource
from conf.settings import (
    IAM_API_HOST,
    IAM_API_PAAS_HOST,
    IAM_REGISTERED_APP_CODE,
    IAM_REGISTERED_APP_SECRET,
    IAM_REGISTERED_SYSTEM,
)

logger = logging.getLogger(__name__)

# 配置文件
iam = IAM(
    IAM_REGISTERED_APP_CODE, IAM_REGISTERED_APP_SECRET, IAM_API_HOST, IAM_API_PAAS_HOST
)


class BaseHandler:
    def to_iam_action(self, action_id):
        """
        IAM 关于动作的定义有格式要求，不允许有点号，在两个系统间需要做装换
        """
        # 判断该action是否需要转换
        if action_id in action_maps.keys():
            return action_maps[action_id].replace(".", "-")
        return action_id.replace(".", "-")


class SyncHandler(BaseHandler):
    GRANT = "grant"
    REVOKE = "revoke"

    def __init__(self):
        self._iam = iam
        self.system = IAM_REGISTERED_SYSTEM
        self.role_manager = Manager("roles")
        self.action_manager = Manager("actions")
        self.object_manager = Manager("objects")
        self.policy_manager = Manager("polices")

    def sync_role(self, user_roles, role_id, operate=GRANT):
        """
        :param user_roles: 用户权限记录 [auth.models.AuthUserRole, auth.models.AuthUserRole]
        :param role_id: 角色id
        :param operate: 操作 grant or revoke
        """
        if not user_roles:
            return
        user_id = user_roles[0].user_id
        role_resources = []
        # 获取该角色策略
        policies = self.get_related_policies_with_role(role_id=role_id)
        # 获取该角色对应的object_class
        object_class_id = self.get_role(role_id).object_class
        # 将该用户有权限的所有资源进行归总
        for user_role in user_roles:
            role_resources.append(
                Resource(
                    id=user_role.scope_id,
                    resource_type=object_class_id,
                    display_name=user_role.scope_id,
                )
            )

        action_policy_list = self.get_sync_actions(policies)
        # 获取该角色下所有属于实例授权的action
        instance_actions = self.get_batch_instance_actions(
            action_policy_list, role_resources[0].resource_type
        )
        # 获取该角色下所有属于路径授权的action
        resource_action_dict = self.get_batch_path_actions(
            action_policy_list, role_resources[0].resource_type
        )

        try:
            self.sync_instances_actions(
                user_id, role_resources, instance_actions, operate
            )
            self.sync_paths_actions(
                user_id, role_resources, resource_action_dict, operate
            )
        except Exception as err:
            logger.exception(
                "[IAM RoleSync] Fail to batch sync, user={}, role={}, error={}".format(
                    user_id, role_id, err
                )
            )
        logger.info(
            "[IAM RoleSync] Succeed to batch sync, user={}, role={}".format(
                user_id, role_id
            )
        )

    def sync_paths_action(self, user_id, resource_type, paths, action, operate):
        """
        单个操作多资源情况下的拓扑授权
        :param user_id: 用户id
        :param resource_type: 资源类型
        :param paths: 路径集合 :
                [
                    [
                        {'type': 'project', 'id': '13143', 'name': '13143'},
                        {'type': 'flow', 'id': 'e', 'name': 'e'}
                    ]
                ]
        :param action: 操作：iam.auth.models.Action
        :param operate: 操作类型 grant or revoke
        """
        for _paths in list(chunked(paths, RESOURCE_NUMBER)):
            # 生成 resources 对象
            resources = [
                ApiBatchAuthResourceWithPath(
                    IAM_REGISTERED_SYSTEM, resource_type, _paths
                )
            ]
            # 生成 权限中心 request 对象
            request = ApiBatchAuthRequest(
                self.system,
                Subject("user", user_id),
                [action],
                resources,
                operate=operate,
            )
            # 调用批量授权/回收接口
            self._iam.batch_grant_or_revoke_path_permission(
                request, bk_username=user_id
            )

    def sync_instances_actions(self, user_id, role_resources, actions, operate):
        """
        批量实例授权
        :param user_id: 用户 id
        :param role_resources:资源列表：[auth.models.Resource, auth.models.Resource...]
        :param actions:Action列表：[iam.auth.models.Action, iam.auth.models.Action...]
        :param operate: grant or revoke
        """
        if not role_resources or not actions:
            return
        instances = []
        for role_resource in role_resources:
            instances.append({"id": role_resource.id, "name": role_resource.id})

        instances_list = list(chunked(instances, RESOURCE_NUMBER))
        for instance_list in instances_list:
            resources = [
                ApiBatchAuthResourceWithId(
                    IAM_REGISTERED_SYSTEM,
                    role_resources[0].resource_type,
                    instance_list,
                )
            ]
            request = ApiBatchAuthRequest(
                self.system,
                Subject("user", user_id),
                actions,
                resources,
                operate=operate,
            )
            self._iam.batch_grant_or_revoke_instance_permission(
                request, bk_username=user_id
            )

    def sync_paths_actions(
        self, user_id, role_resources, resource_action_dict, operate
    ):
        """
        批量拓扑授权
        :param user_id: 用户 id
        :param role_resources:资源列表： [auth.models.Resource, auth.models.Resource...]
        :param resource_action_dict: 资源_操作字典：{'result_table':[ActionPolicy]}
        :param operate: grant or revoke
        :note 目前仅供 sync_role 同步使用，将多个 Action 按照资源类型进行聚合传递
        """
        if not role_resources or not resource_action_dict:
            return
        paths = []
        # 生成资源路径
        for role_resource in role_resources:
            paths.append(
                [
                    {
                        "type": role_resource.resource_type,
                        "id": role_resource.id,
                        "name": role_resource.id,
                    }
                ]
            )
        # 生成 资源类型: path 映射表，防止多个Action 同步时因为路径出现冲突
        resources_path = {}
        for values in resource_action_dict.values():
            for v in values:
                # 根据资源类型，生成 Resource 对象
                resources_path.setdefault(v.object_class, set()).add(
                    ApiBatchAuthResourceWithPath(
                        IAM_REGISTERED_SYSTEM, v.object_class, paths
                    )
                )
        for object_class in resource_action_dict.keys():
            request = ApiBatchAuthRequest(
                self.system,
                Subject("user", user_id),
                [
                    Action(action_policy.action_id)
                    for action_policy in resource_action_dict[object_class]
                ],
                list(resources_path[object_class]),
                operate=operate,
            )
            self._iam.batch_grant_or_revoke_path_permission(
                request, bk_username=user_id
            )

    def get_related_policies_with_role(self, role_id, action_id=None, to_iam=True):
        """
        获取与角色关联的操作

        @returnExample  role_id=project.manager 样例
            [
                {
                    'action_id': 'flow.manage',
                    'object_class': 'flow',
                    'scope_attr_key': null,
                    'scope_attr_value': null
                },
                {
                    'action_id': 'result_table.query_data',
                    'object_class': 'result_table',
                    'scope_attr_key': 'sensitivity',
                    'scope_attr_value': 'private'
                },
                {
                    'action_id': 'project.manage',
                    'object_class': 'project',
                    'scope_attr_key': null,
                    'scope_attr_value': null
                }
            ]

        """
        if action_id is None:
            raw_policies = self.policy_manager.filter(role_id=role_id)
        else:
            raw_policies = self.policy_manager.filter(
                role_id=role_id, action_id=action_id
            )
        cleaned_policies = list()
        for p in raw_policies:
            if p.action_id == "*":
                actions = self.action_manager.filter(
                    object_class=p.object_class, to_iam=to_iam
                )
                policies = [
                    ActionPolicy(
                        action_id=self.to_iam_action(a.action_id),
                        has_instance=a.has_instance,
                        to_iam=a.to_iam,
                        object_class=p.object_class,
                        scope_attr_key=p.scope_attr_key,
                        scope_attr_value=p.scope_attr_value,
                    )
                    for a in actions
                ]
            else:
                action = self.action_manager.get(action_id=p.action_id)
                if action.to_iam == to_iam:
                    policies = [
                        ActionPolicy(
                            action_id=self.to_iam_action(action.action_id),
                            has_instance=action.has_instance,
                            to_iam=action.to_iam,
                            object_class=p.object_class,
                            scope_attr_key=p.scope_attr_key,
                            scope_attr_value=p.scope_attr_value,
                        )
                    ]
                else:
                    policies = []
            cleaned_policies.extend(policies)

        # 去重
        cleaned_policies = {p.action_id: p for p in cleaned_policies}.values()

        return cleaned_policies

    def get_role(self, role_id):
        return self.role_manager.get(role_id=role_id)

    def get_batch_instance_actions(self, action_policy_list, resource_type):
        """
        获取所有 实例 授权的action
        :param action_policy_list: [ActionPolicy, ActionPolicy, ActionPolicy]
        :param resource_type: 资源类型
        :return: [iam.auth.models.Action, iam.auth.models.Action...]
        """
        instance_actions = []
        for action_policy in action_policy_list:
            if action_policy.object_class == resource_type:
                instance_actions.append(Action(action_policy.action_id))
        return instance_actions

    def get_batch_path_actions(self, action_policy_list, resource_type):
        """
        获取所有 路径 授权的action
        :param action_policy_list: [ActionPolicy, ActionPolicy, ActionPolicy]
        :param resource_type: 资源类型
        :return: resource_action_dict:
            {
                'flow': [ActionPolicy],
                'result_table': [ActionPolicy]
            }
        """
        resource_action_dict = {}
        for action_policy in action_policy_list:
            if action_policy.object_class != resource_type:
                resource_action_dict.setdefault(action_policy.object_class, []).append(
                    action_policy
                )
        return resource_action_dict

    def get_sync_actions(self, policies):
        """
        获取符合条件的的操作列表
        :param policies: [ActionPolicy, ActionPolicy, ActionPolicy]
        :return: [ActionPolicy, ActionPolicy, ActionPolicy]
        """
        action_policy_list = []
        for policy in policies:
            if not policy.has_instance:
                continue
            if not policy.to_iam:
                continue
            if policy.scope_attr_key:
                continue
            action_policy_list.append(policy)
        return action_policy_list


class PullIamDataHandler(BaseHandler):
    def __init__(self):
        self._iam = iam
        self.system = IAM_REGISTERED_SYSTEM
        self.action_manager = Manager("actions")

    def init_data(self):
        """
        从权限那边拉到数据并初始化
        :return:  {
                'user01$flow-retrieve': [auth.models.ResourceScope, auth.models.ResourceScope],
                'user01$project-retrieve': [auth.models.ResourceScope, auth.models.ResourceScope]
            }
        """
        # 获取所有向权限中心注册的action
        iam_action_resource_dict = {}
        action_manager = Manager("actions")
        logger.info("[PullIamDataHandler init_data] start init data from iam...")
        actions = action_manager.filter(to_iam=True, has_instance=True)
        for action in actions:
            iam_action_id = self.to_iam_action(action.action_id)
            self._pull_iam_data(iam_action_id, iam_action_resource_dict)
        return iam_action_resource_dict

    def _pull_iam_data(self, iam_action_id, iam_action_resource_dict):
        """
        向权限中心分页拉取单个操作的策略
        """
        logger.info(
            "[PullIamDataHandler pull_iam_data] start pull data from iam, action_id={}".format(
                iam_action_id
            )
        )
        page = 1
        # 总页数
        page_count = 1
        # 分页拉取
        while page <= page_count:
            data = {
                "action_id": iam_action_id,
                "page": page,
                "page_size": constant.PAGE_SIZE,
            }
            try:
                # 从权限中心拉取策略
                policies = self._iam.query_polices_with_action_id(self.system, data)
                count = policies.get("count", 0)
                # 计算总页数
                page_count = math.ceil(count / constant.PAGE_SIZE)
                page += 1
                for item in policies["results"]:
                    if item["subject"].get("type", None) == constant.USER:
                        # 获取 user_id
                        user_id = item["subject"]["id"]
                        # 解析权限表达式
                        c = DjangoQuerySetConverter(None)
                        # 转换得到权限表达式的内容
                        expression = c.convert(item["expression"]).deconstruct()
                        self._parse_expression(
                            user_id, iam_action_id, expression, iam_action_resource_dict
                        )
            except Exception as err:
                logger.exception(
                    "[PullIamDataHandler _pull_iam_data] policies pull failed, action_id={}, err={}".format(
                        iam_action_id, err
                    )
                )
                # 发生异常退出该action的拉取, 终止操作
                raise err

    def _parse_expression(
        self, user_id, iam_action_id, expression, iam_action_resource_dict
    ):
        """
        解析权限中心表达式，构造 iam_action_resource_dict
        :param user_id: 用户 id
        :param iam_action_id: 操作 id
        :param expression: 支持的表达式类型：
            单个 拓扑授权：
            ('django.db.models.Q', (), {'flow._bk_iam_path___startswith': '/project,68/'})
            or 多个拓扑授权：
            (   'django.db.models.Q',
                (
                    ('flow._bk_iam_path___startswith', '/project,222/'),
                    ('flow._bk_iam_path___startswith', '/project,220/'),
                    ('flow._bk_iam_path___startswith', '/project,228/'),
                ),
                {'_connector': 'OR'})
            or
            # 单个实例授权：
            ('django.db.models.Q', (), {'project.id': '288'})
            or
            # 多个实例授权：
            ('django.db.models.Q', (), {'project.id__in': ['13123', '13141']})
            or
            # 多个复杂路径授权
            (
                'django.db.models.Q',
                (
                    <Q: (AND: ('flow.id', '3213'), ('flow._bk_iam_path___startswith', '/project,13111/'))>,
                    <Q: (AND: ('flow.id', '3287'), ('flow._bk_iam_path___startswith', '/project,13143/'))>
                ),
                {'_connector': 'OR'}
            )
            # 单个复杂路径授权
            ('django.db.models.Q', (('flow.id', '1234'), ('flow._bk_iam_path___startswith', '/project,1111/')), {})
            or
            # 多路径多实例授权
            (
                'django.db.models.Q',
                (
                    ('result_table.id__in', ['591_jere_tdm',
                                             '591_testbeacontmd',
                                             '591_aaa22',
                                             '970_jere_test_tdm',
                                             '591_xxx22']),
                    ('result_table._bk_iam_path___startswith', '/raw_data,100192/')
                ),
                {'_connector': 'OR'}
            )

        :param iam_action_resource_dict: 本地权限范围：
            {
                'user01$flow-retrieve': [auth.models.ResourceScope, auth.models.ResourceScope],
                'user01$project-retrieve': [auth.models.ResourceScope, auth.models.ResourceScope]
            }
        """
        # 当等于OR的时候，expression_content_multiple 存储的是多个权限内容
        expression_content = expression[2]
        # 单个权限的内容 ('django.db.models.Q', (), {'project.id': '1'})
        expression_contents = expression[1]
        # 生成字典的key ： user01$flow-retrieve
        user_action_key = Convert.get_user_action_key(user_id, iam_action_id)
        # 初始化 key 默认值为 []
        iam_action_resource_dict.setdefault(user_action_key, [])

        if expression_content.get("_connector", None) == constant.OR:
            # 如果存在多个条件表达式，路径授权，则遍历
            for content in expression_contents:
                if isinstance(content, Q):
                    # 多个表达式嵌套，递归处理
                    self._build_iam_action_resource_dict(
                        user_action_key,
                        iam_action_id,
                        content,
                        iam_action_resource_dict,
                    )
                else:
                    # content: ('result_table._bk_iam_path___startswith', '/raw_data,100192/') or \
                    # ('result_table.id__in', ['591_jere_tdm', '591_testbeacontmd'])
                    self._build_iam_action_resource_dict(
                        user_action_key,
                        iam_action_id,
                        content[1],
                        iam_action_resource_dict,
                    )
        # 处理单个二级路径的情况
        elif not expression_content and expression_contents:
            self._parse_complex_expression(
                user_action_key,
                iam_action_id,
                expression_contents,
                iam_action_resource_dict,
            )
        else:
            # 实例授权 or 路径授权(只有一个路径的前提下)
            for value in expression_content.values():
                if value is None or isinstance(value, bool):
                    continue
                self._build_iam_action_resource_dict(
                    user_action_key, iam_action_id, value, iam_action_resource_dict
                )

    def _build_iam_action_resource_dict(
        self,
        user_action_key,
        iam_action_id,
        iam_resource_scope,
        iam_action_resource_dict,
    ):
        """
        统一将iam 权限写进 iam_action_resource_dict 中
        :param user_action_key: iam_action_resource_dict的key： user01$flow-retrieve
        :param iam_action_id: 操作id： flow-retrieve
        :param iam_resource_scope: 取出来的权限范围 :
                                多实例授权：['12813', '12814', '12812']
                                单实例授权：12963
                                单路径授权：/project,68/
                                双层路径授权：(AND: ('flow.id', '1234'), ('flow._bk_iam_path___startswith', '/project,1111/'))
        :param iam_action_resource_dict:
                 {
                        'user01$flow-retrieve': [auth.models.ResourceScope, auth.models.ResourceScope],
                        'user01$project-retrieve': [auth.models.ResourceScope, auth.models.ResourceScope]
                    }
        :return:
        """
        if isinstance(iam_resource_scope, Q):
            # 转换格式,拿到 (('flow.id', '1234'), ('flow._bk_iam_path___startswith', '/project,1111/'))
            complex_expression = iam_resource_scope.deconstruct()[1]
            self._parse_complex_expression(
                user_action_key,
                iam_action_id,
                complex_expression,
                iam_action_resource_dict,
            )
        elif isinstance(iam_resource_scope, list):
            # 构造 ResourceScope 列表
            resources = Convert.build_resource_scopes(iam_resource_scope)
            iam_action_resource_dict[user_action_key].extend(resources)
        elif iam_resource_scope.startswith("/"):
            # 构造 资源路径 格式： project&1
            resource = Convert.build_resource_scopes(
                Convert.parse_iam_path_expression(iam_resource_scope)
            )
            iam_action_resource_dict[user_action_key].extend(resource)
        else:
            # 实例授权，构造 ResourceScope 列表
            resource = Convert.build_resource_scopes(iam_resource_scope)
            iam_action_resource_dict[user_action_key].extend(resource)

    def _parse_complex_expression(
        self, user_action_key, iam_action_id, expression, iam_action_resource_dict
    ):
        """
        处理双层路径的情况, 最终生成 {'user01$flow-retrieve': [auth.models.ResourceScope(id=parent&resource-id#chileren&id),
                                                          auth.models.ResourceScope]}
        目前仅支持 二层资源拓扑
        :param key: user_id$iam_action_id
        :param iam_action_id: iam_action_id
        :param expression: 权限表达式：(('flow.id', '3213'), ('flow._bk_iam_path___startswith', '/project,13111/'))
        :param iam_action_resource_dict: iam 权限范围:
            {
                'user01$flow-retrieve': [auth.models.ResourceScope, auth.models.ResourceScope],
                'user01$project-retrieve': [auth.models.ResourceScope, auth.models.ResourceScope]
            }
        """
        resource_path = ""
        for value in expression:
            if value[1].startswith("/"):
                resource_path = "{}{}".format(
                    Convert.parse_iam_path_expression(value[1]) + "#", resource_path
                )
            else:
                try:
                    local_action_id = Convert.to_local_action(iam_action_id)
                    # 如果存在映射，将action转到本地action再进行查询
                    for k, v in action_maps.items():
                        if iam_action_id == v:
                            local_action_id = k

                    object_class = self.action_manager.get(
                        action_id=local_action_id
                    ).object_class
                    resource_path += Convert.to_local_path(object_class, value[1])
                except Exception as err:
                    logger.exception(
                        "[PullIamDataHandler _parse_complex_expression] {} is not exist, err={}".format(
                            iam_action_id, err
                        )
                    )
        resource = Convert.build_resource_scopes(resource_path)
        iam_action_resource_dict[user_action_key].extend(resource)


@attr.s
class ActionPolicy(object):
    action_id = attr.ib(type=str)
    has_instance = attr.ib(type=bool)
    to_iam = attr.ib(type=bool)
    object_class = attr.ib(type=str)
    scope_attr_key = attr.ib(type=str)
    scope_attr_value = attr.ib(type=str)
