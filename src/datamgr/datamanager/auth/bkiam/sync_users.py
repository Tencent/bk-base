# -*- coding:utf-8 -*-
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
import time
from concurrent.futures.thread import ThreadPoolExecutor

from iam import Action

from auth.bkiam import constant
from auth.bkiam.bkiam import PullIamDataHandler, SyncHandler
from auth.bkiam.constant import POOL_SIZE, SEPARATOR
from auth.bkiam.convert import Convert
from auth.bkiam.data_config import action_maps
from auth.bkiam.manager import Manager
from auth.models import AuthUserRole, Resource
from common.db import connections
from conf import settings

SUPPORT_IAM = getattr(settings, "SUPPORT_IAM", False)
logger = logging.getLogger(__name__)


def sync_one_action(user_roles, iam_action_resource_dict):
    """
    一次同步单个用户
    :param user_roles: 用户角色列表 [auth.models.AuthUserRole, auth.models.AuthUserRole...]
    :param iam_action_resource_dict:
        {
            'user01$flow-retrieve': [auth.models.ResourceScope, auth.models.ResourceScope],
            'user01$project-retrieve': [auth.models.ResourceScope, auth.models.ResourceScope]
        }
    """
    # 获取用户 id
    user_id = user_roles[0].user_id

    logger.info(
        "[sync_users sync_one_action] Start to sync {} authorization".format(user_id)
    )

    sync_user = SyncUser()
    # 聚合计算出 {action: [ResourceScope]} 字典
    local_action_resource_dict = sync_user.merge_data(user_roles)
    # 首先对比权限中心是否比本地多注册了 action
    # todo 未来这边可能会和权限中心订阅有冲突，需要考虑
    delete_action_dict = sync_user.contrast_actions(
        user_id, local_action_resource_dict, iam_action_resource_dict
    )
    if not delete_action_dict.keys():
        logger.debug(
            "[sync_users sync_one_action] {} has no action to delete in iam".format(
                user_id
            )
        )
    else:
        # 移除 权限中心 比本地多出来的action
        sync_user.remove_action_in_iam(delete_action_dict)

    for user_action in local_action_resource_dict.keys():
        # 逐条对比 action
        user_local_scope = local_action_resource_dict[user_action]
        user_iam_scope = iam_action_resource_dict.get(user_action, [])
        # 对比得出 add_resources 和 delete_resources
        add_resources, delete_resources = sync_user.contrast_resource(
            user_iam_scope, user_local_scope
        )
        # 从 user_action_key 中拿到 action_id
        action_id = sync_user.parse_user_action_key(user_action, "action")
        if not add_resources and not delete_resources:
            logger.debug(
                "[sync_users sync_one_action] Nothing has changed in this action, and no synchronization"
                " is required, user_id={}, action_id={}".format(user_id, action_id)
            )
            continue
        try:
            sync_user.batch_actions_grant_or_revoke(
                user_id, action_id, delete_resources, sync_user.sync_handler.REVOKE
            )
            sync_user.batch_actions_grant_or_revoke(
                user_id, action_id, add_resources, sync_user.sync_handler.GRANT
            )
            logger.info(
                "[sync_users sync_one_action] Succeed to sync, user_id={}, action_id={}".format(
                    user_id, action_id
                )
            )
        except Exception as err:
            logger.exception(
                "[sync_users sync_one_action] Fail to sync action, user_id={}, action_id={}, "
                "error={}".format(user_id, action_id, err)
            )


def remove_iam_extra_user_autho(user_list, iam_action_resource_dict):
    """
    计算得出需要从权限中心删除的用户，保持本地有权限的用户数量和权限中心有权限的用户数量相同
    :param user_list: 本地有权限的用户set集合
    :param iam_action_resource_dict: 权限中心权限集合
    """
    logger.info("[sync_users remove_iam_extra_user_autho] Start checking users ...")
    sync_user = SyncUser()
    remove_users_dict = {}
    for user_action in iam_action_resource_dict.keys():
        user = sync_user.parse_user_action_key(user_action, "user")
        if user not in user_list:
            remove_users_dict[user_action] = iam_action_resource_dict[user_action]
    if not remove_users_dict:
        logger.info(
            "[sync_users remove_iam_extra_user_autho] No user needs to be synchronized ..."
        )
        return
    logger.info(
        "[sync_users remove_iam_extra_user_autho] Start removing users on IAM... ,users = {}".format(
            remove_users_dict.keys()
        )
    )
    sync_user.remove_action_in_iam(remove_users_dict)


def migrate_iam_autho():
    """
    同步平台用户权限至蓝鲸权限中心
    """
    # 当前环境未部署 BKIAM
    if not SUPPORT_IAM:
        logger.info("[sync_users migrate_iam_autho] No BKIAM, not to migrate...")
        return

    # 初始化数据
    logger.info("[sync_users migrate_iam_autho] start sync ...")
    iam_action_resource_dict = PullIamDataHandler().init_data()
    with connections["basic"].session() as session:
        # 获取用户列表[(user_id,) (user_id)]
        user_group = (
            session.query(AuthUserRole.user_id).distinct(AuthUserRole.user_id).all()
        )
        # 转换成如下格式的 set{1, 2, 3, 4, 5}
        user_list = {user[0] for user in user_group}
        # 遍历用户列表
        pool = ThreadPoolExecutor(POOL_SIZE)
        remove_iam_extra_user_autho(user_list, iam_action_resource_dict)
        for user in user_list:
            # 获取该用户下所有的角色规则
            user_roles = (
                session.query(AuthUserRole).filter(AuthUserRole.user_id == user).all()
            )
            pool.submit(sync_one_action, user_roles, iam_action_resource_dict)

        while not pool._work_queue.empty():
            # 阻塞主线程，每隔五秒
            logger.info(
                "[sync_users migrate_iam_autho] In the synchronous, there are currently {} users waiting for "
                "synchronization".format(pool._work_queue.qsize())
            )
            time.sleep(constant.CHECK_POOL_INTERVAL_TIME)


class SyncUser:
    def __init__(self):
        self.sync_handler = SyncHandler()
        self.role_manager = Manager("roles")
        self.action_manager = Manager("actions")
        self.convert = Convert()

    def batch_actions_grant_or_revoke(
        self, user_id, iam_action_id, resource_scopes, operate
    ):
        """
        :param user_id: 用户id
        :param iam_action_id: 操作id
        :param resource_scopes: list[auth.models.ResourceScope, auth.models.ResourceScope...]
        :param operate: grant or revoke
        """
        instance_resources = self.get_instance_resources(iam_action_id, resource_scopes)
        path_resources = self.get_path_resources(resource_scopes)
        self.sync_handler.sync_instances_actions(
            user_id, instance_resources, [Action(iam_action_id)], operate
        )
        self.sync_paths_action(user_id, path_resources, Action(iam_action_id), operate)

    def contrast_resource(self, user_iam_scope, user_local_scope):
        """
        对比本地策略和权限中心策略的差异，得出需要增加 和 减去的资源
        :param user_iam_scope: 用户本地权限范围 list[auth.models.ResourceScope, auth.models.ResourceScope...]
        :param user_local_scope: 用户在权限中心的权限范围: list[auth.models.ResourceScope, auth.models.ResourceScope...]
        :return: add_resources：list[auth.models.ResourceScope, auth.models.ResourceScope...],
                 delete_resources: list[auth.models.ResourceScope, auth.models.ResourceScope...]
        """

        mapping_user_iam_scope = {
            resource_scope.id: resource_scope for resource_scope in user_iam_scope
        }
        mapping_user_local_scope = {
            resource_scope.id: resource_scope for resource_scope in user_local_scope
        }

        add_resources = [
            mapping_user_local_scope[key]
            for key in mapping_user_local_scope
            if key not in mapping_user_iam_scope
        ]

        delete_resources = [
            mapping_user_iam_scope[key]
            for key in mapping_user_iam_scope
            if key not in mapping_user_local_scope
        ]

        return add_resources, delete_resources

    def contrast_actions(
        self, user_id, local_action_resource_map, iam_action_resource_map
    ):
        """
        对比本地用户和权限中心的action，获取那些需要在权限中心删除的action
        :param user_id: 用户id
        :param local_action_resource_map: 本地权限范围 :
            {
                'user02$data_token-manage': [auth.models.ResourceScope])
            }
        :param iam_action_resource_map: 权限中心权限
             {
                'user01$data_token-manage': [auth.models.ResourceScope])
                'user01$data_token-manage_auth': [auth.models.ResourceScope])
            }
        :return: {
                'user01$data_token-manage_auth': [auth.models.ResourceScope])
            }
        """
        # 过滤得出某个用户在权限中心的所有action
        iam_user_action_resource_map = {
            user_action: iam_action_resource_map[user_action]
            for user_action in iam_action_resource_map
            if user_id == self.parse_user_action_key(user_action, "user")
        }
        # 对比本地，得到那些需要在权限中心删除的action
        delete_action_dict = {
            user_action: iam_action_resource_map[user_action]
            for user_action in iam_user_action_resource_map
            if user_action not in local_action_resource_map
        }

        return delete_action_dict

    def remove_action_in_iam(self, delete_action_dict):
        """
        移除权限中心多出来的 action
        :param user_id: 用户id
        :param delete_action_dict:  需要删除的action
            {
                'user01$data_token-manage': [auth.models.ResourceScope])
            }
        """
        for user_action, resources in delete_action_dict.items():
            try:
                user_id = self.parse_user_action_key(user_action, "user")
                action_id = self.parse_user_action_key(user_action, "action")
                self.batch_actions_grant_or_revoke(
                    user_id, action_id, resources, self.sync_handler.REVOKE
                )
                logger.debug(
                    "[SyncUser remove_action_in_iam]: Succeed to remove action in iam, user_id={}, "
                    "action_id={}".format(user_id, action_id)
                )
            except Exception as err:
                logger.exception(
                    "[SyncUser remove_action_in_iam]: Fail to remove action in iam, user_id={}, "
                    "action_id={}, err={}".format(user_id, action_id, err)
                )

    def get_instance_resources(self, iam_action_id, resource_scopes):
        """
        筛选资源授权相关的资源实例
        :param iam_action_id: 操作id
        :param resource_scopes: 资源列表 [auth.models.ResourceScope, auth.models.ResourceScope...]
        :return: list[auth.models.Resource,auth.models.Resource,auth.models.Resource...]
        """
        instance_resources = []
        for resource_scope in resource_scopes:
            # 实例授权相关的资源类型为 [1，2，3] 路径授权相应的类型为 resource&id
            if resource_scope.type == constant.INSTANCE:
                # 实例授权 资源类型 即为该操作本身
                try:

                    # 如果存在映射，将action转到本地再进行查询
                    # key: 本地 action_id , value: iam action_id
                    action_id = Convert.to_local_action(iam_action_id)
                    for key, value in action_maps.items():
                        if iam_action_id == value:
                            action_id = key

                    object_class_id = self.action_manager.get(
                        action_id=action_id
                    ).object_class
                    instance_resources.append(
                        Resource(
                            id=resource_scope.id,
                            resource_type=object_class_id,
                            display_name=resource_scope.id,
                        )
                    )
                except Exception as err:
                    logger.exception(
                        "[SyncUser get_instance_resources] {} is not exist, err={}".format(
                            action_id, err
                        )
                    )
        return instance_resources

    def get_path_resources(self, resource_scopes):
        """
        筛选路径授权相关的资源实例
        :param resource_scopes: 资源列表 [auth.models.ResourceScope, auth.models.ResourceScope...]
        :return: list[auth.models.Resource, auth.models.Resource...]
        """
        path_resources = []
        for resource_scope in resource_scopes:
            # 处理多层路径的情况，如project下的某个result_table的授权
            if resource_scope.type == constant.COMPLEX_PATH:
                data = resource_scope.id.split("#")
                resource_list = []
                for d in data:
                    object_class_id = d.split(SEPARATOR)[0]
                    resource_id = d.split(SEPARATOR)[1]
                    resource_list.append(
                        Resource(
                            id=resource_id,
                            resource_type=object_class_id,
                            display_name=resource_id,
                        )
                    )
                path_resources.append(resource_list)
                continue
            if resource_scope.type == constant.SIMPLE_PATH:
                # 路径授权对应的资源类型为 resource&id 的 resource
                object_class_id = resource_scope.id.split(SEPARATOR)[0]
                resource_id = resource_scope.id.split(SEPARATOR)[1]
                path_resources.append(
                    Resource(
                        id=resource_id,
                        resource_type=object_class_id,
                        display_name=resource_id,
                    )
                )
        return path_resources

    def sync_paths_action(self, user_id, role_resources, action, operate):
        """
        路径授权
        :param user_id: 用户 id
        :param role_resources: 资源列表 list[auth.models.Resource, auth.models.Resource...]
        :param action: iam.auth.models.Action
        :param operate: grant or revoke
        """
        # 路径授权资源类型为操作对应的资源类型，如 flow-manager 为 flow
        object_class = action.id.split("-")[0]
        # 如果没有资源 或者 没有action 需要同步，直接返回
        if not role_resources:
            return
        paths = []
        # 生成资源路径
        for role_resource in role_resources:
            if isinstance(role_resource, list):
                path = []
                for resource in role_resource:
                    path.append(
                        {
                            "type": resource.resource_type,
                            "id": resource.id,
                            "name": resource.display_name,
                        }
                    )
                paths.append(path)
            else:
                paths.append(
                    [
                        {
                            "type": role_resource.resource_type,
                            "id": role_resource.id,
                            "name": role_resource.display_name,
                        }
                    ]
                )

        self.sync_handler.sync_paths_action(
            user_id, object_class, paths, action, operate
        )

    def merge_data(self, user_roles):
        """
        合并用户本地角色，生成{action：resource_list} 数据
        :param user_roles: [AuthUserRole, AuthUserRole, AuthUserRole]
        :return: 用户本地权限的集合：
            {
                'user01$project-retrieve': [auth.models.ResourceScope, auth.models.ResourceScope],
                'user01$flow-retrieve': [auth.models.ResourceScope, auth.models.ResourceScope],
                'user01$result_table-query_data': [auth.models.ResourceScope, auth.models.ResourceScope]
            }
        """
        action_resource_map = {}
        for user_role in user_roles:
            try:
                role_config = self.role_manager.get(role_id=user_role.role_id)
            except Exception as err:
                logger.exception(
                    "[SyncUser merge_data] {} is not exist, user_id={}, err={}".format(
                        user_role.role_id, user_role.user_id, err
                    )
                )
                continue
            if not role_config.to_iam:
                continue
            object_class_id = role_config.object_class
            # Todo: 这里的策略占不支持 scope_attr_key 属性范围的同步
            policies = self.sync_handler.get_related_policies_with_role(
                role_id=user_role.role_id
            )
            for action_policy in policies:
                key = Convert.get_user_action_key(
                    user_role.user_id, action_policy.action_id
                )
                action_resource_map.setdefault(key, [])
                # 忽略没有实例的action
                if not action_policy.has_instance:
                    continue
                if object_class_id != action_policy.object_class:
                    resource = Convert.to_local_path(
                        object_class_id, user_role.scope_id
                    )
                    action_resource_map[key].extend(
                        self.convert.build_resource_scopes(resource)
                    )
                else:
                    action_resource_map[key].extend(
                        self.convert.build_resource_scopes(user_role.scope_id)
                    )
        return action_resource_map

    def parse_user_action_key(self, user_action_key, type):
        """
        :param user_action_key: user01$project-delete
        :param type: 类型： user or action
        :return: user01 or project-delete
        """
        if type == "user":
            return user_action_key.split(constant.KEY_SEPARATOR)[0]
        else:
            return user_action_key.split(constant.KEY_SEPARATOR)[1]
