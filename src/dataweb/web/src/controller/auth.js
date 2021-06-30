/*
 * Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
 * BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
 *
 * License for BK-BASE 蓝鲸基础平台:
 * --------------------------------------------------------------------
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
 * and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
 * LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 * NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE
 */

/* eslint-disable no-prototype-builtins */
class Auth {
  /** 群集结果格式化 */
  formatClusterResultData(resData, clusterType) {
    const authedClusterGroup = resData[0].data;
    const clusterConfigs = (resData[1].data || []).sort((a, b) => b.priority - a.priority);
    const clusterGroupConfigs = resData[2].data;

    return clusterGroupConfigs
      .map((cgf, index) => {
        const isGroupReadonly = !this.checkIsPermissioned(authedClusterGroup, cgf.cluster_group_id);
        return {
          alias: cgf.cluster_group_alias,
          name: cgf.cluster_group_name,
          id: cgf.cluster_group_id,
          index: index + 1,
          isReadonly: isGroupReadonly,
          children: Array.prototype.map.call(
            Array.prototype.filter.call(
              clusterConfigs,
              cf => cf.cluster_group === cgf.cluster_group_id
                                && (clusterType ? cf.cluster_type === clusterType : true),
            ),
            (item, ix) => ({
              isReadonly: isGroupReadonly,
              alias: item.description || item.cluster_name,
              id: `${cgf.cluster_group_id}_${item.id}`,
              cluster_name: item.cluster_name,
              index: ix + 1,
              clusterGroup: item.cluster_group,
              priority: item.priority,
              connection: item.connection,
              isEnableReplica:
                                item.connection.hasOwnProperty('enable_replica')
                                && String(item.connection.enable_replica) === 'true',
            }),
          ),
        };
      })
      .filter(item => item.children.length > 0 && !item.isReadonly);
  }

  checkIsPermissioned(authedClusterGroup = [], groupId) {
    return authedClusterGroup.some(gp => gp.cluster_group_id === groupId);
  }
}

export default Auth;
