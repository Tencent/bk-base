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

/**
   * 格式化日志计算任务列表
   * @param {*} data
   */
function formatDataLogList(res) {
  const taskList = [];
  const nameMap = {
    batch: window.$t('离线任务'),
    stream: window.$t('实时任务'),
    model: window.$t('模型应用任务'),
  };
  for (const [key, value] of Object.entries(res.data.result_table_dict)) {
    const group = {
      name: nameMap[key],
      id: key,
    };
    const children = Object.keys(value).map((key) => {
      const children = {
        id: key,
        name: value[key].result_tables.map(item => item.result_table_name_alias).join(','),
        type: value[key].component_type.includes('spark') ? 'batch' : 'stream',
        deployMode: value[key].deploy_mode,
      };
      return children;
    });
    group.children = children;
    taskList.push(group);
  }
  res.data.list = taskList;
}

/**
 * 获取任务列表
 * @query flow_id
 */
const getTaskList = {
  url: '/v3/dataflow/flow/flows/:fid/list_rt_by_flowid/',
  callback(res) {
    if (res.result) {
      // 格式化数据
      formatDataLogList(res);
    }
    return res;
  },
};

/**
 * 获取执行记录
 * @query job_id
 * @query get_app_id_flag 默认为false，为true获取appid
 */
const getHistoryList = {
  url: 'v3/dataflow/flow/log/get_flow_job_submit_history/',
};

/**
 * 获取flow中的appID
 * @query job_id
 * @query execute_id
 */
const getAppId = {
  url: 'v3/dataflow/flow/log/get_flow_job_app_id/',
};

/**
 * 获取容器，日志文件长度
 * @query app_id
 * @query log_component_type jobManager/taskManager等
 */
const getContainer = {
  url: 'v3/dataflow/flow/log/get_common_container_info/',
};

/**
 * @query app_id
 * @query job_name
 * @query log_component_type
 * @query log_type
 * @query start
 * @query end
 */
const getYarnRunLog = {
  url: 'v3/dataflow/flow/log/get_common_yarn_log/',
};

const getK8sRunLog = {
  url: 'v3/dataflow/flow/log/get_common_k8s_log/',
};

/**
 * 获取提交日志的大小
 * @query job_id
 * @query execute_id
 */
const getSubmitLogSize = {
  url: 'v3/dataflow/flow/log/get_flow_job_submit_log_file_size/',
};

/**
 * 获取提交日志
 * @query job_id
 * @query execute_id
 * @query begin
 * @query end
 */
const getSubmitLog = {
  url: 'v3/dataflow/flow/log/get_flow_job_submit_log/',
};

export {
  getTaskList,
  getHistoryList,
  getAppId,
  getContainer,
  getYarnRunLog,
  getSubmitLogSize,
  getSubmitLog,
  getK8sRunLog,
};
