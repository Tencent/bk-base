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

import { bkRequest } from '@/common/js/ajax';

// 获取任务列表
const getTaskList = {
  url: '/v3/datalab/queries/',
  method: 'GET',
};
// 新增任务
const addTask = {
  url: '/v3/datalab/queries/',
  method: 'POST',
};
// 更新任务 （重命名&修改SQL）
const updateTask = {
  url: '/v3/datalab/queries/:query_id/',
  method: 'PUT',
};
// 获取用户项目列表
const getProjectLists = {
  url: '/v3/datalab/projects/mine/',
  method: 'GET',
};
// 删除任务
const delTask = {
  url: '/v3/datalab/queries/:query_id/',
  method: 'DELETE',
};
// 获取置顶表
const getDepotList = {
  url: '/v3/datalab/favorites/:project_id/',
  method: 'GET',
};
// 置顶
const topDepot = {
  url: '/v3/datalab/favorites/',
  method: 'POST',
};
// 取消置顶
const cancelTop = {
  url: '/v3/datalab/favorites/cancel/',
  method: 'DELETE',
};
// 获取查询结果
const getResultList = {
  url: '/v3/datalab/queries/:query_id/result/',
  method: 'GET',
};

/** 查看SQL作业轨迹 */
const getTaskStage = {
  url: '/v3/datalab/queries/:query_id/stage/',
};

/**
 * 获取项目下的笔记任务列表
 * query: project_id
 */
const getNotebooks = {
  url: '/v3/datalab/notebooks/',
};

/**
 * 创建笔记任务
 */
const createNotebook = {
  url: '/v3/datalab/notebooks/',
  method: 'POST',
};

/**
 * 删除笔记任务
 */
const deleteNotebook = {
  url: '/v3/datalab/notebooks/:notebook_id/',
  method: 'DELETE',
};

/**
 * 重命名Notebook
 */
const renameNotebook = {
  url: '/v3/datalab/notebooks/:notebook_id/',
  method: 'PUT',
};
/**
 * 查看历史记录
 */
const getHistory = {
  url: '/v3/datalab/queries/:query_id/history/',
  method: 'GET',
};

/**
 * 获取notebook的内核
 */
const getNotebookLanguage = {
  url: '/v3/datalab/notebooks/library/',
};

/**
 * 切换notebook内核
 * @param notebook_id:int 笔记任务id
 * @param kernel_type:string 切换的服务类型
 */
const switchKernel = {
  url: '/v3/datalab/notebooks/switch_kernel/',
  method: 'POST',
};

/**
 * 获取任务支持的存储
 */
const getAvailableStorage = {
  url: '/v3/datalab/queries/available_storage/',
  method: 'GET',
};

/**
 * 获取下载密钥
 * @params query_id:string
 */
const getDownLoadSecret = {
  url: '/v3/datalab/queries/:query_id/download/',
};

/**
 * 判断此时查询任务有没有被用户占用
 * @params query_id:string
 */
const getQueryLock = {
  url: '/v3/datalab/queries/:query_id/lock/',
};

/**
 * 判断此时笔记任务有没有被用户占用
 * @params notebook_id:string
 */
const getNotebookLock = {
  url: '/v3/datalab/notebooks/:notebook_id/lock/',
};

/**
 * 列举项目相关数据
 * @params notebook_id:string
 */
const getDataByProject = {
  url: '/v3/auth/projects/:project_id/data/',
  // url: '/v3/meta/result_tables/'  预计2021年后再修改回来
};

/** 启动/重启笔记服务 */
const startNotebook = {
  url: '/v3/datalab/notebooks/:notebook_id/start/',
  method: 'POST',
};

/**
 * 获取笔记产出物列表
 * @params notebookId
 */
const getNotebookOutputs = {
  url: '/v3/datalab/notebooks/:notebookId/outputs/',
};

/**
 * 获取SQL函数列表
 */
const getSQLList = {
  url: 'v3/datalab/queries/function/',
};

/** 获取算法模型列表 */
const getAlgorithmList = {
  url: '/v3/dataflow/modeling/algorithms/',
  callback(res) {
    Object.assign(res.data, {
      keyList: Object.keys(res.data),
    });
    return res;
  },
};

/** 改变笔记状态 */
const getTackLock = {
  url: '/v3/datalab/notebooks/:notebook_id/take_lock/',
  method: 'POST',
};

/** 笔记文件上传关联文件 */
const relateFile = {
  url: '/v3/auth/projects/:project_id/data/add/',
  method: 'POST',
};

/** 获取项目有权限的文件列表
 */
const getFilesList = {
  url: '/v3/auth/projects/:project_id/data/',
  callback(res) {
    if (res.result && res.data.length) {
      const bizId = res.data[0].bk_biz_id;
      return bkRequest.httpRequest('dataAccess/getDeployPlanList', {
        query: {
          bk_biz_id: bizId,
          data_scenario: 'offlinefile',
          raw_data_ids: res.data.map(item => item.raw_data_id),
        },
      });
    }
    return Promise.resolve(res);
  },
};

/** 笔记克隆
 * @params notebook_id:int
 * @params project_id:int
 * @params project_type:string
 */
const getNotebookClone = {
  url: '/v3/datalab/notebooks/:notebook_id/copy/',
  method: 'POST',
};

/**
 * 笔记生成报告
 * @params notebook_id:int
 */
const createdGenerateReport = {
  url: '/v3/datalab/notebooks/:notebook_id/generate_report/',
  method: 'POST',
};

/**
 * 根据报告秘钥获取报告内容
 */
const getReport = {
  url: '/v3/datalab/notebooks/report/',
};

/**
 * 获取当前笔记任务
 */
const getCurrentNotebook = {
  url: '/v3/datalab/notebooks/:notebook_id/',
  method: 'GET',
};

const getNotebooksContents = {
  url: '/v3/datalab/notebooks/:notebook_id/contents/',
  method: 'GET',
};

/**
 * 判断用户有无此笔记编辑权
 */
const getNotebookEditRight = {
  url: '/v3/datalab/notebooks/:notebook_id/edit_right/',
  method: 'GET',
};

export {
  getAlgorithmList,
  getSQLList,
  getNotebookOutputs,
  getTaskList,
  addTask,
  updateTask,
  getProjectLists,
  delTask,
  getDepotList,
  topDepot,
  cancelTop,
  getResultList,
  getTaskStage,
  getNotebooks,
  createNotebook,
  deleteNotebook,
  renameNotebook,
  getHistory,
  getNotebookLanguage,
  switchKernel,
  getAvailableStorage,
  getDownLoadSecret,
  getQueryLock,
  getNotebookLock,
  getDataByProject,
  startNotebook,
  getTackLock,
  relateFile,
  getFilesList,
  getNotebookClone,
  createdGenerateReport,
  getReport,
  getCurrentNotebook,
  getNotebooksContents,
  getNotebookEditRight,
};
