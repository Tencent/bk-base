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

export default {
  data() {
    return {
      taskStageList: [
        { id: 'run', name: this.$t('运行') },
        { id: 'submit', name: this.$t('提交') },
      ],
      refreshConfigList: [
        { id: 0, name: this.$t('关闭') },
        { id: 5, name: 5 + this.$t('秒') },
        { id: 10, name: 10 + this.$t('秒') },
        { id: 30, name: 30 + this.$t('秒') },
        { id: 60, name: 1 + this.$t('分钟') },
      ],
      submitLogField: {
        batch: ['executeId'],
        stream: [],
      },
      runLogField: {
        batch: ['calcTask', 'container', 'executeId', 'logType', 'role', 'taskStage'],
        stream: ['calcTask', 'container', 'logType', 'role', 'taskStage'],
      },
    };
  },
  computed: {
    selectedItemType() {
      let curTask = null;
      this.calcTaskList.some((group) => {
        const result = group.children.some((item) => {
          if (item.id === this.config.calcTask) {
            curTask = item;
            return true;
          }
          return false;
        });
        return !!result.length;
      });
      return curTask.type;
    },
    roleList() {
      if (this.taskType === 'batch') {
        return [{ name: 'Executor', id: 'executor' }];
      }
      if (this.taskType === 'stream') {
        return [
          { name: 'TaskManager', id: 'taskmanager' },
          { name: 'JobManager', id: 'jobmanager' },
        ];
      }
      return [];
    },
    logTypeList() {
      if (this.taskType === 'batch') {
        return [
          { name: 'Stdout', id: 'stdout' },
          { name: 'Stderr', id: 'stderr' },
        ];
      }
      if (this.taskType === 'stream') {
        return [
          { name: 'Stdout', id: 'stdout' },
          { name: 'Stderr', id: 'stderr' },
          { name: 'Exception', id: 'exception' },
          { name: 'Log', id: 'log' },
        ];
      }
      return [];
    },
  },
};
