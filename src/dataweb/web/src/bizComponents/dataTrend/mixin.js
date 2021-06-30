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
      bizId: -1,
      deleteNodeInfo: {
        isShow: false,
        item: {},
      },
      recentWrapperBg: {
        background: `url(${  window.userPic  }) repeat`,
      },
      loading: {
        historyLoading: true,
        minLoading: true,
        dayLoading: true,
        reportedLoading: false,
        nodeLoading: true,
        buttonLoading: false,
      },
      edit: false,
      issued: {
        // 下发弹窗
        dialogStatus: false,
      },
      tabActiveName: 'nodeInfo',
      paging: {
        // 节点分页
        totalPage: 1,
        page: 1, // 当前页码
        page_size: 10, // 每页数量
        ordering: '',
      },
      defaultDemo: {
        selected: 0,
      },
      reportedData: [], // 最近上报数据
      min_time: '',
      max_time: '',
      minuteParams: {
        // 分钟数据量参数
        frequency: '1m',
        start_time: '',
        end_time: '',
        date: [],
      },
      dayParams: {
        frequency: '1d',
        start_time: '',
        end_time: '',
        date: [],
      },
      noPermissionReason: '',
    };
  },
  methods: {
    // 编辑采集详情
    edit_detail() {
      this.edit = !this.edit;
      if (!this.edit) {
        console.log(this.$t('保存'));
      }
    },
    // 取消编辑
    cancel_edit() {
      this.edit = !this.edit;
    },
    cancelIssued(close) {
      close(true);
    },
  },
};
