

<!--
  - Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
  - Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
  - BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
  -
  - License for BK-BASE 蓝鲸基础平台:
  - -------------------------------------------------------------------
  -
  - Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
  - documentation files (the "Software"), to deal in the Software without restriction, including without limitation
  - the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
  - and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
  - The above copyright notice and this permission notice shall be included in all copies or substantial
  - portions of the Software.
  -
  - THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
  - LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
  - NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
  - WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
  - SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE
  -->

<template>
  <div class="flow-table">
    <!-- 表格 -->
    <table class="dataflow-table"
      :class="{ 'list-none': tableList.length === 0 }">
      <thead>
        <tr>
          <th
            v-for="(field, index) in tableFields"
            :key="index"
            :title="field.subtitle"
            :class="[field.name, { click: field.sortable }]"
            :width="field.width"
            @click="sorting(field)">
            {{ field.title }}
            <template v-if="field.sortable">
              <i v-if="field.is_reverse === 1">
                <img src="../../common/images/icon/order-sort2.png">
              </i>
              <i v-else-if="field.is_reverse === -1">
                <img src="../../common/images/icon/order-sort.png">
              </i>
              <i v-else-if="field.is_reverse === 0">
                <img src="../../common/images/icon/order-reverse2.png">
              </i>
              <i v-else><img src="../../common/images/icon/order-reverse.png"></i>
            </template>
          </th>
        </tr>
      </thead>
      <tbody>
        <template v-if="tableList.length > 0">
          <tr v-for="(data, index) in tableList"
            :key="data.value">
            <template v-for="(value, key) in data">
              <td v-if="key === 'name'"
                :key="key"
                :class="key"
                @click="edit(data)">
                <template v-if="key !== 'status'">
                  <span>{{ value }}</span>
                </template>
              </td>
              <td v-else-if="key === 'result_table'"
                :key="key"
                :class="key"
                @click="details(data)">
                <span>
                  {{ value }}
                </span>
              </td>
              <td v-else-if="key === 'data_set'"
                :key="key"
                :title="value"
                :class="key"
                @click="details(data)">
                <span>
                  {{ value }}
                </span>
              </td>
              <td v-else-if="key === 'created_by'"
                :key="key"
                :class="key">
                <span class="user_photo">
                  <img :src="data.pic">
                </span>
                <span class="user_name">{{ value }}</span>
              </td>
              <td v-else-if="key === 'biz_id'"
                :key="key"
                :class="key">
                <span class="omitted"
                  :title="value">
                  {{ value }}
                </span>
              </td>
              <td v-else-if="key === 'has_clean'"
                :key="key"
                :class="key">
                <span v-if="value"
                  class="yes">
                  {{ $t('是') }}
                </span>
                <span v-else
                  class="no">
                  {{ $t('否') }}
                </span>
              </td>
              <td v-else
                :key="key"
                :class="key"
                @click="expandDetail($event)">
                <template v-if="key !== 'status'">
                  <span
                    :class="{
                      'no-data':
                        value === $t('暂未关联存储')
                        || value === $t('暂未关联任务') || value === $t('暂无上报数据'),
                    }">
                    {{ value }}
                  </span>
                </template>
                <template v-else>
                  <i
                    v-if="value === 'running' && !data.has_exception"
                    :title="$t('运行正常')"
                    class="task-status-run icons icon-flow" />
                  <i v-else-if="value === 'no-start'"
                    :title="$t('未启动')"
                    class="task-status icons icon-flow" />
                  <i
                    v-else-if="value === 'running' && data.has_exception"
                    :title="$t('运行异常')"
                    class="status-exception icons icon-flow" />
                </template>
              </td>
            </template>
            <td>
              <a
                class="bk-text-button bk-primary"
                :class="{ 'is-disabled': isDelete }"
                :title="$t('查看详情')"
                @click="details(data)">
                {{ $t('查看详情') }}
              </a>
              <a
                v-if="tableType === 'dataflowManage'"
                class="bk-text-button bk-primary"
                :class="{ 'is-disabled': isDelete || data.status !== 'no-start' }"
                :title="data.status !== 'no-start' ? $t('无法删除正在运行的项目') : $t('删除')"
                @click="delectDataFlow(data)">
                {{ $t('删除') }}{{ isDelete }}
              </a>
              <a
                v-if="tableType === 'resultTable'"
                class="bk-text-button bk-primary"
                :class="{ 'is-disabled': isDelete || data.storage === 'hdfs' }"
                :title="$t('查询')"
                @click="check(data)">
                {{ $t('查询') }}
              </a>
              <template v-if="tableType === 'dataflowManage'">
                <a
                  v-if="data.is_processing"
                  class="bk-text-button bk-primary is-disabled"
                  :class="{ 'is-disabled': isDelete }"
                  :title="$t('停止')">
                  {{ $t('执行中') }}
                </a>
                <a
                  v-else-if="data.status == 'no-start'"
                  class="bk-text-button bk-primary"
                  :class="{ 'is-disabled': isDelete }"
                  :title="$t('启动')"
                  @click="activate(data, index)">
                  {{ $t('启动') }}
                </a>
                <a
                  v-else
                  class="bk-text-button bk-primary"
                  :class="{ 'is-disabled': isDelete }"
                  :title="$t('停止')"
                  @click="stop(data, index)">
                  {{ $t('停止') }}
                </a>
              </template>
            </td>
          </tr>
        </template>
        <div v-if="tableList.length === 0 && loaded"
          class="noDdata">
          <img src="../../common/images/no-data.png"
            alt="">
          <p>{{ $t('暂无数据') }}</p>
        </div>
      </tbody>
    </table>
    <div class="table-page">
      <bk-paging :curPage.sync="paging.page"
        :size="'small'"
        :totalPage="paging.totalPage" />
    </div>
  </div>
</template>
<script>
export default {
  props: {
    fields: {
      // 表头
      type: Array,
      required: true,
    },
    tableList: {
      // 内容
      type: Array,
      default() {
        return [];
      },
    },
    paging: {
      // 分页
      type: Object,
      default() {
        return {
          page: 1,
          totalPage: 1,
          type: 'compact',
        };
      },
    },
    tableType: String,
    isDelete: Boolean,
    loaded: Boolean,
  },
  data() {
    return {
      orderName: '',
      is_reverse: 0,
      isExpand: false,
      tableFields: [],
      tableData: [],
      flowId: 0,
    };
  },
  mounted() {
    this.init();
  },
  methods: {
    expandDetail(e) {
      this.isExpand = !this.isExpand;
      if (this.isExpand) {
        e.target.style.wordBreak = 'break-all';
      } else {
        e.target.style.wordBreak = '';
      }
    },
    // 排序
    sorting(title) {
      if (title.sortable) {
        for (var i = 0; i < this.tableFields.length; i++) {
          if (this.tableFields[i].name !== title.name) {
            if (this.tableFields[i].is_reverse === 0) {
              this.tableFields[i].is_reverse = -2;
            } else if (this.tableFields[i].is_reverse === 1) {
              this.tableFields[i].is_reverse = -1;
            }
          }
        }
        if (title.is_reverse === 0) {
          title.is_reverse = 1;
        } else if (title.is_reverse === -2) {
          title.is_reverse = 0;
        } else if (title.is_reverse === 1) {
          title.is_reverse = 0;
        } else {
          title.is_reverse = 1;
        }
        this.$emit('sorting', title);
      }
    },
    // 停止流程
    stop(data, index) {
      if (this.isDelete) {
        return;
      }
      this.$emit('stop', data, index);
    },
    // 启动流程
    activate(data, index) {
      if (this.isDelete) {
        return;
      }
      this.$emit('activate', data, index);
    },
    check(data) {
      if (this.isDelete || data.storage === 'hdfs') {
        return;
      }
      this.$emit('checkResult', data);
    },
    // 编辑
    edit(data) {
      if (this.isDelete) {
        return;
      }
      this.$emit('editDataFlow', data);
    },
    // 删除
    delectDataFlow(data) {
      if (data.status !== 'no-start' || this.isDelete) {
        return;
      }
      this.$emit('deleteDataFlow', data);
    },
    details(data) {
      if (this.isDelete) {
        return;
      }
      this.$emit('detail', data);
    },
    init() {
      this.tableData = this.data;
      this.tableFields = [];
      let obj;
      this.fields.forEach((field, i) => {
        if (typeof field === 'string') {
          obj = {
            name: field,
            title: field,
            titleClass: '',
            callback: '',
            visible: '',
          };
        } else {
          obj = {
            name: field.name,
            title: field.title,
            subtitle: field.subtitle,
            width: field.width,
            sortable: field.sortable ? field.sortable : false,
            is_reverse: field.is_reverse,
          };
        }
        this.tableFields.push(obj);
      });
    },
  },
};
</script>
<style lang="scss" media="screen">
$data-success: #60a72b;
$data-fail: #fe771d;
$textColor: #737987;
.flow-table {
  .dataflow-table {
    table-layout: fixed;
    font-size: 14px;
    width: 100%;
    border-top: 1px solid #eaeaea;
    border-bottom: 1px solid #eaeaea;
    &.list-none {
      min-height: 255px;
      border-bottom: none;
    }
    thead {
      .flow_id {
        display: none;
      }
      th {
        height: 50px;
        padding-left: 20px;
        font-weight: normal;
        text-align: left;
        color: $textColor;
        &:first-of-type {
          padding-left: 75px;
        }
        &.status {
          padding-left: 75px;
        }
        i {
          float: right;
          margin-right: 15px;
        }
        border-right: 1px solid #eaeaea;
        border-bottom: 1px solid #eaeaea;
      }
      .click {
        cursor: pointer;
        &:hover {
          background: #fafafa;
        }
      }
      tr {
        height: 50px;
      }
    }
    tbody {
      height: 60px;
      position: relative;
      .pic,
      .biz_id,
      .data_src_item,
      .op_platform,
      .is_reverse,
      .is_processing,
      .storage,
      .has_exception,
      .flow_id {
        display: none;
      }
      td {
        text-overflow: ellipsis;
        overflow: hidden;
        height: 60px;
        padding-left: 20px;
        color: #444444;
        overflow: hidden;
        text-overflow: ellipsis;
        .task-status-run {
          color: #9dcb6b;
        }
        .status-exception {
          color: #fe771d;
        }
        &.status {
          padding-left: 75px;
        }
        &.result_table {
          &:hover {
            color: #3a84ff;
            cursor: pointer;
          }
        }
        &.data_set {
          &:hover {
            color: #3a84ff;
            cursor: pointer;
          }
        }
        img {
          width: 15px;
          height: 15px;
        }
        .no-data {
          color: #c3cdd7;
        }
        .icon-flow {
          font-size: 16px;
        }
        .user_photo {
          display: inline-block;
          width: 32px;
          height: 32px;
          border-radius: 50%;
          text-align: center;
          background: #fff;
          border: 1px solid #ceced0;
          vertical-align: middle;
          img {
            position: relative;
            vertical-align: -webkit-baseline-middle;
          }
        }
        .user_name {
          vertical-align: middle;
          width: calc(100% - 37px);
          display: inline-block;
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;
        }
        &:first-of-type {
          padding-left: 75px;
        }
        &:last-of-type {
          border-left: 1px solid #eaeaea;
        }
        .data-edit {
          padding-left: 33px;
        }
        &.name {
          cursor: pointer;
          &:hover {
            color: #3a84ff;
          }
        }
        &.biz_id {
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;
        }
        .yes {
          color: $data-success;
        }
        .no {
          color: $data-fail;
        }
        .bk-text-button.bk-primary {
          padding: 20px 0;
        }
      }
      tr {
        height: 60px;
        &:nth-child(odd) {
          background: #f2f4f9;
        }
        &:hover {
          background: #e8e9ec;
        }
      }
      .bk-text-button.is-disabled {
        color: #babddd !important;
      }
      .noDdata {
        text-align: center;
        position: absolute;
        left: 50%;
        top: 50%;
        transform: translate(-50%, -50%);
        p {
          color: #cfd3dd;
        }
      }
    }
    .nodes-num {
      display: inline-block;
      width: 50px;
      height: 26px;
      line-height: 26px;
      text-align: center;
      border: 1px solid #d9dbe0;
      border-radius: 2px;
    }
    .founder {
      margin-right: 5px;
      display: inline-block;
      vertical-align: middle;
    }
  }
  .table-page {
    margin: 30px 75px;
    text-align: right;
  }
}
</style>
