

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
  <bkdata-dialog v-model="dialog.dialogStatus"
    :okText="dialog.confirm"
    :cancelText="dialog.cancel"
    :closeIcon="false"
    :extCls="`dialog.hideCancel bkdata-dialog`"
    :hasHeader="false"
    :theme="dialog.dialogTheme"
    width="890"
    @confirm="confirmFn"
    @cancel="closeDialog">
    <div slot="content">
      <div>
        <a class="close-pop"
          href="javascript:;"
          @click="closeDialog">
          <i :title="$t('关闭')"
            class="bk-icon icon-close" />
        </a>
        <p class="title">
          {{ $t('接入历史') }}({{ historyId }})
        </p>
        <div v-bkloading="{ isLoading: loading.historyLoading }"
          class="table">
          <table class="bk-table has-table-bordered has-table-hover">
            <thead>
              <tr>
                <th>
                  {{ $t('操作时间') }}
                  <i class="sorting" />
                </th>
                <th>{{ $t('操作人员') }}</th>
                <th>{{ $t('操作类型') }}</th>
                <th>{{ $t('操作结果') }}</th>
                <th />
              </tr>
            </thead>
            <tbody>
              <template v-for="(item, index) in historyList">
                <tr :key="index + '_1'">
                  <td>{{ item.deploy_record__created_at }}</td>
                  <td class="member">
                    {{ item.deploy_record__created_by }}
                  </td>
                  <td v-if="item.deploy_record__operate_type === 'DEPLOY'"
                    class="success">
                    {{ $t('接入') }}
                  </td>
                  <td v-else
                    class="failure">
                    {{ $t('删除') }}
                  </td>
                  <td v-if="item.result === 'wating'"
                    class="doing">
                    {{ $t('进行中') }}
                  </td>
                  <td v-else-if="item.result === 'success'"
                    class="success">
                    {{ $t('成功') }}
                  </td>
                  <td v-else
                    class="failure">
                    <span :title="item.errormsg">{{ $t('失败') }}</span>
                  </td>
                  <td @click="unfolded(item)">
                    <i v-if="item.result === 'failed'"
                      class="bk-icon icon-angle-down" />
                  </td>
                </tr>
                <tr v-show="item.row_expanded"
                  :key="index + '_2'">
                  <td class="errormsg"
                    colspan="5">
                    <div class="expand-wrapper">
                      {{ item.errormsg }}
                    </div>
                  </td>
                </tr>
              </template>
            </tbody>
          </table>
          <div class="table-page">
            <bkdata-pagination :current.sync="dialog.page"
              :showLimit="false"
              :size="'small'"
              :count="dialog.totalPage"
              :limit="dialog.page_size" />
          </div>
        </div>
      </div>
    </div>
  </bkdata-dialog>
</template>

<script>
export default {
  props: {
    loading: {
      type: Object,
      default: () => ({})
    },
  },
  data() {
    return {
      dialog: {
        // 弹窗控制参数
        dialogStatus: false,
        dialogTheme: 'primary',
        confirm: this.$t('关闭'),
        hideCancel: 'dialog-pop hideCancel access-history',
        cancel: '',
        page: 1,
        page_size: 10,
        totalPage: 1,
      },
      historyId: 0,
      historyList: [], // 执行历史
    };
  },
  watch: {
    /*
                监听弹窗分页
            */
    'dialog.page': function (newVal, oldVal) {
      this.dialog.page = newVal;
      this.getHistoryList();
    },
  },
  methods: {
    // 点击展开
    unfolded(item) {
      if (item.result === 'failed') {
        item.row_expanded = !item.row_expanded;
      }
    },
    closeDialog() {
      this.dialog.dialogStatus = false;
    },
    confirmFn() {
      this.dialog.dialogStatus = false;
    },
    /*
                获取接入历史数据
            */
    getHistoryList() {
      let params = {
        ip: this.historyId,
        page: this.dialog.page,
        page_size: this.dialog.page_size,
      };
      let url = `dataids/${this.$route.params.did}/get_exc_history/?${this.qs.stringify(params)}`;
      this.axios.get(url).then(res => {
        if (res.result) {
          this.historyList = res.data.results;
          for (var i = this.historyList.length - 1; i >= 0; i--) {
            // .row_expanded = false
            this.$set(this.historyList[i], 'row_expanded', false);
          }
          let totalPage = res.data.count;
          // 总页数不能为0
          this.dialog.totalPage = totalPage !== 0 ? totalPage : 1;
        } else {
          this.getMethodWarning(res.message, res.code);
        }
        this.loading.historyLoading = false;
      });
    },
  }
};
</script>

<style media="screen" lang="scss">
$bk-primary: #3a84ff;
$text-color: #212232;
$bg-color: #fff;
$data-success: #9dcb6b;
$data-doing: #fe771d;
$data-fail: #ff5656;
$data-nostart: #dedede;
.access-history {
    .table {
      width: 100%;
      text-align: left;
      margin-top: 25px;
      .sorting {
        float: right;
        width: 14px;
        height: 14px;
        background: url('../../../common/images/icon/icon-sort.png');
        cursor: pointer;
        &:hover {
          opacity: 0.8;
        }
      }
      .bk-table > thead > tr > th {
        font-weight: normal;
        color: #212232;
      }
      .bk-table > tbody > tr > td {
        font-size: 14px;
        color: #212232;
        &.success {
          color: $data-success;
        }
        &.doing {
          color: $data-doing;
        }
        &.failure {
          color: $data-fail;
        }
        &.member {
          color: #3a84ff;
        }
        &.errormsg {
          padding: 0;
        }
      }
    }
    .table-page {
      text-align: right;
      padding: 20px;
      border: 1px solid #e6e6e6;
      border-top: none;
    }
    .bk-dialog-outer button {
      margin-top: 0;
    }
    .dialog-pop .bk-dialog-footer,
    .bk-dialog-footer {
      height: 80px;
      line-height: 60px;
    }
    .dialog-pop .close-pop {
      right: -13px;
      top: -14px;
    }
    .expand-wrapper {
      height: 100px;
      padding: 20px;
      overflow-y: auto;
      background-color: #f4f6fb;
    }
  }
  .el-date-editor .el-range__close-icon {
    right: 0;
  }
</style>
