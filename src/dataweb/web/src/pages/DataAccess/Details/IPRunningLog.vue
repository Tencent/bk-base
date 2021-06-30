

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
  <div class="history">
    <!--eslint-disable-->
    <bkdata-dialog
      v-model="isShow"
      extCls="bkdata-dialog"
      :width="container.width"
      :hasHeader="container.hasHeader"
      :closeIcon="container.closeIcon"
      @cancel="closeFn"
      @confirm="closeFn"
    >
      <div class="history-content">
        <div class="ip">
          {{ $t('运行日志') }}（{{ baseData.ip }}）<i
            :title="$t('关闭')"
            class="bk-icon icon-close fr"
            @click="closeFn"
          />
        </div>
        <div v-bkloading="{ isLoading: loading }" class="running-log-container">
          <bkdata-table
            :data="logListView"
            :emptyText="$t('暂无数据')"
            :pagination="pagination"
            @page-change="handlePageChange"
            @page-limit-change="handlePageLimitChange"
          >
            <bkdata-table-column width="100" prop="bk_username" />
            <bkdata-table-column>
              <div slot-scope="item" class="bk-table-inlineblock">
                <bkdata-popover placement="top">
                  <span class="item-white-space">{{ item.row.log }}</span>
                  <div slot="content">
                    <span style="white-space: pre-wrap; display: inline-block">{{ item.row.log }}</span>
                  </div>
                </bkdata-popover>
              </div>
            </bkdata-table-column>
          </bkdata-table>
        </div>
      </div>
    </bkdata-dialog>
  </div>
</template>
<script>
export default {
  props: {
    isShow: {
      type: Boolean,
      default: false,
    },
    baseData: {
      type: Object,
      default: () => ({
        deploy_plan_id: '',
        bk_cloud_id: '',
        ip: '',
      }),
    },
  },
  data() {
    return {
      container: {
        width: 900,
        height: 500,
        hasHeader: false,
        closeIcon: false,
      },
      loading: false,
      curPage: 1,
      pageCount: 10,
      logList: [],
      tableDataTotal: 1,
    };
  },
  computed: {
    totalPage() {
      return Math.ceil(this.logList.length / this.pageCount) || 1;
    },
    logListView() {
      // eslint-disable-next-line vue/no-side-effects-in-computed-properties
      this.tableDataTotal = this.logList.length;
      return this.logList.slice(this.pageCount * (this.curPage - 1), this.pageCount * this.curPage);
    },
    pagination() {
      return {
        count: this.tableDataTotal,
        limit: this.pageCount,
        current: this.curPage || 1,
      };
    },
  },
  watch: {
    isShow(val) {
      if (val) {
        this.getRunningLog(this.baseData);
      }
    },
  },
  methods: {
    handlePageChange(page) {
      this.curPage = page;
    },
    handlePageLimitChange(pageSize) {
      this.curPage = 1;
      this.pageCount = pageSize;
    },
    getRunningLog(data) {
      this.logList = [];
      this.loading = true;
      this.bkRequest
        .request('v3/access/collector/task_log/', {
          method: 'get',
          useSchema: false,
          query: {
            raw_data_id: this.$route.params.did,
            ip: data.ip,
          },
        })
        .then(res => {
          if (res.result) {
            this.logList = res.data.task_log || [];
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.loading = false;
        });
    },
    closeFn() {
      this.$emit('update:isShow', false);
    },
  },
};
</script>

<style lang="scss" scoped>
.running-log-container {
  width: 100%;
  .bk-table {
    background: #f5f5f5;
    border-left: 1px solid #e6e6e6;
    border-right: 1px solid #e6e6e6;
  }

  .name {
    white-space: nowrap;
  }
}
.no_data {
  height: 120px;
  display: flex;
  align-items: center;
  flex-direction: column;
  justify-content: center;
}
</style>
