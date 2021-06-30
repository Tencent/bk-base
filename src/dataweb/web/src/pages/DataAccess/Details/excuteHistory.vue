

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
          {{ $t('执行历史') }}（{{ ip }}）
          <i :title="$t('关闭')" class="bk-icon icon-close fr" @click="closeFn" />
        </div>

        <bkdata-table
          v-bkloading="{ isLoading: loading }"
          :data="orderData"
          :emptyText="$t('暂无数据')"
          :pagination="pagination"
          :maxHeight="halfWindowHeight"
          @page-change="handlePageChange"
          @page-limit-change="handlePageLimitChange"
        >
          <bkdata-table-column width="200" :label="$t('操作时间')" prop="updated_at" />
          <bkdata-table-column width="100" :label="$t('操作结果')" prop="deploy_status">
            <div slot="deploy_status" slot-scope="{ item }" class="bk-table-inlineblock">
              <span :class="[item.row.deploy_status === 'success' ? 'success' : 'failed']">{{
                item.row.deploy_status
              }}</span>
            </div>
          </bkdata-table-column>
          <bkdata-table-column :label="$t('备注')">
            <div slot-scope="item" class="bk-table-inlineblock">
              <span class="item-white-space">{{ item.row.error_message }}</span>
            </div>
          </bkdata-table-column>
        </bkdata-table>
      </div>
    </bkdata-dialog>
  </div>
</template>
<script>
import { getMethodWarning } from '@/common/js/util';
export default {
  components: {
    // excuteHistory
  },
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
      lists: [],
      ip: '',
      page: 1,
      pageCount: 5, // 每一页数据量设置
      tableDataTotal: 1,
    };
  },
  computed: {
    halfWindowHeight() {
      return document.body.clientHeight * 0.7;
    },
    orderData() {
      let tableList = this.lists.slice(this.pageCount * (this.page - 1), this.pageCount * this.page);
      return tableList.sort((pre, next) => new Date(next.updated_at) - new Date(pre.updated_at));
    },
    pagination() {
      return {
        count: (this.lists || []).length,
        limit: this.pageCount,
        current: this.curPage || 1,
      };
    },
  },
  watch: {
    isShow(val) {
      if (val) {
        this.getExcuteHistory(this.baseData);
      }
    },
  },
  methods: {
    handlePageChange(page) {
      this.page = page;
    },
    handlePageLimitChange(pageSize) {
      this.page = 1;
      this.pageCount = pageSize;
    },
    getExcuteHistory(data) {
      this.lists = [];
      this.ip = data.ip;
      this.loading = true;
      let params = {
        raw_data_id: this.$route.params.did,
      };
      const queryParams = {
        deploy_plan_id: data.deploy_plan_id,
        bk_cloud_id: data.bk_cloud_id,
        ip: data.ip,
      };
      this.$store
        .dispatch('getDeployStatusHistory', { params: params, query: queryParams })
        .then(res => {
          if (res.result) {
            this.lists = res.data;
          } else {
            getMethodWarning(res.message, 'error');
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
<style lang="scss">
.history-content {
  padding: 20px;
  .ip {
    font-size: 15px;
    margin-bottom: 10px;
    font-weight: bold;
    position: relative;
    padding-left: 15px;
    .icon-close {
      cursor: pointer;
    }
    &::after {
      position: absolute;
      content: '';
      left: 0px;
      width: 4px;
      height: 100%;
      background: #3a84ff;
    }
  }
}
.history {
  .bk-dialog-btn.bk-dialog-btn-cancel {
    display: none;
  }
  .success {
    color: #9dcb6b;
  }
  .failed {
    color: #fe771d;
  }
}
</style>
