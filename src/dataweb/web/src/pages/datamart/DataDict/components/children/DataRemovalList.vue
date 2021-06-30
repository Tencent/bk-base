

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
  <div class="removal-wrap">
    <div class="type">
      <span>{{ $t('变更记录') }}</span>
    </div>
    <div class="removal-container">
      <bkdata-table :data="dataRemovalList"
        :emptyText="$t('暂无数据')"
        @page-change="handlePageChange"
        @page-limit-change="handlePageLimitChange">
        <bkdata-table-column :label="$t('操作类型')"
          prop="cloudAreaName">
          迁移
        </bkdata-table-column>
        <bkdata-table-column :label="$t('状态')">
          <div slot-scope="props"
            class="text-overflow">
            {{ status[props.row.status] ? status[props.row.status] : status.running }}
          </div>
        </bkdata-table-column>
        <bkdata-table-column :label="$t('创建人')"
          prop="created_by" />
        <bkdata-table-column :label="$t('创建时间')"
          prop="created_at" />
        <bkdata-table-column :label="$t('操作')">
          <template slot-scope="item">
            <div class="detail-container">
              <a href="javascript:;"
                @click="previewItem(item.row)">
                {{ $t('详情') }}
              </a>
              <div v-if="item.row.status === 'init'">
                <i class="icon-prompt" />{{ $t('您刚创建的迁移任务在此') }}
              </div>
            </div>
          </template>
        </bkdata-table-column>
      </bkdata-table>
    </div>
    <bkdata-dialog v-model="isShow"
      theme="primary"
      :showFooter="false"
      :width="1000"
      :maskClose="false">
      <DataRemoval v-if="isShow"
        :activePage="activeModule"
        :itemObj="itemObj" />
    </bkdata-dialog>
  </div>
</template>
<script>
export default {
  components: {
    DataRemoval: () => import('@/pages/datamart/DataDict/components/children/DataRemoval/index.vue'),
  },
  data() {
    return {
      isLoading: false,
      dataRemovalList: [],
      itemObj: {},
      activeModule: 'preview',
      isShow: false,
      isShowRemoval: false,
      status: {
        init: this.$t('准备中'),
        running: this.$t('运行中'),
        finish: this.$t('完成'),
      },
    };
  },
  mounted() {
    // this.getDataRemovalList()
  },
  methods: {
    previewItem(data) {
      this.itemObj = data;
      this.isShow = true;
      this.isShowRemoval = true;
    },
    closeDialog() {
      this.isShowRemoval = false;
    },
    handlePageChange() {},
    handlePageLimitChange() {},
    getDataRemovalList() {
      this.isLoading = true;
      const options = {
        query: {
          result_table_id: this.$route.query.result_table_id,
        },
      };
      this.bkRequest
        .httpRequest('dataDict/getDataChangeRecord', options)
        .then(res => {
          if (res.result) {
            this.dataRemovalList = res.data;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.isLoading = false;
        });
    },
  },
};
</script>
<style lang="scss" scoped>
.detail-container {
  display: flex;
  & > a {
    margin-right: 10px;
  }
  .icon-prompt {
    margin-right: 5px;
    font-size: 15px;
    color: #ff9c01;
  }
}
.removal-container {
  padding: 0 15px;
}
.type {
  height: 55px;
  line-height: 55px;
  padding: 0 15px;
  font-weight: 700;
  &::before {
    content: '';
    width: 2px;
    height: 19px;
    background: #3a84ff;
    display: inline-block;
    margin-right: 15px;
    position: relative;
    top: 4px;
  }
}
</style>
