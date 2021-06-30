

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
  <div class="storage-container">
    <div class="options-bar">
      <div class="type">
        <span>{{ $t('存储列表') }}</span>
      </div>
      <bkdata-button :theme="'primary'"
        :disabled="!$modules.isActive('lifecycle')"
        :title="$t('数据迁移')"
        @click="isShow = true">
        {{ $t('数据迁移') }}
      </bkdata-button>
    </div>
    <div v-bkloading="{ isLoading: isLoading }"
      class="storage-tabel">
      <bkdata-table :pagination="calcPagination"
        :data="tableList"
        @page-change="handlePageChange"
        @page-limit-change="handlePageLimitChange">
        <bkdata-table-column :width="200"
          :label="$t('存储类型')"
          prop="cluster_type" />
        <bkdata-table-column :minWidth="200"
          :label="$t('存储集群')"
          prop="storage_cluster.cluster_name" />
        <bkdata-table-column :width="200"
          :label="$t('状态')">
          <template slot-scope="{ row }">
            {{ row.active ? $t('在用') : $t('不再用') }}
          </template>
        </bkdata-table-column>
        <bkdata-table-column :minWidth="200"
          :label="$t('过期时间')"
          prop="expires">
          <template slot-scope="{ row }">
            {{ parseInt(row.expires) === -1 ? $t('永久保存') : `${parseInt(row.expires)}${$t('天')}` }}
          </template>
        </bkdata-table-column>
        <bkdata-table-column :width="200"
          :label="$t('更新人')"
          prop="updated_by" />
        <bkdata-table-column :minWidth="200"
          :label="$t('更新时间')"
          prop="updated_at" />
      </bkdata-table>
    </div>
    <bkdata-dialog v-model="isShow"
      :hasHeader="false"
      :showFooter="false"
      :closeIcon="true"
      :maskClose="false"
      :width="1000"
      @cancel="refreshRemovalList">
      <DataRemoval v-if="isShow"
        :activePage="'add'"
        @refreshRemovalList="close" />
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
      storageList: [],
      currentPage: 1,
      pageSize: 10,
      isShow: false,
      isAdd: false,
    };
  },
  computed: {
    calcPagination() {
      return {
        current: Number(this.currentPage),
        count: this.storageList.length,
        limit: this.pageSize,
      };
    },
    tableList() {
      return this.storageList.slice((this.currentPage - 1) * this.pageSize, this.currentPage * this.pageSize);
    },
  },
  mounted() {
    this.getDictionaryStorageList();
  },
  methods: {
    close() {
      this.isAdd = true;
      this.$emit('refreshRemovalList');
      this.isShow = false;
    },
    refreshRemovalList() {
      if (this.isAdd) {
        this.$emit('refreshRemovalList');
      }
    },
    getDictionaryStorageList() {
      this.isLoading = true;
      const id = this.$route.query.dataType === 'result_table'
        ? this.$route.query.result_table_id
        : this.$route.query.data_id;
      this.bkRequest
        .httpRequest('dataDict/getDictionaryStorageList', { params: { id: id } })
        .then(res => {
          if (res.result) {
            if (!Object.keys(res.data).length) return;
            this.storageList = [];
            Object.keys(res.data.storages).forEach(item => {
              res.data.storages[item].cluster_type = item;
              this.storageList.push(res.data.storages[item]);
            });
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.isLoading = false;
        });
    },
    handlePageChange(page) {
      this.currentPage = page;
    },
    handlePageLimitChange(pageSize) {
      this.pageSize = pageSize;
    },
  },
};
</script>
<style lang="scss" scoped>
.storage-container {
  margin-bottom: 5px;
}
.storage-tabel {
  padding: 0 15px;
}
.options-bar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding-right: 15px;
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
}
</style>
