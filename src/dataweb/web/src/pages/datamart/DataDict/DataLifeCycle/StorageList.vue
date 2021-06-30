

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
    <HeaderType :title="$t('存储成本')">
      <div slot="title-right">
        <bkdata-button
          :theme="'primary'"
          :disabled="!$modules.isActive('lifecycle')"
          :title="$t('数据迁移')"
          @click="isShow = true">
          {{ $t('数据迁移') }}
        </bkdata-button>
      </div>
      <div v-bkloading="{ isLoading }">
        <bkdata-table
          :pagination="calcPagination"
          :data="tableList"
          @page-change="handlePageChange"
          @page-limit-change="handlePageLimitChange">
          <bkdata-table-column :width="100"
            :label="$t('存储类型')"
            prop="type" />
          <bkdata-table-column :width="130"
            :label="$t('存储集群')"
            prop="storage_cluster.cluster_name" />
          <template v-if="isShowStorage">
            <bkdata-table-column :width="110"
              headerAlign="right"
              :label="$t('占用存储')"
              :showOverflowTooltip="true">
              <div slot-scope="{ row }"
                class="text-right"
                :title="row.capacity">
                {{ row.capacity }}
              </div>
            </bkdata-table-column>
            <bkdata-table-column :width="20" />
            <bkdata-table-column
              :width="180"
              headerAlign="right"
              :label="$t('最近7日日平均增量（条）')"
              :showOverflowTooltip="true">
              <div slot-scope="{ row }"
                class="text-right"
                :title="row.incre_count">
                {{ row.incre_count }}
              </div>
            </bkdata-table-column>
            <bkdata-table-column :width="20" />
          </template>
          <bkdata-table-column :width="130"
            :label="$t('状态')">
            <span slot-scope="{ row }"
              :class="{ 'bk-text-success': row.active }">
              {{ row.active ? $t('在用') : $t('不在用') }}
            </span>
          </bkdata-table-column>
          <bkdata-table-column :width="120"
            :label="$t('过期时间')"
            prop="expires"
            :showOverflowTooltip="true">
            <template slot-scope="{ row }">
              {{ parseInt(row.expires) === -1 ? $t('永久保存') : `${parseInt(row.expires)}${$t('天')}` }}
            </template>
          </bkdata-table-column>
          <bkdata-table-column :width="130"
            :label="$t('更新人')"
            prop="updated_by"
            :showOverflowTooltip="true" />
          <bkdata-table-column :minWidth="180"
            :label="$t('更新时间')"
            prop="updated_at" />
        </bkdata-table>
      </div>
    </HeaderType>
    <bkdata-dialog
      v-model="isShow"
      :hasHeader="false"
      :showFooter="false"
      :closeIcon="true"
      :maskClose="false"
      :width="1000"
      @cancel="refreshRemovalList">
      <DataRemoval v-show="isShow"
        :activePage="'add'"
        @refreshRemovalList="close" />
    </bkdata-dialog>
  </div>
</template>
<script>
import HeaderType from '@/pages/datamart/common/components/HeaderType';
import DataRemoval from '@/pages/datamart/DataDict/components/children/DataRemoval/index.vue';

export default {
  components: {
    HeaderType,
    DataRemoval,
  },
  data() {
    return {
      isLoading: false,
      storageList: [],
      currentPage: 1,
      pageSize: 10,
      isShow: false,
      isAdd: false,
      isShowStorage: false,
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
    // this.getDictionaryStorageList()
    if (this.$route.query.dataType === 'result_table') {
      this.getLifeCycleStorage();
    }
  },
  methods: {
    getLifeCycleStorage() {
      this.isLoading = true;
      this.bkRequest
        .httpRequest('dataDict/getLifeCycleStorage', {
          query: {
            result_table_id: this.$route.query.result_table_id,
          },
        })
        .then(res => {
          if (res.result && res.data) {
            this.storageList = [];
            this.isShowStorage = true;
            Object.keys(res.data).forEach(child => {
              res.data[child].type = child;
              res.data[child].capacity = ['hdfs', 'tspider'].includes(child) ? res.data[child].format_capacity : '—';
              this.storageList.push(res.data[child]);
            });
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.isLoading = false;
        });
    },
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
.text-right {
  text-align: right;
}
</style>
