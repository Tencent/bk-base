

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
  <div class="data-dict-preview">
    <bkdata-table v-bkloading="{ isLoading: isTableLoading }"
      :data="tableData"
      :stripe="true"
      :emptyText="$t('暂无数据')">
      <template v-if="(isShowPermission && !hasPermission && !isPermissionProcessing) || isPermissionProcessing"
        slot="empty">
        <div v-if="isShowPermission && !hasPermission && !isPermissionProcessing"
          class="data-preview">
          {{ $t('暂无该数据权限') }}，{{ $t('请') }}
          <span @click="applyPermission">{{ $t('申请数据权限') }}</span>
          <i v-if="resultTableInfo.no_pers_reason"
            v-bk-tooltips.top="resultTableInfo.no_pers_reason"
            class="bk-icon note icon-info-circle" />
        </div>
        <div v-if="isPermissionProcessing && !hasPermission"
          class="data-preview">
          {{ $t('数据权限正在申请中') }}
        </div>
      </template>
      <template v-if="res.select_fields_order.length">
        <template v-for="(item, index) in res.select_fields_order">
          <bkdata-table-column :key="index"
            :label="item"
            minWidth="100">
            <div slot-scope="props">
              <span :title="props.row[item]"
                class="searchrt-table-cell">
                {{ props.row[item] }}
              </span>
            </div>
          </bkdata-table-column>
        </template>
      </template>
      <template v-else-if="fieldInfo.length">
        <bkdata-table-column v-for="(item, index) in fieldInfo"
          :key="index"
          :label="item"
          minWidth="100" />
      </template>
    </bkdata-table>
  </div>
</template>
<script>
import Bus from '@/common/js/bus.js';
export default {
  props: {
    hasPermission: {
      type: Boolean,
      default: false,
    },
    isShowPermission: {
      type: Boolean,
      default: false,
    },
    isPermissionProcessing: {
      type: Boolean,
      default: false,
    },
    resultTableInfo: {
      type: Object,
      default: () => {
        {
        }
      },
    },
  },
  data() {
    return {
      isTableLoading: false,
      res: {
        list: [],
        select_fields_order: [],
      },
      pageCount: 10,
      curPage: 1,
      fieldInfo: [],
    };
  },
  computed: {
    pagination() {
      return {
        current: this.curPage,
        count: this.res.list.length,
        limit: this.pageCount,
        limitList: [10, 20, 30, 50, 100],
      };
    },
    tableData() {
      return this.res.list.slice(this.pageCount * (this.curPage - 1), this.pageCount * this.curPage);
    },
  },
  watch: {
    hasPermission: {
      immediate: true,
      handler(val) {
        if (val) {
          this.getDataPreviewInfo();
        }
      },
    },
  },
  mounted() {
    Bus.$on('postFieldInfo', data => {
      this.fieldInfo = data;
    });
  },
  beforeDestroy() {
    Bus.$off('postFieldInfo');
  },
  methods: {
    handlePageChange(page) {
      this.curPage = page;
    },
    handlePageLimitChange(pageSize) {
      this.curPage = 1;
      this.pageCount = pageSize;
    },
    getDataPreviewInfo() {
      this.isTableLoading = true;
      const options = {
        params: {
          result_table_id: this.$route.query.result_table_id,
        },
      };
      this.bkRequest.httpRequest('dataDict/getDataPreviewInfo', options).then(res => {
        if (res.result) {
          if (res.data.list) {
            this.res = res.data;
            if (this.res.list.length) {
              Bus.$emit('GiveNewField', this.res.list[0]); // 拿到第一个数据填入字段信息表的最新字段值列
            } else {
              Bus.$emit('requestFieldInfo'); // 接口无数据时，填入字段信息表格的字段名
            }
          }
        } else {
          this.$emit('requestError', res);
          Bus.$emit('requestFieldInfo'); // 接口无数据时，填入字段信息表格的字段名
        }
        this.isTableLoading = false;
      });
    },
    applyPermission() {
      this.$emit('applyPermission');
    },
  },
};
</script>
<style lang="scss" scoped>
.data-dict-preview {
  .flex-normal {
    display: flex;
    justify-content: space-between;
    align-items: center;
    .icon-info-circle {
      margin-right: 10px;
    }
  }
  .data-preview {
    font-size: 12px;
    font-weight: normal;
    span {
      color: #3a84ff;
      cursor: pointer;
    }
  }
  ::v-deep .bk-table-body-wrapper {
    // height: 30vh;
    overflow-y: auto;
    &::-webkit-scrollbar {
      width: 8px;
      height: 8px;
      background-color: transparent;
    }
    &::-webkit-scrollbar-thumb {
      border-radius: 8px;
      background-color: #a0a0a0;
    }
    .bk-table-row {
      td {
        // height: 27px;
        em {
          color: #000034;
          background: #ff0;
          font-style: normal;
        }
        .searchrt-table-cell {
          width: 100%;
          overflow: hidden;
          display: inline-block;
          white-space: nowrap;
          text-overflow: ellipsis;
        }
      }
    }
  }
}
</style>
