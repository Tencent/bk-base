

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
  <div class="data-storage-container">
    <div class="type">
      <span>{{ $t('迁移列表') }}</span>
      <!-- <bkdata-button @click="addDataRemoval"
                class="add-data-btn fr"
                size="small"
                theme="primary">
                <i class="bk-icon icon-plus"></i> {{$t('创建迁移')}}
            </bkdata-button> -->
    </div>
    <div v-bkloading="{ isLoading: isLoading }"
      class="data-storage-table bk-scroll-x">
      <bkdata-table :emptyText="$t('暂无数据')"
        :data="tableDataList"
        :pagination="pagination"
        @page-change="handlePageChange"
        @page-limit-change="handlePageLimitChange">
        <!-- <bkdata-table-column min-width="180"
                    :label="$t('数据源名称')">
                    <div class="text-overflow" :title="props.row.result_table_id"
                        slot-scope="props">
                        {{props.row.result_table_id}}
                    </div>
                </bkdata-table-column> -->
        <bkdata-table-column width="100"
          :label="$t('源存储')">
          <div slot-scope="props"
            class="text-overflow">
            {{ props.row.source }}
          </div>
        </bkdata-table-column>
        <bkdata-table-column :label="$t('目标存储')"
          width="100">
          <div slot-scope="props"
            class="text-overflow">
            {{ props.row.dest }}
          </div>
        </bkdata-table-column>
        <bkdata-table-column :minWidth="200"
          :label="$t('时间范围')"
          prop="expire_time_alias">
          <div slot-scope="props"
            class="text-overflow">
            {{ `${forMateDate(props.row.start)} - ${forMateDate(props.row.end)}` }}
          </div>
        </bkdata-table-column>
        <bkdata-table-column :minWidth="150"
          :label="$t('创建时间')"
          prop="created_at" />
        <bkdata-table-column :minWidth="150"
          :label="$t('最后一次更新时间')">
          <div slot-scope="props"
            class="text-overflow test-center">
            {{ props.row.updated_at || '' }}
          </div>
        </bkdata-table-column>
        <bkdata-table-column width="115"
          :label="$t('创建者')"
          prop="created_by" />
        <bkdata-table-column width="80"
          :label="$t('运行状态')">
          <div slot-scope="props"
            class="text-overflow">
            {{ status[props.row.status] ? status[props.row.status] : status.running }}
          </div>
        </bkdata-table-column>
        <bkdata-table-column prop="detail"
          :label="$t('操作')"
          width="100">
          <div slot-scope="item"
            v-bkloading="{ isLoading: item.row.isLoading }"
            class="bk-table-inlineblock operation">
            <span class="edit-preview">
              <a class="preview"
                href="javascript:;"
                @click="previewItem(item.row)">
                {{ $t('详情') }}
              </a>
            </span>
          </div>
        </bkdata-table-column>
      </bkdata-table>
    </div>
  </div>
</template>

<script>
export default {
  props: {
    details: {
      type: Object,
      default: () => ({}),
    },
  },
  data() {
    return {
      isLoading: false,
      itemObj: {},
      dataList: [],
      page: 1,
      pageCount: 10, // 每一页数据量设置
      tableDataTotal: 1,
      status: {
        init: this.$t('准备中'),
        running: this.$t('运行中'),
        finish: this.$t('完成'),
      },
    };
  },
  computed: {
    pagination() {
      return {
        count: this.tableDataTotal,
        limit: this.pageCount,
        current: this.page || 1,
      };
    },
    tableDataList() {
      return this.dataList.slice((this.page - 1) * this.pageCount, this.page * this.pageCount);
    },
  },
  mounted() {
    // this.$route.params.did
    this.getDataRemovalList(100453);
  },
  methods: {
    forMateDate(time) {
      let preArr = Array.apply(null, Array(10)).map((elem, index) => {
        return '0' + index;
      });
      let date = new Date(time);
      let year = date.getFullYear();
      let month = date.getMonth() + 1; // 月份是从0开始的
      let day = date.getDate();
      let hour = date.getHours();
      let newTime = year + '-' + (preArr[month] || month) + '-' + (preArr[day] || day) + ' ' + (preArr[hour] || hour);
      return newTime;
    },
    handlePageChange(page) {
      this.page = page;
    },
    handlePageLimitChange(pageSize) {
      this.page = 1;
      this.pageCount = pageSize;
    },
    getDataRemovalList(id) {
      const query = {
        raw_data_id: id,
      };
      this.isLoading = true;
      this.bkRequest
        .httpRequest('dataRemoval/getDataRemovalList', { query })
        .then(res => {
          if (res.result) {
            this.dataList = (res.data || []).sort((a, b) => {
              return new Date(b.created_at) - new Date(a.created_at);
            });
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](_ => {
          this.isLoading = false;
          this.tableDataTotal = this.dataList.length;
        });
    },
    previewItem(item) {
      this.$parent.activeModule = 'preview';
      this.$parent.itemObj = item;
    },
    addDataRemoval() {
      this.$parent.activeModule = 'add';
    },
  },
};
</script>

<style lang="scss">
.data-storage-container {
  .edit {
    float: inherit;
  }
  a.disabled {
    color: #ccc;
    cursor: not-allowed;
  }
}
</style>
<style lang="scss" scoped>
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
::v-deep .layout-body {
  overflow: initial !important;
}
.no_data {
  height: 120px;
  display: flex;
  align-items: center;
  flex-direction: column;
  justify-content: center;
  border: 1px solid #e6e6e6;
  border-top: none;
}

$succeedColor: #30d878;
$dangerColor: #ff5656;
$abnormalColor: #ffb400;
$stoppedColor: #c3cdd7;
$primaryColor: #3a84ff;

.data-storage-container {
  .add-data-btn {
    margin-top: 12px;

    .icon-plus {
      color: #fff;
    }
  }
  .data-storage-table {
    padding: 0 15px;
    overflow-x: auto;
    table {
      border: 1px solid #e6e6e6;
      min-width: 1200px;

      th {
        &:last-child {
          text-align: center;
        }
      }

      tr:hover {
        background: #f4f6fb;
      }
      tr {
        .data-name {
          width: 130px;
          white-space: nowrap;
          text-overflow: ellipsis;
          overflow: hidden;
          display: block;
        }
      }
    }
  }

  .operation {
    color: $primaryColor;

    a {
      display: inline-block;
      color: $primaryColor;

      &.clean-edit-fs12 {
        font-size: 12px;
      }

      &.disabled {
        color: #ccc;
        cursor: not-allowed;
      }
    }

    .edit-preview {
      display: flex;
      align-items: center;
      .separator {
        display: inline-block;
        margin: 0 3px;
      }
    }
  }

  .data-status {
    &.finish,
    &.running,
    &.started {
      color: $succeedColor;
    }
    &.ready {
      color: $abnormalColor;
    }

    &.failed {
      color: $dangerColor;
    }

    &.stopped,
    &.deleted {
      color: $stoppedColor;
    }
  }
}
</style>
