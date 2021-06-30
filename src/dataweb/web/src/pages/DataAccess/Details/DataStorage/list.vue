

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
      <span>{{ $t('入库列表') }}</span>
      <bkdata-button class="add-data-btn fr"
        size="small"
        theme="primary"
        @click="addDataStorage">
        <i class="bk-icon icon-plus" /> {{ $t('添加入库') }}
      </bkdata-button>
    </div>
    <bkdata-dialog
      v-model="showStopDialog"
      extCls="bkdata-dialog"
      :theme="stopTaskDialogSetting.theme"
      :maskClose="false"
      :okText="$t('确定')"
      :cancelText="$t('取消')"
      :width="stopTaskDialogSetting.width"
      :title="stopTaskDialogSetting.title"
      @confirm="taskOption('stop', stopitem, stopActive)"
      @cancel="cancelFn">
      <p class="clean-list-stop">
        {{ stopTaskDialogSetting.content }}
      </p>
    </bkdata-dialog>
    <div v-bkloading="{ isLoading: isLoading }"
      class="data-storage-table bk-scroll-x">
      <bkdata-table
        :emptyText="$t('暂无数据')"
        :data="tableDataList"
        :pagination="pagination"
        @page-change="handlePageChange"
        @page-limit-change="handlePageLimitChange">
        <bkdata-table-column width="150"
          :label="$t('数据源名称')">
          <div slot-scope="item"
            class="bk-table-inlineblock">
            <bkdata-popover placement="left">
              <span class="item-white-space">{{ item.row.data_name }}</span>
              <div slot="content">
                <span style="white-space: pre-wrap; display: inline-block">
                  <div class="clean-data-tooltip">
                    <span>{{ $t('数据源名称') }}：</span>
                    <p>{{ item.row.data_alias }}</p>
                    <span>{{ $t('存储表名称') }}：</span>
                    <p>{{ item.row.result_table_name_alias }}</p>
                    <span>{{ $t('开始时间') }}/{{ $t('创建人') }}：</span>
                    <p>{{ item.row.created_at }} {{ item.row.created_by }}</p>
                  </div>
                </span>
              </div>
            </bkdata-popover>
          </div>
        </bkdata-table-column>
        <bkdata-table-column width="160"
          :label="$t('存储表名称')">
          <div slot-scope="item"
            class="bk-table-inlineblock">
            <bkdata-popover placement="right">
              <span class="item-white-space">{{ item.row.storageTableName }}</span>
              <div slot="content">
                <span style="white-space: pre-wrap; display: inline-block">{{ item.row.storageTableName }}</span>
              </div>
            </bkdata-popover>
          </div>
        </bkdata-table-column>
        <bkdata-table-column :label="$t('存储表类型')"
          width="170">
          <div slot-scope="item"
            :title="item.row.storage_type_alias"
            class="text-overflow">
            {{ item.row.storage_type_alias }}
          </div>
        </bkdata-table-column>
        <bkdata-table-column width="100"
          :label="$t('有效期')"
          prop="expire_time_alias" />
        <bkdata-table-column width="160"
          :label="$t('创建时间')"
          prop="created_at" />
        <bkdata-table-column width="110"
          :label="$t('创建者')"
          prop="created_by" />
        <bkdata-table-column minWidth="125"
          :label="$t('运行状态')"
          prop="status">
          <div
            slot-scope="item"
            class="bk-table-inlineblock alarm-count-container"
            :class="{ 'flex-start': item.row.alertCount }">
            <span :class="`data-status ${item.row.status_en}`">{{ item.row.status }}</span>
            <div
              v-if="item.row.alertCount"
              :title="`${$t('告警')}：${item.row.alertCount}`"
              class="alarm-count text-overflow">
              {{ $t('告警') }}{{ item.row.alertCount }}
            </div>
          </div>
        </bkdata-table-column>
        <bkdata-table-column prop="detail"
          :label="$t('操作')"
          minWidth="225">
          <div slot-scope="item"
            v-bkloading="{ isLoading: item.row.isLoading }"
            class="bk-table-inlineblock operation">
            <span class="edit-preview">
              <a
                v-if="item.row.status_en === 'failed'"
                :class="['mr5']"
                href="javascript:;"
                @click="taskOption('restart', item.row)">
                {{ $t('重试') }}
              </a>
              <a
                v-else
                :class="[
                  'mr5',
                  item.row.status_en === 'stopped' ? '' : 'disabled',
                  item.row.data_type === 'raw_data' ? 'disabled' : '',
                ]"
                href="javascript:;"
                @click="
                  taskOption('start', item.row, item.row.data_type !== 'raw_data' && item.row.status_en === 'stopped')
                ">
                {{ $t('启动') }}
              </a>
              <a
                :class="[
                  ['stopped', 'failed', 'deleted'].includes(item.row.status_en) ? 'disabled' : '',
                  'mr5',
                  item.row.data_type === 'raw_data' ? 'disabled' : '',
                ]"
                href="javascript:;"
                @click="
                  tryStop(
                    item.row,
                    item.row.data_type !== 'raw_data' && !['stopped', 'failed', 'deleted'].includes(item.row.status_en)
                  )
                ">
                {{ $t('停止') }}
              </a>
              <a href="javascript:;"
                @click="editItem(item.row)">
                {{ $t('编辑') }}
              </a>
              <span class="separator">|</span>
              <a class="preview"
                href="javascript:;"
                @click="previewItem(item.row)">
                {{ $t('详情') }}
              </a>
              <span class="separator">|</span>
              <a href="javascript:;"
                @click="showDelete(item.row)">
                {{ $t('删除') }}
              </a>
            </span>
          </div>
        </bkdata-table-column>
      </bkdata-table>
    </div>
    <bkdata-dialog
      v-model="isDeleteShow"
      :title="$t('请确认是否删除入库任务')"
      :theme="'danger'"
      :loading="isDeleteLoading"
      :hasHeader="false"
      :closeIcon="true"
      :hasFooter="false"
      :maskClose="false"
      @confirm="confirmDeleteRawData">
      {{ $t('删除入库任务无法删除实际存储的数据') }}
    </bkdata-dialog>
  </div>
</template>

<script>
import { postMethodWarning } from '@/common/js/util.js';
import optionsMixins from '@/pages/DataAccess/Details/DataStorage/mixin/options.js';
import { mapState } from 'vuex';

export default {
  mixins: [optionsMixins],
  props: {
    details: {
      type: Object,
      default: () => ({}),
    },
  },
  data() {
    return {
      stopitem: {},
      isLoading: false,
      itemObj: {},
      dataList: [],
      rawDataType: {
        raw_data: window.$t('原始数据'),
        clean: window.$t('清洗数据'),
      },
      page: 1,
      pageCount: 10, // 每一页数据量设置
      isDeleteShow: false,
      isDeleteLoading: false,
      deleteData: null,
    };
  },
  computed: {
    pagination() {
      return {
        count: this.dataList.length,
        limit: this.pageCount,
        current: this.page || 1,
      };
    },
    tableDataList() {
      const startIndex = (this.page - 1) * this.pageCount;
      const endIndex = startIndex + this.pageCount;
      return this.dataList.slice(startIndex, endIndex);
    },
    ...mapState({
      alertData: state => state.accessDetail.alertData,
    }),
  },
  mounted() {
    /** 获取入库列表（查询rt以及rt相关配置信息和分发任务信息 */
    this.getDataStorageList(this.$route.params.did);
  },
  methods: {
    confirmDeleteRawData() {
      this.isDeleteLoading = true;
      this.bkRequest
        .httpRequest('dataStorage/deleteStorageConfig', {
          params: {
            rt_id:
              this.deleteData.data_type === 'raw_data' ? this.deleteData.raw_data_id : this.deleteData.result_table_id,
            storage_type: this.deleteData.storage_type,
            data_type: this.deleteData.data_type,
          },
        })
        .then(res => {
          if (res.result) {
            this.getDataStorageList(this.$route.params.did);
            this.isDeleteShow = false;
            this.$bkMessage({
              message: this.$t('入库任务删除成功'),
              theme: 'success',
            });
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](_ => {
          this.isDeleteLoading = false;
        });
    },
    showDelete(data) {
      this.deleteData = data;
      this.isDeleteShow = true;
    },
    getAlertCount() {
      const query = {
        alert_config_ids: this.alertData.id,
        dimensions: JSON.stringify({
          data_set_id: this.dataList.map(item => item.result_table_id),
          module: 'shipper',
          generate_type: 'user',
        }),
        group: 'data_set_id',
      };
      this.bkRequest.httpRequest('dataAccess/getAlertCount', { query: query }).then(res => {
        if (res.result) {
          const groups = Object.keys(res.data.groups);
          if (groups.length) {
            groups.forEach(item => {
              const index = this.dataList.findIndex(child => child.result_table_id === item);
              if (index >= 0) {
                this.$set(this.dataList[index], 'alertCount', res.data.groups[item].alert_count);
              }
            });
          }
        } else {
          this.getMethodWarning(res.message, res.code);
        }
      });
    },
    handlePageChange(page) {
      this.page = page;
    },
    handlePageLimitChange(pageSize) {
      this.page = 1;
      this.pageCount = pageSize;
    },
    getDataStorageList(id) {
      const query = {
        raw_data_id: id,
      };
      this.isLoading = true;
      this.bkRequest
        .httpRequest('dataStorage/getStorageList', { query })
        .then(res => {
          if (res.result) {
            const { data } = res;
            this.dataList = data;
            this.dataList.forEach(i => {
              i.created_by = i.created_by || '-';
              i.storageTableName = i.result_table_name_alias || i.result_table_name;
            });
            this.page = 1;
            this.getAlertCount();
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](_ => {
          this.isLoading = false;
        });
    },
    editItem(item) {
      this.$parent.activeModule = 'edit';
      this.$parent.itemObj = item;
    },
    previewItem(item) {
      this.$parent.activeModule = 'preview';
      this.$parent.itemObj = item;
    },
    addDataStorage() {
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
::v-deep .bk-dialog-wrapper {
  .bk-dialog-body {
    font-size: 15px;
    padding: 3px 24px 26px;
  }
}
.alarm-count-container {
  min-width: 120px;
  display: flex;
  align-items: center;
  white-space: nowrap;
  .alarm-count {
    max-width: 81px;
    background-color: #ff9c01;
    color: white;
    padding: 3px 8px;
    border-radius: 12px;
    margin-left: 5px;
  }
}
.flex-start {
  justify-content: flex-start;
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
      margin-left: 10px;
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
