

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
  <div class="data-clean-list">
    <div class="type">
      <span>{{ $t('清洗列表') }}</span>
      <bkdata-button class="add-data-btn fr"
        :disabled="isAddClean"
        size="small"
        theme="primary"
        @click="addClean">
        <i class="bk-icon icon-plus" /> {{ $t('添加清洗') }}
      </bkdata-button>
    </div>
    <!--整体状态预览-->
    <!-- <taskStatusOverview :details="details" :accessSummaryStatus="accessSummaryStatus" /> -->

    <bkdata-dialog
      v-model="showStopDialog"
      extCls="bkdata-dialog"
      :theme="stopTaskDialogSetting.theme"
      :maskClose="false"
      :okText="$t('确定')"
      :cancelText="$t('取消')"
      :width="stopTaskDialogSetting.width"
      :title="stopTaskDialogSetting.title"
      @confirm="stop"
      @cancel="cancelFn">
      <p class="clean-list-stop">
        {{ $t('停止清洗任务描述') }}
      </p>
    </bkdata-dialog>
    <div v-bkloading="{ isLoading: loading }"
      class="clean-list-table bk-scroll-x">
      <bkdata-table
        ref="dataidTable"
        :emptyText="$t('暂无数据')"
        :data="listView"
        :pagination="pagination"
        @page-change="handlePageChange"
        @page-limit-change="handlePageLimitChange">
        <bkdata-table-column width="265"
          :label="$t('清洗任务名称')">
          <div slot-scope="item"
            class="bk-table-inlineblock">
            <bkdata-popover placement="right">
              <div class="item-white-space text-overflow">
                {{ item.row.clean_config_name }}
              </div>
              <div slot="content">
                <span style="white-space: pre-wrap; display: inline-block">{{ item.row.clean_config_name }}</span>
              </div>
            </bkdata-popover>
          </div>
        </bkdata-table-column>
        <bkdata-table-column width="285"
          :label="$t('数据输出')"
          prop="output">
          <div slot-scope="item"
            class="bk-table-inlineblock">
            <bkdata-popover placement="left">
              <div class="item-white-space text-overflow">
                {{ item.row.output }}
              </div>
              <div slot="content">
                <span style="white-space: pre-wrap; display: inline-block">
                  <div class="clean-data-tooltip"
                    style="min-width: 250px; padding: 10px; color: #fff">
                    <span>{{ $t('数据输出') }}：</span>
                    <p>{{ item.row.processing_id }}</p>
                    <span>{{ $t('输出中文名') }}：</span>
                    <p>{{ item.row.clean_result_table_name_alias }}</p>
                    <span>{{ $t('输出英文名') }}：</span>
                    <p>{{ item.row.clean_result_table_name }}</p>
                  </div>
                </span>
              </div>
            </bkdata-popover>
          </div>
        </bkdata-table-column>
        <bkdata-table-column :label="$t('创建时间')"
          width="150"
          prop="created_at" />
        <bkdata-table-column width="100"
          :label="$t('创建者')">
          <div slot-scope="item"
            :title="item.row.created_by"
            class="text-overflow">
            {{ item.row.created_by }}
          </div>
        </bkdata-table-column>
        <bkdata-table-column :label="$t('运行状态')"
          minWidth="180"
          prop="status_display">
          <div slot-scope="item"
            class="alarm-count-container">
            <span :class="`data-status ${item.row.status}`">{{ item.row.status_display }}</span>
            <div
              v-if="item.row.alertCount"
              :title="`${$t('告警')}：${item.row.alertCount}`"
              class="alarm-count text-overflow"
              :class="{ 'danger-status': item.row.dangerNum }">
              {{ $t('告警') }}：{{ item.row.alertCount }}
            </div>
          </div>
        </bkdata-table-column>
        <bkdata-table-column prop="detail"
          :label="$t('操作')"
          width="220">
          <div slot-scope="item"
            v-bkloading="{ isLoading: item.row.isLoading }"
            class="bk-table-inlineblock">
            <span class="edit-preview">
              <a
                :class="['mr5', !item.row.canStart ? 'disabled' : '']"
                href="javascript:;"
                @click="start(item.row, item.row.canStart)">
                {{ $t('启动') }}
              </a>
              <a
                :class="[!item.row.canStop ? 'disabled' : '', 'mr5']"
                href="javascript:;"
                @click="tryStop(item.row, item.row.canStop)">
                {{ $t('停止') }}
              </a>
              <a class="preview"
                href="javascript:;"
                @click="detail(item.row)">
                {{ $t('详情') }}
              </a>
              <span class="separator">|</span>
              <a href="javascript:;"
                @click="edit(item.row)">
                {{ $t('编辑') }}
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
      <bkdata-dialog
        v-model="isDeleteShow"
        :extCls="'bkdata-dialog data-manage-dialog'"
        :title="$t('请确认是否删除清洗任务')"
        :theme="'danger'"
        :loading="isDeleteLoading"
        :hasHeader="false"
        :closeIcon="true"
        :hasFooter="false"
        :maskClose="false"
        @confirm="confirmDelete" />
    </div>
  </div>
</template>

<script>
import { postMethodWarning } from '@/common/js/util.js';
import { mapState } from 'vuex';
// import taskStatusOverview from '@/pages/dataManage/datadetailChildren/components/taskStatusOverview'

export default {
  components: {
    // taskStatusOverview
  },
  props: {
    details: {
      type: Object,
      default() {
        return {
          scopes: [],
        };
      },
    },
    isAddClean: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      stopitem: {},
      stopActive: true,
      showStopDialog: false,
      stopTaskDialogSetting: {
        width: '422px',
        title: window.$t('请确认是否停止清洗任务'),
        content: window.$t('停止清洗任务描述'),
        theme: 'danger',
      },
      loading: false,
      cleanList: [],
      page: 1,
      pageCount: 10, // 每一页数据量设置
      tableDataTotal: 1,
      deleteData: null,
      isDeleteShow: false,
      isDeleteLoading: false,
    };
  },
  computed: {
    ...mapState({
      alertData: state => state.accessDetail.alertData,
      accessSummaryStatus: state => state.accessDetail.accessSummaryStatus,
    }),
    cleanAllList() {
      // eslint-disable-next-line vue/no-side-effects-in-computed-properties
      this.tableDataTotal = this.cleanList.length;
      return this.cleanList.map(item => {
        item.canStart = item.status === 'stopped' || item.status === 'failed';
        item.canStop = item.status === 'started';

        return item;
      });
    },
    listView() {
      const startIndex = (this.page - 1) * this.pageCount;
      const endIndex = startIndex + this.pageCount;
      const resultArr = this.cleanAllList.slice(startIndex, endIndex);
      resultArr.forEach(i => {
        i.output = `${i.clean_result_table_name_alias}(${i.clean_result_table_name})`;
      });
      return resultArr;
    },
    pagination() {
      return {
        count: this.tableDataTotal,
        limit: this.pageCount,
        current: this.page || 1,
      };
    },
  },
  mounted() {
    this.init();
  },
  methods: {
    showDelete(data) {
      this.deleteData = data;
      this.isDeleteShow = true;
    },
    confirmDelete() {
      this.isDeleteLoading = true;
      this.bkRequest
        .httpRequest('dataAccess/deleteCleanTask', {
          params: {
            processing_id: this.deleteData.processing_id,
            raw_processing_id: this.deleteData.raw_data_id,
            bk_app_code: this.details.bk_app_code,
          },
        })
        .then(res => {
          if (res.result) {
            this.getDataCleanList();
          } else {
            this.getMethodWarning(res.message, res.code);
          }
          this.isDeleteLoading = false;
          this.isDeleteShow = false;
        });
    },
    handlePageChange(page) {
      this.page = page;
    },
    handlePageLimitChange(pageSize) {
      this.page = 1;
      this.pageCount = pageSize;
    },
    cancelFn() {
      this.showStopDialog = false;
    },
    tryStop(item, active) {
      if (!item.canStop) return;
      this.$set(this, 'stopitem', item);
      this.stopActive = active;
      this.showStopDialog = true;
    },
    init() {
      this.getDataCleanList();
    },

    /**
     * 获取数据清洗列表数据
     */
    getDataCleanList() {
      this.loading = true;
      this.cleanList = [];
      this.bkRequest
        .httpRequest('dataClear/getDataCleanList', { query: { raw_data_id: this.$route.params.did } })
        .then(res => {
          if (res.result) {
            this.cleanList = res.data;
            this.cleanList.forEach((element, index) => {
              this.$set(element, 'startLoading', false);
              this.$set(element, 'stopLoading', false);
            });
            this.getAlertCount();
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.loading = false;
        });
      // this.axios.get(`etls/?raw_data_id=${this.$route.params.did}`).then(res => {

      // })
    },

    getAlertCount() {
      const query = {
        alert_config_ids: this.alertData.id,
        dimensions: JSON.stringify({
          data_set_id: this.cleanList.map(item => item.processing_id),
          module: 'clean',
          generate_type: 'user',
        }),
        group: 'data_set_id',
      };
      this.bkRequest.httpRequest('dataAccess/getAlertCount', { query: query }).then(res => {
        if (res.result) {
          console.log(res);
          const groups = Object.keys(res.data.groups);
          if (groups.length) {
            groups.forEach(item => {
              const index = this.cleanList.findIndex(child => child.processing_id === item);
              console.log(index);
              if (index >= 0) {
                this.$set(this.cleanList[index], 'alertCount', res.data.groups[item].alert_count);
                if (res.data.groups[item].alert_levels.danger) {
                  // 严重告警的数量
                  this.$set(this.cleanList[index], 'dangerNum', res.data.groups[item].alert_levels.danger);
                }
              }
            });
          }
        } else {
          this.getMethodWarning(res.message, res.code);
        }
      });
    },

    /**
     * 添加清洗配置
     */
    addClean() {
      this.$router.push({
        name: 'create_clean',
        params: {
          rawDataId: this.$route.params.did,
        },
      });
    },

    /**
     * 点击停止
     */
    stop() {
      if (this.stopActive) {
        // this.stopitem.stopLoading = true
        this.showStopDialog = false;
        this.$set(this.stopitem, 'isLoading', true);
        this.axios
          .post(`etls/${this.stopitem.processing_id}/stop/`)
          .then(res => {
            if (res.result) {
              this.$set(this.stopitem, 'status', res.data.status);
              this.$set(this.stopitem, 'status_display', res.data.status_display);
              // this.stopitem.status = res.data.status
              // this.stopitem.status_display = res.data.status_display
              postMethodWarning(this.$t('停止成功'), 'success');
            } else {
              postMethodWarning(res.message, 'error');
            }
          })
          ['finally'](() => {
            // this.stopitem.stopLoading = false
            this.$set(this.stopitem, 'isLoading', false);
          });
      }
    },

    /**
     * 点击启动
     */
    start(item, active = true) {
      if (active) {
        item.startLoading = true;
        this.$set(item, 'isLoading', true);
        this.axios
          .post(`etls/${item.processing_id}/start/`)
          .then(res => {
            if (res.result) {
              item.status = res.data.status;
              item.status_display = res.data.status_display;
              postMethodWarning(this.$t('启动成功'), 'success');
            } else {
              postMethodWarning(res.message, 'error');
            }
          })
          ['finally'](() => {
            item.startLoading = false;
            this.$set(item, 'isLoading', false);
          });
      }
    },

    /**
     * 编辑
     */
    edit(item) {
      this.$router.push({
        name: 'edit_clean',
        params: {
          rawDataId: this.$route.params.did,
          rtid: item.processing_id,
        },
      });
    },

    /**
     * 详情
     */
    detail(item) {
      item.result_table_id = item.processing_id;
      item.data_type = 'raw_data';
      item.storage_type = 'clean';
      item.storage_cluster = 'clean';
      this.$emit('detail', item);
    },
  },
};
</script>
<style lang="scss" scoped>
.alarm-count-container {
  display: flex;
  justify-content: flex-start;
  align-items: center;
  white-space: nowrap;
  .alarm-count {
    // max-width: 100px;
    background-color: #ff9c01;
    color: white;
    padding: 3px 8px;
    border-radius: 12px;
    margin-left: 20px;
  }
  .danger-status {
    background-color: #ea3636;
  }
}
::v-deep .clean-list-stop {
  padding: 20px;
  line-height: 1.5;
}
::v-deep .clean-data-tooltip {
  padding: 10px;
  color: #fff;

  span {
    font-size: 15px;
    font-weight: normal;
    margin: 0;
    padding: 0;
  }

  p {
    padding: 2px 20px 5px;
    font-size: 14px;
    margin: 0;
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

.data-clean-list {
  .add-data-btn {
    margin-top: 12px;

    .icon-plus {
      color: #fff;
    }
  }

  .clean-list-table {
    padding: 0 15px;
    overflow: auto;
    min-height: 505px;
    ::v-deep .bk-table-inlineblock {
      .bk-tooltip {
        width: 100%;
        .bk-tooltip-ref {
          width: 100%;
        }
      }
    }
    table {
      min-width: 1000px;
      border: 1px solid #e6e6e6;
      table-layout: fixed;
      th {
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
        &:last-child {
          text-align: center;
        }
      }
      tbody {
        tr {
          td {
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
          }
        }
      }

      tr:hover {
        background: #f4f6fb;
      }
    }

    .data-name {
      width: 130px;
      white-space: nowrap;
      text-overflow: ellipsis;
      overflow: hidden;
    }
  }

  .operation {
    color: $primaryColor;

    a {
      display: inline-block;
      color: $primaryColor;

      &.disabled {
        color: #ccc;
        cursor: not-allowed;
      }

      &.clean-edit-fs12 {
        float: inherit;
        font-size: 12px;
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
    &.started {
      color: $succeedColor;
    }

    &.stopped {
      color: $stoppedColor;
    }

    &.failed {
      color: $dangerColor;
    }
  }

  .page {
    text-align: right;
    padding-top: 10px;
  }

  .nowrap-text {
    width: 220px;
    white-space: nowrap;
    text-overflow: ellipsis;
    overflow: hidden;
    display: block;
  }
}
</style>
