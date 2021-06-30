

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
  <Layout :showHead="false"
    :showSubNav="true">
    <template slot="subNav">
      <AuthNav :activeName="'Todos'" />
    </template>

    <div class="application-record">
      <SearchLayout>
        <template slot="searchTitle">
          <div class="search-box mt20">
            <div class="search-raw">
              <div class="search-item">
                <label class="search-label">{{ $t('申请状态') }}：</label>
                <div class="search-content">
                  <bkdata-selector
                    :selected.sync="search.status.value"
                    :placeholder="$t('请选择')"
                    :list="statusList"
                    :settingKey="'id'"
                    :displayKey="'name'"
                    :allowClear="true"
                    @item-selected="paramsChange" />
                </div>
              </div>
              <div class="search-item">
                <label class="search-label">{{ $t('申请类型') }}：</label>
                <div class="search-content">
                  <bkdata-selector
                    :selected.sync="search.ticketType.value"
                    :list="ticketTypeList"
                    :placeholder="$t('请选择')"
                    :settingKey="'id'"
                    :displayKey="'name'"
                    :allowClear="true"
                    @item-selected="paramsChange" />
                </div>
              </div>
            </div>
          </div>
        </template>

        <template slot="searchMain">
          <div v-bkloading="{ isLoading: isLoading }"
            class="table-box mt20">
            <bkdata-table
              :data="tableSearchList"
              :stripe="true"
              :emptyText="$t('暂无数据')"
              :pagination="calcPagination"
              @page-change="handlePageChange"
              @page-limit-change="handlePageLimitChange">
              <bkdata-table-column width="130"
                :label="$t('申请人')">
                <template slot-scope="item">
                  {{ item.row.ticket.created_by }}
                </template>
              </bkdata-table-column>
              <bkdata-table-column width="180"
                :label="$t('申请时间')">
                <template slot-scope="item">
                  {{ item.row.ticket.created_at }}
                </template>
              </bkdata-table-column>
              <bkdata-table-column width="180"
                :label="$t('申请类型')"
                prop="ticket_type_display">
                <template slot-scope="item">
                  {{ item.row.ticket.ticket_type_display }}
                </template>
              </bkdata-table-column>
              <bkdata-table-column width="180"
                :label="$t('申请状态')"
                prop="status_display ">
                <div slot-scope="item"
                  class="bk-table-inlineblock">
                  <span :class="[statusInfos[item.row.status].class]">
                    {{ item.row.status_display }}
                  </span>
                </div>
              </bkdata-table-column>
              <bkdata-table-column width="140"
                :label="$t('当前步骤')"
                prop="step">
                <div slot-scope="item"
                  class="bk-table-inlineblock">
                  <span class="bk-primary"> {{ item.row.process_step + 1 }} </span>/<span class="bk-primary">
                    {{ item.row.ticket.process_length }}
                  </span>
                </div>
              </bkdata-table-column>
              <bkdata-table-column :label="$t('当前处理人')"
                prop="processors">
                <div slot-scope="item">
                  <div
                    v-if="item.row.processors.length > 0"
                    v-tooltip="item.row.processors.join(', ')"
                    class="bk-primary hiding">
                    {{ item.row.processors.join(',') }}
                  </div>
                  <div v-else>
                    -
                  </div>
                </div>
              </bkdata-table-column>
              <bkdata-table-column width="240"
                :label="$t('操作')"
                prop="processors">
                <div slot-scope="item"
                  class="bk-table-inlineblock">
                  <span class="bk-text-button"
                    @click="detail(item.row)">
                    {{ $t('查看') }}
                  </span>

                  <OperateBtn
                    v-if="statusInfos[item.row.status].actions.indexOf('agree') !== -1"
                    :id="item.row.id"
                    :hasMessage="true"
                    :defaultValue="$t('同意')"
                    @confirm="agree">
                    {{ $t('同意') }}
                  </OperateBtn>

                  <OperateBtn
                    v-if="statusInfos[item.row.status].actions.indexOf('reject') !== -1"
                    :id="item.row.id"
                    :hasMessage="true"
                    :defaultValue="$t('驳回')"
                    @confirm="reject">
                    {{ $t('驳回') }}
                  </OperateBtn>
                </div>
              </bkdata-table-column>
            </bkdata-table>
          </div>
        </template>
      </SearchLayout>
      <bkdata-sideslider :quickClose="true"
        :isShow.sync="sideslider.isShow"
        :width="560"
        :title="$t('单据详情')">
        <template slot="content">
          <template v-if="sideslider.stateId !== null && sideslider.isShow">
            <TicketDetail :stateId="sideslider.stateId"
              :statusInfos="statusInfos" />
          </template>
        </template>
      </bkdata-sideslider>
    </div>
  </Layout>
</template>

<script>
import Layout from '@/components/global/layout';
import { submitApprove, ticketState, ticketStatus, ticketTypes } from '@/common/api/auth';
import { showMsg } from '@/common/js/util.js';

import { AuthNav, BKDataAlert, SearchLayout } from '../parts/index.js';
import OperateBtn from './components/OperateBtn';
import TicketDetail from './Detail';
import { mapGetters } from 'vuex';

export default {
  components: {
    Layout,
    AuthNav,
    SearchLayout,
    TicketDetail,
    OperateBtn,
  },
  data() {
    return {
      isLoading: false,
      search: {
        status: {
          value: 'processing',
        },
        ticketType: {
          value: '',
        },
      },
      statusList: [],
      ticketTypeList: [],
      pageCount: 0,
      sideslider: {
        isShow: false,
        stateId: null,
      },

      applicationList: [],
      pageSize: 10,

      statusInfos: {
        pending: {
          id: 'pending',
          name: '待处理',
          location: 'head',
          class: 'label-primary',
          actions: [],
        },
        processing: {
          id: 'processing',
          name: '处理中',
          location: 'middle',
          class: 'label-primary',
          actions: ['agree', 'reject'],
        },
        succeeded: {
          id: 'succeeded',
          name: '已同意',
          location: 'tail',
          class: 'label-success',
          actions: [],
        },
        failed: {
          id: 'failed',
          name: '已驳回',
          location: 'tail',
          class: 'label-danger',
          actions: [],
        },
        stopped: {
          id: 'stopped',
          name: '已终止',
          location: 'stopped',
          class: 'label-danger',
          actions: [],
        },
      },
    };
  },
  computed: {
    ...mapGetters({
      todoCount: 'auth/getTodoCount',
    }),
    tableSearchList() {
      return this.applicationList;
    },
    calcPagination() {
      return {
        current: Number(this.currentPage),
        count: this.pageCount,
        limit: this.calcPageSize,
      };
    },
    currentPage() {
      return this.$route.query.page || 1;
    },
    calcPageSize() {
      return this.$route.query.pageSize || this.pageSize;
    },
  },
  watch: {
    search: {
      handler: function (newVal) {
        this.$nextTick(() => {
          this.loadApplicationList();
        });
      },
      deep: true,
    },
  },

  mounted() {
    this.init();
  },
  methods: {
    paramsChange() {
      this.$router.push({
        query: Object.assign({}, this.$route.query, { page: 1 }),
      });
      this.pageCurrent = 1;
    },
    handlePageChange(page) {
      this.$router.push({
        query: Object.assign({}, this.$route.query, { page: page }),
      });
      this.pageCurrent = page;
      this.loadApplicationList();
    },
    handlePageLimitChange(pageSize, preLimit) {
      this.pageSize = pageSize;
      this.$router.push({
        query: Object.assign({}, this.$route.query, { pageSize: pageSize, page: 1 }),
      });
      this.loadApplicationList();
    },

    detail(item) {
      this.sideslider.stateId = item.id;
      this.sideslider.isShow = true;
    },
    agree(params) {
      params.status = this.statusInfos.succeeded.id;
      this.approve(params);
    },
    reject(params) {
      params.status = this.statusInfos.failed.id;
      this.approve(params);
    },
    approve(params) {
      showMsg(this.$t('审批中') + '...', 'success');
      submitApprove(params.id, {
        process_message: params.message,
        status: params.status,
      }).then(res => {
        if (res.result) {
          showMsg(this.$t('提交成功'), 'success');
          for (let application of this.applicationList) {
            if (application.id === params.id) {
              application.status = res.data.status;
              application.status_display = res.data.status_display;
            }
          }
          this.applicationList = this.applicationList.splice(0, this.applicationList.length);

          // 处理一个单据后，待办数量减一
          this.$store.dispatch('auth/actionSetTodoCount', this.todoCount - 1);
        } else {
          showMsg(res.message, 'error');
        }
      });
    },
    init() {
      this.loadTicketStatus();
      this.loadTicketType();

      this.loadApplicationList();
    },
    loadApplicationList() {
      this.isLoading = true;
      // 根据不同页面，传入参数有所调整
      let queryParam = {
        ticket__ticket_type: this.search.ticketType.value,
        status: this.search.status.value,
        is_processor: true,
        page: this.currentPage,
        page_size: this.pageSize,
      };

      ticketState(queryParam).then(res => {
        if (res.result) {
          this.applicationList = res.data.results;
          this.pageCount = res.data.count;
        } else {
          showMsg(res.message, 'error');
        }
        this.isLoading = false;
      });
    },
    loadTicketType() {
      ticketTypes().then(res => {
        if (res.result) {
          this.ticketTypeList = res.data;
        } else {
          showMsg(res.message, 'error');
        }
      });
    },
    loadTicketStatus() {
      ticketStatus().then(res => {
        if (res.result) {
          this.statusList = res.data;
        } else {
          showMsg(res.message, 'error');
        }
      });
    },
  },
};
</script>

<style lang="scss">
@import '~@/common/scss/conf.scss';
@import '../scss/base.scss';

.application-record {
  .list-table {
    overflow: inherit;
  }
  .table-box {
    .table-processors {
      overflow: hidden;
    }
  }

  .bk-sideslider {
    top: 60px;
  }

  .bk-sideslider-closer {
    background-color: $primaryColor !important;
  }
}

.search-box {
  padding: 0;

  .search-item {
    margin-bottom: 0;
  }
}

.bkdata-en {
  .application-record {
    .status-width {
      width: 180px;
    }
    .search-item {
      width: auto;
      .search-label {
        width: auto;
      }
    }
  }
}
</style>
