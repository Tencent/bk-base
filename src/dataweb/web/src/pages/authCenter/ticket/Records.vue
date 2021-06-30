

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
      <AuthNav :activeName="'Records'" />
    </template>

    <div class="application-record">
      <SearchLayout>
        <template slot="searchTitle">
          <div class="search-box">
            <div class="search-raw">
              <div class="search-item">
                <label class="search-label">{{ $t('申请状态') }}：</label>
                <div class="search-content">
                  <bkdata-selector
                    :selected.sync="search.status.value"
                    :list="statusList"
                    :placeholder="$t('请选择')"
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
              <bkdata-table-column :label="$t('申请人')"
                prop="created_by"
                width="130" />
              <bkdata-table-column :label="$t('申请时间')"
                prop="created_at"
                width="180" />
              <bkdata-table-column :label="$t('申请类型')"
                prop="ticket_type_display"
                width="180" />
              <bkdata-table-column :label="$t('申请状态')"
                width="180">
                <div slot-scope="status"
                  class="bk-table-inlineblock">
                  <span :class="[statusInfos[status.row.status].class]">
                    {{ status.row.status_display }}
                  </span>
                </div>
              </bkdata-table-column>
              <bkdata-table-column :label="$t('当前步骤')"
                width="140">
                <div slot-scope="steps"
                  class="bk-table-inlineblock">
                  <span class="bk-primary"> {{ steps.row.process_step }} </span>/<span class="bk-primary">
                    {{ steps.row.process_length }}
                  </span>
                </div>
              </bkdata-table-column>
              <bkdata-table-column :label="$t('当前处理人')">
                <div slot-scope="processes">
                  <div
                    v-if="processes.row.processors.length > 0"
                    v-tooltip="processes.row.processors.join(', ')"
                    class="bk-primary hiding">
                    {{ processes.row.processors.join(',') }}
                  </div>
                  <div v-else>
                    -
                  </div>
                </div>
              </bkdata-table-column>
              <bkdata-table-column :label="$t('操作')"
                width="200">
                <div slot-scope="props"
                  class="bk-table-inlineblock">
                  <span class="bk-text-button"
                    @click="detail(props.row)">
                    {{ $t('查看') }}
                  </span>

                  <!--<OperateBtn-->
                  <!--v-if="statusInfos[props.row.status].actions.indexOf('resubmit') !== -1"-->
                  <!--:id="props.row.id"-->
                  <!--@confirm="resubmit"-->
                  <!--&gt;-->
                  <!--{{ $t('重新提交') }}-->
                  <!--</OperateBtn>-->

                  <!--<OperateBtn-->
                  <!--v-if="statusInfos[props.row.status].actions.indexOf('withdraw') !== -1"-->
                  <!--:id="props.row.id"-->
                  <!--@confirm="withdraw"-->
                  <!--&gt;-->
                  <!--{{ $t('撤回') }}-->
                  <!--</OperateBtn>-->
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
import { showMsg } from '@/common/js/util.js';

import { listTicket, ticketStatus, ticketTypes } from '@/common/api/auth';
import { AuthNav, SearchLayout } from '../parts/index.js';
import TicketDetail from './Detail';

export default {
  components: {
    Layout,
    AuthNav,
    SearchLayout,
    TicketDetail,
  },
  data() {
    return {
      isLoading: false,
      search: {
        status: {
          value: '',
        },
        ticketType: {
          value: '',
        },
      },
      statusList: [],
      ticketTypeList: [],
      sideslider: {
        isShow: false,
        stateId: null,
      },

      applicationList: [],
      pageCount: 1,
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
          actions: ['withdraw'],
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
          actions: ['resubmit'],
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
    calcPagination() {
      return {
        current: Number(this.currentPage),
        count: this.pageCount,
        limit: this.pageSize,
      };
    },
    tableSearchList() {
      return this.applicationList;
    },
    currentPage() {
      return this.$route.query.page || 1;
    },
  },
  watch: {
    search: {
      handler: function (newVal) {
        this.loadApplicationList();
      },
      deep: true,
    },
    '$route.query': {
      deep: true,
      handler: function (val) {
        this.loadApplicationList();
      },
    },
  },
  mounted() {
    this.init();
  },
  methods: {
    handlePageChange(page) {
      this.$router.push({
        query: Object.assign({}, this.$route.query, { page: page }),
      });
      this.pageCurrent = page;
      this.loadApplicationList();
    },
    handlePageLimitChange(limit) {
      this.pageSize = limit;
      this.loadApplicationList();
    },
    paramsChange() {
      // this.getDataIdList()
      this.$nextTick(() => {
        this.$refs.dataidTable.pageRevert();
      });
    },
    detail(item) {
      this.sideslider.stateId = item.state_id;
      this.sideslider.isShow = true;
    },
    resubmit(params) {
      showMsg('提交成功', 'success');
    },
    withdraw(params) {
      showMsg('提交成功', 'success');
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
        ticket_type: this.search.ticketType.value,
        status: this.search.status.value,
        is_creator: true,
        page: this.currentPage,
        page_size: this.pageSize,
      };
      listTicket(queryParam).then(res => {
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
    .label-success {
      padding: 0px 8px;
    }
    .label-primary {
      padding: 0px 8px;
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
    .search-item {
      width: auto;
      .search-label {
        width: auto;
      }
    }
  }
}
</style>
