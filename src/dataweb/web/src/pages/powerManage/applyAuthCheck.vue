

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
  <div class="data-powerManage">
    <div class="inquire-content clearfix">
      <ticket-selector :title="$t('状态')"
        :selected.sync="search.ticketStateStatus"
        :placeholder="placeholder.ticketStateStatus"
        :list="condition.statusList" />
      <ticket-selector :title="$t('申请类型')"
        :selected.sync="search.ticketType"
        :placeholder="placeholder.ticketType"
        :list="condition.typeList" />
    </div>
    <div v-bkloading="{ isLoading: loading }"
      class="table">
      <auth-table :pageSize="10"
        :isCache="true"
        :className="'main-table'"
        :tableList="stateFilterList"
        :tableFields="tableFields">
        <template slot="created_by"
          slot-scope="{ item }">
          {{ item.ticket.created_by }}
        </template>
        <template slot="created_at"
          slot-scope="{ item }">
          {{ item.ticket.created_at }}
        </template>
        <template slot="ticket_type_display"
          slot-scope="{ item }">
          {{ item.ticket.ticket_type_display }}
        </template>
        <template slot="applyContent"
          slot-scope="{ item }">
          <template v-if="item.ticket.permissions[0].object_class === 'biz'">
            <p>{{ $t('业务') }}：</p>
            <p v-for="permission in item.ticket.permissions"
              :key="permission.id">
              {{ permission.scope.bk_biz_info.name }}
            </p>
            <p>{{ $t('接入的全部非敏感数据') }}</p>
          </template>
          <template v-else>
            <p>{{ $t('结果表') }}：</p>
            <p v-for="permission in item.ticket.permissions"
              :key="permission.id">
              {{ permission.scope.result_table_info.name }}
            </p>
            <p>{{ $t('接入的数据') }}</p>
          </template>
        </template>
        <template slot="status"
          slot-scope="{ item }">
          <span :class="['label', parseStatusDisplay(item.status)]">
            {{ item.status_display }}
          </span>
        </template>
        <template slot="detail"
          slot-scope="{ item }">
          <span class="bk-text-button bk-primary"
            @click="detail(item)">
            {{ $t('查看详情') }}
          </span>
        </template>
      </auth-table>
    </div>
  </div>
</template>

<script>
import { placeholder, postMethodWarning } from '@/common/js/util.js';
import TicketSelector from './children/ticketSelector';

export default {
  components: {
    TicketSelector,
  },
  data() {
    return {
      loading: false,
      search: {
        ticketStateStatus: '',
        ticketType: '',
      },
      placeholder: {
        ticketStateStatus: this.$t('请选择状态'),
        ticketType: '',
      },
      condition: {
        statusList: [
          { name: this.$t('待处理'), id: 'processing' },
          { name: this.$t('成功'), id: 'succeeded' },
          { name: this.$t('失败'), id: 'failed' },
        ],
        typeList: [], // 获取申请类型
      },
      ticketStateList: [],
      tableFields: [
        { text: this.$t('申请人'), width: '175px', sortable: true, name: 'created_by' },
        { text: this.$t('申请时间'), width: '230px', sortable: true, name: 'created_at' },
        { text: this.$t('申请类型'), width: '230px', sortable: true, name: 'ticket_type_display' },
        { text: this.$t('申请内容'), name: 'applyContent' },
        { text: this.$t('状态'), width: '110px', name: 'status' },
        { text: this.$t('操作'), width: '100px', name: 'detail' },
      ],
    };
  },
  computed: {
    stateFilterList() {
      return this.ticketStateList.filter(state => {
        let status = state.status === this.search.ticketStateStatus
          || this.search.ticketStateStatus === '' || this.search.ticketStateStatus === '';
        let type = state.ticket.ticket_type === this.search.ticketType
          || this.search.ticketType === '' || this.search.ticketType === '';
        return status && type;
      });
    },
  },
  watch: {
    search: {
      handler(newVal) {
        let params;
        if (JSON.stringify(this.$route.query) !== '{}') {
          params = Object.assign({}, this.$route.query, newVal);
        } else {
          params = newVal;
        }
        this.$router.push({
          query: params,
        });
      },
      deep: true,
    },
  },
  methods: {
    init() {
      this.search.ticketStateStatus = 'processing';
      this.getTicketType();
      this.getTicketStateList();
    },
    parseStatusDisplay(status) {
      switch (status) {
        case 'succeeded':
          return 'success';
        case 'failed':
        case 'stopped':
          return 'refuse';
        default:
          return 'checking';
      }
    },
    parseStatusDisplayStr(status) {
      for (let item of this.condition.statusList) {
        if (item.id === status) {
          return item.name;
        }
      }
      return this.$t('待处理');
    },
    getTicketType() {
      this.placeholder.ticketType = this.$t('数据加载中');
      this.axios.get('v3/auth/tickets/ticket_types/').then(resp => {
        if (resp.result) {
          this.condition.typeList = resp.data;
          this.placeholder.ticketType = placeholder(this.condition.typeList, '请选择申请类型');
        }
      });
    },
    getTicketStateList() {
      this.loading = true;
      this.axios
        .get('v3/auth/ticket_state/?is_processor=true')
        .then(resp => {
          if (resp.result) {
            this.ticketStateList = resp.data;
          } else {
            this.getMethodWarning(resp.message, resp.code);
          }
        })
        ['finally'](() => {
          this.loading = false;
        });
    },
    detail(item) {
      this.$router.push({ name: 'applyauth_check_detail', params: { applyid: item.id } });
    },
  },
};
</script>

<style lang="scss" type="text/css">
$textColor: #737987;
.data-powerManage {
  .table-header {
    background: #f2f4f9;
    padding: 19px 75px;
    color: #1a1b2d;
    font-size: 16px;
    p {
      float: left;
      line-height: 36px;
      padding-left: 22px;
      position: relative;
      &:before {
        content: '';
        width: 4px;
        height: 20px;
        position: absolute;
        left: 0px;
        top: 8px;
        background: #3a84ff;
      }
    }
  }
  .inquire-content {
    // height: 80px;
    line-height: 36px;
    // padding: 22px 75px;
    box-shadow: 0 0 10px 0 rgba(31, 32, 36, 0.04);
    .inquire {
      float: left;
      //   margin-left: 30px;
      &:first-of-type {
        margin-left: 0;
      }
    }
    .title {
      float: left;
      margin-right: 5px;
      font-size: 15px;
    }
    .inquire-select {
      float: left;
      width: 250px;
      .bk-select-list {
        z-index: 1005;
      }
    }
    .inquire-button {
      float: left;
      margin-left: 22px;
    }
  }
  .dialog {
    .title {
      margin-bottom: 48px;
      font-size: 15px;
      span {
        color: #212232;
        display: inline-block;
        width: 4px;
        height: 19px;
        background-color: #3a84ff;
        margin-right: 17px;
        position: relative;
        top: 5px;
      }
    }
    .errot-tip {
      padding-left: 120px;
      color: red;
    }
    .close-button {
      position: absolute;
      top: 0;
      right: 0;
      width: 36px;
      height: 36px;
      line-height: 36px;
      text-align: center;
      font-size: 12px;
      color: #666;
    }
    .content {
      margin-left: 79px;
      width: 460px;
      display: inline-block;
      .required {
        color: red;
        vertical-align: middle;
      }
      .error {
        border-color: red;
        .bk-select-input {
          border-color: red;
        }
        .bkdata-selector-input {
          border-color: red;
        }
      }
      span.label-title {
        text-align: right;
        width: 120px;
        float: left;
        font-size: 14px;
        font-weight: normal;
        color: #666;
        line-height: 1;
        box-sizing: border-box;
        padding: 10px 20px 10px 0;
        &.create-appid {
          width: 120px;
        }
      }
      .bk-form-content {
        display: inline-block;
        width: 320px;
        textarea {
          min-height: 80px;
        }
      }
      .bk-form-radio {
        margin-right: 12px;
      }
      button {
        width: 120px;
        height: 42px;
        margin-bottom: 35px;
        margin-right: 15px;
        &.submit {
          background: #3a84ff;
          color: #fff;
        }
      }
      .demo-select-create {
        height: 42px;
        line-height: 42px;
        background-color: #fafbfd;
        border-top: 1px solid #e5e5e5;
        padding: 0 10px;
        margin: 0;
        font-size: 14px;
        cursor: pointer;
      }
    }
    #result-tables {
      .is-all ::-webkit-input-placeholder {
        color: #666;
      }
    }
    .button-group {
      margin-left: 190px;
      .bk-button.is-loading {
        background: #3a84ff !important;
      }
    }
  }
  .table {
    .apply-content {
      width: 100%;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .label {
      display: block;
      padding: 0 9px;
      line-height: 26px;
      border-radius: 3px;
      color: #fff;
      position: relative;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      text-align: center;
      width: 100%;
      &.success {
        background: #9dcb6b;
      }
      &.refuse {
        background: #fe771d;
      }
      &.checking {
        background: #f2f4f9;
        color: #444444;
        border: 1px solid #d9dbe0;
      }
    }
  }
}
</style>
