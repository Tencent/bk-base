

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
    <HeaderType :title="$t('变更记录')">
      <div v-bkloading="{ isLoading }">
        <bkdata-table
          :data="tableList"
          :emptyText="$t('暂无数据')"
          :pagination="pagination"
          @page-change="handlePageChange"
          @page-limit-change="handlePageLimitChange">
          <bkdata-table-column :label="$t('操作类型')"
            prop="type_alias">
            <template slot-scope="{ row }">
              <template v-if="row.show_type === 'add_tips' && row.type !== 'update_task_status'">
                <div v-bk-tooltips="getUpdateTips(row)">
                  [{{ row.type_alias }}]
                  <span style="border-bottom: 1px dashed #979ba5; display: inline-block">{{ row.sub_type_alias }}</span>
                </div>
              </template>
              <template v-else>
                {{ row.type_alias }}
              </template>
            </template>
          </bkdata-table-column>
          <bkdata-table-column :label="$t('状态')">
            <div slot-scope="props"
              class="text-overflow">
              {{ props.row.status_alias || '--' }}
            </div>
          </bkdata-table-column>
          <bkdata-table-column :label="$t('创建人')"
            prop="created_by" />
          <bkdata-table-column :label="$t('创建时间')"
            prop="datetime" />
          <bkdata-table-column :label="$t('操作')">
            <template slot-scope="item">
              <div class="detail-container">
                <a
                  :class="{ disabled: handerDisabled(item.row.type) }"
                  href="javascript:;"
                  @click="handleOperation(item.row)">
                  {{ $t('详情') }}
                </a>
                <div v-if="item.row.status === 'init'">
                  <i class="icon-prompt" />您刚创建的迁移任务在此
                </div>
              </div>
            </template>
          </bkdata-table-column>
        </bkdata-table>
      </div>
    </HeaderType>
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
import HeaderType from '@/pages/datamart/common/components/HeaderType';
import DataRemoval from '@/pages/datamart/DataDict/components/children/DataRemoval/index.vue';

export default {
  components: {
    HeaderType,
    DataRemoval,
  },
  data() {
    return {
      isLoading: true,
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
      currentPage: 1,
      calcPageSize: 10,
    };
  },
  computed: {
    pagination() {
      return {
        current: Number(this.currentPage),
        count: this.dataRemovalList.length,
        limit: this.calcPageSize,
      };
    },
    tableList() {
      return this.dataRemovalList.slice(
        (this.currentPage - 1) * this.calcPageSize,
        this.currentPage * this.calcPageSize
      );
    },
  },
  watch: {
    '$attrs.footData'(val) {
      if (val.length) {
        this.isLoading = false;
        this.dataRemovalList = val;
      }
    },
  },
  methods: {
    getUpdateTips(data = {}) {
      let tips = [];
      const descList = (data.details && data.details.desc_params) || [];
      descList.forEach(desc => {
        let kvTpl = data.details.kv_tpl;
        Object.keys(desc).forEach(key => {
          kvTpl = kvTpl.replace(`{${key}}`, desc[key]);
        });
        tips.push(kvTpl);
      });
      return {
        content: tips.join('<br />'),
        placement: 'left',
      };
    },
    handerDisabled(type) {
      return !['update_task_status', 'migrate'].includes(type);
    },
    handleOperation(data) {
      if (this.handerDisabled(data.type)) return;
      if (data.type === 'update_task_status') {
        this.linkToFlow(data.details.jump_to || {});
      } else {
        const params = (data.sub_events && data.sub_events[data.sub_events.length - 1]) || {};
        this.previewItem((params.details && params.details.jump_to) || {});
      }
    },
    previewItem(data) {
      this.itemObj = Object.assign({}, data, { result_table_id: this.$route.query.result_table_id });
      this.isShow = true;
      this.isShowRemoval = true;
    },
    linkToFlow(data) {
      let res = confirm(window.gVue.$t('离开此页面_将前往对应任务详情'));
      if (!res) return;
      this.$router.push({
        name: 'dataflow_ide',
        params: {
          fid: data.flow_id,
        },
      });
    },
    handlePageChange(page) {
      this.currentPage = page;
    },
    handlePageLimitChange(pageSize, preLimit) {
      this.currentPage = 1;
      this.calcPageSize = preLimit;
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
</style>
