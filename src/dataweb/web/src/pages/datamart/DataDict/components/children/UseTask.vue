

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
  <div v-bkloading="{ isLoading: isTableLoading }"
    class="data-dict-task">
    <bkdata-table :data="orderData"
      :pagination="pagination"
      :emptyText="$t('暂无数据')"
      @page-change="handlePageChange"
      @page-limit-change="handlePageLimitChange">
      <bkdata-table-column minWidth="100"
        :label="$t('项目名称')">
        <div slot-scope="data"
          :title="data.row.project_name">
          {{ data.row.project_name }}
        </div>
      </bkdata-table-column>
      <bkdata-table-column v-if="queryParams.dataType === 'result_table'"
        minWidth="100"
        :label="$t('任务名称')"
        prop="taskName">
        <div slot-scope="data"
          :title="`${data.row.flow_name}${data.row.description ? `（${data.row.description}）` : ''}`">
          {{ data.row.flow_name }}{{ data.row.description ? `（${data.row.description}）` : '' }}
        </div>
      </bkdata-table-column>
      <bkdata-table-column v-else
        minWidth="100"
        :label="$t('任务名称')">
        <div slot-scope="data"
          :title="`${data.row.clean_config_name}`">
          {{ data.row.clean_config_name }}（{{ data.row.description }}）
        </div>
      </bkdata-table-column>
      <bkdata-table-column width="200"
        :label="$t('负责人')"
        prop="people">
        <div slot-scope="data"
          :title="data.row.created_by">
          {{ data.row.created_by }}
        </div>
      </bkdata-table-column>
      <bkdata-table-column width="200"
        :label="$t('创建日期')"
        prop="created_at" />
      <bkdata-table-column v-if="queryParams.dataType === 'raw_data'"
        width="125"
        :label="$t('操作')">
        <template slot-scope="data">
          <a v-if="permissionStatus === '详情'"
            href="javascript:;"
            @click="linkToDetail(data.row)">
            {{ permissionStatus }}
          </a>
          <span v-else-if="permissionStatus === $t('权限正在申请中')"> {{ permissionStatus }} </span>
          <a v-else-if="permissionStatus === $t('申请')"
            href="javascript:;"
            @click="applyRawData(data.row)">
            {{ permissionStatus }}
          </a>
        </template>
      </bkdata-table-column>
      <bkdata-table-column v-if="queryParams.dataType === 'result_table'"
        width="125"
        :label="$t('操作')">
        <template slot-scope="data">
          <a v-if="data.row.has_pro_permission"
            href="javascript:;"
            @click="linkToDetail(data.row)">
            {{ $t('详情') }}
          </a>
          <a v-else
            href="javascript:;"
            @click="applyPermission(data.row)">
            {{ $t('申请') }}
          </a>
        </template>
      </bkdata-table-column>
    </bkdata-table>
    <PermissionAppWin ref="perMissionAppWin"
      :isOpen="isPermissionAppWinOpen"
      :objectId="projectId"
      :showWarning="true"
      @closeDialog="isPermissionAppWinOpen = false"
      @applySuccess="handleApplySuccess" />
    <PermissionApplyWindow v-if="isShowApplyPermission"
      ref="apply" />
  </div>
</template>
<script>
import PermissionAppWin from '@/pages/authCenter/permissions/PermissionApplyWindow';
import PermissionApplyWindow from '@/pages/authCenter/permissions/PermissionApplyWindow';

export default {
  components: {
    PermissionAppWin,
    PermissionApplyWindow,
  },
  props: {
    resultTableInfo: {
      type: Object,
      default: () => {},
    },
    dataParams: {
      type: Object,
      required: false,
      default: null,
    },
  },
  data() {
    return {
      isTableLoading: false,
      taskInfo: [],
      curPage: 1,
      pageCount: 10,
      isPermissionAppWinOpen: false,
      projectId: 249,
      permissionStatus: this.$t('详情'),
      isShowApplyPermission: false,
    };
  },
  computed: {
    pagination() {
      return {
        count: (this.taskInfo || []).length,
        limit: this.pageCount,
        current: this.curPage || 1,
      };
    },
    orderData() {
      return this.taskInfo.slice(this.pageCount * (this.curPage - 1), this.pageCount * this.curPage);
    },
    queryParams() {
      return this.dataParams || this.$route.query;
    },
  },
  mounted() {
    this.getUseTaskInfo();
    if (this.queryParams.dataType === 'result_table') {
      this.dataJudgePermission();
    } else if (this.queryParams.dataType === 'raw_data') {
      this.rawDataEtlPermission();
    }
  },
  methods: {
    dataJudgePermission() {
      this.isPermissionLoading = true;
      const dataType = this.queryParams.dataType;
      let objectId = this.queryParams.result_table_id;
      const options = {
        query: {
          action_id: dataType + '.query_data',
          object_id: objectId,
        },
      };
      this.bkRequest.httpRequest('dataDict/dataJudgePermission', options).then(res => {
        if (res.result) {
          if (res.data.has_permission && this.queryParams.dataType === 'result_table') {
            this.permissionStatus = this.$t('详情');
          } else if (!res.data.has_permission && res.data.ticket_status === 'processing') {
            this.permissionStatus = this.$t('权限正在申请中');
          } else if (!res.data.has_permission && res.data.ticket_status !== 'processing') {
            this.permissionStatus = this.$t('申请');
          }
        } else {
          this.getMethodWarning(res.message, res.code);
        }
        this.isPermissionLoading = false;
      });
    },
    rawDataEtlPermission() {
      this.isPermissionLoading = true;
      let objectId = this.queryParams.data_id;
      const options = {
        query: {
          action_id: 'raw_data.etl',
          object_id: objectId,
        },
      };
      this.bkRequest.httpRequest('dataDict/dataJudgePermission', options).then(res => {
        if (res.result) {
          if (res.data.has_permission && this.queryParams.dataType === 'raw_data') {
            this.permissionStatus = this.$t('详情');
          } else if (!res.data.has_permission && res.data.ticket_status === 'processing') {
            this.permissionStatus = this.$t('权限正在申请中');
          } else if (!res.data.has_permission && res.data.ticket_status !== 'processing') {
            this.permissionStatus = this.$t('申请');
          }
        } else {
          this.getMethodWarning(res.message, res.code);
        }
        this.isPermissionLoading = false;
      });
    },
    handleApplySuccess() {
      this.$router.push({ name: 'Records' });
    },
    linkToDetail(data) {
      let res = confirm(window.gVue.$t('离开此页面_将前往对应任务详情'));
      if (!res) return;
      if (this.queryParams.dataType === 'raw_data') {
        if (this.resultTableInfo.has_permission) {
          this.$router.push({
            name: 'data_detail',
            params: {
              tabid: 3,
              did: data.raw_data_id,
            },
          });
        } else {
          this.$refs.perMissionAppWin.selectedValue.roleId = 'project.flow_member';
          this.$set(this, 'isPermissionAppWinOpen', true);
          this.projectId = data.project_id;
        }
      } else {
        if (data.has_pro_permission) {
          this.$router.push({
            name: 'dataflow_ide',
            params: {
              fid: data.flow_id,
            },
            query: {
              project_id: data.project_id,
            },
          });
        }
      }
    },
    applyPermission(data) {
      this.$refs.perMissionAppWin.selectedValue.roleId = 'project.flow_member';
      this.$set(this, 'isPermissionAppWinOpen', true);
      this.projectId = data.project_id;
    },
    applyRawData() {
      let roleId, id;
      roleId = 'raw_data.cleaner';
      id = this.queryParams.data_id;
      this.isShowApplyPermission = true;
      this.$nextTick(() => {
        this.$refs.apply
          && this.$refs.apply.openDialog({
            data_set_type: this.queryParams.dataType,
            bk_biz_id: this.queryParams.bk_biz_id,
            data_set_id: id,
            roleId: roleId,
          });
      });
    },
    handlePageChange(page) {
      this.curPage = page;
    },
    handlePageLimitChange(pageSize) {
      this.curPage = 1;
      this.pageCount = pageSize;
    },
    getUseTaskInfo() {
      this.isTableLoading = true;
      let url;
      const options = {};
      if (this.queryParams.dataType === 'result_table') {
        url = 'dataDict/getResultTaskInfo';
        (options.params = {
          result_table_id: this.queryParams.result_table_id,
        }),
        (options.query = {
          has_perm: 1,
        });
      } else {
        url = 'dataDict/getSourceTaskInfo';
        options.query = {
          raw_data_id: this.queryParams.data_id,
          has_project_name: 1,
        };
      }
      this.bkRequest.httpRequest(url, options).then(res => {
        if (res.result) {
          this.taskInfo = res.data;
        } else {
          this.getMethodWarning(res.message, res.code);
        }
        this.isTableLoading = false;
      });
    },
  },
};
</script>
<style lang="scss" scoped>
.data-dict-task {
  .disabled {
    color: #babddd !important;
    outline: none;
  }
}
</style>
