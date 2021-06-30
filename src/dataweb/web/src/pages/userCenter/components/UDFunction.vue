

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
  <div class="functionWrapper">
    <div class="header">
      <bkdata-input v-model="searchKey"
        :placeholder="$t('搜索函数关键字')"
        :rightIcon="'bk-icon icon-search'"
        style="width: 273px" />
      <bkdata-button :theme="'primary'"
        style="width: 130px; margin: 0"
        @click="createFunc">
        {{ $t('创建自定义函数') }}
      </bkdata-button>
    </div>
    <div v-bkloading="{ isLoading: listLoading }"
      class="table">
      <bkdata-table :data="udflistShow">
        <bkdata-table-column :label="$t('函数名')">
          <template slot-scope="props">
            <span class="auth table-text"
              @click="editUDF(props.row.func_name, openUdfSlider)">
              {{ props.row.func_name }}
            </span>
          </template>
        </bkdata-table-column>
        <bkdata-table-column :label="$t('中文名')"
          prop="func_alias" />
        <bkdata-table-column :label="$t('状态')"
          :filters="statusFilters"
          :filterMethod="statusFilterMethod"
          :filterMultiple="true"
          prop="status">
          <template slot-scope="props">
            <span class="text">{{ statusMap[props.row.status] }}</span>
          </template>
        </bkdata-table-column>
        <bkdata-table-column :label="$t('最新版本')"
          prop="version" />
        <bkdata-table-column :label="$t('创建人')"
          prop="created_by" />
        <bkdata-table-column :label="$t('更新时间')"
          prop="updated_at" />
        <bkdata-table-column :label="$t('操作')"
          :align="'center'"
          width="300">
          <template slot-scope="props">
            <span class="auth table-text"
              @click="getAuth(props.row.func_name)">
              {{ $t('权限管理') }}
            </span>
            <span :class="['detail', 'table-text', { disabled: props.row.status === 'developing' }]"
              @click="getDetail(props.row.func_name, props.row.status === 'developing')">
              {{ $t('使用详情') }}
            </span>
            <span :class="['log', 'table-text', { disabled: props.row.status === 'developing' }]"
              @click="getLog(props.row.func_name, props.row.status === 'developing')">
              {{ $t('日志') }}
            </span>
            <Delete-confirm :ref="`deletePop${props.$index}`"
              @confirmHandle="deleteUDF(props.row.func_name, props.$index)"
              @cancleHandle="closeDeleteTip(props.$index)">
              <span class="delete table-text">{{ $t('删除') }}</span>
            </Delete-confirm>
          </template>
        </bkdata-table-column>
      </bkdata-table>
    </div>
    <!------------------ 使用详情弹框 ----------------------->
    <bkdata-dialog v-model="detailShow"
      :title="$t('使用详情')"
      :headerPosition="'left'"
      :width="686"
      @value-change="clearData">
      <div v-bkloading="{ isLoading: detailLoading }"
        class="funcDetailWrapper">
        <div class="detail-item">
          <span class="title">{{ $t('项目') }}</span>
          <bkdata-selector
            :selected.sync="relatedProj.selected"
            :placeholder="$t('请选择')"
            :list="relatedProject"
            :isLoading="relatedProj.isLoading"
            :searchable="true"
            :settingKey="'project_id'"
            :displayKey="'displayName'"
            :searchKey="'displayName'"
            style="width: 200px; margin-right: 35px"
            @item-selected="relatedProjChange" />
          <span class="title">{{ $t('任务') }}</span>
          <bkdata-selector
            :selected.sync="relatedFlowCont.selected"
            :placeholder="$t('请选择')"
            :list="relatedFlow"
            :isLoading="relatedFlowCont.isLoading"
            :searchable="true"
            :settingKey="'flow_id'"
            :displayKey="'displayName'"
            :searchKey="'displayName'"
            style="width: 200px; margin-right: 35px"
            @item-selected="relatedFlowChange" />
        </div>
        <div class="detail-item start-align">
          <span class="title mt13">{{ $t('节点') }}</span>
          <div v-bkloading="{ isLoading: nodeLoading }"
            class="nodesGroup">
            <bkdata-popover v-for="(item, index) in relatedNode"
              :key="index">
              <div
                :class="{
                  nodes: true,
                  success: item.status === 'running',
                  failure: item.status === 'failure',
                  modified: item.status === 'warning',
                }"
                @click="jumpToFlow(item)">
                {{ item.node_name }}
              </div>
              <div slot="content">
                <p>{{ $t('版本') }}{{ item.version }}</p>
                <p>{{ item.node_name }}</p>
              </div>
            </bkdata-popover>
            <p v-if="!relatedNode.length"
              class="no-content-tip">
              {{ $t('暂无节点') }}
            </p>
          </div>
        </div>
      </div>
    </bkdata-dialog>

    <!------------------ 日志的侧边栏 ----------------------->
    <bkdata-sideslider :isShow.sync="logShow"
      :width="756"
      :title="$t('日志详情')">
      <div slot="content">
        <div v-bkloading="{ isLoading: logLoading }"
          class="logTable">
          <bkdata-table :data="logList">
            <bkdata-table-column :label="$t('版本号')"
              :width="90"
              prop="version" />
            <bkdata-table-column :label="$t('操作人')"
              :width="124"
              prop="updated_by" />
            <bkdata-table-column :label="$t('时间')"
              :width="153"
              prop="updated_at" />
            <bkdata-table-column :label="$t('内容')"
              prop="release_log" />
          </bkdata-table>
        </div>
      </div>
    </bkdata-sideslider>

    <!------------------ 创建自定义函数 ----------------------->
    <div v-if="showUDFProcess"
      class="udfWrapper">
      <udf-slider ref="udfSlider"
        @udfSliderClose="closeSlider" />
    </div>

    <!------------------ 人员管理器 ----------------------->
    <MemberManagerWindow ref="member"
      :roleIds="udfMemberParams.roleIds"
      :isOpen.sync="udfMemberParams.isOpen"
      :objectClass="udfMemberParams.objectClass"
      :scopeId="udfMemberParams.scopeId" />
  </div>
</template>

<script>
import udfMethods from '@/components/tree/udfMixin.js';
import DeleteConfirm from '@/components/tooltips/DeleteConfirm.vue';
import udfSlider from '@/pages/DataGraph/Graph/Components/UDF/sliderIndex';
import MemberManagerWindow from '@/pages/authCenter/parts/MemberManagerWindow';
import { showMsg, postMethodWarning, confirmMsg } from '@/common/js/util.js';
import Bus from '@/common/js/bus.js';
export default {
  components: { udfSlider, MemberManagerWindow, DeleteConfirm },
  mixins: [udfMethods],
  props: {
    activeName: {
      type: String,
    },
  },
  data() {
    return {
      statusFilters: [
        { text: '开发中', value: 'developing' },
        { text: '已发布', value: 'released' },
      ],
      statusMap: {
        developing: '开发中',
        released: '已发布',
      },
      listLoading: false,
      activeFunName: '',
      nodeLoading: false,
      relatedFlowCont: {
        selected: '',
        isLoading: false,
      },
      relatedProj: {
        selected: '',
        isLoading: false,
      },
      udfMemberParams: {
        isOpen: false,
        objectClass: 'function',
        scopeId: '',
        roleIds: ['function.manager', 'function.developer'],
      },
      logLoading: false,
      detailLoading: false,
      udfList: [],
      relatedProject: [],
      relatedFlow: [],
      relatedNode: [],
      showUDFProcess: false,
      searchKey: '',
      logList: [],
      logShow: false,
      detailShow: false,
    };
  },
  computed: {
    udflistShow() {
      let list = JSON.parse(JSON.stringify(this.udfList));
      return list.filter(item => item.func_name.includes(this.searchKey));
    },
  },
  watch: {
    relatedFlow(newValue, oldValue) {
      if (newValue !== oldValue) {
        this.relatedNode = [];
      }
    },
  },
  mounted() {
    this.getUDFlist();
    Bus.$on('getUDFList', this.getUDFlist);
  },
  beforeDestroy() {
    Bus.$off('getUDFList');
  },
  methods: {
    statusFilterMethod(value, row, column) {
      const property = column.property;
      return row[property] === value;
    },
    closeDeleteTip(index) {
      const ref = `deletePop${index}`;
      this.$refs[ref].closeTip();
    },
    deleteUDF(udfName, index) {
      const ref = `deletePop${index}`;
      this.bkRequest
        .httpRequest('udf/deleteUDF', {
          params: {
            function_name: udfName,
          },
        })
        .then(res => {
          if (res.result) {
            postMethodWarning(this.$t('删除成功'), 'success');
            this.getUDFlist(); // 刷新列表
          } else {
            postMethodWarning(res.message, 'error');
          }
        });
      this.$refs[ref].closeTip();
    },
    openUdfSlider(data) {
      this.$store.dispatch('udf/editUDFStatusSet', data);
      this.createFunc();
    },
    clearData(val) {
      if (val === false) {
        this.relatedProj.selected = '';
        this.relatedFlowCont.selected = '';
        this.relatedNode = [];
      }
    },
    tippyContent(item) {
      return {
        customComponent: {
          template: `<div><p>版本${item.version}</p><p>${item.node_name}</p></div>`,
        },
      };
    },
    jumpToFlow(item) {
      this.$router.push({
        query: {
          project_id: this.relatedProj.selected,
          NID: `ch_${item['node_id']}`,
        },
        params: { fid: this.relatedFlowCont.selected },
        name: 'dataflow_ide',
      });
    },
    relatedFlowChange(flowId) {
      this.nodeLoading = true;
      this.bkRequest
        .httpRequest('udf/getUdfFlowOrNodes', {
          params: {
            function_name: this.activeFunName,
          },
          query: {
            object_type: 'flow',
            object_id: flowId,
          },
        })
        .then(res => {
          if (res.result) {
            if (Object.keys(res.data).includes(flowId.toString())) {
              this.relatedNode = res.data[flowId];
            } else {
              this.relatedNode = [];
            }
          } else {
            postMethodWarning(res.message, 'error');
          }
        })
        ['finally'](() => {
          this.nodeLoading = false;
        });
    },
    relatedProjChange(id) {
      this.relatedFlowCont.isLoading = true;
      this.relatedFlowCont.selected = '';
      this.bkRequest
        .httpRequest('udf/getUdfFlowOrNodes', {
          params: {
            function_name: this.activeFunName,
          },
          query: {
            object_type: 'project',
            object_id: id,
          },
        })
        .then(res => {
          if (res.result) {
            if (Object.keys(res.data).includes(id.toString())) {
              this.relatedFlow = res.data[id];
              this.relatedFlow.forEach(item => {
                item.displayName = `[${item.flow_id}]${item.flow_name}`;
              });
            } else {
              this.relatedFlow = [];
            }
          } else {
            postMethodWarning(res.message, 'error');
          }
        })
        ['finally'](() => {
          this.relatedFlowCont.isLoading = false;
        });
    },
    getAuth(funcName) {
      this.udfMemberParams.scopeId = funcName;
      this.udfMemberParams.isOpen = true;
    },
    getUDFlist() {
      this.listLoading = true;
      this.bkRequest
        .httpRequest('udf/getUDFList')
        .then(res => {
          if (res.result) {
            this.udfList = res.data;
          } else {
            postMethodWarning(res.message, 'error');
          }
          this.listLoading = false;
        })
        ['catch'](err => {
          postMethodWarning(err, 'error');
          this.listLoading = false;
        });
    },
    closeSlider() {
      this.udfInit(); // 初始化UDF
      this.showUDFProcess = false;
    },
    udfInit() {
      this.$store.commit('udf/changeInitStatus', false); // 初始化UDF状态
      this.$store.commit('udf/saveDevParams'); // 清空配置
    },
    createFunc() {
      this.showUDFProcess = true;
      this.$nextTick(() => {
        this.$refs.udfSlider.isShow = true;
      });
    },
    getLog(funcName, disabled) {
      if (disabled) return;
      this.logLoading = true;
      this.bkRequest
        .httpRequest('udf/getExactUDF', {
          params: {
            function_name: funcName,
          },
          query: {
            add_release_log_info: true,
          },
        })
        .then(res => {
          if (res.result) {
            this.logList = res.data.release_log;
            this.logLoading = false;
          } else {
            postMethodWarning(res.message, 'error');
          }
        })
        ['catch'](err => {
          postMethodWarning(err, 'error');
        });

      this.logShow = true;
    },
    getDetail(funcName, disabled) {
      if (disabled) return;
      this.activeFunName = funcName;
      this.detailLoading = true;
      this.bkRequest
        .httpRequest('udf/getExactUDF', {
          params: {
            function_name: funcName,
          },
          query: {
            add_project_info: true,
          },
        })
        .then(res => {
          if (res.result) {
            this.relatedProject = res.data.related_project;
            this.relatedProject.forEach(item => {
              item.displayName = `[${item.project_id}]${item.project_name}`;
            });
          } else {
            postMethodWarning(res.message, res.code);
          }
        })
        ['finally'](_ => {
          this.detailLoading = false;
        });
      this.detailShow = true;
    },
  },
};
</script>

<style lang="scss" scoped>
.functionWrapper {
  .header {
    display: flex;
    justify-content: space-between;
  }
  .table {
    margin-top: 15px;
    .table-text {
      font-size: 14px;
      font-family: MicrosoftYaHeiUI;
      color: rgba(58, 132, 255, 1);
      line-height: 18px;
      margin-right: 8px;
      cursor: pointer;
    }
    .text {
      font-size: 12px;
      font-family: MicrosoftYaHeiUI;
      color: #63656e;
      cursor: default;
    }
    /deep/.bk-table-body-wrapper {
      overflow-y: auto;
      overflow-x: hidden;
      height: calc(100vh - 280px);
    }
  }
  .bk-sideslider {
    top: 60px;
    .logTable {
      width: 696px;
      margin: 20px 42px 0 31px;
    }
  }
}
.funcDetailWrapper {
  width: 638px;
  height: 278px;
  padding: 0px 26px 0 20px;
  .detail-item {
    margin-bottom: 20px;
    display: flex;
    justify-content: flex-start;
    align-items: center;
    position: relative;
    &.start-align {
      align-items: flex-start;
    }
    .title {
      width: 60px;
      font-size: 14px;
      font-family: MicrosoftYaHeiUI;
      color: rgba(99, 101, 110, 1);
      line-height: 18px;
      &.mt13 {
        margin-top: 13px;
      }
    }
    .projct-item {
      width: 90px;
      height: 32px;
      background: rgba(255, 255, 255, 1);
      border-radius: 2px;
      border: 1px solid rgba(220, 222, 229, 1);
      display: flex;
      justify-content: center;
      align-items: center;
      margin-right: 5px;
      .icon-check-circle-shape {
        font-size: 14px;
        color: rgba(45, 203, 86, 1);
        margin: 0 8px 0 10px;
      }
      .icon-with-nothing {
        width: 12px;
        height: 12px;
        background: rgba(255, 255, 255, 1);
        border: 1px solid rgba(220, 222, 229, 1);
        border-radius: 50%;
        margin: 0 9px 0 11px;
      }
      .project-item-content {
        font-size: 12px;
        font-family: MicrosoftYaHei;
        color: rgba(99, 101, 110, 1);
        line-height: 16px;
      }
    }
    .nodesGroup {
      width: 540px;
      display: flex;
      justify-content: flex-start;
      align-items: flex-start;
      flex-wrap: wrap;
      background: #fafbfd;
      border: 1px solid #f0f1f5;
      border-radius: 2px;
      padding: 10px;
      height: 150px;
      .bk-tooltip-ref {
        margin-right: 37px;
      }
      .nodes {
        width: 110px;
        height: 24px;
        padding: 0 10px;
        margin-bottom: 5px;
        background: rgba(240, 241, 245, 1);
        border-radius: 15px;
        cursor: pointer;
        font-size: 12px;
        font-family: MicrosoftYaHei;
        color: rgba(151, 155, 165, 1);
        text-align: center;
        line-height: 24px;
        overflow: hidden;
        white-space: nowrap;
        text-overflow: ellipsis;
      }
      .success {
        background: #2dcb56;
        color: #fff;
      }
      .failure {
        background: rgba(255, 221, 221, 1);
        color: rgba(234, 54, 54, 1);
      }
      .modified {
        background: rgba(255, 232, 195, 1);
        color: rgba(255, 156, 1, 1);
      }
    }
  }
}
</style>
