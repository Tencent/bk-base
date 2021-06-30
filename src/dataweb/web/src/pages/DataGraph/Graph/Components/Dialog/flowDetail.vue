/** * 任务详情 */


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
  <bkdata-dialog
    v-model="show"
    class="zIndex2501"
    :padding="0"
    :hasHeader="false"
    :closeIcon="true"
    :extCls="'dataflow-detail bkdata-dialog'"
    :okText="$t('提交')"
    :cancelText="$t('关闭')"
    :width="890"
    :hasFooter="false"
    @confirm="confirm"
    @cancel="close">
    <section>
      <div class="dfDetail-content">
        <!-- <a href='javascript:;'
                    class='close-pop'
                    @click="show = false">
                    <i class='bk-icon icon-close'></i>
                </a> -->
        <p class="title">
          {{ $t('任务详情') }}
        </p>
        <table class="bk-table flow-detail-table mt25 mb25">
          <template v-if="!dataFlowInfo">
            <tr>
              <td colspan="5"
                class="row-empty">
                {{ $t('暂无数据') }}
              </td>
            </tr>
          </template>
          <template v-else>
            <tr class="dfdetail">
              <td class="label">
                {{ $t('任务ID') }}
              </td>
              <td class="df-content">
                {{ dataFlowInfo.flow_id }}
              </td>
              <td class="label">
                {{ $t('任务名称') }}
              </td>
              <td class="df-content">
                <template v-if="isEdit">
                  <div class="df-td-edit">
                    <bkdata-input
                      v-model="dataFlowInfo.flow_name"
                      :class="['flow-detail-input', { 'is-danger': error.nameError }]" />
                  </div>
                </template>
                <template v-else>
                  <div class="df-td-text">
                    <span>{{ dataFlowInfo.flow_name }}</span>
                  </div>
                </template>
              </td>
            </tr>
            <tr class="dfdetail">
              <td class="label">
                {{ $t('项目ID') }}
              </td>
              <td class="df-content">
                {{ dataFlowInfo.project_id }}
              </td>
              <td class="label">
                {{ $t('项目名称') }}
              </td>
              <td class="df-content">
                {{ dataFlowInfo.project_name }}
              </td>
            </tr>
            <tr class="dfdetail">
              <td class="label">
                {{ $t('创建人') }}
              </td>
              <td class="df-content">
                {{ dataFlowInfo.created_by | dealData }}
              </td>
              <td class="label"
                :title="$t('')">
                {{ $t('创建时间') }}
              </td>
              <td class="df-content">
                {{ dataFlowInfo.created_at | dealData }}
              </td>
            </tr>
            <tr class="dfdetail">
              <td class="label">
                {{ $t('更新人') }}
              </td>
              <td class="df-content">
                {{ dataFlowInfo.updated_by | dealData }}
              </td>
              <td class="label">
                {{ $t('更新时间') }}
              </td>
              <td class="df-content">
                {{ dataFlowInfo.updated_at | dealData }}
              </td>
            </tr>
            <!-- TDW支持组件 -->
            <component :is="tdwFragments.componment"
              v-bind="tdwFragments.bindAttr"
              @changeCheckTdw="changeCheckTdw"
              @tipsClick="$emit('openProjectSetting')" />
          </template>
        </table>
      </div>
    </section>
  </bkdata-dialog>
</template>

<script>
import { mapState } from 'vuex';
import tdwPower from '@/pages/DataGraph/Common/tdwPower.js';
import { postMethodWarning } from '@/common/js/util.js';
import Bus from '@/common/js/bus';
import extend from '@/extends/index';

export default {
  name: 'flow-detail',
  filters: {
    dealData: function (val) {
      return val ? val.replace('T', ' ') : '';
    },
  },
  mixins: [tdwPower],
  props: {
    showDetail: {
      type: Boolean,
      default: undefined,
    },
  },
  data() {
    return {
      supportType: 'task',
      dataFlowInfo: {},
      isEdit: true,
      btnLoading: false,
      tdwDetailLoading: false,
      isCheckEnable: false,
      params: {},
      error: {
        nameError: false,
        nameTips: '',
        applicationError: false,
        applicationTips: '',
        sourceServerError: false,
        sourceServerTips: '',
        clusterVersionError: false,
        clusterVersionTips: '',
        computingClusterError: false,
        computingClusterTips: '',
      },
      currentProject: {},
      canCloseTdw: true,
    };
  },
  computed: {
    ...mapState({
      graphDataNode: state => state.ide.graphData.locations,
    }),
    /** 获取TDW支持组件 */
    tdwFragments() {
      const tdwFragments = extend.getVueFragment('tdwFragment', this) || [];
      return tdwFragments.filter(item => item.name === 'TdwSupportTpl')[0];
    },
    projectList() {
      return this.$store.state.common.projectList;
    },
    dataFlow() {
      return this.$store.state.ide.flowData;
    },
    show: {
      get() {
        return this.showDetail;
      },
      async set(newVal) {
        this.$emit('update:showDetail', newVal);
      },
    },
  },
  watch: {
    projectList: {
      deep: true,
      immediate: true,
      handler(newobj, oldobj) {
        const data = this.$store.state.ide.flowData;
        this.currentProject = this.projectList.list.find(item => item.project_id === data.project_id);
      },
    },
    async dataFlow(newVal) {
      this.reset();
    },
    show(newVal) {
      if (newVal) {
        this.reset();
      }
    },
    graphDataNode(newVal) {
      let result = true;
      const typeArr = ['tdw_source', 'tdw_storage', 'tdw_batch', 'tdw_jar_batch'];
      (newVal || []).forEach(node => {
        const hasType = typeArr.find(item => node.node_type === item);
        if (hasType) {
          result = false;
        }
      });
      this.canCloseTdw = result;
    },
  },
  created() {
    this.params = this.getParams();
  },
  mounted() {
    Bus.$on('toggleTaskDetail', isShow => {
      this.show = isShow;
    });
  },
  methods: {
    requestApplicationDetail(resetParams) {
      if (resetParams) {
        this.resetAppDetailParams();
      }
      this.tdwDetailLoading = true;
      this.$store
        .dispatch('tdw/getApplicationDetail', {
          appGroupName: this.params.tdw_conf.tdwAppGroup,
        })
        .then(res => {
          if (res.result) {
            const servers = res.data.lz_servers_info.filter(item => item.server_type === 'scala_group');
            servers.map(item => {
              item.server_type_desc = `${item.server_tag}(${item.server_type_desc})`;
            });
            const versionList = res.data.gaia_cluster_versions;
            this.sourceServerSelect.list = res.data.lz_servers_info.map(item => {
              item.server_type_desc = `${item.server_tag}(${item.server_type_desc})`;
              return item;
            });
            this.computingClusterSelect.list = [...res.data.gaia_id_info_list];
            this.clusterVersionSelect.list = [];
            for (const key in versionList) {
              this.clusterVersionSelect.list.push({
                id: key,
                name: versionList[key],
              });
            }
          } else {
            if (res.code === '1511301') {
              const h = this.$createElement;
              const locations = window.location;
              const content = h('div', {}, [
                '1511301 :【数据平台元数据模块】暂未绑定TDW账户，请先进行',
                h(
                  'a',
                  {
                    attrs: {
                      href: `${locations.origin}${locations.pathname}#/user-center?tab=account`,
                    },
                    style: { color: '#666bb4' },
                  },
                  'TDW账户绑定'
                ),
              ]);
              postMethodWarning(content, 'error');
            } else {
              this.getMethodWarning(res.message, res.code);
            }
          }
        })
        ['finally'](() => {
          this.tdwDetailLoading = false;
        });
      this.validateApplication();
    },
    toggleEdit(isEdit) {
      // this.isEdit = !isEdit
      if (this.isEdit) {
        this.error.nameError = false;
        this.error.applicationError = false;
        this.error.sourceServerError = false;
        this.error.clusterVersionError = false;
        this.error.computingClusterError = false;
      }
    },
    /*
                项目名称验证
            */
    nameTips() {
      if (this.dataFlowInfo.flow_name !== '') {
        this.error.nameError = false;
      } else {
        this.error.nameError = true;
        this.error.nameTips = this.validator.message.required;
      }
    },
    changeCheckTdw(checked) {
      // 任务-应用组列表 是从项目参数转换而来
      const enableTdw = checked;
      this.enableTdw = checked;
      if (!enableTdw && !this.canCloseTdw) {
        postMethodWarning(this.$t('当前flow存在TDW相关节点，不能关闭TDW配置'), 'error');
        this.enableTdw = !enableTdw;
        return false;
      }
      if (enableTdw) {
        if (this.currentProject && this.currentProject.tdw_app_groups) {
          this.hasPower = 'hasPower';
          this.applicationGroupList = this.currentProject.tdw_app_groups.map(item => {
            return {
              app_group_name: item,
            };
          });
        } else {
          this.hasPower = 'notPower';
        }
      } else {
        this.resetTdwParams();
        this.hasPower = '';
      }
    },
    checkedEnable() {
      // 判断是否启用tdw
      return (
        this.params.tdw_conf.tdwAppGroup
        && this.params.tdw_conf.task_info.spark.spark_version
        && this.params.tdw_conf.sourceServer
        && this.params.tdw_conf.gaia_id
      );
    },
    resetAppDetailParams() {
      this.params.tdw_conf.sourceServer = '';
      this.params.tdw_conf.gaia_id = '';
      this.params.tdw_conf.task_info.spark.spark_version = '';
    },
    /**
     *  tdw相关验证
     */
    validateApplication() {
      if (this.params.tdw_conf.tdwAppGroup) {
        this.error.applicationError = false;
      } else {
        this.error.applicationError = true;
        this.error.applicationTips = this.validator.message.required;
      }
    },
    validateSourceServer() {
      if (this.params.tdw_conf.sourceServer) {
        this.error.sourceServerError = false;
      } else {
        this.error.sourceServerError = true;
        this.error.sourceServerTips = this.validator.message.required;
      }
    },
    validateComputingCluster(assign = true) {
      if (this.params.tdw_conf.gaia_id) {
        this.error.computingClusterError = false;
        let selectObj = this.computingClusterSelect.list.filter(item => {
          return item.gaia_id === this.params.tdw_conf.gaia_id;
        });
        if (assign) {
          // 如果所选计算集群有version，自动选择，否则置空，由用户选择
          this.params.tdw_conf.task_info.spark.spark_version = Object.keys(selectObj[0].gaia_version)[0] || '';
        }
      } else {
        this.error.computingClusterError = true;
        this.error.computingClusterTips = this.validator.message.required;
      }
    },
    validateClusterVersion() {
      if (this.params.tdw_conf.task_info.spark.spark_version) {
        this.error.clusterVersionError = false;
      } else {
        this.error.clusterVersionError = true;
        this.error.clusterVersionTips = this.validator.message.required;
      }
    },
    close() {
      this.enableTdw = false;
      this.hasPower = '';
      this.show = false;
      this.toggleEdit(false);
    },
    reset() {
      const data = this.$store.state.ide.flowData;
      this.currentProject = this.projectList.list.find(item => item.project_id === data.project_id);
      this.dataFlowInfo = JSON.parse(JSON.stringify(data));
      if (!data.tdw_conf || data.tdw_conf === '{}') {
        this.params = this.getParams();
      } else {
        this.params = {
          tdw_conf: JSON.parse(this.$store.state.ide.flowData.tdw_conf),
        };
      }
      this.isCheckEnable = this.checkedEnable();
      if (this.isCheckEnable) {
        this.enableTdw = true;
        this.hasPower = 'hasPower';
        if (!this.applicationGroupLoading) {
          this.changeCheckTdw(this.enableTdw);
          this.requestApplicationDetail();
        }
      } else {
        this.enableTdw = false;
        this.hasPower = '';
      }
    },
    getParams() {
      return {
        tdw_conf: {
          task_info: {
            spark: {
              spark_version: '',
            },
          },
          gaia_id: '',
          tdwAppGroup: '',
          sourceServer: '',
        },
      };
    },
    confirm() {
      this.nameTips();
      if (this.enableTdw) {
        this.validateApplication();
        this.validateSourceServer();
        this.validateComputingCluster(false); // 仅做验证，不赋值
        this.validateClusterVersion();
      }
      const { nameError, applicationError, sourceServerError, clusterVersionError, computingClusterError } = this.error;
      if (!nameError) {
        if (this.enableTdw && (applicationError || sourceServerError || clusterVersionError || computingClusterError)) {
          return false;
        }
        this.updataTask();
      }
    },
    updataTask() {
      const { description } = this.dataFlowInfo;
      const flowName = this.dataFlowInfo.flow_name;
      const projectId = this.dataFlowInfo.project_id;
      const data = Object.assign({ tdw_conf: {} }, { description, flow_name: flowName, project_id: projectId });
      if (this.enableTdw) {
        Object.assign(data, this.params);
      }
      this.btnLoading = true;
      this.axios
        .patch(`v3/dataflow/flow/flows/${this.dataFlowInfo.flow_id}/`, data)
        .then(res => {
          if (res.result) {
            postMethodWarning(this.$t('成功'), 'success');
            this.show = false;
            this.$store.commit('ide/setFlowData', Object.assign(this.dataFlowInfo, res.data));
          } else {
            postMethodWarning(res.message, 'error');
          }
        })
        ['finally'](() => {
          this.btnLoading = false;
        });
    },
  },
};
</script>

<style lang="scss">
.flow-detail-table {
  border: 1px solid #e6e6e6;
  border-right: none;
  border-bottom: none;
  border-spacing: unset;
}
.flow-detail-input {
  padding: 0px;
}
.dfDetail-button {
  border-top: 1px solid #e5e5e5;
  text-align: center;
  padding: 20px 0 20px;
  background: #fafafa;
  button {
    width: 138px;
  }
}
.dfDetail-content {
  padding: 25px 25px 0;
  .title {
    display: inline-block;
    vertical-align: middle;
  }
  .dfDetail-btn-edit {
    margin-left: 10px;
  }
  .is-danger.bk-form-input,
  .is-danger .bk-selector-input {
    border-color: #ff5656;
    background-color: #fff4f4;
    color: #ff5656;
  }
}
.dfdetail {
  height: 60px;
  line-height: 1;
  border-bottom: 1px solid #e6e9f0;
  td.label {
    width: 148px;
    color: #a0a3ac;
    padding-right: 17px;
    text-align: right;
    border-right: 1px solid #e6e9f0;
  }
  td.df-content {
    width: 275px;
    color: #212232;
    padding: 0 17px;
    text-align: left;
    border-right: 1px solid #e6e9f0;
    .bk-form-checkbox {
      margin-right: 0;
      i {
        font-style: normal;
      }
    }
  }
  .td-link-notpower {
    vertical-align: middle;
    text-decoration: underline;
  }
}

.dialog-pop .bk-dialog-footer {
  height: 155px;
  border-top: 1px solid #e5e5e5;
  background-color: #fafafa;
}

.dialog-pop .bk-dialog-footer .bk-dialog-outer button {
  margin-top: 55px;
  width: 138px;
}
</style>
