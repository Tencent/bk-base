

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
  <div>
    <bkdata-dialog
      v-model="newTaskDialog.isShow"
      extCls="bkdata-dialog none-padding"
      :loading="isLoading"
      :width="newTaskDialog.width"
      :maskClose="newTaskDialog.quickClose"
      :okText="$t('创建')"
      :cancelText="$t('取消')"
      @confirm="createNewTask"
      @cancel="close">
      <div class="new-project">
        <div class="hearder">
          <div class="text fl">
            <p class="title">
              <i class="bk-icon icon-dataflow" />
              {{ $t('创建任务') }}
            </p>
            <p>{{ $t('请确认已有原始数据任务使用') }}</p>
          </div>
        </div>
        <div class="content">
          <div class="bk-form">
            <div class="bk-form-item">
              <label class="bk-label"><span class="required">*</span>{{ $t('所属项目') }}：</label>
              <div class="bk-form-content">
                <bkdata-selector
                  :selected.sync="params.project_id"
                  :placeholder="project.placeholder"
                  :disabled="project.isDisabled"
                  :searchable="true"
                  :list="project.projectLists"
                  :settingKey="'project_id'"
                  :displayKey="'project_name'" />
              </div>
            </div>
            <div class="bk-form-item">
              <label class="bk-label"><span class="required">*</span>{{ $t('任务名称') }}：</label>
              <div class="bk-form-content">
                <bkdata-input
                  v-model.trim="params.flow_name"
                  autofocus="autofocus"
                  :class="[{ 'is-danger': error.nameError }]"
                  :maxlength="50"
                  :placeholder="$t('请输入任务名称_50个字符限制')"
                  @input="tips()" />
                <div v-if="error.nameError"
                  class="bk-form-tip">
                  <p class="bk-tip-text">
                    {{ error.tips }}
                  </p>
                </div>
              </div>
            </div>
            <div class="bk-form-item">
              <label class="bk-label">{{ $t('任务描述') }}：</label>
              <div class="bk-form-content">
                <textarea
                  id=""
                  v-model.trim="params.description"
                  name=""
                  :maxlength="50"
                  class="bk-form-textarea"
                  :placeholder="$t('请输入任务描述_50个字符限制')" />
              </div>
            </div>
            <!-- TDW支持组件 -->
            <component :is="tdwFragments.componment"
              v-bind="tdwFragments.bindAttr"
              @changeCheckTdw="changeCheckTdw"
              @tipsClick="$refs.projectEdit.showProjectDetails()"
              @applicationDetail="requestApplicationDetail"
              @sourceServer="validateSourceServer"
              @computingCluster="validateComputingCluster"
              @clusterVersion="validateClusterVersion" />
          </div>
        </div>
      </div>
    </bkdata-dialog>
    <project-detail ref="projectEdit"
      :item="itemEx"
      :project_id="params.project_id"
      @close="close"
      @open="open" />
  </div>
</template>
<script>
import tdwPower from '@/pages/DataGraph/Common/tdwPower.js';
import projectDetail from '../Dialog/projectDetail';
import { postMethodWarning } from '@/common/js/util.js';
import extend from '@/extends/index';

export default {
  components: {
    projectDetail,
  },
  mixins: [tdwPower],
  props: {
    isCreated: {
      type: Boolean,
    },
    newTask: {
      type: Boolean,
    },
  },
  data() {
    return {
      supportType: 'task',
      dialogLoading: true,
      itemEx: {},
      isLoading: false,
      tdwDetailLoading: false,
      currentProject: {},
      project: {
        projectLists: [],
        placeholder: this.$t('请选择项目'),
        isDisabled: false,
      },
      newTaskDialog: {
        hasHeader: false,
        hasFooter: false,
        quickClose: false,
        width: 576,
        isShow: false,
      },
      params: {},
      error: {
        nameError: false,
        tips: '',
        applicationError: false,
        applicationTips: '',
        sourceServerError: false,
        sourceServerTips: '',
        clusterVersionError: false,
        clusterVersionTips: '',
        computingClusterError: false,
        computingClusterTips: '',
      },
    };
  },
  computed: {
    /** 获取TDW支持组件 */
    tdwFragments() {
      const tdwFragments = extend.getVueFragment('tdwFragment', this) || [];
      return tdwFragments.filter(item => item.name === 'TdwSupport')[0];
    }
  },
  watch: {
    isCreated(val) {
      this.isLoading = !val;
    },
  },
  created() {
    this.params = this.getParams();
  },
  methods: {
    getProjectList(project) {
      this.project.placeholder = this.$t('数据加载中');
      this.project.isDisabled = true;
      let param = {
        add_del_info: 1,
        add_flow_info: 1,
        ordering: 'active',
      };
      this.project.projectLists = [];
      this.axios
        .get('projects/list_my_projects/?' + this.qs.stringify(param))
        .then(res => {
          if (res.result) {
            res.data.projects.forEach(item => {
              this.project.projectLists.push(item);
            });
            this.project.placeholder = this.$t('请选择项目');
            if (project) {
              this.currentProject = this.project.projectLists
                .find(item => item.project_id === project.project_id);
            }
          } else {
            this.getMethodWarning(res.message, res.code);
            this.project.placeholder = this.$t('数据加载失败');
          }
        })
        ['finally'](() => {
          this.project.isDisabled = false;
        });
    },
    requestApplicationDetail() {
      this.resetAppDetailParams();
      this.tdwDetailLoading = true;
      this.$store
        .dispatch('tdw/getApplicationDetail', {
          appGroupName: this.params.tdw_conf.tdwAppGroup,
        })
        .then(res => {
          if (res.result) {
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
              postMethodWarning(content, 'error', { htmlContent: true });
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
    projectChange(value, item) {
      this.currentProject = item;
      this.resetTdwParams();
      this.changeCheckTdw(this.enableTdw);
    },
    changeCheckTdw(checked) {
      // 任务-应用组列表 是从项目参数转换而来
      if (!this.params.project_id) {
        return false;
      }
      const enableTdw = checked;
      this.enableTdw = enableTdw;
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
    open(item) {
      this.itemEx = item;
      this.getProjectList(item);
      this.params = this.getParams(item);
      this.enableTdw = false;
      this.hasPower = '';
      this.error.nameError = false;
      this.error.applicationError = false;
      this.error.sourceServerError = false;
      this.error.clusterVersionError = false;
      this.error.computingClusterError = false;
      this.newTaskDialog.isShow = true;
      this.$emit('dialogChange', true);
    },
    close() {
      this.newTaskDialog.isShow = false;
      this.$emit('dialogChange', false);
      this.params = this.getParams();
    },
    tips() {
      if (this.params.flow_name) {
        this.error.nameError = false;
      } else {
        this.error.nameError = true;
        this.error.tips = this.validator.message.required;
      }
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
    validateComputingCluster() {
      if (this.params.tdw_conf.gaia_id) {
        this.error.computingClusterError = false;
        let selectObj = this.computingClusterSelect.list.filter(item => {
          return item.gaia_id === this.params.tdw_conf.gaia_id;
        });
        if (!this.params.tdw_conf.task_info.spark.spark_version) {
          this.params.tdw_conf.task_info.spark.spark_version = Object.keys(selectObj[0].gaia_version)[0]
                    || ''; // 如果所选计算集群有version，自动选择，否则置空，由用户选择
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
    createNewTask() {
      this.tips();
      if (this.enableTdw) {
        this.validateApplication();
        this.validateSourceServer();
        this.validateComputingCluster();
        this.validateClusterVersion();
      }
      const {
        nameError,
        applicationError,
        sourceServerError,
        clusterVersionError,
        computingClusterError
      } = this.error;
      if (!nameError) {
        if (this.enableTdw
                && (applicationError || sourceServerError || clusterVersionError || computingClusterError)) {
          this.isLoading = true;
          setTimeout(() => {
            this.isLoading = false;
          }, 0);
          return false;
        }
        this.isLoading = true;
        const data = JSON.parse(JSON.stringify(this.params));
        if (!this.enableTdw) {
          // 如果没有启用tdw,需要删掉相关属性
          delete data.tdw_conf;
        }
        this.$emit('createNewTask', data);
      } else {
        this.isLoading = true;
        setTimeout(() => {
          this.isLoading = false;
        }, 0);
      }
    },
    getParams(item) {
      return {
        project_id: item ? item.project_id : '',
        flow_name: '',
        description: '',
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
    resetAppDetailParams() {
      this.params.tdw_conf.sourceServer = '';
      this.params.tdw_conf.gaia_id = '';
      this.params.tdw_conf.task_info.spark.spark_version = '';
    },
  },
};
</script>
<style media="screen" lang="scss">
.new-project {
  .hearder {
    height: 70px;
    background: #23243b;
    position: relative;
    .close {
      display: inline-block;
      position: absolute;
      right: 0;
      top: 0;
      width: 40px;
      height: 40px;
      line-height: 40px;
      text-align: center;
      cursor: pointer;
    }
    .icon {
      font-size: 32px;
      color: #abacb5;
      line-height: 60px;
      width: 142px;
      text-align: right;
      margin-right: 16px;
    }
    .text {
      margin-left: 28px;
      font-size: 12px;
    }
    .title {
      margin: 16px 0 3px 0;
      padding-left: 36px;
      position: relative;
      color: #fafafa;
      font-size: 18px;
      text-align: left;
      i {
        position: absolute;
        left: 10px;
        top: 2px;
      }
    }
  }
  .content {
    max-height: 500px;
    padding: 30px 0 45px 0;
    overflow-y: auto;
    .bk-form {
      width: 480px;
      .bk-label {
        // padding:11px 16px 11px 10px;
        width: 180px;
        .bk-form-checkbox {
          margin-right: 5px;
        }
        .bk-checkbox-text {
          font-style: normal;
          font-weight: normal;
          cursor: pointer;
          vertical-align: middle;
        }
      }
      .bk-form-content {
        margin-left: 180px;
      }
      .tdw-form-content {
        display: flex;
        align-items: end;
      }
    }
    .bk-form-action {
      margin-top: 20px;
    }
    .required {
      color: #f64646;
      display: inline-block;
      vertical-align: -2px;
      margin-right: 5px;
    }
    .bk-button {
      min-width: 120px;
    }
    .bk-label-checkbox {
      width: 79px !important;
      margin: 0;
      padding-top: 9px;
      padding-bottom: 9px;
    }
    .bk-form-not-power {
      margin: 0 !important;
      line-height: 25px !important;
      a {
        vertical-align: sub;
        text-decoration: underline;
      }
    }
    .bk-form-power-loading {
      height: 36px;
    }
  }
}
</style>
