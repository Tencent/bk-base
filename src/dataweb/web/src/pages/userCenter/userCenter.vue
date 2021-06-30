

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
    :withMargin="false"
    class="bkdata-user-center">
    <div class="user-center">
      <div v-if="projectLists.length === 0 && activeName === 'project'"
        v-show="!loading.contentsLoading"
        class="no-project">
        <div>
          <span v-show="radio.name === 'releted'"><i class="bk-icon icon-arrows-up" /></span>
          <template v-if="radio.name === 'releted'">
            {{ $t('无项目_请创建项目') }}
          </template>
          <template v-else>
            {{ $t('暂无数据') }}
          </template>
        </div>
      </div>
      <div class="tabs">
        <bkdata-tab :active.sync="activeName"
          type="unborder-card">
          <bkdata-tab-panel v-if="$modules.isActive('dataflow') && !isFlowIframe"
            :label="$t('我的项目')"
            name="project">
            <template v-if="activeName==='project'">
              <ProjectList ref="refProjectList"
                :activeName="activeName"
                :activeType="radio.name"
                :projectItems="projectLists"
                :contentsLoading="loading.contentsLoading"
                @link-to-df="linkToDf"
                @project-type-changed="handleProjectTypeChanged"
                @project-created="handleProjectCreated" />
            </template>
          </bkdata-tab-panel>
          <bkdata-tab-panel v-if="$modules.isActive('dataflow') && !isFlowIframe"
            :label="$t('我的任务')"
            name="dataflow">
            <div v-if="activeName === 'dataflow' && /user-center$/.test($route.path)"
              class="tabs-project-opbar">
              <!-- 匹配路由'user-center'是为了防止路由激活取消当前请求 -->
              <my-task ref="mytask"
                :selectedProjectId="selectedProject"
                :activeProject="project"
                :activeName="activeName"
                @clearProject="clearProject"
                @taskLoading="taskLoading" />
            </div>
          </bkdata-tab-panel>
          <template v-for="(comp, index) in userCenterFragments">
            <bkdata-tab-panel v-if="comp.validate()"
              :key="index"
              :label="comp.label"
              :name="comp.name">
              <template v-if="activeName === comp.name">
                <component :is="comp.componment"
                  v-bind="comp.bindAttr" />
              </template>
            </bkdata-tab-panel>
          </template>
          <bkdata-tab-panel v-if="$modules.isActive('dmonitor_center') && !isFlowIframe"
            :label="$t('我的告警')"
            name="alert">
            <div v-show="activeName === 'alert'"
              class="tabs-project-opbar">
              <project-bar :activeName="activeName"
                :scenarios="'alert'" />
            </div>
            <div v-if="activeName === 'alert'"
              class="tabs-project-opbar">
              <alertTable-list />
            </div>
          </bkdata-tab-panel>
          <bkdata-tab-panel v-if="$modules.isActive('udf') && !isFlowIframe"
            :label="$t('我的函数')"
            name="function">
            <div v-if="activeName === 'function' && /user-center$/.test($route.path)"
              class="tabs-project-opbar">
              <!-- 匹配路由'user-center'是为了防止路由激活取消当前请求 -->
              <my-function :activeName="activeName" />
            </div>
          </bkdata-tab-panel>
          <bkdata-tab-panel v-if="$modules.isActive('resource_manage') && !isFlowIframe"
            :label="$t('资源管理')"
            name="resourceGroup">
            <div v-show="activeName === 'resourceGroup' && /user-center$/.test($route.path)">
              <ResourceCenter ref="resourceCenter"
                :groupItemCount="groupItemCount" />
            </div>
          </bkdata-tab-panel>
        </bkdata-tab>
      </div>
      <!-- 新建项目start -->
      <div>
        <new-project ref="project"
          :idNum="2"
          @createNewProject="createNewProject" />
      </div>
      <!-- 新建项目end -->

      <!-- 新建任务start -->
      <div>
        <new-task ref="newtask"
          :isCreated="isCreated"
          @createNewTask="createNewTask" />
      </div>
      <!-- 新建任务end -->

      <!-- 申请敏感数据start -->
      <div class="sensitive-data">
        <apply-sensitivedata ref="sensitive" />
      </div>
    </div>
  </Layout>
</template>
<script>
import { postMethodWarning } from '@/common/js/util.js';
import myTask from './components/myTask';
import myFunction from './components/UDFunction';
import newProject from '@/pages/DataGraph/Graph/Components/project/newProject';
import newSampleSet from '@/pages/DataGraph/Graph/Components/project/newSampleSet';
import vEditPop from '@/components/pop/dataFlow/editPop';
import applySensitivedata from './components/applySensitiveData';
import Bus from '@/common/js/bus.js';
import Layout from '../../components/global/layout';
import alertTableList from '../dmonitorCenter/alertTableList';
import ResourceCenter from './ResourceManage/ResourceCenter';
import { UserCenterNaviList, UserCenterNavi } from './subNaviConfig';
import ProjectList from './components/projectList.vue';
import projectBar from './components/projectOperationBar';
import newTask from '@/pages/DataGraph/Graph/Components/project/newTask';
import extend from '@/extends/index';

export default {
  components: {
    Layout,
    myTask,
    myFunction,
    vEditPop,
    applySensitivedata,
    newProject,
    alertTableList,
    ResourceCenter,
    ProjectList,
    projectBar,
    newTask
  },

  data() {
    return {
      currentPageUpdating: false,
      currentPage: 1,
      pageSize: 50,
      // 成员管理
      isCreated: false,

      guideShow: false, // 新手指引
      project: {},
      searchContent: '',
      activeName: 'project',
      todoNumber: {
        // 待办数量
        // @todo 从接口获取待办数量
        approve: 0,
      },
      radio: {
        name: 'releted',
      },
      projectLists: [], // 所有的项目列表

      loading: {
        mainLoading: true,
        contentsLoading: true,
        projectDetailLoading: false,
        projectEditLoading: false,
      },
      selectedProject: 0,
      isCreateSample: false,
      userCenterNaviList: new UserCenterNaviList(),
      groupItemCount: 1,
    };
  },
  computed: {
    userCenterFragments() {
      return extend.getVueFragment('userCenterFragment', this) || [];
    },
    /** 是否被iframe 嵌入 */
    isFlowIframe() {
      return this.$route.query.flowIframe;
    },
    isModeflowRedirect() {
      return window.BKBASE_Global.modelFlowUrl;
    },
    taskCount() {
      return this.todoNumber.approve < 100 ? this.todoNumber.approve : '99+';
    },
  },
  watch: {
    activeName: function (newVal) {
      if (newVal === 'project') {
        // 添加路由参数
        this.$router.push({
          path: '/user-center',
          query: { tab: 'project' },
        });
        this.selectedProject = null; // 切回project时，清空selectedProject
        // this.init();
      } else if (newVal === 'dataflow') {
        // 添加路由参数
        this.$router.push({
          path: '/user-center',
          query: {
            tab: 'dataflow',
            projectName: this.project.project_name,
          },
        });
      } else if (newVal === 'function') {
        // 添加路由参数
        this.$router.push({
          path: '/user-center',
          query: { tab: 'function' },
        });
        this.selectedProject = null; // 切回project时，清空selectedProject
      } else if (newVal === 'alert') {
        // 添加路由参数
        this.$router.push({
          path: '/user-center',
          query: {
            tab: 'alert',
            alert_target_type: this.$route.query.alert_target_type,
            alert_target_id: this.$route.query.alert_target_id,
            alert_target_alias: this.$route.query.alert_target_alias,
          },
        });
      } else if (newVal === 'resourceGroup') {
        this.$router.push({
          path: '/user-center',
          query: { tab: 'resourceGroup' },
        });
        this.$refs.resourceCenter.getGroupList();
      }

      this.userCenterFragments.forEach(comp => {
        comp.activeNameChanged(newVal);
      });
    },
    '$route.query.tab': {
      handler(newVal) {
        if (newVal) {
          this.activeName = newVal;
        }
      },
    },
  },
  mounted() {
    this.activeName = this.$route.query.tab || 'project';
    this.getProjectList();
    this.initNaviConfig();
    Bus.$on('clearSelectedProjectId', () => {
      this.selectedProject = null;
    });
    Bus.$on('showCreateNewTaskWindow', data => {
      this.$refs.newtask && this.$refs.newtask.open(data);
    });
    Bus.$on('showCreateNewProjectWindow', () => {
      this.$refs.project && this.$refs.project.open();
    });
    Bus.$on('applySensitiveData', () => {
      this.$refs.sensitive.show();
    });
    Bus.$on('showCreateNewSampleSetWindow', data => {
      this.isCreateSample = true;
      this.$nextTick(() => {
        this.$refs.newsampleset && this.$refs.newsampleset.open(data);
      });
    });
    window.onscroll = () => {
      if (this.$route.name === 'user_center') {
        let top = document.getElementsByTagName('html')[0].scrollTop;
        let bar = document.querySelectorAll('.el-tabs__header')[0];
        if (top > 60) {
          bar.classList.add('fixed');
        } else {
          bar.classList.remove('fixed');
        }
      } else {
        window.onscroll = null;
      }
    };
  },
  methods: {
    /* @description
       * 获取项目列表
       */
    getProjectList(params) {
      this.loading.contentsLoading = true;
      let param;
      if (!params) {
        param = {
          add_del_info: 1,
          add_flow_info: 1,
          ordering: 'active',
          add_model_info: 1,
          add_note_info: 1,
        };
      } else {
        param = params;
      }
      this.bkRequest
        .httpRequest('dataFlow/getMyProjectList', {
          query: param,
        })
        .then(res => {
          if (res.result) {
            this.projectLists = [];
            this.$nextTick(() => {
              this.$set(this, 'projectLists', res.data.projects);
            });
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.loading.contentsLoading = false;
        });
    },
    createNewTask(params) {
      this.isCreated = false;
      this.bkRequest
        .httpRequest('dataFlow/createNewTask', {
          params,
        })
        .then(res => {
          if (res.result) {
            postMethodWarning(this.$t('成功'), 'success', { delay: 1500 });
            this.handleTaskCreated(res.data);
            // this.$refs.refProjectList && this.$refs.refProjectList.updateProjectList(res.data);
            this.getProjectList();
            this.$refs.newtask.close();

          } else {
            postMethodWarning(res.message, 'error');
          }
        })
        ['finally'](() => {
          this.isCreated = true;
        });
    },
    handleProjectTypeChanged(type) {
      this.radio.name = type;
      let params;
      if (type === 'releted') {
        params = {
          add_del_info: 1,
          add_flow_info: 1,
          active: 1,
          add_model_info: 1,
        };
      } else if (type === 'manage') {
        params = {
          add_del_info: 1,
          add_flow_info: 1,
          active: 1,
          role: 'admin',
          add_model_info: 1,
        };
      } else {
        params = {
          active: 0,
          add_del_info: 1,
          add_flow_info: 1,
          disabled: 1,
          add_model_info: 1,
        };
      }
      this.getProjectList(params);
    },
    handleProjectCreated() {
      this.getProjectList();
    },
    changeProject(data) {
      this.selectedProject = data;
    },

    clearProject() {
      this.project = {};
    },
    taskLoading(data) {
      this.loading.mainLoading = data;
      this.loading.contentsLoading = data;
    },
    linkToDf(item) {
      if (this.radio.name === 'delete') return;
      this.activeName = 'dataflow';
      this.selectedProject = item.project_id;
      this.project = item;
    },
    createProjectSuccess(data, params) {
      let list = {};
      list.normal_count = 0;
      list.no_start_count = 0;
      list.exception_count = 0;
      list.active = true;
      list.project_id = data;
      list.project_name = params.project_name;
      list.description = params.description;
      this.$refs.refProjectList.updateProjectList(list);
    },

    /** @description
       *  创建新项目
       */
    showCreateNewProjectWindow() {
      this.$refs.project.open();
    },
    async createNewProject(params) {
      let createRes = null;
      await this.bkRequest
        .httpRequest('meta/creatProject', {
          params,
        })
        .then(res => {
          createRes = res;
        });

      if (!createRes.result) {
        postMethodWarning(createRes.message, 'error');
        this.$refs.project.reset();
        return;
      }
      let project_id = createRes.data;

      // 补充添加人员
      await this.bkRequest.httpRequest('auth/addProjectMember', {
        params: {
          project_id,
          role_users: [
            { role_id: 'project.manager', user_ids: params.admin },
            {
              role_id: 'project.flow_member',
              user_ids: params.member,
            },
          ],
        },
      });

      // 补充申请业务数据
      for (let bizId of params.biz_ids) {
        await this.bkRequest.httpRequest('auth/applyBiz', {
          params: {
            action: 'result_table.query_data',
            reason: 'Initial businesses after creating',
            subject_class: 'project',
            subject_id: projectId,
            object_class: 'result_table',
            scope: { bk_biz_id: bizId },
          },
        });
      }

      // 创建成功后，必要的处理内容
      this.search = '';
      postMethodWarning(this.$t('成功'), 'success', { delay: 1500 });
      this.$refs.project.close();
      this.createProjectSuccess(createRes.data, params);
      this.$refs.project.reset();
    },

    /** 初始化导航 */
    initNaviConfig() {
      [
        {
          name: 'project',
          query: {},
          init: () => {
            this.selectedProject = null; // 切回project时，清空selectedProject
            // this.init();
          },
        },
        { name: 'dataflow', query: { projectName: this.project.project_name } },
        { name: 'account', query: {} },
        { name: 'sample', query: { projectName: this.project.project_name } },
        { name: 'model', query: { projectName: this.project.project_name } },
        { name: 'function', query: {} },
        {
          name: 'alert',
          query: {
            alert_target_type: this.$route.query.alert_target_type,
            alert_target_id: this.$route.query.alert_target_id,
            alert_target_alias: this.$route.query.alert_target_alias,
          },
        },
        { name: 'resourceGroup', query: {} },
      ].forEach(item => {
        this.userCenterNaviList.push(new UserCenterNavi(item.name, '/user-center', item.query));
      });
    },

    /**
         * 创建任务成功更新Task列表
         */
    handleTaskCreated(item) {
      this.$refs.mytask && this.$refs.mytask.newTaskSuccess(item);
    }
  },
};
</script>
<style lang="scss" scoped>
.bkdata-user-center {
  ::v-deep .layout-body {
    overflow: auto !important;
  }

  ::v-deep .layout-content {
    height: 100% !important;
    padding: 0 !important;
  }

  ::v-deep .bk-tab-header {
    padding: 0 30px;
  }

  ::v-deep .bk-tab-section {
    padding: 20px 0px !important;
  }

  ::v-deep .bkdata-virtue-content {
    padding: 0 40px;
    // height: auto;
  }
}
.user-center {
  overflow: hidden;
  height: 100%;
  .project-detail {
    .close-pop {
      font-size: 12px;
      color: #747a87;
      padding: 10px;
      position: absolute;
      right: 0;
      top: 0;
      font-weight: 700;
      z-index: 2000;
    }
  }
  .data-powerManage {
    .inquire-content {
      padding: 5px 0 20px 0;
      .inquire {
        margin-right: 20px;
      }
    }
  }

  .tabs {
    .bk-tab-label-wrapper {
      margin: 0 -70px;
      padding: 0 70px;
      background: #efefef;
    }
  }

  .no-project {
    position: absolute;
    top: 95px;
    left: 0;
    right: 0;
    bottom: 0;
    background: #fff;
    div {
      position: absolute;
      left: 50%;
      top: 50%;
      transform: translate(-50%, -50%);
      font-size: 17px;
      color: #737987;
      span {
        display: inline-block;
        width: 24px;
        height: 24px;
        background: #3a84ff;
        margin-right: 5px;
        i {
          position: relative;
          top: 2px;
          left: 2px;
          display: inline-block;
          color: #fff;
          -webkit-transform: rotate(-45deg);
          transform: rotate(-45deg);
        }
      }
    }
  }
  .tabs {
    position: relative;
    padding-bottom: 20px;

    ::v-deep .bk-tab-label-list {
        &.bk-tab-label-list-has-bar {
            &::after {
                display: none;
            }
        }

        .bk-tab-label-item {
            &.active {
                &::after {
                    content: "";
                    position: absolute;
                    bottom: 0;
                    left: 10px;
                    height: var(--activeBarHeight);
                    width: var(--activeBarWidth);
                    background: #3a84ff;
                }
            }
        }
    }
  }
  .dialog,
  .project-detail {
    .close-pop {
      font-size: 12px;
      color: #747a87;
      padding: 10px;
      position: absolute;
      right: 0;
      top: 0;
      font-weight: 700;
      z-index: 2000;
    }
    .title {
      height: 20px;
      line-height: 20px;
      border-left: 4px solid #3a84ff;
      padding-left: 15px;
      color: #212232;
      font-size: 16px;
    }
    .bk-dialog-footer {
      height: 80px;
      line-height: 80px;
      .bk-dialog-outer {
        text-align: center;
        button {
          width: 120px;
        }
      }
    }
  }
}
.bkdata-en .user-center {
  .tabs {
    .contents-item-mid {
      .dataflows {
        & .title {
          display: block;
          width: 48px;
          padding: 0;
        }
        .dataflows-status {
          div {
            white-space: nowrap;
          }
          > div:last-of-type {
            min-width: 62px;
          }
        }
      }
    }
  }
}
.tabs-project-opbar {
  padding: 0 50px;
  h3 {
    padding: 10px 0;
    font-size: 16px;
    color: #212232;
  }
}
</style>
