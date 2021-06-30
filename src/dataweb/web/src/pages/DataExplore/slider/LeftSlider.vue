/** 数据探索左栏菜单 */


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
  <div class="left-slider">
    <!-- 左栏头部 -->
    <div class="left-slider-header">
      <div class="header-tab clearfix">
        <span
          v-for="tab in headerTabs"
          :key="tab.id"
          :class="[
            'tab-item',
            { 'tab-active': curType === tab.id },
            { disabled: isContentLoading && curType !== tab.id },
          ]"
          @click.stop="handleToggleType(tab.id)">
          {{ tab.name }}
        </span>
      </div>
      <div v-if="isProject"
        class="project-seletor">
        <bkdata-selector
          ref="projectSelector"
          :selected.sync="projectId"
          :list="projectList"
          :settingKey="'project_id'"
          :displayKey="'disName'"
          class="left-slider-select"
          :isLoading="projectLoading"
          :searchable="true"
          :clearable="false"
          :placeholder="$t('请选择项目')"
          :optionTip="true"
          :toolTipTpl="getProjectInfo"
          :popoverOptions="{ appendTo: 'parent' }"
          :disabled="notebookLoading"
          @change="changeProject">
          <template slot="bottom-option">
            <div v-if="isProject"
              slot="extension"
              class="extension-btns">
              <span class="icon-btn"
                @click="handlePermissionApply">
                <i class="icon-apply-2 icon-item" />
                {{ $t('申请权限') }}
              </span>
              <span class="icon-btn"
                @click="handleNewProject">
                <i class="icon-plus-circle icon-item" />
                {{ $t('创建项目') }}
              </span>
            </div>
          </template>
        </bkdata-selector>
      </div>
    </div>
    <!-- 左栏任务列表 -->
    <div class="query-list"
      :style="{ height: leftSideBarHeight }">
      <bkdata-collapse v-model="activeName"
        class="my-menu"
        @item-click="handleToggleCollapse">
        <drag-layout
          layout="vertical"
          splitColor="#DCDEE5"
          :class="['drag-layout', { 'is-disabled': !isNotebookActive }]"
          :gutter="2"
          :max="50"
          :init="35"
          :min="30"
          :disabled="!isNotebookActive">
          <template slot="1">
            <bkdata-collapse-item name="notebook"
              :customTriggerArea="true"
              :hideArrow="true">
              <div class="custom-item-header">
                <span class="custom-item-name">
                  <i class="icon-notebook-2 name-icon" />
                  笔记
                </span>
                <div class="custom-item-operations">
                  <template v-if="hasOperationAuth">
                    <i
                      v-if="isNotebookActive"
                      v-bk-tooltips="setTipsConfig($t('新建笔记'))"
                      class="icon-plus head-icons"
                      @click.stop="createNotebook" />
                  </template>
                  <i
                    v-bk-tooltips="setTipsConfig(setTipsText('notebook'))"
                    :class="['head-icons', getCollapseIcon('notebook')]" />
                </div>
              </div>
              <div slot="content">
                <div
                  v-bkloading="{
                    isLoading: notebookLoading || projectLoading,
                    title: notebookLoadingText,
                  }"
                  class="query-list-main">
                  <template v-for="(item, index) in notebooks">
                    <div :key="item.notebook_id"
                      class="query-list-item">
                      <div
                        v-if="curEditData.id === item.notebook_id
                          && curEditData.type === 'notebook'"
                        class="item-rename">
                        <bkdata-input
                          ref="rename"
                          v-model="curEditData.name"
                          @enter="handleRenameNotebook(item, index)"
                          @blur="handleRenameNotebook(item, index)" />
                      </div>
                      <div
                        v-else
                        v-bk-tooltips="setItemTips(item.notebook_name)"
                        class="item-head"
                        :class="{ hover: item.isHover }">
                        <div class="item-name"
                          @click="handleNotebookClick(item)">
                          {{ item.notebook_name }}
                        </div>
                        <div class="item-operations">
                          <i
                            v-tooltip.top="setTipsConfig($t('重命名'))"
                            class="icon-edit-big btn-icon mr5"
                            @click.stop="handleShowRename(item, 'notebook')" />
                          <i
                            v-tooltip.top="setTipsConfig($t('克隆'))"
                            class="icon-data-copy btn-icon"
                            @click="handleClone($event, item)" />
                          <i
                            v-tooltip.top="setTipsConfig($t('删除'))"
                            class="icon-delete btn-icon"
                            @click.stop="handleDeleteNotebook(item, index)" />
                        </div>
                      </div>
                    </div>
                  </template>
                </div>
              </div>
            </bkdata-collapse-item>
          </template>
          <template slot="2">
            <bkdata-collapse-item name="dataquery"
              :customTriggerArea="true"
              :hideArrow="true">
              <div class="custom-item-header">
                <span class="custom-item-name"
                  @click="handleClone">
                  <i class="icon-icon-audit name-icon" />
                  查询
                </span>
                <div class="custom-item-operations">
                  <template v-if="hasOperationAuth">
                    <i
                      v-bk-tooltips="setTipsConfig($t('新建查询'))"
                      class="icon-plus head-icons"
                      @click.stop="addTask" />
                  </template>
                  <i
                    v-bk-tooltips="setTipsConfig(setTipsText('dataquery'))"
                    :class="['head-icons', getCollapseIcon('dataquery')]" />
                </div>
              </div>
              <div slot="content">
                <div v-bkloading="{ isLoading: queryLoading || projectLoading }"
                  class="query-list-main">
                  <template v-for="(item, index) in queryList">
                    <div :key="item.query_id"
                      class="query-list-item">
                      <div v-if="curEditData.id === item.query_id && curEditData.type === 'query'"
                        class="item-rename">
                        <bkdata-input
                          ref="rename"
                          v-model="curEditData.name"
                          @enter="handleRenameQuery(item)"
                          @blur="handleRenameQuery(item)" />
                      </div>
                      <div v-else
                        v-bk-tooltips="setItemTips(item.query_name)"
                        class="item-head">
                        <div class="item-name"
                          @click="chooseTask(item, index)">
                          {{ item.query_name }}
                        </div>
                        <div class="item-operations">
                          <i
                            v-bk-tooltips="setTipsConfig($t('重命名'))"
                            class="icon-edit-big btn-icon mr5"
                            @click.stop="handleShowRename(item, 'query')" />
                          <i
                            v-bk-tooltips="setTipsConfig($t('删除'))"
                            class="icon-delete btn-icon"
                            @click.stop="handleDeleteQuery(item, index)" />
                        </div>
                      </div>
                    </div>
                  </template>
                </div>
              </div>
            </bkdata-collapse-item>
          </template>
        </drag-layout>
      </bkdata-collapse>
    </div>
    <new-project ref="newproject"
      @createNewProject="handleCreatedProject" />
    <permission-apply ref="permissionApply"
      :defaultSelectValue="defaultSelectedValue" />
    <bkdata-dialog
      v-model="packageDialog.isShow"
      extCls="package-dialog"
      renderDirective="if"
      headerPosition="left"
      :width="640"
      :showFooter="false"
      :title="$t('库管理')">
      <package-management @close="handleClosePackage" />
    </bkdata-dialog>
    <!--克隆弹窗 -->
    <popContainer ref="clonePopContainer"
      v-bk-clickoutside="popHidden">
      <div v-bkloading="{ isLoading: btnLoading }"
        class="item-bounced">
        <div class="clone-content">
          <div class="clone-tip-title">
            笔记克隆
          </div>
          <div class="clone-form-content">
            <bkdata-form :labelWidth="80">
              <bkdata-form-item label="克隆至"
                :required="true">
                <bkdata-button :theme="'default'"
                  :class="isShow ? 'active' : ''"
                  @click="onProName">
                  项目
                </bkdata-button>
                <bkdata-button :theme="'default'"
                  :class="!isShow ? 'active' : ''"
                  @click="onPersonal">
                  个人
                </bkdata-button>
              </bkdata-form-item>
              <bkdata-form-item v-show="isShow"
                label="选择项目"
                :required="true">
                <bkdata-selector
                  ref="projectSelector"
                  :selected.sync="copyProjectId"
                  :list="projectList"
                  :settingKey="'project_id'"
                  :displayKey="'disName'"
                  class="left-slider-select"
                  :isLoading="projectLoading"
                  :searchable="true"
                  :clearable="false"
                  :placeholder="$t('请选择项目')"
                  :optionTip="true"
                  :toolTipTpl="getProjectInfo"
                  :popoverOptions="{ appendTo: 'parent' }"
                  :disabled="notebookLoading" />
              </bkdata-form-item>
            </bkdata-form>
          </div>
        </div>
        <div class="clone-form-button">
          <bkdata-button extCls="mr5"
            theme="primary"
            title="确定"
            @click="handleConfirm">
            确定
          </bkdata-button>
          <bkdata-button extCls="mr5"
            theme="default"
            title="取消"
            @click="handleCancel">
            取消
          </bkdata-button>
        </div>
      </div>
    </popContainer>

    <div class="clonePopMask"
      :style="{ display: maskDisplay ? 'block' : 'none' }" />
  </div>
</template>
<script>
import { bkOverflowTips } from 'bk-magic-vue';
import { postMethodWarning } from '@/common/js/util.js';
import newProject from '@/pages/DataGraph/Graph/Components/project/newProject.vue';
import permissionApply from '@/pages/authCenter/permissions/PermissionApplyWindow';
import packageManagement from '../components/PackageManagement.vue';
import bkStorage from '@/common/js/bkStorage.js';
import dragLayout from '@/components/dragableLaytout/index.vue';
import popContainer from '@/components/popContainer';

export default {
  components: {
    newProject,
    permissionApply,
    packageManagement,
    dragLayout,
    popContainer,
  },
  directives: {
    bkOverflowTips,
  },
  props: {
    reLoading: { type: [String, Number], default: '' },
    isDataAnalyst: {
      type: Boolean,
      default: true,
    },
  },
  data() {
    return {
      maskDisplay: false,
      activeBookItem: null,
      activeName: ['dataquery'],
      projectLoading: false,
      queryLoading: false,
      btnLoading: false,
      projectList: [],
      projectId: null,
      copyProjectId: null,
      taskId: '',
      queryList: [],
      notebooks: [],
      notebookLoading: false,
      activeNotebook: {},
      activeTaskIndex: -1,
      isNotebookFirstLoad: true,
      notebookLoadingText: window.$t('正在初始化笔记资源'),
      isNotebookLoaded: false,
      curType: 'personal',
      bkStorageKey: 'dataExploreProjectId',
      bkStorageKeyType: 'dataExploreTypeTrack',
      headerTabs: [
        {
          id: 'common',
          name: $t('项目'),
        },
        {
          id: 'personal',
          name: $t('个人'),
        },
      ],
      packageDialog: {
        isShow: false,
      },
      curEditData: {
        id: '',
        name: '',
        type: '',
        editing: false,
        awaitPrevious: null,
        previousResolve: null,
        previousReject: null,
      },
      showBounced: false,
      isShow: true,
    };
  },
  computed: {
    isContentLoading() {
      return this.notebookLoading || this.projectLoading;
    },
    isNotebook() {
      return !!this.$route.query.notebook;
    },
    isNotebookActive() {
      return this.activeName.includes('notebook');
    },
    isDataQueryActive() {
      return this.activeName.includes('dataquery');
    },
    isProject() {
      return this.curType === 'common';
    },
    routerParams() {
      return {
        ...this.$route.query,
        type: this.curType,
        projectId: this.isProject ? this.projectId : undefined,
      };
    },
    hasOperationAuth() {
      return this.isProject ? !this.isDataAnalyst : true;
    },
    defaultSelectedValue() {
      return {
        roleId: 'project.viewer',
        objectClass: 'project',
        scopeId: this.projectId,
      };
    },
    projectInfo() {
      return this.projectList.find(item => item.project_id === this.projectId) || {};
    },
    leftSideBarHeight() {
      return this.isProject ? 'calc(100% - 91px)' : 'calc(100% - 43px)';
    },
    leftTopIcon() {
      return this.isShow ? 'icon-data-shrink-line' : 'icon-data-expand-line';
    },
  },
  watch: {
    notebookLoading(val) {
      if (val) {
        if (this.isNotebookFirstLoad) {
          this.isNotebookFirstLoad = false;
        } else {
          this.notebookLoadingText = '';
        }
      }
    },
    reLoading(newVal, oldVal) {
      if (newVal !== oldVal) {
        this.getQueryList();
      }
    },
    isProject: {
      immediate: true,
      handler(val) {
        this.$emit('changeType', val);
      },
    },
  },
  mounted() {
    const { type, notebook } = this.$route.query;
    const storageType = bkStorage.get(this.bkStorageKeyType);
    if (Number(notebook)) {
      this.activeName.push('notebook');
    }
    // 优先取路由的type
    if (type) {
      if (type === 'common') {
        this.curType = 'common';
        this.getMineProjectList(true);
      } else {
        this.getProjectList();
      }
    } else if (storageType === 'common' && bkStorage.get(this.bkStorageKey)) {
      this.curType = 'common';
      this.getMineProjectList(true);
    } else {
      this.getProjectList();
    }
  },
  methods: {
    onPersonal() {
      this.isShow = false;
    },
    onProName() {
      this.isShow = true;
    },
    handleConfirm() {
      this.getNotebookClone();
    },
    handleCancel() {
      this.popHidden();
    },
    popHidden() {
      if (this.activeBookItem) {
        this.activeBookItem.isHover = false;
      }
      this.activeBookItem = null;
      this.maskDisplay = false;
      this.$refs.clonePopContainer.handlePopHidden();
    },
    handleClone(e, item) {
      this.activeBookItem = item;
      item.isHover = true;
      this.maskDisplay = true;
      this.$refs.clonePopContainer.handlePopShow(e, {
        placement: 'bottom',
        hideOnClick: false,
        maxWidth: '478px',
        multiple: true,
        interactive: true,
      });
    },
    setTipsConfig(content) {
      return {
        interactive: false,
        placement: 'top',
        zIndex: '100',
        boundary: document.body,
        content,
      };
    },
    setItemTips(content) {
      return {
        placement: 'right',
        boundary: document.body,
        interactive: false,
        content,
      };
    },
    setTipsText(name) {
      return this.activeName.includes(name) ? this.$t('收起') : this.$t('展开');
    },
    getCollapseIcon(name) {
      return this.activeName.includes(name) ? 'icon-angle-up' : 'icon-angle-down';
    },
    getProjectInfo(option) {
      return `${this.$t('项目描述')}: ${option.description}`;
    },
    handleToggleType(type) {
      console.log(type);
      if (type === this.curType || this.isContentLoading) return;
      bkStorage.set(this.bkStorageKeyType, type);
      this.curType = type;
      this.notebooks = [];
      this.queryList = [];
      this.isNotebookLoaded = false;
      this.projectId = null;
      // this.activeName = ['dataquery']
      if (type === 'common') {
        this.projectList = [];
        this.getMineProjectList();
      } else {
        this.getProjectList();
      }
    },
    handleSqlChanged(sql) {
      this.queryList[this.activeTaskIndex].sql_text = sql;
    },
    getNotebookClone() {
      this.btnLoading = true;
      if (this.isShow) {
        this.activeBookItem.project_type = 'common';
      } else {
        this.activeBookItem.project_type = 'personal';
      }
      this.bkRequest
        .httpRequest('dataExplore/getNotebookClone', {
          params: {
            notebook_id: this.activeBookItem.notebook_id,
            project_id: this.copyProjectId,
            project_type: this.activeBookItem.project_type,
          },
        })
        .then(res => {
          if (res.result) {
            this.btnLoading = false;
            this.popHidden();
            const type = res.data.project_type;
            const notebook = res.data.notebook_id;
            const projectId = res.data.project_id;
            const h = this.$createElement;
            this.box = this.$bkInfo({
              type: 'success',
              title: this.$t('克隆成功'),
              showFooter: false,
              subHeader: h(
                'a',
                {
                  style: {
                    color: '#3a84ff',
                    textDecoration: 'none',
                    cursor: 'pointer',
                  },
                  on: {
                    click: () => {
                      this.box.value = false;
                      const url = this.$router.resolve({
                        name: 'DataExploreNotebook',
                        query: { type: type, notebook: notebook, projectId: projectId },
                      });
                      window.open(url.href);
                    },
                  },
                },
                '前往克隆的新页面'
              ),
            });
          } else {
            this.btnLoading = false;
            this.popHidden();
            this.$bkMessage({
              theme: 'error',
              message: res.message,
            });
          }
        });
    },
    createNotebook() {
      this.notebookLoading = true;
      this.bkRequest
        .httpRequest('dataExplore/createNotebook', {
          params: {
            project_id: this.projectId,
            project_type: this.curType,
          },
        })
        .then(res => {
          if (res.result) {
            const notebook = Object.assign(res.data, { isActive: true });
            this.$set(this, 'activeNotebook', notebook);
            this.notebooks.splice(0, 0, notebook);
            this.$emit('notebookChanged', { item: notebook, isNew: true });
            this.changeActiveRoute();
          } else {
            this.$bkMessage({
              theme: 'error',
              message: res.message,
            });
          }
        })
        ['finally'](_ => {
          this.notebookLoading = false;
        });
    },
    changeActiveRoute(isNotebook = true) {
      isNotebook && this.$set(this, 'taskId', '');
      !isNotebook && this.$set(this, 'activeNotebook', {});
      const query = isNotebook ? { notebook: this.activeNotebook.notebook_id } : { query: this.taskId };
      const params = Object.assign({}, this.routerParams, query);
      isNotebook ? delete params.query : delete params.notebook;
      this.$router.replace({
        name: isNotebook ? 'DataExploreNotebook' : 'DataExploreQuery',
        query: params,
      });
    },
    handleNotebookClick(item) {
      // this.activeNotebook = item
      this.$set(this, 'activeNotebook', item);
      this.$emit('notebookChanged', { item });
      this.changeActiveRoute();
    },
    // 切换项目
    changeProject(val) {
      // 重新加载notebook
      this.isNotebookLoaded = false;
      if (this.isProject && val) {
        bkStorage.set(this.bkStorageKey, val);
        /** change事件发生时，可能projectid还没来得及更新，故将接口拉取操作放入队列，下个周期执行 */
        setTimeout(() => {
          this.getQueryList();
          this.isNotebookActive && this.getNotebooks();
        }, 0);
      }
      this.$router.replace({
        query: this.routerParams,
      });
      this.$emit('getProjectId', { id: val, name: this.projectInfo.project_name });
    },
    // 获取个人项目
    getProjectList() {
      this.projectLoading = true;
      this.bkRequest
        .httpRequest('dataExplore/getProjectLists')
        .then(res => {
          if (res.data) {
            this.getCommonProjectRequest().then(res => {
              if (res.result) {
                this.projectList = res.data
                  .map(item => {
                    item.disName = `[${item.project_id}]${item.project_name}`;
                    return item;
                  })
                  .filter(d => d.active);
                if (this.projectList.length) {
                  this.copyProjectId = this.projectList[0].project_id;
                }
              } else {
                this.getMethodWarning(res.message, res.code);
              }
            });
            this.projectId = res.data[0].project_id;
            this.$emit('getProjectId', { id: this.projectId, name: this.projectInfo.project_name });
            this.getQueryList();
            this.isNotebookActive && this.getNotebooks();
          }
        })
        ['finally'](() => {
          this.projectLoading = false;
        });
    },
    // 获取有权限项目
    getMineProjectList(init) {
      this.projectLoading = true;
      const storageId = bkStorage.get(this.bkStorageKey);
      const queryId = Number(this.$route.query.projectId);
      this.getCommonProjectRequest()
        .then(res => {
          if (res.result) {
            this.projectList = res.data
              .map(item => {
                item.disName = `[${item.project_id}]${item.project_name}`;
                return item;
              })
              .filter(d => d.active);
            if (this.projectList.length) {
              const firstId = this.projectList[0].project_id;
              this.copyProjectId = firstId;
              const existenceStorageId = storageId
                            && this.projectList.find(item => item.project_id === storageId);
              if (init) {
                const existenceQueryId = queryId
                                && this.projectList.find(item => item.project_id === queryId);
                this.projectId = existenceQueryId ? queryId : existenceStorageId ? storageId : firstId;
              } else {
                this.projectId = existenceStorageId ? storageId : firstId;
              }
              this.getQueryList();
              this.isNotebookActive && this.getNotebooks();
            } else {
              bkStorage.set(this.bkStorageKey, '');
            }
          } else {
            postMethodWarning(`获取项目列表失败：${res.message}`, 'error', res.code);
          }
        })
        ['finally'](_ => {
          this.projectLoading = false;
        });
    },
    getCommonProjectRequest() {
      return this.bkRequest.httpRequest('dataFlow/getProjectList', {
        query: {
          active: true,
        },
      });
    },
    // 选择SQL
    chooseSQL(item, index, isNew) {
      this.activeTaskIndex = index;
      this.taskId = item.query_id;
      this.changeActiveRoute(false);
      this.$emit('getSQL', item.sql_text);
      this.$emit('handleTask', { item, isNew });
    },
    // 获取任务列表
    getQueryList() {
      this.queryLoading = true;
      return this.bkRequest
        .httpRequest('dataExplore/getTaskList', {
          query: {
            project_id: this.projectId,
            project_type: this.curType,
          },
        })
        .then(res => {
          if (res.data) {
            this.queryList = res.data.map(item => Object.assign(item, { isActive: false, isHover: false }))
              .reverse();
            // 切换项目没有展开notebooks列表则默认加载第一个查询
            const defaultSelectedQuery = !this.isNotebookActive && this.isNotebook;
            if (!this.isNotebook || defaultSelectedQuery) {
              // 切换项目没有展开项则默认展开查询
              if (!this.activeName.length || !this.activeName.includes('dataquery')) {
                this.activeName.push('dataquery');
              }
              const routerQuery = this.$route.query.query;
              let index = 0;
              if (this.reLoading || routerQuery) {
                const findIndex = this.queryList.findIndex(
                  item => item.query_id === this.reLoading || item.query_id === Number(routerQuery)
                );
                index = findIndex === -1 ? 0 : findIndex;
              }
              const item = this.queryList[index];
              item.isActive = true;
              this.chooseTask(item, index);
              // this.chooseSQL(item, index)
              const params = Object.assign({}, this.routerParams, { query: item.query_id });
              delete params.notebook;
              this.$router.replace({
                query: params,
              });
            }
          }
        })
        ['finally'](() => {
          this.queryLoading = false;
        });
    },
    // 点击选择任务
    chooseTask(item, index, isNew) {
      this.taskId = item.query_id;
      // this.$emit('handleTask', item)
      this.chooseSQL(item, index, isNew);
    },
    // 切换选择结果集
    resultSet(item) {
      this.changeActiveRoute(false);
      this.taskId = item.query_id;
      // this.$emit('setTabKey', item.query_id)
      this.$emit('handleTask', { item });
    },
    // 创建任务
    addTask() {
      this.bkRequest
        .httpRequest('dataExplore/addTask', {
          params: {
            project_id: this.projectId,
            project_type: this.curType,
          },
        })
        .then(res => {
          if (res.code === '00') {
            this.queryList.splice(0, 0, res.data);
            this.chooseTask(res.data, 0, true);
          }
        });
    },
    // 复制功能
    handleSqlCopy(item) {
      const el = document.createElement('textarea');
      el.value = item.sql_text;
      el.style.zIndex = '-10';
      document.body.appendChild(el);
      el.select();
      document.execCommand('copy');
      document.body.removeChild(el);
      this.$bkMessage({ theme: 'primary', message: 'SQL复制成功', delay: 2000, dismissable: false });
    },
    getNotebooks() {
      this.notebookLoading = true;
      this.bkRequest
        .httpRequest('dataExplore/getNotebooks', {
          query: {
            project_id: this.projectId,
            project_type: this.curType,
          },
        })
        .then(res => {
          if (res.result) {
            this.isNotebookLoaded = true;
            this.notebooks = res.data.map(item => Object.assign(item, { isActive: false, isHover: false }))
              .reverse();
            if (this.isNotebook) {
              const routerNotebook = this.$route.query.notebook;
              let index = 0;
              if (routerNotebook) {
                const findIndex = this.notebooks
                  .findIndex(item => item.notebook_id === Number(routerNotebook));
                index = findIndex === -1 ? 0 : findIndex;
              }
              const curNotebook = this.notebooks[index] || {};
              this.$set(this, 'activeNotebook', curNotebook);
              this.$emit('notebookChanged', { item: curNotebook });
              const params = Object.assign({}, this.routerParams, {
                notebook: this.activeNotebook.notebook_id,
              });
              delete params.query;
              this.$router.replace({
                query: params,
              });
            }
          } else {
            this.$bkMessage({
              theme: 'error',
              message: res.message,
            });
          }
        })
        ['finally'](_ => {
          this.notebookLoading = false;
        });
    },
    handleNewProject() {
      this.$refs.newproject.open();
    },
    handleCreatedProject(params) {
      this.bkRequest.httpRequest('meta/creatProject', { params }).then(res => {
        if (res.result) {
          this.$bkMessage({
            message: this.$t('成功'),
            theme: 'success',
          });
          this.$refs.newproject.close();
          Object.assign(params, { project_id: res.data });
          params.disName = `[${params.project_id}]${params.project_name}`;
          this.projectList.unshift(params);
          this.projectId = res.data;

          // 补充添加人员
          this.bkRequest.httpRequest('auth/addProjectMember', {
            params: {
              project_id: this.projectId,
              role_users: [
                {
                  role_id: 'project.manager',
                  user_ids: params.admin,
                },
                {
                  role_id: 'project.flow_member',
                  user_ids: params.member,
                },
              ],
            },
          });
        } else {
          postMethodWarning(res.message, 'error');
          this.$refs.newproject.reset();
        }
      });
    },
    handlePermissionApply() {
      this.$refs.permissionApply && this.$refs.permissionApply.openDialog();
    },
    handleShowPackage() {
      this.packageDialog.isShow = true;
    },
    handleClosePackage() {
      this.packageDialog.isShow = false;
    },
    handleToggleCollapse(activeName) {
      if (activeName.includes('notebook') && !this.isNotebookLoaded && this.projectId) {
        this.getNotebooks();
      }
    },
    async handleShowRename(item, type) {
      try {
        if (this.curEditData.awaitPrevious) {
          await this.curEditData.awaitPrevious;
        }
        const id = type === 'notebook' ? item.notebook_id : item.query_id;
        const name = type === 'notebook' ? item.notebook_name : item.query_name;
        this.curEditData.id = id;
        this.curEditData.name = name;
        this.curEditData.type = type;
        this.curEditData.editing = true;
        this.curEditData.awaitPrevious = new Promise((resolve, reject) => {
          this.curEditData.previousResolve = resolve;
          this.curEditData.previousReject = reject;
        });
        this.$nextTick(() => {
          this.$refs.rename[0].$refs.input.focus();
        });
      } catch (_) {
        console.error('cancel rename');
      }
    },
    handleClearEditData() {
      this.curEditData.previousResolve && this.curEditData.previousResolve();
      this.curEditData.id = '';
      this.curEditData.name = '';
      this.curEditData.type = '';
      this.curEditData.editing = false;
      this.curEditData.awaitPrevious = null;
      this.curEditData.previousResolve = null;
      this.curEditData.previousReject = null;
    },
    handleCancelNextEditData() {
      this.$refs.rename[0].$refs.input.focus();
      if (this.curEditData.previousReject) {
        this.curEditData.previousReject();
        this.curEditData.previousReject = null;
      }
    },
    handleDeleteNotebook(item, index) {
      const h = this.$createElement;
      this.$bkInfo({
        title: this.$t('删除确认'),
        subHeader: h(
          'div',
          {
            style: { color: '#63656e', textAlign: 'center' },
          },
          [
            h('p', '笔记生成的结果集和临时结果表将会一并删除'),
            h('p', this.$t('是否确认要删除xx', { name: item.notebook_name })),
          ]
        ),
        subTitle: this.$t('是否确认要删除xx', { name: item.notebook_name }),
        confirmFn: () => {
          this.notebookLoading = true;
          this.bkRequest
            .httpRequest('dataExplore/deleteNotebook', {
              params: { notebook_id: item.notebook_id },
            })
            .then(res => {
              if (res.result) {
                this.notebooks.splice(index, 1);
                // this.$set(this, 'activeNotebook', this.notebooks[0] || {})
                // this.$emit('notebookChanged', this.notebooks[0])
                // this.changeActiveRoute()
                this.$emit('on-delete', {
                  type: 'notebook',
                  id: item.notebook_id,
                });
              } else {
                this.$bkMessage({
                  theme: 'error',
                  message: res.message,
                });
              }
            })
            ['finally'](_ => {
              this.notebookLoading = false;
            });
        },
      });
    },
    async handleRenameNotebook(item, index) {
      if (!this.curEditData.editing) return;
      if (!this.curEditData.name) {
        this.$bkMessage({
          theme: 'error',
          message: this.$t('名称不能为空'),
        });
        this.handleCancelNextEditData();
        return;
      }
      if (item.notebook_name === this.curEditData.name) {
        this.handleClearEditData();
        return;
      }
      this.notebookLoading = true;
      try {
        const res = await this.bkRequest.httpRequest('dataExplore/renameNotebook', {
          params: {
            notebook_id: this.curEditData.id,
            notebook_name: this.curEditData.name,
          },
        });
        if (res.result) {
          this.$emit('on-rename', {
            type: 'notebook',
            id: this.curEditData.id,
            name: this.curEditData.name,
          });
          this.notebooks[index] && this.$set(this.notebooks[index], 'notebook_name', this.curEditData.name);
          this.handleClearEditData();
        } else {
          this.$bkMessage({
            theme: 'error',
            message: res.message,
          });
          this.handleCancelNextEditData();
        }
        this.notebookLoading = false;
      } catch (_) {
        console.error(_);
        this.handleCancelNextEditData();
      }
    },
    async handleRenameQuery(item) {
      if (!this.curEditData.editing) return;
      if (!this.curEditData.name) {
        this.$bkMessage({
          theme: 'error',
          message: this.$t('名称不能为空'),
        });
        this.handleCancelNextEditData();
        return;
      }
      if (item.query_name === this.curEditData.name) {
        this.handleClearEditData();
        return;
      }
      try {
        const res = await this.bkRequest.httpRequest('dataExplore/updateTask', {
          params: {
            query_name: this.curEditData.name,
            query_id: this.curEditData.id,
          },
        });
        if (res.code === '00') {
          this.$emit('on-rename', {
            type: 'query',
            id: this.curEditData.id,
            name: this.curEditData.name,
          });
          this.getQueryList();
          this.handleClearEditData();
        } else {
          this.$bkMessage({
            theme: 'error',
            message: res.message,
          });
          this.handleCancelNextEditData();
        }
      } catch (_) {
        console.error(_);
        this.handleCancelNextEditData();
      }
    },
    handleDeleteQuery(item, index) {
      this.$bkInfo({
        title: this.$t('删除确认'),
        subTitle: this.$t('是否确认要删除xx', { name: item.query_name }),
        confirmFn: () => {
          this.bkRequest
            .httpRequest('dataExplore/delTask', {
              params: { query_id: item.query_id },
            })
            .then(res => {
              if (res.code === '00') {
                this.queryList.splice(index, 1);
                this.$emit('on-delete', {
                  type: 'query',
                  id: item.query_id,
                });
                // this.getQueryList().then(() => {
                //     this.$emit('on-delete', {
                //         type: 'query',
                //         id: item.query_id
                //     })
                // })
              } else {
                this.$bkMessage({
                  theme: 'error',
                  message: res.message,
                });
              }
            });
        },
      });
    },
  },
};
</script>
<style lang="scss" scoped>
.extension-btns {
  display: flex;
  align-items: center;
  .icon-btn {
    flex: 1;
    height: 32px;
    line-height: 32px;
    color: #63656e;
    cursor: pointer;
    &:hover {
      color: #3a84ff;
    }
    .icon-item {
      font-size: 14px;
    }
  }
}
.left-slider {
  width: 100%;
  height: 100%;
  overflow: hidden;
  .left-slider-header {
    width: 100%;
  }
  .project-seletor {
    border-bottom: 1px solid #dcdee5;
  }
  .left-slider-select {
    width: auto;
    margin: 10px 16px;
    background: #f0f1f5;
    border-color: transparent;
    &.is-focus {
      background: #ffffff;
      border-color: #3a84ff;
    }
  }
  .header-tab {
    display: flex;
    align-items: center;
    height: 43px;
    line-height: 42px;
    .tab-item {
      flex: 0 0 50%;
      position: relative;
      font-size: 16px;
      text-align: center;
      color: #979ba5;
      background-color: #fafbfd;
      border-bottom: 1px solid #dcdee5;
      cursor: pointer;
      &:first-child {
        border-right: 1px solid #dcdee5;
      }
      &.tab-active {
        color: #3a84ff;
        background-color: #ffffff;
        border-bottom-color: transparent;
        &::before {
          content: '';
          position: absolute;
          top: 0;
          left: 0;
          width: 100%;
          height: 3px;
          background-color: #3a84ff;
        }
      }
    }
  }
}
.query-list {
  width: 100%;
  font-size: 12px;
  .disabled {
    background: #fafbfd;
    height: 28px;
    line-height: 28px;
  }
  .my-menu {
    height: 100%;
    ::v-deep .split-panel-part {
      overflow: hidden;
    }
  }
  .drag-layout {
    &.is-disabled {
      ::v-deep .split-panel-part {
        &:first-child {
          height: 40px !important;
        }
        &:nth-child(3) {
          height: calc(100% - 50px) !important;
        }
      }
    }
    ::v-deep .split-panel-gutter {
      border-top-color: transparent !important;
    }
  }
  ::v-deep .bk-collapse-item-header {
    height: 40px;
    line-height: 40px;
    padding: 0 16px;
    .trigger-area {
      width: 100%;
      .custom-item-header {
        display: flex;
        justify-content: space-between;
        font-size: 12px;
        color: #797d85;
        .name-icon {
          font-size: 14px;
          color: #979ba5;
          padding-right: 2px;
        }
        .custom-item-operations {
          display: flex;
          align-items: center;
        }
        .head-icons {
          font-size: 24px;
          color: #979ba5;
          cursor: pointer;
          &:hover {
            color: #3a84ff;
          }
          &.icon-cog {
            width: 24px;
            font-size: 12px;
          }
        }
      }
    }
  }
  ::v-deep .bk-collapse-item {
    height: 100%;
    .bk-collapse-item-content {
      height: calc(100% - 40px);
      overflow-y: auto;
      overflow-x: hidden;
      padding: 0;
      .bk-collapse-item-detail {
        .query-list-main {
          min-height: 100px;
          padding-bottom: 4px;
          ::v-deep .bk-loading-wrapper {
            width: 100% !important;
          }
        }
        .query-list-item {
          width: 100%;
          font-size: 12px;
          position: relative;
          .item-head {
            display: flex;
            justify-content: space-between;
            width: 100%;
            height: 28px;
            line-height: 28px;
            color: #63656e;
            padding: 0 16px;
            cursor: pointer;
            &.hover {
              background: #eaf3ff;
              .item-name {
                color: #3a84ff;
              }
              .item-operations {
                display: block;
              }
            }
            &:hover {
              background: #eaf3ff;
              .item-name {
                color: #3a84ff;
              }
              .item-operations {
                display: block;
              }
            }
            .btn-icon {
              color: #979ba5;
              font-size: 14px;
              cursor: pointer;
              &:hover {
                color: #3a84ff;
              }
            }
          }
          .item-name {
            flex: 1;
            height: 28px;
            line-height: 28px;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
          }
          .item-operations {
            display: none;
          }
          .item-rename {
            margin: 0 16px;
            ::v-deep .bk-form-input {
              height: 28px;
              line-height: 28px;
            }
          }
          .item-txt {
            width: 100%;
            height: 28px;
            line-height: 28px;
            display: inline-block;
            color: #313238;
            padding-left: 40px;
            cursor: pointer;
            &:hover {
              background: #e1ecff;
            }
            &.disabled {
              color: #c4c6cc;
              cursor: not-allowed;
            }
            .icons {
              margin-left: -5px;
              margin-right: 6px;
              vertical-align: baseline;
            }
          }
          .gray-icon {
            color: #979ba5;
            font-size: 18px;
            font-weight: 900;
            cursor: pointer;
          }
          .blue-icon {
            color: #3a84ff;
          }
        }
        .is-choose {
          .item-head {
            background: #e1ecff;
            .item-name {
              color: #3a84ff;
            }
          }
        }
      }
    }
  }
}
::v-deep .package-dialog {
  .bk-dialog-body {
    padding: 3px 0 0;
  }
}
.item-bounced {
  width: 360px;
  height: 220px;
  .clone-content {
    padding: 20px;
    .clone-tip-title {
      font-size: 16px;
      font-family: MicrosoftYaHei;
      text-align: left;
      color: #313238;
    }
    .clone-form-content {
      padding-top: 15px;
      .bk-button {
        padding: 0 45px;
      }
      .select {
        width: 320px;
      }
      .active {
        background: #e1ecff;
        border: 1px solid #3a84ff;
      }
    }
  }
  .clone-form-button {
    position: absolute;
    bottom: 0;
    left: 0;
    width: 100%;
    height: 52px;
    line-height: 52px;
    background: #fafbfd;
    text-align: right;
    padding-right: 25px;
  }
}
.clonePopMask {
  display: none;
  position: fixed;
  height: 100%;
  width: 100%;
  background-color: transparent;
  top: 0;
  z-index: 9;
}
</style>
