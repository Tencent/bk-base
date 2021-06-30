/** 数据探索主页 */


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
  <div class="data-explore">
    <!-- 左栏 -->
    <div :class="['data-explore-left', { 'is-left-hide': !isShowLeftMenu }]">
      <left-slider
        ref="leftnavi"
        :reLoading="reInit"
        :is-data-analyst="isDataAnalyst"
        @getProjectId="getProjectId"
        @getSQL="getSQL"
        @handleTask="handleTask"
        @notebookChanged="handleNotebookChanged"
        @changeType="handleChangeType"
        @on-rename="handleRename"
        @on-delete="handleDelete"
        @getContents="getContents" />
      <i :class="['toggle-btn', expandIcon]"
        @click.stop="handleToggleSlider" />
    </div>
    <div class="data-explore-center">
      <template v-if="mainTabs.length">
        <bkdata-tab ref="mainTabs"
          :class="['data-explore-main',{'is-main-hide': !isShowLeftMenu}]"
          type="unborder-card"
          closable
          :active.sync="activeTab"
          @close-panel="handleCloseMainTabsPanel"
          @tab-change="handleMainTabsChange">
          <div slot="setting"
            class="header-setting" />
          <bkdata-tab-panel v-for="item in mainTabs"
            :key="item.displayId + item.tabType"
            :name="item.activeName"
            :label="item.displayName">
            <div slot="label"
              v-bk-tooltips.top="getTabTips(item.displayName)"
              class="custom-tab-label">
              <i :class="['label-icon', getTabIcon(item.tabType)]" />
              <span class="display-name">{{ item.displayName }}</span>
            </div>
            <template v-if="item.tabType === 'notebook'">
              <template v-if="item.notebook_url">
                <notebook-main :activeNotebook="item"
                  :isCommon="isCommon"
                  :contents="contents" />
                <NotebookRight :activeNotebook="item"
                  :isCommon="isCommon"
                  :projectName="projectName"
                  @refreshNotebook="refreshNotebook(item)" />
              </template>
              <template v-else>
                <div class="no-result">
                  <i class="no-result-img" />
                  <div class="larger-font">
                    暂无数据
                  </div>
                </div>
              </template>
            </template>
            <template v-else>
              <!-- Sql编辑器 -->
              <CenterMain :ref="item.activeName"
                :sqlData.sync="item.sql_text"
                :projectId="projectId"
                :projectName="projectName"
                :task="item"
                :taskList.sync="mainTabs"
                :isCommon="isCommon"
                :is-data-analyst="isDataAnalyst"
                @reLoad="reLoad"
                @updateOriginSql="handleUpdateOriginSql" />
            </template>
          </bkdata-tab-panel>
        </bkdata-tab>
      </template>
      <template v-else>
        <div class="no-result">
          <div class="larger-font">
            请选择笔记/查询进行编辑
          </div>
        </div>
      </template>
    </div>
  </div>
</template>
<script>
const LeftSlider = () => import(/* webpackPreload: true */ './slider/LeftSlider');
const CenterMain = () => import(/* webpackPreload: true */ './slider/CenterMain');
// const StructureSet = () => import(/* webpackPreload: true */ './components/StructureSet');
const NotebookRight = () => import(/* webpackPreload: true */ './components/NotebookRightMenu');
const NotebookMain = () => import(/* webpackPreload: true */ './components/Notebook');

export default {
  components: {
    LeftSlider,
    CenterMain,
    // StructureSet,
    NotebookRight,
    NotebookMain,
  },
  provide() {
    return {
      dataExplore: this,
    };
  },
  beforeRouteLeave(to, from, next) {
    if (from.name === 'DataExploreNotebook') {
      const id = from.query.notebook;
      const jupyter = document.getElementById('jupyter-notebook' + id);
      jupyter
        && jupyter.contentWindow.postMessage(
          {
            eventType: 'saveNoteConfirm',
            saveAsNew: false,
          },
          '*'
        );
      setTimeout(() => next(), 400);
    } else {
      next();
    }
  },
  data() {
    return {
      reInit: null,
      sqlData: '',
      isShowLeftMenu: true,
      isDrop: false,
      startY: 0,
      isSQL: true,
      contentWidth: null,
      clientWidth: null,
      projectId: null,
      task: {},
      tabKey: 0,
      tools: {
        enabled: true,
        title: '',
      },
      activeItem: {
        scope_config: {
          content: '',
        },
      },
      activeNotebook: {},
      resultBtnStatus: {
        disabled: false,
        tips: '',
      },
      isDataAnalyst: true,
      projectName: '',
      isCommon: false,
      mainTabs: [],
      activeTab: '',
      isRestart: false,
      contents: [],
    };
  },
  computed: {
    rightPanelData() {
      return {
        projectId: this.projectId,
        projectName: this.projectName,
        isCommon: this.isCommon,
      };
    },
    isNotebook() {
      return !!this.$route.query.notebook;
    },
    hasOperationAuth() {
      return this.isCommon ? !this.isDataAnalyst : true;
    },
    expandIcon() {
      return this.isShowLeftMenu ? 'icon-shrink-fill' : 'icon-expand-fill';
    },
    tabsIsLimit() {
      return this.mainTabs.length === 16;
    },
  },
  watch: {
    projectId: {
      immediate: true,
      handler(val) {
        if (val) {
          this.checkUserAuth();
          this.activeTab = '';
          this.mainTabs = [];
        }
      },
    },
    activeTab(val, old) {
      // 销毁上一个编辑器
      old.indexOf('|__query__') > -1 && this.handleDisposeEditor(old);
      // 加载当前编辑器
      val.indexOf('|__query__') > -1 && this.isRestart && this.handleLoadEditor(val);
      // 确保下次如果点击tab为query的时候触发加载编辑器
      this.isRestart = true;
    },
  },
  mounted() {
    window.addEventListener('message', this.handleMessageChanged);
    // this.getContents();
  },
  beforeDestroy() {
    window.removeEventListener('message', this.handleMessageChanged);
  },
  methods: {
    getContents(contents) {
      console.log('contents', contents);
      this.contents = contents;
    },
    getTabTips(content) {
      return {
        interactive: false,
        placement: 'top',
        zIndex: '2001',
        boundary: document.body,
        content,
      };
    },
    getTabIcon(type) {
      return type === 'query' ? 'icon-icon-audit' : 'icon-notebook-2';
    },
    checkUserAuth() {
      this.bkRequest
        .httpRequest('auth/checkUserAuth', {
          params: {
            action_id: 'project.manage_flow',
            object_id: this.projectId,
            user_id: this.$store.getters.getUserName,
          },
        })
        .then(res => {
          if (res.result) {
            this.isDataAnalyst = !res.data;
          } else {
            this.isDataAnalyst = true;
          }
        })
        ['catch'](e => {
          this.isDataAnalyst = true;
        });
    },
    handleUpdateOriginSql(sql) {
      this.$refs.leftnavi.handleSqlChanged(sql);
    },
    refreshNotebook(item) {
      document.getElementById('jupyter-notebook' + item.notebook_id).src = item.notebook_url;
    },
    handleNotebookChanged({ item, isNew }) {
      if (this.tabsIsLimit) {
        const tips = isNew ? this.$t('创建成功') + '，' : '';
        this.$bkMessage({
          theme: 'primary',
          message: tips + this.$t('页签数量已到达上限，请删除后再添加'),
        });
        return;
      }
      this.activeNotebook = item;
      const activeName = `${item.notebook_id}${item.notebook_name}|__notebook__`;
      this.activeTab = activeName;
      const existed = this.mainTabs.find(item => item.activeName === activeName);
      if (!existed) {
        this.mainTabs.push({
          ...item,
          activeName,
          displayName: item.notebook_name,
          displayId: item.notebook_id,
          tabType: 'notebook',
        });
      }
    },
    // 选择SQL
    getSQL(sql) {
      this.sqlData = sql;
      this.tabKey = 0;
    },
    // 选择切换当前任务
    handleTask({ item, isNew }) {
      if (this.tabsIsLimit) {
        const tips = isNew ? this.$t('创建成功') + '，' : '';
        this.$bkMessage({
          theme: 'primary',
          message: tips + this.$t('页签数量已到达上限，请删除后再添加'),
        });
        return;
      }
      this.task = item;
      const activeName = `${item.query_id}${item.query_name}|__query__`;
      if (activeName === this.activeTab) return;
      const curActiveObj = this.mainTabs.find(item => item.activeName === this.activeTab);
      const isQuery = this.activeTab.indexOf('|__query__') > -1;
      const isDiff = curActiveObj && curActiveObj.activeName !== activeName && isQuery;
      if (isDiff) {
        // 销毁上个tab编辑器
        const key = this.activeTab;
        this.$refs[key][0].disposeEditor();
      }
      const existed = this.mainTabs.find(item => item.activeName === activeName);
      this.isRestart = !!existed;
      this.activeTab = activeName;
      if (!existed) {
        this.mainTabs.push({
          ...item,
          activeName,
          displayName: item.query_name,
          displayId: item.query_id,
          tabType: 'query',
        });
      }
    },
    // 获取项目id
    getProjectId({ id = '', name = '' }) {
      this.projectId = id;
      this.projectName = name;
    },
    // 获取当前queryID
    reLoad(taskID) {
      this.reInit = taskID;
    },
    handleMessageChanged(event) {
      if (event.data.eventType === 'languageChanged') {
        // console.log('languageChanged', event.data.selected) // 修改为具体的需要执行的函数即可
        // this.handleLanguageChanged(event.data.selected)
      }
      // else if (event.data.eventType === 'getUsername') {
      //     document.getElementById('jupyter-notebook').contentWindow.postMessage({
      //         eventType: 'getUsername',
      //         username: this.$store.getters.getUserName
      //     }, '*')
      // }
    },
    handleChangeType(val) {
      this.isCommon = val;
    },
    handleToggleSlider() {
      this.isShowLeftMenu = !this.isShowLeftMenu;
    },
    handleCloseMainTabsPanel(index) {
      // 保存关闭的笔记
      const curItem = this.mainTabs[index];
      if (curItem && curItem.tabType === 'notebook') {
        const jupyter = document.getElementById('jupyter-notebook' + curItem.notebook_id);
        jupyter
          && jupyter.contentWindow.postMessage(
            {
              eventType: 'saveNoteConfirm',
              saveAsNew: false,
            },
            '*'
          );
        const timer = setTimeout(() => {
          this.mainTabs.splice(index, 1);
          clearTimeout(timer);
        }, 200);
        return;
      }
      this.mainTabs.splice(index, 1);
    },
    handleMainTabsChange(name) {
      const curItem = this.mainTabs.find(item => item.activeName === name);
      if (!curItem) return;
      const isNotebook = curItem.tabType === 'notebook';
      const query = isNotebook ? { notebook: curItem.notebook_id } : { query: curItem.query_id };
      const params = Object.assign({}, this.$route.query, query);
      isNotebook ? delete params.query : delete params.notebook;
      this.$router.replace({
        name: isNotebook ? 'DataExploreNotebook' : 'DataExploreQuery',
        query: params,
      });
    },
    handleDisposeEditor(name) {
      this.mainTabs.find(item => item.activeName === name) && this.$refs[name][0].disposeEditor();
    },
    handleLoadEditor(name) {
      const item = this.mainTabs.find(item => item.activeName === name);
      const vDom = this.$refs[name][0];
      vDom.disposeEditor();
      this.$nextTick(() => {
        vDom.initMonacoClient();
        vDom.handleSqlChange(item.sql_text, true);
      });
    },
    handleRename({ type, id, name }) {
      const updateName = type === 'query' ? 'query_name' : 'notebook_name';
      const index = this.mainTabs.findIndex(item => {
        return item[`${type}_id`] === id && item.activeName.indexOf(`|__${type}__`) > -1;
      });
      if (index > -1) {
        const activeName = `${id}${name}|__${type}__`;
        const item = this.mainTabs[index];
        const oldActiveName = item.activeName;
        this.mainTabs.splice(index, 1, {
          ...item,
          [updateName]: name,
          displayName: name,
          activeName,
        });
        if (this.activeTab === oldActiveName) {
          this.activeTab = activeName;
          type === 'query' && (this.isRestart = false);
        }
        this.$refs.mainTabs.$refs.tabLabel[index].$forceUpdate();
      }
    },
    handleDelete({ type, id }) {
      const index = this.mainTabs.findIndex(item => {
        return item[`${type}_id`] === id && item.activeName.indexOf(`|__${type}__`) > -1;
      });
      index > -1 && this.mainTabs.splice(index, 1);
    },
  },
};
</script>
<style lang="scss" scoped>
.data-explore-main {
  height: 100%;
  position: fixed;
  width: calc(100% - 240px);
  transition: all 0.1s;
  &.is-main-hide {
    width: 100%;
  }
  .header-setting {
    width: 320px;
  }
  .custom-tab-label {
    display: flex;
    align-items: center;
    .label-icon {
      color: #c4c6cc;
      font-size: 16px;
      margin-top: 1px;
    }
    .display-name {
      flex: 1;
      padding-left: 6px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
  }
  ::v-deep {
    > .bk-tab-header {
      height: 43px;
      background-color: #fafbfd;
      background-image: linear-gradient(transparent 42px, #dcdee5 0);
      .bk-tab-label-list {
        display: flex;
      }
      .bk-tab-label-wrapper .bk-tab-label-list .bk-tab-label-item.active {
        background-color: #ffffff;
        border-left: 1px solid #dcdee5 !important;
        border-right: 1px solid #dcdee5 !important;
        position: relative;
        &::after {
          content: '';
          background-color: #3a84ff;
          position: absolute;
          top: 0;
          width: calc(100% + 2px);
          height: 3px !important;
        }
        &::before {
          background-color: #ffffff;
          width: 100%;
          height: 1px;
          bottom: -1px;
          left: 0;
          top: unset;
          transform: unset;
        }
        &.is-first {
          border-left: 0 !important;
        }
      }
      .bk-tab-label-list-has-bar:after {
        content: none;
      }
      .bk-tab-label-item {
        flex: 0 1 240px;
        min-width: 38px;
        display: flex;
        align-items: center;
        padding: 0 10px;
        text-align: left;
        &::before {
          content: '';
          position: absolute;
          right: -1px;
          top: 50%;
          transform: translateY(-50%);
          width: 1px;
          height: 20px;
          background-color: #dcdee5;
        }
        &.active {
          .label-icon {
            color: #3a84ff;
          }
          .bk-tab-close-controller {
            display: inline-block;
          }
          &::after {
            top: 0 !important;
            bottom: auto !important;
            left: -1px;
            width: calc(100% + 2px);
            height: 3px !important;
          }
        }
        &:hover {
          .label-icon {
            color: #3a84ff;
          }
          .bk-tab-close-controller {
            display: inline-block;
          }
        }
        .bk-tab-label {
          flex: 1;
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;
        }
        .bk-tab-close-controller {
          width: 18px !important;
          height: 18px;
          background-color: #ffffff;
          color: #979ba5;
          margin: 0 !important;
          display: none;
          &::before,
          &::after {
            width: 10px;
            top: 8px;
            left: 4px;
            background-color: #979ba5;
          }
          &:hover {
            background-color: #c4c6cc;
            &::before,
            &::after {
              background-color: #ffffff;
            }
          }
        }
      }
    }
    > .bk-tab-section {
      padding: 0;
      height: calc(100% - 43px);
      > .bk-tab-content {
        display: flex;
        height: 100%;
        .right-menu {
          width: 100%;
          height: 100%;
          user-select: none;
          .right-tab {
            &.build-in-library {
              height: 100%;
            }
            .bk-tab-header {
              height: 48px;
              background-image: linear-gradient(transparent 47px, #dcdee5 0);
              text-align: center;
            }
            .bk-tab-section {
              height: calc(100% - 48px);
              padding: 0;
              .bk-tab-content {
                height: 100%;
              }
            }
            .bk-tab-label-item {
              line-height: 48px;
            }
            &.query-right-tab .bk-tab-label-item {
              padding: 0;
              margin: 0 10px;
            }
          }
        }
      }
    }
  }
}
</style>
<style lang="scss">
.no-result {
  min-width: 130px;
  text-align: center;
  color: #63656e;
  font-size: 12px;
  position: absolute;
  left: 50%;
  top: 50%;
  transform: translate(-50%, -50%);
  p {
    font-size: 16px;
    margin-bottom: 6px;
  }
  .no-result-img {
    display: block;
    width: 122px;
    height: 78px;
    margin: 0 auto 5px;
    background: url('../../common/images/no-data.png');
    background-size: 100% 100%;
  }
  .larger-font {
    font-size: 16px;
  }
  a {
    cursor: pointer;
  }
}
.tippy-tooltip {
  background: #3b3c44;
}
.data-explore {
  width: 100%;
  height: 100%;
  display: flex;
  position: relative;
  .list-icon {
    cursor: pointer;
    position: absolute;
    right: 12px;
    top: 14px;
  }
}
.data-explore-left {
  position: relative;
  width: 240px;
  border-right: 1px solid #dcdee5;
  transition: all 0.1s;
  &.is-left-hide {
    width: 0;
    border-right: 0;
  }
  .toggle-btn {
    position: absolute;
    top: 40%;
    right: -18px;
    font-size: 80px;
    color: rgba(151, 155, 165, 0.5);
    cursor: pointer;
    z-index: 99;
    &:hover {
      color: rgba(151, 155, 165, 0.8);
    }
  }
}
.data-explore-center {
  height: 100%;
  flex: 1;
  position: relative;
  overflow: hidden;
  .center-main-header {
    width: 100%;
    height: 40px;
    line-height: 40px;
    border-bottom: 1px solid #d8d8d8;
    color: #313238;
    padding-top: 2px;
    padding-left: 16px;
    // background: #DCDEE5;
    background: #f4f7fa;
    display: flex;
    .header-title {
      min-width: 120px;
      padding-right: 10px;
    }
    .header-icon {
      font-size: 20px;
      font-weight: 900;
      color: #3a84ff;
      margin-top: 10px;
      // cursor: pointer;
    }
    .sql-item {
      height: 32px;
      padding: 0 20px;
      text-align: center;
      line-height: 32px;
      // background: #F0F1F5;
      background: #f4f7fa;
      border: 1px solid #c4c6cc;
      border-radius: 4px 4px 0 0;
      margin: 3px 6px 0;
      position: relative;
      cursor: pointer;
      color: #63656e;
      &:first-child {
        margin-right: 16px;
        position: relative;
        &::after {
          content: '';
          display: block;
          position: absolute;
          right: -12px;
          bottom: 4px;
          height: 24px;
          width: 1px;
          background: #c4c6cc;
        }
      }
      &.disabled {
        cursor: not-allowed;
        color: #c4c6cc;
      }
    }
    .is-active {
      background: #ffffff;
      color: #313238;
      border-bottom: 1px solid #ffffff;
      &::before {
        position: absolute;
        content: '';
        display: inline-block;
        width: 100%;
        height: 3px;
        left: 0;
        background: #3a84ff;
        border-radius: 4px 4px 0 0;
      }
    }
  }
  .center-main-content {
    width: 100%;
    height: calc(100% - 40px);
    display: flex;
    &.jupyter-notebook {
      height: 100%;
    }
  }
  .query-table.bk-table {
    width: 100%;
    height: 100%;
    overflow: auto;
  }
  .bk-table-header {
    width: 100%;
  }
  .query-table.bk-table .is-first {
    .cell {
      padding-left: 0;
      padding-right: 0;
      div {
        background: #fafbfd;
      }
    }
  }
  .query-table.bk-table th,
  .query-table.bk-table td {
    height: 28px;
    .cell {
      height: 28px;
      line-height: 28px;
      font-size: 12px;
      padding: 0 8px;
    }
  }
  .bk-table-body-wrapper {
    overflow: auto;
    // height: calc(100% - 98px);
    // height: calc(100% - 70px);
  }
  .center-main-content .bk-table-pagination-wrapper {
    padding: 5px 15px;
  }
  .center-main-content .bk-page-count-left {
    margin-top: 2px;
    .bk-select .bk-select-name {
      height: 24px;
      line-height: 24px;
    }
    .bk-select .bk-select-angle {
      top: 6px;
    }
  }
  .bk-page.bk-page-small .page-item .page-button {
    min-width: 25px;
    height: 25px;
    line-height: 25px;
    margin-top: 2px;
    .bk-icon {
      line-height: 25px;
    }
  }
}
</style>
