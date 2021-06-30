

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
  <div class="project-slide">
    <div class="project-slide-body">
      <div class="bar-search">
        <i class="bk-icon bk-icon-style icon-folder mr5" />
        <bkdata-selector
          :displayKey="'displayName'"
          :isLoading="projectLoading"
          :list="projectList"
          :searchable="true"
          :selected="selectedProjectId"
          :settingKey="'project_id'"
          :placeholder="$t('请选择项目')"
          :customStyle="{ width: '230px' }"
          class="project-list-flow"
          searchKey="displayName"
          @item-selected="handleProSelect" />
        <i
          v-tooltip="{ content: $t('创建项目'), class: 'flow-newpro-tips' }"
          class="bk-icon bk-icon-style icon-plus-circle"
          @click="newProject" />
      </div>
      <div
        v-scroll-bottom="{ callback: handleScrollCallback }"
        v-bkloading="{ isLoading: taskLoading }"
        class="project">
        <ul v-if="selectedProjectId"
          class="task-list">
          <template v-if="searchedTask.length > 0">
            <li
              v-for="(list, index) in calcTaskList"
              :key="index"
              :class="['clearfix', ($route.params.fid == list.flow_id && 'active') || '']"
              :title="list.description"
              @click="taskDetail(list)">
              <span
                :class="[
                  'fl mr5',
                  list.status === 'no-start'
                    ? 'no-start'
                    : list.status === 'running' && !list.has_exception
                      ? 'running'
                      : 'exception',
                ]">
                <i class="bk-icon bk-icon-style icon-dataflow" />
              </span>
              <p class="fl task-name"
                :title="list.flow_name">
                {{ list.flow_name }}
              </p>
              <span
                v-bk-tooltips="getDeleteTips(list)"
                :class="['icon-delete', 'alert-container', 'mr5', { disabled: list.status === 'running' }]"
                @click.stop="deleteFlow(list)" />
              <div v-if="Object.keys(taskAlertCount).includes(list.flow_id.toString())"
                class="alert-container">
                <i class="icon-alert" />
                <span :title="taskAlertCount[list.flow_id].alert_count"
                  class="count text-overflow">
                  {{ taskAlertCount[list.flow_id].alert_count }}
                </span>
              </div>
            </li>
          </template>
          <li v-else
            @click="newTask()">
            {{ $t('暂无任务_请先创建') }}
          </li>
        </ul>
      </div>
      <div class="slide-bottom"
        @click="newTask()">
        <span> <i class="bk-icon bk-icon-style icon-add_task" /> {{ $t('创建任务') }} </span>
      </div>
      <!-- 新增项目开始 -->
      <newProject
        ref="newproject"
        :idNum="1"
        @createNewProject="handleCreatedProject"
        @dialogChange="handleDialogChange" />
      <!-- 新增项目开始 -->
      <!-- 新增任务开始 -->
      <new-task
        ref="newTask"
        :isCreated="isCreated"
        @createNewTask="createNewTask"
        @dialogChange="handleDialogChange" />
      <!-- 新增任务结束 -->

      <!-- 无权限申请 -->
      <PermissionApplyWindow
        :isOpen.sync="permissionShow"
        :objectId="projectId"
        :showWarning="true"
        :defaultSelectValue="permissionApplyConfig" />
    </div>
  </div>
</template>
<script>
import newProject from './newProject';
import newTask from './newTask';
import { postMethodWarning, confirmMsg } from '@/common/js/util.js';
import scrollBottom from '@/common/directive/scroll-bottom.js';
import bkStorage from '@/common/js/bkStorage.js';
import Bus from '@/common/js/bus.js';
import PermissionApplyWindow from '@/pages/authCenter/permissions/PermissionApplyWindow';
export default {
  directives: {
    scrollBottom,
  },
  components: {
    newProject,
    newTask,
    PermissionApplyWindow,
  },
  props: {
    dataFlowInfo: {
      type: Object,
    },
    isShow: {
      type: Boolean,
      default: false,
    },
    searchValue: {
      type: String,
      default: '',
    },
  },
  data() {
    return {
      permissionApplyConfig: {
        objectClass: 'project',
      },
      permissionShow: false,
      projectLoading: false,
      taskLoading: false,
      isCreated: true,
      loading: true,
      search: '', // 搜索
      projectList: [],
      taskList: [],
      selectedProjectId: 0,
      // searchVal: '',
      currentPageUpdating: false,
      currentPage: 1,
      pageSize: 100,
      bkStorageKey: 'bkStorageProjectId',
      taskAlertCount: {},
    };
  },
  computed: {
    searchedTask() {
      return (this.taskList || []).filter(task => new RegExp(this.searchValue, 'ig').test(task.flow_name));
    },
    calcTaskList() {
      return this.searchedTask.slice(0, this.currentPage * this.pageSize);
    },
    projectId() {
      return this.$route.query.project_id;
    },
  },
  watch: {
    '$route.query': {
      deep: true,
      immediate: true,
      handler(val) {
        if (val && val.project_id && val.project_id !== this.selectedProjectId) {
          this.$set(this, 'selectedProjectId', Number(val.project_id));
          this.getTaskByProId(val.project_id);
        }
      },
    },
  },
  mounted() {
    this.getTsakAlert();
    this.getMineProjectList();
    const routerPid = this.$route.query.project_id;
    const storageId = routerPid || bkStorage.get(this.bkStorageKey);
    if (storageId && /^\d+$/.test(storageId)) {
      this.selectedProjectId = Number(storageId);
      this.getTaskByProId(this.selectedProjectId);
    }
    Bus.$on('refleshTaskName', () => {
      this.getTaskByProId(this.$route.query.project_id);
    });
  },
  methods: {
    deleteFlow(flow) {
      if (flow.process_status === 'running' || flow.process_status === 'pending' || flow.status === 'running') return;
      confirmMsg(
        this.$t('确认删除任务'),
        '',
        () => {
          if (flow.status === 'no-start') {
            this.taskLoading = true;
            this.bkRequest
              .httpRequest('dataFlow/deleteTask', {
                params: {
                  flowId: flow.flow_id,
                },
              })
              .then(res => {
                if (res.result) {
                  this.$bkMessage({
                    message: this.$t('成功'),
                    theme: 'success',
                  });
                  this.getTaskByProId(this.selectedProjectId);
                } else {
                  postMethodWarning(res.message, 'error');
                }
              })
              ['finally'](() => {
                this.taskLoading = false;
              });
          } else {
            this.taskLoading = false;
            this.$bkMessage({
              message: this.$t('不能删除正在运行中的项目'),
              theme: 'error',
            });
          }
        },
        null,
        { type: 'warning', okText: this.$t('确定'), theme: 'danger' }
      );
    },
    getDeleteTips(flow) {
      const content = flow.status === 'no-start' ? this.$t('删除') : this.$t('运行中的任务不能删除');
      return {
        interactive: false,
        placement: 'top',
        boundary: document.body,
        content,
      };
    },
    handleScrollCallback() {
      const pageCount = Math.ceil(this.searchedTask.length / this.pageSize);
      if (pageCount > this.currentPage && !this.currentPageUpdating) {
        this.currentPageUpdating = true;
        this.currentPage++;
        this.$nextTick(() => {
          setTimeout(() => {
            this.currentPageUpdating = false;
          }, 300);
        });
      }
    },
    handleProSelect(val, item) {
      bkStorage.set(this.bkStorageKey, val);
      this.$router.push({
        name: 'dataflow_ide',
        params: { fid: this.$route.params.fid },
        query: { project_id: val },
      });
      this.$nextTick(() => {
        this.getTsakAlert();
      });
    },
    handleDialogChange(isShow) {
      this.$emit('dialogChange', isShow);
    },
    taskDetail(data) {
      // 'dataflow_detail'
      this.$router.push({
        name: 'dataflow_ide',
        params: { fid: data.flow_id },
        query: { project_id: this.selectedProjectId },
      });
      this.$emit('viewTaskClick', data.flow_id);
    },
    getMineProjectList() {
      this.projectLoading = true;
      this.bkRequest
        .httpRequest('dataFlow/getProjectList', {
          query: {
            active: true,
          },
        })
        .then(res => {
          if (res.result) {
            this.projectList = res.data.filter(d => d.active);
            this.$store.dispatch(
              'updateProjectList',
              res.data.filter(d => d.active)
            ); // 任务修改需要用到
            this.projectList.forEach(item => {
              item.displayName = `[${item.project_id}]${item.project_name}`;
            });
            this.$nextTick(() => {
              if (!this.selectedProjectId && this.projectList.length) {
                this.$set(this, 'selectedProjectId', this.projectList[0].project_id);
                this.getTaskByProId(this.projectList[0].project_id);
              }
            });
          } else {
            postMethodWarning(`获取项目列表失败：${res.message}`, 'error', res.code);
          }
        })
        ['finally'](_ => {
          this.projectLoading = false;
        });
    },
    getTaskByProId(id) {
      this.taskLoading = true;
      this.bkRequest
        .httpRequest('dataFlow/getTaskByProjectId', { query: { project_id: id, add_exception_info: 1 } })
        .then(res => {
          if (res.result) {
            this.taskList = res.data;
          } else {
            this.permissionShow = true;
          }
        })
        ['finally'](_ => {
          this.taskLoading = false;
        });
    },
    // 新建项目
    newProject() {
      this.$refs.newproject.open();
    },
    // 新建任务
    newTask(item) {
      this.$refs.newTask.open({ project_id: this.selectedProjectId });
    },
    createNewTask(data) {
      this.isCreated = false;
      this.axios
        .post('v3/dataflow/flow/flows/', data)
        .then(res => {
          if (res.result) {
            this.$refs.newTask.close();
            this.$bkMessage({
              message: this.$t('成功'),
              theme: 'success',
            });
            this.taskList.unshift(res.data);
            this.$router.push({
              name: 'dataflow_ide',
              params: { fid: res.data.flow_id },
              query: { project_id: this.selectedProjectId },
            });
            this.$forceUpdate();
          } else {
            postMethodWarning(res.message, 'error');
          }
        })
        ['finally'](() => {
          this.isCreated = true;
        });
    },

    handleCreatedProject(params) {
      this.axios.post('v3/meta/projects/', params).then(res => {
        if (res.result) {
          this.$bkMessage({
            message: this.$t('成功'),
            theme: 'success',
          });
          this.$refs.newproject.close();
          Object.assign(params, { project_id: res.data, flows: [] });
          params.displayName = `[${params.project_id}]${params.project_name}`;
          this.search = '';
          this.projectList.unshift(params);
          this.$router.push({
            name: 'dataflow_ide',
            params: { fid: this.$route.params.fid },
            query: { project_id: res.data },
          });
        } else {
          postMethodWarning(res.message, 'error');
          this.$refs.newproject.reset();
        }
      });
    },
    getTsakAlert() {
      const query = {
        dimensions: JSON.stringify({
          project_id: this.$route.query.project_id,
          generate_type: 'user',
        }),
        group: 'flow_id',
      };
      this.bkRequest
        .httpRequest('dataAccess/getAlertCount', {
          query,
        })
        .then(res => {
          if (res.result) {
            this.taskAlertCount = res.data.groups;
          } else {
            postMethodWarning(res.message, 'error');
          }
        });
    },
  },
};
</script>
<style media="screen" lang="scss">
.project-list-flow {
  &.bk-select {
    border: none;
  }
}
.flow-newpro-tips {
  z-index: 1001;
}
.bk-form-content {
  text-align: left;
}
.project-slide {
  height: 100%;
  width: 100%;
  position: relative;

  .side-bar-hearder {
    display: flex;
    justify-content: flex-start;
    background: inherit;
    .menu {
      width: 57px;
      height: 49px;
      line-height: 49px;
      text-align: center;
      cursor: pointer;

      i {
        padding: 0;
        margin: 0;
      }
    }
  }

  .bk-icon-style {
    font-size: 14px;
    cursor: pointer;
    margin-left: 8px;
    line-height: 32px;
    &:hover {
      background: #f9f9ff;
      color: #3a84ff;
    }
  }

  .project {
    height: calc(100vh - 235px);
    // margin: 10px;
    // padding-bottom: 40px;
    border-top: solid 1px #ddd;
    // border-radius: 3px;
    overflow: auto;
    // box-shadow: 0px 3px 4px rgba(0, 0, 0, 0.15);
    li {
      border-bottom: 1px solid #eaeaea;
      display: flex;
      padding: 0 15px;
      &.active {
        background: #e1ecff;
        color: #3a84ff;
      }
    }
    .project-box {
      padding: 3px 20px;
      background: #fafafa;
      display: flex;
      /* justify-content: center; */
      align-items: center;
      //   line-height: 46px;
      font-size: 12px;
      color: #212232;
      cursor: pointer;
      &:hover {
        color: #3a84ff;
        background: #eee;
        .new-task {
          display: block;
        }

        .bk-items-len {
          display: none;
        }
      }

      .bk-badge {
        min-width: 15px;
        height: 15px;
        padding: 0 1px;
        border-radius: 15px;
        display: flex;
        justify-content: center;
        align-items: center;
        font-size: 8px;
        line-height: 15px;
      }
    }
    .folder {
      margin-right: 10px;
      font-size: 16px;
      vertical-align: -1px;
      float: left;
    }
    .project-name {
      width: calc(100% - 63px);
      float: left;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .new-task {
      font-size: 12px;
      width: 23px;
      height: 22px;
      line-height: 23px;
      text-align: center;
      cursor: pointer;
      border-radius: 2px;
      //   margin-top: 12px;
      position: relative;
      &:hover {
        background: #3a84ff;
        color: #fff;
        .tooltip {
          display: block;
        }
      }
      .tooltip {
        display: none;
        position: absolute;
        top: -6px;
        left: -8px;
        transform: translate(-100%, 0%);
        background: #212232;
        padding: 6px 8px;
        border-radius: 2px;
        color: #fff;
        width: 68px;
        font-size: 12px;
        &:after {
          content: '';
          width: 0;
          height: 0;
          border: 6px solid #fff;
          position: absolute;
          left: calc(100% + 6px);
          top: 50%;
          border-color: transparent transparent transparent #212232;
          transform: translate(-50%, -50%);
        }
      }
    }
    .task-list {
      font-size: 12px;
      min-height: 35px;
      transition: all linear 0.2s;
      li {
        line-height: 32px;
        // padding: 0 20px;
        cursor: pointer;
        .icon-delete {
          display: none;
        }
        &:hover {
          color: #3a84ff;
          background: #eee;
          .icon-delete {
            display: flex;
          }
        }
        .no-start {
          color: #e0e0e0;
        }
        .exception {
          color: #fe7f2a;
        }
        .running {
          color: #a3ce74;
        }
      }
    }
    .task-name {
      padding-left: 7px;
      width: calc(100% - 20px);
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      text-align: left;
    }
    .alert-container {
      display: flex;
      align-items: center;
      flex-wrap: nowrap;
      .icon-alert {
        margin-right: 5px;
        padding-top: 2px;
        color: #ea3636;
      }
      .count {
        max-width: 35px;
        min-width: 10px;
      }
    }
    .slide-enter-active,
    .slide-leave-active {
      overflow: hidden;
    }
    .slide-enter,
    .slide-leave-to {
      height: 0 !important;
    }
    /* 设置滚动条的样式 */
    &::-webkit-scrollbar {
      width: 5px;
      background-color: #f5f5f5;
    }

    /* 滚动槽 */
    &::-webkit-scrollbar-track {
      box-shadow: inset 0 0 6px #eee;
      border-radius: 10px;
      background-color: #f5f5f5;
    }

    /* 滚动条滑块 */
    &::-webkit-scrollbar-thumb {
      border-radius: 10px;
      background-color: rgba(0, 0, 0, 0.3);
    }
    &::-webkit-scrollbar-thumb {
      border-radius: 10px;
      background-color: rgba(0, 0, 0, 0.3);
    }
  }

  .bar-search {
    display: flex;
    position: relative;
    display: flex;
    align-items: center;
    justify-content: flex-start;
    text-align: left;
    padding: 2.5px 10px;
    border-top: solid 1px #ddd;

    &:hover {
      .search-clear {
        display: block;
      }
    }

    &.search-task {
      border-bottom: solid 1px #ddd;
    }

    .bkdata-selector {
      .bkdata-selector-list {
        .text {
          white-space: nowrap;
          overflow: hidden;
          text-overflow: ellipsis;
        }
      }
      .bkdata-selector-wrapper {
        input {
          height: 30px;
          border: none;
        }

        i {
          top: 50%;
          transform: translateY(-50%);
        }
      }
    }

    .search-task-label {
      width: 57px;
      text-align: right;
    }

    .search-content {
      width: 100%;
      display: flex;
      align-items: center;
      justify-content: flex-start;

      input {
        width: calc(100% - 30px);
        height: 30px;
        line-height: 30px;
        padding: 0 10px;
        border: none;
        border-radius: 2px;
        font-size: 14px;
        color: #666;
        outline: none;
        -webkit-box-shadow: none;
        box-shadow: none;
        cursor: pointer;
        -webkit-transition: border 0.2s linear;
        transition: border 0.2s linear;
      }
    }
  }

  .search-clear {
    display: none;
    width: 25px;
    height: 25px;
    line-height: 25px;
    text-align: center;
    position: absolute;
    right: 40px;
    top: 50%;
    transform: translateY(-50%);
    cursor: pointer;
  }
  .search-btn {
    display: inline-block;
    width: 35px;
    height: 30px;
    line-height: 30px;
    text-align: center;
    cursor: pointer;
    color: #999;
    position: absolute;
    right: 30px;
    top: 50%;
    transform: translateY(-50%);
    #cover-style {
      position: relative;
      top: 0;
      right: 0;
    }

    i {
      margin: 0;
    }
  }

  .no-data {
    text-align: center;
    margin: 20px;
    p {
      color: #cfd3dd;
    }
  }

  .slide-bottom {
    position: absolute;
    bottom: 0px;
    left: 0;
    right: 0;
    line-height: 24px;
    padding: 5px;
    display: flex;
    justify-content: center;
    background: #3a84ff;
    color: #fff;
    align-items: center;
    transform: translateY(calc(100% + 10px));
    box-shadow: 0px 3px 4px rgba(0, 0, 0, 0.32);
    i {
      margin: 0;
      &:hover {
        color: #fff;
        background: #3a84ff;
      }
    }

    &:hover {
      cursor: pointer;
    }
  }
}
</style>
