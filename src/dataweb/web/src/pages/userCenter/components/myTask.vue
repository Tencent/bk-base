

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
  <div class="task">
    <projectOperation-Bar
      ref="taskbar"
      :projectTastList="projectTastList"
      :selectedProjectId="selectedProjectId"
      :isBatchOptionShow="true"
      :selectedList="project.selectArr"
      :status="active"
      :scenarios="'dataflow'"
      @projectCountChange="projectCountChange"
      @Clear-Project-Id="handleClearProId"
      @receiveProjectInfo="changeProject"
      @empty="handleEmpty"
      @changeStatus="changeStatus"
      @del="handleDel" />
    <div v-bkloading="{ isLoading: isLoading }"
      class="my-task">
      <div class="my-task-item">
        <div class="my-task-flows">
          <bkdata-table :data="projectTastPageList"
            :emptyText="$t('暂无数据')"
            :pagination="pagination"
            @select-all="handleRowChecked"
            @select="handleRowChecked"
            @page-change="handlePageChange"
            @page-limit-change="handlePageLimitChange">
            <bkdata-table-column type="selection"
              width="60"
              align="center" />
            <bkdata-table-column :label="$t('任务名称')"
              minWidth="150"
              prop="flow_name">
              <div slot-scope="props"
                class="bk-table-inlineblock">
                <div class="table-row-taskname"
                  @click.stop="taskDetail(props.row)">
                  <span :class="['status',
                                 props.row.status === 'no-start'
                                   ? 'no-start' : props.row.status === 'running'
                                     && !props.row.has_exception ? 'running' : 'abnormal']">
                    <i class="bk-icon icon-dataflow" />
                  </span>
                  <p class="project-name text-overflow"
                    :title="props.row.flow_name">
                    {{ props.row.flow_name }}
                  </p>
                </div>
              </div>
            </bkdata-table-column>
            <bkdata-table-column :label="$t('项目名称')"
              width="200"
              prop="project_name">
              <div slot-scope="props"
                :title="props.row.project_name"
                class="text-overflow">
                {{ props.row.project_name }}
              </div>
            </bkdata-table-column>
            <bkdata-table-column :label="$t('节点数量')"
              width="115"
              prop="node_count" />
            <bkdata-table-column :label="$t('创建人')"
              width="150"
              prop="created_by" />
            <bkdata-table-column :label="$t('更新时间')"
              width="170"
              prop="updated_at" />
            <bkdata-table-column :label="$t('描述')">
              <div slot-scope="props"
                :title="props.row.description"
                class="text-overflow">
                {{ props.row.description }}
              </div>
            </bkdata-table-column>
            <bkdata-table-column :label="$t('操作')"
              width="220">
              <div slot-scope="props"
                class="bk-table-inlineblock">
                <a href="javascript:;"
                  class="operation-button"
                  @click.stop="taskDetail(props.row)">
                  {{ $t('查看详情') }}
                </a>
                <a
                  v-if="props.row.status === 'running'"
                  href="javascript:;"
                  :class="[
                    'operation-button ml20',
                    {
                      disable: props.row.process_status === 'pending'
                        || props.row.status === 'starting'
                        || props.row.status === 'no-start'
                        || props.row.status === 'stopping'
                        || props.row.node_count === 0,
                    },
                  ]"
                  @click.stop="stopPrompt(props.row)">
                  {{ $t('停止') }}
                </a>
                <a
                  v-else
                  href="javascript:;"
                  :class="[
                    'operation-button ml20',
                    {
                      disable: props.row.process_status === 'pending'
                        || props.row.status === 'stopping'
                        || props.row.status === 'running'
                        || props.row.status === 'starting'
                        || props.row.node_count === 0,
                    },
                  ]"
                  @click.stop="startPrompt(props.row)">
                  {{ $t('启动') }}
                </a>
                <a
                  href="javascript:;"
                  :title="props.row.process_status === 'pending'
                    || props.row.status === 'running'
                    || props.row.status === 'starting'
                    || props.row.status === 'stopping' ? $t('不能删除正在运行中的任务') : ''"
                  :class="[
                    'operation-button ml20',
                    {
                      disable: props.row.process_status === 'pending'
                        || props.row.status === 'running'
                        || props.row.status === 'starting'
                        || props.row.status === 'stopping',
                    },
                  ]"
                  @click.stop="delectPrompt(props.row)">
                  {{ $t('删除') }}
                </a>
              </div>
            </bkdata-table-column>
            <bkdata-table-column :label="$t('操作状态')"
              width="120">
              <div slot-scope="props"
                class="bk-table-inlineblock">
                <span v-if="props.row.status === 'running' && props.row.process_status !== 'pending'"
                  class="operation-running"
                  :title="$t('点击查看详情')"
                  @click.stop="operationInfo(props.row)">
                  <i class="bk-icon icon-check-circle-shape" />
                </span>
                <span v-if="props.row.status === 'failure'"
                  class="operation-abnormal"
                  :title="$t('点击查看详情')"
                  @click.stop="operationInfo(props.row)">
                  <i class="bk-icon icon-close-circle-shape" />
                </span>
                <span
                  v-show="props.row.process_status === 'pending'
                    || props.row.status === 'starting'
                    || props.row.status === 'stopping'
                    || props.row.clickStatus === 'starting'
                    || props.row.clickStatus === 'stopping'"
                  v-bkloading="{
                    isLoading: props.row.process_status === 'pending'
                      || props.row.status === 'starting'
                      || props.row.status === 'stopping'
                      || props.row.clickStatus === 'starting'
                      || props.row.clickStatus === 'stopping',
                  }"
                  class="operation-pending"
                  :title="$t('点击查看详情')"
                  @click.stop="operationInfo(props.row)" />
                <span v-if="props.row.status === 'no-start' && props.row.process_status !== 'pending'"
                  :title="$t('最近无操作')">
                  -
                </span>
              </div>
            </bkdata-table-column>
          </bkdata-table>
          <div v-if="projectCount"
            class="project-status fl">
            <label class="bk-label fl"> {{ $t('任务状态') }}： </label>
            <div class="status">
              <div class="status-item">
                {{ $t('正常') }} <span class="running">{{ projectCount.normal_count }}</span>
              </div>
              <div class="status-item">
                {{ $t('异常') }} <span class="abnormal">{{ projectCount.exception_count }}</span>
              </div>
              <div class="status-item">
                {{ $t('未启动') }} <span class="not-started">{{ projectCount.no_start_count }}</span>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- 确认停止流程 start-->
    <bkdata-dialog v-model="process.isShow"
      extCls="bkdata-dialog"
      :hasFooter="false"
      :okText="process.buttonText"
      :cancelText="$t('取消')"
      theme="danger"
      :title="process.title"
      @confirm="confirm(process.data)"
      @cancel="processCancel">
      <span style="height: 50px; display: inline-block" />
    </bkdata-dialog>
    <operatingInfo ref="operationInfo" />
    <new-task ref="newTask"
      :isCreated="isCreated"
      @createNewTask="createNewTask" />
  </div>
</template>
<script>
import operatingInfo from '@/pages/userCenter/components/operatingInfo.vue';
import projectOperationBar from './projectOperationBar.vue';
import newTask from '@/pages/DataGraph/Graph/Components/project/newTask';
import { postMethodWarning } from '@/common/js/util.js';
import Bus from '@/common/js/bus.js';

export default {
  components: {
    newTask,
    operatingInfo,
    projectOperationBar,
  },
  props: {
    activeProject: {
      type: Object,
    },
    activeName: {
      type: String,
    },
    selectedProjectId: {
      type: Number,
    },
  },
  data() {
    return {
      isLoading: false,
      opType: '',
      batch: {
        // 批量参数
        start: [],
        stop: [],
        delete: [],
      },
      isCreated: false, // 任务创建是否完成
      multiple: [],
      thead: [],
      pageSize: 10,
      dataCount: 0,
      totalPage: 1,
      curPage: 1,
      isProject: true, // 传过来的project是否展开
      searchContent: '',
      project: {
        projectList: [], // 项目列表
        projectLoading: false,
        selectArr: [],
      },
      showTasks: {}, // 显示任务全局控制 刷新不更新
      params: {
        add_node_count_info: 1,
        add_process_status_info: 1,
        ordering: 'active',
      },
      count: {
        noStartCount: 0,
        runningCount: 0,
        errorCount: 0,
      },
      visible: -1,
      sort: 'createTime',
      process: {
        // 二次确认参数
        isShow: false,
        title: this.$t('确认启动该流程'),
        buttonText: this.$t('启动'),
        buttonLoading: false,
        content: '',
        data: {},
      },
      projectTastList: [],
      projectTastPageList: [],
      projectId: '',
      active: '',
      updataList: [],
      timer: '',
      startIdList: [], // 需要查看状态的id数组
      stopIdList: [],
      allTaskList: [],
      projectList: [],
      searchVal: '',
      projectCount: null,
    };
  },
  computed: {
    pagination() {
      return {
        current: this.curPage,
        count: this.dataCount,
        limit: this.pageSize,
      };
    },
  },
  watch: {
    projectId(newVal) {
      this.curPage = 1;
      this.initHead(newVal);
    },
    // curPage(newVal) {
    //     if (newVal) {
    //         this.getCurrentTaskList()
    //     }
    // }
  },
  mounted() {
    this.isLoading = true;
    this.curPage = 1;
    Bus.$on('polltaskQuery', () => {
      // 注册轮询请求事件
      this.polltaskQuery();
    });
    Bus.$on('changeStopList', list => {
      // 添加批量操作的任务id
      this.stopIdList = [...this.stopIdList, ...list];
    });
    Bus.$on('changeStartList', list => {
      // 添加批量操作的任务id
      this.startIdList = [...this.startIdList, ...list];
    });
    Bus.$on('myTaskSearch', searchVal => {
      this.searchVal = searchVal;
      this.init(true);
    });
    Bus.$on('taskRtSearchSelected', val => {
      this.getAlltasks(val);
    });
    // 我的项目详情跳转到我的任务初始化
    this.init();
    this.initHead(null);
  },
  beforeDestroy() {
    clearInterval(this.timer);
    Bus.$off('myTaskSearch');
  },
  methods: {
    handlePageLimitChange(limit) {
      this.pageSize = limit;
      this.curPage = 1;
      this.getCurrentTaskList();
    },
    handlePageChange(page) {
      this.curPage = page;
      this.getCurrentTaskList();
    },
    handleRowChecked(e, row) {
      this.project.selectArr = e.map(row => row.flow_id);
    },
    projectCountChange(projectCount) {
      this.projectCount = projectCount;
    },
    handleClearProId() {
      this.curPage = 1;
      this.projectId = '';
      Bus.$emit('clearSelectedProjectId'); // 没有选取项目时，重置selectedProjectId
      this.getAlltasks();
    },
    getCurrentTaskList(isSearch = false) {
      if (isSearch) this.curPage = 1; // 搜索任务时，页码重置为1
      if (!this.projectId && !this.selectedProjectId) {
        this.getAlltasks();
      } else {
        this.getTaskByProjectId(this.projectId || this.selectedProjectId);
      }
    },
    initHead(projectId) {
      this.thead = this.originalThead;
      if (projectId) {
        this.isLoading = true;
        this.getTaskByProjectId(projectId);
      }
    },
    handleAllTask(data) {
      if (data.length) {
        this.projectList = data;
        this.getAlltasks();
      }
    },
    getAlltasks(rtid) {
      this.isLoading = true;
      let projectIdList = this.projectList.map(child => {
        return child.project_id;
      });
      let options = {
        params: {
          page: this.curPage,
          page_size: this.pageSize,
          show_display: 1,
          add_node_count_info: 1,
          add_process_status_info: 1,
          search: this.searchVal,
          result_table_id: rtid,
        },
      };
      this.axios.get('v3/dataflow/flow/flows/', options).then(resp => {
        if (resp.result) {
          this.projectTastList = resp.data.results;
          // 分页功能
          this.projectTastPageList = resp.data.results;
          this.dataCount = resp.data.count;
          this.totalPage = Math.ceil(resp.data.count / this.pageSize);
          this.isLoading = false;
        } else {
          postMethodWarning(resp.message, 'error');
        }
      });
    },
    getTaskByProjectId(projectId, status) {
      // 根据项目ID获取任务信息
      this.isLoading = true;
      let options = {
        query: {
          page: this.curPage,
          page_size: this.pageSize,
          add_node_count_info: 1,
          add_process_status_info: 1,
          add_exception_info: 1,
          search: this.searchVal,
        },
      };

      projectId && Object.assign(options.query, { project_id: projectId });
      this.bkRequest
        .httpRequest('dataFlow/getTaskByProjectId', options)
        .then(resp => {
          if (resp.result) {
            // 默认按照更新时间排序
            let sortObj = {
              name: 'updated_at',
              is_reverse: -1,
            };
            this.projectTastList = this.sorting(sortObj, resp.data.results);
            // 筛选出处于开始中和停止中的数据
            this.projectTastList.forEach(item => {
              if (item.status === 'starting') {
                this.startIdList.push(item.flow_id);
              } else if (item.status === 'stopping') {
                this.stopIdList.push(item.flow_id);
              }
            });
            if (this.startIdList.length || this.stopIdList.length) {
              this.polltaskQuery();
            }
            // 分页功能
            // this.curPage = 1
            this.dataCount = resp.data.count;
            this.projectTastPageList = this.projectTastList.slice(0, this.pageSize);
            this.totalPage = Math.ceil(resp.data.count / this.pageSize);
          } else {
            postMethodWarning(resp.message, 'error');
          }
          this.isLoading = false;
        })
        ['finally'](() => {
          this.isLoading = false;
        });
    },
    operationInfo(task) {
      this.$refs.operationInfo.open(task);
    },
    /*
     * 新建任务
     */
    newTask(item) {
      this.$refs.newTask.open(item);
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
            this.newTaskSuccess(res.data);
          } else {
            postMethodWarning(res.message, 'error');
          }
        })
        ['finally'](() => {
          this.isCreated = true;
        });
    },
    /*
     * 二次确认
     */
    confirm(data) {
      if (this.process.buttonLoading) return;
      this.process.buttonLoading = true;
      if (this.process.buttonText === this.$t('启动')) {
        this.activate(data);
      } else if (this.process.buttonText === this.$t('停止')) {
        this.stop(data);
      } else {
        this.deleteDataFlow(data);
      }
    },
    // 启动
    async activate(data) {
      if (data.node_count === 0) {
        this.$bkMessage({
          message: this.$t('空画布不能被启动'),
          theme: 'warning',
        });
        this.process.isShow = false;
        this.process.buttonLoading = false;
        return;
      }
      this.startIdList.push(data.flow_id);
      let bkMsg = {
        msg: '',
        theme: '',
      };
      data.is_processing = true;
      this.process.buttonLoading = true;
      let res;
      await this.axios.post(`v3/dataflow/flow/flows/${data.flow_id}/start/`).then(response => {
        res = response;
      });
      if (res.result) {
        this.changeStatus('starting');
        this.polltaskQuery();
        bkMsg.msg = this.$t('开始启动任务');
        bkMsg.theme = 'success';
        data.process_status = 'pending';
        this.project.projectList.forEach(p => {
          if (p.project_id === data.project_id) {
            p.selectedFlows = [];
          }
        });
      } else {
        bkMsg.msg = res.message;
        bkMsg.theme = 'error';
      }
      this.process.isShow = false;
      this.$bkMessage({
        message: bkMsg.msg,
        hasCloseIcon: bkMsg.theme === 'success' ? 'false' : 'true',
        delay: bkMsg.theme === 'success' ? 3000 : 0,
        theme: bkMsg.theme,
      });
      this.process.buttonLoading = false;
    },
    // 停止
    async stop(data) {
      if (data.node_count === 0) {
        this.$bkMessage({
          message: this.$t('空画布不能被停止'),
          theme: 'warning',
        });
        return;
      }
      this.stopIdList.push(data.flow_id);
      data.is_processing = true;
      let bkMsg = {
        msg: '',
        theme: '',
      };
      let res;
      let params = this.$route.params;
      this.process.buttonLoading = true;
      await this.axios.post(`v3/dataflow/flow/flows/${data.flow_id}/stop/`).then(response => {
        res = response;
      });
      if (res.result) {
        this.changeStatus('starting');
        this.polltaskQuery();
        bkMsg.msg = this.$t('停止操作成功');
        bkMsg.theme = 'success';
        data.process_status = 'pending';
        this.project.projectList.forEach(p => {
          if (p.project_id === data.project_id) {
            p.selectedFlows = [];
          }
        });
      } else {
        bkMsg.msg = res.message;
        bkMsg.theme = 'error';
      }
      this.process.isShow = false;
      this.$bkMessage({
        message: bkMsg.msg,
        hasCloseIcon: true,
        delay: bkMsg.theme === 'success' ? 3000 : 0,
        theme: bkMsg.theme,
      });
      this.process.buttonLoading = false;
    },
    // 删除dataFlow
    deleteDataFlow(data) {
      if (data.status === 'no-start') {
        this.process.buttonLoading = true;
        this.bkRequest
          .httpRequest('dataFlow/deleteTask', {
            params: {
              flowId: data.flow_id,
            },
          })
          .then(res => {
            if (res.result) {
              this.$bkMessage({
                message: this.$t('成功'),
                theme: 'success',
              });
              this.getTaskByProjectId(this.projectId || this.selectedProjectId);
            } else {
              postMethodWarning(res.message, 'error');
            }
          })
          ['finally'](() => {
            this.process.isShow = false;
            this.process.buttonLoading = false;
          });
      } else {
        this.process.isShow = false;
        this.process.buttonLoading = false;
        this.$bkMessage({
          message: this.$t('不能删除正在运行中的项目'),
          theme: 'error',
        });
      }
    },
    processCancel() {
      this.process.isShow = false;
    },
    /*
     * 任务详情
     */
    taskDetail(data) {
      let projectId = 0;
      // 'dataflow_detail'
      if (!this.projectId) {
        projectId = data.project_id;
      } else {
        projectId = this.project_id;
      }
      this.$router.push({
        name: 'dataflow_ide',
        query: { project_id: projectId },
        params: { fid: data.flow_id },
      });
    },
    /*
     *@停止,删除,启动二次确认
     */
    startPrompt(data) {
      if (data.process_status === 'running' || data.process_status === 'pending' || data.node_count === 0) return;
      this.process.title = this.$t('确认启动该流程');
      this.process.buttonText = this.$t('启动');
      this.process.data = data;
      this.process.content = '';
      this.process.isShow = true;
    },
    stopPrompt(data) {
      if (data.process_status === 'running' || data.process_status === 'pending') return;
      this.process.title = this.$t('确认停止该流程');
      this.process.buttonText = this.$t('停止');
      this.process.data = data;
      this.process.content = '';
      this.process.isShow = true;
    },
    /** @description
     * 删除任务
     */
    delectPrompt(data) {
      if (data.process_status === 'running'
            || data.process_status === 'pending'
            || data.status === 'running') {
        return;
      };
      this.process.title = this.$t('确认删除任务');
      this.process.buttonText = this.$t('删除');
      this.process.content = this.$t('删除任务提示详情');
      this.process.data = data;
      this.process.isShow = true;
    },
    goTop() {
      let activeP;
      let react;
      this.$nextTick(() => {
        activeP = this.$el.querySelector('.my-task div.active');
        activeP
          && activeP.scrollIntoView({
            behavior: 'smooth',
          });
      });
    },
    /*
     * 新建任务成功
     */
    newTaskSuccess(flowData) {
      flowData.node_count = 0;
      flowData.process_status = 'no-status';
      this.projectTastList.unshift(flowData);
      this.projectTastPageList = this.projectTastList.slice(0, this.pageSize);
      if (this.projectId) {
        this.totalPage = Math.ceil(this.projectTastList.length / this.pageSize);
      }
    },
    /*
     * 传递projectId
     */
    changeProject(id) {
      this.projectId = id;
    },
    handleEmpty() {
      // 清空所有选中项
      this.project.selectArr = [];
    },
    handleDel(repeatList) {
      this.projectTastList = this.projectTastList.filter(item => {
        return !repeatList.includes(item);
      });
      // 分页功能
      this.projectTastPageList = this.projectTastList.slice(0, this.pageSize);
      this.totalPage = Math.ceil(this.projectTastList.length / this.pageSize);
      this.curPage = 1;
    },
    init(isSearch = false) {
      this.getCurrentTaskList(isSearch);
    },
    changeStatus(status, filterList = []) {
      // 请求完成后，手动改变任务的状态
      this.projectTastList.forEach(item => {
        if (this.project.selectArr.includes(item.flow_id)) {
          if (!filterList.includes(item.flow_id)) {
            item.clickStatus = status;
            item.status = status;
          }
        }
      });
      this.projectTastPageList.forEach(item => {
        if (this.project.selectArr.includes(item.flow_id)) {
          if (!filterList.includes(item.flow_id)) {
            item.clickStatus = status;
            item.status = status;
          }
        }
      });
    },
    /*
     * 排序传参
     * */
    sorting(item, tasks) {
      return tasks.sort(function (a, b) {
        if (a[item.name] > b[item.name]) {
          return item.is_reverse;
        }
        if (a[item.name] < b[item.name]) {
          return -item.is_reverse;
        }
      });
    },
    pollTaskStatus(id) {
      let options = {
        query: {
          add_node_count_info: 1,
          add_process_status_info: 1,
          add_exception_info: 1,
        },
      };
      id && Object.assign(options.query, { project_id: id });
      this.bkRequest.httpRequest('dataFlow/getTaskByProjectId', options).then(resp => {
        if (resp.result) {
          this.updataList = resp.data;
          this.handleUpdageTaskStatus();
          this.checkIdList();
          if (!this.startIdList.length && !this.stopIdList.length) {
            // 所有需要查询的任务达到目标状态后，取消间断请求
            clearInterval(this.timer);
          }
        } else {
          postMethodWarning(resp.message, 'error');
        }
      });
    },
    handleUpdageTaskStatus() {
      for (var i = 0; i < this.updataList.length; i++) {
        for (var j = 0; j < this.projectTastList.length; j++) {
          if (this.updataList[i].flow_id === this.projectTastList[j].flow_id) {
            if (this.updataList[i].status !== this.projectTastList[j].status) {
              if (this.updataList[i].status === 'running' || this.updataList[i].status === 'no-start') {
                this.projectTastList[j].process_status = '';
                this.projectTastList[j].clickStatus = '';
              }
              this.projectTastList[j].status = this.updataList[i].status;
            }
          }
        }
      }
    },
    changeTaskProcessStatus(id, status) {
      const taskIndex = this.projectTastList.findIndex(task => task.flow_id === id);
      this.projectTastList[taskIndex].process_status = status;
    },

    // 检测请求到的数据里需要更新的状态是否达到目标状态，达到的话，从数组里删除
    checkIdList() {
      this.updataList.forEach(item => {
        if (this.startIdList.includes(Number(item.flow_id))) {
          if (item.status === 'running') {
            this.startIdList.splice(
              this.startIdList.findIndex(child => child === Number(item.flow_id)),
              1
            );
          }
          if (item.process_status === 'failure') {
            this.$bkMessage({
              message: `任务${item.flow_name}执行失败！`,
              theme: 'warning',
            });
            this.changeTaskProcessStatus(item.flow_id, item.process_status);
            this.startIdList.splice(
              this.startIdList.findIndex(child => child === Number(item.flow_id)),
              1
            );
          }
        }
        if (this.stopIdList.includes(Number(item.flow_id))) {
          if (item.status === 'no-start') {
            this.stopIdList.splice(
              this.stopIdList.findIndex(child => child === Number(item.flow_id)),
              1
            );
          }
          if (item.process_status === 'failure') {
            this.$bkMessage({
              message: `任务${item.flow_name}执行失败！`,
              theme: 'warning',
            });
            this.changeTaskProcessStatus(item.flow_id, item.process_status);
            this.stopIdList.splice(
              this.stopIdList.findIndex(child => child === Number(item.flow_id)),
              1
            );
          }
        }
      });
    },
    // 间断请求项目任务的状态
    polltaskQuery() {
      clearInterval(this.timer);
      this.timer = setInterval(() => {
        this.pollTaskStatus(this.projectId);
        Bus.$emit('getProjectSummary');
      }, 10000);
    },
  },
};
</script>
<style lang="scss">
.task {
  .fs18 {
    font-size: 18px;
  }
  min-height: 200px;
  .dialog-button {
    margin: 10px 0 30px;
    text-align: center;
    .bk-button {
      margin-top: 20px;
      width: 120px;
      margin-right: 15px;
    }
  }
  > .bk-loading .bk-loading-wrapper {
    top: 100px;
    z-index: 1;
  }
  td {
    .bk-loading .bk-loading-wrapper {
      top: 50%;
    }
  }
  .my-task {
    border-bottom: none;
    overflow: hidden;
    &:last-of-type {
      border-bottom: 1px solid #eaeaea;
    }
    &-item {
      .name {
        width: 325px;
        padding: 0 14px;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
      }
    }
    &-project {
      line-height: 40px;
      height: 40px;
      background: #fafafa;
      .status {
        display: flex;
        font-size: 12px;
        > div {
          padding-left: 12px;
          .normal {
            color: #81af50;
          }
          .abnormal {
            color: #ff6600;
          }
        }
      }
      &:hover .bath-operation {
        display: block !important;
      }
      .bath-operation {
        .op {
          float: left;
          line-height: 38px;
          border-left: 1px solid #eaeaea;
        }
        .active {
          color: #c3cdd7;
        }
        .stop {
          vertical-align: middle;
        }
        div.op {
          float: left;
          padding: 0 10px;
          cursor: pointer;
          position: relative;
          &:last-of-type {
            border-left: 1px solid #eaeaea;
          }
          &:first-of-type {
            border-left: 1px solid #eaeaea;
          }
          &:hover {
            > span {
              color: #3a84ff;
            }
            .active {
              color: #3a84ff;
            }
          }
        }
        .tip {
          z-index: 1;
          position: absolute;
          right: 0;
          top: 39px;
          min-width: 162px;
          padding: 15px 20px;
          background: #fff;
          border: 1px solid #e6e9f0;
          box-shadow: -2px 0px 6px rgba(26, 27, 45, 0.2);
          p {
            line-height: 18px;
          }
        }
        .tooltip {
          display: none;
          width: 90px;
          position: absolute;
          text-align: center;
          bottom: 39px;
          left: 50%;
          padding: 5px 10px;
          background: #212232;
          line-height: 14px;
          color: #fff;
          border-radius: 2px;
          font-size: 12px;
          transform: translate(-50%, 0);
          &:after {
            content: '';
            width: 0px;
            height: 0px;
            position: absolute;
            left: 50%;
            bottom: -8px;
            border: 4px solid #000;
            border-color: #000 transparent transparent transparent;
            transform: translate(-50%, 0);
          }
          &.show {
            display: block;
          }
        }
        .tip-button {
          height: 40px;
          text-align: center;
        }
      }
    }
    .active {
      .name {
        color: #3a84ff;
        font-weight: 700;
      }
    }
    &-flows {
      margin-top: 20px;
      .bk-table {
        .bk-form-checkbox {
          margin-right: 0px;
        }
      }
      .bk-table-inlineblock {
        .operation-button {
          color: #3a84ff;
        }
        .disable {
          color: #babddd !important;
          cursor: not-allowed;
        }
      }

      .table-row-taskname {
        display: flex;
        cursor: pointer;

        span {
          &.status {
            padding: 0 5px 0 0;
            color: #666;
            &.running {
              color: #a3ce74;
            }
            &.abnormal {
              color: #fe7f2a;
            }
          }
        }
        .project-name {
          color: #3a84ff;
        }
      }

      .operation-running {
        color: #a3ce74;
        text-align: center;
        cursor: pointer;
      }

      .operation-abnormal {
        color: #fe7f2a;
        text-align: center;
        cursor: pointer;
      }

      .project-status {
        margin-top: 20px;
        .status {
          float: left;
          margin-right: 15px;
          color: #666;
          .status-item {
            float: left;
            padding-left: 12px;
            color: #666;
            .running {
              color: #a3ce74;
            }
            .abnormal {
              color: #fe7f2a;
            }
          }
        }
      }
      .bk-page-wrap {
        float: right;
        margin-top: 20px;
      }
    }
  }
}
</style>
