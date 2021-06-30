

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
  <div class="project clearfix">
    <div class="project-left fl">
      <!-- 我的项目 start -->
      <div v-if="scenarios === 'project'"
        class="bk-form bk-inline-form">
        <div class="bk-form-item is-required">
          <div class="bk-form-content"
            style="float: none; display: flex">
            <bkdata-input v-model="searchContent"
              :placeholder="$t('请输入项目名称进行搜索')"
              :rightIcon="'icon-search'"
              style="width: 500px; margin-right: 20px"
              @input="searchProject" />
            <bkdata-radio-group v-model="radio.name">
              <bkdata-radio :value="'releted'">
                {{ $t('我参与的项目') }}
              </bkdata-radio>
              <bkdata-radio :value="'manage'">
                {{ $t('我管理的项目') }}
              </bkdata-radio>
              <bkdata-radio :value="'delete'">
                {{ $t('已删除项目') }}
              </bkdata-radio>
            </bkdata-radio-group>
          </div>
        </div>
      </div>
      <!-- 我的项目 end -->
      <!-- 样本集 start -->
      <div v-if="scenarios === 'sample'"
        class="bk-form bk-inline-form">
        <div class="bk-form-item fl is-required">
          <div class="bk-form-content">
            <div class="fl mb15">
              <span class="fl pr15"> {{ $t('项目名称') }}： </span>
              <div class="selector-wrap fl">
                <bkdata-selector :displayKey="'project_name'"
                  :isLoading="isProjectListLoading"
                  :allowClear="true"
                  :placeholder="placeholder"
                  :list="projectLists"
                  :searchable="true"
                  :selected.sync="sampleProjectId"
                  :settingKey="'project_id'"
                  :searchKey="'project_name'" />
              </div>
            </div>
            <div class="fl mb15">
              <span class="fl pr15"> {{ $t('样本集名称') }}： </span>
              <div class="selector-wrap fl">
                <div class="search-content">
                  <bkdata-input v-model="sampleSetName"
                    :rightIcon="'icon-search'"
                    :clearable="true"
                    :placeholder="$t('回车搜索')"
                    @keydown.native="handleKeyDown" />
                </div>
              </div>
            </div>
            <div class="fl mb15">
              <span class="fl pr15"> {{ $t('场景') }}： </span>
              <div class="selector-wrap fl">
                <bkdata-selector :displayKey="'scene_zh_name'"
                  :allowClear="true"
                  :placeholder="placeholder"
                  :list="sampleSceneList"
                  :selected.sync="selectedSampleScene"
                  :settingKey="'scene_name'" />
              </div>
            </div>
            <div class="fl">
              <span class="fl pr15"> {{ $t('公开') }}： </span>
              <div class="selector-wrap model-public fl">
                <bkdata-selector :displayKey="'name'"
                  :allowClear="true"
                  :list="openList"
                  :selected.sync="samplePublic"
                  :settingKey="'key'" />
              </div>
            </div>
          </div>
        </div>
      </div>
      <!-- 样本集 end -->
      <!-- 我的任务 start -->
      <div class="bk-form bk-inline-form">
        <div class="bk-form-item is-required">
          <div v-if="scenarios === 'dataflow'"
            class="bk-form-content">
            <span class="fl pr15"> {{ $t('项目名称') }}： </span>
            <div class="fl task-flex">
              <div class="selector-wrap fl">
                <bkdata-selector
                  :displayKey="'project_name'"
                  :isLoading="isProjectListLoading"
                  :placeholder="placeholder"
                  :allowClear="true"
                  :list="projectLists"
                  :searchable="true"
                  :selected.sync="project_id"
                  :settingKey="'project_id'"
                  :searchKey="'project_name'"
                  @clear="clearProjectId"
                  @item-selected="handleItemSelected" />
              </div>
            </div>
            <div class="fl task-flex">
              <label class="bk-label"> {{ $t('任务名称') }}： </label>
              <div class="selector-wrap fl">
                <div class="search-content">
                  <bkdata-input id="searchTask"
                    v-model="searchVal"
                    class="search-input"
                    :placeholder="$t('回车搜索')"
                    @keydown.native="handleKeyDown" />
                  <span v-if="searchVal.length"
                    class="search-clear"
                    @click="clearSearch">
                    <i class="icon-close-circle-shape" />
                  </span>
                  <span v-if="searchVal.length === 0"
                    class="search-btn fr">
                    <i id="cover-style"
                      class="icon-search" />
                  </span>
                </div>
              </div>
            </div>
            <div class="fl task-flex">
              <label class="bk-label"> {{ $t('结果数据表') }}： </label>
              <div class="select-wrapper">
                <RtSelect />
              </div>
            </div>
          </div>
        </div>
      </div>
      <!-- 我的任务 end -->
      <!-- 我的模型 start -->
      <div class="bk-form fl bk-inline-form">
        <div class="bk-form-item is-required">
          <div v-if="scenarios === 'model'"
            class="bk-form-content">
            <span class="fl pr15"> {{ $t('项目名称') }}： </span>
            <div class="fl task-flex">
              <div class="selector-wrap fl">
                <bkdata-selector :displayKey="'project_name'"
                  :isLoading="isProjectListLoading"
                  :allowClear="true"
                  :placeholder="placeholder"
                  :list="projectLists"
                  :searchable="true"
                  :selected.sync="modelProjectId"
                  :settingKey="'project_id'"
                  :searchKey="'project_name'" />
              </div>
            </div>
            <div class="fl task-flex">
              <label class="bk-label"> {{ $t('模型名称') }}： </label>
              <div class="selector-wrap fl">
                <div class="search-content">
                  <bkdata-input v-model="searchModelName"
                    class="search-input"
                    :placeholder="$t('回车搜索')"
                    @keydown.native="handleKeyDown" />
                  <span v-if="searchModelName.length"
                    class="search-clear"
                    @click="clearModelName">
                    <i class="icon-close-circle-shape" />
                  </span>
                  <span v-if="searchModelName.length === 0"
                    class="search-btn fr">
                    <i id="cover-style"
                      class="icon-search" />
                  </span>
                </div>
              </div>
            </div>
            <span class="fl pr15"> {{ $t('公开') }}： </span>
            <div class="selector-wrap fl model-public">
              <bkdata-selector :displayKey="'name'"
                :allowClear="true"
                :list="openList"
                :selected.sync="samplePublic"
                :settingKey="'key'" />
            </div>
          </div>
        </div>
      </div>
      <!-- 我的模型 end -->
      <!-- 我的告警 start -->
      <div class="bk-form fl bk-inline-form">
        <div class="bk-form-item is-required">
          <div v-if="scenarios === 'alert'"
            class="bk-form-content">
            <div class="fl alert-flex alert-filter">
              <div class="fl alert-flex">
                <div class="alert-date-picker fl">
                  <bkdata-date-picker v-model="alertParams.selectedAlertTime"
                    :type="'datetimerange'"
                    :placeholder="$t('选择告警时间范围')"
                    :clearable="false"
                    @pick-success="changeAlertTime()" />
                </div>
              </div>
              <div class="fl alert-flex">
                <div class="alert-selector fl">
                  <bkdata-selector :displayKey="'name'"
                    :allowClear="true"
                    :list="alertParams.alertTypeList"
                    :placeholder="$t('告警类型')"
                    :selected.sync="alertParams.selectedAlertType"
                    :settingKey="'key'" />
                </div>
              </div>
              <div class="fl alert-flex">
                <div class="alert-selector fl">
                  <bkdata-selector :displayKey="'name'"
                    :allowClear="true"
                    :list="alertParams.alertLevelList"
                    :placeholder="$t('告警级别')"
                    :selected.sync="alertParams.selectedAlertLevel"
                    :settingKey="'key'" />
                </div>
              </div>
              <div class="fl alert-flex">
                <div class="alert-selector fl">
                  <bkdata-selector :displayKey="'name'"
                    :allowClear="true"
                    :list="alertParams.alertStatusList"
                    :placeholder="$t('告警状态')"
                    :selected.sync="alertParams.selectedAlertStatus"
                    :settingKey="'key'"
                    :style="{ width: '100px' }" />
                </div>
              </div>
            </div>
            <br>
            <div class="fl alert-flex alert-target-filter">
              <div class="fl alert-flex">
                <div class="alert-bottom-selector first-bottom fl">
                  <bkdata-selector :selected.sync="alertParams.alertTargetType"
                    :list="alertParams.targetTypeList"
                    :settingKey="'key'"
                    :placeholder="$t('告警对象类型')"
                    :displayKey="'name'"
                    :allowClear="true" />
                </div>
              </div>
              <div v-if="alertParams.alertTargetType === 'rawdata'"
                class="fl alert-flex">
                <div class="alert-bottom-selector fl">
                  <bkdata-selector :selected.sync="alertParams.bkBizId"
                    :filterable="true"
                    :searchable="true"
                    :placeholder="alertParams.bizHoder"
                    :list="alertParams.bizList"
                    :settingKey="'bk_biz_id'"
                    :displayKey="'bk_biz_name'"
                    :allowClear="true"
                    searchKey="bk_biz_name" />
                </div>
              </div>
              <div v-if="alertParams.alertTargetType === 'dataflow'"
                class="fl alert-flex">
                <div class="alert-bottom-selector fl">
                  <bkdata-selector :selected.sync="alertParams.projectId"
                    :filterable="true"
                    :searchable="true"
                    :placeholder="alertParams.projectHoder"
                    :list="alertParams.projectList"
                    :settingKey="'project_id'"
                    :displayKey="'project_alias'"
                    :allowClear="true"
                    searchKey="project_alias" />
                </div>
              </div>
              <div v-if="alertParams.alertTargetType === 'dataflow'
                     || alertParams.alertTargetType === 'rawdata'"
                class="fl alert-flex">
                <div class="alert-target-selector alert-bottom-selector fl">
                  <bkdata-selector :displayKey="'name'"
                    :allowClear="true"
                    :list="alertParams.alertTargetList"
                    :placeholder="$t('告警对象')"
                    :selected.sync="alertParams.selectedAlertTarget"
                    :settingKey="'key'" />
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
      <!-- 我的告警 end -->
    </div>
    <!-- 我的项目 start -->
    <div v-if="radio.name === 'delete' && scenarios === 'project'"
      class="project-right">
      <div class="project-right-sort">
        <span class="fl pr15"> {{ $t('项目排序') }}： </span>
        <div class="fl">
          <bkdata-selector :list="sort.lists"
            :selected.sync="sort.selected"
            :settingKey="'id'"
            :displayKey="'label'"
            @item-selected="sortProject" />
        </div>
      </div>
    </div>
    <div v-else-if="scenarios === 'project'"
      class="project-right">
      <div class="project-right-sort">
        <span class="fl pr15"> {{ $t('项目排序') }}： </span>
        <div class="fl pr15">
          <bkdata-selector :list="sort.list"
            :selected.sync="sort.selected"
            :settingKey="'id'"
            :displayKey="'label'"
            @item-selected="sortProject" />
        </div>
        <div class="fl"
          style="display: flex; align-items: center">
          <bkdata-button v-show="activeName === 'project'"
            theme="primary"
            @click="showCreateNewProjectWindow">
            <i :title="$t('创建项目')"
              class="icon-add_dataflow mr5" />
            {{ $t('创建项目') }}
          </bkdata-button>

          <bkdata-button v-show="activeName !== 'project'"
            theme="primary"
            @click="showCreateNewTaskWindow">
            <i :title="$t('创建任务')"
              class="icon-add_dataflow mr5" />
            {{ $t('创建任务') }}
          </bkdata-button>
        </div>
      </div>
    </div>
    <!-- 我的项目 end -->
    <!-- 我的任务 start -->
    <div v-if="scenarios === 'dataflow'"
      class="project-right">
      <div class="project-right-sort">
        <div class="fr">
          <bkdata-button v-show="activeName !== 'project'"
            type="primary"
            style="z-index: 2; height: 30px"
            @click="showCreateNewTaskWindow">
            <i :title="$t('创建任务')"
              class="icon-add_dataflow mr5" />
            {{ $t('创建任务') }}
          </bkdata-button>
        </div>
        <div v-if="isBatchOptionShow"
          v-tooltip.notrigger.left="batchOptToolTip"
          class="bath-operation fr"
          @mouseenter="enterBatchOpt"
          @mouseleave="leaveBatchOpt">
          <div class="op"
            :class="{ disable: !project_id }"
            @click.stop="batchClick('start')"
            @mouseleave="batchOperation">
            <span class="active">
              <i :title="$t('批量启动')"
                class="icon-play-shape" />
            </span>
            <span>{{ $t('批量启动') }}</span>

            <div v-show="batchOptionObj.start.isDetailShow"
              class="tip">
              <p>{{ $t('即将启动_个任务', { num: selectedList.length, start: activeList.length }) }}</p>
              <div class="tip-blist_project_tagutton mt10">
                <bkdata-button theme="primary"
                  size="small"
                  @click.stop="batchStart">
                  {{ $t('确定') }}
                </bkdata-button>
                <bkdata-button theme="default"
                  size="small"
                  @click.stop="batchOperation('all')">
                  {{ $t('取消') }}
                </bkdata-button>
              </div>
            </div>
            <div :class="['tooltip', { show: batchOptionObj.start.isShow }]">
              {{ $t('请选择任务') }}
            </div>
          </div>
          <div class="op"
            :class="{ disable: !project_id }"
            @click.stop="batchClick('stop')"
            @mouseleave="batchOperation">
            <span class="active fs18 stop">
              <i :title="$t('批量停止')"
                class="icon-stop-shape" />
            </span>
            <span>{{ $t('批量停止') }}</span>
            <div :class="['tooltip', { show: batchOptionObj.stop.isShow }]">
              {{ $t('请选择任务') }}
            </div>
            <div v-show="batchOptionObj.stop.isDetailShow"
              class="tip">
              <p>{{ $t('即将停止_个任务', { num: selectedList.length, stop: activeList.length }) }}</p>
              <div class="tip-button mt15">
                <bkdata-button theme="primary"
                  size="small"
                  @click.stop="batchStop">
                  {{ $t('确定') }}
                </bkdata-button>
                <bkdata-button theme="default"
                  size="small"
                  @click.stop="batchOperation('all')">
                  {{ $t('取消') }}
                </bkdata-button>
              </div>
            </div>
          </div>
          <div class="op batch-delete"
            @click.stop="batchClick('del')"
            @mouseenter="enterBatchDel"
            @mouseleave="batchOperation">
            <span class="active">
              <i :title="$t('批量删除')"
                class="icon-delete active" />
            </span>
            <span>{{ $t('批量删除') }}</span>
            <div :class="['tooltip', { show: batchOptionObj.del.isShow }]">
              {{ $t('请选择任务') }}
            </div>
            <div v-show="batchOptionObj.del.isDetailShow"
              class="tip">
              <p>{{ $t('即将删除_个任务', { num: selectedList.length, delete: activeList.length }) }}</p>
              <div class="tip-button mt15">
                <bkdata-button theme="primary"
                  size="small"
                  @click.stop="batchDelete">
                  {{ $t('确定') }}
                </bkdata-button>
                <bkdata-button theme="default"
                  size="small"
                  @click.stop="batchOperation('all')">
                  {{ $t('取消') }}
                </bkdata-button>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
    <!-- 我的任务 end -->
    <div v-if="scenarios === 'sample'"
      class="project-right">
      <div class="project-right-sort">
        <div class="fr">
          <bkdata-button theme="primary"
            @click="showCreateNewSampleSetWindow">
            <i :title="$t('创建样本集')"
              class="icon-add_dataflow mr5" />
            {{ $t('创建样本集') }}
          </bkdata-button>
        </div>
      </div>
    </div>
    <div v-if="scenarios === 'alert'"
      class="project-right fr">
      <div class="project-right-sort">
        <div class="fr">
          <bkdata-button theme="primary"
            class="mr5"
            @click="linkAlertConfigAdd">
            <i :title="$t('新建告警配置')"
              class="icon-add_dataflow mr5" />
            {{ $t('新建告警配置') }}
          </bkdata-button>
          <bkdata-button theme="primary"
            @click="linkAlertConfigList">
            <i :title="$t('查看告警配置')"
              class="icon-outline-list mr5" />
            {{ $t('查看告警配置') }}
          </bkdata-button>
        </div>
      </div>
    </div>
  </div>
</template>
<script>
import Vue from 'vue';
import Bus from '@/common/js/bus.js';
import moment from 'moment';
import bkStorage from '@/common/js/bkStorage.js';
import { userPermScopes } from '@/common/api/auth';
import { postMethodWarning, compare } from '@/common/js/util.js';
import RtSelect from './RtSelect';

export default {
  components: {
    RtSelect,
  },
  props: {
    activeName: {
      type: String,
    },
    defaultRadioName: {
      type: String,
      default: ''
    },
    selectedList: {
      type: Array,
      default: () => {
        [];
      },
    },
    isBatchOptionShow: {
      type: Boolean,
    },
    status: {
      type: String,
    },
    projectTastList: {
      type: Array,
      default: () => {
        [];
      },
    },
    selectedProjectId: {
      type: Number,
    },
    scenarios: {
      type: String,
    },
  },
  data() {
    return {
      batchOptToolTip: {
        content: this.$t('请选择具体项目进行批量启停'),
        visible: false,
      },
      initObj: {
        show: {
          isSampleShow: false,
        },
      },
      placeholder: this.$t('全部项目'),
      createHolder: this.$t('创建'),
      searchContent: '',
      radio: {
        name: 'releted',
      },
      project_id: '',
      projectLists: [],
      sort: {
        list: [
          {
            label: this.$t('创建时间'),
            id: 'createTime',
          },
          {
            label: this.$t('项目名称'),
            id: 'projectName',
          },
        ],
        lists: [
          {
            label: this.$t('删除时间'),
            id: 'createTime',
          },
          {
            label: this.$t('项目名称'),
            id: 'projectName',
          },
        ],
        selected: 'createTime',
      },
      projectCount: null,
      batchOptionObj: {
        start: {
          isShow: false,
          isDetailShow: false,
        },
        stop: {
          isShow: false,
          isDetailShow: false,
        },
        del: {
          isShow: false,
          isDetailShow: false,
        },
      },
      activeList: [],
      timer: '',
      projectList: [
        {
          id: 1,
          name: '张三',
        },
      ],
      searchVal: '',
      sampleProjectId: 0,
      samplePublic: '',
      sampleSetName: '',
      selectedSampleScene: '',
      sampleSceneList: [],
      sampleScene: '',
      openList: [
        {
          name: this.$t('是'),
          key: 'public',
        },
        {
          name: this.$t('否'),
          key: 'noPublic',
        },
      ],
      alertParams: {
        inited: false,
        bkBizId: '',
        bizList: [],
        bizHoder: '',
        bizLoading: false,
        projectId: '',
        projectList: [],
        projectHoder: '',
        projectLoading: false,
        targetTypeList: [
          {
            name: this.$t('数据开发任务'),
            key: 'dataflow',
          },
          {
            name: this.$t('数据源'),
            key: 'rawdata',
          },
        ],
        alertTargetType: '',
        selectedAlertTime: [moment().subtract(1, 'days')
          .format('YYYY-MM-DD HH:mm:ss'), moment().format('YYYY-MM-DD HH:mm:ss')],
        selectedAlertStatus: ['alerting'],
        selectedAlertType: '',
        selectedAlertLevel: '',
        selectedAlertTarget: '',
        alertSearchTime: [],
        alertTargetList: [],
        alertTypeList: [
          {
            name: this.$t('数据监控'),
            key: 'data_monitor',
          },
          {
            name: this.$t('任务监控'),
            key: 'task_monitor',
          },
        ],
        alertLevelList: [
          {
            name: this.$t('警告'),
            key: 'warning',
          },
          {
            name: this.$t('严重'),
            key: 'danger',
          },
        ],
        alertStatusList: [
          {
            name: this.$t('告警中'),
            key: 'alerting',
          },
          {
            name: this.$t('已屏蔽'),
            key: 'shielded',
          },
          {
            name: this.$t('已收敛'),
            key: 'converged',
          },
        ],
      },
      searchModelName: '',
      modelProjectId: 0,
      isProjectListLoading: false,
      bkStorageKey: 'UserCenterMyTaskProjectId',
    };
  },
  computed: {
    endDate() {
      return moment(new Date()).format('YYYY-MM-DD');
    },
  },
  watch: {
    selectedSampleScene(newVal) {
      Bus.$emit('sampleSceneChange', newVal);
    },
    // 仅查看公开样本集
    samplePublic: {
      handler(newVal) {
        if (newVal === 'public') {
          Bus.$emit('samplePublicChange', true);
        } else if (newVal === 'noPublic') {
          Bus.$emit('samplePublicChange', false);
        } else {
          Bus.$emit('samplePublicChange', '');
        }
      },
    },
    sampleProjectId(newVal) {
      Bus.$emit('sampleProjectIdChange', newVal);
    },
    modelProjectId(val) {
      Bus.$emit('modelProjectIdChange', val);
    },
    'radio.name'(val) {
      if (this.activeName === 'dataflow') {
        // Bus.$emit('taskTypeChange', val)
      } else {
        this.$emit('projectTypeChange', val);
      }
    },
    activeName: function (newVal) {
      if (newVal === 'dataflow') {
        this.radio.name = 'releted';
        this.sort.selected = 'createTime';
      }
    },
    selectedList(newVal) {
      let activeArr = this.projectTastList.filter(item => {
        return newVal.includes(item.flow_id);
      });
      this.activeList = activeArr.filter(item => {
        return item.status !== 'running';
      });
    },
    selectedProjectId: {
      immediate: true,
      handler(newVal) {
        this.project_id = newVal;
      },
    },
    scenarios: {
      immediate: true,
      handler(newVal) {
        if (newVal === 'sample') {
          this.initObj.show.isSampleShow = true;
          // this.getSampleSceneList()
        }
      },
    },
    projectCount(newVal) {
      if (newVal) {
        this.$emit('projectCountChange', newVal);
      }
    },
    'alertParams.selectedAlertType': {
      handler(alertType) {
        Bus.$emit('alertTypeChange', alertType);
      },
    },
    'alertParams.selectedAlertTime': {
      handler(alertTime) {
        // 控件设置默认值时会自动触发一次watch事件
        if (!this.alertParams.inited) {
          this.alertParams.inited = true;
        }
        if (this.alertParams.alertTargetType === 'dataflow' || this.alertParams.alertTargetType === 'rawdata') {
          this.updateAlertTargetList();
        }
      },
    },
    'alertParams.selectedAlertTarget': {
      handler(alertTarget) {
        Bus.$emit('alertTargetChange', alertTarget);
      },
    },
    'alertParams.selectedAlertLevel': {
      handler(alertLevel) {
        Bus.$emit('alertLevelChange', alertLevel);
      },
    },
    'alertParams.selectedAlertStatus': {
      handler(alertStatus) {
        Bus.$emit('alertStatusChange', alertStatus);
      },
    },
    'alertParams.alertTargetType': {
      handler(targetType) {
        if (targetType === 'rawdata') {
          if (this.alertParams.bizList.length === 0 && !this.alertParams.bizLoading) {
            this.getAlertBizList();
          }
        } else {
          if (this.alertParams.projectList.length === 0 && !this.alertParams.projectLoading) {
            this.getAlertProjectList();
          }
        }
        this.updateAlertTargetList();
      },
    },
    'alertParams.bkBizId': {
      handler(bkBizId) {
        this.updateAlertTargetList();
      },
    },
    'alertParams.projectId': {
      handler(projectId) {
        this.updateAlertTargetList();
      },
    },
  },
  mounted() {
    const projectId = this.$route.query.projectId || this.selectedProjectId;
    let storageProjectId = null;
    if (!projectId) {
      storageProjectId = bkStorage.get(this.bkStorageKey);
      this.project_id = storageProjectId;
    }
    this.getProjectSummary(projectId || storageProjectId);
    // 初始化
    if (this.$route.query.projectId || storageProjectId) {
      this.$emit('receiveProjectInfo', this.$route.query.projectId || storageProjectId);
    }
    Bus.$on('getProjectSummary', () => {
      // 注册获取项目统计信息事件
      this.getProjectSummary(this.project_id);
    });
    this.getProjectInfo();
    if (this.$route.query.alert_target_id) {
      this.alertParams.alertTargetType = this.$route.query.alert_target_type;
      if (this.$route.query.alert_target_type === 'dataflow') {
        this.alertParams.selectedAlertTarget = this.$route.query.alert_target_id;
      } else if (this.$route.query.alert_target_type === 'rawdata') {
        this.alertParams.selectedAlertTarget = `rawdata${this.$route.query.alert_target_id}`;
      }
      this.alertParams.alertTargetList.push(this.formatAlertTargetOption(this.$route.query));
    }

    if (this.defaultRadioName !== '') {
      this.radio.name = this.defaultRadioName;
    }
  },
  methods: {
    // 样本集场景列表
    getSampleSceneList() {
      this.bkRequest.httpRequest('sample/getSampleScene').then(res => {
        if (res.result) {
          this.sampleSceneList = res.data.map(item => {
            item.disabled = item.status !== 'finished';
            return item;
          });
        } else {
          this.getMethodWarning(res.message, res.code);
        }
      });
    },
    clearModelName() {
      this.searchModelName = '';
      Bus.$emit('sampleSetModelNameChange', this.searchModelName);
    },
    // 清除样本集项目id
    clearSampleSelectedId() {
      Bus.$emit('sampleProjectIdChange', 0);
    },
    // 任务搜索
    handleKeyDown(e) {
      if (e.key === 'Enter') {
        let key = this.$route.query.tab;
        if (key === 'dataflow') {
          Bus.$emit('myTaskSearch', this.searchVal);
        } else if (key === 'sample') {
          Bus.$emit('sampleSetNameChange', this.sampleSetName);
        } else if (key === 'model') {
          Bus.$emit('sampleSetModelNameChange', this.searchModelName);
        } else if (key === 'alert') {
          Bus.$emit('alertTypeChange', alertType);
        }
      }
    },
    clearSearch() {
      this.searchVal = '';
      this.sampleSetName = '';
      Bus.$emit('myTaskSearch', this.searchVal);
      Bus.$emit('sampleSetNameChange', this.sampleSetName);
    },
    handleItemSelected(val, item) {
      bkStorage.set(this.bkStorageKey, val);
      // 添加路由参数
      this.$router.push({
        path: '/user-center',
        query: {
          tab: 'dataflow',
          projectId: val,
          projectName: item.project_name,
        },
      });
      this.$emit('empty');
      this.getProjectSummary(val);
      this.$emit('receiveProjectInfo', val);
    },
    enterBatchDel() {
      this.batchOptToolTip.visible = false;
    },
    enterBatchOpt(e) {
      if (!this.project_id) {
        this.batchOptToolTip.visible = true;
      }
    },
    leaveBatchOpt(e) {
      if (!this.project_id) {
        this.batchOptToolTip.visible = false;
      }
    },
    clearProjectId() {
      this.$router.push({
        path: '/user-center',
        query: {
          tab: 'dataflow',
        },
      });
      this.getProjectSummary();
      this.$emit('Clear-Project-Id', this.projectLists);
      bkStorage.set(this.bkStorageKey, '');
    },

    /** @description
         * 项目排序
         */
    sortProject() {
      this.$emit('sort', this.sort.selected);
    },

    reset() {
      this.searchContent = '';
    },

    /** @description
         * 搜索相关任务
         */
    searchProject() {
      if (this.activeName === 'dataflow') {
        Bus.$emit('searchTask', this.searchContent);
      } else {
        this.$emit('search', this.searchContent);
      }
    },

    /** @description
         *  创建新项目
         */
    showCreateNewProjectWindow() {
      Bus.$emit('showCreateNewProjectWindow');
    },
    showCreateNewTaskWindow() {
      // if (!this.project_id) return
      let data = this.projectLists.filter(item => {
        return item.project_id === Number(this.project_id);
      })[0];
      Bus.$emit('showCreateNewTaskWindow', data);
    },
    // 创建样本集
    showCreateNewSampleSetWindow() {
      let data = this.projectLists.filter(item => {
        return item.project_id === Number(this.sampleProjectId);
      })[0];
      Bus.$emit('showCreateNewSampleSetWindow', data);
    },
    getProjectInfo() {
      // 获取我的项目信息
      this.isProjectListLoading = true;
      this.bkRequest
        .httpRequest('dataFlow/getProjectList', {
          query: {
            active: true,
          },
        })
        .then(resp => {
          if (resp.result) {
            this.projectLists = resp.data;
            // if (!this.$route.query.projectId) {
            //     this.$emit('sendProjectIdList', resp.data) // 往mytask组件发送项目id
            // }
          } else {
            postMethodWarning(resp.message, 'error');
          }
          this.isProjectListLoading = false;
        });
    },
    getProjectSummary(projectId) {
      // 获取项目统计信息
      let options = {
        query: {},
      };
      projectId && Object.assign(options, { query: { project_id: projectId } });
      this.bkRequest.httpRequest('dataFlow/getProjectSummary', options).then(resp => {
        if (resp.result) {
          this.projectCount = resp.data.reduce(
            (pre, current) => {
              return Object.assign(pre, {
                count: pre.count + current.count,
                running_count: pre.running_count + current.running_count,
                exception_count: pre.exception_count + current.exception_count,
                normal_count: pre.normal_count + current.normal_count,
                no_start_count: pre.no_start_count + current.no_start_count,
              });
            },
            {
              count: 0,
              running_count: 0,
              exception_count: 0,
              normal_count: 0,
              no_start_count: 0,
            }
          );
        } else {
          postMethodWarning(resp.message, 'error');
        }
      });
    },
    /*
         * 批量操作点击
         * */
    batchOperation(status) {
      this.batchOptToolTip.visible = false;
      Object.keys(this.batchOptionObj).forEach(attr => {
        Object.keys(this.batchOptionObj[attr]).forEach(item => {
          if (status === 'all') {
            this.batchOptionObj[attr][item] = false;
          } else {
            this.batchOptionObj[attr].isShow = false;
          }
        });
      });
    },
    batchClick(active) {
      this.batchOperation('all');
      if (!this.selectedList.length) {
        this.batchOptionObj[active].isShow = true;
      } else {
        this.batchOptionObj[active].isDetailShow = true;
      }
      let status;
      if (active === 'start') {
        status = 'no-start';
      } else if (active === 'stop') {
        status = 'running';
      } else if (active === 'del') {
        status = 'no-start';
      }
      this.getActiveTask(status);
    },
    async batchStop() {
      this.activeList = this.activeList.filter(item => item.node_count !== 0);
      if (this.activeList.length === 0) {
        this.$bkMessage({
          message: this.$t('没有任务停止'),
          theme: 'warning',
          delay: 0,
          hasCloseIcon: true,
        });
        this.batchOperation('all');
        return;
      }
      Bus.$emit(
        'changeStopList',
        this.activeList.map(item => item.flow_id)
      );
      let idList = this.activeList.map(item => {
        return item.flow_id;
      });
      let params = {
        flow_ids: idList,
      };
      let res;
      await this.axios.post('/v3/dataflow/flow/flows/multi_stop/', params).then(response => {
        res = response;
      });
      if (res.result) {
        if (Object.keys(res.data.success_flows).length) {
          this.$bkMessage({
            message: this.$t('批量停止操作成功'),
            theme: 'success',
          });
        }
        this.batchSuccess(res.data);
      } else {
        postMethodWarning(res.message, 'error');
      }
      this.$emit('changeStatus', 'stopping'); // 手动修改状态
      this.$emit('empty'); // 清空所有选中项
      this.pollTaskStatus();
      this.batchOperation('all');
    },

    /*
         *@批量停止,批量删除,批量启动 二次确认
         */
    async batchStart() {
      this.activeList = this.activeList.filter(item => item.node_count !== 0);
      if (this.activeList.length === 0) {
        this.$bkMessage({
          message: this.$t('没有任务启动'),
          theme: 'warning',
          delay: 0,
          hasCloseIcon: true,
        });
        this.batchOperation('all');
        return;
      }
      Bus.$emit(
        'changeStartList',
        this.activeList.map(item => item.flow_id)
      );
      let idList = this.activeList.map(item => {
        return item.flow_id;
      });
      let params = {
        flow_ids: idList,
      };
      let res;
      let filterList = [];

      await this.axios.post('v3/dataflow/flow/flows/multi_start/', params).then(response => {
        res = response;
        if (res.result) {
          this.batchSuccess(res.data);
          if (Object.keys(res.data.success_flows).length) {
            this.$bkMessage({
              message: this.$t('批量启动操作成功'),
              theme: 'success',
            });
          }
          // 失败任务的提示
          if (!Object.keys(res.data.fail_flows).length) return;
          let failFlows = res.data.fail_flows;
          let message = '';
          let tempArr = [];
          Object.keys(res.data.fail_flows).forEach(attr => {
            filterList.push(Number(attr));
          });
          for (let key in failFlows) {
            let fName = this.getFlowByFid(key).flow_name;
            tempArr.push(this.$createElement('p', `${this.$t('任务')}${fName}: ${failFlows[key]}`));
          }
          message = this.$createElement('div', tempArr);
          this.$bkMessage({
            message: message,
            theme: 'warning',
            hasCloseIcon: true,
            delay: 0,
          });
        } else {
          postMethodWarning(res.message, 'error');
        }
      });
      this.$emit('changeStatus', 'starting', filterList);
      this.$emit('empty'); // 清空所有选中项
      this.pollTaskStatus(); // 轮询
      this.batchOperation('all');
    },
    async batchDelete() {
      if (this.activeList.length === 0) {
        this.$bkMessage({
          message: this.$t('没有任务删除'),
          theme: 'warning',
          delay: 0,
          hasCloseIcon: true,
        });
        this.batchOperation('all');
        return;
      }
      let idList = this.activeList.map(item => {
        return item.flow_id;
      });
      let params = {
        flow_ids: idList,
      };
      let res;
      let bkMsg = {
        msg: '',
        theme: '',
      };
      await this.axios['delete']('/v3/dataflow/flow/flows/multi_destroy/', { data: params }).then(response => {
        res = response;
      });
      if (res.result) {
        if (Object.keys(res.data.fail_flows).length) {
          let failFlows = res.data.fail_flows;
          let message = '';
          let tempArr = [];
          for (let key in failFlows) {
            let fName = this.getFlowByFid(key).flow_name;
            tempArr.push(this.$createElement('p', `${this.$t('任务')}${fName}: ${failFlows[key]}`));
          }
          message = this.$createElement('div', tempArr);
          this.$bkMessage({
            message: message,
            theme: 'warning',
            hasCloseIcon: true,
            delay: 0,
          });
          this.$emit('empty');
          this.batchOperation('all');
          return;
        }
        this.$bkMessage({
          message: this.$t('批量删除成功'),
          theme: 'success',
        });
        // 删除
        this.delectSuccess();
      } else {
        postMethodWarning(res.message, 'error');
      }
      this.$emit('empty');
      this.batchOperation('all');
    },

    /** @description
         * 根据flowid获取项目
         */
    getFlowByFid(fid) {
      let len = this.projectTastList.length;
      let tempF = {};
      this.projectTastList.forEach(p => {
        if (Number(p.flow_id) === Number(fid)) {
          tempF = p;
        }
      });
      return tempF;
    },

    /*
         *@批量删除成功
         */
    delectSuccess() {
      let repeatList = [];
      this.projectTastList.forEach(item => {
        if (this.activeList.includes(item)) {
          repeatList.push(item);
        }
      });
      this.$emit('del', repeatList);
    },

    /*
         * 批量操作成功
         */
    batchSuccess(result) {
      if (Object.keys(result.success_flows).length > 0) {
        let successArr = Object.keys(result.success_flows);
        this.projectTastList.forEach(item => {
          if (successArr.includes(item.flow_id)) {
            item.process_status = 'pending';
          }
        });
      } else {
        let fail = result.fail_flows[Object.keys(result.fail_flows)[0]];
        this.$bkMessage({
          message: fail,
          theme: 'error',
        });
      }
    },
    getActiveTask(status) {
      let activeArr = this.projectTastList.filter(item => {
        return this.selectedList.includes(item.flow_id);
      });
      this.activeList = activeArr.filter(item => {
        return item.status === status;
      });
    },
    pollTaskStatus() {
      Bus.$emit('polltaskQuery');
    },

    /**
         * 告警时间范围选择
         */
    changeAlertTime() {
      this.updateAlertTargetList();
      Bus.$emit('alertTimeChange', this.alertParams.selectedAlertTime);
    },

    /**
         * 更新告警对象下拉框可选值
         */
    updateAlertTargetList() {
      const startTime = moment(this.alertParams.selectedAlertTime[0]).format('YYYY-MM-DD HH:mm:ss');
      const endTime = moment(this.alertParams.selectedAlertTime[1]).format('YYYY-MM-DD HH:mm:ss');
      const options = {
        query: {
          start_time: startTime,
          end_time: endTime,
          base: 'alert',
        },
      };
      if (this.alertParams.bkBizId) {
        options.query.bk_biz_id = this.alertParams.bkBizId;
      }
      if (this.alertParams.projectId) {
        options.query.project_id = this.alertParams.projectId;
      }
      if (this.alertParams.alertTargetType) {
        options.query.alert_target_type = this.alertParams.alertTargetType;
      }
      this.bkRequest.httpRequest('dmonitorCenter/getMineAlertTargetList', options).then(res => {
        if (res.result) {
          this.alertParams.alertTargetList = res.data;
          this.alertParams.alertTargetList.map(alertTarget => {
            Object.assign(alertTarget, this.formatAlertTargetOption(alertTarget));
            return alertTarget;
          });
        } else {
          this.getMethodWarning(res.message, 'error');
        }
      });
    },
    getAlertBizList() {
      this.alertParams.bizHoder = this.$t('数据加载中');
      this.alertParams.bizLoading = true;
      const options = {
        params: {
          action_id: 'raw_data.update',
          dimension: 'bk_biz_id',
        },
      };
      this.bkRequest
        .httpRequest('dmonitorCenter/getMineAuthScopeDimension', options)
        .then(res => {
          if (res.result) {
            this.alertParams.bizList = res.data;
            this.alertParams.bizList.map(biz => {
              biz.bk_biz_id = Number(biz.bk_biz_id);
              biz.bk_biz_name = `[${biz.bk_biz_id}]${biz.bk_biz_name}`;
              return biz;
            });
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.alertParams.bizHoder = this.$t('请选择业务');
          this.alertParams.bizLoading = false;
        });
    },
    getAlertProjectList() {
      this.alertParams.projectHoder = this.$t('数据加载中');
      this.alertParams.projectLoading = true;

      userPermScopes({
        show_display: true,
        action_id: 'project.manage_flow',
      })
        .then(res => {
          if (res.result) {
            this.alertParams.projectList = res.data.filter(project => {
              if (project.project_id === undefined) {
                console.log(project);
                return false;
              }
              return true;
            });
            this.alertParams.projectList.map(project => {
              project.project_id = Number(project.project_id);
              project.project_alias = project.project_name;
              return project;
            });
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.alertParams.projectHoder = this.$t('请选择项目');
          this.alertParams.projectLoading = false;
        });
    },

    /**
         * 跳转到告警配置列表页
         */
    linkAlertConfigList() {
      this.$router.push('/dmonitor-center/alert-config/');
    },

    /**
         * 新建告警配置
         */
    linkAlertConfigAdd() {
      this.$router.push('/dmonitor-center/alert-config/add');
    },

    /**
         * 格式化告警对象选项
         */
    formatAlertTargetOption(alertTarget) {
      if (alertTarget.alert_target_type === 'dataflow') {
        return {
          name: `[${alertTarget.alert_target_id}] ${alertTarget.alert_target_alias}`,
          key: alertTarget.alert_target_id,
        };
      } else {
        return {
          name: `[${alertTarget.alert_target_id}] ${alertTarget.alert_target_alias}`,
          key: `rawdata${alertTarget.alert_target_id}`,
        };
      }
    },
  },
};
</script>
<style lang="scss">
.bk-option {
  min-width: 80px;
}

.bk-inline-form {
  display: flex;
  align-items: center;
  .bk-form-item {
    .bk-form-content {
      .selector-wrap {
        width: 227px;
        margin-right: 15px;
      }
      .model-public {
        width: 135px;
      }
      .alert-flex {
        .alert-date-picker {
          width: 300px;
          .bk-date-picker {
            width: 300px;
          }
        }
        .alert-bottom-selector {
          margin-top: 10px;
          width: 200px;
          margin-left: 15px;
        }
        .alert-bottom-selector.first-bottom {
          margin-left: 0;
        }
        .alert-target-selector {
          width: 200px;
        }
        .alert-selector {
          width: 100px;
          margin-left: 15px;
        }
      }
    }
  }
}
$textColor: #3a84ff;
.project {
  position: relative;
  &-left {
    font-size: 14px;
    .selector-wrap {
      width: 227px;
      margin: 0px 15px 20px 0px;
      .search-content {
        position: relative;
        width: 100%;
        display: flex;
        align-items: center;
        justify-content: flex-start;
        border: 1px solid #c3cdd7;
        height: 32px;
        line-height: 32px;
        input {
          width: calc(100% - 30px);
          height: 30px;
          line-height: 30px;
          padding: 0 10px;
          border: none;
          border-radius: 2px;
          color: #666;
          outline: none;
          -webkit-box-shadow: none;
          box-shadow: none;
          cursor: pointer;
          -webkit-transition: border 0.2s linear;
          transition: border 0.2s linear;
          display: block;
        }
        .search-clear {
          width: 25px;
          height: 25px;
          line-height: 25px;
          text-align: center;
          position: absolute;
          right: 0px;
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
          right: 0px;
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
      }
    }
    .selector-wrap:last-child {
      margin-bottom: 0px;
    }
    .bk-button {
      padding: 0px 10px;
      margin: 0px 20px 0px 0px;
    }
    .bk-form {
      position: relative;
      .bk-form-radio {
        .icon-check {
          margin-top: -3px;
        }
      }
      &-input {
        padding-right: 28px;
      }
      &-item {
        .bk-form-content {
          margin-left: 1px;
          .task-flex {
            display: flex;
            align-items: center;
          }
          .fl {
            .bk-label {
              white-space: nowrap;
              color: #737987;
              font-size: 14px;
              font-weight: 400;
            }
            .bk-label::after {
              content: '';
            }
            .radio-button {
              font-size: 14px;
              height: 30px;
              line-height: 30px;
            }
          }
          span {
            white-space: nowrap;
            height: 32px;
            line-height: 32px;
          }
        }
      }
      .search-icon {
        position: absolute;
        left: calc(100% - 20px);
        top: 12px;
      }
    }
  }
  &-left {
    width: 85%;
  }
  &-right {
    position: absolute;
    right: 0px;
    .project-right-sort {
      display: flex;
      align-items: center;
      .project-status {
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
      .bath-operation {
        .disable {
          color: #babddd !important;
          cursor: not-allowed;
          pointer-events: none;
        }
        .op {
          float: left;
          height: 30px;
          line-height: 30px;
          border: 1px solid #eaeaea;
          margin-left: -1px;
          display: flex;
          justify-content: space-between;
        }
        .active {
          color: #c3cdd7;
          display: flex;
          align-items: center;
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
          z-index: 1;
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
        .tip-blist_project_tagutton {
          display: flex;
          button:first-child {
            margin-right: 10px;
          }
        }
        .tip-button {
          display: flex;
          height: 40px;
          button:first-child {
            margin-right: 10px;
          }
        }
      }
    }
  }
}
</style>
