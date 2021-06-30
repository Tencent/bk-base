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
    <div class="tabs-project-opbar">
      <project-bar ref="projectbar"
        :scenarios="'project'"
        :activeName="activeName"
        :isBatchOptionShow="false"
        :defaultRadioName="activeType"
        @sort="projectSort"
        @projectTypeChange="projectTypeChange"
        @search="searchProject" />
    </div>
    <div v-bkloading="{ isLoading: contentsLoading }"
      class="contents">
      <bkdataVirtueList v-slot="slotProps"
        :list="resultProjectList"
        :lineHeight="248"
        :pageSize="30"
        :groupItemCount="groupItemCount"
        class="contents-list-container">
        <div v-for="item of slotProps.data"
          :key="item.$index"
          class="contents-list">
          <div v-show="!contentsLoading"
            class="contents-item">
            <div class="contents-item-top clearfix"
              :class="{ 'contents-item-deleting': item.deleting }"
              @click="linkToFlow(item)">
              <div class="title">
                <div class="title-left">
                  <span class="title-text">
                    [{{ item.project_id }}]{{ item.project_name }}
                  </span>
                  <span class="title-edit"
                    @click.stop="projectDetails(item)">
                    <i :title="$t('编辑')"
                      class="bk-icon icon-edit" />
                  </span>
                </div>

                <span class="title-icon">
                  <i class="bk-icon icon-folder-shape" />
                </span>
              </div>
            </div>
            <div class="contents-item-mid"
              :class="{ 'contents-item-mid-deleting': item.deleting }">
              <div class="dataflows clearfix">
                <span class="title">{{ $t('任务') }}</span>
                <div class="dataflows-status">
                  <div class="status-item-wrapper">
                    <span class="status-title">{{ $t('正常') }}</span>
                    <span class="normal">{{ item.normal_count }}</span>
                  </div>
                  <div class="status-item-wrapper">
                    <span class="status-title">{{ $t('异常') }}</span>
                    <span class="warning">{{ item.exception_count }}</span>
                  </div>
                  <div class="status-item-wrapper">
                    <span class="status-title">{{ $t('未启动') }}</span>
                    <span class="no-start">{{ item.no_start_count }}</span>
                  </div>
                </div>
                <span v-if="item.active"
                  v-show="!item.tags || !item.tags.manage.lol_billing"
                  class="details"
                  @click="linkToDf(item)">
                  {{ $t('详情') }}
                </span>
              </div>
              <div class="dataflows clearfix">
                <span class="title">{{ $t('笔记') }}</span>
                <div class="dataflows-status">
                  <div class="dataflows-status-book status-item-wrapper text-align">
                    <span class="status-title">{{ $t('数量') }}</span>
                    <span class="no-start">{{ item.notebook_count }}</span>
                  </div>
                </div>
                <span v-if="item.active"
                  v-show="!item.tags || !item.tags.manage.lol_billing"
                  class="details"
                  @click="toDataExplore(item.project_id)">
                  {{ $t('详情') }}
                </span>
              </div>
              <div v-if="item.modelflow_counts"
                class="dataflows">
                <span class="title">{{ $t('模型') }}</span>
                <div class="dataflows-status">
                  <div>
                    {{ item.modelflow_counts.building.status_display }}
                    <span class="normal">
                      {{ item.modelflow_counts.building.count }}
                    </span>
                  </div>
                  <div>
                    {{ item.modelflow_counts.complete.status_display }}
                    <span class="normal">
                      {{ item.modelflow_counts.complete.count }}
                    </span>
                  </div>
                </div>
                <a class="details"
                  :href="item.modelflow_counts.project_url">
                  {{ $t('详情') }}
                </a>
              </div>
            </div>
            <div v-if="item.active"
              class="contents-item-bottom"
              :class="{ 'contents-item-bottom-deleting': item.deleting }">
              <div class="op-left">
                <i :title="$t('创建任务')"
                  class="bk-icon icon-add_task"
                  @click="showCreateNewDataflowWindow(item)">
                  <span>
                    {{ $t('创建任务') }}
                  </span>
                </i>
                <span v-if="item.tags
                        && item.tags.manage
                        && item.tags.manage.geog_area
                        && item.tags.manage.geog_area.length"
                  class="item-cloud-area"
                  :title="item.tags.manage.geog_area[0].alias">
                  {{ item.tags.manage.geog_area[0].alias }}
                </span>
              </div>
              <div class="op-right">
                <i v-if="$modules.isActive('resource_manage')"
                  class="bk-icon icon-resourses"
                  @click="openResourceGroupApply(item)">
                  <span>
                    {{ $t('资源组') }}
                  </span>
                </i>
                <i v-if="isModeflowRedirect"
                  class="bk-icon icon-model-flow-62"
                  @click="openModeFlow(item)">
                  <span>
                    {{ $t('管理模型') }}
                  </span>
                </i>
                <!--<i class="bk-icon icon-user"  @click="addMember(item)">-->
                <i class="bk-icon icon-user"
                  @click="openMemberManager(item)">
                  <span>
                    {{ $t('成员管理') }}
                  </span>
                </i>
                <i class="bk-icon icon-data2"
                  @click="applyBiz(item)">
                  <span>
                    {{ $t('业务数据') }}
                  </span>
                </i>
                <i class="bk-icon icon-delete"
                  @click="delectProject(item)">
                  <span>
                    {{ $t('删除项目') }}
                  </span>
                </i>
              </div>
            </div>
            <div v-else
              class="contents-item-bottom">
              <div class="op-left">
                <i v-tooltip="$t('恢复')"
                  class="bk-icon icon-back2"
                  @click="recoveryProject(item)" />
                <i v-tooltip="$t('此功能将在后续版本开放')"
                  class="bk-icon icon-delete button-disable" />
              </div>
              <div class="op-right">
                <span>{{ $t('删除时间') }}: {{ item.deleted_at }}</span>
              </div>
            </div>
          </div>
        </div>
      </bkdataVirtueList>
      <!-- 添加成员start -->

      <MemberManagerWindow ref="member"
        :isOpen.sync="memberManager.isOpen"
        :objectClass="memberManager.objectClass"
        :scopeId="memberManager.scopeId"
        :roleIds="memberManager.roleIds" />

      <!-- 申请业务数据start -->
      <div v-if="isApplyBizShow"
        class="apply-biz-data">
        <apply-biz ref="applyBiz"
          @closeApplyBiz="isApplyBizShow = false" />
      </div>
      <!-- 申请业务数据end -->

      <!-- 项目资源组申请 -->
      <ResourceApply ref="resourceGroupApply" />

      <!-- 项目详情start -->
      <div class="project-detail">
        <bkdata-dialog v-model="projectDetail.isShow"
          :loading="projectEditLoading"
          extCls="bkdata-dialog"
          width="890"
          :padding="0"
          :okText="$t('保存')"
          :cancelText="$t('取消')"
          @confirm="confirmProject"
          @cancel="closeDialog">
          <div>
            <v-editPop ref="edit"
              :showClose="false"
              :editData="projectDetail.projectBasics"
              @close-pop="closeDialog('edit')" />
          </div>
        </bkdata-dialog>
      </div>
      <!-- 项目详情end -->
    </div>
  </div>
</template>
<script>
import bkdataVirtueList from '@/components/VirtueScrollList/bkdata-virtual-list.vue';
import { postMethodWarning } from '@/common/js/util.js';
import { MemberManagerWindow } from '@/pages/authCenter/parts/index';
import applyBiz from './applyBizData';
import ResourceApply from '../ResourceManage/components/ProjectApplyRes.vue';
import vEditPop from '@/components/pop/dataFlow/editPop';
import projectBar from './projectOperationBar';
import Bus from '@/common/js/bus.js';

export default {
  components: {
    bkdataVirtueList,
    MemberManagerWindow,
    applyBiz,
    ResourceApply,
    vEditPop,
    projectBar
  },
  props: {
    activeName: {
      type: String,
      default: ''
    },
    activeType: {
      type: String,
      default: ''
    },
    projectItems: {
      type: Array,
      default: () => []
    },
    contentsLoading: {
      type: Boolean,
      default: false
    }
  },
  data() {
    return {
      groupItemCount: 1,
      memberManager: {
        isOpen: false,
        objectClass: '',
        scopeId: '',
        roleIds: [],
      },
      isApplyBizShow: false,
      projectEditLoading: false,
      renderProjectList: [], // 用于渲染的项目列表
      projectLists: [], // 所有的项目列表
      filterProjectLists: [], // 过滤后的列表
      sortField: 'createTime',
      projectDetail: {
        isShow: false,
        data: {},
        projectBasics: {},
      },
      viewMember: {
        error: false,
        isShow: false,
        data: {},
        projectMember: [],
        hasAuth: false, // 是否有修改权限
      },
      resultProjectList: []
    };
  },
  computed: {
    /** 是否被iframe 嵌入 */
    isFlowIframe() {
      return this.$route.query.flowIframe;
    },
    isModeflowRedirect() {
      return window.BKBASE_Global.modelFlowUrl;
    },
  },
  watch: {
    projectItems() {
      this.initProjectLists();
    }
  },
  mounted() {
    this.init();
    this.initProjectLists();
  },
  methods: {
    init() {
      const width = this.$el.scrollWidth;
      let count = 5;
      if (1470 < width && width < 1810) {
        count = 4;
      }

      if (width <= 1470) {
        count = 3;
      }

      this.groupItemCount = count;
    },
    linkToFlow(item) {
      if (item.tags && item.tags.manage.lol_billing) {
        this.$router.push({
          name: 'lol',
          query: {
            project_id: item.project_id,
            project_name: item.project_name,
          },
        });
      } else {
        this.$router.push({
          name: 'dataflow_ide',
          query: {
            project_id: item.project_id,
          },
        });
      }
    },
    /** @description
       *  项目类型选择
       */
    projectTypeChange(type) {
      this.contendsLoading = true;
      this.$emit('project-type-changed', type);
      this.$refs.projectbar.reset();
    },
    /** @description
       * 搜索查询
       */
    searchProject(data) {
      this.searchContent = data.trim();
      const reg = new RegExp(this.searchContent, 'i');
      this.resultProjectList = this.renderProjectList.filter(
        item => !item.deleting && (reg.test(item.project_name) || reg.test(item.project_id))
      );
    },
    linkToDf(item) {
      this.$emit('link-to-df', item);
    },
    toDataExplore(projectId) {
      this.$router.push({
        path: '/datalab',
        query: {
          type: 'common',
          projectId: projectId,
          notebook: 0,
        },
      });
    },

    /** @description
       *  创建新任务
       */
    showCreateNewDataflowWindow(item) {
      Bus.$emit('showCreateNewTaskWindow', item);
    },
    openResourceGroupApply(project) {
      this.$refs.resourceGroupApply.setProject(project);
    },
    openModeFlow(item) {
      window.location.href = `${window.BKBASE_Global.modelFlowUrl}/#/?project_id=${item.project_id}`;
    },
    /** @description
       * 申请敏感数据
       */
    applyBiz(project) {
      this.isApplyBizShow = true;
      this.$nextTick(() => {
        this.$refs.applyBiz.show(project);
      });
    },
    /** @description
       *  恢复项目
       */
    recoveryProject(item) {
      let self = this;
      this.$bkInfo({
        title: this.$t('请确认是否恢复项目') + item.project_name,
        theme: 'primary',
        confirm: this.$t('恢复'),
        confirmFn: async function () {
          await self.bkRequest
            .httpRequest('meta/restoreProject', {
              params: {
                project_id: item.project_id,
              },
            })
            .then(res => {
              if (res.result) {
                postMethodWarning(self.$t('成功'), 'success', { delay: 1500 });
                self.renderProjectList = self.renderProjectList.filter(p => {
                  return p.project_id !== item.project_id;
                });
              } else {
                postMethodWarning(res.message, 'error');
              }
            });
        },
      });
    },

    /** @description
       * 删除项目
       */
    delectProject(item, index) {
      let self = this;
      let flowCount = item.normal_count + item.no_start_count + item.exception_count;
      this.$bkInfo({
        title: this.$t('请确认是否删除项目'),
        subTitle:
            flowCount === 0
              ? ''
              : '(' + item.project_name + ') ' + this.$t('项目中还有任务在运行_是否强制停止所有运行任务并删除该项目_'),
        theme: 'danger',
        okText: this.$t('删除'),
        confirmFn: async function () {
          await self.bkRequest
            .httpRequest('meta/deleteProject', {
              params: {
                project_id: item.project_id,
              },
            })
            .then(res => {
              if (res.result) {
                postMethodWarning(self.$t('删除成功'), 'success', { delay: 1500 });
                self.renderProjectList = self.renderProjectList.filter(p => {
                  return p.project_id !== item.project_id;
                });
              } else {
                postMethodWarning(res.message, 'error');
              }
            });
        },
      });
    },
    openMemberManager(item) {
      this.memberManager.isOpen = true;
      this.memberManager.objectClass = 'project';
      this.memberManager.scopeId = item.project_id;
      this.memberManager.roleIds = ['project.manager', 'project.flow_member'];
    },
    /** @description 项目详情
       *
       */
    projectDetails(item) {
      this.projectDetail.isShow = true;
      this.projectDetail.data = item;
      this.getProjectBasics();
    },
    getProjectBasics() {
      this.projectDetailLoading = true;

      this.bkRequest
        .httpRequest('meta/modifyProjectInfo', {
          params: {
            project_id: this.projectDetail.data.project_id,
          },
        })
        .then(res => {
          if (res.result) {
            this.projectDetail.projectBasics = res.data;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        .then(() => {
          this.$refs.edit.init();
          this.projectDetailLoading = false;
        });
    },
    closeDialog() {
      this.viewMember.isShow = false;
      this.viewMember.error = false;
      this.projectDetail.isShow = false;
      this.$refs.edit.reset();
      // this.$refs.viewMember.reset()
    },

    confirmProject() {
      let params = this.$refs.edit.getOptions();

      this.projectEditLoading = true;
      if (
        !params.data.project_name
          || !params.data.description
          || (params.enableTdw && !params.data.tdw_app_groups.length)
      ) {
        setTimeout(() => {
          this.projectEditLoading = false;
        }, 0);
        return false;
      }
      this.bkRequest
        .httpRequest('meta/modifyProjectInfoPut', {
          params: {
            project_id: this.projectDetail.data.project_id,
            project_name: params.data.project_name,
            description: params.data.description,
            tdw_app_groups: params.enableTdw ? [...params.data.tdw_app_groups] : [],
          },
        })
        .then(res => {
          if (res.result) {
            postMethodWarning(this.$t('成功'), 'success', { delay: 1500 });
            this.$emit('project-created');
            Bus.$emit('closeEditPop');
          } else {
            postMethodWarning(res.message, 'error');
          }
        })
        ['finally'](() => {
          this.projectDetail.isShow = false;
        });
    },

    /** @description
       * 排序
       */
    projectSort(type, list=null) {
      let attr;
      let num;
      if (type !== this.sortField) {
        this.sortField = type;
      }
      if (type === 'createTime') {
        attr = 'created_at';
        num = -1;
      } else {
        attr = 'project_name';
        num = 1;
      }

      if (!Array.isArray(list)) {
        list = this.renderProjectList;
      }
      list.sort(this.compare(attr, num));
      return list;
    },
    updateProjectList(item) {
      this.renderProjectList.unshift(item);
      this.projectLists = this.renderProjectList;
    },
    searchProjectList() {
      this.$refs.projectbar && this.$refs.projectbar.searchProject();
    },
    initProjectLists() {
      this.projectLists = JSON.parse(JSON.stringify(this.projectItems));
      this.renderProjectList = this.projectSort(this.sortField, JSON.parse(JSON.stringify(this.projectItems)));
      this.searchProjectList();
    }
  }
};
</script>
<style lang="scss" scoped>
  .contents-list-container {
    margin-right: 1px;
    ::v-deep .bkdata-virtue-content {
      display: flex;
      flex-wrap: wrap;
      align-content: flex-start;
    }

    &::-webkit-scrollbar {
      width: 6px;
    }

    &::-webkit-scrollbar-thumb {
      background-color: #c4c6cc;
    }
  }

      .contents {
      padding-top: 20px;
      min-height: 200px;
      overflow: hidden;
      height: calc(100vh - 180px);

      @media screen and (max-width: 1470px) {
        .contents-list {
          width: 33.333%;
        }
      }
      @media screen and (min-width: 1470px) and (max-width: 1810px) {
        .contents-list {
          width: 25%;
        }
      }
      @media screen and (min-width: 1810px) {
        .contents-list {
          width: 20%;
        }
      }
      .contents-list {
        display: flex;
        margin-bottom: 30px;
        justify-content: center;
        height: 218px;
      }
      .contents-item {
        height: 218px;
        width: calc(100% - 20px);
        border-radius: 4px;
        box-shadow: 0px 2px 5px rgba(56, 64, 201, 0.09);
        &:hover {
          box-shadow: 0px 5px 9px rgba(0, 0, 0, 0.32);
          .contents-item-top {
            border-color: #3a84ff;
            cursor: pointer;
            background: #3a84ff;
            .title {
              color: #fff;
            }
            .time {
              color: #fff;
            }
            .title-icon {
              background: #e1ecff;
              color: #3a84ff;
            }
            .title-edit {
              cursor: pointer;
              display: flex;
              line-height: 26px;
            }
          }
        }
        &-top {
          text-align: left;
          /* height: 70px; */
          padding: 18px 20px;
          line-height: 1;
          border: 1px solid #ddd;
          /* padding: 17px 17px 0px 17px; */
          background: #e9eaec;
          border-radius: 4px 4px 0 0;
          .title {
            font-size: 16px;
            line-height: 30px;
            color: #212232;
            font-weight: bold;
            display: flex;
            align-items: center;
            justify-content: space-between;

            .title-left {
              display: flex;
              align-items: center;
              max-width: calc(100% - 60px);

              .title-text {
                display: inline-block;
                overflow: hidden;
                text-overflow: ellipsis;
                white-space: nowrap;
              }
            }

            .title-icon {
              float: right;
              width: 30px;
              height: 30px;
              line-height: 30px;
              text-align: center;
              border-radius: 50%;
              background: #d8dadd;
              color: #ffffff;
            }
            .title-edit {
              margin-left: 8px;
              font-size: 12px;
              display: none;
            }
          }
          .time {
            padding-left: 27px;
            font-weight: normal;
            font-size: 12px;
            color: #737987;
            line-height: 26px;
          }
        }
        &-deleting {
          .title {
            color: #787882;
          }
          .time {
            color: #abafb7;
          }
        }
        &-mid {
          height: 100px;
          border: 1px solid #ddd;
          border-top: none;
          padding: 17px 0;
          > div {
            box-sizing: border-box;
            padding: 7px 20px;
            &:hover {
              background: #f5f5f5;
            }
            > span {
              border-right: 1px solid #eaeaea;
            }
          }
          .dataflows {
            display: flex;
            font-size: 12px;
            & .title {
              width: 40px;
              padding: 0 10px 0 0;
            }
            & .dataflows-status {
              float: left;
              > span {
                display: inline-block;
                text-align: right;
                padding: 0 5px 0 0;
                border-right: 1px solid #eaeaea;
              }
            }
            .dataflows-status-query {
              .title {
                border-right: 1px solid #eaeaea;
              }
            }
            &-status {
              width: 75%;
              display: flex;
              .status-item-wrapper {
                width: 33%;
                padding: 0 2px;
                text-align: left;
                white-space: nowrap;
                /* margin-right: 10px; */
                &:last-of-type {
                  margin-right: 0;
                }
                span {
                  display: inline-block;
                  text-align: left;
                  &.status-title {
                    min-width: calc(50% - 2px);
                    text-align: right;
                  }
                }
              }
              .normal {
                color: #95bb70;
              }
              .warning {
                color: #ff6600;
              }
              .no-start {
                color: #737987;
              }
            }
            .details {
              width: 15%;
              /* border-left: 1px solid #eaeaea; */
              display: inline-block;
              text-align: right;
              &:last-of-type {
                border-right: none;
                padding-left: 5px;
                color: #3a84ff;
                cursor: pointer;
                &:hover {
                  color: #3e416d;
                }
              }
            }
          }
        }
        &-mid-deleting {
          color: #abafb7;
          span.normal,
          .warning,
          .no-start {
            color: #abafb7 !important;
          }
        }
        &-bottom {
          height: 50px;
          border: 1px solid #ddd;
          border-radius: 0 0 4px 4px;
          border-top: none;
          padding-right: 10px;
          display: flex;
          justify-content: space-between;
          .op-left {
            position: relative;
            border-right: 1px solid #eaeaea;
            padding: 13px 20px 0px 14px;
            font-size: 18px;
            display: flex;
            .button-disable {
              color: #babddd;
              cursor: auto;
            }
            .item-cloud-area {
              position: absolute;
              top: 13px;
              left: 110%;
              display: inline-block;
              padding: 5px 6px;
              max-width: 100px;
              height: 28px;
              line-height: 18px;
              border-radius: 4px;
              font-size: 12px;
              white-space: nowrap;
              overflow: hidden;
              text-overflow: ellipsis;
              background: #e7e7e7;
              cursor: default;
            }
            i {
              padding: 5px 6px;
              position: relative;
              cursor: pointer;
              top: -2px;
              display: block;
              width: 29px;
              height: 28px;
              border-radius: 2px;
              span {
                display: none;
                background: #000;
                border-radius: 2px;
                height: 32px;
                line-height: 32px;
                padding: 0 10px;
                font-size: 12px;
                position: absolute;
                min-width: 68px;
                white-space: nowrap;
                top: calc(-100% - 10px);
                left: 50%;
                transform: translate(-50%, 0%);
                &::after {
                  position: absolute;
                  top: 100%;
                  left: 50%;
                  content: '';
                  width: 0;
                  height: 0px;
                  border: 6px solid;
                  border-color: #000 transparent transparent transparent;
                  -webkit-transform: translate(-50%, 0);
                  transform: translate(-50%, 0);
                }
              }
              &:hover {
                background: #3a84ff;
                color: #fff;
              }
              &:hover span {
                display: block;
              }
            }
            span {
              font-size: 15px;
            }
          }
          .op-left-delete {
            &:hover {
              cursor: pointer;
              background: #3a84ff;
              i {
                color: #fff;
                top: -2px;
              }
              span {
                color: #fff;
              }
            }
          }
          .op-right {
            padding-top: 13px;
            font-size: 18px;
            display: flex;
            i {
              padding: 5px 6px;
              margin: 0px 1px;
              position: relative;
              cursor: pointer;
              border-radius: 2px;
              display: block;
              width: 29px;
              height: 28px;
              span {
                display: none;
                background: #000;
                border-radius: 2px;
                height: 32px;
                line-height: 32px;
                padding: 0 10px;
                font-size: 12px;
                position: absolute;
                min-width: 68px;
                white-space: nowrap;
                top: calc(-100% - 10px);
                left: 50%;
                transform: translate(-50%, 0%);
                &::after {
                  position: absolute;
                  top: 100%;
                  left: 50%;
                  content: '';
                  width: 0;
                  height: 0px;
                  border: 6px solid;
                  border-color: #000 transparent transparent transparent;
                  -webkit-transform: translate(-50%, 0);
                  transform: translate(-50%, 0);
                }
              }
              &:hover {
                background: #3a84ff;
                color: #fff;
              }
              &:hover span {
                display: block;
              }
            }
            span {
              font-size: 15px;
            }
          }
        }
        &-bottom-deleting {
          color: #abafb7;
          span {
            color: #abafb7 !important;
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
