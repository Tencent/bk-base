

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
  <div class="graph-header-wrapper">
    <div class="sub-header">
      <flow-title />
      <flow-toolbar :showDetail.sync="showFlowDetail"
        :graphLoading="graphLoading" />
    </div>
    <!-- 弹窗 -->
    <flow-detail ref="flowDetail"
      :showDetail.sync="showFlowDetail"
      @openProjectSetting="showProjectDetails" />

    <!-- 项目详情start -->
    <div v-bkloading="{ isLoading: projectDetailLoading }"
      class="project-detail">
      <bkdata-dialog
        v-model="projectDetail.isShow"
        extCls="bkdata-dialog"
        class="zIndex2501"
        width="890"
        :padding="0"
        :okText="$t('保存')"
        :cancelText="$t('取消')"
        :hasHeader="false"
        :closeIcon="false"
        @confirm="confirmProject"
        @cancel="closeDialog">
        <div>
          <v-editPop ref="edit"
            :editData="projectDetail.projectBasics"
            @close-pop="closeDialog('edit')" />
        </div>
      </bkdata-dialog>
    </div>
    <!-- 项目详情end -->
  </div>
</template>

<script>
import FlowTitle from './flowTitle';
import FlowToolbar from './flowToolbar/index';
import FlowDetail from '../Dialog/flowDetail';
import vEditPop from '@/components/pop/dataFlow/editPop';
import Bus from '@/common/js/bus.js';
import { mapState } from 'vuex';
import { postMethodWarning } from '@/common/js/util.js';
export default {
  name: 'flow-header',
  components: {
    FlowTitle,
    FlowToolbar,
    FlowDetail,
    vEditPop,
  },
  props: {
    // eslint-disable-next-line vue/prop-name-casing
    project_id: {
      type: [String, Number],
      default: '',
    },
    graphLoading: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      showFlowDetail: false,
      projectDetail: {
        isShow: false,
        projectBasics: {},
      },
      projectDetailLoading: false,
    };
  },
  methods: {
    confirmProject() {
      let params = this.$refs.edit.getOptions();
      if (!params.data.tdw_app_groups.length) {
        return false;
      }
      this.axios
        .put('v3/meta/projects/' + this.project_id + '/', {
          project_name: params.data.project_name,
          description: params.data.description,
          tdw_app_groups: [...params.data.tdw_app_groups],
        })
        .then(res => {
          if (res.result) {
            this.bkRequest
              .httpRequest('dataFlow/getProjectList')
              .then(res => {
                if (res.result) {
                  this.$store.dispatch(
                    'updateProjectList',
                    res.data.filter(d => d.active)
                  ); // 任务修改需要用到
                } else {
                  this.getMethodWarning(res.message, res.code);
                }
              })
              ['finally'](_ => {
                postMethodWarning(this.$t('成功'), 'success');
                Bus.$emit('closeEditPop');
              });
          } else {
            postMethodWarning(res.message, 'error');
          }
        })
        ['finally'](() => {
          this.projectDetail.isShow = false;
        });
    },
    showProjectDetails() {
      this.projectDetail.isShow = true;
      this.$refs.flowDetail.close();
      this.getProjectBasics();
    },
    getProjectBasics() {
      this.projectDetailLoading = true;
      this.axios
        .get('v3/meta/projects/' + this.project_id + '/')
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
      this.projectDetail.isShow = false;
      this.showFlowDetail = true;
      this.$refs.edit.reset();
    },
  },
};
</script>
<style lang="scss">
.graph-header-wrapper {
  height: 100%;
  .sub-header {
    height: 100%;
    padding-left: 20px;
    user-select: none;
    color: #737987;
    display: flex;
    justify-content: space-between;
    position: relative;
    background-color: #efefef;
    box-sizing: border-box;
    user-select: text;
    .right-content {
      display: flex;
      align-items: center;
      .unit {
        float: left;
        position: absolute;
        border-radius: 2px;
        left: -150px;
        p {
          color: #737987;
          span {
            font-weight: bold;
          }
        }
      }
    }
    .editInput {
      display: inline-block;
      vertical-align: middle;
    }
    .dataflow-name {
      color: #212232;
    }
  }
}
</style>
