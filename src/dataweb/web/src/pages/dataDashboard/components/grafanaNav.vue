

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
  <div class="subnav-tabs">
    <div class="subnav-tab"
      :class="{ active: activeName === 'GrafanaIndex' }"
      @click="jump('GrafanaIndex')">
      {{ $t('首页') }}
    </div>
    <div class="subnav-tab"
      :class="{ active: activeName === 'GrafanaManage' }"
      @click="jump('GrafanaManage')">
      {{ $t('管理仪表板') }}
    </div>
    <div class="nav-project-select fr">
      <div class="selector-tip">
        {{ $t('切换项目: ') }}
      </div>
      <div class="project-selector">
        <bkdata-selector
          :selected.sync="projectId"
          :filterable="true"
          :searchable="true"
          :placeholder="projectHoder"
          :list="projectList"
          :settingKey="'project_id'"
          :displayKey="'project_alias'"
          :allowClear="false"
          searchKey="project_alias" />
      </div>
      <div class="create-dashboard-button">
        <bkdata-button theme="primary"
          class="ml10"
          @click="jump('GrafanaNewDashboard')">
          <i :title="$t('创建仪表板')"
            class="icon-add_dataflow mr5" />
          {{ $t('创建仪表板') }}
        </bkdata-button>
      </div>
      <div class="create-folder-button">
        <bkdata-button theme="primary"
          class="ml10"
          @click="jump('GrafanaNewFolder')">
          <i :title="$t('创建文件夹')"
            class="icon-add_dataflow mr5" />
          {{ $t('创建文件夹') }}
        </bkdata-button>
      </div>
    </div>
  </div>
</template>

<script>
import Cookies from 'js-cookie';

export default {
  props: {
    activeName: {
      type: String,
      default: 'GrafanaIndex',
    },
  },
  data() {
    return {
      projectId: '',
      projectList: [],
      projectHoder: '',
      projectLoading: false,
    };
  },
  watch: {
    projectId: {
      handler(projectId) {
        this.jump('GrafanaIndex');
        Cookies.set('grafana_project', projectId, { expires: 30 });
      },
    },
  },
  async mounted() {
    this.getAlertProjectList();
    this.projectId = parseInt(Cookies.get('grafana_project'));
  },
  methods: {
    jump(routeName) {
      this.$emit('grafanaChangeTab', routeName, this.projectId);
    },

    getAlertProjectList() {
      this.projectHoder = this.$t('数据加载中');
      this.projectLoading = true;

      this.bkRequest
        .httpRequest('dataFlow/getProjectList', {
          query: {
            active: true,
          },
        })
        .then(res => {
          if (res.result) {
            this.projectList = res.data.filter(project => {
              if (project.project_id === undefined) {
                return false;
              }
              return true;
            });
            this.projectList.map(project => {
              project.project_id = Number(project.project_id);
              project.project_alias = `[${project.project_id}]${project.project_name}`;
              return project;
            });
            if (!this.projectId && this.projectList.length > 0) {
              this.projectId = this.projectList[0].project_id;
            }
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.projectHoder = this.$t('请选择项目');
          this.projectLoading = false;
        });
    },
  },
};
</script>

<style lang="scss" scoped>
@import '~@/common/scss/conf.scss';

.subnav-tabs {
  .subnav-tab {
    padding: 0 20px;
    height: 40px;
    -webkit-box-sizing: border-box;
    box-sizing: border-box;
    line-height: 40px;
    display: inline-block;
    list-style: none;
    font-size: 14px;
    font-weight: 500;
    color: #303133;
    position: relative;

    &:last-child {
      padding-right: 0px;
    }

    &:first-child {
      padding-left: 0px;
    }

    &:hover {
      cursor: pointer;
      color: $primaryColor;
    }
    &.active {
      color: $primaryColor;
    }

    &.active:after {
      content: '';
      position: absolute;
      bottom: 0;
      left: 20px;
      width: calc(100% - 40px);
      height: 2px;
      background-color: $primaryColor;
      z-index: 1;
      -webkit-transition: -webkit-transform 0.3s cubic-bezier(0.645, 0.045, 0.355, 1);
      transition: -webkit-transform 0.3s cubic-bezier(0.645, 0.045, 0.355, 1);
      transition: transform 0.3s cubic-bezier(0.645, 0.045, 0.355, 1);
      transition: transform 0.3s cubic-bezier(0.645, 0.045, 0.355, 1),
        -webkit-transform 0.3s cubic-bezier(0.645, 0.045, 0.355, 1);
      list-style: none;
    }
    &.active:last-child:after,
    &.active:first-child:after {
      width: calc(100% - 20px);
    }
    &.active:first-child:after {
      left: 0px;
    }

    .count {
      position: absolute;
      left: 100%;
      top: 2px;
      min-width: 16px;
      height: 16px;
      font-size: 12px;
      color: #fff;
      line-height: 16px;
      text-align: center;
      background: #ff5656;
      border-radius: 8px;
      padding: 0 3px;
    }
  }
  .nav-project-select {
    width: 540px;
    display: flex;
    flex-direction: row;
    justify-content: center;
    align-items: center;
    .selector-tip {
      width: 80px;
    }
    .project-selector {
      width: 240px;
      display: flex;
    }
    .create-dashboard-button {
      display: flex;
    }
    .create-folder-button {
      display: flex;
    }
  }
}
</style>
