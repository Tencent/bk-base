

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
  <div class="condition-select">
    <div class="condition-item">
      <bkdata-selector
        class="platform-select"
        searchable
        :allowClear="true"
        :isLoading="loading.isPlatformLoading"
        :selected.sync="value.channelKeyWord"
        :displayKey="$i18n.locale === 'en' ? 'platform_name' : 'platform_alias'"
        :placeholder="$t('数据所在系统')"
        :list="channelList"
        :settingKey="'platform_name'"
        searchKey="platform_alias" />
    </div>
    <div class="condition-item biz-item">
      <bkdata-selector class="map-select"
        searchable
        :allowClear="true"
        :selected.sync="value.biz_id"
        :displayKey="'bk_biz_name'"
        :placeholder="$t('按业务筛选')"
        :list="bizList"
        :settingKey="'bk_biz_id'"
        searchKey="bk_biz_name"
        @clear="handleClear('biz_id')" />
    </div>
    <div class="condition-item">
      <bkdata-selector
        class="map-select"
        :disabled="value.channelKeyWord === 'tdw' || isDisabledProj"
        searchable
        :allowClear="true"
        :selected.sync="value.project_id"
        :isLoading="loading.projectLoading"
        :displayKey="'displayName'"
        :placeholder="projPlacehoder"
        :list="projectList"
        :settingKey="'project_id'"
        searchKey="displayName"
        @clear="handleClear('project_id')" />
    </div>
  </div>
</template>

<script>
import { confirmMsg, postMethodWarning } from '@/common/js/util';
export default {
  props: {
    value: {
      type: Object,
      default: () => {
        return {
          project_id: '',
          biz_id: '',
          channelKeyWord: 'bk_data',
        };
      },
    },
    isDisabledProj: {
      type: Boolean,
      default: false,
    },
    projPlacehoder: {
      type: String,
      default: $t('按项目筛选'),
    },
    isTdwDisabled: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      projectList: [],
      loading: {
        projectLoading: false,
        isPlatformLoading: false,
      },
      channelList: [],
    };
  },
  computed: {
    bizList() {
      return this.$store.state.global.allBizList;
    },
  },
  watch: {
    isTdwDisabled(val) {
      this.channelList.length
        && this.$set(
          this.channelList.find(item => item.platform_name === 'tdw'),
          'disabled',
          val
        );
    },
  },
  mounted() {
    this.init();
  },
  methods: {
    handleClear(key) {
      // eslint-disable-next-line vue/no-mutating-props
      this.value[key] = '';
    },
    init() {
      this.getDataDictionaryPlatformList();
      this.getAllProjectList();
    },
    getDataDictionaryPlatformList() {
      this.loading.isPlatformLoading = true;
      this.bkRequest
        .httpRequest('dataDict/getDataDictionaryPlatformList')
        .then(res => {
          if (res.result) {
            this.channelList = res.data;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](_ => {
          this.loading.isPlatformLoading = false;
        });
    },
    // 获取项目列表
    getAllProjectList() {
      this.loading.projectLoading = true;
      this.axios.get(`${window.BKBASE_Global.siteUrl}projects/all_projects/`).then(response => {
        if (response.result) {
          this.projectList = response.data;
          this.projectList.forEach(item => {
            item.displayName = `[${item.project_id}]${item.project_name}`;
          });
        } else {
          postMethodWarning(response.message, 'error');
        }
        this.loading.projectLoading = false;
      });
    },
  },
};
</script>

<style lang="scss" scoped>
.condition-select {
  display: flex;
  flex-wrap: nowrap;
  width: 539px;
  .condition-item {
    margin-left: 15px;
    width: 152px;
  }
  .biz-item {
    width: 190px;
  }
}
</style>
