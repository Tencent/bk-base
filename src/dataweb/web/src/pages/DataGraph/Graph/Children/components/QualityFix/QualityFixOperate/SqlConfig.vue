

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
  <div class="height450">
    <Monaco
      :code="sql"
      :options="{
        readOnly: true,
      }"
      :tools="{
        guidUrl: $store.getters['docs/getPaths'].querySqlRule,
        toolList: {
          view_data: true,
          full_screen: true,
          event_fullscreen_default: true,
        },
      }" />
  </div>
</template>

<script lang="ts">
import Monaco from '@/components/monaco/index.vue';
import { Component, Vue, Watch, Prop } from 'vue-property-decorator';
import { getQualityFixSql } from '@/pages/datamart/Api/DataQuality';
import { CancelToken } from '@/common/js/ajax';

@Component({
  components: {
    Monaco,
  },
})
export default class SqlConfig extends Vue {
  sql = '';

  timer: any = null;

  isLoading = false;

  /** API请求取消缓存Token */
  requestCancelToken = {};

  @Watch('$attrs.correct_configs', { immediate: true, deep: true })
  handleModelIdChanged(val: boolean) {
    if (this.timer) clearTimeout(this.timer);
    this.timer = setTimeout(() => {
      this.getQualityFixSql();
    }, 500);
  }

  mounted() {
    this.getQualityFixSql();
  }

  getQualityFixSql() {
    if (this.isLoading) {
      this.requestCancelToken.getQualityFixSql && this.requestCancelToken.getQualityFixSql();
    }
    this.isLoading = true;
    if (!this.$attrs.sourceSql) return;
    getQualityFixSql(
      this.$attrs.dataSetId,
      this.$attrs.sourceSql,
      this.$attrs.correct_configs,
      this.getCancelToken('getQualityFixSql')
    ).then(res => {
      if (res.result && res.data) {
        this.sql = res.data;
      }
    });
    this.isLoading = false;
  }

  /**
   * 获取API请求取消Token
   * @param key 唯一标识
   * @return { cancelToken }
   */
  getCancelToken(key: string) {
    const self = this;
    return {
      cancelToken: new CancelToken(function executor(c: any) {
        self.requestCancelToken[key] = c;
      }),
    };
  }
}
</script>

<style lang="scss" scoped>
.height450 {
  height: 450px;
}
</style>
