

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
  <div v-bkloading="{ isLoading: loading }"
    :class="['notebook-diff-layout', { loading: loading }]">
    <div v-if="isError"
      class="notebook-diff-error">
      <img src="../../../common/images/diff-error.svg"
        alt="">
      <p>
        {{ $t('请求失败') }}<span @click="$parent.$parent.handleGetDiffAgain">{{ $t('重试') }}</span>
      </p>
    </div>
    <template v-else>
      <div class="version-info">
        <div class="info-item">
          <span class="name">{{ $t('当前版本') }}</span>
          <span class="time">（{{ $t('最后更新') }}{{ curTime }}）</span>
        </div>
        <div class="info-item">
          <span class="name">{{ diffVersion.name }}</span>
          <span class="time">（{{ $t('最后更新') }}{{ diffVersion.time }}）</span>
        </div>
      </div>
      <bk-diff :oldContent="localOriginal"
        :newContent="localModified"
        :format="'side-by-side'" />
    </template>
  </div>
</template>

<script>
// import * as monaco from 'monaco-editor'
// import 'plugins/monaco/index.js'
import { bkDiff } from 'bk-magic-vue';
export default {
  components: {
    bkDiff,
  },
  props: {
    dataModel: {
      type: Object,
      default: () => {},
    },
    diffVersion: {
      type: Object,
      default: () => {},
    },
    curTime: {
      type: String,
      default: '',
    },
    loading: {
      type: Boolean,
      default: false,
    },
    isError: {
      type: Boolean,
      default: false,
    },
  },
  computed: {
    localOriginal() {
      const original = this.dataModel.original;
      let res = '';
      if (!original) return '';
      // 过滤py文件自带注释
      res = original.replace(/# ---[\s\S]*?# ---\n\n/, '');
      return res;
    },
    localModified() {
      const modified = this.dataModel.modified;
      let res = '';
      if (!modified) return '';
      // 过滤py文件自带注释
      res = modified.replace(/# ---[\s\S]*?# ---\n\n/, '');
      return res;
    },
  },
};
</script>
<style lang="scss" scoped>
.notebook-diff-layout {
  padding: 27px 26px 24px;
  &.loading {
    .version-info,
    .diff-container,
    .notebook-diff-error {
      opacity: 0;
    }
  }
  .notebook-diff-error {
    text-align: center;
    font-size: 12px;
    color: #979ba5;
    padding: 100px 0;
    img {
      width: 102px;
    }
    span {
      color: #3a84ff;
      margin-left: 5px;
      cursor: pointer;
    }
  }
  .version-info {
    display: flex;
    line-height: 42px;
    text-align: center;
    background-color: #f0f1f5;
    .info-item {
      flex: 1;
      font-size: 14px;
      color: #313238;
      .time {
        color: #63656e;
        font-size: 12px;
      }
    }
  }
  .diff-container {
    height: 500px;
    border-top: 1px solid #cfd3e6;
  }
}
</style>

<style lang="scss">
#diffContainer {
  span,
  div {
    font-family: 'Menlo,Monaco,Consolas,Courier,monospace' !important;
  }
  .monaco-editor .line-delete,
  .monaco-editor .char-delete {
    background-color: #fbe9eb;
  }
  .monaco-diff-editor .line-insert,
  .monaco-diff-editor .char-insert {
    background-color: #ecfdf0;
  }
  .monaco-editor .margin-view-overlays .line-numbers {
    min-width: 40px;
    text-align: center;
    background-color: #f7f8fc;
    color: #c4c6cc;
  }
  .monaco-editor .view-line {
    padding-left: 16px;
  }
}
</style>
