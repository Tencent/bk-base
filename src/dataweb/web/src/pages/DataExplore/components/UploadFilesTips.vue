

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
  <div class="upload-tips">
    <p class="text-tip-block">
      <span class="text">{{ $t('只支持Excel') }}</span>
      <a href="https://tools.ietf.org/html/rfc4180"
        target="_blank"
        class="link-text">
        RFC 4180
      </a>
      <span class="text">{{ $t('文件小于等于1') }}</span>
    </p>
    <p class="template-block">
      <a
        :href="fileTemplateUrl.excel"
        class="mr10"
        :download="$i18n.locale === 'zh-cn' ? 'example-cn.xlsx' : 'example-us.xlsx'">
        {{ $t('excel模板') }}
      </a>
      <a :href="fileTemplateUrl.csv"
        :download="$i18n.locale === 'zh-cn' ? 'example-cn.csv' : 'example-us.csv'">
        {{ $t('csv模板') }}
      </a>
    </p>
  </div>
</template>

<script>
export default {
  computed: {
    fileTemplateUrl() {
      let lang = '';
      if (this.$i18n.locale === 'zh-cn') {
        lang = '-cn';
      } else {
        lang = '-us';
      }
      return {
        excel: window.staticUrl + `assets/templatefile/example${lang}.xlsx`,
        csv: window.staticUrl + `assets/templatefile/example${lang}.csv`,
      };
    },
  },
};
</script>
<style lang="scss" scoped>
.upload-tips {
  display: flex;
  justify-content: space-between;
  flex: 1;
  font-size: 14px;
  .link-text {
    color: #3a84ff;
    cursor: pointer;
  }
}
</style>
