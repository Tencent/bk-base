

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
  <div class="build-in-library-wrapper">
    <div class="filters">
      <bkdata-input v-model="searchKey"
        :placeholder="$t('请输入库名')"
        :rightIcon="'bk-icon icon-search'" />
    </div>
    <div class="content-header">
      {{ $t('支持库') }}
    </div>
    <div class="notebook-content bk-scroll-y">
      <div v-for="(item, index) in packages"
        :key="index"
        class="content-item">
        <a v-bk-tooltips="{ content: item.version, placement: 'top' }"
          class="title"
          :href="item.link"
          target="_blank">
          {{ item.name }}：
        </a>
        <span class="desp">{{ item.desp }}</span>
      </div>
    </div>
  </div>
</template>

<script>
import { STATUS_CODES } from 'http';
export default {
  props: {
    languageList: {
      type: Array,
      default: () => [],
    },
    selectedLanguage: {
      type: String,
      default: '',
    },
  },
  data() {
    return {
      searchKey: '',
      libsOrder: [],
    };
  },
  computed: {
    packages() {
      const packages = [];
      const languageType = this.languageList.find(item => item.name === this.selectedLanguage);
      if (languageType && languageType.content) {
        languageType.order.forEach(key => {
          packages.push({
            name: key,
            desp: languageType.content[key].description,
            version: 'version:' + languageType.content[key].version,
            link: languageType.content[key].doc,
          });
        });
      }
      if (this.searchKey) {
        const key = this.searchKey.toLowerCase();
        return packages.filter(item => item.name.toLowerCase().indexOf(key) >= 0);
      }
      return packages;
    },
  },
  watch: {
    selectedLanguage(val, old) {
      if (val !== old) {
        this.searchKey = '';
      }
    },
  },
};
</script>

<style lang="scss" scoped>
.build-in-library-wrapper {
  height: 100%;
  .filters {
    padding: 16px;
  }
  .content-header {
    color: #000;
    padding: 0 16px;
    margin-bottom: 18px;
  }
  .notebook-content {
    height: calc(100% - 43px);
    padding: 0 16px;
    overflow-y: auto;
    .content-item {
      padding: 0 10px 0 15px;
      line-height: 30px;
      margin-bottom: 5px;
      .title {
        color: #3a84ff;
        outline: none;
        cursor: pointer;
        font-size: 14px;
        font-weight: 900;
      }
      .desp {
        font-size: 12px;
      }
    }
  }
}
</style>
