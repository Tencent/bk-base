

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
  <div class="data-tag-wrapper">
    <div class="data-tag-inputs no-margin">
      <div class="data-tag-collection">
        <span v-for="(tag, index) in tags"
          :key="index"
          :class="['tag', { 'single-tag': !tag.multiple }]">
          {{ tag[tagName] }}
          <span class="icon-close"
            @click="removeTag(tag)" />
        </span>
        <input v-model="text"
          type="text"
          class="tag-input"
          :placeholder="dynamicPlaceholder">
      </div>
      <div class="data-tag-icon-group">
        <span class="icon-close-circle-shape"
          :title="$t('清空')" />
      </div>
    </div>
  </div>
</template>

<script>
export default {
  props: {
    tags: {
      type: Array,
      default: () => [],
    },
    placeholder: {
      type: String,
      default: '',
    },
    tagKeysMap: {
      type: Object,
      default: () => ({
        subList: 'sub_list',
        tagName: 'tag_alias',
        subTopList: 'sub_top_list',
        tagId: 'tag_code',
      }),
    },
  },
  data() {
    return {
      text: '',
    };
  },
  computed: {
    dynamicPlaceholder() {
      if (this.tags.length) {
        return this.$t('数据标签_点击进行编辑');
      }
      return this.placeholder;
    },
    tagName() {
      return this.tagKeysMap.tagName;
    },
  },
};
</script>
<style lang="scss" scoped>
@import './DataTag.scss';
.data-tag-wrapper {
  .no-margin {
    margin: 0;
  }
}
</style>
