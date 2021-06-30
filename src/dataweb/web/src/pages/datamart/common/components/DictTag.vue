

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
  <div class="platform">
    <span v-for="(item, index) in tagList.slice(0, 3)"
      :key="index"
      :title="getEnName(item)"
      class="platform-item text-overflow">
      {{ getEnName(item) }}
    </span>
    <bkdata-popover placement="top">
      <span v-if="tagList.length > 3"
        class="icon-ellipsis platform-item-ellipsis" />
      <div slot="content"
        class="clearfix"
        style="white-space: normal; max-width: 500px; max-height: 400px">
        <span v-for="(item, index) in tagList.slice(3)"
          :key="index"
          class="data-platform-item">
          {{ getEnName(item) }}
        </span>
      </div>
    </bkdata-popover>
  </div>
</template>

<script>
export default {
  props: {
    tagList: {
      type: Array,
      default: () => [],
    },
  },
  methods: {
    getEnName(row, enField = 'tag_code', zhField = 'tag_alias') {
      if (this.$i18n.locale === 'en') {
        return row[enField];
      }
      return row[zhField];
    },
  },
};
</script>

<style lang="scss" scoped>
.platform {
  display: flex;
  align-items: center;
  .platform-name {
    max-width: 110px;
  }
  .platform-item {
    display: inline-block;
    max-width: 69px;
    min-width: 34px;
    margin-left: 10px;
    padding: 2px 4px;
    border: 1px solid #ddd;
  }
  .platform-item:first-child {
    margin-left: 0px;
  }
  .platform-item-ellipsis {
    display: flex;
    align-items: center;
    color: #63656e;
    height: 23px;
    padding: 0px 4px;
    margin-left: 10px;
    border: 1px solid #ddd;
  }
  .platform-online {
    background-color: #3a84ff;
    border-color: #3a84ff;
    color: white;
  }
}
.data-platform-item {
  white-space: nowrap;
  margin-left: 10px;
  padding: 2px 4px;
  border: 1px solid #ddd;
  float: left;
  &:first-child {
    margin-left: 0px;
  }
}
</style>
