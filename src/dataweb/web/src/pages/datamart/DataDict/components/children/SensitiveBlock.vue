

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
  <div class="sensitive-container">
    <div class="sensitive-block">
      <div class="basic-style"
        :class="warningClass">
        {{ text }}
      </div>
      <span v-if="isShowEdit"
        v-bk-tooltips="sensitiveTips[tagMethod]"
        class="icon-info-circle-shape ml5"
        :class="[warningClass]" />
    </div>
    <span v-if="isShowEdit"
      v-bk-tooltips="disabled ? $t('权限不足') : $t('点击修改敏感度')"
      class="icon-edit"
      :class="{ disabled: disabled }"
      @click="editChange" />
  </div>
</template>
<script>
export default {
  props: {
    text: {
      type: String,
      default: '',
    },
    warningClass: {
      type: String,
      default: 'public',
    },
    tagMethod: {
      type: String,
      default: 'default',
    },
    isShowEdit: {
      type: Boolean,
      default: false,
    },
    disabled: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      sensitiveTips: {
        default: '【默认规则】通过血缘关系继承父表的敏感度',
        user: '【用户定义】优先以用户指定的敏感度为准，子表按照规则继承下去',
      },
    };
  },
  methods: {
    editChange() {
      if (this.disabled) return;
      this.$emit('edit');
    },
  },
};
</script>
<style lang="scss" scoped>
.sensitive-container {
  display: flex;
  flex-wrap: nowrap;
  align-items: center;
  .sensitive-block {
    width: calc(100% - 34px);
    .basic-style {
      padding: 1px 20px !important;
      border-radius: 20px;
      line-height: 23px !important;
      display: inline;
      white-space: nowrap;
      border: 1px solid #ddd;
    }
    .public {
      border-color: #2dcb56;
      color: #2dcb56;
    }
    .private {
      border-color: #ffb848;
      color: #ffb848;
    }
    .confidential {
      border-color: #8e3e1f;
      color: #8e3e1f;
    }
    .topsecret {
      border-color: #ea3636;
      color: #ea3636;
    }
    .icon-info-circle-shape {
      border: none;
    }
  }
  .icon-edit {
    cursor: pointer;
    color: #3a84ff;
  }
}
</style>
