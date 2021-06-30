

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
  <div class="auth-role-wrap">
    <i class="bk-icon icon-user mr5" />
    {{ name || roleId }}
    <span class="note"
      @click.stop="showInfo()">
      <i v-bk-tooltips.top="$t('单击查看角色说明')"
        class="bk-icon note icon-info-circle" />
    </span>
    <div class="bkdata-dialog">
      <bkdata-dialog
        v-model="isShow"
        extCls="bkdata-dialog"
        :hasHeader="false"
        :hasFooter="false"
        :closeIcon="true"
        :maskClose="true"
        :showFooter="false"
        :height="'210px'"
        :width="800">
        <div v-if="isShow">
          <RoleWrapContent :roleId="roleId" />
        </div>
      </bkdata-dialog>
    </div>
  </div>
</template>

<script>
import RoleWrapContent from './RoleWrapContent.vue';

export default {
  components: {
    RoleWrapContent,
  },
  props: {
    name: {
      type: String,
      default: '',
    },
    description: {
      type: String,
      default: '',
    },
    roleId: {
      type: String,
      required: true,
    },
  },
  data() {
    return {
      isShow: false,
    };
  },
  methods: {
    showInfo() {
      this.isShow = true;
    },
  },
};
</script>

<style lang="scss">
.auth-role-wrap {
  display: inline-block;
  min-width: 130px;
  margin-right: 20px;
  font-size: 12px;

  .icon-user {
    padding: 5px;
    border-radius: 60px;
    color: #3a84ff;
    background-color: #3a84ff38;
  }

  .note {
    cursor: pointer;
  }
}
</style>
