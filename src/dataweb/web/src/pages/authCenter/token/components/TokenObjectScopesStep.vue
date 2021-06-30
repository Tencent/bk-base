

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
  <div class="token-objects-scopes">
    <!-- <BKDataAlert>{{ $t('请选择数据范围，不同对象的范围需要分别进行选择') }}</BKDataAlert> -->
    <TokenObjectScopes
      :scopes="scopes"
      :isCollapse="true"
      :collapsesStatus="true"
      :isEditable="true"
      :selectedScopes="selectedScopes"
      :selectedPermissions.sync="selectedPermissionsCopy"
      @updatePermissions="updatePermissions" />
  </div>
</template>

<script>
import TokenObjectScopes from './TokenObjectScopes';

export default {
  components: {
    TokenObjectScopes,
  },
  props: {
    isCollapse: {
      type: Boolean,
      default: true,
    },
    collapsesStatus: {
      type: Boolean,
      default: false,
    },
    scopes: {
      type: Array,
    },
    selectedScopes: {
      type: Array,
      default: () => [],
    },
    selectedPermissions: {
      type: Array,
      default: () => [],
    },
  },
  data() {
    return {
      selectedPermissionsCopy: [],
    };
  },
  watch: {
    selectedPermissions: {
      immediate: true,
      deep: true,
      handler(list) {
        this.selectedPermissionsCopy = list;
      },
    },
  },
  methods: {
    updatePermissions(permissions) {
      this.$emit('update:selectedPermissions', permissions);
    },
  },
};
</script>

<style lang="scss" scoped>
.token-objects-scopes {
  .object-selector {
    cursor: pointer;
  }
  .bk-form {
    .bk-form-item {
      .bk-label {
        width: 80px;
      }
      .bk-form-content {
        margin-left: 80px;
      }
    }
  }
}
</style>
