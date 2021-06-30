

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
  <span class="running-icons">
    <span v-tooltip.top="$t('成功')">
      <i class="bk-icon icon-circle-shape bk-success" />{{ status.success || 0 }}
    </span>
    <span v-tooltip.top="$t('异常')"> <i class="bk-icon icon-circle-shape bk-danger" />{{ status.failure || 0 }} </span>
    <span v-tooltip.top="$t('运行中')">
      <i class="bk-icon icon-circle-shape bk-primary" />{{ status.running || 0 }}
    </span>
    <span v-tooltip.top="$t('刷新')"
      class="running-icon-refresh">
      <i :class="`bk-icon icon-refresh ${loading ? 'refresh-loading' : ''}`"
        @click="refresh" />
    </span>
  </span>
</template>
<script>
export default {
  props: {
    status: {
      type: Object,
      default: () => {
        return {
          success: 0,
          failure: 0,
          running: 0,
        };
      },
    },
  },
  data() {
    return {
      loading: false,
    };
  },
  methods: {
    // 刷新当前状态
    refresh() {
      if (this.loading) {
        return;
      }
      this.loading = true;
      this.$emit('refresh', this.clearLoading);
    },
    clearLoading() {
      this.loading = false;
    },
  },
};
</script>

<style lang="scss" scoped>
.running-icons {
  padding: 5px 15px 5px 0;

  .running-icon-refresh {
    margin-left: 5px;
  }
  .bk-icon {
    margin: 0 5px;

    @media (min-width: 1400px) {
      margin: 0 5px 0 15px;
    }

    &.icon-refresh {
      cursor: pointer;
      margin: 0;
    }

    &:hover {
      color: #5354a5;
    }

    &.bk-success {
      color: #8dc358;
    }
    &.bk-danger {
      color: #fb6118;
    }
    &.bk-primary {
      color: #5354a5;
    }
  }
}
</style>
