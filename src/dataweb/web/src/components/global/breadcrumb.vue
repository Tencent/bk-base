

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
  <div class="global-crumb">
    <div class="crumb-left">
      <slot v-if="isCrumbNameArray"
        name="button">
        <template v-for="(item, index) in crumbName">
          {{ (index && '>') || '' }}
          <a
            :key="index"
            href="javascript:void(0);"
            :class="['crumb-item', (item.to && 'with-router') || '']"
            @click.stop="handleGoto(item.to)">
            {{ item.name }}
          </a>
        </template>
      </slot>
      <template v-else>
        <span class="crumb-name"
          @click="$emit('crumbNameClick')">
          {{ crumbName }}
        </span>
      </template>
    </div>
    <div class="crumb-right">
      <slot />
    </div>
  </div>
</template>
<script>
export default {
  props: {
    crumbName: {
      type: [String, Array],
      default: 'Breadcrumb',
    },
  },
  computed: {
    isCrumbNameArray() {
      return Array.isArray(this.crumbName);
    },
  },
  methods: {
    handleGoto(to) {
      if (to) {
        if (typeof to === 'string') {
          this.$router.push({ path: to });
        } else if (typeof to === 'object') {
          this.$router.push(to);
        } else {
          return false;
        }
      } else {
        return false;
      }
    },
  },
};
</script>
<style lang="scss">
.global-crumb {
  display: flex;
  justify-content: space-between;
  height: 100%;
  .crumb-left {
    padding-left: 12px;
    position: relative;
    &::before {
      content: '';
      width: 2px;
      height: 20px;
      position: absolute;
      left: 0px;
      top: 50%;
      background: #3a84ff;
      transform: translateY(-50%);
      font-weight: 500;
    }

    .crumb-name {
      cursor: pointer;
    }

    .crumb-item {
      color: #606266;
      cursor: inherit;
      &.with-router {
        color: #303133;
        cursor: pointer;
        font-weight: 700;
        &:hover {
          color: #3a84ff;
          transition: all 0.5s linear;
        }
      }
    }
  }
}
</style>
