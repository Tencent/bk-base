

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
  <div class="ticket-base">
    <div class="title">
      {{ $t('补充信息') }}
    </div>
    <div v-if="isString"
      class="base-info mb0">
      {{ extraInfo }}
    </div>
    <template v-else>
      <div v-for="(item, index) in JSON.parse(extraInfo)"
        :key="index"
        class="base-item">
        <div class="base-wrap">
          <div class="base-row">
            <div class="base-label">
              {{ item.name }}
            </div>
            <div class="base-content">
              {{ item.value }}
            </div>
          </div>
        </div>
        <div class="base-splitter" />
      </div>
    </template>
  </div>
</template>

<script>
export default {
  props: {
    extraInfo: {
      type: [String],
      required: true,
    },
  },
  data() {
    return {
      isString: true,
    };
  },

  watch: {
    extraInfo: {
      immediate: true,
      handler(val) {
        if (!val) return;
        this.isString = typeof JSON.parse(this.extraInfo) === 'string';
      },
    },
  },
};
</script>

<style lang="scss" scoped>
@import '../../scss/base.scss';

.title {
  font-size: 14px;
  width: 120px;
  color: #313238;
  padding: 10px 0;
}
.base-info {
  border-bottom: 1px dashed #ccc;
  padding: 10px 0px;
  word-break: break-word;
}
.base-item {
  background-color: #efefef59;
  padding: 10px 20px 0 20px;
  margin-bottom: 0px;
}

.base-wrap {
  .base-row {
    margin-top: 5px;
    margin-bottom: 5px;
    display: flex;
  }
  .base-label {
    float: left;
    min-width: 100px;
    line-height: 18px;
    border-right: 1px dashed #ccc;
  }
  .base-content {
    margin-left: 25px;
    line-height: 18px;
  }
  .auth-object-wrap {
    margin-left: 0;
  }
}
.base-splitter {
  border-bottom: 1px solid #ccc;
  border-bottom-style: dashed;
}
</style>
