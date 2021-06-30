

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
  <div>
    <div class="bk-form-item access-item mt10">
      <label class="bk-label">{{ $t('采集范围') }}：</label>
      <div class="bk-form-content">
        <bkdata-input :value="collectionRange"
          disabled />
      </div>
    </div>
    <div class="bk-form-item access-item mt10">
      <label class="bk-label">{{ $t('数据库_表') }}：</label>
      <div class="bk-form-content">
        <div class="db-table-set">
          <bkdata-input class="db"
            :value="`${details.db_name}`"
            disabled />
          <span class="split-line"> / </span>
          <bkdata-input class="table"
            :value="`${details.table_name}`"
            disabled />
        </div>
      </div>
    </div>
    <div class="bk-form-item access-item mt10">
      <label class="bk-label">{{ $t('过滤条件') }}：</label>
      <div class="bk-form-content db-filter-panel">
        {{ filterCondition }}
      </div>
    </div>
  </div>
</template>

<script>
import mixin from '@/pages/DataAccess/Config/mixins.js';
export default {
  mixins: [mixin],
  props: {
    details: {
      type: Object,
      default: () => {},
    },
    filterCondition: {
      type: String,
      default: '',
    },
  },
  data() {
    return {
      //
    };
  },
  computed: {
    collectionRange() {
      if (!this.details) {
        return '域名:端口';
      }
      return `${this.details.db_host}:${this.details.db_port}`;
    },
  },
};
</script>

<style lang="scss" scoped>
.bk-form-item.access-item .bk-form-content {
  width: 100%;
}

.db-filter-panel {
  max-height: 70px;
  overflow: hidden;
  overflow-y: scroll;

  &::-webkit-scrollbar {
    width: 8px;
    background-color: transparent;
  }

  &::-webkit-scrollbar-thumb {
    border-radius: 8px;
    background-color: #a0a0a0;
  }
}

.db-table-set {
  display: flex;

  .split-line {
    display: inline-block;
    width: 20px;
    line-height: 28px;
    text-align: center;
  }

  .db {
    width: 45%;
  }

  .table {
    width: 55%;
  }
}
</style>
