

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
    <table class="bk-table bk-table-outer-border"
      :class="{ 'dark-theme': isShowDark, 'small-table': isSmallTable }"
      border="1"
      cellspacing="0"
      cellpadding="0">
      <tr>
        <th colspan="2">
          {{ tableTime }}
        </th>
        <slot name="primary-index">
          <th>{{ tableHead }}</th>
        </slot>
        <slot name="rate">
          <th class="data-rate">
            {{ tableRateDes }}
          </th>
        </slot>
      </tr>
      <tbody>
        <template v-if="tableData.length">
          <tr v-for="(item, index) in tableData"
            :key="index">
            <td v-if="item.isShowLeftLabel"
              rowspan="3">
              <span class="vertical-word">{{ $t('最近7天') }}</span>
            </td>
            <td :colspan="item.colspan">
              {{ item.label }}
            </td>
            <td :title="item.num"
              class="pl15 text-left text-overflow">
              {{ item.num }}
            </td>
            <td class="pl15 text-left text-overflow">
              <span v-if="(!item.diffNum && item.diffNum !== 0) || isHiddenRate || item.label === $t('今日')">—</span>
              <div v-else
                class="container"
                :class="[item.diffNum > 0 ? 'warning' : item.diffNum === 0 ? '' : 'primary']">
                <span :class="['mr5', 'symbol-width', !Number(item.diffNum)? 'transparent' : '']">
                  {{ item.diffNum > 0 ? '+' : '-' }}
                </span>
                <span class="max-width text-overflow"
                  :title="handleDiffNum(item.diffNum)">
                  {{ handleDiffNum(item.diffNum) }}
                </span>
                {{ item.rate === 0 ? '' : `（${item.rate}）` }}
                <span v-if="item.diffNum !== 0"
                  :class="['font16', item.diffNum > 0 ? 'icon-arrows-up' : 'icon-arrows-down']" />
              </div>
            </td>
          </tr>
        </template>
        <tr v-else>
          <td colspan="4"
            class="text-center">
            {{ $t('暂无数据') }}
          </td>
        </tr>
      </tbody>
    </table>
  </div>
</template>

<script>
export default {
  props: {
    data: {
      type: Array,
      default: () => [],
    },
    varyExplain: {
      type: String,
      default: '',
    },
    tableHead: {
      type: String,
      default: '',
    },
    tableTime: {
      type: String,
      default: '',
    },
    isShowDark: {
      type: Boolean,
      default: false,
    },
    isSmallTable: {
      type: Boolean,
      default: false,
    },
    tableRateDes: {
      type: String,
      default: '',
    },
    isFixed: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      isHiddenRate: false,
    };
  },
  computed: {
    tableData() {
      if (!Object.keys(this.data).length) return [];

      let target = this.data.find(item => item.label === this.$t('今日'));
      // eslint-disable-next-line vue/no-side-effects-in-computed-properties
      this.isHiddenRate = [0, '—'].includes(target.num);

      return this.data.map(item => {
        item.diffNum = this.getDataRate(target.num, item.num).diffNum;
        item.rate = this.getDataRate(target.num, item.num).rate || 0;
        return item;
      });
    },
  },
  methods: {
    handleDiffNum(num) {
      return Math.abs(num).toFixed(this.isFixed ? 2 : 0);
    },
    getDataRate(todayNum, num) {
      const diffNum = todayNum - num || 0;
      // 除数或者被除数为0时，rate值为—
      const rate = !todayNum || !num ? '—' : `${Math.abs((diffNum / num) * 100).toFixed(2)}%`;

      return {
        rate,
        diffNum,
      };
    },
  },
};
</script>

<style lang="scss" scoped>
.transparent {
  opacity: 0;
}
.vertical-word {
  width: 18px;
  display: inline-block;
  text-align: center;
}
.container {
  display: flex;
  align-items: center;
  justify-content: flex-start;
  .max-width {
    display: inline-block;
  }
  .symbol-width {
    display: inline-block;
    width: 7px;
  }
}
.bk-table {
  width: 100%;
  border-collapse: collapse;
  border: none;
  td {
    text-align: left;
    min-width: 50px;
    padding: 0 15px;
    max-width: 200px;
  }
  .text-center {
    text-align: center;
    height: 252px;
  }
  th {
    .icon-exclamation-circle {
      color: #3a84ff;
      font-size: 14px;
    }
  }
  .text-left,
  th {
    padding: 0 15px;
    text-align: left;
    .icon {
      font-weight: bold;
    }
    .font16 {
      font-weight: bold;
      font-size: 16px;
    }
  }
  .warning {
    color: #ea3636;
  }
  .primary {
    color: #2dcb56;
  }
  .no-data-table {
    width: 100%;
  }
}
.dark-theme {
  td,
  th {
    color: white;
    background-color: rgba(0, 0, 0, 0.8);
  }
}
.small-table {
  td,
  th {
    height: 30px;
    line-height: 30px;
  }
}
</style>
