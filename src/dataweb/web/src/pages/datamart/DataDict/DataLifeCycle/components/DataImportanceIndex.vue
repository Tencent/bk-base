

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
  <div class="data-importance-container">
    <table
      v-bkloading="{ isLoading: isLoading }"
      class="bk-table bk-table-outer-border bk-table-samll"
      border="1"
      cellspacing="0"
      cellpadding="0">
      <tr>
        <th class="table-head"
          colspan="4">
          {{ $t('数据重要度相关的指标（业务、项目、结果表、使用APP四个维度）') }}
        </th>
      </tr>
      <tbody>
        <template v-if="Object.keys(tableData).length">
          <tr>
            <td class="pl15 text-left text-overflow">
              {{ $t('业务运营状态') }}
            </td>
            <td class="pl15 text-left text-overflow">
              {{ tableData.oper_state_name || '—' }}
            </td>
            <td class="pl15 text-left text-overflow">
              {{ $t('数据标签') }}
            </td>
            <td class="pl15 text-left text-overflow max150">
              <DictTag :tagList="tagList" />
            </td>
          </tr>
          <tr>
            <td class="pl15 text-left text-overflow">
              {{ $t('业务星级') }}
            </td>
            <td class="pl15 text-left text-overflow">
              {{ tableData.bip_grade_name || '—' }}
            </td>
            <td class="pl15 text-left text-overflow">
              {{ $t('数据敏感度') }}
            </td>
            <td class="pl15 text-left text-overflow">
              {{ sensitivity }}
            </td>
          </tr>
          <tr>
            <td class="pl15 text-left text-overflow">
              {{ $t('业务重要级别') }}
            </td>
            <td class="pl15 text-left text-overflow">
              {{ tableData.app_important_level_name }}
            </td>
            <td class="pl15 text-left text-overflow">
              {{ $t('数据生成类型') }}
            </td>
            <td class="pl15 text-left text-overflow">
              {{ tableData.generate_type === 'user' ? $t('用户') : $t('系统') }}
            </td>
          </tr>
          <tr>
            <td class="pl15 text-left text-overflow">
              {{ $t('是否关联BIP') }}
            </td>
            <td class="pl15 text-left text-overflow">
              {{ tableData.is_bip }}
            </td>
            <td v-bk-tooltips="$t('最近7天')"
              class="pl15 text-left text-overflow">
              {{ $t('是否有数据') }}
            </td>
            <td class="pl15 text-left text-overflow">
              {{ tableData.has_data ? $t('是') : $t('否') }}
            </td>
          </tr>
          <tr>
            <td class="pl15 text-left text-overflow">
              {{ $t('项目运营状态') }}
            </td>
            <td class="pl15 text-left text-overflow">
              {{ tableData.active ? $t('运营') : $t('删除') }}
            </td>
            <td class="pl15 text-left text-overflow">
              {{ $t('数据质量评分') }}
            </td>
            <td class="pl15 text-left text-overflow">
              {{ '—' }}
            </td>
          </tr>
          <tr>
            <td class="pl15 text-left text-overflow">
              {{ $t('项目重要级别') }}
            </td>
            <td class="pl15 text-left text-overflow">
              {{ '—' }}
            </td>
            <td v-bk-tooltips="$t('最近1天')"
              class="pl15 text-left text-overflow">
              {{ $t('是否有告警') }}
            </td>
            <td
              v-bk-tooltips="{
                content: $t('最近1天'),
                zIndex: tableData.has_alert ? 9999 : -1,
              }"
              class="pl15 text-left text-overflow">
              {{ tableData.has_alert ? $t('是') : $t('否') }}
            </td>
          </tr>
          <tr>
            <td class="pl15 text-left text-overflow">
              {{ $t('应用APP等级') }}
            </td>
            <td class="pl15 text-left text-overflow">
              {{ '—' }}
            </td>
            <td class="pl15 text-left text-overflow">
              {{ $t('用户是否关注') }}
            </td>
            <td class="pl15 text-left text-overflow">
              {{ '—' }}
            </td>
          </tr>
        </template>
        <tr v-else>
          <td colspan="4"
            class="min-height">
            <NoData />
          </td>
        </tr>
      </tbody>
    </table>
  </div>
</template>

<script>
import { dataTypeIds } from '@/pages/datamart/common/config';
import DictTag from '@/pages/datamart/common/components/DictTag';
import NoData from '@/pages/datamart/DataDict/components/children/chartComponents/NoData.vue';

export default {
  components: {
    DictTag,
    NoData,
  },
  props: {
    sensitivity: {
      type: String,
      default: '',
    },
    tagList: {
      type: Array,
      default: () => [],
    },
  },
  data() {
    return {
      tableData: {},
      isLoading: false,
    };
  },
  mounted() {
    this.getTableData();
  },
  methods: {
    getTableData() {
      this.isLoading = true;
      this.bkRequest
        .httpRequest('dataDict/getImportanceIndex', {
          query: {
            dataset_id: this.$route.query[dataTypeIds[this.$route.query.dataType]],
            dataset_type: this.$route.query.dataType,
          },
        })
        .then(res => {
          if (res.result && Object.keys(res.data).length) {
            this.tableData = res.data;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](err => {
          this.isLoading = false;
        });
    },
  },
};
</script>

<style lang="scss" scoped>
.vertical-word {
  width: 18px;
  display: inline-block;
}
.container {
  display: flex;
  align-items: center;
  justify-content: flex-start;
  .max-width {
    display: inline-block;
  }
}
.data-importance-container {
  display: flex;
  align-items: center;
  box-shadow: 0 0 10px 0 rgba(33, 34, 50, 0.05);
  padding: 10px;
  flex: 1;
  .bk-table {
    width: 100%;
    border-collapse: collapse;
    border: none;
    table-layout: fixed;
    td {
      text-align: center;
      min-width: 50px;
      padding: 0 10px;
      max-width: 200px;
    }
    .max150 {
      // max-width: 300px;
    }
    th {
      .icon-exclamation-circle {
        color: #3a84ff;
        font-size: 14px;
      }
    }
    .min-height {
      height: 300px;
    }
    .table-head {
      text-align: center;
    }
    .text-left,
    th {
      padding: 0 10px;
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
