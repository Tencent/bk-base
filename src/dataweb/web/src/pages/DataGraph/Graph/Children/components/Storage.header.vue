

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
  <div class="form-wrapper">
    <p v-if="parentConfig.node_type === 'offline' && nodeParams.node_type !== 'hdfs_storage'"
      class="bk-info">
      <i class="bk-icon icon-info-circle-shape" />
      {{ $t('离线数据导入该存储') }}
    </p>
    <bkdata-form :labelWidth="125"
      :model="formData">
      <bkdata-form-item class="bk-node-node_name"
        :label="$t('节点名称')"
        :required="true"
        :property="'nodeName'">
        <bkdata-input v-model="formData.nodeName"
          :disabled="true" />
        <slot name="nodeName" />
      </bkdata-form-item>
      <bkdata-form-item
        class="bk-node-result_table"
        :label="$t('结果数据表')"
        :required="true"
        :property="'resultTable'">
        <!--eslint-disable vue/no-mutating-props-->
        <bkdata-input v-if="isTdwStorage"
          v-model="tdwDisplay"
          :disabled="true" />
        <bkdata-input v-else
          v-model="formData.resultTable"
          :disabled="true" />
        <slot name="resultTable" />
        <template v-if="jumpResult">
          <span
            v-bk-tooltips="jumpTooltip"
            :class="{ disabled: nodeParams.node_type === 'queue_storage' }"
            class="bk-icon icon-bar-chart fl"
            @click="handleJumpToResultTable" />
        </template>
      </bkdata-form-item>
    </bkdata-form>
  </div>
</template>
<script>
export default {
  model: {
    prop: 'nodeParams',
    event: 'change',
  },
  props: {
    isTdwStorage: {
      type: Boolean,
      default: false,
    },
    tdwDisplay: {
      type: String,
      default: '',
    },
    jumpResult: {
      type: Boolean,
      default: false,
    },
    parentConfig: {
      type: Object,
      default: () => ({}),
    },
    nodeParams: {
      type: Object,
      default: () => ({}),
    },
    isNewForm: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      resultTableData: {},
      resultTableId: '',
      formData: {
        nodeName: '',
        resultTable: '',
      },
    };
  },
  computed: {
    jumpTooltip() {
      return this.nodeParams.node_type === 'queue_storage'
        ? this.$t('该存储暂不支持查询')
        : this.$t('跳转到结果数据表');
    },
  },
  watch: {
    nodeParams: {
      deep: true,
      handler(val) {
        if (val) {
          this.handleNodeConfigChange();
        }
      },
    },
  },
  methods: {
    handleJumpToResultTable(data) {
      if (this.nodeParams.node_type === 'queue_storage') return;
      const rtName = this.parentConfig.result_table_ids[0];
      this.$router.push({
        name: 'DataExploreQuery',
        query: {
          bizId: rtName.split('_')[0],
          rt_name: rtName,
          type: 'personal',
        },
      });
    },
    handleNodeConfigChange() {
      if (!this.isNewForm) {
        this.formData.nodeName = this.nodeParams.config.name;
        this.formData.resultTable = `${this.nodeParams.config.result_table_id}(${this.parentConfig.node_name})`;
      } else {
        this.getResultTables();
      }
    },
    getResultTables() {
      const resultTableId = (this.parentConfig.result_table_ids && this.parentConfig.result_table_ids[0]) || '';
      if (this.resultTableId === resultTableId) {
        return;
      }
      this.resultTableId = resultTableId;
      this.bkRequest.httpRequest('meta/getResultTablesBase', { params: { rtid: resultTableId } }).then(res => {
        if (res.result) {
          this.resultTableData = res.data;
          this.formData.nodeName = `${this.resultTableData.result_table_name_alias}(${this.nodeParams.node_type})`;
          this.formData.resultTable = `${this.resultTableData.result_table_name}(${this.parentConfig.node_name})`;
          // eslint-disable-next-line vue/no-mutating-props
          this.nodeParams.config.name = this.formData.nodeName;
          // this.nodeParams.config.result_table_id = this.formData.resultTable
          this.$emit('change', this.nodeParams);
        }
      });
    },
  },
};
</script>
<style lang="scss" scoped>
.form-wrapper {
  .bk-info {
    display: flex;
    align-items: center;
    width: 100%;
    height: 42px;
    background: rgb(255, 244, 226);
    box-shadow: 0px 2px 4px 0px rgb(237, 230, 219);
    border-radius: 2px;
    border: 1px solid rgb(255, 232, 195);
    .bk-icon {
      color: #ff9c03;
      margin: 0px 10px 0px 15px;
    }
    margin-bottom: 10px;
  }
  .icon-bar-chart {
    position: absolute;
    top: 1px;
    right: 0;
    transform: translateX(calc(100% + 5px));
    width: 32px;
    height: 32px;
    line-height: 32px;
    border: 1px solid #eaeaea;
    background-color: #fafafa;
    text-align: center;
    border-radius: 2px;
    margin-left: 10px;
    cursor: pointer;

    &:hover {
      background: #3a84ff;
      color: #fff;
    }
  }
}
</style>
