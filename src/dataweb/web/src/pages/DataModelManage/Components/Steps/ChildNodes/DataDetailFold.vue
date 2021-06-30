

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
  <ul v-if="dataList.length"
    class="data-detail-container">
    <li v-for="(item, index) in ulDataList"
      :key="index">
      <template v-if="!item.groupDataList">
        <p v-bk-overflow-tips="{ content: item.value }"
          class="data-detail-item text-overflow">
          <span class="data-label">{{ item.label }}：</span>
          {{ item.value }}
        </p>
      </template>
      <template v-else>
        <operationGroup v-if="isShowOperationGroup">
          <template v-for="(child, idx) in item.groupDataList">
            <p v-if="!child.isHidden"
              :key="idx"
              class="data-detail-item">
              <span class="data-label">{{ child.label }}：</span>
              {{ child.value }}
            </p>
          </template>
        </operationGroup>
      </template>
    </li>
    <span v-if="!isAlwaysOpen"
      class="icon-container"
      @click="isOpen = !isOpen">
      <i :class="['icon', isOpen ? 'icon-angle-double-up' : 'icon-angle-double-down']" />{{
        isOpen ? $t('收起') : $t('更多')
      }}
    </span>
  </ul>
</template>

<script lang="ts">
import { bkOverflowTips } from 'bk-magic-vue';
import { Component, Emit, Prop, PropSync, Ref, Vue, Watch } from 'vue-property-decorator';
import operationGroup from '@/bkdata-ui/components/operationGroup/operationGroup.vue';

@Component({
  components: {
    operationGroup,
  },
  directives: {
    bkOverflowTips,
  },
})
export default class DataDetailFold extends Vue {
  /**  示例
     * [
        {
            label: $t('指标统计口径'),
            value: `${this.tableInfoData.data.calculationAtomAlias}（${this.tableInfoData.data.calculationAtomName}）`
        },
        {
            label: $t('口径聚合逻辑'),
            value: this.tableInfoData.data.calculationFormula
        },
        {
            label: $t('聚合字段'),
            value: aggregationFieldsStr
        },
        {
            label: $t('过滤条件'),
            value: this.tableInfoData.data.filterFormulaStrippedComment
        },
        {
            groupDataList: [
                {
                    label: $t('计算类型'),
                    value: this.tableInfoData.data.schedulingType === 'stream' ? $t('实时计算') : $t('离线计算')
                },
                {
                    label: $t('窗口类型'),
                    value: this.windowTypeMap[this.tableInfoData.data.schedulingContent.windowType]
                }
            ]
        }
        ]
    */
  @Prop({ default: () => [] }) dataList: any[];
  @Prop({ default: false }) isAlwaysOpen: boolean;

  get ulDataList() {
    return this.dataList.slice(0, this.isOpen ? this.dataList.length : 6);
  }

  @Emit('on-change-status')
  handleChangeStatus() {
    return this.isOpen;
  }

  @Watch('dataList', { deep: true })
  onDataChanged() {
    // operationGroup组件的分隔符在数据改变时不能重新渲染，需要手动刷新
    this.isShowOperationGroup = false;
    this.isOpen = this.isAlwaysOpen;
    this.$nextTick(() => {
      this.isShowOperationGroup = true;
    });
  }

  @Watch('isAlwaysOpen', { immediate: true })
  handleSetOpenStatus() {
    this.isOpen = this.isAlwaysOpen;
  }

  @Watch('isOpen')
  onChangestatus() {
    this.handleChangeStatus();
  }

  isOpen: boolean = false;
  isShowOperationGroup = true;
}
</script>

<style lang="scss" scoped>
.data-detail-container {
  position: relative;
  display: flex;
  flex-wrap: wrap;
  margin-top: 16px;
  font-size: 12px;
  li {
    max-width: 500px;
    min-width: 450px;
    width: 50%;
    text-align: left;
    margin-bottom: 16px;
    .data-detail-item {
      display: inline-block;
      height: 16px;
      line-height: 16px;
      text-align: left;
      color: #63656e;
      &.text-overflow {
        width: 100%;
      }
      .data-label {
        color: #979ba5;
      }
    }
  }
  .icon-container {
    position: absolute;
    right: 5px;
    bottom: 17px;
    color: #3a84ff;
    cursor: pointer;
  }
}
::v-deep .operation-separator {
  margin: 0 8px;
}
</style>
