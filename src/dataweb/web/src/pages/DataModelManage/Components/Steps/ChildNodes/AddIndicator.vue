

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
  <bkdata-sideslider
    :class="['bk-graph-node-container', { 'full-screen': isFullScreen }]"
    :isShow.sync="isShow"
    :width="slideWidth"
    @hidden="reSetParams">
    <div slot="header">
      <NodeHeader :doc="helpDocUrl"
        @headerFullScreenClick="fullScreenHandle">
        <!--eslint-disable vue/no-lone-template-->
        <template>
          <span class="node-header-title">
            <i class="bk-icon icon-quota" />
            {{ isSaveNode ? $t('编辑指标') : $t('创建指标') }}
          </span>
          <span class="node-header-description">
            {{
              $t('指标是基于统计口径、时间周期和维度，来统计分析的具体指标数值。')
            }}
          </span>
        </template>
      </NodeHeader>
    </div>
    <div slot="content"
      v-bkloading="{ isLoading: isMainLoading }"
      class="bkdata-node-action bk-scroll-y">
      <Layout>
        <template #left-pane>
          <IndicatorInput :fieldInfo="fieldInfo"
            :isChildIndicator="isChildIndicator" />
        </template>
        <template #mid-pane>
          <IndexConfig
            ref="indexConfig"
            :params="params"
            :nodeType.sync="nodeType"
            :initNodeType="initNodeType"
            :aggregateFieldList="configAggregateFieldList"
            :calculationFormula="calculationFormula"
            :calculationAtomsList="calculationAtomsList"
            :isSaveNode="isSaveNode"
            :isChildIndicator="isChildIndicator"
            :indexInfo="indexInfo"
            :isRestrictedEdit="isRestrictedEdit" />
        </template>
        <template #right-pane>
          <IndicatorOutput
            ref="dataOutput"
            :nodeType="nodeType"
            :params="params"
            :isSaveNode="isSaveNode"
            :indicatorTableFields="indicatorTableFields"
            :isRestrictedEdit="isRestrictedEdit"
            @changeIndicatorParams="changeIndicatorParams" />
        </template>
      </Layout>
    </div>
    <div slot="footer"
      class="bkdata-node-footer">
      <bkdata-button :loading="isLoading"
        :style="'width: 120px'"
        theme="primary"
        @click="handleBtnSave">
        {{ isSaveNode ? $t('修改') : $t('保存') }}
      </bkdata-button>
      <bkdata-button :disabled="isLoading"
        theme="default"
        :style="'width: 86px'"
        @click="handlebtnClose">
        {{ $t('关闭') }}
      </bkdata-button>
    </div>
  </bkdata-sideslider>
</template>

<script lang="ts" src="./AddIndicator.ts"></script>

<style lang="scss">
@import '~@/pages/DataGraph/Scss/graph.node.scss';
</style>
<style lang="scss" scoped></style>
