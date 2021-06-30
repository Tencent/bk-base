

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
    <p class="rule-title">
      {{ $t('规则逻辑配置') }}
    </p>
    <bkdata-tab :active.sync="active">
      <bkdata-tab-panel :label="$t('简单配置')"
        :name="'easyConfig'">
        <div class="container">
          <div v-if="active === 'easyConfig'"
            class="easy-config">
            <div v-for="(item, index) in ruleList"
              :key="index"
              class="rule-item">
              <bk-cascade ref="cascadeInstance"
                v-model="cascadeValues[index]"
                :checkAnyLevel="false"
                :disabled="disabled"
                extCls="cascade-style"
                :list="ruleIndexList"
                @change="data => onCascadeChange(data, index)" />
              <bkdata-selector extCls="width300"
                :disabled="disabled"
                :displayKey="'functionAlias'"
                :list="ruleFuncList"
                :multiSelect="false"
                :selected.sync="item.function"
                :settingKey="'functionName'"
                :searchKey="'functionAlias'" />
              <bkdata-input v-model="item.constant.constant_value"
                type="number"
                :disabled="disabled"
                extCls="width350"
                :placeholder="$t('请选择阈值')"
                :clearable="true" />
              <bkdata-selector :extCls="`ml10 width300 ${index !== ruleList.length - 1 ? '' : 'opacity-hidden'}`"
                :disabled="disabled"
                :displayKey="'name'"
                :list="symbolList"
                :multiSelect="false"
                :searchable="true"
                :selected.sync="item.operation"
                :settingKey="'id'"
                :searchKey="'name'" />
              <span class="icon icon-plus"
                :class="{ disabled: disabled }"
                :title="$t('添加')"
                @click="add(index)" />
              <span class="icon icon-minus"
                :class="{ disabled: ruleList.length === 1 || disabled }"
                :title="$t('删除')"
                @click="del(index)" />
              <span :class="['icon', 'icon-arrows-up', { disabled: index === 0 || disabled }]"
                :title="$t('上移')"
                @click="upMove(index)" />
              <span :class="['icon', 'icon-arrows-down', { disabled: index === ruleList.length - 1 || disabled }]"
                :title="$t('下移')"
                @click="downMove(index)" />
            </div>
          </div>
          <!-- <p class="bk-text-primary rule-explain">{{ $t('规则使用说明') }}<span class="ml10 icon-link-to"></span></p> -->
        </div>
      </bkdata-tab-panel>
    </bkdata-tab>
  </div>
</template>

<script lang="ts" src="./RuleContentConfig.ts"></script>

<style lang="scss">
.bk-option {
  min-width: auto;
}
</style>
<style lang="scss" scoped>
::v-deep .bk-cascade {
  width: 180px;
  .bk-cascade-dropdown {
    width: 180px !important;
  }
}
.cascade-style {
  width: 180px;
  ::v-deep .bk-cascade-dropdown {
    width: 180px !important;
  }
}
.rule-title {
  line-height: 36px;
  font-size: 14px;
  background-color: #f5f5f5;
  padding-left: 10px;
  border-left: 1px solid #ddd;
  border-right: 1px solid #ddd;
}
.container {
  padding: 20px;
  .easy-config {
    .rule-item {
      display: flex;
      flex-wrap: nowrap;
      align-items: center;
      padding-top: 5px;
      .icon {
        width: 24px;
        height: 24px;
        margin-left: 15px;
        border-radius: 12px;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 24px;
        border: 1px solid #ddd;
        cursor: pointer;
      }
      .icon-minus {
        transform: scale(0.96);
        &::before {
          transform: scale(0.6);
          font-weight: 700;
        }
      }
      .width300 {
        width: 200px;
      }
      .width350 {
        width: 300px;
        ::v-deep .bk-input-number {
          .input-number-option {
            top: 2px;
          }
        }
        ::v-deep .bk-option {
          min-width: auto;
        }
      }
      .width400 {
        width: 400px;
      }
      .icon-arrows-up,
      .icon-arrows-down {
        &::before {
          transform: scale(0.8);
        }
      }
    }
  }
}
.rule-explain {
  cursor: pointer;
}
.opacity-hidden {
  opacity: 0;
}
</style>
