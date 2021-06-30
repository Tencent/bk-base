

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
  <section class="menu-item-list">
    <template v-for="item in list">
      <div
        :key="getGenerateModelId(item.model_id)"
        v-bk-tooltips="getModelInfoTooltips(item.model_id)"
        class="list-view-item"
        :class="{ selected: modelId == item.model_id }">
        <span class="item-label"
          @click.stop="click(item)">
          <i
            :class="['icon', item.publish_status !== 'developing' ? 'icon-publish-fill' : 'icon-unpublished-line']"
            :style="{
              'font-size': '16px',
              color: item.publish_status === 're-developing' ? '#FFB848' : '#C4C6CC',
            }" />
          {{ item.model_name }} ({{ item.model_alias }})
        </span>
        <span v-if="isShowIcon"
          class="item-icon-group">
          <i
            v-bk-tooltips="iconTips"
            :class="`bk-icon icon-${type} item-icons`"
            @click.stop="handleModelTopChanged(item)" />
          <label class="item-count">{{ item.applied_count }}</label>
        </span>
        <span v-if="isShowDelete"
          class="item-icon-group">
          <i
            v-bk-tooltips="deleteIconTips(item)"
            :class="[
              'bk-icon',
              'icon-delete',
              'item-icons',
              { disabled: item.is_instantiated || item.is_quoted || item.is_related },
            ]"
            @click.stop="handleDeleteModel(item)" />
        </span>
        <div :id="`model_${item.model_id}`"
          class="item-tooltips">
          <p>
            <span>{{ $t('模型类型：') }}</span>
            {{ modelType[item.model_type] }}
          </p>
          <p>
            <span>{{ $t('英文名称：') }}</span>
            {{ item.model_name }}
          </p>
          <p>
            <span>{{ $t('中文名称：') }}</span>
            {{ item.model_alias }}
          </p>
          <p>
            <span>{{ $t('应用数量：') }}</span>
            {{ item.applied_count }}
          </p>
          <p>
            <span>{{ $t('模型状态：') }}</span>
            {{ status[item.publish_status] }}
          </p>
        </div>
      </div>
    </template>
  </section>
</template>
<script lang="ts" src="./MenuItemList.ts"></script>
<style lang="scss" scoped>
.menu-item-list {
  display: inline-block;
  width: 100%;
  color: #63656e;
  font-size: 12px;
  position: relative;
  .list-view-item {
    display: flex;
    padding: 0 16px;
    height: 28px;
    line-height: 28px;
    cursor: pointer;
    &:hover,
    &.selected {
      cursor: pointer;
      background: #eaf3ff;
      color: #3a84ff;
      .item-icon-group {
        .item-icons {
          display: inline-block;
        }
        .item-count {
          color: #fff;
          background: #a3c5fd;
        }
      }
    }
    .item-label {
      flex: 1;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
  }
  .item-icon-group {
    max-width: 48px;
    margin-left: 4px;
    .item-icons {
      display: none;
    }
    .urge-trans {
      transform: rotate(45deg);
    }
    .item-count {
      margin-left: 4px;
      text-align: center;
      color: #979ba5;
      line-height: 14px;
      background: #fafbfd;
      border-radius: 2px;
      padding: 0 4px;
    }
  }
}
</style>

<style lang="scss">
.tippy-tooltip.data-model-tooltips-theme {
  border: 1px solid #dcdee5;
  border-radius: 2px;
  padding: 10px 20px 2px;
  min-width: 240px;
  &[x-placement^='right'] .tippy-arrow {
    filter: drop-shadow(0px 1px 0px rgba(0, 0, 0, 0.2));
  }
  .item-tooltips {
    p {
      margin-bottom: 6px;
    }
    span {
      color: #979ba5;
    }
  }
}
</style>
