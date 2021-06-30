

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
  <div class="edit-cell"
    @click="onFieldClick">
    <template v-if="tooltipMode">
      <bkdata-popover
        v-if="!editMode && !showInput"
        :placement="toolTipPlacement"
        :openDelay="toolTipDelay"
        :content="toolTipContent">
        <div tabindex="0"
          class="cell-content"
          :class="{ 'edit-enabled-cell': canEdit }"
          @keyup.enter="onFieldClick">
          <slot name="content" />
        </div>
      </bkdata-popover>
    </template>
    <template v-else>
      <div
        v-show="!editMode"
        tabindex="0"
        class="cell-content"
        :class="{ 'edit-enabled-cell': canEdit }"
        @keyup.enter="onFieldClick">
        <slot name="content" />
      </div>
    </template>
    <component
      :is="editableComponent"
      v-if="editMode || showInput"
      ref="input"
      v-bind="$attrs"
      v-model="model"
      @change="onInputChange"
      @focus="onFieldClick"
      @keyup.enter.native="onInputExit"
      v-on="listeners">
      <slot name="edit-component-slot" />
    </component>
  </div>
</template>
<script>
export default {
  name: 'editable-cell',
  inheritAttrs: false,
  props: {
    value: {
      type: [String, Number],
      default: '',
    },
    tooltipMode: {
      type: Boolean,
      default: false,
    },
    toolTipContent: {
      type: String,
      default: 'click to edit',
    },
    toolTipDelay: {
      type: Number,
      default: 500,
    },
    toolTipPlacement: {
      type: String,
      default: 'top-start',
    },
    showInput: {
      type: Boolean,
      default: false,
    },
    editableComponent: {
      type: String,
      default: 'bkdata-input',
    },
    closeEvent: {
      type: String,
      default: 'blur',
    },
    canEdit: {
      type: Boolean,
      default: true,
    },
  },
  data() {
    return {
      editMode: false,
    };
  },
  computed: {
    model: {
      get() {
        return this.value;
      },
      set(val) {
        this.$emit('input', val);
      },
    },
    listeners() {
      return {
        [this.closeEvent]: this.onInputExit,
        ...this.$listeners,
      };
    },
  },
  methods: {
    onFieldClick() {
      if (this.canEdit) {
        this.editMode = true;
        this.$nextTick(() => {
          let inputRef = this.$refs.input;
          if (inputRef && inputRef.focus) {
            inputRef.focus();
          }
        });
      }
    },
    onInputExit() {
      this.editMode = false;
    },
    onInputChange(val) {
      this.$emit('inputChange', val);
    },
  },
};
</script>
<style lang="scss" scoped>
.cell-content {
  outline: none;
  cursor: text;
  min-height: 40px;
  line-height: 40px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  border: 1px solid transparent;
}
</style>
