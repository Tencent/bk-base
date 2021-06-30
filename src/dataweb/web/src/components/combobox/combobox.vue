

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
  <div :class="[extCls, { open: open }]"
    class="bkdata-combobox">
    <div ref="comboxInput"
      class="bkdata-combobox-wrapper">
      <input
        v-model="localText"
        :class="{ active: open, pr30: (showOpen && !showClear) || (!showOpen && showClear) }"
        :placeholder="placeholder"
        :disabled="disabled"
        class="bkdata-combobox-input"
        @focus="openList"
        @input="onInput">
      <div
        v-if="isClearEnable"
        class="bkdata-combobox-icon-clear"
        :class="{ isEmpty: !localValue.length }"
        @click="localValue = ''">
        <span title="clear">
          <i class="bk-icon icon-close" />
        </span>
      </div>
      <div v-if="showOpen"
        class="bkdata-combobox-icon-box"
        :class="{ isEmpty: !localValue.length }"
        @click="openFn">
        <i class="bk-icon icon-angle-down bkdata-combobox-icon" />
      </div>
    </div>
    <div v-show="false">
      <div ref="comboxContent"
        class="bkdata-combobox-list"
        :style="popStyle">
        <ul v-if="showList.length > 0">
          <li
            v-for="(item, index) in showList"
            :key="index"
            :class="{ 'bkdata-combobox-item-target': item.highlight && localValue.length > 0 }"
            class="bkdata-combobox-item"
            @click.stop="selectItem(item)">
            <div class="text">
              {{ item.displayValue }}
            </div>
          </li>
        </ul>
        <ul v-else-if="(!showList || showList.length <= 0) && showEmpty">
          <li class="bkdata-combobox-item"
            disabled>
            <div class="text">
              无匹配数据
            </div>
          </li>
        </ul>
      </div>
    </div>
  </div>
</template>

<script>
import clickoutside from '../../common/js/clickoutside';
import tippy from 'tippy.js';
export default {
  name: 'bkdata-combobox',
  directives: {
    clickoutside,
  },
  props: {
    placeholder: {
      type: String,
      default: '',
    },
    list: {
      type: Array,
    },
    value: {
      type: [String, Number],
      required: true,
    },
    extCls: {
      type: String,
    },
    // 是否显示清空按钮
    showClear: {
      type: Boolean,
      default: true,
    },
    // 是否显示展开按钮
    showOpen: {
      type: Boolean,
      default: true,
    },
    // 是否显示无数据提醒
    showEmpty: {
      type: Boolean,
      default: true,
    },
    // 是否允许字列表超出父容器宽度
    bypass: {
      type: Boolean,
      default: false,
    },
    // 用于显示的Key
    displayKey: {
      type: String,
      default: '',
    },

    // 返回的ID
    idKey: {
      type: String,
      default: '',
    },

    disabled: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      open: false,
      localValue: this.value,
      localText: '',
      bkPopoverTimerId: '',
      popInstances: [],
      popStyle: {},
    };
  },
  computed: {
    isClearEnable() {
      return this.showClear && !this.disabled && this.localValue.length > 0;
    },
    showList() {
      let newList = [];
      let targetIndexes = [];
      for (let i = 0; i < this.list.length; i++) {
        let item = this.list[i];
        const itemValue = this.getValue(item);
        if (this.localValue && itemValue === this.localValue) {
          targetIndexes.push(i);
        }
      }
      for (let i = 0; i < targetIndexes.length; i++) {
        newList.push({
          value: this.getValue(this.list[targetIndexes[i]]),
          displayValue: this.getValue(this.list[targetIndexes[i]], false),
          highlight: i === 0,
        });
      }
      for (let i = 0; i < this.list.length; i++) {
        if (!targetIndexes.includes(i)) {
          newList.push({
            value: this.getValue(this.list[i]),
            displayValue: this.getValue(this.list[i], false),
            highlight: false,
          });
        }
      }
      return newList;
    },
  },
  watch: {
    showList: {
      deep: true,
      handler(val) {
        this.localText = this.filterLocalText(this.localValue);
      },
    },
    localValue() {
      this.$emit('update:value', this.localValue);
    },
    value: {
      immediate: true,
      handler(newVal) {
        this.localValue = newVal;
        this.$nextTick(() => {
          this.localText = this.filterLocalText(newVal);
        });
      },
    },
  },
  methods: {
    filterLocalText(value) {
      const listItem = (value && this.showList.find(item => item.value === value)) || {};
      return listItem.displayValue ? listItem.displayValue : value;
    },
    getValue(item, isValue = true) {
      const key = isValue ? this.idKey : this.displayKey;
      if (typeof item === 'object') {
        return key ? item[key] : item.value;
      }

      if (typeof item === 'string') {
        return item;
      }

      return '';
    },
    selectItem(item) {
      this.localValue = item.value;
      this.localText = item.displayValue;
      this.close();

      this.$emit('update:value', this.localValue);
      this.$emit('item-selected', this.localValue);
    },
    openFn() {
      if (!this.disabled) {
        this.openList(!this.open);
      }
    },
    close() {
      this.openList(false, true);
    },
    onInput() {
      this.localValue = this.localText;
      this.$emit('update:value', this.localValue);
      this.$emit('input', this.localValue);
      this.openList(true, false);
    },
    openList(isOpen = true, emitEvent = true) {
      this.open = isOpen;
      emitEvent && this.$emit('visible-toggle', this.open);
      this.openContent(isOpen);
    },

    openContent(isOpen) {
      if (isOpen) {
        this.bkPopoverTimerId && clearTimeout(this.bkPopoverTimerId);
        this.bkPopoverTimerId = setTimeout(() => {
          tippy.setDefaults({
            duration: 0,
            updateDuration: 0,
            animateFill: false,
          });
          this.popStyle = { width: this.$refs.comboxInput.offsetWidth + 'px' };
          const popInstance = tippy(this.$refs.comboxInput, {
            content: this.$refs.comboxContent,
            interactive: true,
            trigger: 'manual',
            theme: 'light',
            arrow: false,
            placement: 'bottom-start',
            maxWidth: this.$refs.comboxInput.offsetWidth,
            distance: 0,
            allowHTML: true,
            appendTo: () => document.body,
            onHidden: () => {
              this.close();
            },
          });
          this.fireInstance();
          popInstance && popInstance.show();
          this.popInstances.push(popInstance);
        }, 300);
      } else {
        this.fireInstance();
      }
    },

    fireInstance() {
      while (this.popInstances.length) {
        const instance = this.popInstances[0];
        instance && instance.destroy();
        this.popInstances.shift();
      }
    },
  },
};
</script>

<style lang="scss">
@import './combobox.scss';
.pr60 {
  padding-right: 60px;
}
.pr30 {
  padding-right: 30px;
}
.bypass {
  float: left;
  max-width: 500px;
}
.non-bypass {
  width: 100%;
}
</style>
