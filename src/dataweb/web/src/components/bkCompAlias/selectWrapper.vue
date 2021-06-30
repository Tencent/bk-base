

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
  <bkSelect
    ref="selector"
    v-model="value"
    :searchable="searchable"
    :multiple="multiSelect"
    :clearable="allowClear"
    :loading="isLoading"
    :disabled="disabled"
    :fontSize="fontSize"
    :placeholder="calcPlaceHolder"
    :remoteMethod="remoteMethod"
    :popoverWidth="dropDownWidth || calcPopoverWidth"
    :style="customStyle"
    :popoverOptions="popoverOptions"
    :extCls="extCls"
    :showOnInit="showOnInit"
    @selected="handleSelected"
    @toggle="handleToggle"
    @clear="handleClear"
    @change="handleChange">
    <div
      v-scroll-bottom="{ callback: handleScrollCallback }"
      class="select-option-wrapper bk-scroll-ys bk-scroll-x"
      :style="calcStyle">
      <template v-if="calcHasChild">
        <bk-option-group
          v-for="(group, index) in calcList"
          :key="index"
          :name="group[groupKey] || group[displayKey] || group[settingKey]">
          <bk-option
            v-for="option in group.children"
            :id="option[settingKey]"
            :key="option[settingKey]"
            :name="option[displayKey]"
            :disabled="!!option.disabled || !!group.isReadonly || !!group.disabled"
            @mouseenter.native="e => handleEnter(e, option)"
            @mouseleave.native="handleLeave">
            <template v-if="customIcon">
              <div class="option-custom-icon-wrapper"
                :title="option[displayKey]">
                <i :class="['bk-icon option-custom-icon',
                            option.colorClass || '',
                            'icon-' + customIcon]" />
                {{ option[displayKey] }}
              </div>
            </template>
          </bk-option>
        </bk-option-group>
      </template>
      <template v-else>
        <bkOption
          v-for="option in calcList"
          :id="option[settingKey]"
          :key="option[settingKey]"
          :name="option[displayKey]"
          :disabled="!!option.disabled"
          @mouseenter.native="e => handleEnter(e, option)"
          @mouseleave.native="handleLeave">
          <template v-if="customIcon">
            <div class="option-custom-icon-wrapper">
              <i :class="[
                'bk-icon option-custom-icon',
                option.colorClass || '',
                'icon-' + customIcon]" />
              {{ option[displayKey] }}
            </div>
          </template>
        </bkOption>
      </template>
    </div>
    <div slot="extension"
      @click="handleBottomClick">
      <slot name="bottom-option" />
    </div>
  </bkSelect>
</template>
<script>
import { bkSelect, bkOption, bkOptionGroup } from 'bk-magic-vue';
import scrollBottom from '@/common/directive/scroll-bottom.js';
import tippy from 'tippy.js';

export default {
  name: 'bkdata-selector',
  directives: {
    scrollBottom,
  },
  components: {
    bkSelect,
    bkOption,
    bkOptionGroup,
  },
  props: {
    extCls: {
      type: String,
      default: '',
    },
    fontSize: {
      type: String,
      default: 'normal',
    },
    autoSort: {
      type: Boolean,
      default: true,
    },
    placeholder: {
      type: String,
      default: '',
    },
    isLoading: {
      type: Boolean,
      default: false,
    },
    hasCreateItem: {
      type: Boolean,
      default: false,
    },
    createText: {
      type: String,
      default: '新增数据源',
    },
    hasChildren: {
      type: [Boolean, String],
      default: false,
    },
    list: {
      type: Array,
      required: true,
      default: () => []
    },
    selected: {
      type: [String, Object, Array, Number, Boolean],
      default: ''
    },
    displayKey: {
      type: String,
      default: 'name',
    },
    groupKey: {
      type: String,
      default: 'name',
    },
    disabled: {
      type: [String, Boolean, Number],
      default: false,
    },
    multiSelect: {
      type: Boolean,
      default: false,
    },
    searchable: {
      type: Boolean,
      default: false,
    },
    searchKey: {
      type: String,
      default: null,
    },
    allowClear: {
      type: Boolean,
      default: false,
    },
    settingKey: {
      type: String,
      default: 'id',
    },
    optionTip: {
      type: Boolean,
      default: false,
    },
    toolTipTpl: {
      type: [String, Function],
      default: '',
    },
    /** 下拉item的hover的tippy配置 */
    toolTipConfig: {
      type: Object,
      default: () => ({}),
    },
    customStyle: {
      type: Object,
      default: () => ({}),
    },
    customExtendCallback: {
      type: Function,
      default: () => true,
    },
    customIcon: {
      type: String,
      default: '',
    },
    popoverOptions: {
      type: Object,
      default() {
        return {};
      },
    },
    dropDownWidth: {
      type: [String, Number],
      default: '',
    },
    showOnInit: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      value: '',
      scrollHeight: 'auto',
      searchVal: '',
      currentPage: 1,
      pageSize: 100,
      currentPageUpdating: false,
      listSlideName: 'toggle-slide',
      timerId: 0,
      bkPopoverInstance: null,
      bkPopoverTimerId: 0,
      popoverWidth: '100%',
      popInstances: [],
      tippyConfig: {
        trigger: 'manual',
        theme: 'light',
        arrow: true,
        placement: 'right',
        interactive: true,
        boundary: 'window',
      },
      canShowTippy: true,
    };
  },
  computed: {
    calcStyle() {
      return Object.assign({}, {
        'max-height': this.scrollHeight + 'px',
        width: this.popoverWidth
      }, this.customStyle);
    },
    calcHasChild() {
      return this.hasChildren === 'auto'
        ? this.list.some(item => item.children && Array.isArray(item.children))
        : this.hasChildren;
    },
    calcPlaceHolder() {
      return this.placeholder || this.$t('请选择');
    },
    isToolTipEnabled() {
      return this.optionTip;
    },
    calcSearchKey() {
      return this.searchable && (this.searchKey || this.displayKey);
    },
    searchedList() {
      if (!this.calcHasChild) {
        return this.autoSort
          ? this.orderListBySelectedValue().filter(item => this.stringToRegExp(this.searchVal, 'i')
            .test(item[this.calcSearchKey])
          )
          : this.list;
      } else {
        const cloneList = this.autoSort
          ? JSON.parse(JSON.stringify(this.orderListBySelectedValue()))
          : JSON.parse(JSON.stringify(this.list));
        return cloneList
          .map(item => Object.assign(item, {
            children: (item.children || []).filter(child => this.stringToRegExp(this.searchVal, 'ig')
              .test(child[this.calcSearchKey])
            ),
          })
          )
          .filter(
            node => this.stringToRegExp(this.searchVal, 'i').test(node[this.groupKey || this.calcSearchKey])
              || node.children.length
          );
      }
    },
    calcList() {
      if (!this.calcHasChild) {
        return this.searchedList.slice(0, this.currentPage * this.pageSize);
      } else {
        if (this.searchedList.length) {
          const cloneList = JSON.parse(JSON.stringify(this.searchedList));
          let nodeSign = { groupIndex: 0, nodeIndex: 0, counted: 0 };
          const lastIndex = this.currentPage * this.pageSize;
          cloneList.some((item, index) => {
            nodeSign.groupIndex = index;
            const childeLen = Array.isArray(item.children) ? item.children.length : 0;
            if (nodeSign.counted + childeLen >= lastIndex) {
              nodeSign.nodeIndex = lastIndex - nodeSign.counted;
              return true;
            } else {
              nodeSign.counted += childeLen;
              return false;
            }
          });

          let target = cloneList.slice(0, nodeSign.groupIndex + 1);
          if (nodeSign.nodeIndex) {
            target[nodeSign.groupIndex].children = target[nodeSign.groupIndex].children
              .slice(0, nodeSign.nodeIndex);
          }
          return target;
        } else {
          return this.searchedList;
        }
      }
    },
    calcPopoverWidth() {
      let calcWidth = 0;
      if ((this.customStyle && this.customStyle.width) || this.customStyle['min-width']) {
        const width = this.customStyle.width || this.customStyle['min-width'];
        if (/px$/.test(width)) {
          calcWidth = Number(width.replace(/px/gi, ''));
        } else if (/%$/.test(width)) {
          calcWidth = 0;
        } else {
          calcWidth = Number(width);
        }
      }

      return calcWidth;
    },
  },
  watch: {
    selected: {
      immediate: true,
      handler(val) {
        val !== this.value && this.$set(this, 'value', val === undefined || val === null ? '' : val);
      },
    },

    list(val) {
      this.currentPage = 1;
    },

    value(val, oldValue) {
      val !== oldValue && this.$emit('update:selected', val);

      // 当为空数组时，将selector.selectedNameCache设置为空字符串，才能清空select
      if (val && val.length === 0) {
        setTimeout(() => {
          this.$refs.selector.selectedNameCache = '';
        }, 0);
      }
    },
  },
  created() {
    this.tippyConfig = Object.assign(this.tippyConfig, this.toolTipConfig);
  },
  methods: {
    handleBottomClick() {
      this.customExtendCallback && typeof this.customExtendCallback === 'function' && this.customExtendCallback();
      this.$refs.selector.close();
    },
    /** 重新排序列表，提升已选择的条目位置 */
    orderListBySelectedValue() {
      if (this.selected !== null && this.selected !== undefined && this.selected !== '') {
        /** 有字目录时，做排序，保证已选择模块在第一页显示 */
        if (this.calcHasChild) {
          return this.list
            .map(item => {
              const orderItems = this.orderList(item.children);
              return Object.assign(item, {
                order: orderItems.isReOrdered ? 1 : 0,
                children: orderItems.newList,
              });
            })
            .sort((a, b) => b.order - a.order);
        } else {
          return this.orderList(this.list).newList;
        }
      } else {
        return this.list;
      }
    },

    /** 根据已选择的Value重新排序，保证已选择的条目在第一页就有，要不回填时显示会有问题 */
    orderList(list) {
      /** 筛选出已选择条目，包含只有一层，有Children，多选 */
      let compList = list.filter(item => {
        const itemValue = typeof item === 'string' ? item : item[this.settingKey];
        if (Array.isArray(this.selected)) {
          return this.selected.includes(itemValue);
        } else {
          return itemValue === this.selected;
        }
      });

      const hasSelectedItem = compList.length > 0;

      /** 筛选剩余条目 */
      const otherList = list.filter(other => {
        const isString = typeof other === 'string';
        return !compList.some(item => {
          const itemValue = isString ? item : item[this.settingKey];
          const otherValue = isString ? other : other[this.settingKey];
          return itemValue === otherValue;
        });
      });

      return {
        isReOrdered: hasSelectedItem,
        newList: compList.concat(otherList),
      };
    },
    stringToRegExp(pattern, flags) {
      return new RegExp(
        pattern.replace(/[\\[\]\\{}()+*?.$^|]/g, function (match) {
          return '\\' + match;
        }),
        flags
      );
    },
    handleEnter(e, option) {
      if (this.isToolTipEnabled && this.canShowTippy) {
        this.bkPopoverTimerId && clearTimeout(this.bkPopoverTimerId);
        this.bkPopoverTimerId = setTimeout(() => {
          const popInstance = tippy(e.target, {
            content: this.getCalcTip(option),
            ...this.tippyConfig,
          });
          this.fireInstance();
          popInstance && popInstance.show();
          this.popInstances.push(popInstance);
        }, 300);
      }
    },
    handleLeave() {
      if (this.isToolTipEnabled) {
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

    getCalcTip(option) {
      return (
        (typeof this.toolTipTpl === 'function' && this.toolTipTpl(option))
        || this.toolTipTpl
        || this.getDefaultTpl(option)
      );
    },
    getDefaultTpl(option) {
      return `${this.displayKey}:${option[this.displayKey]}`;
    },
    handleScrollCallback() {
      const pageCount = this.getTotalCount();
      if (pageCount > this.currentPage && !this.currentPageUpdating) {
        this.currentPageUpdating = true;
        this.currentPage++;
        this.$nextTick(() => {
          setTimeout(() => {
            this.currentPageUpdating = false;
          }, 300);
        });
      }
    },

    getTotalCount() {
      if (!this.hasChildren) {
        return Math.ceil(this.searchedList.length / this.pageSize);
      } else {
        const total = this.searchedList.reduce((pre, node) => {
          const childLen = Array.isArray(node.children) ? node.children.length : 0;
          pre += childLen + 1;
          return pre;
        }, 0);
        return Math.ceil(total / this.pageSize);
      }
    },

    remoteMethod(val) {
      this.timerId && clearTimeout(this.timerId);
      this.timerId = setTimeout(() => {
        this.searchVal = val;
        this.currentPage = 1;
      }, 300);
    },

    handleSelected(value, option) {
      this.canShowTippy = false;
      this.value = value;

      this.$nextTick(() => {
        let selectedItem = null;
        if (this.calcHasChild) {
          this.calcList.some(item => item.children.some(child => {
            if (child[this.settingKey] === value) {
              selectedItem = child;
              return true;
            } else {
              return false;
            }
          })
          );
        } else {
          selectedItem = this.calcList.find(item => item[this.settingKey] === value);
        }
        this.bkPopoverTimerId && clearTimeout(this.bkPopoverTimerId);
        this.fireInstance();
        this.$emit('item-selected', value, selectedItem, option);
        this.searchVal = '';
      });
    },

    handleToggle(isOpen) {
      if (!isOpen) {
        this.searchVal = '';
        this.fireInstance();
      } else {
        this.canShowTippy = true;
      }
      this.$emit('visible-toggle', isOpen);
    },

    handleClear(oldValue) {
      this.$emit('clear', oldValue);
    },

    handleChange(newValue, oldValue) {
      this.$emit('change', newValue, oldValue);
    },
  },
};
</script>
<style lang="scss" scoped>
.bkdata-select-popcontainer {
  position: absolute;
  min-width: 50px;
  min-height: 30px;
}

.select-option-wrapper {
  width: 100%;
  max-height: 216px;

  ::v-deep .bk-option {
    display: flex;
    &:last-child {
      margin-bottom: 0;
    }
    &.is-selected {
      .bk-option-content {
        background-color: #eaf3ff;
      }
    }
  }

  ::v-deep .bk-option-name {
    white-space: nowrap;
    display: inline-block;
  }
  ::v-deep .option-custom-icon-wrapper {
    width: 100%;
    text-overflow: ellipsis;
    white-space: nowrap;
    overflow: hidden;
    .option-custom-icon {
      display: inline-block;
      margin-right: 4px;
    }
  }
}

.bk-select {
  width: 100%;
}
</style>

<style lang="scss">
.bk-tooltip-content {
  .bk-select-dropdown-content {
    .bk-options {
      overflow-x: hidden;
    }
  }
}
</style>
