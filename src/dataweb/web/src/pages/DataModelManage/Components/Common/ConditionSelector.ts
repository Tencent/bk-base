/*
 * Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
 * BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
 *
 * License for BK-BASE 蓝鲸基础平台:
 * --------------------------------------------------------------------
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
 * and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
 * LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 * NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE
 */

import { Vue, Component, Prop, Ref, Watch, Emit } from 'vue-property-decorator';
import { VNode } from 'vue/types/umd';
import popContainer from '@/components/popContainer';
import BkConditionSelector from '@/components/conditionSelector/ConditionSelector.vue';
import { generateId, debounce } from '@/common/js/util';

@Component({
  components: {
    popContainer,
    BkConditionSelector,
  },
})
export default class ConditionSelector extends Vue {
  @Prop({ default: () => ({}) }) public constraintContent!: object;
  @Prop({ default: () => [] }) public constraintList!: Array<any>;
  @Prop({ default: false }) public readonly!: boolean;
  @Prop({ default: '' }) public fieldDisplayName!: String;

  @Ref() public readonly conditionSelector!: VNode;
  @Ref() public readonly conditionPopEl!: HTMLDivElement;

  @Watch('constraintContent', { immediate: true, deep: true })
  handleConstraintContent() {
    this.constraintContent && this.formatRenderData();
    this.resetInputEllipsis();
  }

  @Emit('change')
  handleContentChange(content: object | null) {
    return content;
  }

  get conditionMap() {
    const map: any = {};
    for (const condition of this.constraintList) {
      map[condition.constraintId] = condition;
      if (condition.children) {
        for (const child of condition.children) {
          child.parentId = condition.constraintId;
          map[child.constraintId] = child;
        }
      }
    }
    return map;
  }

  public conditionInputId: string = generateId('ConditionInputId_');

  public conditionSelectorInput: Element = {};

  public isShowInputEllipsis = false;

  public renderInputContent = [];

  public isShow = false;

  public observer = null;

  public inputObserver = null;

  public debounceReset = () => { };

  public hasScrollbar = false;

  public conditionPop = {
    isShow: false,
    instance: null,
  };

  public resetInputEllipsis() {
    if (this.renderInputContent.length) {
      // 使用 setTimeout 确保取到正确的 wrapperWidth
      const timer = setTimeout(() => {
        const style = window.getComputedStyle(this.conditionSelectorInput, null);
        const wrapperWidth = parseInt(style.width);
        const padding = 10;
        let totalWidth = 0;
        const nodes = Array.from(this.conditionSelectorInput.childNodes).filter(
          node => node.className && node.className.includes('input-condition-group')
        );
        for (const node of nodes) {
          totalWidth += node.offsetWidth || 0;
        }
        this.isShowInputEllipsis = wrapperWidth - padding <= totalWidth;
        clearTimeout(timer);
      }, 100);
    }
  }

  public formatRenderData() {
    this.renderInputContent = [];
    const { op: groupCondition, groups } = this.constraintContent;
    if (groups) {
      for (const group of groups) {
        const itemOp = group.op;
        const groupItem = {
          op: groupCondition,
          items: [],
        };
        for (const item of group.items) {
          const condition = this.conditionMap[item.constraintId];
          const name = condition?.constraintName;
          if (name) {
            const content = condition.editable ? item.constraintContent : '';
            groupItem.items.push({
              op: itemOp,
              content: name + ' ' + content,
            });
          }
        }
        !group.isEmpty && this.renderInputContent.push(groupItem);
      }
    }
  }

  public handleShowCondition() {
    if (this.readonly) {
      this.conditionPop.instance && this.conditionPop.instance.destroy();
      this.conditionPop.instance = this.$bkPopover(this.conditionSelectorInput, {
        content: this.conditionPopEl,
        theme: 'light',
        zIndex: 9999,
        trigger: 'manual',
        boundary: 'window',
        arrow: true,
        interactive: true,
        width: 556,
        distance: 20,
        // extCls: 'bk-data-model-confirm',
        // placement: 'bottom'
      });
      this.conditionPop.isShow = true;
      this.$nextTick(() => {
        this.conditionPop.instance.show();
      });
    } else {
      this.isShow = true;
    }
    this.$emit('click');
  }

  public handleHideConditionPop() {
    this.conditionPop.instance && this.conditionPop.instance.hide(0);
    this.$nextTick(() => {
      this.conditionPop.isShow = false;
    });
  }

  public handleClear() {
    this.renderInputContent = [];
    this.isShowInputEllipsis = false;
    this.handleContentChange(null);
  }

  public handleSubmit() {
    this.conditionSelector.handleSubmitCondition().then(data => {
      this.handleContentChange(data);
      this.isShow = false;
    });
  }

  public handleSidesliderShow() {
    const self = this;
    const targetNode = this.conditionSelector.$el;
    this.observer = new ResizeObserver(entries => {
      self.hasScrollbar = targetNode.scrollHeight !== targetNode.offsetHeight;
    });
    this.observer.observe(targetNode);
  }

  public handleSidesliderHidden() {
    if (this.observer) {
      this.observer.disconnect();
      this.observer = null;
    }
  }

  // input 宽度改变重新计算是否需要展示省略号
  public handleInputResize() {
    const self = this;
    this.inputObserver = new ResizeObserver(entries => self.debounceReset());
    this.inputObserver.observe(this.conditionSelectorInput);
  }

  public mounted() {
    this.conditionSelectorInput = this.$refs[this.conditionInputId];
    this.debounceReset = debounce(this.resetInputEllipsis, 0);
    this.handleInputResize();
  }

  public beforeDestroy() {
    if (this.observer) {
      this.observer.disconnect();
      this.observer = null;
    }
    if (this.inputObserver) {
      this.inputObserver.disconnect();
      this.inputObserver = null;
    }
  }
}
