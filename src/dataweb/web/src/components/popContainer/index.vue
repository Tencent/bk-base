

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
  <transition name="fade">
    <div v-show="instanceShow"
      class="pop-container">
      <slot />
    </div>
  </transition>
</template>
<script>
import tippy from 'tippy.js';
export default {
  name: 'popContainer',
  data() {
    return {
      popInstances: [],
      bkPopoverTimerId: 0,
      instanceShow: false,
    };
  },
  methods: {
    handlePopShowByTarget(target, option, delay = 300) {
      const defauoltOpt = {
        appendTo: document.body,
        content: this.$el,
        trigger: 'manual',
        theme: 'light',
        arrow: true,
        placement: 'left',
        boundary: 'window',
        interactive: true,
        hideOnClick: true,
        maxWidth: 400,
        onHidden: this.emitHiddenEvent,
        sticky: false,
        zIndex: 9999,
      };
      const options = Object.assign({}, defauoltOpt, option);
      this.bkPopoverTimerId && clearTimeout(this.bkPopoverTimerId);
      this.bkPopoverTimerId = setTimeout(() => {
        this.fireInstance();
        this.instanceShow = true;
        const popInstance = tippy(target, options);
        popInstance && popInstance.show();
        this.popInstances.push(popInstance);
      }, delay);
    },

    handlePopShow(e, option) {
      this.handlePopShowByTarget(e.target, option);
    },

    handlePopHidden(duration = 0) {
      this.fireInstance(duration);
    },

    handleUpdateOptions(options) {},

    emitHiddenEvent() {
      this.$emit('popHidden');
    },

    fireInstance(duration = 0) {
      let isContinue = true;
      this.instanceShow = false;
      while (isContinue) {
        const instance = this.popInstances[0];
        if (this.popInstances.length === 1) {
          instance && instance.hide(duration);
          isContinue = false;
          setTimeout(() => {
            instance && instance.destroy();
            this.popInstances.shift();
          }, duration + 10);
        } else {
          instance && instance.destroy();
          this.popInstances.shift();
          isContinue = this.popInstances.length;
        }
      }
    },
  },
};
</script>
<style lang="scss" scoped></style>
