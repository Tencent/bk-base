

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
  <div :class="['path-wrap', (isDisabled && 'is-disabled') || '']">
    <div class="path-type"
      @click="handleClick">
      <i :class="[`bk-icon bk-icon-20 icon-${pickedSys.icon}`]" />
      <i v-if="!isDisabled"
        class="bk-icon icon-down-shape" />
    </div>
    <div v-show="dropShow"
      class="path-list">
      <ul>
        <li v-for="(sys, index) of systemList"
          :key="index"
          @click="systemSelected(sys)">
          <i :class="[`bk-icon icon-${sys.icon}`]" />
        </li>
      </ul>
    </div>
  </div>
</template>
<script>
export default {
  props: {
    selectedItem: {
      type: String,
      default: 'linux',
    },
    isDisabled: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      dropShow: false,
      defaultPickedSys: {
        name: 'linux',
        value: 'linux',
        icon: 'linux',
        placeHolder: `${window.$t('请填写Linux文件路径_如')}：/data/log/nginx.log`,
      },
      systemList: [
        {
          name: 'windows',
          value: 'windows',
          icon: 'win',
          placeHolder: `${window.$t('请填写Windows文件路径_如')}：c:\\data\\nginx.log`,
        },
        {
          name: 'linux',
          value: 'linux',
          icon: 'linux',
          placeHolder: `${window.$t('请填写Linux文件路径_如')}：/data/log/nginx.log`,
        },
        // {
        //     value: 'apple',
        //     name: 'apple',
        //     icon: 'apple',
        //     placeHolder: '请填写Linux文件路径，如：/data/log/nginx.log'
        // }
      ],
    };
  },
  computed: {
    pickedSys() {
      return this.systemList.find(sys => sys.value === this.selectedItem) || this.defaultPickedSys;
    },
  },
  mounted() {
    this.$emit('systemChecked', Object.assign(this.defaultPickedSys, { isMounted: true }));
  },
  methods: {
    handleClick() {
      if (!this.isDisabled) {
        this.dropShow = !this.dropShow;
      }
    },
    systemSelected(sys) {
      // this.pickedSys = sys
      this.dropShow = !this.dropShow;
      this.$emit('systemChecked', sys);
    },
  },
};
</script>
<style lang="scss" scoped>
.path-wrap {
  position: relative;

  &.is-disabled {
    .path-type {
      cursor: auto;
      justify-content: center;
    }
  }
}
.path-type {
  border: 1px solid #c3cdd7;
  border-right: 0;
  width: 40px;
  height: 32px;
  padding: 0 5px 0 0;
  line-height: 36px;
  background: #fff;
  display: flex;
  font-size: 16px;
  align-items: center;
  justify-content: space-between;
  padding-left: 2px;
  //   border-right: 0;
  background: #f8f8f8;
  cursor: pointer;

  i {
    // padding: 0 2px;
    font-size: 0.1em;
    &.bk-icon-20 {
      font-size: 20px;
    }
  }
}
.path-list {
  width: 100%;
  position: absolute;
  top: 36px;
  background: #fff;
  z-index: 1;
  border: solid 1px #eee;
  li {
    line-height: 42px;
    font-size: 14px;
    text-align: center;
    &:hover {
      background-color: #eef6fe;
      color: #3c96ff;
      cursor: pointer;
    }

    i {
      padding: 0 2px;
      font-size: 20px;
    }
  }
}
</style>
