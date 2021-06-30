

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
    <div :class="['tree-li', { 'no-released': model.level === 3 && !model.released }]">
      <div v-bk-tooltips="getContent(model)"
        :class="['tree-content', { bold: isFolder }]"
        @click="toggle(model)">
        <span v-if="isFolder"
          :class="['bk-icon icon-right-shape', { turn: model.open }]" />
        <!-- 删除UDF的icon -->
        <!-- <span v-show="model.level === 3"
                    class=" bk-icon icon-udf function"></span> -->
        <p class="model-name">
          {{ model.name }}
        </p>
        <div v-if="model.type_name === 'udf'"
          class="editGroup">
          <span v-if="model.editable"
            class="icon-edit2"
            @click="editUDF(model.name, openUDFSlider)" />
          <span class="icon-user"
            @click="getPermission(model.name)" />
        </div>
      </div>
    </div>
    <div v-show="model.open"
      v-if="isFolder"
      class="tree-child">
      <bk-tree v-for="item in model.children"
        :key="item.name"
        class="item"
        :search="search"
        :model="item" />
      <!-- <li class="add" @click="addChild">+</li> -->
    </div>
  </div>
</template>
<script>
import udfMethods from './udfMixin';
import { mapGetters } from 'vuex';
import { showMsg, postMethodWarning, confirmMsg } from '@/common/js/util.js';
import Bus from '@/common/js/bus.js';
// import bkTree from '@/components/tree/tree.vue';
export default {
  name: 'bk-tree',
  components: {
    // bkTree,
  },
  mixins: [udfMethods],
  props: {
    model: {
      type: Object,
    },
    search: {
      type: String,
    },
  },
  data: function () {
    return {
      tips: '',
    };
  },
  computed: {
    ...mapGetters({
      devlopParams: 'udf/devlopParams',
    }),
    isFolder: function () {
      return this.model.children && this.model.children.length;
    },
  },
  watch: {
    search: function (newVal) {
      if (newVal.length > 0) {
        // eslint-disable-next-line vue/no-mutating-props
        this.model.open = true;
      } else {
        // this.model.open = false
      }
    },
  },
  methods: {
    openUDFSlider(data) {
      this.$store.dispatch('udf/editUDFStatusSet', data);
      Bus.$emit('openUDFSlider'); // 打开UDF
    },
    getPermission(name) {
      Bus.$emit('getUDFAuth', name);
    },
    getContent(model) {
      if (model && ((model.display && model.display.length > 0) || model.level === 3)) {
        return {
          placement: 'right',
          html: this.tooltip(model),
          boundary: 'window',
        };
      }
    },
    // 点击展开
    toggle: function (model) {
      if (this.isFolder) {
        // eslint-disable-next-line vue/no-mutating-props
        this.model.open = !this.model.open;
      }
    },
    addChild: function () {
      // eslint-disable-next-line vue/no-mutating-props
      this.model.children.push({
        name: 'new stuff',
      });
    },
    tooltip(data) {
      let string = '';
      data = data || {};
      if (data.level === 3 && data.released) {
        string = `<div class="tooltip-content" style="width: 310px; word-break: break-all">
                        <div class="clearfix tooltip-line"><span class="text-title fl">${this.$t(
          '函数名称'
        )}: </span> <p class="text fl" title="${data.name}">${data.name}</p></div>
                        <div class="clearfix tooltip-line"><span class="text-title fl">${this.$t(
          '函数说明'
        )}: </span> <p class="text fl">${data.explain}</p></div>
                        <div class="clearfix tooltip-line">
                            <span class="text-title fl">${this.$t('使用样例')}: </span>
                            <div class="fl text">
                                <p>${data.usage_case}</p>
                            </div>
                        </div>
                        <div class="clearfix tooltip-line"><span class="text-title fl">${this.$t(
          '样例返回'
        )}: </span> <p class="text fl">${data.example_return_value}</p></div>
                        <div class="clearfix tooltip-line"><span class="text-title fl">${this.$t('函数用法')}: </span>`;
        data.usages.forEach(element => {
          string += `<p class="text desp-text fl" title="${element}">${element}</p>`;
        });
        string += '</div>';
        string += data.support_framework
          ? `<div class="clearfix tooltip-line"><span class="text-title fl">${this.$t(
            '计算类型'
          )}: </span> <p class="text fl" title="${data.support_framework}">${data.support_framework}</p></div></div>`
          : '</div>';
      } else {
        string = `<div class="tooltip-content">${data.display || this.$t('未发布')}</div>`;
      }
      return string;
    },
  },
};
</script>
<style lang="scss">
.tree-li {
  height: 32px;
  line-height: 32px;
  padding: 0 0px 0 32px;
  .tree-content {
    position: relative;
    outline: none;
  }
  .bk-icon {
    font-size: 13px;
    position: absolute;
    left: -20px;
    top: 7px;
    transition: all 0.1s ease 0s;
  }
  .function {
    font-size: 15px;
  }
  .turn {
    transform: rotate(45deg);
  }
  &:hover {
    background: #e1ecff;
    color: #3a84ff;
    .editGroup {
      display: inline-block;
    }
  }
  .model-name {
    max-width: calc(100% - 60px);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .editGroup {
    display: none;
    position: absolute;
    left: 170px;
    font-size: 15px;
    top: 0px;
    cursor: pointer;
    span {
      position: relative;
      top: 0;
      margin-right: 5px;
      transition: all 0.1s linear;
      &:hover {
        top: -1px;
      }
    }
  }
}
.no-released {
  color: rgba(196, 198, 204, 1);
}
.tree-child {
  padding-left: 20px;
}
.tooltip-content {
  font-size: 12px;
  .text-title {
    display: inline-block;
    color: #b2bac0;
    width: 60px;
    text-align: right;
    margin-right: 5px;
  }
  .title {
    font-weight: 700;
    margin-bottom: 5px;
  }
  .text {
    width: calc(100% - 65px);
  }
  .usage {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .tooltip-line {
    line-height: 18px;
    margin-bottom: 5px;
    .desp-text:not(:first-of-type) {
      margin-left: 55px;
    }
  }
  .tree-sample {
    text-align: right;
    float: left;
    p + p {
      border-top: 1px dashed #e3e3e5;
    }
  }
}
</style>
