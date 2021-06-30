

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
  <div class="tree tree-parens">
    <div
      :id="model.node_id"
      :outline-id="model.id"
      class="tree-item"
      @click="toggle"
      @mouseenter="showTip(model.node_id, model, 'right')"
      @mouseleave="hidden()">
      <div @click="nodeFocus(model)"
        @dblclick="setConfig(model)">
        <span :class="{ parant: model.types === 'parant', child: model.types === 'child' }"
          class="icon">
          <span
            :class="{
              'icon-container': isFolder || isDynamicFolder,
              'child-icon': model.types === 'child',
            }">
            <i v-if="model.types === 'parant'"
              :class="[isFolder ? folderIcon : 'file-text']" />
            <i v-else
              :class="['bk-icon', model['iconName'], 'child-icon']" />
          </span>
          {{ model.menuName }}
          <span v-show="model.types === 'parant'"
            class="fr node-count">
            {{ model.children ? model.children.length : 0 }}
          </span>
        </span>
      </div>
    </div>
    <div v-if="open">
      <tree-menu v-for="item in model.children"
        :key="item.id"
        :model="item" />
    </div>
  </div>
</template>

<script>
import Bus from '@/common/js/bus.js';
import $ from 'jquery';
export default {
  name: 'treeMenu',
  props: {
    model: Object,
    subMenuData: Object,
    isOpen: {
      type: Boolean,
      default() {
        return false;
      },
    },
  },
  data() {
    return {
      folderIcon: 'bk-icon icon-plus-square', // 折叠icon
      isDynamicFolder: false, // 是否动态插入内容
      isFolder: true,
      open: this.isOpen,
      tooltipsMatchs: [
        { reg: /^etl_source/i, name: 'template1' },
        { reg: /^rtsource/i, name: 'template2' },
        { reg: /^stream_source/i, name: 'template2' },
        { reg: /^clean/i, name: 'template3' },
        { reg: /^realtime/i, name: 'template3' },
        { reg: /^batch_source/i, name: 'template3' },
        { reg: /^offline/i, name: 'template3' },
        { reg: /^algorithm_model/i, name: 'template4' },
        { reg: /^mysql/i, name: 'template5' },
        { reg: /^hdfs_storage/i, name: 'template5' },
        { reg: /^elastic_storage/i, name: 'template5' },
        { reg: /^druid/i, name: 'template5' },
        { reg: /^tspider_storage/i, name: 'template5' },
        { reg: /^queue_storage/i, name: 'template5' },
        { reg: /^hermes_storage/i, name: 'template5' },
        { reg: /^tredis_storage/i, name: 'template5' },
        { reg: /^tsdb_storage/i, name: 'template5' },
      ],
    };
  },
  watch: {
    isDynamicFolder() {
      this.open = true;
      this.folderIcon = 'bk-icon icon-minus-square';
    },
  },
  mounted() {
    this.$nextTick(() => {
      let lastParent = document.querySelectorAll('.tree-parens:last-of-type')[0];
      if (lastParent) {
        let child = lastParent.querySelectorAll('.child');
        if (child.length > 1) {
          lastParent.querySelectorAll('.child:last-of-type')[0].classList.add('bd0');
        }
      }
    });
  },
  methods: {
    getIconNameFirstChar(name) {
      return (name && name.substring(0, 1).toUpperCase() + name.substring(1, 2).toLocaleLowerCase()) || '';
    },
    // 显示
    showTip(id, model, dir) {
      if (typeof id !== 'number') return;
      let content = '';
      let template = this.tooltipsMatchs.find(match => match.reg.test(model.instanceTypeName));
      switch (template.name) {
        case 'template1':
          let name = this.$t('清洗数据');
          content = `<div class='tip-box'>
                        <div class='tip-left mr5'>
                            <div>${name}</div>
                        </div>
                        <div class='tip-right'>
                            <div>${model.node_name || model.name}</div>
                        </div>
                    </div>`;
          break;
        case 'template2':
          let rtid = this.$t('结果数据表');
          let rtname = this.$t('中文名称');
          content = `<div class='tip-box'>
                        <div class='tip-left mr5'>
                            <div>${rtid}</div>
                            <div>${rtname}</div>
                        </div>
                        <div class='tip-right'>
                            <div>${model.node_name || model.name}</div>
                            <div>${model.output_description}</div>
                        </div>
                    </div>`;
          break;
        case 'template3':
          let cchName = this.$t('节点名称');
          let outputLabel = this.$t('数据输出');
          let outputNameLabel = this.$t('输出中文名');
          content = `<div class='tip-box'>
                        <div class='tip-left mr5'>
                            <div>${cchName}</div>
                            <div>${outputLabel}</div>
                            <div>${outputNameLabel}</div>
                        </div>
                        <div class='tip-right'>
                            <div>${model.config.name}</div>
                            <div>${model.output || model.config.output_name}</div>
                            <div>${model.output_description}</div>
                        </div>
                    </div>`;
          break;
        case 'template4':
          let algoNodeName = this.$t('节点名称');
          let algoOutputLabel = this.$t('数据输出');
          let algostatus = this.$t('运行状态');
          content = `<div class='tip-box'>
                        <div class='tip-left mr5'>
                            <div>${algoNodeName}</div>
                            <div>${algoOutputLabel}</div>
                            <div>${algostatus}</div>
                        </div>
                        <div class='tip-right'>
                            <div>${model.config.name}</div>
                            <div>${model.output || model.node_config.output_name}</div>
                            <div>${model.status_display}</div>
                        </div>
                    </div>`;
          break;
        case 'template5':
          let sname = this.$t('结果数据表');
          let schName = this.$t('节点名称');
          content = `<div class='tip-box'>
                        <div class='tip-left mr5'>
                            <div>${schName}</div>
                            <div>${sname}</div>
                        </div>
                        <div class='tip-right'>
                            <div>${model.node_name || model.name}</div>
                            <div>${(model.output && `${model.output}(${model.output_description})`)
                              || model.output_description} </div>
                        </div>
                    </div>`;
          break;
      }
      this.toolTip(id, content, dir, false, 0);
    },
    // 隐藏
    hidden() {
      $('.def-tooltip').remove();
    },
    // 双击子节点
    setConfig(model) {
      if (model.hasOwnProperty('children')) return;
      Bus.$emit('setConfig', model);
    },
    // 单击子节点
    nodeFocus(model) {
      if (model.hasOwnProperty('children')) return;
      model.isActive = !model.isActive;
      Bus.$emit('focusNode', model);
    },
    // 动态插入内容，需要设置submenudata
    toggle() {
      const menuData = this.model;
      const subMenuData = this.subMenuData;

      if (subMenuData && menuData.id === subMenuData.parentId && subMenuData.list && !menuData.children) {
        menuData.children = subMenuData.list;
        this.isDynamicFolder = !!(menuData.children && menuData.children.length);
        this.open = true;
        this.folderIcon = 'bk-icon icon-minus-square';
      }
      if (this.isFolder || this.isDynamicFolder) {
        this.open = !this.open;
        this.folderIcon = this.open ? 'bk-icon icon-minus-square' : 'bk-icon icon-plus-square';
      }
    },
  },
};
</script>

<style lang="scss">
.tree {
  position: relative;
  &::after {
    content: '';
    position: absolute;
    border-left: 1px dashed #7c828f;
    left: 22px;
    height: 10px;
    top: 20px;
  }
  &::before {
    content: '';
    position: absolute;
    border-left: 1px dashed #7c828f;
    left: 22px;
    height: 5px;
    top: 0px;
  }
  &:first-child {
    &::before {
      content: '';
      position: absolute;
      border: none;
    }
  }
  &:last-child {
    &::after {
      content: '';
      position: absolute;
      border-left: 1px dashed #7c828f;
      left: 22px;
      height: 7px;
      top: 0px;
    }
    &::before {
      content: '';
      position: absolute;
      border: none;
    }
    .tree {
      &:last-child {
        &::after {
          content: '';
          position: absolute;
          /* border: none; */
          top: -12px;
        }
        &::before {
          content: '';
          position: absolute;
          border-left: 1px dashed #7c828f;
          left: 22px;
          height: 14px;
          top: 0px;
        }
      }
      &.bd0 {
        &::after {
          border: none;
        }
      }
      &:first-child {
        &::before {
          content: '';
          position: absolute;
          border-left: 1px dashed #7c828f;
          left: 22px;
          height: 9px;
          top: -12px;
        }
      }
    }
  }

  .tree {
    .icon-container {
      margin-right: 5px;
    }
    &::after {
      content: '';
      position: absolute;
      border-left: 1px dashed #7c828f;
      left: 22px;
      height: calc(100%);
      top: 0px;
    }
  }
  /* 设置滚动条的样式 */
  &::-webkit-scrollbar {
    width: 5px;
    background-color: #f5f5f5;
  }

  /* 滚动槽 */
  &::-webkit-scrollbar-track {
    box-shadow: inset 0 0 6px #eee;
    border-radius: 10px;
    background-color: #f5f5f5;
  }

  /* 滚动条滑块 */
  &::-webkit-scrollbar-thumb {
    border-radius: 10px;
    box-shadow: inset 0 0 6px #eee;
    background-color: rgba(0, 0, 0, 0.3);
  }
  &::-webkit-scrollbar-thumb {
    border-radius: 10px;
    box-shadow: inset 0 0 6px #eee;
    background-color: rgba(0, 0, 0, 0.3);
  }
  &-item {
    margin: 3px 0px;
    height: 30px;
    line-height: 30px;
    text-align: left;
    div {
      padding: 0 15px;
      cursor: pointer;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    &:hover {
      background: #eeeeee;
    }
    .active {
      background: #e1ecff;
      .child-icon {
        i {
          color: #3a84ff;
        }
      }
      .icon {
        color: #3a84ff;
      }
      .node-count {
        background: #3a84ff;
      }
    }
    &:hover .icon-container {
      // border: 1px solid #3a84ff;
      i {
        color: #3a84ff;
      }
    }
    &:hover .child-icon {
      border: 0;
      i {
        color: #3a84ff;
      }
    }

    &:hover .icon {
      color: #3a84ff;
    }
    &:hover .node-count {
      background: #3a84ff;
    }
  }

  span.icon {
    position: relative;
  }
  .parant {
    font-weight: bold;
  }
  .child {
    padding-left: 30px;
    font-size: 12px;
    color: rgb(116, 122, 136);
    &::after {
      content: '';
      border-top: 1px dashed #7c828f;
      position: absolute;
      top: 9px;
      width: 20px;
      height: 1px;
      left: 8px;
    }
  }
  .icon-container {
    // border: 1px solid #747a88;
    display: inline-block;
    width: 15px;
    height: 15px;
    border-radius: 1px;
    margin-right: 10px;
  }
  .tree-item {
    &:hover {
      .icon-container {
        .bk-icon-ABC {
          &::before {
            background: #eee;
          }
          &::after {
            background: #eee;
          }
        }
      }
    }

    .icon-container {
      &.child-icon {
        margin-right: 0;
        height: auto;
        width: auto;
      }
      .bk-icon-ABC {
        position: relative;
        min-width: 20px;
        display: inline-block;
        min-height: 20px;

        &::before {
          z-index: 1;
          position: absolute;
          background: #fafafa;
          display: flex;
          top: 0;
          left: 0;
          width: 100%;
          height: 100%;
          justify-content: flex-start;
          align-items: center;
        }
        &::after {
          content: attr(data-icon-r);
          z-index: 0;
          position: absolute;
          top: 50%;
          left: 50%;
          transform: translate(-50%, -50%);
          font-size: 14px;
        }
      }
    }
  }

  .child-icon {
    border: 0;
    font-size: 19px;
    top: 0px;
  }
  .node-count {
    background: #737987;
    border-radius: 2px;
    font-weight: normal;
    position: relative;
    top: 8px;
    display: inline-block;
    width: 23px;
    height: 15px;
    line-height: 15px;
    color: #fff;
    text-align: center;
  }
  i {
    display: inline-block;
    position: relative;
    top: -2px;
    left: -2px;
    transform: scale(0.8);
    font-size: 19px;
    color: #747a88;
    background-repeat: no-repeat;
    vertical-align: middle;
  }
}
body .tip-box {
  display: flex;
  .tip-left {
    text-align: right;
    color: #b2bac0;
    min-width: 100px;
    div {
      padding: 5px;
    }
  }
  .tip-right {
    text-align: left;
    color: #fff;
    div {
      padding: 5px 5px 5px 0px;
    }
  }
}
</style>
