

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
  <div class="sql-function-wrapper">
    <div class="sql-header">
      <bkdata-select v-if="type === 'alModel'"
        v-model="modelName"
        extCls="sql-header-select"
        :clearable="false">
        <bkdata-option v-for="option in keyList"
          :id="option"
          :key="option"
          :name="option" />
      </bkdata-select>
      <bkdata-input
        v-model="searchKey"
        :placeholder="$t('请输入搜索关键词')"
        :rightIcon="'bk-icon icon-search'"
        style="margin-bottom: 10px"
        :extCls="inputClass" />
    </div>
    <div v-bkloading="{ isLoading: loading }"
      class="sql-content bk-scroll-y">
      <bkdata-collapse v-model="activeSQLClass">
        <bkdata-collapse-item v-for="(sql, index) in getSQLList"
          :key="index"
          :name="sql.group"
          :hideArrow="true">
          <i :class="['icon-item', getIconClass(sql.group)]" /><span class="title">{{ sql.group }}</span>
          <div slot="content">
            <ul class="sql-list">
              <li
                v-for="(func, idx) in sql.content"
                :key="idx"
                v-bk-overflow-tips
                :class="{ 'active-func': currentFunc.name === func.name }"
                @click="setCurrentFunc(func)">
                {{ func.name }}
              </li>
            </ul>
          </div>
        </bkdata-collapse-item>
      </bkdata-collapse>
    </div>

    <!-- func详细信息 -->
    <div class="func-detail"
      :class="{ 'panel-close': !activeCurrentFunc }">
      <div class="func-header">
        <span class="func-name">{{ currentFunc.name }}</span>
        <span class="icon-close"
          @click="closeDetail" />
        <div v-if="type === 'alModel'"
          class="clip-board"
          @click="copySQL(currentFunc)">
          <span class="icon-copy" />
          <span class="copy-text">默认SQL</span>
        </div>
      </div>
      <div class="func-content bk-scroll-y">
        <div v-for="(info, index) in infoMap"
          :key="index"
          class="info-item">
          <span class="title">{{ info.title }}</span>：
          <span class="content">{{ currentFunc[info.key] }}</span>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import { bkOverflowTips } from 'bk-magic-vue';
import { clipboardCopy, showMsg } from '@/common/js/util.js';
export default {
  directives: {
    bkOverflowTips,
  },
  props: {
    type: {
      type: String,
      default: '',
    },
    // eslint-disable-next-line vue/require-default-prop
    keyList: {
      type: Array,
      defatul: () => [],
    },
    infoMap: {
      type: Array,
      default: () => [],
    },
    list: {
      type: Array,
      default: () => [],
    },
    loading: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      modelName: '',
      searchKey: '',
      activeSQLClass: ['字符串'],
      currentFunc: {},
      activeCurrentFunc: false,
    };
  },
  computed: {
    inputClass() {
      return this.type === 'alModel' ? 'alModel' : 'normal';
    },
    getSQLList() {
      const key = this.searchKey.toLowerCase();
      /** 如果没有关键词，直接返回sqllist */
      // eslint-disable-next-line vue/no-side-effects-in-computed-properties
      this.activeSQLClass = [];
      if (key === '') {
        return this.list;
      }

      let list = JSON.parse(JSON.stringify(this.list));
      list = list.filter(group => {
        group.content = group.content.filter(func => {
          return func.name.toLowerCase().includes(key);
        });
        if (group.content.length) {
          this.activeSQLClass.push(group.group);
        }
        return group.content.length;
      });
      return list;
    },
  },
  watch: {
    modelName(val) {
      this.$emit('changeModelName', val);
    },
    keyList(val) {
      if (val.length && !this.modelName) {
        this.modelName = val[0];
      }
    },
  },
  methods: {
    copySQL(item) {
      const sql = item.sql;
      clipboardCopy(sql, showMsg('已复制默认SQL到剪贴板', 'success'));
    },
    getIconClass(group) {
      return this.activeSQLClass.includes(group) ? 'icon-down-shape' : 'icon-right-shape';
    },
    setCurrentFunc(func) {
      this.$set(this, 'currentFunc', func);
      this.activeCurrentFunc = true;
    },
    closeDetail() {
      this.activeCurrentFunc = false;
      setTimeout(() => {
        this.currentFunc = {};
      }, 700);
    },
  },
};
</script>

.
<style lang="scss" scoped>
.sql-function-wrapper {
  display: flex;
  flex-direction: column;
  justify-content: flex-end;
  padding-top: 16px;
  height: 100%;
  .sql-header {
    padding: 0 16px;
    font-size: 0;
    .sql-header-select {
      width: 110px;
      display: inline-block;
      vertical-align: top;
      border-top-right-radius: 0;
      border-bottom-right-radius: 0;
    }
    ::v-deep .alModel {
      width: 177px;
      display: inline-block;
      input {
        border-top-left-radius: 0;
        border-bottom-left-radius: 0;
        border-left: none;
        &:focus {
          border-left: 1px solid #3a84ff;
        }
      }
    }
  }
  ::v-deep .sql-content {
    flex: 1 1;
    .bk-collapse-item-header {
      padding: 0 16px;
      height: 28px;
      line-height: 28px;
      width: 100%;
      &:hover {
        background-color: #eaf3ff;
        .icon-item {
          color: #3a84ff;
        }
      }
      .title {
        font-size: 12px;
        color: #313238;
      }
      .icon-item {
        position: relative;
        top: -1px;
        margin-right: 4px;
        color: #979ba5;
        font-size: 12px;
      }
    }
    .bk-collapse-item-content {
      padding: 0;
      .sql-list {
        font-size: 12px;
        li {
          width: 100%;
          padding: 0 33px;
          height: 28px;
          line-height: 28px;
          cursor: pointer;
          color: #63656e;
          overflow: hidden;
          white-space: nowrap;
          text-overflow: ellipsis;
          &:hover {
            background-color: #eaf3ff;
            color: #3a84ff;
          }
        }
        .active-func {
          background-color: #f4f6fa;
          color: #3a84ff;
        }
      }
    }
  }
  .func-detail {
    height: 414px;
    width: 100%;
    transition: height 0.7s ease-in-out;
    box-shadow: 0px -3px 4px -1px #dcdee5;
    &.panel-close {
      height: 0;
    }
    .func-header {
      position: relative;
      height: 44px;
      border-bottom: 1px solid #dcdee5;
      padding: 0 16px;
      line-height: 44px;
      .func-name {
        color: #313238;
        display: inline-block;
        width: 198px;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
      }
      .icon-close {
        position: absolute;
        font-size: 18px;
        cursor: pointer;
        right: 13px;
        top: 50%;
        transform: translateY(-50%);
        color: #979ba5;
      }
      .clip-board {
        position: absolute;
        top: 0;
        right: 42px;
        font-size: 12px;
        cursor: pointer;
        color: #3a84ff;
      }
      .clip-board:hover {
        color: #699df4;
      }
    }
    .func-content {
      padding: 16px;
      height: calc(100% - 44px);
      .info-item {
        line-height: 20px;
        font-size: 12px;
        margin-bottom: 12px;
        color: #b2b5bd;
        .title {
          display: inline-block;
          width: 48px;
        }
        .content {
          color: #63656e;
          display: inline-block;
          width: calc(100% - 64px);
          vertical-align: top;
          white-space: pre-line;
        }
      }
    }
  }
}
</style>
