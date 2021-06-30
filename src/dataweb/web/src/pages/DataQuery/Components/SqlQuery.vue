

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
  <div class="sqlQuery">
    <div class="resultsData">
      <div v-if="showTitle"
        class="clearfix titel-wraper">
        <p class="fl title">
          {{ $t('结果数据表') }}：
          <span>{{ resultTable }}</span>
        </p>
      </div>
      <div v-if="!hideNote"
        class="text-note">
        <i class="bk-icon icon-exclamation-circle-shape" />
        <i18n path="如果想调用API来读取数据_请在权限管理中申请API权限"
          tag="span">
          <router-link :to="{ path: '/auth-center/token-management' }"
            class="nav-item"
            place="book"
            tag="a">
            {{ $t('权限管理') }}
          </router-link>
        </i18n>
        <span class="bk-icon icon-close fr text-note-close"
          @click="hideNote = !hideNote" />
      </div>
      <div class="__editor">
        <Monaco
          ref="editor"
          :code="sqlData"
          :language="editorOpts.language"
          :options="ediotrOptions"
          :theme="editorOpts.theme"
          :tools="editorTools"
          class="sqlEditor"
          height="100%"
          @codeChange="editorChange">
          <ul v-if="showTitle && formData && formData.order && !buttonDisable"
            slot="header-tool"
            class="sql-button">
            <template v-for="(item, index) in formData.order">
              <li
                v-if="item !== 'es'"
                :key="index"
                :class="[`icon-${item}_storage`, (activeType === item && 'selected') || '']"
                class="bk-icon fl"
                @click="handleActiveTypeClick(item)">
                <button :title="formatOrder[item]"
                  class="bk-button bk-primary data-format"
                  type="button">
                  <i class="arrow" />
                  <span>{{ formatOrder[item] }}</span>
                </button>
              </li>
            </template>
          </ul>
        </Monaco>
      </div>
      <SqlQuerySearch
        :activeType="activeType"
        :nodeSearch="nodeSearch"
        :resultTable.sync="resultTable"
        :sqlData.sync="sqlData" />
    </div>
    <div v-if="topShow"
      class="back-top">
      <i class="bk-icon icon-back-top" />
      <span class="tips">{{ $t('返回顶部') }}</span>
    </div>
  </div>
</template>
<script>
import sqlFormatter from '@/components/monaco/sql-format/sqlFormatter';
import Monaco from '@/components/monaco';
import SqlQuerySearch from './SqlQuerySearch';
import Bus from '@/common/js/bus';

export default {
  components: {
    Monaco,
    SqlQuerySearch,
  },
  props: {
    buttonDisable: {
      type: Boolean,
      default: false,
    },
    formData: {
      type: Object,
    },
    showTitle: {
      type: Boolean,
      default: true,
    },
    ediotrOptions: {
      type: Object,
      default: () => {
        return { readOnly: false };
      },
    },
    resultTable: {
      type: String,
    },
    activeType: {
      type: String,
    },
    /** 查询历史是否显示 */
    nodeSearch: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      hideNote: false,
      sqlData: '',
      editorOpts: {
        language: 'bk-SqlLanguage-v1',
        theme: 'vs-dark',
      },
      editToolClass: { mysql: 'icon-mysql', tspider1: 'icon-tspider', default: 'icon-save-node' },
      editorTools: {
        enabled: true,
        title: '',
        guidUrl: this.$store.getters['docs/getPaths'].querySqlRule,
        toolList: {
          format_sql: true,
          view_data: false,
          guid_help: true,
          full_screen: true,
          event_fullscreen_default: true,
        },
      },
      formatOrder: [],
      topShow: false,
    };
  },

  watch: {
    formData: {
      handler(val) {
        val && val.storage && val.storage[this.activeType] && this.editorSql(val.storage[this.activeType].sql);
      },
      deep: true,
      immediate: true,
    },
    activeType: {
      handler(newVal) {
        if (this.formData && this.formData.storage[newVal]) {
          if (/^es/gi.test(newVal)) {
            this.editorOpts.language = 'json';
            this.editorOpts.theme = 'vs-dark';
          } else {
            this.editorOpts.language = 'bk-SqlLanguage-v1';
            this.editorOpts.theme = 'vs-dark';
          }

          this.$nextTick(() => {
            this.editorSql(this.formData.storage[newVal].sql);
          });
        }
      },
      immediate: true,
    },
  },
  async mounted() {
    await this.initEditor();
  },

  methods: {
    editorChange(value) {
      this.sqlData = value;
    },
    async initEditor() {
      let self = this;
      await this.bkRequest.httpRequest('dataStorage/getStorageCommon').then(resp => {
        this.formatOrder = resp.data.formal_name;
      });
    },
    editorSql(value) {
      if (!/^es/gi.test(this.activeType)) {
        this.sqlData = sqlFormatter.format(value);
      } else {
        this.sqlData = value;
      }
    },

    async handleActiveTypeClick(item) {
      if (this.activeType !== item) {
        await this.$store.commit('dataQuery/setClusterType', item);
        this.$nextTick(() => {
          const sql = (this.formData.storage[item] && this.formData.storage[item].sql) || '';
          Bus.$emit('DataQuery-QueryResult-History', item, sql);
        });
      }
    },
  },
};
</script>

<style media="screen" lang="scss">
.sqlQuery {
  &.noselect {
    user-select: none;
  }
  .no-data {
    display: block;
    text-align: center;
    padding: 65px 0;
    border-bottom: none;
    p {
      color: #cfd3dd;
    }
  }
  .bk-table {
    border-bottom: none;
  }
  .titel-wraper {
    margin: 0 0 15px 0;
    .title {
      color: #212232;
      font-size: 16px;
      padding-left: 17px;
      line-height: 26px;
      position: relative;
      &:before {
        content: '';
        width: 4px;
        height: 26px;
        background: #3a84ff;
        position: absolute;
        left: 0;
        top: 0px;
      }
      span {
        background: #fafafa;
      }
    }
  }
  .resultsData {
    padding: 20px 10px 40px;
    .__editor {
      height: 230px;
    }
    .sqlEditor {
      position: relative;
    }
    .sql-button {
      position: absolute;
      left: 15px;
      top: auto;
      li {
        margin-right: 5px;
        &.selected {
          background-color: #262b36;
        }

        .bk-button {
          &.data-format {
            position: absolute;
            top: 10px;
            left: 50%;
            transform: translate(-50%, 100%);
          }
        }
      }
    }
    .text-note {
      display: flex;
      align-items: center;
      position: relative;
      font-size: 14px;
      background: #fff3da;
      color: #e19f00;
      line-height: 20px;
      overflow: hidden;
      padding: 10px 12px 10px 10px;
      margin-bottom: 20px;
      border: solid 1px rgba(255, 180, 0, 0.2);
      border-radius: 4px;
      .bk-icon {
        margin-right: 10px;
        display: inline-block;
        vertical-align: -4px;
        font-size: 20px;
      }
      a {
        color: #3a84ff;
      }
      .text-note-close {
        position: absolute;
        right: 12px;
        margin-right: 0;
        display: inline-block;
        width: 20px;
        height: 20px;
        line-height: 20px;
        text-align: center;
        cursor: pointer;
      }
    }
    .tag {
      margin-bottom: 15px;
      .tag-label {
        padding: 2px 6px;
        background: #f5f5f5;
        border-radius: 2px;
        margin-left: 2px;
        cursor: pointer;
        &:hover {
          background: #e1ecff;
        }
      }
    }
  }
  .back-top {
    position: fixed;
    bottom: 46px;
    right: 30px;
    width: 36px;
    height: 36px;
    line-height: 36px;
    text-align: center;
    background: rgba(238, 238, 238, 0.48);
    box-shadow: -4px 0px 6px 0px rgba(27, 28, 17, 0.03);
    border-radius: 50%;
    border: solid 1px rgba(221, 221, 221, 0.48);
    opacity: 0.8;
    font-size: 16px;
    cursor: pointer;
    &:hover {
      background: #3a84ff;
      border-color: #3a84ff;
      opacity: 1;
      color: #fff;
      .tips {
        display: block;
      }
    }
    .tips {
      display: none;
      position: absolute;
      top: 50%;
      right: 50px;
      padding: 8px 12px;
      background: #212232;
      border-radius: 2px;
      font-size: 12px;
      color: #fff;
      line-height: 12px;
      min-width: 80px;
      transform: translate(0, -50%);
      &:after {
        position: absolute;
        right: -6px;
        top: 50%;
        content: '';
        width: 0;
        height: 0px;
        border: 3px solid #fff;
        border-color: transparent transparent transparent #212232;
        transform: translate(0, -50%);
      }
    }
  }
  .moreLoading {
    line-height: 44px;
    text-align: center;
    background: #fafafa;
    border-top: 1px solid #e6e6e6;
    p {
      display: inline-block;
      position: relative;
      padding: 20px;
    }
    .bk-icon {
      position: absolute;
      left: -20px;
      top: 35px;
      animation: rotate 0.5s infinite linear;
    }
  }
  @keyframes rotate {
    0% {
      transform: rotate(0deg);
      -webkit-transform: rotate(0deg);
      -moz-transform: rotate(0deg);
    }
    100% {
      transform: rotate(360deg);
      -webkit-transform: rotate(360deg);
      -moz-transform: rotate(360deg);
    }
  }
}
</style>
