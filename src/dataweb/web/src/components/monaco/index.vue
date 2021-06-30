

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
  <div class="bk-monaco-editor"
    :class="{ 'bk-scroll-y': isContainerScroll }"
    :style="containerStyle">
    <div v-if="toolsConfig.enabled"
      class="bk-monaco-tools"
      :style="toolStyle">
      <span>{{ $t(toolsConfig.title) }}</span>
      <div>
        <slot name="header-tool-left" />
        <ul class="fr">
          <li
            v-if="toolsConfig.toolList.font_size"
            v-bk-tooltips="handleTippyConfig(fontTippyConfig)"
            class="bk-icon icon-font fl" />
          <li
            v-if="toolsConfig.toolList.format_sql"
            v-bk-tooltips="handleTippyConfig(formatText)"
            class="bk-icon icon-code fl"
            @click="formatMonacoSqlCode">
            <!-- <button type="button"
                            class="bk-button bk-primary data-format"
                            :title="formatText">
                            <i class="arrow"></i>
                            <span>{{ formatText }}</span>
                        </button> -->
          </li>
          <li
            v-if="toolsConfig.toolList.view_data"
            v-bk-tooltips="handleTippyConfig($t('查看数据格式'))"
            class="bk-icon icon-empty fl"
            @click="handleViewDataFormat" />
          <li
            v-if="toolsConfig.toolList.guid_help"
            v-bk-tooltips="handleTippyConfig($t('帮助文档'))"
            class="bk-icon icon-exclamation-circle fl"
            @click="jumpToGuide">
            <!-- <button type="button"
                            class="bk-button bk-primary info"
                            :title="$t('帮助文档')">
                            <i class="arrow"></i>
                            <span>{{ $t('帮助文档') }}</span>
                        </button> -->
          </li>
          <li
            v-if="toolsConfig.toolList.full_screen"
            v-bk-tooltips="handleTippyConfig(fullscreenOptions.text)"
            :class="fullscreenOptions.className"
            @click="handleFullscreenClick">
            <!-- <button type="button"
                            class="bk-button bk-primary full-screen"
                            :title="$t(fullscreenOptions.text)">
                            <i class="arrow"></i>
                            <span>{{ $t(fullscreenOptions.text) }}</span>
                        </button> -->
          </li>
          <template v-if="toolsConfig.toolList.editor_fold">
            <li
              v-if="isEditorFold"
              v-bk-tooltips="$t('展开编辑器')"
              class="icon-down-circle fl bk-icon-new"
              @click="unfoldEditor" />
            <li v-else
              v-bk-tooltips="$t('折叠编辑器')"
              class="icon-up-circle fl bk-icon-new"
              @click="foldEditor" />
          </template>
        </ul>
        <slot name="header-tool" />
      </div>
    </div>
    <div>
      <slot />
    </div>
    <div ref="bkMonacoEditor"
      :style="editorStyle"
      class="monaco-editor-layer bk-scroll-y" />

    <!-- tooltip 字体的content -->
    <div v-if="toolsConfig.toolList.font_size"
      id="font-size-tippy">
      <span v-for="(item, index) in fontList"
        :key="index"
        class="icon-item">
        <i :class="['icon-font', item]"
          @click="setFontSize(item)" />
      </span>
    </div>
  </div>
</template>

<script>
import { compareStr, debounce } from './sql-format/core/utils';
import bkSqlLanguage from './bkSqlLanguage.js';
import { deepAssign } from '@/common/js/util.js';

export default {
  props: {
    width: { type: [String, Number], default: '100%' },
    height: { type: [String, Number], default: '100%' },
    code: { type: String, default: '// code \n' },
    language: { type: String, default: 'bk-SqlLanguage-v1' },
    theme: { type: String, default: 'vs-dark' },
    options: { type: Object, default: () => {} },
    tools: { type: Object, default: () => {} },
    changeThrottle: { type: Number, default: 0 },
  },
  data() {
    return {
      defaults: {
        selectOnLineNumbers: true,
        roundedSelection: false,
        readOnly: false,
        cursorStyle: 'line',
        automaticLayout: true,
        glyphMargin: false,
        fontSize: 16,
        fontFamily: 'SourceCodePro, Menlo, Monaco, Consolas, "Courier New", monospace',
        lineNumbers: 'on',
        lineDecorationsWidth: '0px',
        lineNumbersMinChars: 3,
        scrollBeyondLastLine: false,
        quickSuggestions: true,
        wordBasedSuggestions: true,
        minimap: {
          enabled: false,
        },
      },
      fontList: ['mini', 'small', 'normal'],
      sizeMap: {
        mini: '12px',
        small: '14px',
        normal: '16px',
      },
      toolbar: {
        isFullscreen: false,
      },
      EVENTS: {
        screenfull: null,
      },
      editor: null,
      isEditorFold: false,
      isContainerScroll: false,
      fontTippyConfig: {
        allowHtml: true,
        placement: 'bottom',
        content: '#font-size-tippy',
        appendTo: function () {
          return document.querySelector('.bk-monaco-editor');
        },
      },
    };
  },
  computed: {
    formatText() {
      return this.language === '' ? this.$t('格式化SQL') : this.$t('格式化');
    },
    containerStyle() {
      const fold = this.isEditorFold;
      return fold
        ? {
          height: '40px',
        }
        : {
          height: this.height.indexOf('px') > 0 ? this.height : '100%',
        };
    },
    toolStyle() {
      let calHeight = (this.toolsConfig.enabled && this.toolsConfig.height) || 0;
      return {
        height: calHeight + 'px',
        'line-height': calHeight + 'px',
      };
    },
    editorStyle() {
      const { width, height } = this;
      const fixedWidth = width.toString().indexOf('%') !== -1 ? width : `${width}px`;
      const fixedHeight = `calc(100% - ${(this.toolsConfig.enabled && this.toolsConfig.height) || 0}px)`;
      return {
        width: fixedWidth,
        height: fixedHeight,
      };
    },
    editorOptions() {
      return deepAssign({}, [
        this.defaults,
        this.options,
        {
          value: this.code,
          language: this.language,
          theme: this.theme,
        },
      ]);
    },
    toolsConfig() {
      let defaultConfig = {
        height: 40,
        enabled: true,
        title: 'SQL设置',
        guidUrl: 'index.html',
        toolList: {
          font_size: false,
          format_sql: true,
          view_data: false,
          guid_help: true,
          full_screen: false,
          event_fullscreen_default: false,
        },
      };

      let targetConfig = Object.assign({}, defaultConfig, this.tools);
      targetConfig.toolList = Object.assign({}, defaultConfig.toolList, (this.tools || {}).toolList);
      return targetConfig;
    },
    fullscreenOptions() {
      return {
        className: {
          'bk-icon': true,
          'icon-un-full-screen': this.toolbar.isFullscreen,
          'icon-full-screen': !this.toolbar.isFullscreen,
          fl: true,
        },
        text: (this.toolbar.isFullscreen && '缩小') || '全屏',
      };
    },
  },
  watch: {
    language() {
      window.monaco.editor.setModelLanguage(this.editor.getModel(), this.language);
    },
    code(val) {
      let oldVal = this.editor.getValue();
      if (!compareStr(val, oldVal)) {
        let position = this.editor.getPosition();
        this.editor.setValue([val].join('\n'));
        this.editor.setPosition(position);
      }
    },
  },
  mounted() {
    this.createMonaco();
  },
  destroyed() {
    this.destroyMonaco();
  },
  beforeDestroy() {
    this.editor && this.editor.dispose();
  },
  methods: {
    handleTippyConfig(data) {
      const result = {
        boundary: document.body,
        content: data,
        placement: 'bottom',
      };
      if (typeof data === 'string') {
        return result;
      } else {
        return Object.assign({}, result, data);
      }
    },
    setFontSize(size) {
      const options = { fontSize: this.sizeMap[size] };
      this.editor.updateOptions(options);
    },
    foldEditor() {
      this.isEditorFold = true;
    },
    unfoldEditor() {
      this.isEditorFold = false;
    },
    editorHasLoaded(editor, monaco) {
      this.editor = editor;
      this.monaco = monaco;
      this.editor.onDidChangeModelContent(event => this.codeChangeHandler(editor, event));
      this.$emit('mounted', editor);
    },
    codeChangeHandler: function (editor) {
      if (this.codeChangeEmitter) {
        this.codeChangeEmitter(editor);
      } else {
        this.codeChangeEmitter = debounce(function (editor) {
          this.$emit('codeChange', editor.getValue());
        }, this.changeThrottle);
        this.codeChangeEmitter(editor);
      }
    },
    createMonaco() {
      this.language === 'bk-SqlLanguage-v1' && bkSqlLanguage.load();
      this.editor = window.monaco.editor.create(this.$refs.bkMonacoEditor, this.editorOptions);
      this.editorHasLoaded(this.editor, window.monaco);
    },
    destroyMonaco() {
      if (typeof this.editor !== 'undefined') {
        this.editor.dispose();
      }
    },
    formatMonacoSqlCode() {
      this.editor.getAction('editor.action.formatDocument').run();
    },
    jumpToGuide() {
      console.log(this.toolsConfig.guidUrl);
      if (this.toolsConfig.enabled && this.toolsConfig.guidUrl) {
        window.open(this.toolsConfig.guidUrl);
      }
    },
    handleFullscreenClick() {
      if (this.toolsConfig.toolList.event_fullscreen_default) {
        if (!this.EVENTS.screenfull) {
          const screenfull = require('./fullscreen');
          this.EVENTS.screenfull = screenfull;
          // window.screenfull = null  此处将导致不刷新页面重新加载组件时报错，udf去掉
          this.EVENTS.screenfull.onchange(() => {
            this.toolbar.isFullscreen = !this.toolbar.isFullscreen;
          });
        }
        if (this.EVENTS.screenfull.enabled) {
          this.toolbar.isFullscreen ? this.EVENTS.screenfull.exit(this.$el) : this.EVENTS.screenfull.request(this.$el);
        }
      } else {
        this.toolbar.isFullscreen = !this.toolbar.isFullscreen;
      }

      this.$emit('fullscreenChange', this.toolbar.isFullscreen);
    },
    handleViewDataFormat() {
      this.isContainerScroll = !this.isContainerScroll;
      this.$emit('onViewDataFormatClick');
    },
  },
};
</script>
<style lang="scss">
.bk-monaco-editor {
  width: 100%;
  height: 100%;
  //   display: flex;
  //   flex-direction: column;
  //   justify-content: flex-start;

  .view-lines {
    div,
    span {
      font-family: inherit;
    }
  }
  .bk-scroll-y {
    &::-webkit-scrollbar {
      width: 0px;
      background-color: transparent;
    }
    &::-webkit-scrollbar-thumb {
      border-radius: 0px;
      background-color: #a0a0a0;
    }
  }
  ::v-deep .tippy-popper .tippy-tooltip {
    padding: 7px 0;
    background: #2f3033;
    box-shadow: 0px 2px 4px 0px rgba(0, 0, 0, 0.5);
    #font-size-tippy {
      font-size: 0;
    }
    .icon-item {
      display: inline-block;
      height: 18px;
      width: 52px;
      color: #979ba5;
      .icon-font {
        cursor: pointer;
        line-height: 18px;
        &:hover {
          color: #fafbfd;
        }
      }
      &:not(:last-child) {
        border-right: 1px solid #63656e;
      }
      .mini {
        font-size: 12px;
      }
      .small {
        font-size: 14px;
      }
      .normal {
        font-size: 16px;
      }
    }
  }
}

.monaco-editor-layer {
  //   padding-bottom: 5px;
  padding-top: 5px;
  background: #1e1e1e;
}

.bk-monaco-tools {
  color: #fff;
  background: #1c212b;
  padding: 0 20px;
  display: flex;
  justify-content: space-between;
  //   border-bottom: 1px solid #3f51b594;

  > div {
    display: flex;
    align-items: center;
  }

  ul {
    // padding-top: 5px;
    li {
      line-height: 30px;
      text-align: center;
      width: 34px;
      height: 30px;
      cursor: pointer;
      border-radius: 5px;
      position: relative;
      display: block;
      color: #b2bac0;
      &:hover {
        background-color: #262b36;
      }
      &:hover button {
        display: block;
      }
      i.arrow {
        position: absolute;
        top: -5px;
        left: 50%;
        border: 5px solid #8b90d2;
        border-top-color: transparent;
        border-left-color: transparent;
        border-right-color: transparent;
        -webkit-transform: translate(-50%, -50%);
        -moz-transform: translate(-50%, -50%);
        -ms-transform: translate(-50%, -50%);
        -o-transform: translate(-50%, -50%);
        transform: translate(-50%, -50%);
      }
      button {
        background-color: #8b90d2;
        padding: 0 8px;
        display: none;
        height: 26px;
        line-height: 1;
        z-index: 350;
        position: absolute;
        top: 34px;
        font-size: 12px;
      }
      .data-format {
        left: -28px;
        z-index: 1001;
      }
      .info {
        z-index: 1001;
        left: -16px;
      }
      .full-screen {
        left: -3px;
        z-index: 1001;
      }
    }
  }
}
</style>
