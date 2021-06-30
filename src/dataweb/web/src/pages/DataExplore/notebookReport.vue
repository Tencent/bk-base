

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
  <div v-bkloading="{ isLoading: basicLoading, zIndex: 10 }"
    :style="[addWatermark ? waterMark : '']"
    class="notebookReport-wrapper">
    <div v-for="(cells, index) in content"
      :key="index"
      class="mavonEditor">
      <div v-if="cells.cell_type === 'markdown'">
        <mavon-editor ref="md"
          :scrollStyle="true"
          :subfield="false"
          :toolbarsFlag="false"
          :ishljs="true"
          :boxShadow="false"
          :editable="false"
          :value="cells.source"
          previewBackground="#fff"
          defaultOpen="preview" />
      </div>
      <div v-else-if="cells.cell_type === 'code'"
        class="code-wrapper">
        <div v-show="hideCode !== 'hideCode'">
          <div class="code-main">
            <div class="count">
              [<span>{{ cells.execution_count }}</span>]
            </div>
            <code-preview :code.sync="cells.source"
              class="python" />
          </div>
        </div>

        <div v-for="(outputs, childIndex) in cells.outputs"
          :key="childIndex"
          class="outputs">
          <div v-if="outputs.data && outputs.data.hasOwnProperty('text/plain')">
            <div v-if="outputs.data.hasOwnProperty('text/html')"
              class="out-box">
              <div class="out-count">
                Out[{{ cells.execution_count }}]：
              </div>
              <!--eslint-disable vue/no-v-html-->
              <div class="table-data"
                v-html="outputs.data['text/html']" />
            </div>
            <div v-else-if="outputs.data.hasOwnProperty('image/png')">
              <img class="outputs-image"
                :src="`data:image/png;base64,${outputs.data['image/png']}`">
            </div>
            <div v-else
              class="out-box">
              <div class="out-count">
                Out[{{ cells.execution_count }}]：
              </div>
              <div class="table-data textarea"
                v-html="outputs.data['text/plain']" />
            </div>
          </div>
          <div v-if="childIndex === 0">
            <div v-if="outputs.output_type === 'stream'"
              class="out-box">
              <div class="out-count" />
              <div v-if="outputs.text.includes('bk_status')"
                class="status">
                <div class="stage_item">
                  <span class="icon-check-line" />
                  <span class="stage_description">SQL解析</span>
                  <span class="stage_time">1s</span>
                </div>
                <div class="stage_item">
                  <span class="icon-check-line" />
                  <span class="stage_description">提交任务</span>
                  <span class="stage_time">1s</span>
                </div>
                <div class="stage_item">
                  <span class="icon-check-line" />
                  <span class="stage_description">任务执行</span>
                  <span class="stage_time">24s</span>
                </div>
              </div>
              <div v-else
                class="table-data textarea"
                v-html="outputs['text']" />
            </div>
          </div>
          <template v-if="outputs.data && outputs.data.hasOwnProperty('bk_chart')">
            <div class="table">
              <div class="chart-count">
                Out[{{ outputs.execution_count }}]：
              </div>
              <notebook-chart class="chart-box"
                :list="JSON.parse(outputs.data.list)"
                :isChart="outputs.data.bk_chart === 'query_table'"
                :select_fields_order="JSON.parse(outputs.data.select_fields_order)"
                :chartConfig="JSON.parse(outputs.data.chart_config)" />
            </div>
          </template>
        </div>
      </div>
    </div>

    <!-- 无权限申请 -->
    <PermissionApplyWindow
      :isOpen.sync="isPermissionShow"
      :objectId="$route.query.projectId"
      :showWarning="true"
      :defaultSelectValue="{
        objectClass: 'project',
      }" />
  </div>
</template>

<script>
const PermissionApplyWindow = () => import('@/pages/authCenter/permissions/PermissionApplyWindow');

import { mavonEditor } from 'mavon-editor';
// import Monaco from '@/components/monaco';
import notebookChart from './notebookChart.vue';
import codePreview from './components/CodePreview.vue';

export default {
  components: { mavonEditor, notebookChart, codePreview, PermissionApplyWindow },
  data() {
    return {
      content: [],
      treeList: [],
      basicLoading: false,
      hideCode: '',
      hideNum: '',
      addWatermark: '',
      waterMark: {
        background: 'url(' + window.userPic + ') repeat',
      },
      isPermissionShow: false,
      curType: '',
    };
  },
  created() {
    this.basicLoading = true;
    this.curType = this.$route.query.type;
    /** 根据是否key值生成笔记，生成笔记后根据页面内容生成大纲 */
    this.generateNotebookContents().then(() => {
      this.$nextTick(() => {
        this.getDirectories();
      });
    });
  },
  methods: {
    generateNotebookContents() {
      let reportSecret = this.$route.query.key;
      if (reportSecret) {
        return this.bkRequest
          .httpRequest('dataExplore/getReport', {
            query: {
              report_secret: reportSecret,
            },
          })
          .then(res => {
            if (res.data) {
              this.$emit('noteContent', res.data);
              this.content = res.data.notebook_content.content.cells;
              let reportConfig = JSON.parse(res.data.report_config);
              console.log('content', this.content);
              reportConfig.map(item => {
                if (item === 'hideCode') {
                  this.hideCode = item;
                } else if (item === 'hideNum') {
                  this.hideNum = item;
                } else if (item === 'addWatermark') {
                  this.addWatermark = item;
                } else {
                  return;
                }
              });
              this.basicLoading = false;
            } else {
              this.basicLoading = false;
              if (this.curType === 'common' && res.message === '1511001') {
                this.isPermissionShow = true;
              } else {
                this.getMethodWarning(res.message, res.code);
              }
            }
          });
      } else {
        return this.getNotebooksContents();
      }
    },
    getDirectories() {
      this.treeList = Array.from(this.$el.querySelectorAll(
        '.scroll-style-border-radius h1, .scroll-style-border-radius h2, .scroll-style-border-radius h3'))
        .map((element, index) => {
          return {
            name: element.innerText,
            title: element.innerText,
            el: element,
            offsetTop: element.offsetTop,
            isActive: index === 0,
          };
        });
      this.$emit('noteList', this.treeList);
    },
    getNotebooksContents() {
      return this.bkRequest
        .httpRequest('dataExplore/getNotebooksContents', {
          params: {
            notebook_id: this.$route.query.notebook,
          },
        })
        .then(res => {
          if (res.data) {
            this.basicLoading = false;
            this.content = res.data.contents.content.cells;
          } else {
            this.basicLoading = false;
            this.getMethodWarning(res.message, res.code);
          }
        });
    },
  },
};
</script>

<style lang="scss" scoped>
.notebookReport-wrapper {
  /deep/.mavonEditor {
    box-sizing: border-box;
    margin-bottom: 24px;
    .v-note-wrapper,
    .markdown-body {
      min-height: 0 !important;
      position: static;
      border: none;
      padding-left: 70px;
      width: 100%;
    }
    .markdown-body {
      font-size: 14px;
      position: static;
      .v-note-panel {
        position: static !important;
      }
      .v-show-content .scroll-style .scroll-style-border-radius {
        height: 200px;
      }
      img {
        max-width: 100%;
        height: auto;
        display: block;
        margin-left: auto;
        margin-right: auto;
      }
      h1 {
        font-size: 26px;
        font-weight: 700;
        border-bottom: none;
        margin-top: 10px;
      }
      h2 {
        font-size: 22px;
        font-weight: 700;
        border-bottom: none;
        margin-top: 10px;
      }
      ul {
        line-height: 20px;
        li {
          list-style-type: disc;
        }
      }
      blockquote {
        margin: 14px 28px;
        padding: 9px 18px;
      }
      a {
        color: #296eaa;
        text-decoration: underline;
      }
    }
    .markdown-body blockquote {
      color: #000;
    }
    .python {
      width: 100%;
      font-size: 14px;
    }
    .table {
      width: 100%;
      display: flex;
      .chart-count {
        min-width: 80px;
        text-align: right;
        color: #d84315;
        font-family: monospace;
      }
      .chart-box {
        width: 96%;
      }
    }
    .code-wrapper {
      padding-left: 15px;
      .outputs {
        line-height: 20px;
        .table-data table {
          border-collapse: collapse;
          border: 1px solid #cbcbcb;
          text-align: center;
          font-size: 12px;
          tr {
            border-bottom: 1px solid #cbcbcb;
          }
          td,
          th {
            padding: 5px 10px;
            border-right: 1px solid #cbcbcb;
          }
        }
        .out-box {
          display: flex;
          margin-bottom: 15px;
          .out-count {
            min-width: 80px;
            text-align: right;
            color: #d84315;
            font-family: monospace;
          }
          .table-data {
            width: 100%;
            overflow-x: auto;
          }
          .status {
            display: flex;
            .stage_item {
              &:not(:last-child)::after {
                content: '';
                display: block;
                width: 28px;
                height: 11px;
                float: right;
                margin: 0 8px;
                border-bottom: 1px solid #dcdee5;
              }
              .icon-check-line {
                display: inline-block;
                color: #c4c6cc;
                font-size: 13px;
                margin-right: 3px;
              }
              .stage_description {
                font-size: 14px;
                color: #63656e;
                font-weight: 400;
              }
              .stage_time {
                color: #979ba5;
                margin-left: 3px;
              }
            }
          }
        }
        .outputs-image {
          margin-left: 80px;
        }
        .textarea {
          white-space: pre-wrap;
          font-family: monospace;
          line-height: 25px;
        }
      }
    }
    .code-main {
      display: flex;
      padding: 20px 0 10px 0;
      .count {
        min-width: 80px;
        padding: 13px 20px 0 0;
        text-align: right;
        color: #979ba5;
        font-family: monospace;
        span {
          padding: 0 2px;
          font-family: monospace;
        }
      }
    }
  }
}
</style>
