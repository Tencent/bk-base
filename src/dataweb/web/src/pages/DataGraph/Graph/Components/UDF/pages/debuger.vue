

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
  <div v-bkloading="{ isLoading: pageLoading }"
    class="wrapper">
    <block-content :header="$t('函数配置')">
      <div class="block-item">
        <span class="pub-desp">{{ $t('函数名称') }}：</span>
        <span class="content">{{ devlopParams.func_name }}</span>
      </div>
      <div class="block-item start-align">
        <span class="pub-desp lh28">{{ $t('输入参数类型') }}：</span>
        <div class="input-params-wrapper">
          <text-item
            v-for="(item, index) in devlopParams.input_type"
            :key="index"
            style="margin-right: 9px"
            :text="item"
            :shape="'rect'" />
        </div>
      </div>
      <div class="block-item start-align">
        <span class="pub-desp lh28">{{ $t('函数返回类型') }}：</span>
        <div class="items-container">
          <div class="output-params-wrapper">
            <text-item
              v-for="(item, index) in devlopParams.return_type"
              :key="index"
              style="margin-right: 9px"
              :text="item"
              :shape="'rect'" />
          </div>
        </div>
      </div>
    </block-content>
    <block-Content :header="$t('计算类型')">
      <div class="block-item">
        <div class="checkboxGroup">
          <bkdata-checkbox-group v-model="calculateType">
            <bkdata-checkbox :value="'stream'">
              {{ $t('实时') }}
            </bkdata-checkbox>
            <bkdata-checkbox :value="'batch'">
              {{ $t('离线') }}
            </bkdata-checkbox>
            <bkdata-checkbox :value="'query'"
              disabled>
              {{ $t('查询') }}
            </bkdata-checkbox>
          </bkdata-checkbox-group>
        </div>
        <p class="info">
          <span class="icon-info" />
          <span>{{ $t('选择计算类型后_可以在对应的计算节点SQL中使用该发布的函数') }}</span>
        </p>
      </div>
    </block-Content>
    <block-Content :header="$t('调试配置')"
      :paddingBottom="'25px'">
      <div class="block-item">
        <span class="desp">{{ $t('调试源') }}</span>
        <bkdata-radio-group v-model="debugSource">
          <bkdata-radio :value="'platform_source'">
            平台数据源
          </bkdata-radio>
          <bkdata-radio :value="'uploaded_source'"
            :disabled="true">
            上传数据
          </bkdata-radio>
        </bkdata-radio-group>
      </div>
      <div class="block-item">
        <span class="desp">{{ $t('数据源') }}</span>
        <bkdata-selector
          :selected.sync="bizs.selected"
          :placeholder="$t('请先选择业务')"
          :list="bizs.list"
          :isLoading="bizs.isLoading"
          :searchable="true"
          :searchKey="'bk_biz_name'"
          :settingKey="'bk_biz_id'"
          :displayKey="'bk_biz_name'"
          style="width: 240px; margin-right: 10px"
          @item-selected="bizChange" />
        <bkdata-selector
          :selected.sync="results.selected"
          :placeholder="$t('再选择结果数据表_以项目维度分类')"
          :isLoading="results.isLoading"
          :searchable="true"
          :hasChildren="true"
          :list="resultsLists"
          :settingKey="'result_table_id'"
          :displayKey="'name'"
          style="width: 280px"
          @item-selected="resultsChange" />
      </div>
      <div v-if="tabelHeader.length"
        class="block-item big-margin-top">
        <bkdata-popover ref="tippy"
          placement="bottom"
          width="440"
          theme="light"
          trigger="click">
          <div class="tabelConfig">
            <span class="icon-cog-shape" />
          </div>
          <div slot="content"
            style="margin: -8px -14px">
            <div class="tabel-config-hedaer">
              设置列表字段
            </div>
            <div class="table-config-tippyBody">
              <bkdata-checkbox-group v-model="tabelHeaderOptions"
                :checked="true">
                <span v-for="(item, index) in debug_data.schema"
                  :key="index"
                  class="option-item">
                  <bkdata-checkbox :value="item.field_name"
                    :title="`${item.field_name}(${item.field_type})`">
                    {{ `${item.field_name}(${item.field_type})` }}
                  </bkdata-checkbox>
                </span>
              </bkdata-checkbox-group>
              <bkdata-checkbox v-model="isAllCheck"
                :indeterminate="indeterminate"
                @change="checkAll">
                {{ $t('全选') }}
              </bkdata-checkbox>
            </div>
            <div class="table-config-tippyFooter">
              <bkdata-button :theme="'primary'"
                @click="changeTableInfo">
                {{ $t('确定') }}
              </bkdata-button>
              <bkdata-button @click="closeTippy">
                {{ $t('取消') }}
              </bkdata-button>
            </div>
          </div>
        </bkdata-popover>
        <bkdata-table
          :data="tableData"
          :border="true"
          style="width: 95%
                            margin-top: 20px;">
          <bkdata-table-column
            v-for="(item, index) in tabelHeader"
            :key="index"
            :label="item"
            :minWidth="'120px'"
            :resizable="true"
            :align="'center'"
            :renderHeader="renderHeader">
            <editable-cell v-model="row[index]"
              slot-scope="{ row }">
              <span slot="content"
                :title="row[index]">
                {{ row[index] }}
              </span>
            </editable-cell>
          </bkdata-table-column>
        </bkdata-table>
        <div v-for="(item, index) in tableData"
          :key="index"
          class="operateBtn"
          :style="getIconTop(index)">
          <span
            v-if="index === tableData.length - 1"
            class="icon-plus-circle-shape icon"
            @click="handleExpOption('add', index)" />
          <span
            v-if="tableData.length !== 1"
            class="icon-minus-circle-shape icon"
            @click="handleExpOption('remove', index)" />
        </div>
      </div>
      <div class="block-item">
        <span class="desp">{{ $t('调试') }}SQL</span>
        <div v-bkloading="{ isLoading: sqlLoading }"
          class="monaco-wrapper"
          style="width: 608px">
          <Monaco
            height="215px"
            :options="{ readOnly: false, fontSize: 12 }"
            :tools="tools"
            :code="sql"
            @codeChange="code => editorChange(code)" />
        </div>
      </div>
      <div class="block-item"
        style="padding-left: 130px; margin-bottom: 0">
        <bkdata-button :theme="btnTheme"
          class="mr10"
          @click="debuger">
          {{ btnText }}
        </bkdata-button>
      </div>
      <div v-if="showDebugStatus"
        v-bkloading="{ isLoading: debugLoading }"
        class="debugInfo">
        <debug-status
          v-for="(item, index) in debugStatusInfo.logs"
          :key="index"
          :calculateType="calculateType"
          :logs="item" />
      </div>
    </block-Content>
  </div>
</template>

<script>
import textItem from '../components/textItem';
import EditableCell from '../components/editTableCell';
import Monaco from '@/components/monaco';
import blockContent from '../components/contentBlock';
import debugStatus from '../components/debugStatusBlock';
import { mapGetters } from 'vuex';
import { showMsg, postMethodWarning } from '@/common/js/util.js';
import tippy from 'tippy.js';
export default {
  components: { Monaco, blockContent, debugStatus, EditableCell, textItem },
  props: {
    isShow: Boolean,
  },
  data() {
    return {
      indeterminate: false,
      isAllCheck: false,
      pageLoading: false,
      canPolling: true,
      curHeaderLength: 0,
      sqlLoading: false,
      isDebug: false,
      debugLoading: false,
      showDebugStatus: false,
      debugStatusInfo: {
        status: '',
        logs: [],
      },
      tabelHeaderOptions: [],
      data: [],
      calculateType: [],
      tippyInstance: null,
      debugSource: 'platform_source',
      bizs: {
        selected: '',
        placeholder: this.$t('请输入业务相关的关键字'),
        list: [],
        isLoading: false,
      },
      tabelHeader: [],
      tabelDataToSend: {},
      results: {
        // 结果数据表下拉列表默认值
        selected: '',
        searchable: true,
        placeholder: this.$t('请输入结果数据表'),
        resultList: [],
        isLoading: false,
      },
      debug_data: {
        schema: [
          {
            field_name: 'a',
            field_type: 'string',
          },
          {
            field_name: 'b',
            field_type: 'string',
          },
          {
            field_name: 'c',
            field_type: 'string',
          },
          {
            field_name: 'd',
            field_type: 'number',
          },
        ],
        value: [
          ['x', 'xx'],
          ['t', 'tt'],
        ],
      },
      taskId: '',
      bussinessList: [],
      sql: '',
      tools: {
        enabled: true,
        title: '',
        guidUrl: this.$store.getters['docs/getNodeDocs'].udf,
        toolList: {
          format_sql: false,
          view_data: false,
          guid_help: true,
          full_screen: true, // 是否启用全屏设置
          event_fullscreen_default: true, // 是否启用默认的全屏功能，如果设置为false需要自己监听全屏事件组后续处理
        },
      },
      debuged: false,
    };
  },
  computed: {
    ...mapGetters({
      devlopParams: 'udf/devlopParams',
    }),
    /**
     * 按照业务进行分组,组装结果表列表
     */
    resultsLists: function () {
      let rawRtList = this.results.resultList;
      let _resultsList = [];
      let mProjectRts = {};
      for (let i = 0; i < rawRtList.length; i++) {
        let _rt = rawRtList[i];
        let _projectIndex = `${_rt.project_name}`;
        if (mProjectRts[_projectIndex] === undefined) {
          mProjectRts[_projectIndex] = [];
        }

        mProjectRts[_projectIndex].push({
          result_table_id: _rt.result_table_id,
          name: _rt.result_table_id + ' (' + _rt.description + ')',
        });
      }
      let tempArr = [];
      for (let _projectIndex in mProjectRts) {
        tempArr.push({
          name: _projectIndex,
          children: mProjectRts[_projectIndex],
        });
      }
      return tempArr.sort(function (a, b) {
        return a.name < b.name;
      });
    },
    tableData() {
      let data = this.tabelDataToSend.value.map(item => {
        let retObj = {};
        item.forEach((data, index) => {
          retObj[index] = data;
        });
        return retObj;
      });
      return data;
    },
    btnText() {
      return this.isDebug ? this.$t('停止') : this.debuged ? this.$t('再次调试') : this.$t('开始调试');
    },
    btnTheme() {
      return this.isDebug ? 'danger' : this.debuged ? 'default' : 'primary';
    },
  },
  watch: {
    tabelHeaderOptions(val) {
      if (val.length === this.debug_data.schema.length) {
        this.isAllCheck = true;
        this.indeterminate = false;
      } else if (val.length === 0) {
        this.indeterminate = false;
        this.isAllCheck = false;
      } else {
        this.isAllCheck = false;
        this.indeterminate = true;
      }
    },
    calculateType() {
      this.$emit('dubugFinish', false);
    },
    'tabelHeader.length'(val, oldVal) {
      if (oldVal === 0 && val) {
        this.$nextTick(() => {
          this.reateTableTippy();
        });
      }
    },
  },

  created() {
    this.pageLoading = true;
    this.bkRequest
      .httpRequest('udf/getUDFDebugerInfo', {
        params: {
          function_name: this.devlopParams.func_name,
        },
      })
      .then(res => {
        if (res.result && Object.keys(res.data).length) {
          this.calculateType = res.data.calculation_types;
          this.bizs.selected = Number(res.data.result_table_id.split('_')[0]);
          this.results.selected = res.data.result_table_id;
          this.sql = res.data.sql;
          this.getResultsList(this.bizs.selected);
          this.formatTableData(res.data.debug_data);
        } else if (!res.result) {
          this.getMethodWarning(res.message, res.code);
        }
      })
      ['finally'](() => {
        this.pageLoading = false;
      });
  },
  mounted() {
    this.getBizList();
  },
  methods: {
    reateTableTippy() {
      const target = document.getElementsByClassName('edit-cell')[0];
      console.log(target);
      this.tippyInstance = tippy(target, {
        content: this.$t('点击以进行编辑'),
        theme: 'dark',
        placement: 'left',
        showOnInit: true,
        trigger: 'manual',
        arrow: true,
        boundary: 'window',
      });
    },
    checkAll(val) {
      if (val) {
        const selected = [];
        this.debug_data.schema.forEach(item => {
          selected.push(item.field_name);
        });
        this.tabelHeaderOptions = Array.from(selected);
      } else {
        this.tabelHeaderOptions = [this.debug_data.schema[0].field_name];
      }
    },
    renderHeader(h, data) {
      const tooltip = {
        content: data.column.label,
      };
      return <span v-bk-tooltips={tooltip}>{data.column.label}</span>;
    },
    checkValid() {
      // 重置报错弹窗信息
      const bkMsgIns = document.querySelector('.bk-message');
      bkMsgIns && bkMsgIns.remove();

      // 计算类型必选
      if (!this.calculateType.length) {
        showMsg(this.$t('请选择计算类型'), 'warning');
        return false;
      }

      // 权限对象必选
      if (!this.results.selected) {
        showMsg(this.$t('请选择数据源'), 'warning');
        return false;
      }

      // SQL 必选
      if (this.sql === '') {
        showMsg(this.$t('请填写调试SQL'), 'warning');
        return false;
      }
      return true;
    },
    getResultList() {
      this.sqlLoading = true;
      this.bkRequest.httpRequest('meta/getSQLFields', { params: { rtId: this.results.selected } }).then(res => {
        if (res.result) {
          this.sql = '';
          let mySqlTable = [];
          for (let i = 0; i < res.data.fields_detail.length; i++) {
            let temp = {
              name: res.data.fields_detail[i].name,
              type: res.data.fields_detail[i].type,
              des: res.data.fields_detail[i].description,
            };
            mySqlTable.push(temp);
          }

          for (let i = 0; i < mySqlTable.length; i++) {
            this.sql += mySqlTable[i].name + ', ';
            if (i % 3 === 0 && i !== 0) {
              this.sql += '\n    ';
            }
          }
          let index = this.sql.lastIndexOf(',');
          this.sql = this.sql.slice(0, index);
          if (!this.sql) {
            this.sql = '*';
          }
          this.sql = 'select ' + this.sql + '\nfrom ' + this.results.selected;
        } else {
          this.getMethodWarning(res.message, res.code);
        }
        this.sqlLoading = false;
      });
    },
    tabelHeaderSet() {
      this.tabelHeader = this.debug_data.schema
        .filter(item => this.tabelHeaderOptions.includes(item.field_name))
        .map(item => {
          return `${item.field_name}(${item.field_type})`;
        });
    },
    changeTableInfo() {
      this.tabelHeaderSet(); // 表头重置
      let indexFix = 0;
      let debugValue = [];
      if (this.curHeaderLength > this.tabelHeaderOptions.length) {
        // 删除列时，保持表格修改值不变
        this.tableData.forEach(item => {
          debugValue.push(Object.values(item));
        });
      } else {
        debugValue = JSON.parse(JSON.stringify(this.debug_data.value));
      }
      let schema = this.debug_data.schema.filter((item, index) => {
        if (!this.tabelHeaderOptions.includes(item.field_name)) {
          debugValue.forEach(item => {
            item.splice(index - indexFix, 1);
          });
          indexFix++;
          return false;
        }
        return true;
      });
      Object.assign(this.tabelDataToSend, { schema: schema, value: debugValue });
      this.curHeaderLength = this.tabelHeaderOptions.length;
      this.closeTippy();
    },
    editorChange(code) {
      this.sql = code;
    },
    /* 轮询 */
    async debugerPolling(taskId) {
      if (!this.canPolling) return;
      let timeId = setTimeout(async () => {
        await this.bkRequest
          .httpRequest('udf/getDebugStatus', {
            params: {
              function_name: this.devlopParams.func_name,
              debugger_id: taskId,
            },
          })
          .then(res => {
            this.debugLoading = false;
            if (res.result) {
              if (res.data.status !== 'failure' && res.data.status !== 'success') {
                clearTimeout(timeId);
                this.debugStatusInfo = res.data;
                this.debugerPolling(taskId);
              } else {
                // 如果成功或者失败，只展示一条内容
                this.debugStatusInfo = res.data;
                this.isDebug = false; // 调试结束
                this.debuged = true;
                this.$store.commit(
                  'udf/saveDevParams',
                  Object.assign(this.devlopParams, { calculateType: this.calculateType })
                );
                res.data.status === 'success' && this.$emit('dubugFinish', true); // 触发调试成功事件
              }
            } else {
              this.debugStatusDown();
              postMethodWarning(res.message, 'error');
            }
          })
          ['catch'](err => {
            this.debugStatusDown();
            postMethodWarning(err, 'error');
          });
      }, 2000);
    },
    debugStatusDown() {
      this.showDebugStatus = false;
      this.isDebug = false; // 调试结束
      this.debuged = true;
    },
    /* 开始调试 */
    debuger() {
      this.canPolling = true;
      if (this.isDebug) {
        // 正在调试，此时按下停止调试
        this.canPolling = false;
        this.debugStatusDown();
        return;
      }
      if (this.checkValid()) {
        this.isDebug = true; // 开始调试，禁用按钮
        this.tabelDataToSend.value.forEach((item, index) => {
          // 拿去tableData，确保数据一致
          this.tabelDataToSend.value[index] = Object.values(this.tableData[index]);
        });
        this.bkRequest
          .httpRequest('udf/debugStart', {
            params: {
              function_name: this.devlopParams.func_name,
              calculation_types: this.calculateType,
              debug_data: this.tabelDataToSend,
              result_table_id: this.results.selected,
              sql: this.sql,
            },
          })
          .then(res => {
            if (res.result) {
              this.taskId = res.data.task_id;
              this.showDebugStatus = true;
              this.debugLoading = true;
              this.debugerPolling(this.taskId); // 开始轮询
            } else {
              postMethodWarning(res.message, 'error');
              this.isDebug = false;
            }
          });
      }
    },
    /*  选择结果数据表下拉选择  */
    async resultsChange() {
      this.getResultList();
      this.bkRequest
        .httpRequest('udf/getDebugData', {
          query: {
            result_table_id: this.results.selected,
          },
        })
        .then(res => {
          if (res.result) {
            this.formatTableData(res.data);
          } else {
            postMethodWarning(res.message, 'error');
          }
        });
    },

    /**
     * 格式化表格数据，这里的表格数据结构比较特殊
     */
    formatTableData(data) {
      this.debug_data = data;
      this.tabelHeaderOptions = data.schema.map(item => item.field_name);
      this.curHeaderLength = data.schema.length;
      // 初始化表格&表头
      this.tabelDataToSend = JSON.parse(JSON.stringify(this.debug_data));
      this.tabelHeaderSet();
    },

    /*
                获取选择结果表下拉
            */
    async getResultsList(bizId) {
      this.results.resultList = [];
      this.results.placeholder = this.$t('数据加载中');
      this.results.isLoading = true;
      await this.bkRequest
        .httpRequest('meta/getUDFResultTables', { query: { bk_biz_id: bizId } })
        .then(res => {
          if (res.result) {
            /** 用于触发select的赋值 */
            const preSelected = this.results.selected;
            this.results.selected = '';
            this.results.resultList = res.data;
            this.results.selected = preSelected;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.results.placeholder = this.$t('请选择');
          this.results.isLoading = false;
        });
    },
    /** 选择业务 */
    bizChange(bizid) {
      this.getResultsList(bizid);
    },
    getIconTop(idx) {
      let top = (idx + 1) * 42 + 18;
      return `top: ${top}px`;
    },
    handleExpOption(method, index) {
      if (method === 'add') {
        this.tabelDataToSend.value.push(this.tabelDataToSend.value[0].slice(0));
        this.debug_data.value.push(this.tabelDataToSend.value[0].slice(0));
      } else {
        if (this.tabelDataToSend.value.length === 1) return;
        this.tabelDataToSend.value.splice(index, 1);
      }
    },
    /*
     * 获取业务列表
     */
    getBizList() {
      let actionId = 'result_table.query_data';
      let dimension = 'bk_biz_id';
      this.bizs.isLoading = true;
      this.bkRequest
        .httpRequest('meta/getMineBizs', { params: { action_id: actionId, dimension: dimension } })
        .then(res => {
          if (res.result) {
            this.bizs.placeholder = this.$t('请先选择业务');
            let businesses = res.data;
            if (businesses.length === 0) {
              this.noData = true;
            } else {
              this.noData = false;
              businesses.map(v => {
                this.bizs.list.push({
                  bk_biz_id: parseInt(v.bk_biz_id),
                  bk_biz_name: v.bk_biz_name,
                });
              });
            }
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.bizs.isLoading = false;
        });
    },
    closeTippy() {
      this.$refs.tippy.instance.hide();
    },
  },
};
</script>

<style lang="scss" scoped>
.wrapper {
  .content-block-wrapper:last-child {
    ::v-deep .block-content {
      padding-bottom: 25px;
    }
  }
  .block-item {
    text-align: left;
    &.start-align {
      align-items: flex-start;
    }
    .input-params-wrapper,
    .output-params-wrapper {
      display: flex;
      flex-wrap: wrap;
    }
    .lh28 {
      line-height: 28px;
    }
    .items-container {
      display: flex;
      flex-wrap: wrap;
    }
    .bk-bable {
      .cell {
        cursor: pointer;
      }
    }
    &.big-margin-top {
      display: block;
      position: relative;
      width: 562px;
      margin-left: 130px;
      .bk-tooltip {
        float: right;
      }
      .tabelConfig {
        margin: 10px 0;
        span {
          font-size: 14px;
          cursor: pointer;
        }
      }
    }
    .checkboxGroup {
      width: 300px;
      padding-left: 41px;
    }
    .info {
      display: flex;
      align-items: center;
      font-size: 14px;
      font-family: MicrosoftYaHeiUI;
      color: rgba(196, 198, 204, 1);
      line-height: 18px;
      .icon-info {
        color: rgba(196, 198, 204, 1);
        font-size: 20px;
        margin-right: 8px;
      }
    }
    .operateBtn {
      position: absolute;
      left: 548px;
      width: 55px;
      color: rgba(151, 155, 165, 1);
      .icon {
        font-size: 20px;
        cursor: pointer;
      }
      span:first-child {
        margin-right: 7px;
      }
    }
  }
  .debugInfo {
    width: 610px;
    margin: 6px 0 0px 130px;
  }
}
.tabel-config-hedaer {
  padding: 12px 24px 5px 12px;
  border-radius: 2px;
  text-align: left;
}
.table-config-tippyBody {
  padding: 15px;
  width: 100%;
  min-width: 400px;
  min-height: 50px;
  max-height: 400px;
  overflow-y: auto;
  .option-item {
    width: 33.3%;
    display: inline-block;
    margin-bottom: 15px;
    padding-right: 5px;
    ::v-deep .bk-checkbox-text {
      text-overflow: ellipsis;
      white-space: nowrap;
      max-width: 78px;
      overflow: hidden;
    }
  }
}
.table-config-tippyFooter {
  padding: 12px 24px 12px 24px;
  background-color: #fafbfd;
  border-top: 1px solid #dcdee5;
  border-radius: 2px;
  text-align: right;
}
</style>
