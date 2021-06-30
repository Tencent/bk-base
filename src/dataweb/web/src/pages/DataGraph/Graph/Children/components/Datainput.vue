

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
  <div class="input-wrapper bk-scroll-y">
    <div v-if="showTitle"
      class="header">
      {{ $t('数据输入') }}
    </div>
    <div class="content">
      <div class="input-select"
        style="display: none">
        <bkdata-select v-model="selectedInput"
          multiple
          :disabled="hasNodeId"
          :clearable="false"
          style="width: 100%">
          <bkdata-option-group v-for="group in dataInputList"
            :key="group.id"
            :name="group.name">
            <bkdata-option v-for="option in group.children"
              :id="option.id"
              :key="option.id"
              :disabled="option.disabled"
              :name="option.name" />
          </bkdata-option-group>
        </bkdata-select>
      </div>
      <bkdata-collapse v-model="activeInput">
        <bkdata-collapse-item v-for="(input, index) in fieldsList"
          :key="index"
          :name="input.key"
          :hideArrow="true"
          :customTriggerArea="true"
          extCls="data-input-collapse-item">
          <i v-if="activeInput.includes(input.key)"
            v-bk-tooltips="$t('收起')"
            class="icon-fold iconColor" />
          <i v-else
            v-bk-tooltips="$t('展开')"
            class="icon-unfold iconColor" />
          <span class="input-field">{{ input.key }}</span>
          <span slot="no-trigger"
            class="no-trigger-area">
            <i v-bk-tooltips="$t('查看数据字典')"
              class="icon-notebook ml10 iconColor"
              @click="linkToDictionary(input.key)" /><i v-bk-tooltips="$t('展示最近一条数据')"
              class="icon-origin-source ml10 iconColor"
              @click="getLastMsg(input)" />
          </span>
          <div slot="content">
            <ul class="filed-list">
              <li v-for="(field, fieldIdx) in input.fields"
                :key="fieldIdx">
                <div class="title"
                  @click="fileClickHandle(field.name)">
                  <i class="icon icon-dimens" />
                  <span class="text">{{ field.name }}</span>
                </div>
                <div class="type">
                  {{ field.type }}
                </div>
              </li>
            </ul>
            <div v-if="input.msgShow"
              class="last-message-wrapper">
              <fieldset v-bkloading="{ isLoading: input.msgLoading }"
                disabled="disabled">
                <legend>{{ $t('最近一条数据') }}</legend>
                <div class="lastest-data-content">
                  <pre class="data">{{ input.msg }}</pre>
                </div>
              </fieldset>
            </div>
          </div>
        </bkdata-collapse-item>
      </bkdata-collapse>
    </div>
  </div>
</template>

<script>
const beautify = require('json-beautify');
import Bus from '@/common/js/bus.js';
export default {
  props: {
    /** 给定domain，表示在Card下，只能一个list */
    domain: {
      type: String,
      default: '',
    },
    showTitle: {
      type: Boolean,
      default: true,
    },
    resultList: {
      type: Object,
      default: () => ({}),
    },
    params: {
      type: Object,
      default: () => ({}),
    },
    dataInputList: {
      type: Array,
      default: () => [],
    },
    inputData: {
      type: Array,
      default: () => [],
    },
    hasNodeId: {
      type: Boolean,
      default: false,
    },
    inputFieldsList: {
      type: Array,
      default: () => [],
    },
  },
  data() {
    return {
      activeInput: [],
      selectedInput: [],
      fieldsList: [],
    };
  },
  computed: {
    /** 获取具有多输出的输入项 */
    getMultipleOptionsGroup() {
      return this.dataInputList.filter(group => group.children.length > 1);
    },
  },
  watch: {
    /** watch数据输入变量,保证至少每个输入节点至少选择一个rt */
    selectedInput(val, oldValue) {
      const diffGroup = oldValue && oldValue.filter(item => !val.includes(item));
      diffGroup.length && this.getMultipleOptionsGroup.length && this.inputDataProcess(diffGroup[0]);
    },
    resultList() {
      if (this.selectedInput.length === 0) return;
      this.initFieldsList();
    },
  },
  mounted() {
    Bus.$on('nodeConfigBack', this.initSelectedInput);
  },
  methods: {
    initFieldsList() {
      /** 字段列表，根据所选输入生成 */
      if (JSON.stringify(this.resultList) === '{}') return; // 旧节点此时可能还未获取resultList

      const dataInput = this.selectedInput.reduce((acc, cur) => {
        if (this.domain) {
          this.domain === cur.split('(')[0] && acc.push(this.domain);
        } else {
          acc.push(cur.split('(')[0]);
        }
        return acc;
      }, []);

      for (let [key, value = {}] of Object.entries(this.resultList)) {
        if (dataInput.includes(key)) {
          this.fieldsList.push({
            key: key,
            fields: value.fields,
            msg: window.$t('暂无数据'),
            msgLoading: false,
            msgShow: false,
          });
        }
      }

      const fieldsList = this.fieldsList
        .map(item => {
          return item.fields.map(item => {
            return {
              name: item.name,
              type: item.type,
            };
          });
        })
        .flat();

      this.$emit('update:inputFieldsList', Array.from(new Set(fieldsList)));

      /** 自动展开第一个输入表结构 */
      this.activeInput = [this.fieldsList[0].key];
    },
    linkToDictionary(id) {
      const routeUrl = this.$router.resolve({
        name: 'DataDetail',
        query: {
          dataType: 'result_table',
          result_table_id: id,
        },
      });
      window.open(routeUrl.href, '_blank');
    },
    fileClickHandle(field) {
      Bus.$emit('insert-monaco', field);
    },
    getLastMsg(input) {
      if (input.msgLoading || input.msgShow) {
        if (input.msgShow) input.msgShow = !input.msgShow;
        return;
      }
      const rtId = input.key;

      this.activeInput.push(rtId);
      input.msgShow = true;
      input.msgLoading = true;
      this.bkRequest
        .httpRequest('dataFlow/getRtLastMsg', {
          query: {
            result_table_id: rtId,
            node_type: 'flink_streaming',
          },
        })
        .then(res => {
          if (res.result) {
            const data = res.data.data;
            console.log(data, Object.keys(data));
            input.msg = Object.keys(data).length ? beautify(this.extractObj(data), null, 4, 80) : this.$t('暂无数据');
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          input.msgLoading = false;
        });
    },
    extractObj(str) {
      try {
        return JSON.parse(str);
      } catch (e) {
        return str;
      }
    },
    inputDataProcess(diffGroup) {
      const group = this.getMultipleOptionsGroup.find(group => group.children.includes(diffGroup));
      const index = this.selectedInput.findIndex(item => group.children.includes(item));

      /** 如果对应组别的没有一个被选中的，强制将当前剔除的option塞入 */
      if (index === -1) {
        setTimeout(() => {
          this.selectedInput.push(diffGroup);
        }, 0);
      }
    },
    /** 根据from_nodes初始化默认选项 */
    initSelectedInput() {
      const fromNodes = this.params.from_nodes || this.params.inputs;
      fromNodes.forEach(item => {
        this.selectedInput.push(`${item.from_result_table_ids}(${item.id})`);
      });

      this.initFieldsList();
    },
  },
};
</script>

<style lang="scss" scoped>
.input-wrapper {
  width: 100%;
  height: 100%;
  background: #fff;
  padding: 17px 24px 24px 24px;
  overflow-y: auto;
  border-right: 1px solid #dcdee5;
  .header {
    width: 64px;
    height: 22px;
    font-size: 16px;
    font-family: PingFangSC, PingFangSC-Medium;
    font-weight: bold;
    text-align: left;
    color: #313238;
    line-height: 22px;
    margin-bottom: 9px;
  }
  ::v-deep .content {
    .input-select {
      width: 100%;
      margin-top: 30px;
      margin-bottom: 10px;
    }
    .data-input-collapse-item > .bk-collapse-item-header {
      padding: 0;
      display: flex;
      align-items: center;
      font-size: 0;
      .trigger-area {
        display: flex;
        align-items: center;
        overflow: hidden;
      }
      i,
      span {
        font-size: 14px;
      }
      .input-field {
        margin-left: 10px;
        display: inline-block;
        width: 100%;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
      }
      .no-trigger-area {
        i {
          cursor: pointer;
          &:hover {
            color: #3a84ff;
          }
        }
      }
    }
    .bk-collapse-item-content {
      padding: 0;
      .filed-list {
        margin-left: 27px;
        li {
          display: flex;
          justify-content: space-between;
          line-height: 32px;
          .title {
            font-size: 0;
            flex-grow: 1;
            width: 0;
            display: flex;
            align-items: center;
            cursor: pointer;
            span,
            i {
              font-size: 14px;
            }
            .icon {
              color: #979ba5;
              margin-right: 10px;
            }
            .text {
              color: #63656e;
              text-overflow: ellipsis;
              overflow: hidden;
              white-space: nowrap;
              width: calc(100% - 24px);
            }
          }
          .type {
            font-size: 14px;
            color: #c4c6cc;
            width: 48px;
            margin-left: 5px;
            text-align: right;
          }
        }
      }
    }
    fieldset {
      border-color: #dde4eb;
      border-radius: 2px;

      legend {
        padding: 10px;
      }
    }

    .lastest-data-content {
      word-wrap: break-word;
      padding: 0 10px;
      width: 100%;
      border-radius: 5px;
      min-height: 150px;
    }
  }
}
.iconColor {
  color: #979ba5;
}
</style>
