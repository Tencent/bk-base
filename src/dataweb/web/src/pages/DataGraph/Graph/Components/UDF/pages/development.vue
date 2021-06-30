

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
  <div class="wrapper">
    <block-Content :header="$t('函数配置')">
      <div class="block-item">
        <span class="desp">{{ $t('开发语言') }}</span>
        <bkdata-radio-group v-model="params.func_language">
          <bkdata-radio :value="'java'">
            Java
          </bkdata-radio>
          <bkdata-radio :value="'python'">
            Python
          </bkdata-radio>
        </bkdata-radio-group>
      </div>
      <div class="block-item">
        <span class="desp">{{ $t('函数类型') }}</span>
        <bkdata-radio-group v-model="params.func_udf_type">
          <bkdata-radio v-bk-tooltips="$t('UDF介绍')"
            :value="'udf'">
            UDF
          </bkdata-radio>
          <bkdata-radio v-bk-tooltips="$t('UDTF介绍')"
            :value="'udtf'">
            UDTF
          </bkdata-radio>
          <bkdata-radio v-bk-tooltips="$t('UDAF介绍')"
            :value="'udaf'">
            UDAF
          </bkdata-radio>
        </bkdata-radio-group>
      </div>
      <div class="block-item">
        <span class="desp required">{{ $t('函数名称') }}</span>
        <bkdata-input v-model="namePrefix"
          :disabled="true"
          style="width: 60px; margin-right: 5px" />
        <bkdata-input
          v-model="nameSufffix"
          v-tooltip.notrigger="validate['func_name']"
          :placeholder="$t('SQL中使用函数')"
          :disabled="initStatus"
          style="width: 431px"
          @blur="funcNameEdited" />
      </div>
      <div class="block-item block-item-prefix">
        <div class="content-fix">
          {{ namePrefix }}{{ nameSufffix }}
        </div>
      </div>
      <div class="block-item">
        <span class="desp required">{{ $t('函数中文名称') }}</span>
        <bkdata-input
          v-model="params.func_alias"
          v-tooltip.notrigger="validate['func_alias']"
          :placeholder="$t('中文名称')"
          style="width: 496px" />
      </div>
      <div class="block-item start-align">
        <span class="desp required"
          style="line-height: 32px">
          {{ $t('输入参数类型') }}
        </span>
        <div class="inputsWrapper">
          <div v-for="(input, xindex) in params.input_type"
            :key="xindex"
            class="input-group">
            <bkdata-selector
              v-tooltip.notrigger="input.validate"
              :selected.sync="input.content"
              style="width: 224px"
              :list="paramsTypeList"
              :settingKey="'name'"
              :displayKey="'name'" />
            <span
              v-if="xindex === params.input_type.length - 1"
              class="icon-plus-circle-shape add-content"
              @click="handleAddorDeleteInput('input_type', 'add', xindex)" />
            <span
              v-if="params.input_type.length !== 1"
              :class="[
                'icon-minus-circle-shape',
                'minus-content',
                { 'current-line': xindex === params.input_type.length - 1 },
              ]"
              @click="handleAddorDeleteInput('input_type', 'delete', xindex)" />
          </div>
        </div>
      </div>
      <div class="block-item start-align">
        <span class="desp required"
          style="line-height: 32px">
          {{ $t('函数返回类型') }}
        </span>
        <div class="returnsWrapper">
          <div v-for="(item, xindex) in params.return_type"
            :key="xindex"
            class="output-group">
            <bkdata-selector
              v-tooltip.notrigger="item.validate"
              :selected.sync="item.content"
              style="width: 224px"
              :list="paramsTypeList"
              :settingKey="'name'"
              :displayKey="'name'" />
            <span
              v-if="xindex === params.return_type.length - 1"
              class="icon-plus-circle-shape add-content"
              @click="handleAddorDeleteInput('return_type', 'add', xindex)" />
            <span
              v-if="params.return_type.length !== 1"
              :class="[
                'icon-minus-circle-shape',
                'minus-content',
                { 'current-line': xindex === params.return_type.length - 1 },
              ]"
              @click="handleAddorDeleteInput('return_type', 'delete', xindex)" />
          </div>
        </div>
      </div>
      <div class="block-item">
        <span class="desp required">{{ $t('函数说明') }}</span>
        <bkdata-input
          v-model="params.explain"
          v-tooltip.notrigger="validate['explain']"
          :placeholder="$t('函数作用说明描述')"
          style="width: 496px"
          @focus="getCode" />
      </div>
      <div class="block-item">
        <span class="desp required">{{ $t('使用样例') }}</span>
        <bkdata-input
          v-model="params.example"
          v-tooltip.notrigger="validate['example']"
          :placeholder="$t('函数的SQL使用示例')"
          style="width: 496px"
          @focus="getCode"
          @blur="checkSqlExample" />
        <div v-show="sqlChecking"
          class="info">
          <loading-ui size="mini" />
          <span>{{ $t('校验中') }}</span>
        </div>
      </div>
      <div class="block-item">
        <span class="desp required">{{ $t('样例返回') }}</span>
        <bkdata-input
          v-model="params.example_return_value"
          v-tooltip.notrigger="validate['example_return_value']"
          :placeholder="$t('实例返回值')"
          style="width: 496px"
          @focus="getCode" />
      </div>
    </block-Content>
    <block-Content :header="$t('代码开发')">
      <div v-if="params.func_language === 'python'"
        class="pythonModule">
        <div class="block-item"
          style="width: 715px">
          <span class="desp"> {{ $t('支持Python包') }}</span>
          <bkdata-table :data="pythonModules"
            :maxHeight="200"
            :emptyText="$t('暂无支持包信息')">
            <bkdata-table-column :label="$t('包名')"
              :width="'100px'"
              prop="name" />
            <bkdata-table-column :label="$t('版本号')"
              :width="'100px'"
              prop="version" />
            <bkdata-table-column
              :label="$t('描述')"
              :minWidth="'120px'"
              :showOverflowTooltip="true"
              prop="description" />
          </bkdata-table>
        </div>
        <div class="block-item">
          <div class="monaco-wrapper"
            style="width: 700px; margin-left: 23px">
            <Monaco
              height="215px"
              language="python"
              theme="vs-dark"
              :options="monacoOptions"
              :tools="tools"
              :code="params.code_config.code"
              @codeChange="code => editorChange(code)" />
          </div>
        </div>
      </div>
      <div v-else
        class="javaModule">
        <div class="block-item start-align">
          <span class="desp"
            style="line-height: 36px">
            <span>{{ $t('选填') }}</span>
            {{ $t('依赖配置') }}
          </span>
          <div class="config-wrapper"
            style="width: 600px">
            <ul>
              <li
                :is="item.component"
                v-for="(item, idx) in items"
                :key="idx"
                :index="item.index"
                :params.sync="params.code_config.dependencies[idx]"
                @remove="removeContigBlock" />
            </ul>
            <add-config-block @addBlock="addConfigBlock" />
          </div>
        </div>
        <div class="block-item">
          <div class="monaco-wrapper"
            style="width: 708px; margin-left: 23px">
            <Monaco
              height="215px"
              language="java"
              theme="vs-dark"
              :options="monacoOptions"
              :tools="tools"
              :code="params.code_config.code"
              @codeChange="code => editorChange(code)" />
          </div>
        </div>
      </div>
    </block-Content>
  </div>
</template>

<script>
import Monaco from '@/components/monaco';
import LoadingUi from '@/components/loading/loading.default';
import blockContent from '../components/contentBlock';
import devConfig from '../components/devpConfig';
import addConfigBlock from '../components/addConfigBlock';
import { mapGetters } from 'vuex';
import Bus from '@/common/js/bus.js';
import { showMsg, postMethodWarning } from '@/common/js/util.js';
import { validateScope } from '@/common/js/validate.js';
export default {
  components: { blockContent, Monaco, devConfig, addConfigBlock, LoadingUi },
  data() {
    return {
      monacoOptions: {
        readOnly: false,
        fontSize: 10,
        lineNumbersMinChars: 4,
      },
      hasSqlChecked: false,
      sqlChecking: false,
      isSetBacking: false,
      pythonModules: [],
      namePrefix: 'udf_',
      nameSufffix: '',
      validate: {
        func_name: {
          regs: [
            { required: true, error: window.$t('不能为空') },
            {
              regExp: /^[a-z][a-z|0-9|_]*$/,
              error: window.$t('由英文字母_下划线和数字组成_且字母开头_不包含大写字母'),
            },
          ],
          content: '',
          visible: false,
          class: 'error-red',
        },
        func_alias: {
          regs: [{ required: true, error: window.$t('不能为空') }],
          content: '',
          visible: false,
          class: 'error-red',
        },
        explain: {
          regs: [{ required: true, error: window.$t('不能为空') }],
          content: '',
          visible: false,
          class: 'error-red',
        },
        example: {
          regs: [{ required: true, error: window.$t('不能为空') }],
          content: '',
          visible: false,
          class: 'error-red',
        },
        example_return_value: {
          regs: [{ required: true, error: window.$t('不能为空') }],
          content: '',
          visible: false,
          class: 'error-red',
        },
      },
      isFirstValidata: true,
      paramsInit: false,
      canGetCode: false,
      index: 1,
      items: [],
      tools: {
        enabled: true,
        title: this.$t('逻辑开发'),
        guidUrl: this.$store.getters['docs/getNodeDocs'].udf,
        toolList: {
          format_sql: false,
          view_data: false,
          guid_help: true,
          full_screen: true, // 是否启用全屏设置
          event_fullscreen_default: true, // 是否启用默认的全屏功能，如果设置为false需要自己监听全屏事件组后续处理
        },
      },
      params: {
        func_language: 'java',
        func_udf_type: 'udf',
        func_name: '',
        func_alias: '',
        input_type: [
          {
            index: 0,
            content: 'string',
            validate: {
              visible: false,
              content: this.$t('不能为空'),
              class: 'error-red',
            },
          },
        ],
        return_type: [
          {
            index: 0,
            content: 'string',
            validate: {
              visible: false,
              content: this.$t('不能为空'),
              class: 'error-red',
            },
          },
        ],
        explain: '',
        example: '',
        example_return_value: '',
        code_config: {
          dependencies: [],
          code: '',
        },
        support_framework: [],
      },
      functionType: 'UDF',
      paramsTypeList: [
        { id: 1, name: 'string' },
        { id: 2, name: 'long' },
        { id: 3, name: 'int' },
        { id: 4, name: 'double' },
        { id: 4, name: 'float' },
      ],
    };
  },
  computed: {
    ...mapGetters({
      initStatus: 'udf/initStatus',
      devlopParams: 'udf/devlopParams',
    }),
    paramsToSend() {
      let inputs = this.params.input_type.map(item => item.content);
      let returns = this.params.return_type.map(item => item.content);
      return Object.assign(JSON.parse(JSON.stringify(this.params)), { input_type: inputs }, { return_type: returns });
    },
  },
  watch: {
    params: {
      deep: true,
      handler(params) {
        if (
          params.func_language
          && params.func_udf_type
          && params.input_type[0].content
          && params.return_type[0].content
          && params.func_name
          && params.func_alias
          && !this.paramsInit
        ) {
          this.canGetCode = true;
        }
      },
    },
    nameSufffix() {
      this.params.func_name = this.namePrefix + this.nameSufffix;
    },
    'params.func_language'(value) {
      let params = this.params;
      if (
        params.func_udf_type
        && params.input_type[0].content
        && params.return_type[0].content
        && params.func_name
        && params.func_alias
        && !this.isSetBacking
      ) {
        this.canGetCode = true;
        this.getCode();
      }
      if (value === 'python') {
        this.bkRequest
          .httpRequest('udf/getPythonModule')
          .then(res => {
            if (res.result) {
              this.pythonModules = res.data;
            } else {
              postMethodWarning(res.message, 'error');
            }
          })
          ['catch'](err => {
            postMethodWarning(err, 'error');
          });
      }
    },
  },

  mounted() {
    if (this.initStatus) {
      //  配置回填
      this.isSetBacking = true; // 回填不允许重新拉去code
      this.hasSqlChecked = true; // 回填后，sql不需要在函数名称修改后重新验证
      this.paramsInit = true; // 回填后，不需要再重新拉取框架代码，防止覆盖用户代码
      let inputs = this.devlopParams.input_type.map((item, index) => {
        return {
          index: index,
          content: item,
          validate: {
            visible: false,
            content: this.$t('不能为空'),
            class: 'error-red',
          },
        };
      });
      let returns = this.devlopParams.return_type.map((item, index) => {
        return {
          index: index,
          content: item,
          validate: {
            visible: false,
            content: this.$t('不能为空'),
            class: 'error-red',
          },
        };
      });
      let dependencies = this.devlopParams.code_config.dependencies || [];
      let codeConfig = Object.assign(this.devlopParams.code_config, { dependencies: dependencies });
      this.nameSufffix = this.devlopParams.func_name.split('udf_')[1];
      Object.assign(this.params, this.devlopParams, {
        input_type: inputs,
        return_type: returns,
        code_config: codeConfig,
      });
      setTimeout(() => {
        for (let i = 0; i < dependencies.length; i++) {
          this.items.push({
            component: devConfig,
            index: this.index++,
          });
        }
        this.isSetBacking = false;
      }, 0);
    }
  },
  methods: {
    funcNameEdited() {
      if (this.hasSqlChecked) return; //if (this.hasSqlChecked || !this.params.example) return;
      this.validate.func_name.visible = false;
      this.$forceUpdate();
      var regex = /^[a-z][a-z|0-9|_]*$/;
      if (!this.nameSufffix) {
        this.validate.func_name.visible = true;
        this.validate.func_name.content = this.$t('不能为空');
        this.$forceUpdate();
        return;
      } else if (!regex.test(this.nameSufffix)) {
        this.validate.func_name.visible = true;
        this.validate.func_name.content = this.$t('由英文字母_下划线和数字组成_且字母开头_不包含大写字母');
        this.$forceUpdate();
        return;
      }
    },
    sqlStatusChange(status) {
      this.sqlChecking = status;
      this.$emit('sqlChecked', status);
    },
    checkSqlExample() {
      let isValidate = true;
      // if (!this.nameSufffix) {
      //   this.validate.func_name.visible = true;
      //   this.validate.func_name.content = this.$t('不能为空');
      //   this.$forceUpdate();
      //   return;
      // }
      this.validate.example.visible = false;
      this.sqlStatusChange(true);
      return this.bkRequest
        .httpRequest('udf/checkUdfSql', {
          params: {
            function_name: this.paramsToSend.func_name,
          },
          query: {
            sql: this.params.example,
          },
        })
        .then(res => {
          if (res.result) {
            this.validate.example.visible = false;
          } else {
            this.validate.example.content = res.message;
            this.validate.example.visible = true;
            isValidate = false;
            postMethodWarning(res.message, 'error');
          }
          return isValidate;
        })
        ['finally'](() => {
          this.$forceUpdate();
          this.sqlStatusChange(false);
        });
    },
    editorChange(code) {
      this.params.code_config.code = code;
    },
    getCode() {
      if (this.canGetCode) {
        let targartParams = this.paramsToSend;
        this.bkRequest
          .httpRequest('udf/getUDFCodeFrame', {
            params: {
              function_name: targartParams.func_name,
            },
            query: {
              func_language: targartParams.func_language,
              func_udf_type: targartParams.func_udf_type,
              input_type: targartParams.input_type,
              return_type: targartParams.return_type,
            },
          })
          .then(res => {
            if (res.result) {
              this.params.code_config.code = res.data.code;
              this.paramsInit = true;
            } else {
              postMethodWarning(res.message, 'error');
            }
          });
        this.canGetCode = false;
      }
    },
    handleAddorDeleteInput(type, mode, index) {
      let inputs = this.params[type];
      if (mode === 'delete') {
        inputs.forEach((item, idx) => {
          if (idx === index && inputs.length > 1) {
            inputs.splice(idx, 1);
          }
        });
      } else if (mode === 'add') {
        if (type === 'return_type' && this.params.func_udf_type !== 'udtf') {
          postMethodWarning(this.$t('当前函数类型只允许一个返回值'), 'warning');
        } else {
          inputs.push({
            index: inputs.length,
            content: 'string',
            validate: {
              visible: false,
              content: this.$t('不能为空'),
              class: 'error-red',
            },
          });
        }
      }
    },
    addConfigBlock() {
      this.items.push({
        component: devConfig,
        index: this.index++,
      });
      this.params.code_config.dependencies.push({
        group_id: '',
        artifact_id: '',
        version: '',
      });
    },
    removeContigBlock(index) {
      let target = this.items.find(item => item.index === index);
      let circle = this.index--;
      this.items = this.items.filter(item => item.index !== target.index);
      this.params.code_config.dependencies.splice(target.index - 1, 1);

      // 防止从中间删除，index序号跳断， 每次删除后重新排序
      for (let i = 0; i < circle; i++) {
        if (this.items[i]) {
          this.items[i].index = i + 1;
        }
      }
    },
    valiedateInputsOrReturn(type) {
      let isValidate = true;
      this.params[type].forEach(item => {
        if (item.content === '') {
          item.validate.visible = true;
          isValidate = false;
        } else {
          item.validate.visible = false;
        }
      });
      return isValidate;
    },
    async validateForm() {
      let isValidate = true;
      if (!this.valiedateInputsOrReturn('input_type')) {
        isValidate = false;
      }
      if (!this.valiedateInputsOrReturn('return_type')) {
        isValidate = false;
      }
      if (!validateScope(this.params, this.validate)) {
        isValidate = false;
      }
      isValidate = await this.checkSqlExample();
      this.$forceUpdate();
      return isValidate;
    },
  },
};
</script>

<style lang="scss" scoped>
.wrapper {
  .block-item {
    text-align: left;
    &.start-align {
      align-items: flex-start;
    }
    .info {
      margin-left: 10px;
    }
    .devTitle {
      position: absolute;
      left: 15px;
      top: auto;
    }
    .input-group,
    .output-group {
      margin-right: 15px;
      display: flex;
      align-items: center;
      .bk-select {
        margin-bottom: 5px;
      }
      .bk-select:last-child {
        margin: 0;
      }
      span {
        cursor: pointer;
        font-size: 17px !important;
        height: 17px;
        &.add-content {
          margin-left: 15px;
        }
        &.minus-content {
          margin-left: 15px;
        }
        &.current-line {
          margin-left: 6px;
        }
      }
    }
  }
  .block-item-prefix {
    margin-left: 130px;
    width: 496px;
    height: 32px;
    color: white;
    background: rgb(58, 132, 255);
    margin-top: -15px;
    border-radius: 2px;
    margin-bottom: 15px;
    .content-fix {
      color: #ffffff;
      padding-left: 10px;
    }
  }
}
.pythonModule {
  margin: 0 15px 25px 0px;
  text-align: left;
}
</style>
