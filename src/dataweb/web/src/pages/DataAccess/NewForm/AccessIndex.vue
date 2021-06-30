

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
  <Layout :crumbName="[{ name: $t('数据源列表'), to: '/data-access/' }, { name: formTitle }]">
    <div class="btn-back">
      <bkdata-button
        id="return-btn"
        size="small"
        theme="primary"
        @click="$router.push({ path: $route.query.from || '/data-access/' })">
        {{ returnText }}
      </bkdata-button>
    </div>

    <AccessDataSource
      :data-scenario-id="formData.data_scenario_id"
      @getAccessTypeStatus="handleLoadingStatus"
      @dataSourceLoaded="handleDataSourceLoaded" />
    <AccessRow
      v-bkloading="{ isLoading: joinLoading }"
      :collspan="false"
      :step="1"
      :stepName="$t('接入类型')"
      class="bk-access row">
      <div class="access-types-container">
        <template v-if="isEditForm">
          <AccessForm class="bk-access-types diaplay-mode"
            formTitle>
            <div class="bk-form-items display">
              <span>{{ activeItem.data_scenario_alias || $t('加载中') }}</span>
            </div>
          </AccessForm>
        </template>
        <template v-else>
          <template v-for="(item, index) in accessTypes">
            <AccessForm
              v-if="accessTypeList[item.id] && accessTypeList[item.id].length"
              :key="index"
              :class="['bk-access-types', item.id]"
              :formTitle="$t(item.name)"
              titlePosition="center">
              <div :class="['bk-form-items', item.id]">
                <div v-for="list of accessTypeList[item.id]"
                  :key="list.data_src_item"
                  class="type">
                  <label
                    :class="['bk-form-radio', (formData.data_scenario === list.data_scenario_name && 'active') || '']"
                    :disabled="!list.active || isEditForm"
                    :for="list.data_src_item">
                    <input
                      :id="list.data_src_item"
                      v-model="formData.data_scenario"
                      :disabled="!list.active || isEditForm"
                      :value="list.data_scenario_name"
                      class="hide-input"
                      name="AccessTypeItem"
                      type="radio">
                    <!-- <i class="scenario-type"></i> -->
                    <i class="bk-radio-text">{{ list.data_scenario_alias }}</i>
                  </label>
                </div>
              </div>
            </AccessForm>
          </template>
        </template>
      </div>
      <div v-if="isScenarioChecked"
        class="access-instructions">
        <h3 class="title">
          {{ $t('接入说明') }}
        </h3>
        <div class="info">
          <p v-for="(tip, index) in rightTips.content"
            :key="index">
            <template v-if="tip === '<hyperLink>'">
              <a :href="rightTips['hyperLink'].href"
                @click="() => rightTips['hyperLink'].onClick()">
                {{ rightTips['hyperLink'].text }}
              </a>
            </template>
            <template v-else-if="tip.indexOf('<hyperLink>') >= 0">
              <!--eslint-disable-next-line-->
              <section v-html="getHyperLinkHtml(tip, rightTips)" />
            </template>
            <template v-else-if="tip === 'HttpRequest'">
              <template v-for="item in ipList">
                <section :key="item.id">
                  {{ item[rightTips.httpRequest.listKey] }}
                </section>
              </template>
            </template>
            <template v-else>
              {{ tip }}
            </template>
          </p>
        </div>
        <!-- <p>数据流示意图：</p>
                <div class="instructions-image">
                    <img :src="accessInstructions.image"
                         alt="">
                </div>-->
      </div>
    </AccessRow>
    <AccessRow
      id="define"
      :isActive="isScenarioChecked"
      :isOpen="isScenarioChecked || !isFormValidate"
      :step="2"
      :stepName="$t('数据信息')"
      class="bk-access row">
      <AccessForm :formTitle="$t('数据定义')"
        class="bk-access-define mb30">
        <span class="bk-item-des">{{ $t('对数据定义的描述或者说明信息') }}</span>
        <DataDefine
          :scenarioType="formData.data_scenario"
          :scenarioId="formData.data_scenario_id"
          :encodeLists="dataSources['encodeList']"
          :schemaConfig="dataDefineSchema"
          @showDataTag="handleShowDataTag"
          @bizIdSekected="handleBizIdSelected" />
      </AccessForm>
      <template v-if="activeTypeSchema.reporting && activeTypeSchema.reporting.enabled">
        <AccessForm :formTitle="$t('上报说明')"
          class="bk-access-define mb30 t5">
          <span class="bk-item-des">
            {{ $t('自定义接入上报说明') }}，<a
              target="_blank"
              :href="$store.getters['docs/getPaths'].customAccessReporting">
              {{ $t('参考文档') }}
            </a>
          </span>
        </AccessForm>
      </template>
      <template v-if="!accessObjectSchema.disable">
        <AccessForm :formTitle="$t('接入对象')"
          class="bk-access-object">
          <messageContainer
            :ipList="ipList"
            :show-data-tag="showDataTag"
            :formData="formData"
            @changeScenario="changeScenario" />
        </AccessForm>
      </template>
      <template v-if="!accessMethodSchema.disable">
        <AccessForm :formTitle="$t('接入方式')"
          class="bk-access-method mb30">
          <span class="bk-item-des">{{ $t('设置数据接入同步方式和参数') }}</span>
          <div class="bk-form-items">
            <template v-if="isActiveTypeSchemaExist">
              <AccessMethod :schemaConfig="accessMethodSchema" />
            </template>
            <template v-else>
              <AccessEmpty />
            </template>
          </div>
        </AccessForm>
      </template>
      <AccessForm v-if="formData.data_scenario === 'http'"
        :formTitle="$t('请求示例')"
        class="bk-access-filter mb30">
        <div class="bk-form-items">
          <RequestExample :bizid="formData.bk_biz_id" />
        </div>
      </AccessForm>
      <AccessForm
        v-if="!filterConditionSchema.disable"
        :formTitle="$t('过滤条件')"
        :subTitle="$t('选填')"
        class="bk-access-filter mb30">
        <div class="bk-form-items">
          <bkdata-collapse>
            <bkdata-collapse-item class="bk-position-relative"
              contentHiddenType="hidden">
              <span class="bk-item-des">{{ $t('数据采集_过滤条件描述') }}</span>
              <div slot="content">
                <FilterCondition :configJson="filterConditionSchema"
                  :modeType="formData.data_scenario" />
              </div>
            </bkdata-collapse-item>
          </bkdata-collapse>
        </div>
      </AccessForm>
    </AccessRow>
    <AccessRow
      id="auth"
      :isActive="formData.data_scenario !== ''"
      :isOpen="!isFormValidate"
      :step="3"
      :stepName="$t('数据权限')"
      class="bk-access row">
      <AccessForm :formTitle="$t('数据管理员')"
        class="bk-access-permission">
        <span class="bk-item-des">{{ $t('默认为配置平台的_业务运维_和_运营规划_角色') }}</span>
        <DataPermission :bizId="formData.bk_biz_id"
          :isEdit="!!isEditForm" />
      </AccessForm>
      <AccessForm :formTitle="$t('数据敏感度')"
        class="bk-access-datasense mt30">
        <span class="bk-item-des">
          {{ $t('对数据敏感度的一个说明_并给出不同级别的权限对照表_请参考') }}
          <a :href="$store.getters['docs/getDocsUrl'].dataPermission"
            class="data-auth"
            target="_blank">
            {{ $t('数据权限') }}
          </a>
        </span>
        <DataSense :bizId="formData.bk_biz_id" />
      </AccessForm>
    </AccessRow>
    <AccessRow
      id="marks"
      :collspan="false"
      :step="0"
      :stepName="$t('备注')"
      :subTitle="$t('选填')"
      class="bk-access row">
      <AccessForm class="bk-access-define">
        <Remarks />
      </AccessForm>
    </AccessRow>
    <!-- 数据清洗测试 start -->
    <div class="submit">
      <bkdata-button
        :class="['bk-button', !isScenarioChecked && 'is-disabled']"
        :disabled="!isScenarioChecked || isReadonlyMode"
        :loading="isLoading"
        theme="primary"
        @click="submit">
        {{ $t('提交') }}
      </bkdata-button>
      <bkdata-button v-bk-tooltips="$t('功能实现中_敬请期待')"
        class="bk-button bk-primary is-disabled">
        {{ $t('保存为模板') }}
      </bkdata-button>
    </div>
  </Layout>
</template>
<script>
import Layout from '../../../components/global/layout';
import AccessRow from './AccessRow';
import AccessDataSource from './AccessDataSource';
import AccessForm from './AccessForm';
import getFormSchemaConfig from './SubformConfig/formSchema.js';
import { ACCESS_METHODS, ACCESS_OPTIONS } from './Constant/index';
import { validateComponentForm } from './SubformConfig/validate.js';
import {
  DataDefine,
  AccessMethod,
  FilterCondition,
  DataPermission,
  Remarks,
  DataSense,
  RequestExample,
  AccessEmpty,
} from './FormItems/index.js';
import messageContainer from './messageContainer';
// import { accessTypes } from '../../dataManage/datadetailChildren/config'
export default {
  components: {
    Layout,
    AccessRow,
    AccessDataSource,
    AccessForm,
    DataDefine,
    AccessMethod,
    FilterCondition,
    DataPermission,
    Remarks,
    DataSense,
    RequestExample,
    AccessEmpty,
    messageContainer,
  },
  data() {
    return {
      /** 数据标签弹窗打开 */
      showDataTag: false,
      debugShow: false,
      isLoading: false,
      ipList: [],
      accessTypes: [
        { name: '通用', id: 'common' },
        { name: '其他', id: 'other' },
      ],
      dataSources: {},
      formData: {
        data_scenario_id: 0,
        data_scenario: '',
        bk_biz_id: 0,
        description: '',
        bk_username: this.$store.getters.getUserName,
        permission: 'all',
      },
      formConfig: {},
      accessInstructions: {
        description: '',
        authorizationInfor: '',
        image: '../../../common/images/no-data.png',
      },

      isFormValidate: true,
      joinLoading: false,
      scenario: '',
    };
  },
  computed: {
    returnText() {
      if (this.$route.query.from && this.$route.query.from.indexOf('data-detail') >= 0) {
        return this.$t('返回详情');
      }
      return this.$t('返回列表');
    },
    activeItem() {
      let item = {};
      Object.keys(this.accessTypeList).forEach(key => {
        let _item = this.accessTypeList[key].find(list => this.formData.data_scenario === list.data_scenario_name);
        if (_item) {
          item = _item;
        }
      });

      return item;
    },
    formTitle() {
      return (this.isEditForm && this.$t('编辑数据源')) || this.$t('新接入数据源');
    },
    /** 是否是编辑页面 */
    isEditForm() {
      return this.rawDataId;
    },

    /** 表单ID */
    rawDataId() {
      return this.$route.params.did;
    },

    /** 计入类型是否已选择 */
    isScenarioChecked() {
      return this.formData.data_scenario !== '';
    },

    /** 数据源：接入类型  */
    accessTypeList() {
      return this.dataSources.accessTypes || {};
    },

    /** 当前选中接入类型的配置文件 */
    activeTypeConfig() {
      return this.formConfig[this.scenario];
    },

    /** 当前选中接入类型表单提交配置 */
    activeTypeFormConf() {
      return (this.activeTypeConfig && this.activeTypeConfig.form) || null;
    },

    /** 检查当前选中类型是否有配置文件 */
    isActiveTypeSchemaExist() {
      return Object.keys(this.activeTypeSchema).length > 0;
    },

    /** 当前选中接入类型页面组件配置 */
    activeTypeSchema() {
      return (this.activeTypeConfig && this.activeTypeConfig.schema) || {};
    },

    /** 接入方式配置文件 */
    accessMethodSchema() {
      return this.activeTypeSchema.accessMethod || {};
    },

    /** 过滤条件配置文件 */
    filterConditionSchema() {
      return this.activeTypeSchema.filterCondition || {};
    },

    /** 数据定义配置文件 */
    dataDefineSchema() {
      return this.activeTypeSchema.dataDefine || {};
    },

    /** 接入对象配置文件 */
    accessObjectSchema() {
      return this.activeTypeSchema.accessObject || {};
    },

    /** 右侧弹出提示配置文件 */
    rightTips() {
      return this.activeTypeSchema.rightTips || {};
    },

    /** 是否为只读模式，指定的旧的数据不允许编辑 */
    isReadonlyMode() {
      return this.formData.permission === 'read_only';
    },
  },
  watch: {
    /** 监控接入类型改变事件，编辑模式初始化选中的接入类型ID */
    accessTypeList(val) {
      if (val.length) {
        this.setScenarioId(this.formData.data_scenario);
      }
    },

    /** 监控接入类型改变，改变时初始化接入类型ID */
    'formData.data_scenario': {
      immediate: true,
      handler(val) {
        this.setScenarioId(val);
        if (['http', 'db', 'tube'].includes(val)) {
          this.getHostConfig(val);
        }
        this.scenario = val;
      },
    },
  },
  async mounted() {
    this.formConfig = await getFormSchemaConfig();
    if (this.isEditForm) {
      this.getData();
    }
  },
  methods: {
    changeScenario(val) {
      this.scenario = val;
    },
    getHyperLinkHtml(tip, rightTips) {
      const html = `${tip.replace(
        '<hyperLink>',
        `<a target="_blank" href="${rightTips.hyperLink.href}">${rightTips.hyperLink.text}</a>`
      )}`;
      return html;
    },
    handleLoadingStatus(status) {
      this.joinLoading = status;
    },
    setScenarioId(name) {
      let list = null;
      this.accessTypes.forEach(type => {
        const _type = this.accessTypeList[type.id] || [];
        if (!list) {
          list = _type.find(l => l.data_scenario_name === name);
        }
      });
      list && this.handleAccessTypeChange(list);
    },

    /** 接入类型选中事件 */
    handleAccessTypeChange(list) {
      this.formData.data_scenario_id = list.id;
    },

    /**
     * 所有初始化数据源
     */
    handleDataSourceLoaded(data) {
      this.accessType = data;
      this.$set(this.dataSources, data.key, data.data);
    },

    /** 数据标签弹窗打开 */
    handleShowDataTag(show) {
      this.showDataTag = show;
    },
    /**
     * 业务ID改变
     * @param {number} item:选择后的业务ID
     * @return:
     */
    handleBizIdSelected(item) {
      this.formData['bk_biz_id'] = item;
    },

    /** 格式化具体的一个参数
     *  @param root:提交表单一个跟对象
     *  @param key: 需要格式化的对象键
     *  @param componentReturn: 组件返回的对象结果
     */
    formatFormItem(root, key, componentReturn, output) {
      if (typeof root[key] === 'function') {
        const res = root[key](componentReturn);
        output[key] = this.assignItem(output[key], res);
      } else {
        if (root[key] !== undefined && root[key] !== null && typeof root[key] === 'object') {
          output[key] = this.assignItem(output[key], componentReturn);
        }
      }

      return output;
    },

    /** 根据配置文件，映射数据到提交表单数据结构，过滤掉多于的数据 */
    assignItem(origin, source) {
      if (Array.isArray(origin)) {
        const originItem = origin[0];
        origin = [];
        origin.push(
          ...source.map(item => {
            let _origin = typeof originItem === 'object' ? JSON.parse(JSON.stringify(originItem)) : originItem;
            return this.assignItem(_origin, item);
          })
        );
      } else {
        if (typeof origin === 'object') {
          Object.keys(origin).forEach(key => {
            if (typeof source[key] !== 'object') {
              if (source[key] !== undefined) {
                origin[key] = source[key];
              }
            } else {
              origin[key] = this.assignItem(origin[key], source[key]);
            }
          });
        } else {
          origin = source;
        }
      }

      return origin;
    },

    /** 编辑页面回填页面数据
     *  获取到的数据库存储数据
     */
    renderData(data) {
      Object.keys(this.formData).forEach(key => {
        data[key] && this.$set(this.formData, key, data[key]);
      });
    },

    /**
     * @msg: 深度遍历子组件，获取Form数据
     * @param {object} node: 根节点
     * @param {object} config：配置文件
     * @return:
     */
    deepFormatChildParams(node, config, output) {
      let format = typeof node.formatFormData === 'function' ? node.formatFormData() : {};
      /** 处理返回结果是数组，数据属于多个不同组的情况 */
      if (Array.isArray(format)) {
        format.forEach(_format => {
          this.formatChildFormatReponse(config, _format, output);
        });
      } else {
        this.formatChildFormatReponse(config, format, output);
      }
      !node.isDeepTarget
        && Array.prototype.forEach.call(node.$children, child => this.deepFormatChildParams(child, config, output));
    },

    /** 根据配置文件，格式化子组件返回的格式化表单提交数据 */
    formatChildFormatReponse(config, format, output) {
      if (format && format.group) {
        this.formatFormItem(config[format.group], format.identifier, format.data, output[format.group]);
      } else {
        (format.identifier && this.formatFormItem(config, format.identifier, format.data, output))
          || Object.assign(output, format.data);
      }
    },

    /**
     * 编辑页面，初始化已有数据
     * @param node: 根节点
     * @param data: 接口获取数据
     */
    deepRenderEditData(node, data) {
      if (node.renderData) {
        typeof node.renderData === 'function' && node.renderData(data);
      }
      !node.isDeepTarget
        && Array.prototype.forEach.call(node.$children, child => child.$nextTick(() => {
          this.deepRenderEditData(child, data);
        })
        );
    },

    /** 校验所有子组件表单数据*/
    validaAllComponents(node) {
      let isValidate = true;
      if (node.validateForm) {
        if (typeof node.validateForm === 'function') {
          const validateResult = node.validateForm(validateComponentForm);
          if (isValidate) {
            isValidate = validateResult;
          }
        }
      }

      !node.isDeepTarget
        && Array.prototype.forEach.call(node.$children, child => {
          const _validate = this.validaAllComponents(child);
          if (isValidate) {
            isValidate = _validate;
          }
        });

      return isValidate;
    },

    /**
     * @msg: 格式化提交表单数据
     * @return:
     */
    formatSubmitFormData() {
      this.isFormValidate = true;
      if (this.validaAllComponents(this)) {
        this.isLoading = true;
        let config = this.activeTypeFormConf;
        let output = JSON.parse(JSON.stringify(config));
        output
          && Object.assign(output, this.formData, { bk_username: this.$store.getters.getUserName })
          && this.deepFormatChildParams(this, config, output);
        if (this.isEditForm) {
          this.eidtFormData(output);
        } else {
          this.newform(output);
        }
      } else {
        this.isFormValidate = false;
        this.getMethodWarning(this.$t('数据校验失败'), this.$t('页面数据校验'));
      }
    },

    /** 新建表单，提交事件
     *  @param config 提交表单格式化之后的配置
     */
    newform(config) {
      this.bkRequest
        .request('v3/access/deploy_plan/', { method: 'post', params: config, useSchema: false })
        .then(res => {
          this.saveSuccessCallback(false, res, res.data && res.data.raw_data_id);
        })
        ['finally'](res => {
          this.isLoading = false;
        });
    },

    /** 编辑页面保存事件 */
    eidtFormData(config) {
      this.bkRequest
        .request(`v3/access/deploy_plan/${this.rawDataId}/`, {
          useSchema: false,
          params: config,
          method: 'put',
        })
        .then(res => {
          this.saveSuccessCallback(true, res, this.rawDataId);
        })
        ['finally'](res => {
          this.isLoading = false;
        });
    },

    /** 提交成功回调函数 */
    saveSuccessCallback(isEdit, res, rawDataId) {
      let self = this;
      !res.result && this.getMethodWarning(res.message, res.code);
      res.result && this.$router.push({ name: 'success', params: { rawDataId: rawDataId } });
    },

    testGogo() {
      this.$router.push({ name: 'success', params: { rawDataId: 301281 } });
    },

    /**
     * @msg: 保存为模板
     */
    saveAsTemplate() {},
    /**
     * @msg: 提交表单
     */
    submit() {
      this.formatSubmitFormData();
    },

    dataClearing() {
      this.debugShow = true;
    },

    /** 获取已保存的数据
     * 编辑、查看模式下面
     */
    getData() {
      this.isLoading = true;
      this.bkRequest
        .httpRequest('/dataAccess/getDeployPlan', { params: { raw_data_id: this.$route.params.did } })
        .then(res => {
          if (res.result) {
            this.deepRenderEditData(this, res.data);
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.isLoading = false;
        });
    },

    /** 获取场景Host配置 */
    getHostConfig(dataScenario) {
      this.bkRequest.httpRequest('/dataAccess/getHostConfig', { query: { data_scenario: dataScenario } }).then(res => {
        if (res.result) {
          this.ipList = res.data;
        }
      });
    },
  },
};
</script>
<style lang="scss">
.btn-back {
  margin: auto;
  margin-bottom: 15px;
  width: 1180px;
  #return-btn {
    // position: absolute;
    left: -4px;
  }
}
.access-form-container.t5 {
  top: 5px;
}
.bkdata-combobox {
  &.open {
    .bkdata-combobox-icon {
      transform: rotate(180deg) translateY(50%);
    }
  }
}
@media (min-width: 900px) {
  .access-instructions {
    right: 100px;
    z-index: 1;
    background: #fff;
  }

  .bk-access {
    margin: 0;
  }
}

@media (min-width: 1441px) {
  #return-btn {
    left: 70px;
  }
  .access-instructions {
    right: 0;
  }

  .bk-access {
    margin: auto;
  }
}
.bk-access {
  &.row {
    margin-bottom: 15px;
  }
  width: 1180px;
  min-width: 1180px;
  // margin: auto;

  .access-instructions {
    width: 200px;
    position: absolute;
    top: 0;
    border: 1px solid #ddd;
    border-radius: 5px;
    transform: translate(calc(100% + 10px), 0);
    padding: 10px;
    line-height: 20px;
    box-shadow: 0px 0px 10px #ddd;
    &:after {
      content: '';
      border: 10px solid;
      border-color: transparent #fff transparent transparent;
      position: absolute;
      left: 0;
      top: 30px;
      transform: translate(-100%, 0);
    }
    &:before {
      content: '';
      border: 10px solid;
      border-color: transparent #3a84ff transparent transparent;
      position: absolute;
      left: 0;
      top: 30px;
      transform: translate(-100%, 0);
    }
    .instructions {
      margin: 20px 0;
    }
    .info {
      font-size: 12px;
      min-height: 50px;
    }
    .instructions-image {
      width: 100%;
      height: 100px;
      background: #ccc;
      img {
        width: 100%;
        height: 100%;
      }
    }
  }
  .access-types-container {
    min-height: 32px;
    .bk-access-types {
      align-items: center;
      min-height: 32px;
      &.common {
        font-size: 15px;
      }

      &.diaplay-mode {
        align-items: flex-start;
      }

      &.other {
        margin-bottom: 0;
      }
      .bk-form-items {
        display: flex;
        flex-wrap: wrap;
        min-height: 32px;

        &.common {
          .bk-form-radio {
            min-width: 70px;
            .bk-radio-text {
              font-size: 14px;
              padding: 7px;
            }
          }
        }

        &.display {
          span {
            background: #3a84ff;
            color: #fff;
            padding: 5px 8px;
            border-radius: 2px;
            font-size: 16px;
          }
        }
        .bk-form-radio {
          border-radius: 2px;
          background: #eee;
          padding: 0;
          display: flex;
          flex-direction: column;
          align-items: center;
          justify-content: center;
          min-width: 60px;
          cursor: pointer;
          .bk-radio-text {
            cursor: pointer;
          }
          &.active {
            background: #3a84ff;
            color: #fff;
          }

          i {
            padding: 5px;
          }
          &[disabled='disabled'] {
            &:hover {
              cursor: not-allowed;
            }

            i {
              &:hover {
                cursor: not-allowed;
              }
            }
          }
          .hide-input {
            width: 1px;
            height: 0;
            border: none;
          }
        }
      }
    }
  }

  .bk-access-filter {
    .bk-form-items {
      display: flex;
      flex-direction: column;
    }
  }

  .bk-item-des {
    color: #a5a5a5;
    padding: 1px;
    display: inline-block;
  }
  .data-auth {
    font-weight: bold;
    color: #3a84ff;
    padding-left: 5px;
    cursor: pointer;
  }
  .bk-access-define {
    align-items: flex-start;
  }

  .bk-access-method {
    .bk-form-items {
      display: flex;
      flex-direction: column;
    }
  }
}

.submit {
  text-align: center;

  button {
    padding: 10px 50px;
    font-size: 18px;
    height: auto;
    line-height: 1;
  }
}
</style>
