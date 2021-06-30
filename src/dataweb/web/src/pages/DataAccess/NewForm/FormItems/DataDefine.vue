

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
  <Container id="accDataDefine">
    <Item>
      <label class="bk-label required">{{ $t('所属业务') }}</label>
      <div :class="['bk-form-content']">
        <bkdata-selector
          v-tooltip.notrigger.right="validate['bk_biz_id']"
          :disabled="$route.params.did && $route.name === 'updateDataid'"
          :displayKey="'bk_biz_name'"
          :isLoading="bkBizLoading"
          :list="calcBizLists"
          :searchable="calcBizLists.length > 0"
          :selected.sync="params.bk_biz_id"
          :settingKey="'bk_biz_id'"
          searchKey="bk_biz_name"
          @item-selected="handleBkBizSelect">
          <template slot="bottom-option">
            <a
              class="bkdata-selector-create-item"
              target="_blank"
              :href="$store.getters['docs/getPaths'].applyBizPermission">
              <i class="text"
                style="font-size: 12px">
                {{ $t('仅对应业务角色可以进行数据接入') }}
              </i>
            </a>
          </template>
        </bkdata-selector>
      </div>
    </Item>
    <Item>
      <label class="bk-label">
        <span v-bk-tooltips="$t('数据来源描述')"
          class="has-desc">
          {{ $t('数据来源') }}
        </span>
      </label>
      <div class="bk-form-content">
        <bkdata-tag-input
          v-model="params.data_source_tags"
          v-tooltip.notrigger="validate['data_source_tags']"
          :placeholder="sourceTagPlaceholder"
          :list="sourceTagList"
          :contentWidth="250"
          :hasDeleteIcon="true"
          :disabled="!sourceTagList.length"
          :useGroup="true"
          :trigger="'focus'"
          extCls="data-define-taginput"
          style="width: 100%" />
      </div>
    </Item>
    <Item>
      <label class="bk-label required">{{ $t('英文名称') }}</label>
      <div class="bk-form-content">
        <bkdata-input
          slot="colleft"
          v-model="params.raw_data_name"
          v-tooltip.notrigger="validate['raw_data_name']"
          :disabled="!!($route.params.did || (rawDataName.disabled && !rowDataEdit))"
          :placeholder="$t('请输入数据源英文名称')"
          :maxlength="50"
          type="text" />
      </div>
    </Item>
    <Item>
      <label class="bk-label required">{{ $t('中文名称') }}</label>
      <div class="bk-form-content">
        <bkdata-input
          slot="colright"
          v-model="params.raw_data_alias"
          v-tooltip.notrigger="validate['raw_data_alias']"
          :disabled="!!rawDataAlias.disabled"
          :placeholder="$t('请输入数据源中文名称')"
          :maxlength="50"
          type="text" />
      </div>
    </Item>
    <Item>
      <label class="bk-label">{{ $t('字符集编码') }}</label>
      <div class="bk-form-content">
        <bkdata-selector
          :displayKey="'encoding_alias'"
          :list="encodeLists"
          :selected.sync="params.data_encoding"
          :settingKey="'encoding_name'" />
      </div>
    </Item>
    <Item>
      <label class="bk-label">{{ $t('区域') }}</label>
      <div class="bk-form-content area-select">
        <bkdata-selector
          v-tooltip.notrigger="validate['data_region']"
          :displayKey="'alias'"
          :disabled="!isNewForm"
          :list="regionInfo"
          :selected.sync="params.data_region"
          :settingKey="'region'" />
        <i
          v-bk-tooltips="$t('请确认数据区域_跨区域传输成本高_需用户承担')"
          class="bk-icon icon-exclamation-circle-shape bk-icon-new" />
      </div>
    </Item>
    <!-- <FieldCol2 :label="$t('数据源名称')">
            <bkdata-input :disabled="!!($route.params.did || (rawDataName.disabled && !rowDataEdit))"
                :placeholder="$t('英文名称')"
                :maxlength="50"
                slot="colleft"
                type="text"
                v-model="params.raw_data_name"
                v-tooltip.notrigger="validate['raw_data_name']" />

            <bkdata-input :disabled="!!rawDataAlias.disabled"
                :placeholder="$t('中文名称')"
                :maxlength="50"
                slot="colright"
                type="text"
                v-model="params.raw_data_alias"
                v-tooltip.notrigger="validate['raw_data_alias']" />
        </FieldCol2> -->
    <FieldX2 :label="$t('数据源描述')">
      <bkdata-input
        v-model="params.description"
        :placeholder="$t('非必填_准确的数据描述能帮助系统提取标签_提升数据分析的准确性')"
        :maxlength="255"
        type="text" />
    </FieldX2>
    <FieldX2 :label="$t('数据标签')"
      :labelTips="$t('数据标签描述')">
      <DataTag
        ref="dataTag"
        v-model="selectedTags"
        :data="getDataTagList"
        :placeholder="$t('非必填_但准确的标签能够提升后续检索的精准性及分析的准确性')"
        :inputFocusHandle="focusHandle"
        :cancelHandle="tagCancelHandle"
        :confirmHandle="tagConfirmHandle"
        :fullWrapperTop="tagTopPosition" />
    </FieldX2>
    <dialogWrapper
      :dialog="dialog"
      :title="$t('数据跨境确认')"
      :bkScroll="false"
      :subtitle="$t('数据跨境确认子标题')"
      :defaultFooter="true"
      :footerConfig="complianceReminder.footerConfig"
      :disabled="!dialogEnabled"
      icon=""
      @confirm="() => handleCloseConfirm(false)"
      @cancle="() => handleCloseConfirm(true)">
      <template #content>
        <div class="bk-data-hub compliance-reminder-dialog">
          <div class="dialog-content-row">
            {{ $t('是否跨境确认', { region: dataRegionDisplayName }) }}
            <span v-bk-tooltips="complianceReminder.tips"
              class="conten-tips">
              {{ $t('数据跨境') }}
            </span>
            ?
          </div>
          <div class="optoin-radio dialog-content-row">
            <bkdata-radio-group v-model="complianceReminder.isCrossBorder">
              <bkdata-radio :value="true">
                {{ $t('是') }}
              </bkdata-radio>
              <bkdata-radio :value="false">
                {{ $t('否') }}
              </bkdata-radio>
            </bkdata-radio-group>
          </div>
          <template v-if="complianceReminder.isCrossBorder">
            <div class="dialog-content-row">
              {{ $t('跨境数据应用场景选择') }}
            </div>
            <div class="dialog-content-row scenarios"
              :class="{ checked: complianceReminder.scenarios === 'option1' }">
              <div>
                <bkdata-radio
                  value="option1"
                  :checked="complianceReminder.scenarios === 'option1'"
                  @change="evt => handleCrossBorderChanged(evt, 'option1')">
                  {{ $t('跨境场景选项1') }}
                </bkdata-radio>
              </div>
              <div class="sub-content">
                {{ $t('跨境场景选项1描述') }}
              </div>
            </div>
            <div class="dialog-content-row scenarios"
              :class="{ checked: complianceReminder.scenarios === 'option2' }">
              <bkdata-radio
                value="option2"
                :checked="complianceReminder.scenarios === 'option2'"
                @change="evt => handleCrossBorderChanged(evt, 'option2')">
                {{ $t('跨境场景选项2') }}
              </bkdata-radio>
            </div>
            <div class="dialog-content-row scenarios"
              :class="{ checked: complianceReminder.scenarios === 'option3' }">
              <bkdata-radio
                value="option3"
                :checked="complianceReminder.scenarios === 'option3'"
                @change="evt => handleCrossBorderChanged(evt, 'option3')">
                {{ $t('跨境场景选项3') }}
              </bkdata-radio>
            </div>
            <div v-if="complianceReminder.scenarios === 'option3'"
              class="dialog-content-row warning">
              {{ $t('不支持场景提示') }}
            </div>
          </template>
          <div class="dialog-content-row promise-section">
            <bkdata-checkbox v-model="complianceReminder.isPromised"
              :checked="complianceReminder.isPromised">
              {{ $t('跨境数据确认CheckBox') }}
            </bkdata-checkbox>
          </div>
        </div>
      </template>
    </dialogWrapper>
  </Container>
</template>
<script>
import mixin from '@/components/dataTag/tagMixin.js';
import { mapGetters } from 'vuex';
import Container from './ItemContainer';
import Item from './Item';
import FieldX2 from './FieldComponents/FieldX2';
import { validateComponentForm, validateRules } from '../SubformConfig/validate.js';
import { getGeogTags } from './Components/fun.js';
import Bus from '@/common/js/bus';
import DataTag from '@/components/dataTag/DataTag.vue';
import dialogWrapper from '@/components/dialogWrapper';
export default {
  components: { Container, Item, FieldX2, DataTag, dialogWrapper },
  mixins: [mixin],
  props: {
    scenarioType: {
      type: String,
      default: '',
    },
    scenarioId: {
      default: 0,
      type: Number,
    },
    encodeLists: {
      type: Array,
      default: () => [],
    },
    schemaConfig: {
      type: Object,
      default: () => {
        return {};
      },
    },
  },
  data() {
    return {
      bkBizLoading: false,
      bizLists: [],
      sourceTagList: [], // 数据来源标签列表,
      sourceTagMap: {
        // 数据来源标签关键词映射
        subTopList: 'sub_top_list',
        subList: 'sub_list',
        tagId: 'code',
        tagName: 'alias',
      },
      backupTagData: [], // 标签列表缓存,
      backupTags: [], // 标签选择缓存
      selectedTags: [], // 已选标签
      treeProps: {
        label: 'category_alias',
        children: 'sub_list',
        value: 'category_name',
      },
      treeData: [],
      isDeepTarget: true,
      isFirstValidate: true,
      datasourceLoading: true,
      eiditDataCategory: '',
      rowDataEdit: false,
      regionInfo: [
        {
          parent_id: 347,
          description:
            '中国内地（或简称内地）是指除香港、澳门、台湾以外的中华人民共和国主张管辖区，多用于与香港、澳门特别行政区同时出现的语境',
          id: 348,
          alias: '中国内地',
          region: 'inland',
        },
      ],
      params: {
        bk_biz_id: '',
        raw_data_name: '',
        raw_data_alias: '',
        tags: [],
        data_source_tags: [],
        data_encoding: 'UTF-8',
        description: '',
        data_region: '',
      },
      validate: {
        data_source_tags: {
          regs: [{ required: true, error: window.$t('不能为空') }],
          content: '',
          visible: false,
          class: 'error-red',
        },
        bk_biz_id: {
          regs: { required: true, type: 'string', error: window.$t('不能为空') },
          content: '',
          visible: false,
          class: 'error-red',
        },
        raw_data_name: {
          regs: [
            { required: true, error: window.$t('不能为空') },
            { regExp: /^[_a-zA-Z0-9]*$/, error: window.$t('由英文_字母_数字下划线组成') },
            { length: 50, error: window.$t('长度不能大于50') },
          ],
          content: '',
          visible: false,
          class: 'error-red',
        },
        raw_data_alias: {
          regs: [
            { required: true, error: window.$t('不能为空') },
            { length: 50, error: window.$t('长度不能大于50') },
          ],
          content: '',
          visible: false,
          class: 'error-red',
        },
        data_region: {
          regs: [{ required: true, error: window.$t('不能为空') }],
          content: '',
          visible: false,
          class: 'error-red',
        },
      },
      tdbank_topic: '',
      tdbank_tid: '',
      dialog: {
        isShow: false,
        width: 800,
        quickClose: false,
        loading: false,
        closeIcon: false,
      },
      complianceReminder: {
        isCrossBorder: undefined,
        isPromised: false,
        scenarios: undefined,
        isMounted: false,
        tips: {
          content: window.$t('跨境说明Tips'),
          width: 250,
        },
        footerConfig: {
          confirmText: window.$t('确定'),
          cancleText: window.$t('取消'),
          loading: false,
        },
      },
    };
  },
  computed: {
    ...mapGetters({
      getUserName: 'getUserName',
      getDataTagList: 'dataTag/getTagList',
      getMultipleTagsGroup: 'dataTag/getMultipleTagsGroup',
      getSingleTagsGroup: 'dataTag/getSingleTagsGroup',
      flatTags: 'dataTag/getAllFlatTags',
    }),
    tagTopPosition() {
      return this.$route.path.includes('data-access') ? '97px' : '0';
    },
    sourceTagPlaceholder() {
      return this.sourceTagList.length ? this.$t('必填_请选择') : this.$t('暂无选项');
    },
    isNewForm() {
      return !this.$route.params.did;
    },
    rawDataAlias() {
      return this.schemaConfig['raw_data_alias'] || {};
    },
    rawDataName() {
      return this.schemaConfig['raw_data_name'] || {};
    },
    tdbankName() {
      return this.tdbank_topic && this.tdbank_tid ? this.tdbank_topic + '_' + this.tdbank_tid : '';
    },
    calcBizLists() {
      return this.bizLists || [];
    },
    dataRegionDisplayName() {
      return (
        ((this.params.data_region && this.regionInfo.find(r => r.region === this.params.data_region)) || {}).alias || ''
      );
    },
    dialogEnabled() {
      return (
        this.complianceReminder.isCrossBorder === false
        || (['option1', 'option2'].includes(this.complianceReminder.scenarios) && this.complianceReminder.isPromised)
      );
    },
  },
  watch: {
    /** 修改接入类型，重新请求接口，返回数据来源标签列表 */
    scenarioId(val, old) {
      if (val !== old) {
        this.params.data_source_tags = []; // 清空选择
        this.getSourceTagData();
      }
    },
    selectedTags(val) {
      this.params.tags = val.map(tag => {
        /** tag标签系统，个别tag会有多个tag_code，以|分隔，取第一个发送至后端 */
        if (tag.tag_code.includes('|')) {
          return tag.tag_code.split('|')[0];
        }
        return tag.tag_code;
      });
    },
    tdbankName(val) {
      let data = {
        raw_data_name: val,
      };
      Object.assign(this.params, data);
    },
    scenarioType: {
      immediate: true,
      handler(val) {
        this.bizLists = [];
        if (this.isNewForm && val === 'beacon') {
          this.params.data_encoding = 'GBK';
        } else {
          this.params.data_encoding = 'UTF-8';
        }

        /** 根据不同的接入类型，请求业务列表接口不同 */
        const jobBiz = ['log', 'tlog', 'script'];
        if (jobBiz.includes(val)) {
          this.getBizList('biz.job_access');
        } else {
          this.getBizList('biz.common_access');
        }
      },
    },
    'params.data_region': {
      immediate: true,
      handler(val) {
        this.$store.dispatch('updateRegion', val);
        val && this.handleRegionChangedReminder();
      },
    },
    'params.raw_data_name'(newVal) {
      this.validate.raw_data_name.visible = false;
      newVal && validateRules(this.validate.raw_data_name.regs, newVal, this.validate.raw_data_name);
    },
    params: {
      handler(newVal) {
        !this.isFirstValidate && validateComponentForm(this.validate, newVal);
      },
      deep: true,
    },
    schemaConfig(val) {
      if (val) {
        this.isFirstValidate = true;
        Object.assign(this.params, {
          raw_data_name: '',
          raw_data_alias: '',
        });

        Object.keys(this.validate).forEach(key => {
          this.validate[key].visible = false;
        });
      }
    },
    'complianceReminder.isCrossBorder': {
      handler(isShow) {
        this.$nextTick(() => {
          this.complianceReminder.isMounted = isShow;
        });
      },
    },
  },
  mounted() {
    this.formatRegion();
  },
  created() {
    this.getSourceTagData();
    Bus.$on('access.raw_data.change', (val, oth = { enabled: false }) => {
      Object.assign(this.params, val);
      this.rowDataEdit = oth.enabled;
    });

    Bus.$on('Access-httpExample-DebugValidate2', validate => {
      this.$set(this.validate.bk_biz_id, 'visible', !!validate.visible);
      this.$set(this.validate.bk_biz_id, 'content', validate.content);
      this.$forceUpdate();
    });
    this.getDataTagListFromServer();

    // 5月25新需求，放开接口，不需要自动拼接，用户自己填写，对应事件在tdbank组件已经关闭
    /* Bus.$on('tdbank_topic.change', val => {
                this.tdbank_topic = val
            })
            Bus.$on('tdbank_tid.change', val => {
                this.tdbank_tid = val
            }) */
  },
  methods: {
    handleCloseConfirm(isCancel = false) {
      if (isCancel) {
        this.params.data_region = '';
      }
      this.dialog.isShow = false;
    },
    handleCrossBorderChanged(isChecked, option) {
      if (this.complianceReminder.isMounted && isChecked) {
        this.complianceReminder.scenarios = option;
      }
    },
    handleRegionChangedReminder() {
      if (this.$modules.isActive('compliance_reminder') && this.isNewForm) {
        this.dialog.isShow = true;
      }
    },
    getBizList(actionID) {
      this.bkBizLoading = true;
      this.bkRequest
        .httpRequest('auth/getAccessBizList', {
          params: {
            bkUser: this.getUserName,
          },
          query: {
            action_id: actionID,
          },
        })
        .then(res => {
          if (res.result) {
            this.bizLists = res.data.map(item => {
              item.bk_biz_name = `[${item.bk_biz_id}]${item.bk_biz_name}`;
              return item;
            });
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.bkBizLoading = false;
        });
    },
    linkToCC() {
      this.$linkToCC();
    },
    handleBkBizSelect(item) {
      this.validate.bk_biz_id.visible = false;
      this.$emit('bizIdSekected', item);
    },
    formatFormData() {
      const postParams = Object.assign({ tags: [this.params.data_region] }, this.params);
      return {
        group: '',
        identifier: 'access_raw_data',
        data: postParams,
      };
    },
    renderData(data) {
      Object.keys(this.params).forEach(key => {
        if (key === 'data_region') {
          try {
            this.$set(this.params, key, data['access_raw_data'].tags.manage.geog_area[0].code);
          } catch (err) {
            console.log(err);
          }
        } else if (key === 'data_source_tags') {
          try {
            this.getSourceTagData().then(() => {
              let dataSource = [];
              data['access_raw_data'][key].forEach(tagObj => {
                if (typeof tagObj === 'object') {
                  dataSource.push(this.getTagCode(tagObj.code));
                } else {
                  dataSource.push(tagObj);
                }
              });
              this.$set(this.params, key, dataSource);
            });
          } catch (err) {
            console.log(err);
          }
        } else if (key === 'tags') {
          /** 取出数据标签中的tagCode */
          const { business = [], desc = [] } = data['access_raw_data'][key];
          const tags = business.concat(desc).map(tag => {
            return tag.code;
          });
          /** 遍历寻找标签状态 */
          const selectedTag = [];
          this.flatTags.forEach(tagGroup => {
            tagGroup.forEach(tag => {
              if (tags.includes(this.getTagCode(tag.tag_code))) {
                selectedTag.push(tag);
                this.$store.commit('dataTag/setTagStatus', {
                  id: tag.tag_code,
                  status: true,
                });
              }
            });
          });
          const deduplicateTags = this.tagsDeduplicate(selectedTag, 'tag_code');
          this.$set(this, 'selectedTags', deduplicateTags);
        } else {
          data['access_raw_data'][key] && this.$set(this.params, key, data['access_raw_data'][key]);
        }
      });
    },
    validateForm(validateFunc) {
      this.isFirstValidate = false;
      const isvalidate = validateFunc(this.validate, this.params);
      !isvalidate && this.$forceUpdate();
      return isvalidate;
    },
    formatRegion() {
      getGeogTags().then(res => {
        this.regionInfo = res.data;
        if (this.regionInfo.length === 1) {
          // 当只有一条数据时，默认选中第一条
          if (!this.$modules.isActive('compliance_reminder')) {
            this.params.data_region = this.regionInfo[0].region;
          }
        }
      });
    },
    /** 根据接入类型，获取数据来源标签 */
    getSourceTagData() {
      return this.axios.get(`v3/datamanage/datamart/source_tag_scenario/${this.scenarioId}/`).then(res => {
        if (res.result) {
          this.sourceTagList = res.data.map((tagGroup, groupIdx) => {
            const tags = {};
            tags.id = this.getTagCode(tagGroup.code);
            tags.name = tagGroup.alias;
            tags.children = tagGroup.sub_list.map((item, itemIdx) => {
              const tag = {};
              tag.id = this.getTagCode(item.code);
              tag.name = item.alias;
              return tag;
            });
            return tags;
          });
          /** 如果只要一个选项，默认选中 */
          this.sourceTagList.length === 1
            && this.sourceTagList[0].children.length === 1
            && this.sourceTagDefaultSelected();
        }
      });
    },
    sourceTagDefaultSelected() {
      this.params.data_source_tags.push(this.sourceTagList[0].children[0].id);
    },
    focusHandle() {
      const list = this.getDataTagList;
      this.$refs.dataTag.switchFullScreen(true);
      this.backupTagData = JSON.parse(JSON.stringify(list));
      this.backupTags = JSON.parse(JSON.stringify(this.selectedTags));
      this.$emit('showDataTag', true);
    },
    tagCancelHandle() {
      this.$refs.dataTag.switchFullScreen(false);
      this.$emit('showDataTag', false);
      this.$refs.dataTag.text = ''; // 清空input输入内容
      this.selectedTags = this.backupTags;
      this.backupTagData[0].data.length && this.$store.commit('dataTag/setDataTagList', this.backupTagData);
    },
    tagConfirmHandle() {
      this.$refs.dataTag.switchFullScreen(false);
      this.$emit('showDataTag', false);
      this.$refs.dataTag.text = ''; // 清空input输入内容
      this.backupTagData = [];
      this.backupTags = [];
    },
    enterTagPage() {
      this.$refs.dataTag.enterFullScreen(true);
    },
  },
};
</script>
<style lang="scss" scoped>
.bk-form-content {
  ::v-deep .data-define-taginput {
    align-self: flex-start;
    .bk-tag-input {
      display: flex;
      justify-content: space-between;
      align-items: center;
    }
    .clear-icon {
      margin-right: 5px;
      position: static;
      transform: translateY(0);
    }
    .tag-list .key-node,
    .tag {
      background: #fdf6ec;
      color: #faad14;
      border-color: #fdf6ec;
    }
    .remove-key {
      transform: translateX(2px);
    }
  }
  .bk-form-icon {
    cursor: pointer;
    position: absolute;
    left: 16px;
  }
  &.en-name {
    width: 387px;
  }
  &.area-select {
    position: relative;
    .bk-icon {
      position: absolute;
      top: 50%;
      right: -25px;
      margin-left: 10px;
      line-height: 30px;
      color: #ff9c01;
      transform: translateY(-50%);
      cursor: pointer;
    }
  }
}

.bkdata-selector-create-item {
  height: 42px;
  line-height: 42px;
  .text {
    font-style: normal;
  }
}
::v-deep .data-tag-collection {
  width: 100%;
  max-height: 65px;
  overflow: auto;
}
</style>

<style lang="scss">
.bk-data-hub {
  &.compliance-reminder-dialog {
    height: 450px;
    margin: 25px 40px 0 40px;
    font-size: 14px;
    position: relative;
    padding-bottom: 40px;

    .dialog-content-row {
      margin-bottom: 20px;

      &.promise-section {
        position: absolute;
        bottom: 0;
      }

      &.scenarios {
        // width: 700px;
        height: 60px;
        background: #f0f1f5;
        border: 1px solid #f0f1f5;
        border-radius: 2px;
        padding: 0 10px;
        display: flex;
        flex-direction: column;
        justify-content: center;
        color: #63656e;

        &.checked {
          background: #ebf2ff;
          border: 1px solid #3a84ff;
        }

        ::v-deep .bk-form-radio {
          margin-right: 0px;
        }

        .sub-content {
          height: 24px;
          font-size: 12px;
          text-align: left;
          color: #979ba5;
          line-height: 24px;
          padding-left: 20px;
        }
      }

      &.warning {
        height: 16px;
        font-size: 12px;
        text-align: left;
        color: #ea3636;
        line-height: 16px;
        margin-top: -15px;
      }

      .conten-tips {
        border-bottom: dashed 1px;
      }
    }
  }
}
</style>
