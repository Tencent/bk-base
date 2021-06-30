

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
  <!--
        存储配置信息展示模块
     -->
  <div class="bk-form storage-config">
    <div style="display: flex">
      <div class="bk-form-item">
        <label class="bk-label">{{ $t('数据源') }}</label>
        <div v-if="showInput"
          class="bk-form-content">
          <bkdata-input :placeholder="$t('数据源')"
            :value="inputStoragerawData"
            :disabled="disabled" />
        </div>
        <div v-else
          class="bk-form-content clearfix">
          <bkdata-selector
            v-tooltip.notrigger="validate['rawData']"
            :isLoading="storageConfigModuleData.dataSourceLoading"
            :placeholder="$t('请选择')"
            :hasChildren="true"
            :disabled="disabled || storageConfigModuleData.dataSourceDisable"
            :list="storageConfigModuleData.rawDataList"
            :selected.sync="storageConfigModuleData.rawData.id"
            @item-selected="dataSourceSelect" />
        </div>
      </div>
      <div class="bk-form-item">
        <label class="bk-label">{{ $t('存储表英文名') }}</label>
        <div class="bk-form-content input-with-bizid">
          <span v-if="!isRawData"
            class="biz-id-bar">
            {{ bizId }}_
          </span>
          <bkdata-input
            v-model="StorageConfigModuleValue.dataName"
            v-tooltip.notrigger="validate['dataName']"
            :maxlength="50"
            :disabled="disabled || storageConfigModuleData.dataNameDisable || isRawData" />
        </div>
      </div>
      <div class="bk-form-item">
        <label class="bk-label">{{ $t('存储表中文名') }}</label>
        <div class="bk-form-content">
          <bkdata-input
            v-if="mode === 'edit'"
            v-model="StorageConfigModuleValue.dataAlias"
            v-tooltip.notrigger="validate['dataAlias']"
            :maxlength="50"
            :disabled="isRawData" />
          <bkdata-input
            v-else
            v-model="StorageConfigModuleValue.dataAlias"
            v-tooltip.notrigger="validate['dataAlias']"
            :disabled="disabled || storageConfigModuleData.dataNameAliasDisable || isRawData" />
        </div>
      </div>
    </div>
    <div style="display: flex">
      <div class="bk-form-item mt20">
        <label class="bk-label">{{ $t('存储类型') }}</label>
        <div v-if="showInput"
          class="bk-form-content">
          <bkdata-input :value="displayItem.storage_type_alias"
            :disabled="disabled" />
        </div>
        <div v-else
          class="bk-form-content">
          <bkdata-selector
            v-tooltip.notrigger="validate['storageType']"
            :hasChildren="true"
            :placeholder="$t('请选择')"
            :disabled="disabled || storageConfigModuleData.dataStorageTypeDisable"
            :list="storageConfigModuleData.storageTypeList"
            :selected.sync="storageConfigModuleData.storageType.id"
            :optionTip="true"
            :toolTipTpl="getOptionTpl"
            @item-selected="storageTypeSelect" />
        </div>
      </div>
      <div class="bk-form-item mt20">
        <label class="bk-label">{{ $t('存储集群') }}</label>
        <div v-if="mode === 'detail'"
          class="bk-form-content">
          <bkdata-input :value="inputStorageCluster"
            :disabled="disabled" />
        </div>
        <div v-else
          class="bk-form-content">
          <bkdata-selector
            v-tooltip.notrigger="validate['storageCluster']"
            class="storage-module-cluster"
            :isLoading="loading"
            hasChildren="true"
            :placeholder="$t('请选择')"
            :disabled="isNotEditable"
            :customIcon="'circle-shape'"
            :optionTip="true"
            :toolTipTpl="getColorLevelTpl"
            :list="storageConfigModuleData.storageClusterList"
            :selected.sync="storageConfigModuleData.storageCluster.id"
            @item-selected="storageClusterSelect" />
        </div>
      </div>
      <div v-if="!disableDeadLine"
        class="bk-form-item mt20">
        <label class="bk-label">{{ $t('过期时间') }}</label>
        <div v-if="mode === 'detail' || isRawData"
          class="bk-form-content">
          <bkdata-input :value="StorageDeadLineDisplay"
            :disabled="disabled || disableDeadLine || isRawData" />
        </div>
        <div v-else
          class="bk-form-content">
          <bkdata-selector
            v-tooltip.notrigger="validate['deadline']"
            :hasChildren="false"
            :placeholder="$t('请选择')"
            :list="activeExpirTimeList"
            :selected.sync="storageConfigModuleData.deadline"
            :settingKey="'value'"
            :displayKey="'name'"
            :disabled="isRawData" />
          <!-- // "storageConfigModuleData.deadlineList" -->
        </div>
      </div>
      <div v-else-if="storageType === 'tcaplus'"
        class="bk-form-item mt20">
        <label class="bk-label">{{ $t('游戏区') }}</label>
        <div class="bk-form-content">
          <GameZoneSelect
            :clusterName="inputStorageCluster"
            :zoneID.sync="storageConfigModuleData.zoneID"
            @zoneselect="setZone" />
        </div>
      </div>
    </div>
    <div v-if="storageConfigModuleData.storageType.id === 'ignite'
      || storageConfigModuleData.storageType === 'ignite'">
      <div class="bk-form-item mt20">
        <label class="bk-label">{{ $t('最大数据量') }}</label>
        <div class="bk-form-content">
          <bkdata-input
            v-model="StorageConfigModuleValue.maxRecords"
            type="number"
            :max="maxRecordLimitation"
            :disabled="mode === 'detail'"
            extCls="max-records-input" />
        </div>
      </div>
      <div class="tips">
        <TipsInfo :text="$t('单表不能超过xx条_超出数据无法导入', { count: calculateCount })"
          size="small" />
      </div>
    </div>
    <!-- 海外版隐藏副本存储功能，恢复时注意在add.vue与edit.vue恢复校验 -->
    <div v-if="isShowCopyStorage || duplicationConfig"
      style="display: flex">
      <div v-show="duplicationConfig"
        class="bk-form-item mt20 bk-form-flex">
        <label class="bk-label">{{ $t('开启去重') }}</label>
        <div class="bk-form-content">
          <bkdata-radio-group v-model="StorageConfigModuleValue.allowDuplication"
            @change="changeDuplication">
            <bkdata-radio :value="true"
              :disabled="mode === 'detail'">
              {{ $t('是') }}
            </bkdata-radio>
            <bkdata-radio :value="false"
              :disabled="mode === 'detail'">
              {{ $t('否') }}
            </bkdata-radio>
          </bkdata-radio-group>
        </div>
        <i v-bk-tooltips="$t('开启去重时')"
          class="bk-icon icon-question-circle icon-no-cluster-apply" />
      </div>

      <div v-show="isShowCopyStorage"
        class="bk-form-item mt20">
        <label class="bk-label">{{ $t('副本存储') }}</label>
        <div v-tooltip.notrigger.top="validate['copyStorage']"
          class="bk-form-content">
          <bkdata-radio-group v-model="StorageConfigModuleValue.copyStorage">
            <bkdata-radio
              v-bk-tooltips="$t('数据存一个副本')"
              :disabled="isCopyStorageDisabled || mode === 'detail'"
              :value="true">
              {{ $t('是') }}
            </bkdata-radio>
            <bkdata-radio v-bk-tooltips="$t('数据不存副本')"
              :disabled="mode === 'detail'"
              :value="false">
              {{ $t('否') }}
            </bkdata-radio>
          </bkdata-radio-group>
        </div>
      </div>
    </div>

    <div v-if="storageType === 'tcaplus' || storageType === 'clickhouse'"
      class="mt20"
      style="margin: 0 12px 0 68px">
      <TipsInfo :text="$t('主键配置_字段名称提示')"
        size="small" />
    </div>

    <div
      v-show="
        displayItem.data_type === 'raw_data' &&
          displayItem.storage_type === 'queue' &&
          topicName !== (undefined && null && '')
      "
      class="mt20 display-item">
      <div>{{ $t('转发到消息队列的 Topic 为：') }}</div>
      <div>{{ topicName }}</div>
    </div>
  </div>
</template>

<script>
import TipsInfo from '@/components/TipsInfo/TipsInfo.vue';
import Cookies from 'js-cookie';
import GameZoneSelect from './GameZoneSelect';
import { numberFormat } from '@/common/js/util.js';
export default {
  components: {
    TipsInfo,
    GameZoneSelect,
  },
  props: {
    validate: {
      type: Object,
      default: () => ({}),
    },
    mode: {
      type: String,
    },
    disabled: {
      type: Boolean,
    },
    bizId: {
      type: Number,
    },
    storageConfigModuleData: {
      type: Object,
      default: () => ({}),
    },
    displayItem: {
      type: Object,
      default: () => ({}),
    },
    loading: {
      type: Boolean,
      default: false,
    },
    hasReplica: {
      type: Boolean,
      default: false,
    },
    topicName: {
      type: Array,
    },
  },
  data() {
    return {
      clusterLoading: true,
      cluster: '',
      expirTimeInfo: {},
      storageScenario: {},
      isCopyStorage: '',
      isShowCopyStorage: false,
      allowDuplication: false,
      isCopyStorageDisabled: false,
      maxRecordLimitation: 2000000,
      zoneList: [],
    };
  },
  computed: {
    StorageConfigModuleValue: {
      get() {
        return this.storageConfigModuleData;
      },
      set(val) {
        // eslint-disable-next-line vue/no-mutating-props
        this.storageConfigModuleData = val;
      },
    },
    zoneID() {
      return this.storageConfigModuleData.zoneID;
    },
    disableDeadLine() {
      return ['tcaplus'].includes(this.storageType) || false;
    },
    duplicationConfig() {
      return false; // 暂时关闭去重功能
      // return ['es', 'tspider', 'mysql'].includes(this.storageType) || false
    },
    calculateCount() {
      const curLang = Cookies.get('blueking_language') || 'zh-cn';
      const formatter = curLang === 'zh-cn' ? numberFormat.cnFormatter : numberFormat.kFormatter;
      const num = this.maxRecordLimitation;
      return formatter(num);
    },
    rawDataType() {
      return this.displayItem.data_type !== undefined
        ? this.displayItem.data_type
        : this.storageConfigModuleData.rawData.data_type;
    },

    /** 是否为原始数据 */
    isRawData() {
      return this.rawDataType === 'raw_data';
    },

    inputStoragerawData() {
      return typeof this.storageConfigModuleData.rawData === 'object'
        ? this.storageConfigModuleData.rawData.id
        : this.storageConfigModuleData.rawData;
    },
    inputStorageCluster() {
      return typeof this.storageConfigModuleData.storageCluster === 'object'
        ? this.storageConfigModuleData.storageCluster.id
        : this.storageConfigModuleData.storageCluster;
    },
    storageType() {
      return this.storageConfigModuleData.storageType.id || this.storageConfigModuleData.storageType;
    },
    storageCluster() {
      return this.storageConfigModuleData.storageCluster.id || this.storageConfigModuleData.storageCluster;
    },
    showInput() {
      return this.mode === 'detail' || this.mode === 'edit';
    },
    /**
     * 新增入库时，若未选择存储类型情况下，存储集群和过期时间不能编辑
     * id 有为 0 的情况
     */
    isNotEditable() {
      return this.mode === 'add' && [undefined, null, ''].includes(this.storageType);
    },

    activeExpirTimeList() {
      return (this.expirTimeInfo.expire && this.expirTimeInfo.expire.list_expire) || [];
    },
    StorageDeadLineDisplay() {
      if (this.isRawData) {
        return this.storageConfigModuleData.deadline;
      }
      const storageCluster = this.storageConfigModuleData.storageCluster;
      const exactTimeItem = this.activeExpirTimeList
        .find(item => item.value === this.storageConfigModuleData.deadline);

      return (exactTimeItem && exactTimeItem.name) || this.storageConfigModuleData.deadline;
    },
  },
  watch: {
    expirTimeInfo(val) {
      this.maxRecordLimitation = val.connection.cache_records_limit
        ? val.connection.cache_records_limit
        : 2000000;
    },
    'storageConfigModuleData.storageCluster': {
      immediate: true,
      handler(val) {
        if (
          typeof val === 'string'
          && val.includes('es')
          && this.mode === 'detail'
          && this.storageConfigModuleData.storageType === 'es'
        ) {
          this.isShowCopyStorage = true;
          this.isCopyStorage = this.hasReplica;
        }
      },
    },
    'displayItem.storage_type': {
      immediate: true,
      handler(val) {
        if (val === 'es' && this.mode === 'edit') {
          this.isShowCopyStorage = true;
          this.isCopyStorage = this.hasReplica;
        }
      },
    },
    hasReplica: {
      immediate: true,
      handler(val) {
        this.isCopyStorage = val;
        if (this.mode === 'edit' && this.displayItem.storage_type === 'es') {
          this.isShowCopyStorage = true;
        } else if (
          this.mode === 'detail'
          && typeof this.storageConfigModuleData.storageCluster === 'string'
          && this.storageConfigModuleData.storageCluster.includes('es')
        ) {
          this.isShowCopyStorage = true;
        }
      },
    },
  },
  mounted() {
    if (this.mode === 'edit' && !this.isRawData) {
      this.$nextTick(() => {
        this.handleExpirTime();
      });
    }

    if (this.mode !== 'display') {
      this.getStorageCommon();
    }
  },
  methods: {
    setZone(zone) {
      this.StorageConfigModuleValue.setID = zone.set_id;
      this.StorageConfigModuleValue.zoneID = zone.zone_id;
    },
    zoneChangeHandle(val, option) {
      console.log(val, option);
    },
    changeDuplication(val) {
      this.$emit('changeDuplication', val);
    },
    getOptionTpl(option) {
      return `<span style="white-space: pre-line;">${option.description || option.name}</span>`;
    },
    getColorLevelTpl(option) {
      return `<span style="white-space: pre-line;">${option.colorTip || ''}</span>`;
    },
    dataSourceSelect(id, item, option) {
      this.StorageConfigModuleValue.rawData = {
        id: item.id,
        name: item.name,
        data_type: item.data_type,
      };
      if (item.data_type === 'raw_data') {
        this.StorageConfigModuleValue.dataName = '--';
        this.StorageConfigModuleValue.dataAlias = '--';
        this.StorageConfigModuleValue.deadline = '--';
        this.StorageConfigModuleValue.dataNameDisable = false;
        this.StorageConfigModuleValue.dataNameAliasDisable = false;
      } else {
        this.StorageConfigModuleValue.dataName = item.name;
        this.StorageConfigModuleValue.dataAlias = item.name_alias;
        this.StorageConfigModuleValue.dataNameDisable = true;
        this.StorageConfigModuleValue.dataNameAliasDisable = true;
      }

      this.$emit('dataSourceSelect', item);
    },
    storageTypeSelect(id, item) {
      this.StorageConfigModuleValue.storageClusterList = [];
      if (this.StorageConfigModuleValue.storageType.id !== 'es') {
        this.isShowCopyStorage = false;
      }
      this.StorageConfigModuleValue.storageType = {
        id: item.id,
        name: item.name,
      };
      this.StorageConfigModuleValue.storageCluster = {};
      !this.isRawData && this.handleExpirTime();
      this.$emit('changeType', item);
    },
    storageClusterSelect(id, item) {
      if (this.StorageConfigModuleValue.storageType.id === 'es') {
        this.isShowCopyStorage = true;
      }
      if (item.isEnableReplica) {
        this.StorageConfigModuleValue.copyStorage = '';
        this.isCopyStorageDisabled = false;
      } else {
        this.isCopyStorageDisabled = true;
        this.StorageConfigModuleValue.copyStorage = false;
      }
      this.StorageConfigModuleValue.storageCluster = {
        id: item.id,
        name: item.name,
      };
      !this.isRawData && this.handleExpirTime();
      this.$emit('changeCluster', item);
    },
    // 获取各个存储类型的过期时间列表信息
    async handleExpirTime() {
      if (typeof this.storageCluster === 'string' && this.storageCluster && this.storageType) {
        const options = {
          params: {
            cluster_name: this.storageCluster.id || this.storageCluster,
            cluster_type: this.storageType,
          },
        };
        const res = await this.bkRequest.httpRequest('meta/getStorageClusterExpiresConfigsByType', options);
        if (res.result) {
          this.expirTimeInfo = res.data;
          this.maxRecordLimitation = this.expirTimeInfo.connection
                        && this.expirTimeInfo.connection.cache_records_limit;
          this.updateSelectedExpires();
        } else {
          this.getMethodWarning(res.message, res.code);
        }
      }
    },

    /** 重选集群后，过期时间选中项要更新，防止出现列表中没有的选项 */
    updateSelectedExpires() {
      if (!this.isRawData) {
        const currentExpire = this.activeExpirTimeList.find(
          item => item.value === this.StorageConfigModuleValue.deadline
        );
        if (currentExpire === undefined) {
          this.StorageConfigModuleValue.deadline = this.activeExpirTimeList[0].value;
        }
      }
    },
    getStorageCommon() {
      this.bkRequest.httpRequest('dataStorage/getStorageCommon').then(res => {
        if (res.result) {
          this.storageScenario = res.data.storage_scenario;
        }
      });
    },

    // getStorageCommon() {
    //     this.bkRequest.httpRequest('dataStorage/getStorageCommon').then(res => {
    //         if(res.result) {
    //             this.storageScenario = res.data.storage_scenario
    //         }
    //     })
    // }
  },
};
</script>

<style lang="scss" scoped>
.storage-config {
  margin: -15px;
  padding: 20px;
  background: #fafafa;
  border-top: 1px solid #ddd;
  width: calc(100% + 30px);
  .tips {
    margin-left: 50px;
    margin-top: 10px;
    width: 334px;
  }
  .bk-form-item {
    display: inline-block;
    width: 33%;
    margin-top: 0;
    ::v-deep .max-records-input {
      .bk-input-number .input-number-option {
        right: 1px;
        top: 2px;
      }
    }
  }
  .bk-form-flex {
    display: flex;
    align-items: center;
    div.bk-form-content {
      margin-left: 0;
    }
    .bk-icon {
      cursor: pointer;
    }
  }
  .display-item {
    display: flex;
    margin-left: 65px;
    line-height: 32px;
    color: #63656e;
    div {
      padding: 0 5px;
    }
  }
}

/* 当前bk样式中disabled颜色太深，设置成和下拉组件统一 */
.bk-form-input[disabled] {
  background: #fafafa !important;
  color: #aaa;
}

.input-with-bizid {
  display: flex;

  .biz-id-bar {
    line-height: 30px;
    border: 1px solid #c3cdd7;
    padding: 0 10px;
    background: #e1ecff;
    border-right: 0;
  }

  ::v-deep .bk-form-control {
    .bk-input-text {
      display: flex;
    }
  }
}
</style>
