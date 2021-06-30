

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
    <div class="flex mb20">
      <div class="bk-form-item">
        <label class="bk-label">{{ $t('待迁数据') }}</label>
        <div class="bk-form-content">
          <bkdata-input :placeholder="$t('数据源')"
            :value="storageConfigModuleData.rawData.id"
            :disabled="true" />
        </div>
      </div>
      <div class="bk-form-item width-auto">
        <label class="bk-label">{{ $t('时间范围') }}</label>
        <div v-if="showInput"
          class="bk-form-content">
          <bkdata-input :value="displayItem.storage_type_alias"
            :disabled="disabled" />
        </div>
        <div v-else
          class="bk-form-content">
          <bkdata-date-picker
            v-model="storageConfigModuleValue.initDateTime"
            class="mr15"
            :disabled="disabled"
            :timePickerOptions="{
              steps: [1, 60, 60],
            }"
            :placeholder="'选择日期范围'"
            :type="'datetimerange'" />
        </div>
      </div>
    </div>
    <div class="flex space-btween">
      <div class="bk-form-item">
        <label class="bk-label">{{ $t('源存储') }}</label>
        <div class="bk-form-content input-with-bizid">
          <bkdata-selector
            v-bk-tooltips.top="sourceTips"
            v-tooltip.notrigger="validate['sourceStorage']"
            :isLoading="isRequestClusterListLoading"
            :disabled="disabled || !storageInfo.source.length"
            :placeholder="$t('请选择')"
            :list="storageInfo.source"
            :settingKey="'name'"
            :displayKey="'name'"
            :selected.sync="storageConfigModuleData.sourceStorage" />
        </div>
      </div>
      <div class="bk-form-item">
        <label class="bk-label">{{ $t('目标存储') }}</label>
        <div class="bk-form-content">
          <bkdata-selector
            v-bk-tooltips.top="targetTips"
            v-tooltip.notrigger="validate['targetStorage']"
            :isLoading="isRequestClusterListLoading"
            :disabled="disabled || !storageInfo.dest.length"
            :placeholder="$t('请选择')"
            :settingKey="'name'"
            :displayKey="'name'"
            :list="storageInfo.dest"
            :selected.sync="storageConfigModuleData.targetStorage" />
        </div>
      </div>
      <div class="bk-form-item">
        <div class="bk-form-content ml20">
          <bkdata-checkbox v-model="storageConfigModuleValue.coverData"
            v-tooltip.notrigger="validate['coverData']"
            :trueValue="true"
            :disabled="disabled"
            :falseValue="false">
            {{ $t('覆盖目标数据') }}
          </bkdata-checkbox>
        </div>
      </div>
    </div>
    <div class="data-removall-explain">
      <h6>{{ $t('说明') }}</h6>
      <div class="explain-content">
        <div class="explain-content-row">
          1、{{ $t('迁移数据时间范围') }}
        </div>
        <div class="explain-content-row">
          2、{{ $t('来源存储和目标存储需要在数据入库中已配置') }}
        </div>
        <template v-if="Object.keys(storageTypeList).length">
          <div class="explain-content-row">
            3、{{ $t('当前支持迁移类型') }}
          </div>
          <p>{{ $t('源存储') }}：{{ supoortSource }}</p>
          <p>{{ $t('目标存储') }}：{{ supoortDest }}</p>
        </template>
        <template v-if="!storageInfo.dest.length || !storageInfo.source.length">
          <div class="explain-content-row warning-color">
            <i class="icon-info-circle" />{{ $t('当前结果表没有用于数据迁移的源存储或目标存储') }}
          </div>
        </template>
      </div>
    </div>
  </div>
</template>

<script>
export default {
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
  },
  data() {
    return {
      showInput: false,
      cluster: '',
      inputStoragerawData: '',
      isRequestClusterListLoading: false,
      storageInfo: {
        dest: [{ id: 0, name: 'hdfs' }],
        source: [{ id: 0, name: 'hdfs' }],
      },
      isCreatRemovalLoading: false,
      storageTypeList: {},
    };
  },
  computed: {
    supoortSource() {
      return this.storageTypeList.source ? this.storageTypeList.source.join('、') : '';
    },
    supoortDest() {
      return this.storageTypeList.dest ? this.storageTypeList.dest.join('、') : '';
    },
    sourceTips() {
      return this.storageInfo.source.length === 0 ? this.$t('当前结果表没有用于数据迁移的源存储') : '';
    },
    targetTips() {
      return this.storageInfo.dest.length === 0 ? this.$t('当前结果表没有用于数据迁移的目标存储') : '';
    },
    storageConfigModuleValue: {
      get() {
        return this.storageConfigModuleData;
      },
      set(val) {
        // eslint-disable-next-line vue/no-mutating-props
        this.storageConfigModuleData = val;
      },
    },
  },
  created() {
    this.getStorageTypeList();
    // eslint-disable-next-line vue/no-mutating-props
    this.storageConfigModuleData.rawData.id = this.$route.query.result_table_id || this.$route.query.data_id;
    this.getClusterList();
  },
  methods: {
    getStorageTypeList() {
      this.bkRequest.httpRequest('dataRemoval/getStorageTypeList').then(res => {
        if (res.result) {
          this.storageTypeList = res.data || {};
        } else {
          this.getMethodWarning(res.message, res.code);
        }
      });
    },
    createDataRemovalTask() {
      this.isCreatRemovalLoading = true;
      const options = {
        params: {
          result_table_id: this.storageConfigModuleData.rawData.id,
          // result_table_id: '591_test_kv_src1',
          source: this.storageConfigModuleData.sourceStorage,
          dest: this.storageConfigModuleData.targetStorage,
          start: this.storageConfigModuleData.initDateTime[0],
          end: this.storageConfigModuleData.initDateTime[1],
          overwrite: this.storageConfigModuleData.coverData,
        },
      };
      this.bkRequest.httpRequest('dataRemoval/createDataRemovalTask', options).then(res => {
        if (res.result) {
          // this.storageInfo = res.data
        } else {
          this.getMethodWarning(res.message, res.code);
        }
        this.isCreatRemovalLoading = false;
      });
    },
    getClusterList() {
      this.isRequestClusterListLoading = true;
      const options = {
        query: {
          result_table_id: this.storageConfigModuleData.rawData.id,
          // result_table_id: '591_test_kv_src1'
        },
      };
      this.bkRequest
        .httpRequest('dataRemoval/getClusterList', options)
        .then(res => {
          if (res.result) {
            this.storageInfo.source = [];
            this.storageInfo.dest = [];
            res.data.dest.forEach((item, index) => {
              this.storageInfo.dest.push({
                id: index,
                name: item,
              });
            });
            res.data.source.forEach((item, index) => {
              this.storageInfo.source.push({
                id: index,
                name: item,
              });
            });
            if (!this.storageInfo.dest.length || !this.storageInfo.source.length) {
              this.$emit('changeSubmitDisabled', true);
            }
            // eslint-disable-next-line vue/no-mutating-props
            this.storageConfigModuleData.sourceStorage = this.storageInfo.source[0]
              ? this.storageInfo.source[0].name
              : '';
            // eslint-disable-next-line vue/no-mutating-props
            this.storageConfigModuleData.targetStorage = this.storageInfo.dest[0] ? this.storageInfo.dest[0].name : '';
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.isRequestClusterListLoading = false;
        });
    },
    getOptionTpl(option) {
      return `<span style="white-space: pre-line;">${option.description || option.name}</span>`;
    },
    getColorLevelTpl(option) {
      return `<span style="white-space: pre-line;">${option.colorTip || ''}</span>`;
    },
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
  .flex {
    display: flex;
  }
  .space-btween {
    justify-content: space-between;
    .bk-form-item:nth-child(2) {
      margin-right: 12px;
    }
  }
  .bk-form-item {
    display: inline-block;
    width: 33%;
    margin-top: 0;
    .mr-2 {
      margin-right: -2px;
    }
    .ml20 {
      margin-left: 20px;
    }
  }
  .width-auto {
    width: auto;
  }
  .data-removall-explain {
    padding: 10px;
    h6 {
      font-size: 14px;
      color: #666;
      padding: 5px 28px 5px 5px;
      width: 150px;
      text-align: right;
      margin-left: -10px;
    }
    .explain-content {
      margin-left: 140px;
      font-size: 13px;
      .explain-content-row {
        padding: 5px 0;
        .icon-info-circle {
          font-size: 16px;
          margin-right: 10px;
        }
      }
      .warning-color {
        color: #ea3636;
        margin-left: -7px;
      }
      p {
        padding-left: 22px;
      }
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
}
</style>
