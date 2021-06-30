/*
 * Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
 * BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
 *
 * License for BK-BASE 蓝鲸基础平台:
 * --------------------------------------------------------------------
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
 * and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
 * LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 * NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE
 */

/* eslint-disable no-multi-assign */
/* eslint-disable no-param-reassign */
import { postMethodWarning } from '@/common/js/util.js';
import { validateRules } from '../../../NewForm/SubformConfig/validate.js';

// 定义一个混入对象
const optionsMixins = {
  data() {
    return {
      backupField: null,
      FieldConfigModuleData: {},
      changeOptions: {
        type: null,
        cluster: null,
      },
      stopitem: {},
      stopActive: true,
      showStopDialog: false,
      stopTaskDialogSetting: {
        width: '422px',
        title: window.$t('请确认是否停止入库任务'),
        content: window.$t('停止入库描述'),
        theme: 'danger',
      },
      loading: {
        startLoading: false,
        stopLoading: false,
      },
      StorageConfigModuleData: {
        dataSourceLoading: false,
        dataSourceDisable: false,
        dataStorageTypeDisable: false,
        dataNameDisable: false,
        dataNameAliasDisable: false,
        rawDataList: [],
        rawData: {
          id: null,
          name: null,
          data_type: null,
        },
        storageTypeList: [],
        storageType: {
          id: null,
          name: null,
        },
        storageClusterList: [],
        storageCluster: {
          id: null,
          name: null,
        },
        deadlineList: [],
        deadline: null,
        copyStorage: false,
        allowDuplication: false,
        maxRecords: 100000,
        zoneID: '',
        setID: '',
        index_fields: [],
      },
    };
  },
  created() {
    Object.assign(this.StorageConfigModuleData, {
      dataSourceDisable: this.name !== 'add',
      dataStorageTypeDisable: this.name !== 'add',
      dataNameDisable: this.name !== 'add',
    });
  },
  computed: {
    storageType() {
      return this.StorageConfigModuleData.storageType.id || this.StorageConfigModuleData.storageType;
    },

    /** 是否为原始数据 */
    isRawData() {
      return this.StorageConfigModuleData.rawData.data_type === 'raw_data';
    },
  },
  methods: {
    validateForms() {
      const ignoreFields = ['dataName', 'dataAlias', 'deadline'];
      if (!this.isFirstValidate) {
        let isValidate = true;
        Object.keys(this.validate).forEach((key) => {
          this.validate[key].visible = false;
          const field = this.validate[key].objKey
            ? this.StorageConfigModuleData[key][this.validate[key].objKey]
            : this.StorageConfigModuleData[key];

          /** 原始数据忽略中文名、英文名、过期时间 */
          const ignoreValidate = this.isRawData && ignoreFields.includes(key);
          if (!ignoreValidate && !validateRules(this.validate[key].regs, field, this.validate[key])) {
            isValidate = false;
          }
        });

        return isValidate;
      }

      return true;
    },
    generateConfigParams() {
      const baseForm = {
        has_replica: this.StorageConfigModuleData.copyStorage,
        max_records: +this.StorageConfigModuleData.maxRecords || 1000000,
        has_unique_key: this.StorageConfigModuleData.allowDuplication,
      };
      return Object.assign(
        baseForm,
        this.storageType === 'tcaplus'
          ? {
            zone_id: this.StorageConfigModuleData.zoneID,
            set_id: this.StorageConfigModuleData.setID,
          }
          : {},
      );
    },
    processFieldData(allowDuplication) {
      if (['es', 'tspider', 'mysql'].indexOf(this.storageType) === -1) return;
      if (allowDuplication) {
        this.FieldConfigModuleData = JSON.parse(JSON.stringify(this.backupField));
      } else {
        this.FieldConfigModuleData.fields.forEach((item) => {
          item.configs = item.configs.filter((config) => {
            if (config.key === 'storage_keys') {
              config.checked = item[config.field] = false;
            }
            return config.key !== 'storage_keys';
          });
        });
      }
    },
    changeDuplication(value) {
      this.processFieldData(value);
    },
    cancelFn() {
      this.showStopDialog = false;
    },
    tryStop(item, active) {
      if (['stopped', 'failed', 'deleted'].includes(item.status_en)) return;
      // this.stopitem = item
      this.$set(this, 'stopitem', item);
      this.stopActive = active;
      this.showStopDialog = true;
    },
    taskOption(op, item, active = true) {
      console.log(op, item, active);
      if (active) {
        if (op === 'restart') {
          this.editItem(item);
        } else {
          const startOptoin = {
            result_table_id: item.result_table_id,
            storages: [item.storage_type],
            rt_id: item.result_table_id,
          };
          const optionService = {
            restart: 'dataStorage/startTask',
            start: 'dataStorage/startTask',
            stop: 'dataStorage/stopTask',
          };
          if (op === 'start') {
            this.loading.startLoading = true;
          } else if (op === 'stop') {
            this.loading.stopLoading = true;
          }
          this.showStopDialog = false;
          this.$set(item, 'isLoading', true);
          console.log(op, item, active);
          this.bkRequest
            .httpRequest(optionService[op], { params: startOptoin })
            .then((res) => {
              if (!res.result) {
                item.status = this.$t('异常');
                item.status_en = 'failed';
                this.getMethodWarning(res.message, res.code);
              } else {
                if (op === 'stop') {
                  item.status = this.$t('已停止');
                  item.status_en = 'stopped';
                } else {
                  item.status = this.$t('正常');
                  item.status_en = 'started';
                }
                postMethodWarning(res.data, 'success');
              }
            })
            .finally(() => {
              this.$set(item, 'isLoading', false);
              this.loading.startLoading = false;
              this.loading.stopLoading = false;
            });
        }
      }
    },
  },
};

export default optionsMixins;
