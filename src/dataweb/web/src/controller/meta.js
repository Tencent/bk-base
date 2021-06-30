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

/* eslint-disable no-param-reassign */
const timeReg = /^(dtEventTime|dtEventTimeStamp|thedate|localTime)$/;
const indexReg = /^(dtEventTime|dtEventTimeStamp|ip|path|gseindex)$/;
const ipReg = /^(ip|path|gseindex|dtEventTimeStamp)$/;
class Meta {
  constructor() {
    /** 字段映射，用于格式化获取到的存储类型自字段配置表 */
    this.fieldConfigMap = this.getMetaConfig;

    /** 不同节点可配置的字段类型映射，用于设置默认字段配置 */
    this.nodeFieldsConfig = {
      clickhouse: {
        settings: ['primary_keys'],
        optionType: 'checkbox',
        isReadonly: field => timeReg.test(field.physical_field),
      },
      tcaplus: {
        settings: ['primary_keys'],
        optionType: 'checkbox',
        isReadonly: field => timeReg.test(field.physical_field),
      },
      tsdb: {
        settings: ['dim_fields'],
        optionType: 'checkbox',
        isReadonly: field => !field.is_dimension,
        validate() {
          return {
            isValidate:
                            !this.fields.some(field => field.is_dimension)
                            || (this.config.dim_fields && this.config.dim_fields.length),
            msg: 'tsdb存储需配置维度字段，请检查配置的sql语句中是否有维度字段',
          };
        },
      },
      mysql: {
        settings: ['indexed_fields', 'storage_keys'],
        optionType: 'checkbox',
        isReadonly: (field, key) => timeReg.test(field.physical_field) && key !== 'storage_keys',
      },
      postgresql: {
        settings: ['indexed_fields'],
        optionType: 'checkbox',
        isReadonly: field => timeReg.test(field.physical_field),
      },
      tpg: {
        settings: ['indexed_fields'],
        optionType: 'checkbox',
        isReadonly: field => timeReg.test(field.physical_field),
      },
      tspider: {
        settings: ['indexed_fields', 'storage_keys'],
        optionType: 'checkbox',
        isReadonly: (field, key) => timeReg.test(field.physical_field) && key !== 'storage_keys',
      },
      es: {
        settings: ['analyzed_fields', 'doc_values_fields', 'json_fields', 'storage_keys'],
        optionType: 'radio',
        isReadonly: (field, key) => indexReg.test(field.physical_field) && key !== 'storage_keys',
        isChecked: (field, type) => ipReg.test(field.physical_field) && type === 'doc_values_fields',
      },
      tredis: {
        settings: ['storage_keys'],
        optionType: 'checkbox',
        isReadonly: field => /^(dtEventTime|dtEventTimeStamp)$/.test(field.physical_field),
      },
      ignite: {
        settings: ['indexed_fields', 'storage_keys'],
        optionType: 'checkbox',
        isReadonly: field => timeReg.test(field.physical_field),
      },
    };
  }

  /** 格式化返回字段，用于数据入库模块  */
  formatSqlSchemaDataByClusterType(data, option) {
    // return this.formatFlowFields(data, option)
    if (!data) {
      return {
        fields: [],
      };
    }

    const { clusterType } = option.params;
    const clusterData = data.storage[clusterType] || {};
    const config = (clusterData && clusterData.config) || {};
    const currentConfig = this.nodeFieldsConfig[clusterType];
    if (!Object.keys(config).length) {
      const defaultConf = (currentConfig && currentConfig.settings) || null;
      defaultConf
                && defaultConf.forEach((conf) => {
                  config[conf] = [];
                });
    }
    if (clusterData.fields) {
      clusterData.fields = clusterData.fields.map((field) => {
        field.configs = [];
        this.fieldConfigMap().forEach((item) => {
          if (config[item.key] !== undefined) {
            let checked = config[item.key].includes(field.physical_field);
            /** 处理需要默认选中的字段 */
            if (!checked) {
              const defaultChecked = currentConfig.isChecked && currentConfig.isChecked(field, item.key);
              if (defaultChecked) {
                checked = defaultChecked;
                config[item.key].push(field.physical_field);
                field[item.field] = checked;
              }
            }
            const isReadonly = currentConfig && currentConfig.isReadonly(field, item.key);

            field.configs.push(Object.assign({}, item, { checked, isReadonly }));
            field[item.field] = field[item.field] || config[item.key].includes(field.physical_field);
          } else {
            field[item.field] = field[item.field] || false;
          }
        });

        return field;
      });
    }

    if (this.nodeFieldsConfig[clusterType] && this.nodeFieldsConfig[clusterType].optionType) {
      Object.assign(clusterData, { opt: this.nodeFieldsConfig[clusterType].optionType });
    }

    return clusterData;
  }

  /** 扩展字段类型，用于格式化数据入库原始数据类型下面请求的字段列表格式化 */
  extendSqlSchmaFields(data, clusterType) {
    const cloneMap = JSON.parse(JSON.stringify(this.fieldConfigMap()));
    const config = {};
    if (!Object.keys(config).length) {
      const defaultConf = this.nodeFieldsConfig[clusterType];
      defaultConf
                && defaultConf.forEach((conf) => {
                  config[conf] = [];
                });
    }
    (data.fields || []).forEach((field) => {
      field.configs = [];
      cloneMap.forEach((item) => {
        if (config[item.key] !== undefined) {
          field.configs
            .push(Object.assign(item, { checked: config[item.key].includes(field.physical_field) }));
          field[item.field] = config[item.key].includes(field.physical_field);
        } else {
          field[item.field] = false;
        }
      });
    });

    return data;
  }

  /** 数据开发 ide 节点配置获取格式化字段信息 */
  formatFlowFields(data, option) {
    if (!data) {
      return {
        fields: [],
      };
    }

    const { clusterType } = option.params;
    const clusterData = data.storage[clusterType] || {};
    const config = (clusterData && clusterData.config) || {};
    const defaultConf = this.nodeFieldsConfig[clusterType] || {};
    if (!Object.keys(config).length) {
      const defaultSetting = defaultConf.settings;
      defaultSetting
                && defaultSetting.forEach((conf) => {
                  config[conf] = [];
                });
    }
    if (clusterData.fields) {
      clusterData.fields = clusterData.fields.map((field) => {
        field.configs = [];
        this.fieldConfigMap().forEach((item) => {
          if (config[item.key] !== undefined) {
            let checked = config[item.key].includes(field.physical_field);
            if (!checked) {
              const defaultChecked = defaultConf.isChecked && defaultConf.isChecked(field, item.key);
              if (defaultChecked) {
                checked = defaultChecked;
                config[item.key].push(field.physical_field);
                field[item.field] = checked;
              }
            }
            const isReadonly = ((defaultConf.isReadonly && defaultConf.isReadonly(field, item.key))
                            || (item.isReadonly && item.isReadonly(field)));
            field.configs.push(Object.assign({}, item, { checked, isReadonly }));

            /** 处理数据，旧数据已经在Config设置了此配置字段，因为上游节点原因，此字段已经不是之前设置项，
                         * 此时Checked，移除旧数据 */
            if (checked && !field[item.field]) {
              if (Array.isArray(config[item.key])) {
                config[item.key] = config[item.key]
                  .join(',')
                  .replace(field.physical_field, '')
                  .replace(',,', ',')
                  .replace(/^,|,$/, '')
                  .split(',')
                  .filter(item => item !== '');
              }
            }
          }
        });

        return field;
      });
    }

    if (this.nodeFieldsConfig[clusterType] && this.nodeFieldsConfig[clusterType].optionType) {
      Object.assign(clusterData, {
        opt: this.nodeFieldsConfig[clusterType].optionType,
        validate:
                    (this.nodeFieldsConfig[clusterType].validate
                        && this.nodeFieldsConfig[clusterType].validate.bind(clusterData))
                    || (() => ({ isValidate: true })),
      });
    }

    // console.log('clusterData', clusterData)
    return clusterData;
  }

  /** 业务列表返回数据格式化  */
  formatBizsList(data) {
    const filterDic = [];
    return (
      (data
                && data.map((item) => {
                  const name = {
                    source_biz_name: item.bk_biz_name,
                    bk_biz_name: `[${item.bk_biz_id}]${item.bk_biz_name}`,
                  };
                  return Object.assign(item, name);
                })) || []).filter((field) => {
      if (!filterDic.includes(field.bk_biz_id)) {
        filterDic.push(field.bk_biz_id);
        return true;
      }
      return false;
    });
  }

  getMetaConfig() {
    return [
      { key: 'dim_fields', value: window.$t('维度字段'), field: 'is_dimension' },
      { key: 'indexed_fields', value: window.$t('索引字段'), field: 'is_index' },
      { key: 'storage_values', value: window.$t('构成value值'), field: 'is_value' },
      { key: 'storage_keys', value: window.$t('构成key值'), field: 'is_key' },
      { key: 'analyzed_fields', value: window.$t('分词字段'), field: 'is_analyzed' },
      { key: 'doc_values_fields', value: window.$t('聚合字段'), field: 'is_doc_values' },
      { key: 'primary_keys', value: window.$t('构成主键'), field: 'is_pri_key' },
      {
        key: 'json_fields',
        value: window.$t('json格式'),
        field: 'is_json',
        // isReadonly: (field) => !/object/i.test(field.field_type)
      },
    ];
  }

  formatGeogArea(res) {
    const currentArea = res.supported_areas || res;
    return Object.keys(currentArea).map(key => Object.assign({}, currentArea[key], { region: key }));
  }

  formatField(data) {
    const detail = data.fields_detail.filter(item => item.name !== '_startTime_' && item.name !== '_endTime_');

    return Object.assign(data, { fields_detail: detail });
  }
}

export default Meta;
