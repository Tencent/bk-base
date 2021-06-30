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
import { ACCESS_METHODS, ACCESS_OPTIONS, CYCLE_UNIT } from '../Constant/index';
import { linkToCC } from '@/common/js/util';
import extend from '@/extends/index';

/**
 * 将传入时间转换为单位为分钟的时间数
 * @param {number} period 时间数
 * @param {string} unit 时间单位
 * @returns {number} 返回单位为分钟的时间数
 */
function getPeriodTime(period, unit) {
  if (period >= 0) {
    switch (unit) {
      case 'h':
        period = period * 60;
        break;
      case 'm':
        break;
      case 'd':
        period = period * 24 * 60;
        break;
    }
  }

  return period;
}

/**
 * 生成基本的提交参数
 * @param {Array} scopes
 * @returns
 */
function getScopes(scopes) {
  return scopes.map((item) => {
    const data = {
      module_scope: null,
      host_scope: null,
      scope_config: null,
      deploy_plan_id: null,
      odm: null,
      struct_name: null,
    };
    return new ModelInstance(data).formatData(item, true);
  });
}

/** 接入场景数据格式化Callback辅助类
 *  用于动态格式化不同模块数据，以生成符合服务器端格式数据
 */
class ModelInstance {
  constructor(model) {
    this.origin = model;
  }

  /**
     * 格式化数据，用于最终表单提交
     * @param {Object} val 需要格式化的数据
     * @param {boolean} ignoreUndefined 是否忽略undefined
     * @returns
     */
  formatData(val, ignoreUndefined = false) {
    const tempOrigin = {};
    Object.keys(this.origin).forEach((key) => {
      if (ignoreUndefined) {
        val[key] !== undefined && Object.assign(tempOrigin, { [key]: val[key] });
      } else {
        val[key] !== '' && Object.assign(this.origin, { [key]: val[key] });
      }
    });
    if (ignoreUndefined) {
      this.origin = tempOrigin;
    }
    return this.origin;
  }
}

/** 基础表单提交配置，所有模块相同部分 */
const base = {
  bk_app_code: 'bk_dataweb',
  bk_username: '',
  data_scenario: '',
  bk_biz_id: 0,
  description: '',
  access_raw_data: {
    tags: [],
    raw_data_name: '',
    maintainer: '',
    raw_data_alias: '',
    data_source_tags: [],
    data_region: 'inland',
    data_source: 'svr',
    data_encoding: 'UTF-8',
    sensitivity: 'private',
    description: '',
  },
};

/** 日志文件提交表单格式配置 */
const logConfig = Object.assign({}, base, {
  access_conf_info: {
    collection_model(val) {
      return {
        collection_type: val.collection_type,
        start_at: val.start_at,
        period: val.period,
      };
    },
    filters(val) {
      val = val || {};
      return {
        delimiter: val.delimiter || '',
        fields: (val.fields || []).filter(field => field.value !== '' && /^\d+$/.test(field.index)) || [],
        ignore_file_end_with: val.ignore_file_end_with,
      };
    },
    resource(resource) {
      return { scope: getScopes(resource.scope) };
    },
  },
});

/** 提交表单：DB接入数据格式化 */
const dbinstance = new ModelInstance({
  collection_type: 'time',
  start_at: 0,
  period: 0,
  time_format: 'yyyy-MM-dd HH:mm:ss',
  increment_field: 'create_at',
  before_time: 100,
});

/** 数据库提交表单格式配置 */
const dbConfig = Object.assign({}, base, {
  access_conf_info: {
    collection_model(val) {
      if (val.collection_type === 'incr') {
        if (val.increment_field_type === 'id') {
          val.collection_type = 'pri';
        } else {
          val.collection_type = 'time';
        }
      }

      val.period = getPeriodTime(val.period, val.periodUnit);
      return dbinstance.formatData(val);
    },
    filters(val) {
      if (val.fields) {
        return val.fields.map(field => ({
          key: field.index,
          logic_op: field.logic_op,
          value: field.value,
          op: field.op,
        }));
      }
      return [];
    },
    resource(resource) {
      return {
        scope: resource.scope.map((item) => {
          const data = {
            db_host: 'x.x.x.x',
            db_port: 10000,
            db_user: 'user',
            db_pass: 'pwd',
            db_name: 'bkdata_basic',
            table_name: 'access_db_info',
            db_type_id: 1,
            deploy_plan_id: 0,
          };
          return new ModelInstance(data).formatData(item, true);
        }),
      };
    },
  },
});

/** http 模块 collection_model */
const httpinstance = new ModelInstance({
  collection_type: 'pull',
  period: 0,
  time_format: '',
  increment_field: '',
});

/** http表单提交配置 */
const httpConfig = Object.assign({}, base, {
  access_conf_info: {
    collection_model: (val) => {
      val.period = getPeriodTime(val.period, val.periodUnit);
      return httpinstance.formatData(val);
    },
    filters: {
      delimiter: '',
      fields: [],
    },
    resource(resource) {
      return {
        scope: resource.scope.map((item) => {
          const data = {
            url: '',
            method: '',
            deploy_plan_id: 0,
          };
          return new ModelInstance(data).formatData(item, true);
        }),
      };
    },
  },
});

/** 文件上传表单配置 */
const fileConfig = Object.assign({}, base, {
  access_conf_info: {
    collection_model: {
      collection_type: 'incr',
      period: 0,
    },
    filters: {
      delimiter: '|',
      fields: [],
    },
    resource(resource) {
      return {
        scope: resource.scope.map((item) => {
          const data = {
            file_name: '',
            type: 'hdfs',
            deploy_plan_id: 0,
          };
          return new ModelInstance(data).formatData(item, true);
        }),
      };
    },
  },
});

/** 脚本上报 提交表单配置 */
const scriptConfig = Object.assign({}, base, {
  access_conf_info: {
    collection_model: (model) => {
      const period = getPeriodTime(Number(model.period), model.periodUnit);
      return { period };
    },
    filters: {
      delimiter: '|',
      fields: [],
    },
    resource(resource) {
      return { scope: getScopes(resource.scope) };
    },
  },
});

/** kafka 提交配置 */
const kafkaConfig = Object.assign({}, base, {
  access_conf_info: {
    collection_model: {
      collection_type: 'incr',
      start_at: 1,
      period: 0,
    },
    resource(resource) {
      return {
        type: 'kafka',
        scope: resource.scope.map((item) => {
          const data = {
            master: '',
            group: '',
            topic: '',
            tid: '',
            tasks: 1,
            use_sasl: false,
            security_protocol: 'SASL_PLAINTEXT',
            sasl_mechanism: 'SCRAM-SHA-512',
            user: '',
            password: '',
            auto_offset_reset: 'latest',
          };
          return new ModelInstance(data).formatData(item, true);
        }),
      };
    },
  },
});

/** 日志文件动态配置 */
const logSchema = {
  /** 接入方式 */
  accessMethod: {
    [ACCESS_METHODS.COLLECTION_TYPE]: [
      {
        option: ACCESS_OPTIONS[ACCESS_METHODS.COLLECTION_TYPE].INCREMENTAL,
        disabled: false,
        selected: true,
        [ACCESS_METHODS.COLLECTION_CYCLE]: {
          show: true,
          disabled: true,
          options: [ACCESS_OPTIONS[ACCESS_METHODS.COLLECTION_CYCLE].REALTIME],
          units: [],
          child: {
            [ACCESS_METHODS.INCREMENTAL_FIELD]: {
              show: false,
              disabled: true,
              options: [ACCESS_OPTIONS[ACCESS_METHODS.INCREMENTAL_FIELD].ROW_NUMBER],
              child: {
                [ACCESS_METHODS.DATETIME_FORMAT]: {
                  show: false,
                  child: {
                    [ACCESS_METHODS.STOCK_DATA]: {
                      show: false,
                      disabled: false,
                      isTrue: true,
                    },
                  },
                },
              },
            },
          },
        },
      },
      {
        option: ACCESS_OPTIONS[ACCESS_METHODS.COLLECTION_TYPE].ALL,
        disabled: true,
        selected: false,
        [ACCESS_METHODS.COLLECTION_CYCLE]: {
          show: true,
          options: [
            ACCESS_OPTIONS[ACCESS_METHODS.COLLECTION_CYCLE].CYCLE,
            ACCESS_OPTIONS[ACCESS_METHODS.COLLECTION_CYCLE].ONETIME,
          ],
          units: [[CYCLE_UNIT.HOUR], []],
          child: {
            [ACCESS_METHODS.INCREMENTAL_FIELD]: {
              show: false,
              options: [],
              child: {
                [ACCESS_METHODS.DATETIME_FORMAT]: {
                  show: false,
                  child: {
                    [ACCESS_METHODS.STOCK_DATA]: {
                      show: true,
                      disabled: true,
                      isTrue: false,
                    },
                  },
                },
              },
            },
          },
        },
      },
    ],
  },

  /** 过滤条件配置 */
  filterCondition: {
    disable: false,
    tips: true,
    ignoreFiles: true,
    operators: [
      {
        id: '=',
        name: `= ${window.$t('等于')}`,
      },
    ],
  },

  /** 右侧提示部分配置 */
  rightTips: {
    content: [window.$t('日志接入Tips'), '<hyperLink>'],
    hyperLink: {
      text: window.$t('申请权限地址'),
      href: 'javascript:void(0);',
      onClick: () => linkToCC(true),
    },
  },
};

/** 数据库动态配置 */
const dbSchema = {
  /** 接入方式 */
  accessMethod: {
    [ACCESS_METHODS.COLLECTION_TYPE]: [
      {
        option: ACCESS_OPTIONS[ACCESS_METHODS.COLLECTION_TYPE].INCREMENTAL,
        selected: true,
        [ACCESS_METHODS.COLLECTION_CYCLE]: {
          show: true,
          options: [ACCESS_OPTIONS[ACCESS_METHODS.COLLECTION_CYCLE].CYCLE],
          units: [[CYCLE_UNIT.MINUTE]],
          child: [
            {
              [ACCESS_METHODS.INCREMENTAL_FIELD]: {
                show: true,
                disabled: false,
                options: [
                  ACCESS_OPTIONS[ACCESS_METHODS.INCREMENTAL_FIELD].TIME,
                  ACCESS_OPTIONS[ACCESS_METHODS.INCREMENTAL_FIELD].ID,
                ],
                child: [
                  {
                    [ACCESS_METHODS.DATETIME_FORMAT]: {
                      show: true,
                      disabled: false,
                      options: [ACCESS_OPTIONS[ACCESS_METHODS.DATETIME_FORMAT].yyyyMMdd],
                      child: {
                        [ACCESS_METHODS.DATA_DELAY_TIME]: {
                          show: true,
                          disabled: false,
                          units: [CYCLE_UNIT.MINUTE],
                        },
                        [ACCESS_METHODS.STOCK_DATA]: {
                          show: true,
                          disabled: false,
                          isTrue: false,
                        },
                      },
                    },
                  },
                  {
                    [ACCESS_METHODS.DATETIME_FORMAT]: {
                      show: true,
                      disabled: true,
                      options: [],
                      child: {
                        [ACCESS_METHODS.DATA_DELAY_TIME]: {
                          show: true,
                          disabled: true,
                        },
                        [ACCESS_METHODS.STOCK_DATA]: {
                          show: true,
                          disabled: false,
                          isTrue: false,
                        },
                      },
                    },
                  },
                ],
              },
            },
          ],
        },
      },
      {
        option: ACCESS_OPTIONS[ACCESS_METHODS.COLLECTION_TYPE].ALL,
        tips: window.$t('全量采集最大支持5000条'),
        [ACCESS_METHODS.COLLECTION_CYCLE]: {
          show: true,
          options: [ACCESS_OPTIONS[ACCESS_METHODS.COLLECTION_CYCLE].CYCLE],
          units: [[CYCLE_UNIT.MINUTE, CYCLE_UNIT.HOUR]],
          child: [
            {
              [ACCESS_METHODS.INCREMENTAL_FIELD]: {
                show: false,
                options: [],
                child: {
                  [ACCESS_METHODS.DATETIME_FORMAT]: {
                    show: false,
                    options: [],
                    child: {
                      [ACCESS_METHODS.STOCK_DATA]: {
                        show: false,
                        disabled: true,
                        isTrue: false,
                      },
                    },
                  },
                },
              },
            },
          ],
        },
      },
    ],
    period: {
      all: [60, 24],
      incr: [1],
    },
  },

  /** 过滤条件配置 */
  filterCondition: {
    isDbMode: true,
    disable: false,
    tips: false,
    operators: [
      {
        id: '=',
        name: `= ${window.$t('等于')}`,
      },
      {
        id: 'in',
        name: `In ${window.$t('包含')}`,
      },
    ],
    /** 映射保存到数据库的数据格式到前端数据结构 */
    mapDBDataToWeb(filters) {
      filters = filters || [];
      return {
        delimiter: '',
        fields: filters.map(field => ({
          index: field.key,
          logic_op: field.logic_op,
          value: field.value,
          op: field.op,
        })),
      };
    },
  },

  /** 右侧提示部分配置 */
  rightTips: {
    content: [window.$t('DB接入Tip1'), window.$t('DB接入Tip2'), 'HttpRequest'],
    httpRequest: {
      field: 'ipList',
      listKey: 'ip',
    },
  },
};

/** http 动态配置 */
const httpSchema = {
  /** 接入方式 */
  accessMethod: {
    [ACCESS_METHODS.COLLECTION_TYPE]: [
      {
        option: ACCESS_OPTIONS[ACCESS_METHODS.COLLECTION_TYPE].PULL,
        selected: true,
        disabled: false,
        [ACCESS_METHODS.COLLECTION_CYCLE]: {
          show: true,
          options: [ACCESS_OPTIONS[ACCESS_METHODS.COLLECTION_CYCLE].CYCLE],
          units: [[CYCLE_UNIT.MINUTE, CYCLE_UNIT.HOUR, CYCLE_UNIT.DAY]],
          child: {
            [ACCESS_METHODS.INCREMENTAL_FIELD]: {
              show: false,
              options: [ACCESS_OPTIONS[ACCESS_METHODS.INCREMENTAL_FIELD].TIME],
              child: {
                [ACCESS_METHODS.DATETIME_FORMAT]: {
                  show: false,
                  options: [ACCESS_OPTIONS[ACCESS_METHODS.DATETIME_FORMAT].yyyyMMdd],
                  child: {
                    [ACCESS_METHODS.STOCK_DATA]: {
                      show: false,
                      disabled: true,
                      isTrue: false,
                    },
                  },
                },
              },
            },
          },
        },
      },
      {
        option: ACCESS_OPTIONS[ACCESS_METHODS.COLLECTION_TYPE].PUSH,
        selected: false,
        disabled: true,
        [ACCESS_METHODS.COLLECTION_CYCLE]: {
          show: true,
          options: [ACCESS_OPTIONS[ACCESS_METHODS.COLLECTION_CYCLE].CYCLE],
          units: [[CYCLE_UNIT.MINUTE, CYCLE_UNIT.HOUR, CYCLE_UNIT.DAY]],
          child: {
            [ACCESS_METHODS.INCREMENTAL_FIELD]: {
              show: false,
              options: [ACCESS_OPTIONS[ACCESS_METHODS.INCREMENTAL_FIELD].TIME],
              child: {
                [ACCESS_METHODS.DATETIME_FORMAT]: {
                  show: false,
                  options: [ACCESS_OPTIONS[ACCESS_METHODS.DATETIME_FORMAT].yyyyMMdd],
                  child: {
                    [ACCESS_METHODS.STOCK_DATA]: {
                      show: false,
                      disabled: true,
                      isTrue: false,
                    },
                  },
                },
              },
            },
          },
        },
      },
    ],
    period: {
      pull: [60, 24],
      push: [1],
    },
  },
  /** 过滤条件配置 */
  filterCondition: {
    disable: true,
  },

  /** 右侧提示部分配置 */
  rightTips: {
    content: [window.$t('Http接入Tip1'), window.$t('Http接入Tip2'), 'HttpRequest'],
    httpRequest: {
      field: 'ipList',
      listKey: 'ip',
    },
  },
};

/** 文件上传 动态配置 */
const fileSchema = type => ({
  /** 接入方式 */
  accessMethod: {
    [ACCESS_METHODS.COLLECTION_TYPE]: [
      {
        option: ACCESS_OPTIONS[ACCESS_METHODS.COLLECTION_TYPE].ALL,
        selected: true,
        disabled: false,
        [ACCESS_METHODS.COLLECTION_CYCLE]: {
          show: true,
          disabled: true,
          options: [ACCESS_OPTIONS[ACCESS_METHODS.COLLECTION_CYCLE].ONETIME],
          units: [],
          child: {
            [ACCESS_METHODS.INCREMENTAL_FIELD]: {
              show: false,
              disabled: true,
              options: [],
              child: {
                [ACCESS_METHODS.DATETIME_FORMAT]: {
                  show: false,
                  disabled: true,
                  options: [],
                  child: {
                    [ACCESS_METHODS.STOCK_DATA]: {
                      show: false,
                      disabled: true,
                      isTrue: false,
                    },
                  },
                },
              },
            },
          },
        },
      },
      {
        option: ACCESS_OPTIONS[ACCESS_METHODS.COLLECTION_TYPE].INCREMENTAL,
        selected: false,
        disabled: true,
      },
    ],
  },
  /** 过滤条件配置 */
  filterCondition: {
    disable: true,
  },

  /** 右侧提示部分配置 */
  rightTips: {
    content: type === 'file' ? [window.$t('日志接入Tip')] : [window.$t('离线日志接入Tip')],
  },
});

/** script 动态配置 */
const scriptSchema = {
  /** 接入方式 */
  accessMethod: {
    [ACCESS_METHODS.COLLECTION_TYPE]: [
      {
        option: ACCESS_OPTIONS[ACCESS_METHODS.COLLECTION_TYPE].INCREMENTAL,
        selected: true,
        disabled: false,
        [ACCESS_METHODS.COLLECTION_CYCLE]: {
          show: true,
          disabled: false,
          options: [ACCESS_OPTIONS[ACCESS_METHODS.COLLECTION_CYCLE].CYCLE],
          units: [[CYCLE_UNIT.MINUTE, CYCLE_UNIT.HOUR, CYCLE_UNIT.DAY]],
          child: {
            [ACCESS_METHODS.INCREMENTAL_FIELD]: {
              show: false,
              disabled: true,
              options: [],
              child: {
                [ACCESS_METHODS.DATETIME_FORMAT]: {
                  show: false,
                  disabled: true,
                  options: [],
                  child: {
                    [ACCESS_METHODS.STOCK_DATA]: {
                      show: false,
                      disabled: true,
                      isTrue: false,
                    },
                  },
                },
              },
            },
          },
        },
      },
      {
        option: ACCESS_OPTIONS[ACCESS_METHODS.COLLECTION_TYPE].ALL,
        selected: false,
        disabled: true,
      },
    ],
    period: {
      all: [1],
      incr: [60, 24],
    },
  },
  /** 过滤条件配置 */
  filterCondition: {
    disable: true,
  },
  /** 右侧提示部分配置 */
  rightTips: {
    content: [window.$t('脚本上报Tip'), '<hyperLink>'],
    hyperLink: {
      text: window.$t('申请权限地址'),
      href: 'javascript:void(0);',
      onClick: () => linkToCC(true),
    },
  },
};

/** kafka  */
const kafkaSchema = {
  /** 接入方式 */
  accessMethod: {
    [ACCESS_METHODS.COLLECTION_TYPE]: [
      {
        option: ACCESS_OPTIONS[ACCESS_METHODS.COLLECTION_TYPE].INCREMENTAL,
        selected: true,
        disabled: false,
        [ACCESS_METHODS.COLLECTION_CYCLE]: {
          show: true,
          disabled: true,
          options: [ACCESS_OPTIONS[ACCESS_METHODS.COLLECTION_CYCLE].CYCLE],
          units: [[CYCLE_UNIT.HOUR, CYCLE_UNIT.MINUTE, CYCLE_UNIT.DAY]],
          child: {
            [ACCESS_METHODS.INCREMENTAL_FIELD]: {
              show: false,
              disabled: true,
              options: [],
              child: {
                [ACCESS_METHODS.DATETIME_FORMAT]: {
                  show: false,
                  disabled: true,
                  options: [],
                  child: {
                    [ACCESS_METHODS.STOCK_DATA]: {
                      show: true,
                      disabled: true,
                      isTrue: false,
                    },
                  },
                },
              },
            },
          },
        },
      },
      {
        option: ACCESS_OPTIONS[ACCESS_METHODS.COLLECTION_TYPE].ALL,
        selected: false,
        disabled: true,
      },
    ],
  },
  filterCondition: {
    disable: true,
  },

  /** 数据定义配置 */
  dataDefine: {
    raw_data_name: {
      disabled: false,
    },
    raw_data_alias: {
      disabled: false,
    },
  },

  /** 右侧提示部分配置 */
  rightTips: {
    content: [window.$t('kafkaTip1'), window.$t('当前支持kafka队列')],
  },
};

/** 自定义配置 */
const customSchema = {
  accessMethod: {
    disable: true,
  },
  filterCondition: {
    disable: true,
  },
  accessObject: {
    disable: true,
  },
  /** 右侧提示部分配置 */
  rightTips: {
    content: [window.$t('自定义配置Tip')],
  },

  /** 上报说明 */
  reporting: {
    enabled: true,
  },
};

export default async () => {
  const configExt = await extend.callJsFragmentFn('AccessSchema') || {};
  return {
    log: { form: logConfig, schema: logSchema },
    db: { form: dbConfig, schema: dbSchema },
    http: { form: httpConfig, schema: httpSchema },
    file: { form: fileConfig, schema: fileSchema('file') },
    offlinefile: { form: fileConfig, schema: fileSchema('offlineFile') },
    script: { form: scriptConfig, schema: scriptSchema },
    custom: { form: base, schema: customSchema },
    kafka: { form: kafkaConfig, schema: kafkaSchema },
    ...(configExt.default || {})
  };
};
