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
/**
 * @msg:接入方式定义
 * @param {type}
 * @return:
 */
const ACCESS_METHODS = {
  /**
     * @msg: 采集方式
     */
  COLLECTION_TYPE: 1,

  /**
     * @msg: 采集周期
     */
  COLLECTION_CYCLE: 2,

  /**
     * @增量字段
     */
  INCREMENTAL_FIELD: 3,

  /**
     * @时间格式
     */
  DATETIME_FORMAT: 4,

  /**
     * @存量数据
     */
  STOCK_DATA: 5,

  /**
     * 数据延迟时间
     */
  DATA_DELAY_TIME: 6,
};

/**
   * 下拉选项定义
   */
const ACCESS_OPTIONS = {
  /**
     * 采集方式可选项
     */
  [ACCESS_METHODS.COLLECTION_TYPE]: {
    /** 增量 */
    INCREMENTAL: {
      text: window.$t('增量'),
      value: 'incr',
    },

    /** '增量(主键) */
    INCR_PRI: {
      text: window.$t('增量_主键'),
      value: 'pri',
    },

    /** 增量(时间范围) */
    INCR_TIME: {
      text: window.$t('增量_时间范围'),
      value: 'time',
    },

    /** 全量 */
    ALL: {
      text: window.$t('全量'),
      value: 'all',
    },
    /** 拉取数据， Http 场景使用 */
    PULL: {
      text: window.$t('拉取'),
      value: 'pull',
    },

    /** 推送数据, Http场景使用 */
    PUSH: {
      text: window.$t('推送'),
      value: 'push',
    },
  },

  /**
     * 采集周期可选项
     */
  [ACCESS_METHODS.COLLECTION_CYCLE]: {
    /**
       * 实时
       */
    REALTIME: {
      text: window.$t('实时'),
      value: '-1',
    },

    /**
       * 周期
       */
    CYCLE: {
      text: window.$t('周期性'),
      value: 'n',
    },

    /**
       * 一次性
       */
    ONETIME: {
      text: window.$t('一次性'),
      value: '0',
    },
  },

  /**
     * 是否有数据延时时间
     */
  [ACCESS_METHODS.DATA_DELAY_TIME]: {
    TRUE: { text: window.$t('是'), value: true },
    FALSE: { text: window.$t('否'), value: false },
  },

  /**
     * 时间格式
     */
  [ACCESS_METHODS.DATETIME_FORMAT]: {
    yyyyMMdd: { text: 'yyyy-MM-dd HH:mm:ss', value: 'yyyy-MM-dd HH:mm:ss' },
    UnixTimeStamp_mins: { text: 'Unix Time Stamp(mins)', value: 'Unix Time Stamp(mins)' },
  },

  /** 增量字段 */
  [ACCESS_METHODS.INCREMENTAL_FIELD]: {
    /** 行号 */
    ROW_NUMBER: { text: window.$t('行号'), value: 'index' },
    /** 自增字段 */
    INCREMENT: { text: window.$t('自增字段'), value: 'increment' },
    /** 时间字段 */
    TIME: { text: window.$t('时间字段'), value: 'time' },
    /** ID */
    ID: { text: 'ID', value: 'id' },
  },

  /** 存量数据 */
  [ACCESS_METHODS.STOCK_DATA]: {
    TRUE: { text: window.$t('是'), value: true },
    FALSE: { text: window.$t('否'), value: false },
  },
};

/** 采集周期单位 */
const CYCLE_UNIT = {
  /** 小时 */
  HOUR: {
    text: window.$t('时'),
    value: 'h',
  },
  /** 分钟 */
  MINUTE: {
    text: window.$t('分'),
    value: 'm',
  },
  /** 秒 */
  DAY: {
    text: window.$t('天'),
    value: 'd',
  },
};
export { ACCESS_METHODS, ACCESS_OPTIONS, CYCLE_UNIT };
