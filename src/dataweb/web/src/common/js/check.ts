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

import { axios as _axios } from './ajax';
// 工具函数集合
const TOOLS = {
  isArray(value: any) {
    return Object.prototype.toString.apply(value) === '[object Array]';
  },
  isObject(value: any) {
    return value instanceof Object && !(value instanceof Array);
  },
  trim(value: any) {
    if (typeof value === 'string') {
      value = value.trim();
    }
    return value;
  },
  format(source: any, params: any) {
    if (params.constructor !== Array) {
      params = [params];
    }
    for (let i = 0; i < params.length; i++) {
      source = source.replace(new RegExp(`\\{${i}\\}`, 'g'), params[i]);
    }
    return source;
  },
};

// 规则管理对象，支持新增删除规则，需要维护一个全局唯一的对象
const ValidatorHandler = (function() {
  /**
   * 校验方法
   */
  const that = {
    methods: {
      /**
       * 必填项
       * @param {Boolean} param 是否为必填项
       * @paramExample
       *      {required: true}
       */
      required(value: any, param: any) {
        value = TOOLS.trim(value);
        if (param) {
          if (value === '') {
            return false;
          }
          if (value === null) {
            return false;
          }
          if (value === undefined) {
            return false;
          }
          if (TOOLS.isArray(value) && value.length === 0) {
            return false;
          }
        }
        return true;
      },

      /**
       * 检查最小长度
       * @param {Int} param 最小长度
       * @paramExample
       *      {minlength: 1}
       */
      minlength(value: any, param: any) {
        return TOOLS.trim(value).length >= param;
      },

      /**
       * 检查最大长度
       * @param {Int} param 最大长度
       * @paramExample
       *      {maxlength: 100}
       */
      maxlength(value: any, param: any) {
        return TOOLS.trim(value).length <= param;
      },

      /**
       * 检查长度区间
       * @param {[Int]} param 限制最小长度&最大长度
       * @paramExample
       *      {rangelength: [1, 100]}
       */
      rangelength(value: any, param: any) {
        const { length } = TOOLS.trim(value);
        return length >= param[0] && length <= param[1];
      },

      /**
       * 是否符合变量格式
       * @param {Boolean} param 是否需要验证变量格式
       * @paramExample
       *      {wordFormat: true}
       * @paramExample
       *      {wordFormat: {underlineBefore: true}}
       */
      wordFormat(value: any, param: any) {
        if (param) {
          const reg = new RegExp('^[A-Za-z][0-9a-zA-Z_]*$');
          return reg.test(value);
        }
        return true;
      },
      /**
       * 是否符合变量格式
       * @param {Boolean} param 是否需要验证变量格式，支持首字母为下划线
       * @paramExample
       *      {wordUnderlineBefore: true}
       */
      wordUnderlineBefore(value: any, param: any) {
        if (param) {
          const reg = new RegExp('^[_A-Za-z][0-9a-zA-Z_]*$');
          return reg.test(value);
        }
        return true;
      },
      /**
       * 是否符合变量格式
       * @param {Boolean} param 是否需要验证变量格式
       * @paramExample
       *      {required: true}
       */
      wordFormat2(value: any, param: any) {
        if (param) {
          const reg = new RegExp('^/');
          return reg.test(value);
        }
        return true;
      },
      /**
       * 是否符合变量格式
       * @param {Boolean} param 是否需要验证变量格式
       * @paramExample
       *      {required: true}
       */
      numFormat(value: any, param: any) {
        if (param) {
          const reg = /^\d+$/;
          return reg.test(value);
        }
        return true;
      },
    },
    zhErrMessage: {
      required: '必填项不可为空',
      minlength: '允许的最小长度为{0}个字符',
      maxlength: '允许的最大长度为{0}个字符',
      rangelength: '允许的长度为{0}和{1}之间',
      wordFormat: '内容由字母、数字和下划线组成，且以字母开头',
      wordUnderlineBefore: '内容由字母、数字和下划线组成，不可以数字开头',
      wordFormat2: '日志路径由"/"开头',
      numFormat: '请输入整数',
    },
    enErrMessage: {
      required: 'This field is required',
      minlength: 'Please enter at least {0} characters',
      maxlength: 'Please enter no more than {0} characters',
      rangelength: 'Please enter a value between {0} and {1} characters long',
      wordFormat: 'It consists of letters, digits and underlines and start with letters',
      wordFormat2: 'Start with "/"',
      numFormat: 'Please enter integer',
    },
    errMessage: '',
  };

  Object.assign(that, {
    errMessage: that.zhErrMessage,
    /**
     * 添加自定义校验规则
     * @param {String} key 校验规则key，标识
     * @param {Function} validFunc 校验函数
     */
    addMethod(key: string, validFunc: () => void, errMsg: string) {
      // @ts-ignore
      that.methods[key] = validFunc;
      if (errMsg !== undefined) {
        // @ts-ignore
        that.zhErrMessage[key] = errMsg;
      }
    },
    /**
     * 设置翻译语言，目前仅支持 en、zh-cn
     */
    setLang(lang: string) {
      if (lang === 'en') {
        Object.assign(that, { language: 'en', errMessage: that.enErrMessage });
      } else {
        Object.assign(that, { language: 'zh-cn', errMessage: that.zhErrMessage });
      }
    },
  });
  return that;
})();

// 校验对象，根据定义的规则进行校验
function Validator(rules: any) {
  // @ts-ignore
  const that = this;
  that.rules = rules;
  that.errMsg = '';
  that.errFieldMsg = {};
  return that;
}

Validator.prototype._valid = function(value: any, rules: any) {
  for (const key in rules) {
    // @ts-ignore
    if (ValidatorHandler.methods[key] !== undefined) {
      // @ts-ignore
      const func = ValidatorHandler.methods[key];
      const params = rules[key];
      if (!func(value, params)) {
        // @ts-ignore
        return [false, TOOLS.format(ValidatorHandler.errMessage[key], params)];
      }
    }
  }
  return [true, ''];
};

/**
 * 根据传入规则对单个字段进行校验，此时对应的
 */
Validator.prototype.valid = function(value: any) {
  const [ret, errMsg] = this._valid(value, this.rules);

  this.errMsg = errMsg;
  return ret;
};

/**
 * 根据传入规则对多字段进行校验
 */
Validator.prototype.valids = function(value: any, field: any) {
  this.errFieldMsg = {};
  let checkRet = true;

  for (const _f in this.rules) {
    if (field !== undefined && field !== _f) {
      continue;
    }

    const [ret, errMsg] = this._valid(value[_f], this.rules[_f]);
    this.errFieldMsg[_f] = errMsg;
    if (!ret) {
      checkRet = false;
    }
  }
  return checkRet;
};

const JValidatorHandler = ValidatorHandler;
const JValidator = Validator;

export { JValidatorHandler, JValidator };
export default TOOLS;
