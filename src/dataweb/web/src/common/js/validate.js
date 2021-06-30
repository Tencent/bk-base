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
/** 检验表单数据
 *  @param validate:校验设置
 *  @param params: 待校验表单对象
 */
function validateComponentForm(validate, params) {
  let isValidate = true;
  Object.keys(validate).forEach((key) => {
    validate[key].visible = false;
    if (!validateRules(validate[key].regs, params[key], validate[key])) {
      isValidate = false;
    }
  });
  return isValidate;
}

/** 校验规则
 *  @param regs: 表单配置的规则列表
 *  @param field: 待校验字段
 *  @param validate: 校验对象, 用于存储校验结果 { content: string, visible: bool }
 *  @return 校验结果 True | False
 */
function validateRules(regs, field, validate) {
  let isValidate = true;
  if (Array.isArray(regs)) {
    regs.forEach((reg) => {
      if (!validateRules(reg, field, validate)) {
        isValidate = false;
      }
    });
  } else {
    return validateRule(field, validate, regs);
  }
  return isValidate;
}

/** 设置校验对象
 *  @param field : 待校验字段值
 *  @param validate : 校验对象, 用于存储校验结果 { content: string, visible: bool }
 *  @param reg : 校验规则参数
 *  @return 是否校验通过
 */
function validateRule(field, validate, reg) {
  /** 用来表示当前字段是否已经校验过
     *  此处最后进行非空校验，如果没有其他校验则进行非空校验
     *  保证兼容性（旧的数据有些校验没有设置 Required）
     */
  let isValiated = false;

  /** 如果没有校验规则，返回True
     *  如果是多个校验规则，并其中一个已经校验为False（visible = True）则跳过
     */
  if (!validate || validate.visible) {
    return true;
  }

  /** 长度校验 */
  if (reg.length) {
    isValiated = true;
    let isLenValidate = field.length <= reg.length;
    Object.assign(validate, {
      content: reg.error || `最大长度为${reg.length}`,
      visible: !isLenValidate,
    });

    if (reg.min !== undefined && typeof reg.min === 'number') {
      isLenValidate = field.length >= reg.min;
      Object.assign(validate, {
        content: reg.error || `最小长度${reg.min}`,
        visible: !isLenValidate,
      });
    }

    return isLenValidate;
  }

  /** 正则校验 */
  if (reg.regExp) {
    isValiated = true;
    const isRegValidate = typeof reg.regExp === 'object'
      ? reg.regExp.test(field) : new RegExp(reg.regExp).test(field);
    Object.assign(validate, { content: reg.error || '校验失败', visible: !isRegValidate });
    return isRegValidate;
  }

  /** 自定义校验回调 */
  if (reg.customValidateFun && typeof reg.customValidateFun === 'function') {
    isValiated = true;
    const isFuncValidate = reg.customValidateFun(field);
    Object.assign(validate, { content: reg.error || '校验失败', visible: !isFuncValidate });
    return isFuncValidate;
  }

  /** 非空校验,这条校验规则必须放到最后，在没有任何校验规则时，进行非空检验 */
  if (reg.required || !isValiated) {
    const isEmpty = (field === undefined || field === '' || field === null
        || (Array.isArray(field) && field.length === 0));
    Object.assign(validate, { content: reg.error || window.$t('不能为空'), visible: isEmpty });
    return !isEmpty;
  }
}

/** 提交表单校验Scope
 * @param scope:待校验提交表单参数
 * @param validate:表单校验配置, 用于存储校验结果 { field: { content: string, visible: bool } }
 * @param ignoreFields:忽略不做校验的字段列表
 * @return Boollean
 */
function validateScope(scope, validate, ignoreFields = []) {
  let isValidate = true;
  const validateFun = () => Object.keys(scope).forEach((key) => {
    if (!ignoreFields.includes(key)) {
      validate[key] && Object.assign(validate[key], { visible: false });
      if (!validateRules((validate[key] && validate[key].regs) || {}, scope[key], validate[key])) {
        isValidate = false;
      }
    }
  });

  validate && validateFun();

  return isValidate;
}

export { validateComponentForm, validateScope, validateRules, validateRule };
