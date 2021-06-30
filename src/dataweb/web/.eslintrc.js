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

/* eslint-disable prettier/prettier */
const ESLINT_VUE_CONFIG = {
  'vue/component-definition-name-casing': 'off',
  'vue/require-default-prop': 'off',
  'vue/no-mutating-props': 'off',
  'vue/attribute-hyphenation': [
    'error',
    'never',
    {
      ignore: ['custom-prop'],
    },
  ],
  'vue/max-attributes-per-line': [
    'error',
    {
      singleline: 1,
      multiline: {
        max: 1,
        allowFirstLine: true,
      },
    },
  ],
  'vue/html-indent': [
    'error',
    2,
    {
      attribute: 1,
      baseIndent: 1,
      closeBracket: 0,
      alignAttributesVertically: false,
      ignores: [],
    },
  ],
  'vue/html-closing-bracket-newline': [
    'error',
    {
      singleline: 'never',
      multiline: 'never',
    },
  ],
  'vue/attributes-order': [
    'error',
    {
      order: [
        'DEFINITION',
        'LIST_RENDERING',
        'CONDITIONALS',
        'RENDER_MODIFIERS',
        'GLOBAL',
        ['UNIQUE', 'SLOT'],
        'TWO_WAY_BINDING',
        'OTHER_DIRECTIVES',
        'OTHER_ATTR',
        'EVENTS',
        'CONTENT',
      ],
      alphabetical: false,
    },
  ],
  'vue/order-in-components': [
    'error',
    {
      order: [
        'el',
        'name',
        'key',
        'parent',
        'functional',
        ['delimiters', 'comments'],
        ['components', 'directives', 'filters'],
        'extends',
        'mixins',
        ['provide', 'inject'],
        'ROUTER_GUARDS',
        'layout',
        'middleware',
        'validate',
        'scrollToTop',
        'transition',
        'loading',
        'inheritAttrs',
        'model',
        ['props', 'propsData'],
        'emits',
        'setup',
        'asyncData',
        'data',
        'fetch',
        'head',
        'computed',
        'watch',
        'watchQuery',
        'LIFECYCLE_HOOKS',
        'methods',
        ['template', 'render'],
        'renderError',
      ],
    },
  ],
  'vue/multiline-html-element-content-newline': [
    'error',
    {
      ignoreWhenEmpty: true,
      ignores: ['pre', 'textarea'],
      allowEmptyLines: false,
    },
  ],
  'vue/no-multi-spaces': [
    'error',
    {
      ignoreProperties: false,
    },
  ],
};

const ESLINT_CONFIG = {
  'no-underscore-dangle': 'off',
  'no-trailing-spaces': 'error',
  'implicit-arrow-linebreak': ['error', 'beside'],
  'object-curly-spacing': ['error', 'always'],
  indent: ['error', 2, { SwitchCase: 1, ignoredNodes: ['TemplateLiteral'] }],
  quotes: ['error', 'single'],
  'max-len': [
    'error',
    {
      code: 120,
    },
  ],
  'newline-per-chained-call': ['error', { ignoreChainWithDepth: 2 }],
  'operator-linebreak': [
    'error',
    'after',
    {
      overrides: {
        ':': 'before',
        '||': 'before',
        '&&': 'before',
        '+': 'before',
        '?': 'before',
        '===': 'before',
        '=': 'none',
      },
    },
  ],
  semi: ['error', 'always'],
  'space-before-function-paren': ['error', {
    'anonymous': 'always',
    'named': 'never',
    'asyncArrow': 'always'
  }],
};

const TSLINT_CONFIG = {
  '@typescript-eslint/indent': ['error', 2, { SwitchCase: 1, ignoredNodes: ['TemplateLiteral'] }],
};

const PRETTIER_OVERRIDE = {
  'prettier/prettier': ['error', { endOfLine: 'auto' }],
};

const TS_OVERRIDE = require('@tencent/eslint-config-tencent/ts');
module.exports = {
  root: true,
  env: {
    es6: true,
  },
  parser: 'vue-eslint-parser',
  extends: ['plugin:vue/recommended'],
  parserOptions: {
    parser: 'babel-eslint',
    ecmaVersion: 6,
    sourceType: 'module',
    ecmaFeatures: {
      jsx: true,
      modules: true,
      experimentalObjectRestSpread: true,
      legacyDecorators: true,
    },
  },
  overrides: [
    {
      files: ['*.ts', '*.tsx'],
      ...TS_OVERRIDE,
      rules: { ...TSLINT_CONFIG, ...PRETTIER_OVERRIDE },
    },
    {
      files: ['*.js'],
      extends: ['@tencent/eslint-config-tencent'],
      rules: { ...PRETTIER_OVERRIDE },
    },
  ],
  rules: {
    ...ESLINT_CONFIG,
    ...ESLINT_VUE_CONFIG,
  },
};
