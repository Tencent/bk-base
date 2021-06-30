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

/* eslint-disable no-underscore-dangle */
/* eslint-disable no-param-reassign */
/* eslint-disable no-unused-vars */
/**
 * @author: laurent blanes <laurent.blanes@gmail.com>
 * @tutorial: https://hekigan.github.io/vue-directive-tooltip/
 */
import Tooltip from './tooltip.js';

const BASE_CLASS = 'vue-tooltip';
const POSITIONS = ['auto', 'top', 'bottom', 'left', 'right'];
const SUB_POSITIONS = ['start', 'end'];

/**
 * usage:
 *
 * // basic usage:
 * <div v-tooltip="'my content'">
 * or
 * <div v-tooltip="{content: 'my content'}">
 *
 * // change position of tooltip
 * // options: auto (default) | bottom | top | left | right
 *
 * // change sub-position of tooltip
 * // options: start | end
 *
 * <div v-tooltip.top="{content: 'my content'}">
 *
 * // add custom class
 * <div v-tooltip="{class: 'custom-class', content: 'my content'}">
 *
 * // toggle visibility
 * <div v-tooltip="{visible: false, content: 'my content'}">
 */
export default {
  name: 'tooltip',
  config: {},
  install(Vue, installOptions) {
    Vue.directive('tooltip', {
      bind(el, binding, vnode) {
        if (installOptions) {
          Tooltip.defaults(installOptions);
        }
      },
      inserted(el, binding, vnode, oldVnode) {
        if (installOptions) {
          Tooltip.defaults(installOptions);
        }

        const options = filterBindings(binding);
        el.tooltip = new Tooltip(el, options);

        if (binding.modifiers.notrigger && binding.value.visible === true) {
          el.tooltip.show();
        }

        if (binding.value && binding.value.visible === false) {
          el.tooltip.disabled = true;
        }
      },
      // update(el, binding) {
      //     console.log('directive update', el, binding)
      // },
      componentUpdated(el, binding, vnode, oldVnode) {
        update(el, binding);
      },
      unbind(el, binding, vnode, oldVnode) {
        el.tooltip.destroy();
      },
    });
  },
};

function filterBindings(binding) {
  const delay = !binding.value || isNaN(binding.value.delay) ? Tooltip._defaults.delay : binding.value.delay;

  return {
    class: getClass(binding),
    html: binding.value ? binding.value.html : null,
    placement: getPlacement(binding),
    title: getContent(binding),
    triggers: getTriggers(binding),
    fixIosSafari: binding.modifiers.ios || false,
    offset: binding.value && binding.value.offset ? binding.value.offset : Tooltip._defaults.offset,
    delay,
  };
}

/**
 * Get placement from modifiers
 * @param {*} binding
 */
function getPlacement({ modifiers, value }) {
  let MODS = Object.keys(modifiers);
  if (MODS.length === 0 && isObject(value) && typeof value.placement === 'string') {
    MODS = value.placement.split('.');
  }
  let head = '';
  let tail = null;
  for (let i = 0; i < MODS.length; i++) {
    const pos = MODS[i];
    if (POSITIONS.indexOf(pos) > -1) {
      head = pos;
    }
    if (SUB_POSITIONS.indexOf(pos) > -1) {
      tail = pos;
    }
  }
  return head && tail ? `${head}-${tail}` : head;
}

/**
 * Get trigger value from modifiers
 * @param {*} binding
 * @return String
 */
function getTriggers({ modifiers }) {
  const trigger = [];
  if (modifiers.notrigger) {
    return trigger;
  }
  if (modifiers.manual) {
    trigger.push('manual');
  } else {
    if (modifiers.click) {
      trigger.push('click');
    }

    if (modifiers.hover) {
      trigger.push('hover');
    }

    if (modifiers.focus) {
      trigger.push('focus');
    }

    if (trigger.length === 0) {
      trigger.push('hover', 'focus');
    }
  }

  return trigger;
}

/**
 * Check if the variable is an object
 * @param {*} value
 * @return Boolean
 */
function isObject(value) {
  return typeof value === 'object';
}

/**
 * Check if the variable is an html element
 * @param {*} value
 * @return Boolean
 */
function isElement(value) {
  return value instanceof window.Element;
}

/**
 * Get the css class
 * @param {*} binding
 * @return HTMLElement | String
 */
function getClass({ value }) {
  if (value === null) {
    return BASE_CLASS;
  }
  if (isObject(value) && typeof value.class === 'string') {
    return `${BASE_CLASS} ${value.class}`;
  }
  if (Tooltip._defaults.class) {
    return `${BASE_CLASS} ${Tooltip._defaults.class}`;
  }
  return BASE_CLASS;
}

/**
 * Get the content
 * @param {*} binding
 * @return HTMLElement | String
 */
function getContent({ value }) {
  if (value !== null && isObject(value)) {
    if (value.content !== undefined) {
      return `${value.content}`;
    }
    if (value.html && document.getElementById(value.html)) {
      return document.getElementById(value.html);
    }
    if (isElement(value.html)) {
      return value.html;
    }
    return '';
  }
  return `${value}`;
}

/**
 * Action on element update
 * @param {*} el Vue element
 * @param {*} binding
 */
function update(el, binding) {
  if (typeof binding.value === 'string') {
    el.tooltip.content(binding.value);
  } else {
    if (
      binding.value
            && binding.value.class
            && binding.value.class.trim() !== el.tooltip.options.class.replace(BASE_CLASS, '').trim()
    ) {
      el.tooltip.class = `${BASE_CLASS} ${binding.value.class.trim()}`;
    }

    el.tooltip.content(getContent(binding));

    if (!binding.modifiers.notrigger && binding.value && typeof binding.value.visible === 'boolean') {
      el.tooltip.disabled = !binding.value.visible;
      return;
    }
    if (binding.modifiers.notrigger) {
      el.tooltip.disabled = false;
    }

    if (!el.tooltip.disabled && binding.value && binding.value.visible === true) {
      el.tooltip.show();
    } else {
      el.tooltip.hide();
    }
  }
}
