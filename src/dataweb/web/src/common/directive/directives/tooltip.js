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

/* eslint-disable array-callback-return */
/* eslint-disable no-param-reassign */
/* eslint-disable no-unused-vars */
/* eslint-disable no-underscore-dangle */
import Popper from 'popper.js';

const BASE_CLASS = 'h-tooltip  vue-tooltip-hidden';
const PLACEMENT = ['top', 'left', 'right', 'bottom', 'auto'];
const SUB_PLACEMENT = ['start', 'end'];

const EVENTS = {
  ADD: 1,
  REMOVE: 2,
};

const DEFAULT_OPTIONS = {
  container: false,
  delay: 200,
  instance: null, // the popper.js instance
  fixIosSafari: false,
  eventsEnabled: true,
  html: false,
  modifiers: {
    arrow: {
      element: '.tooltip-arrow',
    },
  },
  placement: '',
  placementPostfix: null, // start | end
  removeOnDestroy: true,
  title: '',
  class: '', // ex: 'tooltip-custom tooltip-other-custom'
  triggers: ['hover', 'focus'],
  offset: 5,
};

const includes = (stack, needle) => stack.indexOf(needle) > -1;

export default class Tooltip {
  constructor(el, options = {}) {
    // Tooltip._defaults = DEFAULT_OPTIONS;
    this._options = {
      ...Tooltip._defaults,
      ...{
        onCreate: () => {
          this.content(this.tooltip.options.title);
          this._$tt.update();
        },
        onUpdate: () => {
          this.content(this.tooltip.options.title);
          this._$tt.update();
        },
      },
      ...Tooltip.filterOptions(options),
    };

    const $tpl = this._createTooltipElement(this.options);
    document.querySelector('body').appendChild($tpl);

    this._$el = el;
    this._$tt = new Popper(el, $tpl, this._options);
    this._$tpl = $tpl;
    this._disabled = false;
    this._visible = false;
    this._clearDelay = null;
    this._setEvents();
  }

  destroy() {
    this._cleanEvents();
    document.querySelector('body').removeChild(this._$tpl);
  }

  get options() {
    return { ...this._options };
  }

  get tooltip() {
    return this._$tt;
  }

  _createTooltipElement() {
    // wrapper
    const $popper = document.createElement('div');
    $popper.setAttribute('id', `tooltip-${randomId()}`);
    $popper.setAttribute('class', `${BASE_CLASS} ${this._options.class}`);

    // make arrow
    const $arrow = document.createElement('div');
    $arrow.setAttribute('class', 'tooltip-arrow');
    $arrow.setAttribute('x-arrow', '');
    $popper.appendChild($arrow);

    // make content container
    const $content = document.createElement('div');
    $content.setAttribute('class', 'tooltip-content');
    $popper.appendChild($content);

    return $popper;
  }

  _events(type = EVENTS.ADD) {
    const evtType = type === EVENTS.ADD ? 'addEventListener' : 'removeEventListener';
    if (!Array.isArray(this.options.triggers)) {
      console.error('trigger should be an array', this.options.triggers);
      return;
    }

    const lis = (...params) => this._$el[evtType](...params);

    if (includes(this.options.triggers, 'manual')) {
      lis('click', this._onToggle.bind(this), false);
    } else {
      // For the strange iOS/safari behaviour, we remove any 'hover' and replace it by a 'click' event
      if (this.options.fixIosSafari && Tooltip.isIosSafari() && includes(this.options.triggers, 'hover')) {
        const pos = this.options.triggers.indexOf('hover');
        const click = includes(this.options.triggers, 'click');
        this._options.triggers[pos] = click !== -1 ? 'click' : null;
      }

      this.options.triggers.foreach((evt) => {
        switch (evt) {
          case 'click':
            lis('click', this._onToggle.bind(this), false);
            document[evtType]('click', this._onDeactivate.bind(this), false);
            break;
          case 'hover':
            lis('mouseenter', this._onActivate.bind(this), false);
            lis('mouseleave', this._onDeactivate.bind(this), false);
            break;
          case 'focus':
            lis('focus', this._onActivate.bind(this), false);
            lis('blur', this._onDeactivate.bind(this), true);
            break;
        }
      });

      if (includes(this.options.triggers, 'hover') || includes(this.options.triggers, 'focus')) {
        this._$tpl[evtType]('mouseenter', this._onMouseOverTooltip.bind(this), false);
        this._$tpl[evtType]('mouseleave', this._onMouseOutTooltip.bind(this), false);
      }
    }
  }

  _setEvents() {
    this._events();
  }

  _cleanEvents() {
    this._events(EVENTS.REMOVE);
  }

  _onActivate() {
    this.show();
  }

  _onDeactivate() {
    this.hide();
  }

  _onToggle(e) {
    e.stopPropagation();
    e.preventDefault();
    this.toggle();
  }

  _onMouseOverTooltip() {
    this.toggle(true, false);
  }

  _onMouseOutTooltip() {
    this.toggle(false);
  }

  content(content) {
    const wrapper = this.tooltip.popper.querySelector('.tooltip-content');
    if (typeof content === 'string') {
      this.tooltip.options.title = content;
      wrapper.textContent = content;
    } else if (isElement(content)) {
      if (content !== wrapper.children[0]) {
        wrapper.innerHTML = '';
        wrapper.appendChild(content);
      }
    } else {
      console.error('unsupported content type', content);
    }
  }

  set class(val) {
    if (typeof val === 'string') {
      const classList = this._$tpl.classList.value.replace(this.options.class, val);
      this._options.class = classList;
      this._$tpl.setAttribute('class', classList);
    }
  }

  static filterOptions(options) {
    const opt = { ...options };

    opt.modifiers = {};
    let head = null;
    let tail = null;
    if (opt.placement.indexOf('-') > -1) {
      [head, tail] = opt.placement.split('-');
      opt.placement = (includes(PLACEMENT, head) && includes(SUB_PLACEMENT, tail)
        ? opt.placement : Tooltip._defaults.placement);
    } else {
      opt.placement = includes(PLACEMENT, opt.placement) ? opt.placement : Tooltip._defaults.placement;
    }

    opt.modifiers.offset = {
      fn: Tooltip._setOffset,
    };

    return opt;
  }

  static _setOffset(data) {
    const target = data;
    let { offset } = target.instance.options;

    if (window.isNaN(offset) || offset < 0) {
      offset = Tooltip._defaults.offset;
    }

    if (target.placement.indexOf('top') !== -1) {
      target.offsets.popper.top -= offset;
    } else if (target.placement.indexOf('right') !== -1) {
      target.offsets.popper.left += offset;
    } else if (target.placement.indexOf('bottom') !== -1) {
      target.offsets.popper.top += offset;
    } else if (target.placement.indexOf('left') !== -1) {
      target.offsets.popper.left -= offset;
    }

    return target;
  }

  static isIosSafari() {
    return (
      includes(navigator.userAgent.toLowerCase(), 'mobile')
            && includes(navigator.userAgent.toLowerCase(), 'safari')
            && (navigator.platform.toLowerCase() === 'iphone' || navigator.platform.toLowerCase() === 'ipad')
    );
  }

  static defaults(data) {
    // if (data.placement) {
    //     data.originalPlacement = data.placement;
    // }
    Tooltip._defaults = { ...Tooltip._defaults, ...data };
  }

  set disabled(val) {
    if (typeof val === 'boolean') {
      this._disabled = val;
    }
  }

  show() {
    this.toggle(true);
  }

  hide() {
    this.toggle(false);
  }

  toggle(visible, autoHide = true) {
    let { delay } = this._options;

    if (this._disabled === true) {
      visible = false;
      delay = 0;
      return;
    }

    if (typeof visible !== 'boolean') {
      visible = !this._visible;
    }

    if (visible === true) {
      delay = 0;
    }

    clearTimeout(this._clearDelay);

    if (autoHide === true) {
      this._clearDelay = setTimeout(() => {
        this._visible = visible;
        if (this._visible === true) {
          this._$tpl.classList.remove('vue-tooltip-hidden');
          this._$tpl.classList.add('vue-tooltip-visible');
        } else {
          this._$tpl.classList.remove('vue-tooltip-visible');
          this._$tpl.classList.add('vue-tooltip-hidden');
        }

        this._$tt.update();
      }, delay);
    }
  }
}

Tooltip._defaults = { ...DEFAULT_OPTIONS };

function randomId() {
  return `${Date.now()}-${Math.round(Math.random() * 100000000)}`;
}

/**
 * Check if the variable is an html element
 * @param {*} value
 * @return Boolean
 */
function isElement(value) {
  return value instanceof window.Element;
}
