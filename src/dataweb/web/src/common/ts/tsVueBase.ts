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

import { CancelToken } from '@/common/js/ajax';
import { getServeFormData, getWebData } from '@/common/js/Utils';
import { AsyncLoader } from '@/pages/AsyncLoader/index';
import { Component, Emit, Model, Prop, PropSync, Vue, Watch } from 'vue-property-decorator';

@Component({})
export class TsUtils extends Vue {
  /** API请求取消缓存Token */
  public requestCancelToken = {};

  /** 异步加载组件 */
  public AsyncLoader = AsyncLoader;

  /** 前端定义接口格式化为服务器端格式 */
  public getServeFormData = getServeFormData;

  /**
   * 格式化服务器端接口返回数据 前端数据命名规范：驼峰式 服务器端命名规范：下划线
   */
  public getWebData = getWebData;

  /** 追加Url Query string */
  public appendQuery(query: any) {
    this.$router.push({
      query: Object.assign({}, this.$route.query, query || {}),
    });
  }

  /**
   * 路由不变，只改变相关参数
   * @param params
   * @param query
   * @param replace 是否替换现有参数，默认是 false, 当设置为true时，会使用 query 和 params全部替换现有路由参数
   */
  public appendRouter(params, query = {}, replace = false) {
    if (replace) {
      this.$router.push({
        query,
        params,
      });
    } else {
      this.$router.push({
        query: Object.assign({}, this.$route.query, query || {}),
        params: Object.assign({}, this.$route.params, params || {}),
      });
    }
  }

  /**
   * 保留现有参数，改变路由
   * @param name 新的路由名称
   */
  public changeRouterWithParams(name, query = {}, params = {}) {
    this.$router.push({
      name,
      query: Object.assign({}, this.$route.query, query || {}),
      params: Object.assign({}, this.$route.params, params || {}),
    });
  }

  /** 阻止事件继续向下传递 */
  public handlePauseEvent(e: MouseEvent) {
    if (e.stopPropagation) {
      e.stopPropagation();
    }
    if (e.preventDefault) {
      e.preventDefault();
    }
    e.cancelBubble = true;
    e.returnValue = false;
    return false;
  }

  /**
   * 获取API请求取消Token
   * @param key 唯一标识
   * @return { cancelToken }
   */
  public getCancelToken(key: string) {
    const self = this;
    return {
      cancelToken: new CancelToken(function executor(c: any) {
        self.requestCancelToken[key] = c;
      }),
    };
  }

  /**
   * 取消请求
   * @param key 请求的唯一key，若不传则取消所有当前实例保存的请求
   */
  public handleCancelRequest(key: string = null) {
    if (key !== null && Object.prototype.hasOwnProperty.call(this.requestCancelToken, key)) {
      this.requestCancelToken[key]();
    }
    if (key === null) {
      Object.keys(this.requestCancelToken).forEach(thisKey => {
        this.requestCancelToken[thisKey] && this.requestCancelToken[thisKey]();
      });
    }
  }

  /**
   * 消息提示框
   * @param msg 消息内容
   * @param theme 主题 - 默认 warning,可选项： primary|warning|success|error
   * @param options 其他配置项
   */
  public showMessage(msg: string, theme = 'warning', options = {}) {
    const targetTheme = /^(primary|warning|success|error)$/.test(theme) ? theme : 'primary';
    this.$bkMessage(
      Object.assign(
        {
          message: msg,
          delay: 1000,
          theme: targetTheme,
          offsetY: 80,
          ellipsisLine: 5,
          limit: 1,
        },
        options
      )
    );
  }
}

@Component({})
export class TsRouteParams extends TsUtils {
  /** 路由名称 */
  get routeName() {
    return this.$route.name;
  }

  /** 路由路径参数 */
  get routeParams() {
    return this.$route.params || {};
  }

  /** 路由查询参数 */
  get routeQuery() {
    return this.$route.query || {};
  }
}
