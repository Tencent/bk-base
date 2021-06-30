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

class HttpRequst {
  constructor(axios, options) {
    this.mockJson = null;
    this.services = null;
    this.axios = axios;
    this.options = options;
    // eslint-disable-next-line no-underscore-dangle
    this.__initServices(options);
    // eslint-disable-next-line no-underscore-dangle
    this.__initMockJson(options);
  }

  request(service, options = {}) {
    // eslint-disable-next-line no-underscore-dangle
    const _options = Object.assign({}, this.options, options);

    // eslint-disable-next-line no-underscore-dangle
    this.__initMockJson(_options);
    if (_options.mock) {
      // eslint-disable-next-line no-underscore-dangle
      const mockData = this.__getMockResult(service, options);
      if (mockData !== null) {
        // eslint-disable-next-line no-underscore-dangle
        const _service = this.__getHttpService(service, _options);
        return (
          (_service && _service.callback && _service.callback(mockData, _options))
          || Promise.resolve(mockData)
        );
      }
    }

    // eslint-disable-next-line no-underscore-dangle
    const _service = this.__getHttpService(service, _options);
    if (!_service) {
      console.error('can not find service:', service);
    }
    // eslint-disable-next-line no-underscore-dangle
    return this.__resolveSericeRequset(_service, _options);
  }

  __resolveSericeRequset(service, options) {
    if (typeof service.url === 'string') {
      // eslint-disable-next-line no-underscore-dangle
      let _service = this.__formatService(service, options);
      _service = Object.assign(
        {},
        {
          method: 'get',
        },
        _service,
      );
      if (service.callback && typeof service.callback === 'function') {
        return this.axios(_service.url, _service.method, options.params, options.query, options.ext)
          .then(res => service.callback(res, options));
      }
      return this.axios(_service.url, _service.method, options.params, options.query, options.ext);
    }
    if (Array.isArray(service.url)) {
      const requests = [];
      service.url.forEach((url) => {
        if (typeof url === 'string') {
          // eslint-disable-next-line no-underscore-dangle
          const _url = this.__formatUrl(url, options);
          requests.push(this.axios(_url, service.method || 'get', options.params, options.query, options.ext));
        } else {
          // eslint-disable-next-line no-underscore-dangle
          let _service = this.__formatService(url, options);
          _service = Object.assign(
            {},
            {
              method: 'get',
            },
            _service,
          );
          requests.push(this.axios(_service.url, _service.method, options.params, options.query, options.ext));
        }
      });
      if (service.callback && typeof service.callback === 'function') {
        return Promise.all(requests).then(res => service.callback(res, options));
      }
      return Promise.all(requests);
    }
    return Promise.reject(new Error('Url Resolve Error'));
  }

  __getHttpService(service, options) {
    // eslint-disable-next-line no-underscore-dangle
    const splitor = this.__getSericeSplitor(service);
    // eslint-disable-next-line no-underscore-dangle
    let _service = splitor[1] ? this.services[splitor[0]][splitor[1]] : this.services[splitor[0]];
    if (typeof _service === 'function') {
      _service = _service(service, options);
    }
    return _service;
  }

  __getSericeSplitor(service) {
    return service.split('/').filter(f => f);
  }

  __getMockResult(service, options) {
    if (this.mockJson) {
      // eslint-disable-next-line no-underscore-dangle
      const splitor = this.__getSericeSplitor(service);
      const getSplitorValue = () => (this.mockJson[splitor[0]] ? this.mockJson[splitor[0]][splitor[1]] : null);
      let mock = splitor[1] ? getSplitorValue() : this.mockJson[splitor[0]];
      if (typeof mock === 'function') {
        mock = mock(service, options);
      }
      const defaultMock = {
        data: mock,
        result: true,
      };
      return options.manualSchema ? mock : defaultMock;
    }
    return null;
  }

  __formatService(service, option) {
    // eslint-disable-next-line no-underscore-dangle
    const _service = JSON.parse(JSON.stringify(service));
    const { url } = _service;
    // insert-mock-code-start

    // insert-mock-code-end
    // eslint-disable-next-line no-underscore-dangle
    _service.url = this.__formatUrl(url, option);
    return _service;
  }

  __formatUrl(url, option) {
    if (option && option.params) {
      const matchs = url.match(/:(_|\d|_|[a-z])*/gi);
      if (matchs && matchs.length) {
        matchs.forEach((match) => {
          const key = match.replace(/^:/, '');
          const param = option.params[key];
          // eslint-disable-next-line no-param-reassign
          url = url.replace(match, param);
        });
      }
    }
    return url;
  }

  __initMockJson(_options) {
    if (_options.mock && _options.mockList) {
      if (!this.mockJson) {
        this.mockJson = _options.mockList;
      }
    }
  }

  __initServices(_options) {
    if (_options.serviceList) {
      if (!this.services) {
        this.services = _options.serviceList;
      }
    }
  }

  __initRequestConfig(_options) {
    const defaultOptions = {
      axios: _options.axios.basic,
      interceptors: _options.axios.interceptors,
      methodExtends: _options.axios.methodExtends,
      customAttr: _options.axios.customAttr,
    };
    return _options.axios ? defaultOptions : {};
  }
}

export default HttpRequst;
