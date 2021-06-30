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
import Bus from '@/common/js/bus.js';
import extend from '@/extends/index';

/**
 * 导航配置
 */
class HeaderNaviConfiguration {
  constructor(serverConf, router) {
    this.$modules = serverConf;
    this.$route = router || {};
  }

  /**
   * 过滤当前激活的导航配置项
   * @returns
   */
  getActiveHeadNaviList() {
    return this.getHeadNaviList()
      .filter(navi => navi.active)
      .map((navi) => {
        // eslint-disable-next-line no-param-reassign
        navi.children = (navi.children || []).filter(child => child.active);
        return navi;
      });
  }

  /**
   * 获取顶部导航配置
   * @returns
   */
  getHeadNaviList() {
    const self = this;
    const config = self.$modules || {
      isActive: () => false,
    };


    return [
      {
        order: 2,
        active: config.isActive('dataflow'),
        displayName: '数据开发',
        route: {
          matchActive: ['dataflow_ide'],
          to: {
            name: 'dataflow_ide',
          },
        },
      },
      {
        order: 3,
        active: true,
        displayName: '数据集成',
        route: {
          matchActive: ['createDataid', 'updateDataid', 'data_detail', 'data', 'data-index'],
          to: {
            path: '/data-access/',
          },
        },
      },
      {
        order: 4,
        active: config.isActive('data_explore'),
        displayName: '数据探索',
        route: {
          matchActive: ['DataExploreNotebook', 'DataExploreQuery'],
          to: {
            name: 'DataExploreQuery',
          },
        },
        children: [
          {
            order: 0,
            active: true,
            displayName: '数据查询',
            labelName: '旧版',
            route: {
              matchActive: ['data_result_query', 'DataExplore'],
              to: {
                path: '/data-access/query',
              },
            },
          },
        ],
      },
      {
        order: 5,
        active: config.isActive('data_view'),
        displayName: '数据视图',
        route: {
          matchActive: ['Statement', 'DataDashboard'],
          to: {
            name: 'DataDashboard',
          },
        },
        children: [
          {
            order: 1,
            active: config.isActive('superset'),
            displayName: 'Superset',
            labelName: 'BI',
            onClick() {
              self.linkToDataDashbaord.call(this, { name: 'DataDashboard' });
            },
            route: {
              matchActive: ['DataDashboard'],
            },
          },
          {
            order: 2,
            active: config.isActive('grafana'),
            displayName: 'Grafana',
            onClick() {
              self.linkToDataDashbaord.call(this, { name: 'Grafana' });
            },
            route: {
              matchActive: ['Grafana'],
            },
          },
          {
            order: 3,
            active: config.isActive('data_graph'),
            displayName: '数据报表',
            labelName: '旧版',
            onClick() {
              self.linkToDataDashbaord.call(this, { name: 'Statement' });
            },
            route: {
              matchActive: ['Statement'],
            },
          },
        ],
      },
      {
        order: 1,
        active: config.isActive('datamart'),
        displayName: '数据集市',
        route: {
          matchActive: [
            'DataDetail',
            'DataDictionary',
            'DataMap',
            'DataInventory',
            'DataStandard',
            'DataStandardDetail',
            'DataSearchResult',
            'dataModel',
          ],
          to: {
            name: 'DataDictionary',
          },
        },
        children: [
          {
            order: 1,
            active: config.isActive('datamart'),
            displayName: '数据字典',
            onClick() {
              self.linkToMarket.call(this, { name: 'DataDictionary' });
            },
            route: {
              matchActive: ['DataDictionary'],
            },
          },
          {
            order: 2,
            active: config.isActive('datamart'),
            displayName: '数据地图',
            route: {
              matchActive: ['DataMap'],
            },
            onClick() {
              self.linkToMarket.call(this, { name: 'DataMap' });
            },
          },
          {
            order: 3,
            active: config.isActive('data_inventory'),
            displayName: '数据盘点',
            route: {
              matchActive: ['DataInventory'],
            },
            onClick() {
              self.linkToMarket.call(this, {
                name: 'DataInventory',
              });
            },
          },
          {
            order: 4,
            active: config.isActive('data_model'),
            displayName: '数据模型',
            route: {
              matchActive: ['dataModel'],
            },
            onClick() {
              self.linkToMarket.call(this, {
                name: 'dataModel',
              });
            },
          },
        ],
      },
      {
        order: 7,
        active: true,
        displayName: '权限管理',
        route: {
          matchActive: ['MyPermissions', 'Records', 'Todos', 'TokenEdit', 'TokenNew', 'TokenManagement'],
          to: {
            name: 'MyPermissions',
            query: {
              tab: 'access',
            },
          },
        },
      },
    ];
  }

  /**
   * getUserCenterConfig
   * @returns
   */
  getUserCenterConfig() {
    const self = this;
    /** 获取扩展配置 */
    const extendNavis = extend.callJsFragmentFn('getUsercenterNaviConfig', this, [self]) || [];
    return [
      {
        tabName: '我的项目',
        order: 1,
        active: self.$modules.isActive('dataflow'),
        tab: 'project',
        onClick(e) {
          self.linkToTab.call(this, e, 'project');
        },
      },
      {
        tabName: '我的任务',
        order: 2,
        active: self.$modules.isActive('dataflow'),
        tab: 'dataflow',
        onClick(e) {
          self.linkToTab.call(this, e, 'dataflow');
        },
      },
      {
        tabName: '我的告警',
        order: 4,
        active: self.$modules.isActive('dmonitor_center'),
        tab: 'alert',
        onClick(e) {
          self.linkToTab.call(this, e, 'alert');
        },
      },
      {
        tabName: '我的函数',
        order: 6,
        active: self.$modules.isActive('udf'),
        tab: 'function',
        onClick(e) {
          self.linkToTab.call(this, e, 'function');
        },
      },
      {
        tabName: '资源管理',
        order: 7,
        active: self.$modules.isActive('resource_manage'),
        tab: 'resourceGroup',
        onClick(e) {
          self.linkToTab.call(this, e, 'resourceGroup');
        },
      },
      {
        tabName: '权限管理',
        order: 9,
        active: true,
        tab: 'access',
        onClick(e) {
          self.linkToAccess.call(this, e);
        },
      },
      ...extendNavis,
    ];
  }

  /**
   * 跳转权限管理
   * @param {*} e
   */
  linkToAccess(e) {
    this.$router.push({
      path: '/auth-center/permissions',
      query: {
        objType: 'project',
        tab: 'accsee',
      },
    });
    e.stopPropagation();
  }
  /**
   * 打开指定Tab
   * @param {*} e
   * @param {*} tab
   */
  linkToTab(e, tab) {
    this.$router.push({
      path: '/user-center',
      query: {
        tab,
      },
    });

    e.stopPropagation();
  }
  /**
   * 抛出事件：打开数据集市
   * @param {*} route
   * @returns
   */
  linkToMarket(route) {
    if (route.name === this.$route.name) {
      return Bus.$emit('dataMarketReflshPage');
    }
    this.$router.push(route);
  }
  /**
   * 抛出事件：打开数据视图
   * @param {*} route
   * @returns
   */
  linkToDataDashbaord(route) {
    if (route.name === this.$route.name) {
      return Bus.$emit('dataDashboardRefresh');
    }
    this.$router.push(route);
  }
}

export default HeaderNaviConfiguration;
