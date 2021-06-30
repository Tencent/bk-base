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

import { showMsg } from '@/common/js/util';
import Monaco from '@/components/monaco';
import { BKHttpResponse } from '@/common/js/BKHttpResponse';
import { bkCascade } from 'bk-magic-vue';
import { Component, Inject, Prop, Vue, Watch } from 'vue-property-decorator';
import { getFieldTypeConfig, getMasterTableInfo, sqlFuncs } from '../../../Api/index';
import { IDataModelManage } from '../../../Interface/index';
import { IFieldTypeListData, IFieldTypeListRes, ISqlFuncs, ISqlFuncsData } from '../../../Interface/indexDesign';

@Component({
  components: { Monaco, bkCascade },
})
export default class IndexStatisticalCaliber extends Vue {
  @Inject('activeTabItem')
  public activeTabItem!: function;

  @Prop() public isLoading: boolean;

  @Prop({ default: false }) public isRestrictedEdit!: boolean;

  @Prop({ default: () => ({}) }) public calculationAtomDetailData: Object;

  // 模式（新建、编辑）
  @Prop({ default: 'create' }) public mode: string;

  @Prop({ default: () => [] }) public aggregationLogicList: string[];

  public isFieldTypeLoading = false;

  public isSqlFieldLoading = false;

  public monacoSetting = {
    tools: {
      guidUrl: this.$store.getters['docs/getPaths'].realtimeSqlRule,
      toolList: {
        font_size: true,
        full_screen: true,
        event_fullscreen_default: true,
        editor_fold: false,
        format_sql: false,
      },
      title: this.$t('SQL编辑器'),
    },
    options: {
      fontSize: '14px',
      readOnly: this.isRestrictedEdit,
    },
  };

  public rules = {
    'calculation_content.content.calculation_function': [
      {
        required: true,
        message: '必填项',
        trigger: 'blur',
      },
    ],
    'calculation_content.content.calculation_field': [
      {
        required: true,
        message: '必填项',
        trigger: 'blur',
      },
    ],
    field_type: [
      {
        required: true,
        message: '必填项',
        trigger: 'blur',
      },
    ],
    calculation_atom_name: [
      {
        required: true,
        message: '必填项',
        trigger: 'blur',
      },
      {
        max: 32,
        message: '不能多于32个字符',
        trigger: 'blur',
      },
      {
        regex: /^[a-zA-Z][a-zA-Z0-9_]*$/,
        message: '只能是英文字母、下划线和数字组成，且字母开头',
        trigger: 'blur',
      },
    ],
    calculation_atom_alias: [
      {
        required: true,
        message: '必填项',
        trigger: 'blur',
      },
      {
        max: 50,
        message: '不能多于50个字符',
        trigger: 'blur',
      },
    ],
    description: [
      {
        max: 100,
        message: '不能多于100个字符',
        trigger: 'blur',
      },
    ],
  };

  public fieldTypeList: IFieldTypeListData[] = [];

  public isSelectorModel = true;

  public code = '';

  public params = {
    // 字段类型
    field_type: '',
    description: '',
    // 英文名
    calculation_atom_name: '',
    // 中文名
    calculation_atom_alias: '',
    // 聚合逻辑
    calculation_content: {
      option: 'TABLE',
      content: {
        // 字段
        calculation_field: '',
        // 方法
        calculation_function: '',
        // sql
        calculation_formula: '',
      },
    },
  };

  /** 主表数据 */
  public masterTableInfo: IDataModelManage.IMasterTableInfo;

  public fieldNameList: object[] = [];

  // sql函数列表
  public sqlFuncsList: ISqlFuncsData[] = [];

  public isSqlFuncLoading = false;

  public fieldNameGroupList = [
    {
      id: 1,
      enName: 'measure',
      name: $t('度量'),
      children: [],
    },
    {
      id: 2,
      enName: 'dimension',
      name: $t('维度'),
      children: [],
    },
  ];

  public isSameAggregationLogic = false;

  // 编辑状态下的聚合逻辑的初始值
  public initCode = '';

  // 注释sql
  public commentCode =
  '-- 只需要字段的计算逻辑，不需要 SELECT、FROM 关键字\n-- 示例：人均道具销售额\n-- sum(price)/count(distinct(uin))\n\n';

  get isGetSqlList() {
    return this.sqlFuncsList.length && this.params.calculation_content.content.calculation_function;
  }

  get activeTabName() {
    return this.activeTabItem ? `${this.activeTabItem().name}（${this.activeTabItem().displayName}）` : '';
  }

  // 每个聚合函数对应的需要过滤掉的已存在的字段
  get sqlFilterFieldMap() {
    const globalPatten = /^(\w+)\((\w|\s)+\)$/;
    const spacialPatten = /^count\(distinct/;
    const result = {};
    this.aggregationLogicList.forEach((item: string) => {
      if (globalPatten.test(item)) {
        let [sqlFunc, sqlField] = item.substring(0, item.length - 1).split('(');
        // count(distinct ...) 特殊情况，需要判断
        // count(distinct price) 示例
        if (spacialPatten.test(item)) {
          sqlFunc = 'count_distinct';
          sqlField = item.substring(0, item.length - 1).split(' ')[1];
        }
        if (!result[sqlFunc]) {
          result[sqlFunc] = [sqlField];
        } else {
          result[sqlFunc].push(sqlField);
        }
      }
    });
    return result;
  }

  // 经过去除空格的sqlfield列表
  get existSqlFieldList() {
    return this.aggregationLogicList.map(item => this.dropSpace(item));
  }

  @Watch('isGetSqlList')
  public onIsGetSqlListChanged(val: boolean) {
    if (this.mode === 'create') {
      return;
    }
    if (val) {
      this.getMasterTableInfo(
        this.sqlFuncsList.find(
          item => item.functionName === this.params.calculation_content.content.calculation_function
        )?.allowFieldType
      );
    }
  }

  @Watch('calculationAtomDetailData', { immediate: true, deep: true })
  public onCalculationAtomDetailDataChanged(val) {
    this.params = Object.assign({}, this.params, val);
    this.isSelectorModel = this.params?.calculation_content?.option === 'TABLE';
    this.$nextTick(() => {
      if (!this.isSelectorModel) {
        this.code = this.params?.calculation_content?.content?.calculation_formula;
        if (!this.initCode) {
          this.initCode = this.code;
        }
      }
    });
  }

  public mounted() {
    this.getFieldTypeConfig();
    this.sqlFuncs();
  }

  // 去掉字符串中的空格
  public dropSpace(str: string) {
    return str.replace(/\s+/g, '');
  }

  public sqlFuncChange(sqlName: string, sqlData: ISqlFuncsData) {
    this.params.calculation_content.content.calculation_field = '';
    this.getMasterTableInfo(sqlData.allowFieldType);
  }

  public selectToggle(status: boolean) {
    if (status && !this.params.calculation_content.content.calculation_function) {
      showMsg('请先选择聚合函数！', 'warning', { delay: 5000 });
    }
  }

  public sqlFuncs() {
    this.isSqlFuncLoading = true;
    sqlFuncs()
      .then(res => {
        if (res.validateResult()) {
          const instance = new BKHttpResponse<ISqlFuncs>(res);
          instance.setData(this, 'sqlFuncsList');
        }
      })
      .finally(() => {
        this.isSqlFuncLoading = false;
      });
  }

  public getFieldTypeConfig() {
    this.isFieldTypeLoading = true;
    getFieldTypeConfig('string')
      .then(res => {
        if (res.validateResult()) {
          const instance = new BKHttpResponse<IFieldTypeListRes>(res);
          instance.setData(this, 'fieldTypeList');
        }
      })
      .finally(() => {
        this.isFieldTypeLoading = false;
      });
  }

  public getMasterTableInfo(allowFieldType: string[]) {
    this.isSqlFieldLoading = true;
    this.fieldNameGroupList = [
      {
        id: 1,
        enName: 'measure',
        name: $t('度量'),
        children: [],
      },
      {
        id: 2,
        enName: 'dimension',
        name: $t('维度'),
        children: [],
      },
    ];
    getMasterTableInfo(this.$route.params.modelId, false, allowFieldType)
      .then(res => {
        if (res.validateResult()) {
          const instance = new BKHttpResponse<IDataModelManage.IMasterTableInfo>(res);
          instance.setData(this, 'masterTableInfo');
          this.masterTableInfo.fields.forEach(item => {
            const calcFunc = this.params.calculation_content.content.calculation_function;
            this.fieldNameGroupList
              .find(child => child.enName === item.fieldCategory)
              ?.children.push({
                id: item.fieldName,
                name: `${item.fieldName}（${item.fieldAlias}）`,
                disabled: this.sqlFilterFieldMap[calcFunc]
                  ? this.sqlFilterFieldMap[calcFunc].includes(
                    item.fieldName
                  )
                  : false,
              });
          });
        }
      })
      .finally(() => {
        this.isSqlFieldLoading = false;
      });
  }

  public handleSubmit() {
    if (!this.isSelectorModel) {
      if (this.isSameAggregationLogic) {
        showMsg('聚合逻辑在当前模型下已存在，请重新选择！', 'warning', { delay: 5000 });
        return;
      }
    }
    this.$refs.dimensionForm.validate().then(res => {
      if (!this.isSelectorModel) {
        // sql模式
        this.params.calculation_content = {
          option: 'SQL',
          content: {
            calculation_formula: this.code,
          },
        };
      }
      this.$emit('submit', this.params);
    });
  }

  public handleChangeCode(content: string) {
    this.code = content;
    if (this.code === this.initCode) {
      return;
    } // 编辑状态时,对sql组件初次赋值避免报错

    this.isSameAggregationLogic = this.existSqlFieldList.includes(this.dropSpace(this.code));
    if (this.isSameAggregationLogic) {
      showMsg('聚合逻辑在当前模型下已存在，请重新选择！', 'warning', { delay: 5000 });
    }
  }

  public handleChangeModel() {
    this.isSelectorModel = false;
    if (
      !this.params.calculation_content.content.calculation_function
            && !this.params.calculation_content.content.calculation_field
    ) {
      this.code = this.commentCode;
      return;
    }
    if (this.params.calculation_content.content.calculation_function === 'count_distinct') {
      // distinct特殊情况  形如'count(distinct {})'
      this.code = this.commentCode
                + `count(distinct \`${this.params.calculation_content.content.calculation_field}\`)`;
    } else {
      this.code = this.commentCode
                + `${this.params.calculation_content.content.calculation_function}
                (\`${this.params.calculation_content.content.calculation_field}\`)`;
    }
  }
}
