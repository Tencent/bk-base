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

import { generateId } from '@/common/js/util';
import { validateRules } from '@/common/js/validate';
import customRender from '@/components/customrender/bkDataCustomRender';
import popContainer from '@/components/popContainer';
import { Component, Emit, Prop, PropSync, Ref, Vue, Watch } from 'vue-property-decorator';
import { CreateElement, VNode } from 'vue/types/umd';
import {
  getFieldContraintConfigs,
  getFieldTypeConfig,
  getMasterTableInfo,
  updateMasterTableInfo,
} from '../../Api/index';
import { IUpdateEvent, TabItem } from '../../Controller/DataModelTabManage';
import { CDataModelManage, MasterTableInfo, MstTbField } from '../../Controller/MasterTable';
import { IDataModelManage, IMasterTableInfo } from '../../Interface/index';
import { DragTable, DragTableColumn, DragTableRow } from '../DragTable/index';
import ExtendRows from './ChildNodes/ExtendRows';
import { FieldProcessingLogic, LoadRTTable, ReferDimensionTable } from './ChildNodes/index';
import { DataModelManage, IStepsManage } from './IStepsManage';
import { ConditionSelector } from '../Common/index';
import { debounce } from '@/common/js/util.js';

interface IColumn {
  /** 列显示 */
  label: string;

  /** 对应字段名称 */
  props: string;

  /** 是否必填 */
  require: boolean;

  /** 列宽 */
  width: string;

  /** 输入框宽度 */
  inputWidth: number;

  /** 渲染函数 */
  renderFn: () => {};

  /** 校验配置 */
  validate?: any[];

  /** 输入框placeholder */
  placeholder?: string;
}

const __time__ = {
  name: '__time__',
  uid: generateId('__time__'),
};

Vue.component('ConditionSelector', ConditionSelector);
@Component({
  components: {
    DragTableColumn,
    DragTable,
    DragTableRow,
    FieldProcessingLogic,
    LoadRTTable,
    ReferDimensionTable,
    ExtendRows,
    customRender,
    popContainer,
  },
})
export default class MainTableDesign extends DataModelManage.IStepsManage {
  get activeModelTabItem() {
    return this.DataModelTabManage.getActiveItem()[0];
  }

  get tableList() {
    return this.masterTableInfo.fields
      .filter(item => !item.isExtendedField)
      .map(field => Object.assign({}, field, {
        isDrag: field.fieldName !== __time__.name,
        extends: this.masterTableInfo.fields.filter(f => f.joinFieldUId === field.uid),
      })
      );
  }

  get tableData() {
    return {
      columns: this.columns,
      list: this.tableList,
    };
  }

  set tableData(val: any) {
    Object.assign(this.columns, val.columns);
  }

  get widthNum() {
    return this.offsetWidth / this.tableData.columns.length + 'px';
  }

  /** 是否为维度表 */
  get isDimension() {
    // 'dimension' : 'fact'
    return /^dimension/.test(this.activeModelTabItem.modelType);
  }

  /** 字段类型 */
  get fieldCategoryList() {
    return [
      { id: 'measure', name: '度量', disabled: this.isDimension },
      { id: 'dimension', name: '维度', disabled: false },
    ];
  }
  /** 数据类型 */
  get fieldTypeList() {
    return this.mFieldTypeList;
  }

  set fieldTypeList(val) {
    this.$set(this, 'mFieldTypeList', val);
  }

  get columns(): IColumn[] {
    return [
      {
        label: '字段名',
        props: 'fieldName',
        placeholder: this.$t('由英文字母_下划线和数字组成_且字母开头'),
        require: true,
        width: '200px',
        renderFn: this.renderFieldNameColumn,
        validate: [
          {
            require: true,
          },
          {
            regExp: /^[a-zA-Z][a-zA-Z0-9_]*$/,
            error: this.$t('格式不正确_内容由字母_数字和下划线组成_且以字母开头'),
          },
          {
            customValidateFun: this.validateFieldNameIsInner,
            error: this.$t('该名称为系统内置，请修改名称'),
          },
          {
            customValidateFun: this.validateFieldNameRepeated,
            error: this.$t('字段名重复'),
          },
        ],
      },
      {
        label: '字段中文名',
        props: 'fieldAlias',
        require: true,
        renderFn: () => this.renderFieldNameColumn.call(this, ...arguments, false),
        validate: [{ require: true }],
      },
      {
        label: '数据类型',
        props: 'fieldType',
        require: true,
        renderFn: () => this.renderSelectColumn.call(this, ...arguments, this.fieldTypeList),
        validate: [{ require: true }],
      },
      {
        label: '字段角色',
        props: 'fieldCategory',
        require: true,
        renderFn: () => this.renderSelectColumn.call(this, ...arguments, this.fieldCategoryList),
        validate: [{ require: true }],
      },
      {
        label: '字段描述',
        props: 'description',
        require: false,
        width: '270px',
        renderFn: this.renderDescriptionColumn,
      },
      {
        label: '值约束',
        props: 'fieldConstraintContent',
        require: false,
        width: '400px',
        renderFn: this.renderValueConstraintColumn,
      },
      {
        label: '字段加工逻辑',
        props: 'fieldCleanContent',
        require: false,
        renderFn: this.renderClearColumn,
      },
      {
        label: '主键',
        props: 'isPrimaryKey',
        require: false,
        width: '100px',
        renderFn: this.renderPrimaryKeyCheckbox,
      },
      {
        label: '操作',
        props: 'operating',
        require: false,
        width: '100px',
        renderFn: this.renderOptionColumn,
      },
    ];
  }

  get fieldsWithoutTimeField() {
    return this.masterTableInfo.fields.filter(f => f.fieldName !== __time__.name);
  }

  get canAssociationDimension() {
    return !this.fieldsWithoutTimeField.filter(item => item.fieldName).length;
  }

  /** 字段约束配置列表 */
  public fieldConstraintConfig: IDataModelManage.IFieldContraintConfig[] = [];

  /** 表单验证 */
  public validateResult = {
    isSuccess: true,
  };

  /** 时间字段 */
  public timeField = new CDataModelManage.MstTbField({
    fieldName: __time__.name,
    fieldAlias: '时间字段',
    fieldType: 'timestamp',
    fieldCategory: 'dimension',
    description: '平台内置时间字段，数据入库后将转换为可查询字段，比如 dtEventTime、dtEventTimeStamp、localtime',
    uid: __time__.uid,
  });

  /** 主表数据 */
  public masterTableInfo: IDataModelManage.IMasterTableInfo = new CDataModelManage.MasterTableInfo({
    modelId: 0,
    fields: [new CDataModelManage.MstTbField({})],
    modelRelation: [],
  });

  public isLocalLoading = false;

  public offsetWidth = 0;

  public observer = null;

  /** 加载结果表的结构侧栏弹出 */
  public isLoadRtTableSliderShow = false;

  /** 关联维度表数据模型Z弹出 */
  public isReferDimensionTableShow = false;

  /** 关联维度表数据模型Z 新建/编辑状态 */
  public isReferDimensionTableEdit = false;

  /** 字段加工逻辑侧栏信息 */
  public fieldProcessingLogicSlider: object = {
    isShow: false,
    title: '',
    editable: true,
    field: new CDataModelManage.MstTbField({}),
  };

  public relatedField: IDataModelManage.IMasterTableField = {};

  /** 当前操作行 */
  public activeFieldItem = {
    index: -1,
    columnIndex: -1,
    item: {},
  };

  public validatorMap = {
    number: {
      validator: function (val) {
        return !isNaN(Number(val));
      },
      message: window.$t('请输入数字'),
      trigger: 'blur',
    },
    regex: {
      validator: function (val) {
        return /^\/.+\/[gimsuy]*$/.test(val);
      },
      message: window.$t('请输入正确的正则'),
      trigger: 'blur',
    },
    required: {
      required: true,
      message: window.$t('不能为空'),
      trigger: 'blur',
    },
  };

  private mFieldTypeList = [];

  /** 内置字段 */
  public innerFieldList = [
    'timestamp',
    'offset',
    'bkdata_par_offset',
    'dtEventTime',
    'dtEventTimeStamp',
    'localTime',
    'thedate',
    'rowtime',
  ];

  /**
   * 渲染字段名称列
   * @param h
   * @param column
   * @param row
   * @param index
   * @param isFirstColumn 是否是首列
   */
  public renderFieldNameColumn(
    h: CreateElement,
    column: IColumn,
    row: IDataModelManage.IMasterTableField,
    rowIndex: number,
    columnIndex: number,
    isFirstColumn = true
  ) {
    const fieldName = this.getFieldName(row.fieldName);
    let isValidateFailed = this.validateResult[fieldName]
            && this.validateResult[fieldName][column.props]
            && this.validateResult[fieldName][column.props].visible;

    /** 当校验结果为失败时，重新校正校验结果，同一个字段会存在重复和不为空两种校验 */
    if (isValidateFailed) {
      isValidateFailed = !validateRules(column.validate, row[column.props], {});
    }

    if (this.isSystemTimeField(row) || row.isExtendedField) {
      const showValue = row[column.props] === __time__.name ? '--' : row[column.props];
      return this.renderText(h, showValue, {
        class: ['row-column-container text', column.props === 'fieldName' ? 'offset-left13' : ''],
        style: { color: '#63656e' },
        directives: [
          {
            name: 'bk-tooltips',
            value: {
              content:
                                column.props === 'fieldAlias' && row.fieldName === __time__.name && this.isDimension
                                  ? this.$t(
                                    '时间字段，当用户在选择该字段后，在模型被实例化时，由系统按计算'
                                        + '类型自动添加相应的时间字段，否则没有'
                                  )
                                  : showValue,
              interactive: false,
            },
          },
        ],
      });
    }

    // 限制修改fieldName
    const self = this;
    if (column.props === 'fieldName' && row.editable === false) {
      return h(
        'span',
        {
          class: ['row-column-container text', column.props === 'fieldName' ? 'offset-left13' : ''],
        },
        [
          row.isJoinField && isFirstColumn
            ? h(
              'i',
              {
                class: 'bk-icon icon-left-join column-icon join-field-icon',
              },
              ''
            )
            : '',
          self.renderText(h, row[column.props], {
            style: { color: '#63656e', display: 'block' },
            directives: [
              {
                name: 'bk-tooltips',
                value: {
                  content: `
                                    <p>字段名：${row[column.props]}</p>
                                    <p>该字段无法修改，原因如下：</p>
                                    ${self.getFieldEditableInfo(row.editableDeletableInfo, 'edit')}
                                `,
                  interactive: false,
                },
              },
            ],
          }),
        ]
      );
    }

    return this.renderValidateColumn(
      h,
      column,
      row,
      rowIndex,
      columnIndex,
      [
        row.isJoinField && isFirstColumn
          ? h(
            'i',
            {
              class: 'bk-icon icon-left-join column-icon join-field-icon',
            },
            ''
          )
          : '',
        this.renderInputColumn(h, column, row, rowIndex),
      ],
      [column.props === 'fieldName' ? 'offset-left13' : '']
    );
  }

  /**
   * 渲染操作列
   * @param h
   * @param column
   * @param row
   * @param rowIndex
   * @param columnIndex
   */
  public renderOptionColumn(
    h: CreateElement,
    column: IColumn,
    row: IDataModelManage.IMasterTableField,
    rowIndex: number,
    columnIndex: number
  ) {
    const showIcons = !this.isSystemTimeField(row);

    /** 是否渲染时间字段操作项 */
    const showTimeOpt = this.isSystemTimeField(row) && this.isDimension;

    /** 维度表：选择是否添加时间字段 */
    const timeFieldOpt = showTimeOpt
      ? h('bkdata-switcher', {
        props: {
          theme: 'primary',
          value: !row._disabled,
        },
        class: 'opt-icon',
        on: {
          change: this.handleTimeFieldActive,
        },
      })
      : '';

    const joinFieldOpt = showIcons && row.isJoinField && !row.isExtendedField
      ? (() => {
        // 根据草稿态关联维度表状态设置按钮
        const extendFields = row['extends'] || [];
        const hasError = !extendFields.every(field => field.fieldType && field.fieldCategory);
        const relation = this.masterTableInfo.modelRelation
          .find(relation => relation.fieldName === row.fieldName) || {};
        const { relatedModelActiveStatus, relatedModelName } = relation;
        const isRelatedDimensionDelete = relatedModelActiveStatus === 'disabled';
        const tipsContent = !isRelatedDimensionDelete
          ? this.$t(`当前被关联的维度表（${relatedModelName}）已更新，请重新配置`)
          : this.$t(`当前被关联的维度表（${relatedModelName}）已被删除，请重新配置`);
        return h('i', {
          class: 'bk-icon icon-icon-set-fill opt-icon'
                        + (hasError || isRelatedDimensionDelete ? ' error' : ''),
          directives: [
            {
              name: 'bk-tooltips',
              value: {
                content: hasError || isRelatedDimensionDelete
                  ? tipsContent : this.$t('关联维度表数据模型设置'),
                interactive: false,
              },
            },
          ],
          on: {
            click: () => this.handleEditDimensionField(row, rowIndex),
          },
        });
      })()
      : '';

    const plusOpt = row.isExtendedField || !showIcons
      ? ''
      : h('i', {
        class: 'bk-icon icon-plus-9 opt-icon',
        on: {
          click: () => this.handleAppendRow(row, rowIndex),
        },
      });

    let minusOpt = '';
    const self = this;
    if (row.isExtendedField || this.fieldsWithoutTimeField.length <= 1 || !showIcons) {
      minusOpt = '';
    } else if (row.deletable === false) {
      minusOpt = h('i', {
        class: 'bk-icon icon-minus-6 opt-icon',
        style: { cursor: 'not-allowed' },
        directives: [
          {
            name: 'bk-tooltips',
            value: {
              content: self.getFieldEditableInfo(row.editableDeletableInfo, 'delete'),
              interactive: false,
            },
          },
        ],
      });
    } else {
      minusOpt = h('i', {
        class: 'bk-icon icon-minus-6 opt-icon',
        on: {
          click: () => this.handleRemoveRow(row, rowIndex),
        },
      });
    }

    return h('span', { class: 'icon-group' }, [plusOpt, minusOpt, joinFieldOpt, timeFieldOpt]);
  }

  /**
   * 渲染TagInput
   * 暂时不用，实现字段推荐时在启用
   * @param h
   * @param column
   * @param row
   * @param index
   */
  public renderTagInputColumn(
    h: CreateElement,
    column: IColumn,
    row: IDataModelManage.IMasterTableField,
    index: number
  ) {
    return h('bkdata-tag-input', {
      domProps: {
        list: [],
        'allow-next-focus': false,
        'allow-create': true,
        'allow-auto-match': true,
      },
    });
  }

  /**
   * 渲染Input输入框
   * @param h
   * @param column
   * @param row
   * @param rowIndex
   * @param columnIndex
   */
  public renderInputColumn(
    h: CreateElement,
    column: IColumn,
    row: IDataModelManage.IMasterTableField,
    rowIndex: number,
    columnIndex: number
  ) {
    const self = this;
    return h('bkdata-input', {
      props: {
        value: row[column.props],
        placeholder: column.placeholder,
      },
      directives: [
        {
          name: 'bk-tooltips',
          value: {
            content: row[column.props],
            interactive: false,
            disabled: !row[column.props],
          },
        },
      ],
      on: {
        blur(value) {
          const targetIndex = self.masterTableInfo.fields.findIndex(
            item => (row.id && item.id === row.id) || (row.uid && item.uid === row.uid)
          );
          if (targetIndex >= 0) {
            const targetField = self.masterTableInfo.fields[targetIndex];
            targetField[column.props] = value;
            self.masterTableInfo.fields.splice(targetIndex, 1, targetField);
            self.handleFormDataChanged();
            if (column.validate) {
              self.validateFormItem(targetField.fieldName, column.props, value, column.validate, true);
            }
          }
        },
      },
    });
  }

  /**
   * 渲染带有校验的列
   * @param h
   * @param column
   * @param row
   * @param rowIndex
   * @param columnIndex
   * @param insertColumns
   */
  public renderValidateColumn(
    h: CreateElement,
    column: IColumn,
    row: IDataModelManage.IMasterTableField,
    rowIndex: number,
    columnIndex: number,
    insertColumns: any[],
    cls: string[] = []
  ) {
    const self = this;
    const fieldName = this.getFieldName(row.fieldName);
    let isValidateFailed = this.validateResult[fieldName]
            && this.validateResult[fieldName][column.props]
            && this.validateResult[fieldName][column.props].visible;

    /** 当校验结果为失败时，重新校正校验结果，同一个字段会存在重复和不为空两种校验 */
    if (isValidateFailed) {
      isValidateFailed = !validateRules(column.validate, row[column.props], {});
    }
    return h('span', { class: ['row-column-container', ...cls, isValidateFailed && 'is-validate-fail'] }, [
      ...insertColumns,
      isValidateFailed
        ? h('i', {
          class: 'validate-msg-icon icon icon-exclamation-circle-shape',
          directives: [
            {
              name: 'bk-tooltips',
              value: this.validateResult[fieldName][column.props].content,
            },
          ],
        })
        : '',
    ]);
  }

  /**
   * 渲染下拉列表组件
   * @param h
   * @param column
   * @param row
   * @param rowIndex
   * @param columnIndex
   * @param listOptions
   */
  public renderSelectColumn(
    h: CreateElement,
    column: IColumn,
    row: IDataModelManage.IMasterTableField,
    rowIndex: number,
    columnIndex: number,
    listOptions: any[]
  ) {
    const iconMap = {
      measure: 'icon-measure-line',
      dimension: 'icon-dimens',
    };
    const self = this;
    if (this.isSystemTimeField(row)) {
      return this.renderText(h, '--', { class: 'row-column-container text' });
    }
    if (row.isExtendedField) {
      const option = listOptions.find(item => item.id === row[column.props]);
      let text = '';
      if (option) {
        text = option.name || '--';
        if (column.props === 'fieldCategory' && text !== '--') {
          text = [
            h(
              'i',
              {
                class: iconMap[option.id],
                style: {
                  fontSize: '14px',
                  color: '#C4C6CC',
                  marginRight: '5px',
                },
              },
              ''
            ),
            h('span', {}, text),
          ];
        }
      }
      return this.renderText(h, text, {
        class: 'row-column-container text',
        style: { color: '#63656e' },
      });
    }
    if (column.props === 'fieldType' && row.editableFieldType === false) {
      const option = listOptions.find(item => item.id === row[column.props]);
      const text = option?.name || '--';
      return this.renderText(h, text, {
        class: 'row-column-container text',
        style: { color: '#63656e' },
        directives: [
          {
            name: 'bk-tooltips',
            value: {
              content: self.getFieldEditableInfo(row.editableDeletableInfo, 'edit'),
              interactive: false,
            },
          },
        ],
      });
    }
    return this.renderValidateColumn(h, column, row, rowIndex, columnIndex, [
      column.props === 'fieldCategory' && h('i', { class: `${iconMap[row[column.props]]} category-icon` }, ''),
      h(
        'bkdata-select',
        {
          class: column.props === 'fieldCategory' && 'category-selector',
          props: {
            value: row[column.props],
            clearable: false,
          },
          on: {
            change(value) {
              if (rowIndex >= 0) {
                const targetField = self.masterTableInfo.fields[rowIndex];
                targetField[column.props] = value;
                self.masterTableInfo.fields.splice(rowIndex, 1, targetField);
                self.handleFormDataChanged();
                if (column.validate) {
                  self.validateFormItem(targetField.fieldName,
                    column.props, value, column.validate, true);
                }
              }
            },
          },
        },
        listOptions.map(item => h(
          'bkdata-option',
          {
            props: {
              key: item.id,
              id: item.id,
              name: item.name,
              disabled: item.disabled,
            },
          },
          [
            h(
              'i',
              {
                class: iconMap[item.id],
                style: {
                  fontSize: '14px',
                  color: '#C4C6CC',
                  marginRight: '5px',
                },
              },
              ''
            ),
            h('span', {}, item.name),
          ]
        )
        )
      ),
    ]);
  }

  /**
   * 字段描述
   * @param h
   * @param column
   * @param row
   * @param rowIndex
   * @param columnIndex
   */
  public renderDescriptionColumn(
    h: CreateElement,
    column: IColumn,
    row: IDataModelManage.IMasterTableField,
    rowIndex: number,
    columnIndex: number
  ) {
    return this.isSystemTimeField(row)
      ? this.renderText(h, row[column.props], {
        class: 'row-column-container text',
        directives: [
          {
            name: 'bk-tooltips',
            value: {
              content: row[column.props] || '',
              interactive: false,
            },
          },
        ],
      })
      : this.renderInputColumn(h, column, row, rowIndex, columnIndex);
  }

  /**
   * 渲染纯文本
   * @param h
   * @param column
   * @param row
   */
  public renderText(h: CreateElement, text: string, options: any = {}) {
    return h(
      'span',
      {
        ...options,
      },
      text
    );
  }

  /**
   * 字段加工逻辑列渲染
   * @param h
   * @param column
   * @param row
   */
  public renderClearColumn(
    h: CreateElement,
    column: IColumn,
    row: IDataModelManage.IMasterTableField,
    rowIndex: number,
    columnIndex: number
  ) {
    const fieldCleanContent = (row[column.props] || {}).cleanOption || this.$t('未设置');
    const self = this;
    if (this.isSystemTimeField(row) || row.isExtendedField) {
      return this.renderText(h, '--', {
        class: 'row-column-container text',
        style: { color: '#63656e' },
      });
    }

    return this.renderText(h, fieldCleanContent, {
      on: {
        click: () => {
          self.activeFieldItem.columnIndex = columnIndex;
          self.handleFieldProcessingLogic(row, row.isExtendedField);
          self.sendUserActionData({ name: '点击【字段加工逻辑】' });
        },
      },
      style: {
        color: (row[column.props] || {}).cleanOption ? '#3a84ff' : '#c4c6cc',
      },
      class: ['field-clean-type pointer-cell', { selected: self.activeFieldItem.columnIndex === columnIndex }],
    });
  }

  /**
   * 渲染值约束列
   * @param h
   * @param column
   * @param row
   * @param options
   */
  public renderValueConstraintColumn(
    h: CreateElement,
    column: IColumn,
    row: IDataModelManage.IMasterTableField,
    rowIndex: number,
    columnIndex: number
  ) {
    const self = this;
    if (this.isSystemTimeField(row) || (!row[column.props] && row.isExtendedField)) {
      return this.renderText(h, '--', { class: 'row-column-container text', style: { color: '#63656e' } });
    }
    return h('condition-selector', {
      props: {
        constraintList: self.fieldConstraintConfig
          .filter(cfg => cfg.allowFieldType.some(t => t === row.fieldType)),
        constraintContent: row[column.props],
        readonly: row.isExtendedField,
        fieldDisplayName: `${row.fieldName} (${row.fieldAlias})`,
      },
      on: {
        change(content: object | null) {
          const targetIndex = self.masterTableInfo.fields.findIndex(item => row.uid && item.uid === row.uid);
          if (targetIndex >= 0) {
            const field = self.masterTableInfo.fields[targetIndex];
            field[column.props] = content;
            self.masterTableInfo.fields.splice(targetIndex, 1, field);
          }
          self.sendUserActionData({ name: '确定【值约束】' });
          self.handleFormDataChanged();
        },
        click() {
          !row.isExtendedField && self.sendUserActionData({ name: '点击【值约束】' });
        },
      },
    });
  }

  /**
   * 渲染主键checkbox
   * @param h
   * @param column
   * @param row
   * @param rowIndex
   * @param columnIndex
   */
  public renderPrimaryKeyCheckbox(
    h: CreateElement,
    column: IColumn,
    row: IDataModelManage.IMasterTableField,
    rowIndex: number,
    columnIndex: number
  ) {
    const self = this;
    const config = {
      class: 'row-column-container text',
      style: { color: '#63656e' },
    };
    if (this.isSystemTimeField(row) || row.isExtendedField) {
      return this.renderText(h, '--', config);
    }
    if (row.fieldCategory !== 'dimension') {
      const target = self.masterTableInfo.fields.find(
        item => (row.id && item.id === row.id) || (row.uid && item.uid === row.uid)
      );
      if (target) {
        target[column.props] = false;
      }
      return h('bkdata-checkbox', {
        props: {
          value: false,
          disabled: true,
        },
        style: {
          marginRight: 0,
          marginLeft: '6px',
        },
        directives: [
          {
            name: 'bk-tooltips',
            value: {
              content: '只能将字段角色为维度的字段设置为主键',
              interactive: false,
            },
          },
        ],
      });
    }
    return h('bkdata-checkbox', {
      props: {
        value: row[column.props],
      },
      style: {
        marginRight: 0,
        marginLeft: '6px',
      },
      on: {
        change(value) {
          let hasChange = false;
          self.masterTableInfo.fields.forEach((item, index) => {
            if (item.isPrimaryKey) {
              item[column.props] = false;
              hasChange = true;
            }
            if ((row.id && item.id === row.id) || (row.uid && item.uid === row.uid)) {
              item[column.props] = value;
              hasChange = true;
            }
          });
          hasChange && self.handleFormDataChanged();
        },
      },
    });
  }

  public isSystemTimeField(row: any = {}) {
    return row.uid === __time__.uid && !row.isFromRt;
  }

  /** 表单数据被修改之后,设置上一步、下一步需要二次弹窗确认 */
  public handleFormDataChanged() {
    this.syncPreNextBtnManage.isPreNextBtnConfirm = true;
  }

  /**
   * 计算列宽度
   * @param col
   */
  public getColumnsWidth(col) {
    return col.width || this.widthNum;
  }

  /** 拉取主表信息 */
  public loadMasterTableInfo() {
    this.isLocalLoading = true;
    Promise.all([
      getFieldTypeConfig(),
      getMasterTableInfo(this.modelId, true, undefined, ['editable', 'deletable']),
      getFieldContraintConfigs(),
    ])
      .then(res => {
        const fieldType = res[0];
        const tableInfo = res[1];
        const fieldContraintConfigs = res[2];

        fieldType.setDataFn(data => {
          this.fieldTypeList = (data || [])
            .map(d => Object.assign({}, { name: d.field_type, id: d.field_type }));
        });

        /** 字段约束配置列表 */
        fieldContraintConfigs.setData(this, 'fieldConstraintConfig');

        this.$nextTick(() => {
          tableInfo.setDataFn(data => {
            // 补齐 editableDeletableInfo fields、isExtendedFieldDeletableEditable
            for (const field of data.fields) {
              if (!field.editableDeletableInfo.hasOwnProperty('fields')) {
                field.editableDeletableInfo.fields = [];
              }
              if (!field.editableDeletableInfo.hasOwnProperty('isExtendedFieldDeletableEditable')) {
                field.editableDeletableInfo.isExtendedFieldDeletableEditable = true;
              }
            }
            this.masterTableInfo = data;
            // 关闭time字段后，下次进入主表补齐time字段
            const timeFieldIndex = this.masterTableInfo.fields
              .findIndex(field => field.fieldName === __time__.name);
            if (this.masterTableInfo.fields.length && timeFieldIndex === -1) {
              this.masterTableInfo.fields.push(Object.assign({}, this.timeField, { _disabled: true }));
            } else if (timeFieldIndex >= 0) {
              this.masterTableInfo.fields.splice(
                timeFieldIndex,
                1,
                Object.assign({}, this.masterTableInfo.fields[timeFieldIndex], { _disabled: false })
              );
            }
            this.masterTableInfo.fields.sort((a, b) => {
              if (this.isSystemTimeField(a)) {
                return 1;
              } else if (this.isSystemTimeField(b)) {
                return -1;
              }
              return a.isExtendedField - b.isExtendedField;
            });
            // 前端维护uid
            const nameMap = {};
            for (const field of this.masterTableInfo.fields) {
              nameMap[field.fieldName] = field;
              if (field.fieldName === __time__.name) {
                !field.uid && (field.uid = __time__.uid);
                continue;
              }
              Object.assign(field, { uid: generateId('__mst_rt_field_id_') });
            }
            // 添加关联唯一标识joinFieldUId
            this.masterTableInfo.fields.forEach(field => {
              if (field.joinFieldName && nameMap[field.joinFieldName]) {
                Object.assign(field, { joinFieldUId: nameMap[field.joinFieldName].uid });
              }
            });
            // 关联关系添加唯一标识uid
            this.masterTableInfo.modelRelation.forEach(relation => {
              if (relation.fieldName && nameMap[relation.fieldName]) {
                Object.assign(relation, { joinFieldUId: nameMap[relation.fieldName].uid });
              }
            });
          });
          this.initDefaultField();
        });
      })
      ['finally'](() => {
        this.isLocalLoading = false;
      });
  }

  /** 新增主表时默认初始化一行数据 */
  public initDefaultField() {
    const defaultField = new CDataModelManage.MstTbField({});
    this.isDimension && Object.assign(defaultField, { fieldCategory: 'dimension' });
    const timeField = Object.assign({}, this.timeField, { _disabled: false });
    if (!this.masterTableInfo) {
      this.masterTableInfo = new CDataModelManage.MasterTableInfo({
        modelId: this.modelId,
        fields: [defaultField, timeField],
        modelRelation: [],
      });
    }

    if (!this.masterTableInfo.fields || !this.masterTableInfo.fields.length) {
      this.masterTableInfo.fields = [defaultField, timeField];
    }
  }

  /**
   * 处理字段为空的情况
   * @param fieldName
   */
  public getFieldName(fieldName: string) {
    if (/^\s*$/.test(fieldName)) {
      fieldName = '__empty__';
    }

    return fieldName;
  }

  /**
   * 校验字段是否符合提交规范
   * @param fieldName 字段名称
   * @param value 字段值
   * @param regs 校验规则
   * @param resetFieldName 是否重置本字段的校验
   */
  public validateFormItem(fieldName, columnName, value, regs, resetFieldName = false): boolean {
    fieldName = this.getFieldName(fieldName);
    if (!this.validateResult[fieldName] || resetFieldName) {
      this.validateResult[fieldName] = {};
    }

    if (!this.validateResult[fieldName][columnName]) {
      this.validateResult[fieldName][columnName] = {};
    }

    const result = validateRules(regs, value, this.validateResult[fieldName][columnName]);
    if (this.validateResult.isSuccess) {
      this.validateResult.isSuccess = result;
    }

    return result;
  }

  /**
   * 校验全部数据是否合法
   */
  public validateFormItems() {
    this.validateResult = { isSuccess: true };
    this.masterTableInfo.fields.forEach(field => {
      this.columns.forEach(column => {
        // 内置字段不需要校验
        if (column.validate && field.uid !== __time__.uid) {
          this.validateFormItem(field.fieldName, column.props, field[column.props], column.validate);
        }
      });
    });

    return Promise.resolve(this.validateResult.isSuccess);
  }

  /**
   * 校验主表字段是否合法（不包括扩展字段）
   */
  public validateMainFormItems() {
    this.validateResult = { isSuccess: true };
    this.masterTableInfo.fields
      .filter(field => !field.isExtendedField && field.uid !== __time__.uid)
      .forEach(field => {
        this.columns.forEach(column => {
          if (column.validate) {
            this.validateFormItem(field.fieldName, column.props, field[column.props], column.validate);
          }
        });
      });

    return Promise.resolve(this.validateResult.isSuccess);
  }

  /**
   * 校验字段名称是否重复
   * @param fieldName
   */
  public validateFieldNameRepeated(fieldName: string) {
    return this.masterTableInfo.fields.filter(item => item.fieldName === fieldName).length <= 1;
  }

  /**
   * 校验字段名称是否为系统内置
   * @param fieldName
   */
  public validateFieldNameIsInner(fieldName: string) {
    return !this.innerFieldList.includes(fieldName.toLocaleLowerCase());
  }

  /**
   * 激活时间字段
   * @param isEnabled
   */
  public handleTimeFieldActive(isEnabled: boolean) {
    const timefield = this.masterTableInfo.fields.find(item => item.fieldName === this.timeField.fieldName);
    if (timefield) {
      Object.assign(timefield, { _disabled: !isEnabled });
    }
  }

  /**
   * 点击一行操作
   * @param row
   * @param rowIndex
   */
  public handleRowClick(row: IDataModelManage.IMasterTableField, rowIndex: number, columnIndex?: number) {
    this.activeFieldItem.index = rowIndex;
    this.activeFieldItem.item = row;
    this.activeFieldItem.columnIndex = columnIndex === undefined ? -1 : columnIndex;
  }

  /**
   * 行数据拖拽结束
   * @param args 参数： args[0]：List, args[1]: CustomEvent
   */
  public handleDragEnd(args: any[]) {
    const targetList = args[0] || [];
    const evts = args[1];
    const { oldIndex, newIndex } = evts;

    /**
     * 表格渲染的数据tableList是重新计算的数据结构
     * 和原始数据 masterTableInfo.fields 是有出入的
     * 此处行的拖拽需要计算原始数据行位置
     **/
    const computedOldItem = this.tableList[oldIndex];
    const computedNewItem = targetList[newIndex];

    /** 计算拖拽的行在原始数据中的位置 */
    const sourceOldIndex = this.masterTableInfo.fields.findIndex(
      field => field.fieldName === computedOldItem.fieldName
    );
    const sourceOldItem = this.masterTableInfo.fields[sourceOldIndex];

    /** 根据计算结果重置原始数据 */
    this.masterTableInfo.fields.splice(sourceOldIndex, 1);
    this.masterTableInfo.fields.splice(newIndex, 0, sourceOldItem);

    /** 更新缓存：当前激活行 */
    if (this.activeFieldItem.index >= 0) {
      const activeIndex = newIndex;
      this.handleRowClick(this.tableList[activeIndex], activeIndex);
      this.handleFormDataChanged();
    }
  }

  /**
   * 移除字段设置
   * @param field 字段设置
   * @param index 字段Index
   */
  public handleRemoveRow(field: IDataModelManage.IMasterTableField, rowIndex: number) {
    if (this.masterTableInfo.fields[rowIndex] && this.fieldsWithoutTimeField.length > 1) {
      const deleteFields = this.masterTableInfo.fields.splice(rowIndex, 1);
      if (deleteFields[0] && deleteFields[0].isJoinField) {
        const fieldIndex = this.masterTableInfo.modelRelation.findIndex(
          item => item.joinFieldUId === deleteFields[0].uid
        );
        fieldIndex > -1 && this.masterTableInfo.modelRelation.splice(fieldIndex, 1);
        // 删除关联字段
        for (let i = this.masterTableInfo.fields.length - 1; i >= 0; i--) {
          if (this.masterTableInfo.fields[i].joinFieldUId === deleteFields[0].uid) {
            this.masterTableInfo.fields.splice(i, 1);
          }
        }
      }
      // 更新编辑/删除限制
      for (const fieldItem of this.masterTableInfo.fields) {
        const editableDeletableInfo = fieldItem.editableDeletableInfo || {};
        const limitFields = editableDeletableInfo.fields || [];
        const limitFieldMap = {};
        for (const item of limitFields) {
          limitFieldMap[item.fieldName] = item.fieldName;
        }
        if (limitFieldMap[deleteFields[0].fieldName]) {
          const newLimitFields = [...limitFields
            .filter(item => item.fieldName !== deleteFields[0].fieldName)];
          fieldItem.editableDeletableInfo = Object.assign({}, editableDeletableInfo, {
            fields: newLimitFields,
            isUsedByOtherFields: !!newLimitFields.length,
          });
        }
        const { deletable, editable } = this.getFieldLimit(fieldItem.editableDeletableInfo);
        fieldItem.deletable = deletable;
        fieldItem.editable = editable;
      }
      this.handleUpdateExtendedFieldLimit();
      this.handleFormDataChanged();
    }
  }

  /**
   * 从当前位置追加一行
   * @param field
   * @param rowInde
   */
  public handleAppendRow(field: IDataModelManage.IMasterTableField, rowInde: number) {
    const appendRowIndex = rowInde + 1;
    const newField = new CDataModelManage.MstTbField();
    this.isDimension && Object.assign(newField, { fieldCategory: 'dimension' });
    this.masterTableInfo.fields.splice(appendRowIndex, 0, newField);
    /** 激活新增行 */
    this.handleRowClick(this.masterTableInfo.fields[appendRowIndex], appendRowIndex);
  }

  /**
   * 拉取主表数据
   */
  public handleLoadRtTable() {
    this.isLoadRtTableSliderShow = true;
    this.sendUserActionData({ name: '点击【加载结果表的结构】' });
  }

  /**
   * 点击关联维度表数据模型弹出侧栏
   */
  public async handleReferDimensionTable() {
    if (this.canAssociationDimension || !(await this.validateMainFormItems())) {
      const message = this.canAssociationDimension ? '空表不能进行关联操作' : '请先处理表内错误';
      this.$bkMessage({
        theme: 'warning',
        message: this.$t(message),
      });
      return;
    }
    this.isReferDimensionTableEdit = false;
    this.isReferDimensionTableShow = true;
    this.sendUserActionData({ name: '点击【关联维度表数据模型】' });
  }

  /**
   * 保存结果表
   */
  public handleSaveLoadTable(fieldList: any[]) {
    const appendFields = (fieldList || []).map(
      field => new CDataModelManage.MstTbField({
        fieldName: field.field_name,
        fieldAlias: field.field_alias,
        fieldType: field.field_type,
        fieldCategory: 'dimension',
        description: field.description,
        uid: generateId('__rt_field_'),
        isFromRt: true,
      })
    );

    /** 过滤重复字段，排列到相邻位置 */
    appendFields.forEach(field => {
      const index = this.masterTableInfo.fields.findIndex(f => f.fieldName === field.fieldName);
      if (index >= 0) {
        this.masterTableInfo.fields.splice(index + 1, 0, field);
        field.isAppend = true;
      }
    });

    /** 过滤系统字段Time,将所有Time字段放到最后 */
    const timeFieldList = this.masterTableInfo.fields.filter(f => f.fieldName === __time__.name);
    let timeFieldListLength = timeFieldList.length;
    while (timeFieldListLength >= 0) {
      timeFieldListLength--;
      const timeFieldIndex = this.masterTableInfo.fields.findIndex(f => f.fieldName === __time__.name);
      timeFieldIndex > -1 && this.masterTableInfo.fields.splice(timeFieldIndex, 1);
    }

    this.masterTableInfo.fields.push(...appendFields.filter(f => !f.isAppend));
    if (timeFieldList.length > 0) {
      /** 追加的Time字段会在默认的系统Time字段之后，此处需要翻转，保证系统默认的Time字段在最后位置 */
      this.masterTableInfo.fields.push(...timeFieldList.reverse());
    }
    this.validateFormItems();
    this.isLoadRtTableSliderShow = false;
    this.sendUserActionData({ name: '确定【加载结果表的结构】' });
  }

  /**
   * 编辑关联维度字段
   * @param field
   * @param index
   */
  public async handleEditDimensionField(field: IDataModelManage.IMasterTableField, index: number) {
    if (this.canAssociationDimension || !(await this.validateMainFormItems())) {
      const message = this.canAssociationDimension ? '空表不能进行关联操作' : '请先处理表内错误';
      this.$bkMessage({
        theme: 'warning',
        message: this.$t(message),
      });
      return;
    }
    this.relatedField = field;
    this.isReferDimensionTableEdit = true;
    this.isReferDimensionTableShow = true;
  }

  /**
   * 点击字段加工逻辑弹出侧栏
   */
  public async handleFieldProcessingLogic(row: IDataModelManage.IMasterTableField, isExtendedField = false) {
    if (this.canAssociationDimension || !(await this.validateMainFormItems())) {
      const message = this.canAssociationDimension ? '空表不能进行字段加工操作' : '请先处理表内错误';
      this.$bkMessage({
        theme: 'warning',
        message: this.$t(message),
      });
      return;
    }
    this.fieldProcessingLogicSlider.editable = !isExtendedField;
    this.fieldProcessingLogicSlider.field = row;
    this.fieldProcessingLogicSlider.title = `${row.fieldName} (${row.fieldAlias})`;
    this.fieldProcessingLogicSlider.isShow = true;
  }

  /**
   * 字段加工逻辑, 保存设置
   * @param info
   */
  public handleSaveFieldProcessingLogic(info: object) {
    const { field, fieldCleanContent, originFieldsDict = [] } = info;
    const curField = this.masterTableInfo.fields.find(item => item.uid === field.uid);
    if (curField) {
      Object.assign(curField, { fieldCleanContent });
    }
    this.handleUpdateFieldLimit(originFieldsDict, field);
    this.fieldProcessingLogicSlider.isShow = false;
    this.sendUserActionData({ name: '确定【字段加工逻辑】' });
    this.handleFormDataChanged();
  }

  /**
   * 更新字段编辑/删除限制
   * @param originFieldsDict
   */
  public handleUpdateFieldLimit(originFieldsDict: string[] = [], field: IDataModelManage.IMasterTableField) {
    const originFieldsDictMap = {};
    for (const name of originFieldsDict) {
      originFieldsDictMap[name] = name;
    }
    for (const fieldItem of this.masterTableInfo.fields) {
      if (fieldItem.fieldName === field.fieldName) continue;

      const editableDeletableInfo = fieldItem.editableDeletableInfo || {};
      const limitFields = editableDeletableInfo.fields || [];
      const limitFieldMap = {};
      for (const item of limitFields) {
        limitFieldMap[item.fieldName] = item.fieldName;
      }
      if (originFieldsDictMap[fieldItem.fieldName] && limitFieldMap[field.fieldName]) continue;

      if (originFieldsDictMap[fieldItem.fieldName] && !limitFieldMap[field.fieldName]) {
        limitFields.push({ fieldName: field.fieldName });
        fieldItem.editableDeletableInfo = Object.assign({}, editableDeletableInfo, {
          fields: [...limitFields],
          isUsedByOtherFields: !!limitFields.length,
        });
      } else if (limitFieldMap[field.fieldName]) {
        const newLimitFields = [...limitFields.filter(item => item.fieldName !== field.fieldName)];
        fieldItem.editableDeletableInfo = Object.assign({}, editableDeletableInfo, {
          fields: newLimitFields,
          isUsedByOtherFields: !!newLimitFields.length,
        });
      }
      const { deletable, editable } = this.getFieldLimit(fieldItem.editableDeletableInfo);
      fieldItem.deletable = deletable;
      fieldItem.editable = editable;
    }
    this.handleUpdateExtendedFieldLimit();
  }

  /**
   * 获取是否可删除/编辑
   * @param editableDeletableInfo
   */
  public getFieldLimit(editableDeletableInfo: IDataModelManage.IEditableDeletableInfo) {
    let deletable = true;
    let editable = true;
    if (
      editableDeletableInfo.isUsedByOtherFields
            || editableDeletableInfo.isUsedByCalcAtom
            || editableDeletableInfo.isAggregationField
            || editableDeletableInfo.isConditionField
            || editableDeletableInfo.isExtendedFieldDeletableEditable === false
    ) {
      deletable = false;
    } else {
      deletable = true;
    }

    if (!deletable || editableDeletableInfo.isJoinField) {
      editable = false;
    } else {
      editable = true;
    }
    return { deletable, editable };
  }

  /**
   * 更新字段 editableDeletableInfo 信息
   */
  public handleUpdateExtendedFieldLimit() {
    const mainFields = this.masterTableInfo.fields.filter(
      (field: IDataModelManage.IMasterTableField) => !field.isExtendedField
    );
    const extendFields = this.masterTableInfo.fields.filter(
      (field: IDataModelManage.IMasterTableField) => field.isExtendedField
    );
    for (const fieldItem of mainFields) {
      if (fieldItem.isJoinField) {
        const curExtends = extendFields.filter(
          (field: IDataModelManage.IMasterTableField) => field.joinFieldUId === fieldItem.uid
        );
        const isExtendedFieldDeletableEditable = !curExtends.some(
          (field: IDataModelManage.IMasterTableField) => !field.deletable
        );
        fieldItem.editableDeletableInfo.isExtendedFieldDeletableEditable = isExtendedFieldDeletableEditable;
        const { deletable, editable } = this.getFieldLimit(fieldItem.editableDeletableInfo);
        fieldItem.deletable = deletable;
        fieldItem.editable = editable;
      }
    }
  }

  /**
   * 关联维度表保存修改
   * @param info
   */
  public handleReferDimensionSave(info: any) {
    const { joinFieldName, expandFields, relation } = info.result;
    const index = this.masterTableInfo.fields.findIndex(item => item.fieldName === joinFieldName);
    if (info.isDelete && info.isEdit) {
      // 删除关联关系
      const relatedIndex = this.masterTableInfo.modelRelation.findIndex(
        item => item.joinFieldUId === relation.joinFieldUId && item.modelId === relation.modelId
      );
      if (relatedIndex >= 0) {
        const deleteRelated = this.masterTableInfo.modelRelation.splice(relatedIndex, 1)[0];
        // 删除关联字段
        for (let i = this.masterTableInfo.fields.length - 1; i > 0; i--) {
          const curField = this.masterTableInfo.fields[i];
          if (deleteRelated && curField.joinFieldName === deleteRelated.fieldName) {
            this.masterTableInfo.fields.splice(i, 1);
          }
        }
      }
      if (index >= 0) {
        const field = this.masterTableInfo.fields[index];
        field.isJoinField = false;
      }
    } else if (info.isEdit) {
      const findRelation = this.masterTableInfo.modelRelation.find(
        item => item.joinFieldUId === relation.joinFieldUId && item.modelId === relation.modelId
      );
      if (findRelation) {
        // 更新relation信息
        Object.assign(findRelation, relation, { relatedModelActiveStatus: 'active' });
        // 把旧扩展字段删除
        for (let i = this.masterTableInfo.fields.length - 1; i > 0; i--) {
          const curField = this.masterTableInfo.fields[i];
          if (curField.joinFieldName === findRelation.fieldName) {
            this.masterTableInfo.fields.splice(i, 1);
          }
        }
      }
    } else {
      if (index >= 0) {
        const field = this.masterTableInfo.fields[index];
        field.isJoinField = true;
        this.masterTableInfo.modelRelation.push(relation);
      }
    }

    // 修改 field 设置关联 icon
    const findField = this.masterTableInfo.fields[index];
    if (findField && findField.relatedFieldExist === false) {
      this.masterTableInfo.fields[index].relatedFieldExist = true;
    }

    /** 追加扩展字段到最后位置 */
    if (expandFields?.length) {
      this.masterTableInfo.fields.push(...expandFields);
    }
    this.isReferDimensionTableShow = false;
    this.handleFormDataChanged();
    this.sendUserActionData({ name: '确定【关联维度表数据模型】' });
  }

  /** 新建模型 */
  public handleCreateNewModelTable() {
    if (
      this.DataModelTabManage.insertItem(
        new TabItem({
          id: 0,
          name: 'New Model',
          displayName: this.$t('新建模型'),
          type: this.activeModelTabItem.type,
          isNew: true,
          projectId: this.activeModelTabItem.projectId,
          modelType: this.activeModelTabItem.modelType,
          icon: this.activeModelTabItem.icon,
          lastStep: 0,
        }),
        true
      )
    ) {
      // this.appendRouter({ modelId: 0 }, { project_id: this.activeModelTabItem.projectId })
      this.changeRouterWithParams('dataModelEdit',
        { project_id: this.activeModelTabItem.projectId }, { modelId: 0 });
    }
  }

  public submitForm() {
    return new Promise((resolve, reject) => {
      this.validateMainFormItems().then(validate => {
        if (validate) {
          // 将扩展字段放在对应字段后面
          const fields = this.masterTableInfo.fields.filter(field => !field._disabled);
          const submitFields: any[] = [];
          const extendFields = fields.filter(field => field.isExtendedField);
          const mainFields = fields.filter(field => !field.isExtendedField);
          for (const field of mainFields) {
            submitFields.push(field);
            if (field.isJoinField) {
              const curExtends = extendFields.filter(item => item.joinFieldUId === field.uid);
              submitFields.push(...curExtends);
            }
          }
          submitFields.forEach((field, index) => {
            field.fieldIndex = index;
          });

          updateMasterTableInfo(
            this.modelId,
            this.getServeFormData(submitFields),
            this.getServeFormData(this.masterTableInfo.modelRelation)
          ).then(res => {
            if (res.validateResult()) {
              const activeTabItem = this.DataModelTabManage.getActiveItem()[0];
              activeTabItem.lastStep = res.data.step_id;
              activeTabItem.publishStatus = res.data.publish_status;
              this.DataModelTabManage.updateTabItem(activeTabItem);
              this.DataModelTabManage.dispatchEvent('updateModel', [
                {
                  model_id: this.modelId,
                  publish_status: res.data.publish_status,
                  step_id: res.data.step_id,
                },
              ]);
              this.showMessage(this.$t('保存成功'), 'success');
              this.syncPreNextBtnManage.isPreNextBtnConfirm = false;
              resolve(true);
            } else {
              reject(res.message);
            }
          });
        } else {
          reject('');
        }
      });
    });
  }

  public getFieldEditableInfo(info: object = {}, type: string) {
    const operation = type === 'delete' ? '删除' : '修改';
    const tips = [];
    if (info.isJoinField && type === 'edit') {
      tips.push('该字段已引用维度表数据模型，无法修改');
    }
    if (info.isExtendedFieldDeletableEditable === false) {
      tips.push(`存在扩展字段被引用，无法${operation}`);
    }
    if (info.isUsedByCalcAtom) {
      const usedInfo = (info.calculationAtoms || [])
        .map(item => `${item.calculationAtomName} (${item.calculationAtomAlias})`)
        .join('、');
      tips.push(`该字段被统计口径 ${usedInfo} 引用，无法${operation}`);
    }
    if (info.isAggregationField) {
      const usedInfo = (info.aggregationFieldIndicators || [])
        .map(item => `${item.indicatorName} (${item.indicatorAlias})`)
        .join('、');
      tips.push(`该字段被指标 ${usedInfo} 作为聚合字段，无法${operation}`);
    }
    if (info.isConditionField) {
      const usedInfo = (info.conditionFieldIndicators || [])
        .map(item => `${item.indicatorName} (${item.indicatorAlias})`)
        .join('、');
      tips.push(`该字段被指标 ${usedInfo} 作为过滤字段，无法${operation}`);
    }
    if (info.isUsedByOtherFields) {
      const usedInfo = (info.fields || []).map(item => item.fieldName).join('、');
      tips.push(`该字段被 ${usedInfo} 的字段加工逻辑引用，无法${operation}`);
    }
    return tips.join('<br />');
  }

  public handleResetOffsetWidth() {
    this.offsetWidth = document.querySelector('.drag-table-body').offsetWidth - 40;
  }

  public handleResizeBody() {
    const self = this;
    this.observer = new ResizeObserver(entries => {
      debounce(self.handleResetOffsetWidth, 200)();
    });
    this.observer.observe(document.querySelector('.drag-table-body'));
  }

  public created() {
    this.initPreNextManage();
    this.loadMasterTableInfo();
  }

  public mounted() {
    this.handleResizeBody();
    this.offsetWidth = document.querySelector('.drag-table-body').offsetWidth - 40;
  }

  public beforeDestroy() {
    if (this.observer) {
      this.observer.disconnect();
      this.observer = null;
    }
  }
}
