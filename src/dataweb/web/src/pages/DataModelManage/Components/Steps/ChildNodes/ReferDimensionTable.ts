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
import popContainer from '@/components/popContainer';
import { Component, Emit, Prop, PropSync, Ref, Vue, Watch } from 'vue-property-decorator';
import { VNode } from 'vue/types/umd';
import { getDimensionModelsCanBeRelated, getMasterTableInfo } from '../../../Api/index';
import { CDataModelManage, MasterTableInfo, MstTbField } from '../../../Controller/MasterTable';
import { IDataModelManage, IMasterTableInfo } from '../../../Interface/index';
import { ModelTab } from '../../Common/index';

/**
 * 获取主表字段编辑 tips
 * @param info
 * @param type
 */
const getFieldEditableInfo = (info: object = {}, type: string) => {
  const operation = type === 'delete' ? '删除' : '修改';
  const tips = [];
  if (info.isJoinField && type === 'edit') {
    tips.push('该字段已引用维度表数据模型，无法修改');
  }
  if (info.isExtendedFieldDeletableEditable === false) {
    tips.push(`对应扩展字段不能${operation}`);
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
};
/** 关联维度表数据模型 */
@Component({
  components: { ModelTab, popContainer },
})
export default class ReferDimensionTable extends Vue {
  @Prop({ default: () => ({}) }) public readonly activeModel!: object;
  @Prop({ default: () => ({}) }) public readonly parentMstTableInfo!: object;
  @Prop({ default: () => ({}) }) public readonly relatedField!: object;
  @Prop() public readonly isEdit!: boolean;

  @Ref() public readonly mainContent!: HTMLDivElement;
  @Ref() public readonly dimensionFieldNameItem!: VNode;
  @Ref() public readonly clearConfirmPop!: VNode;
  @Ref() public readonly dimensionModelNode!: VNode;

  get mstTableDisplayName() {
    return `${this.activeModel.name} (${this.activeModel.displayName})`;
  }

  /**
     * 已经被关联的维度表集合
     */
  get hadRelatedModel() {
    const relatedMap = {};
    const relation = this.parentMstTableInfo.modelRelation || [];
    for (const item of relation) {
      if (item.fieldName === this.relatedField.fieldName) {
        continue;
      }
      relatedMap[item.relatedModelId] = true;
    }
    return relatedMap;
  }

  /**
     * 获取维度模型列表
     */
  get filterDimensionModelList() {
    const isCurrent = this.activeSelectedTab === 'current';
    const projectId = Number(this.activeModel.projectId);
    if (isCurrent) {
      return this.dimensionModelList.filter(model => model.project_id === projectId);
    }
    return this.dimensionModelList.filter(model => model.project_id !== projectId);
  }

  /**
     * 过滤与父级主表一样的模型
     */
  get displayDimensionModelList() {
    return this.filterDimensionModelList.filter(
      model => model.model_id !== this.parentMstTableInfo.modelId && !this.hadRelatedModel[model.model_id]
    );
  }

  /**
     * 根据 searchkey 过滤出列表
     */
  get searchDimensionModelList() {
    return this.displayDimensionModelList.filter(
      model => model.model_alias.includes(this.searchKey) || model.model_name.includes(this.searchKey)
    );
  }

  get curDimensionModelName() {
    const model = this.displayDimensionModelList.find(model => model.model_id === this.formData.dimensionModelId);
    return model ? `${model.model_name} (${model.model_alias})` : '';
  }

  /**
     * 获取父级主表的fields
     * 过滤扩展字段 item.isExtendedField
     * 过滤空字段 item.fieldAlias && item.fieldName
     * 过滤已经被关联字段 item.isJoinField
     */
  get mstFields() {
    const fields = this.parentMstTableInfo.fields || [];
    if (this.isEdit) {
      return fields.filter(
        item => !item.isExtendedField && item.fieldAlias && item.fieldName && item.fieldName !== '__time__'
      );
    }
    return fields.filter(
      item => !item.isExtendedField
                && !item.isJoinField
                && item.fieldAlias
                && item.fieldName
                && item.fieldName !== '__time__'
    );
  }

  /**
     * 获取父级主表的扩展fields
     */
  get mstExpandFields() {
    return this.parentMstTableInfo.fields
      .filter(item => item.isExtendedField)
      .map(item => Object.assign({}, item));
  }

  /**
     * 获取关联主表的字段
     */
  get joinField() {
    return this.mstFields.find(item => item.fieldName === this.formData.masterFieldName);
  }

  /**
     * 过滤主键字段
     */
  get nonPrimaryKeyFields() {
    const fields = this.masterTableInfo.fields
      .filter(item => !item.isPrimaryKey)
      .map(item => Object.assign({}, item, {
        sourceFieldName: item.fieldName,
        displayName: `${item.fieldName} (${item.fieldAlias})`,
      })
      );
    if (this.isEdit) {
      const extendsMap = {};
      this.relatedField.extends.forEach(item => {
        extendsMap[item.sourceFieldName] = item;
      });
      return fields.map(item => {
        const exist = extendsMap[item.sourceFieldName];
        if (exist && exist.sourceFieldExist !== false) {
          return Object.assign(item, exist);
        }
        return item;
      });
    }
    return fields;
  }

  get nonPrimaryKeyFieldNameMap() {
    const map = {};
    for (const field of this.nonPrimaryKeyFields) {
      map[field.sourceFieldName] = true;
    }
    return map;
  }

  /**
     * 获取展示的维度字段
     */
  get displayDimensionFields() {
    return this.masterTableInfo.fields.filter(item => item.isPrimaryKey);
  }

  /**
     * 获取展示的扩展字段
     */
  get displayExpandFields() {
    return this.formData.expandFields.map((field: IDataModelManage.IMasterTableField, index: number) => {
      return {
        ...field,
        rules: {
          name: field.id
            ? [
              {
                required: true,
                message: this.$t('必填项'),
                trigger: 'blur',
              },
              {
                regex: /^[a-zA-Z][a-zA-Z0-9_]*$/,
                message: this.$t('格式不正确_内容由字母_数字和下划线组成_且以字母开头'),
                trigger: 'blur',
              },
              {
                message: this.$t('字段英文名重复'),
                validator: (value: string) => this.nameRepeatedValidator(value, index),
                trigger: 'blur',
              },
              {
                message: this.$t('字段英文名与主表中字段名重复'),
                validator: this.nameRepeatedWithMstValidator,
                trigger: 'blur',
              },
              {
                message: this.$t('该名称为系统内置，请修改名称'),
                validator: this.nameIsInnerValidator,
                trigger: 'blur',
              },
            ]
            : [],
          alias: field.id
            ? [
              {
                required: true,
                message: this.$t('必填项'),
                trigger: 'blur',
              },
              {
                message: this.$t('字段中文名重复'),
                validator: (value: string) => this.aliasRepeatedValidator(value, index),
                trigger: 'blur',
              },
            ]
            : [],
        },
      };
    });
  }

  get isDisabled() {
    return this.formData.expandFields.some(field => field.deletable === false || field.editable === false);
  }

  get disabledTips() {
    const fields = this.formData.expandFields
      .filter(field => field.deletable === false || field.editable === false);
    return {
      disabled: !this.isDisabled,
      content: `当前关联表的扩展字段（${fields.map(field => field.fieldName).join('、')}）有被依赖，无法修改`,
    };
  }

  /**
     * 配置维度模型 id 规则
     */
  get dimensionModelIdRules() {
    const rules = [
      {
        required: true,
        message: this.$t('请选择'),
        trigger: 'blur',
      },
    ];
    if (this.isEdit) {
      return rules.concat([
        {
          message: this.$t(`当前被关联的维度表（${this.relatedModelName}）已被删除，请重新配置`),
          validator: (value: string) => this.handleCheckdimensionModel(value),
          trigger: 'blur',
        },
      ]);
    }
    return rules;
  }

  /**
     * 配置关联字段名规则
     */
  get relatedFieldNamedRules() {
    const rules = [
      {
        required: true,
        message: this.$t('请选择'),
        trigger: 'blur',
      },
    ];
    if (this.isEdit) {
      return rules.concat([
        {
          message: this.$t(`当前维度表关联字段（${this.relatedFieldName}）发生变更，请重新配置`),
          validator: (value: string) => this.handleCheckRelatedFieldName(value),
          trigger: 'blur',
        },
      ]);
    }
    return rules;
  }

  public searchKey = '';
  public activeSelectedTab = 'current';
  public selectorBoundary: Element = document.body;
  public modelPanels = [
    { name: 'current', label: this.$t('当前项目') },
    { name: 'common', label: this.$t('公开') },
  ];

  public formData: object = {
    dimensionModelId: '',
    masterFieldName: '',
    dimensionFieldName: '',
    expandFields: [],
  };

  public rules: object = {
    mustSelect: [
      {
        required: true,
        message: this.$t('请选择'),
        trigger: 'blur',
      },
    ],
    selectExpand: [
      {
        required: true,
        message: this.$t('扩展字段必须选择'),
        trigger: 'blur',
      },
    ],
  };

  public relatedFieldName = '';

  public relatedModelName = '';

  public isModelListLoading = false;

  public isMasterTableLoading = false;

  public isInitDefaultFields = true;

  public isInitValidator: boolean = false;

  public dimensionModelList: any[] = [];

  public hasScrollBar = false;

  public observer = null;

  public selectedExpandFields: string[] = [];

  public deleteExpandFields: string[] = [];

  /** 主表数据 */
  public masterTableInfo: IDataModelManage.IMasterTableInfo = new CDataModelManage.MasterTableInfo({
    modelId: 0,
    fields: [],
    modelRelation: [],
  });

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

  @Watch('isEdit', { immediate: true })
  public handleEditStatus(status: boolean) {
    if (status) {
      this.isInitValidator = true;
      this.isInitDefaultFields = false;
      this.formData.masterFieldName = this.relatedField.fieldName;
      const relation = this.parentMstTableInfo.modelRelation.find(
        item => item.fieldName === this.relatedField.fieldName
      );
      if (relation) {
        this.formData.dimensionModelId = relation.relatedModelId;
        this.relatedFieldName = relation.relatedFieldName;
        this.relatedModelName = relation.relatedModelName;
      }
    }
  }

  @Watch('formData.dimensionModelId', { immediate: true })
  public async handleChangeDimensionModelId(id: number | string) {
    if (id) {
      this.dimensionFieldNameItem && this.dimensionFieldNameItem.clearError();
      this.expandFieldSelector && this.expandFieldSelector.clearError();
      this.formData.dimensionFieldName = '';
      this.selectedExpandFields = [];
      this.masterTableInfo = new CDataModelManage.MasterTableInfo({
        modelId: 0,
        fields: [],
        modelRelation: [],
      });
      await this.loadMasterTableInfo();
    }
  }

  @Emit('on-cancel')
  public handleCancel() { }

  @Emit('on-save')
  public handleEmitSave(result: [], isEdit: boolean, isDelete = false) {
    // 如果有扩展字段无法回填，保存的时候会自动删除，这里处理tips
    if (isEdit) {
      const nameMap = {};
      for (const field of this.nonPrimaryKeyFields) {
        nameMap[field.sourceFieldName] = true;
      }
      const autoDeleteExpandFields = this.selectedExpandFields.filter(name => !nameMap[name]);
      if (!!autoDeleteExpandFields.length) {
        const timer = setTimeout(() => {
          this.$bkMessage({
            theme: 'success',
            delay: 3000,
            message: window.$t(
                            `关联关系更新成功！同时系统已自动删除不存在扩展字段：${autoDeleteExpandFields.join('、')}`
            ),
          });
          clearTimeout(timer);
        }, 300);
      }
    }
    return { result, isEdit, isDelete };
  }

  public mounted() {
    this.getDimensionModelsCanBeRelated();
    this.setResizeObserve();
  }

  public beforeDestroy() {
    if (this.observer) {
      this.observer.disconnect();
      this.observer = null;
    }
  }

  /**
     * 判断是否只读
     * @param row
     */
  public isReadonly(row: IDataModelManage.IMasterTableField) {
    return row.deletable === false || row.editable === false;
  }

  /**
     * 获取删除 tips
     * @param row
     * @param type
     */
  public getDeleteTips(row: IDataModelManage.IMasterTableField, type = 'edit') {
    return {
      disabled: type === 'edit' ? row.editable !== false : row.deletable !== false,
      content: getFieldEditableInfo(row.editableDeletableInfo, type),
      interactive: false,
    };
  }

  /**
     * 设置元素高度变动监听
     */
  public setResizeObserve() {
    const self = this;
    const node = this.mainContent;
    this.observer = new ResizeObserver(entries => {
      self.hasScrollBar = node.scrollHeight !== node.offsetHeight;
    });
    this.observer.observe(node);
  }

  /**
     * 判断中文名是否重复
     * @param alias
     * @param index
     */
  public aliasRepeatedValidator(alias: string, index: number) {
    return !this.formData.expandFields.find(
      (field: IDataModelManage.IMasterTableField, expandIndex: number) => expandIndex !== index
                && field.fieldAlias === alias);
  }

  /**
     * 判断名称是否重复
     * @param name
     * @param index
     */
  public nameRepeatedValidator(name: string, index: number) {
    return !this.formData.expandFields.find(
      (field: IDataModelManage.IMasterTableField, expandIndex: number) => expandIndex !== index
                && field.fieldName === name);
  }

  /**
     * 判断名称是否为系统内置
     * @param name
     */
  public nameIsInnerValidator(name: string) {
    return !this.innerFieldList.find(item => item === name.toLocaleLowerCase());
  }

  /**
     * 判断名称是否跟主表字段名称重复
     * @param name
     */
  public nameRepeatedWithMstValidator(name: string) {
    return !this.parentMstTableInfo.fields.find(
      (field: IDataModelManage.IMasterTableField) => field.fieldName === name
                && field.joinFieldName !== this.formData.masterFieldName);
  }

  /**
     * 手动触发扩展字段校验
     */
  public fieldFormItemValidator() {
    const fieldFormItemKeys = Object.keys(this.$refs).filter(key => key.indexOf('fieldFormItem_') === 0);
    fieldFormItemKeys.forEach(key => {
      const item = this.$refs[key];
      if (item) {
        item.clearError();
        item.validate('blur');
      }
    });
  }

  /**
     * 失焦事件
     */
  public handleBlur() {
    this.$nextTick(this.fieldFormItemValidator);
  }

  /**
     * 检查维度模型
     * @param id
     */
  public handleCheckdimensionModel(id: string | number) {
    return !!this.dimensionModelList.find(model => model.model_id === id);
  }

  /**
     * 检查关联字段名称
     * @param name
     */
  public handleCheckRelatedFieldName(name: string) {
    return !!this.displayDimensionFields.find(item => item.fieldName === name);
  }

  /**
     * 获取维度表数据模型列表
     */
  public getDimensionModelsCanBeRelated() {
    this.isModelListLoading = true;
    getDimensionModelsCanBeRelated(this.activeModel.modelId, this.formData.dimensionModelId || undefined, true)
      .then(res => {
        res.setData(this, 'dimensionModelList');
        const model = this.dimensionModelList.find(model => model.model_id === this.formData.dimensionModelId);
        if (this.formData.dimensionModelId && model) {
          this.activeSelectedTab = model.project_id === Number(this.activeModel.projectId)
            ? 'current' : 'common';
        }
        if (this.isEdit) {
          this.dimensionModelNode
                        && this.dimensionModelNode.validate('', () => {
                          const timer = setTimeout(() => {
                            this.dimensionModelNode.validator.state === 'success'
                                    && this.dimensionFieldNameItem
                                    && this.dimensionFieldNameItem.validate();
                            clearTimeout(timer);
                          }, 300);
                        });
        }
      })
      .finally(() => {
        this.isModelListLoading = false;
      });
  }

  /**
     * 获取主表信息
     */
  public loadMasterTableInfo() {
    this.isMasterTableLoading = true;
    getMasterTableInfo(this.formData.dimensionModelId, false, undefined, undefined, true)
      .then(res => {
        if (res.result) {
          this.masterTableInfo = res.data;
          this.initDefaultField();
          // 默认填充第一个主键字段
          if (!this.isEdit && this.displayDimensionFields[0]) {
            this.formData.dimensionFieldName = this.displayDimensionFields[0].fieldName;
          }
        }
      })
      .finally(() => {
        this.isMasterTableLoading = false;
        this.isInitValidator = false;
      });
  }

  /**
     * 初始化扩展字段
     */
  public initDefaultField() {
    if (this.isInitDefaultFields) {
      this.formData.expandFields = [];
    } else {
      // 编辑的时候回填数据
      this.formData.dimensionFieldName = this.relatedFieldName;
      const fields = this.mstExpandFields
        .filter(
          (item: IDataModelManage.IMasterTableField) => item.joinFieldName === this.formData.masterFieldName
                        && item.sourceModelId === this.formData.dimensionModelId
        )
        .map((item: IDataModelManage.IMasterTableField) => Object.assign({}, item));
      const allFields = fields.map((field: IDataModelManage.IMasterTableField) => field.sourceFieldName);
      this.selectedExpandFields = allFields.filter((name: string) => this.nonPrimaryKeyFieldNameMap[name]);
      this.deleteExpandFields = allFields.filter((name: string) => !this.nonPrimaryKeyFieldNameMap[name]);
      this.isInitDefaultFields = true;
      this.isInitValidator
                && this.$nextTick(() => {
                  this.expandFieldSelector && this.expandFieldSelector.validate();
                });
    }
  }

  /**
     * 处理已选择的扩展字段
     * @param newValues
     */
  public handleSelectedExpandField(newValues: string[]) {
    // 删除 fields
    for (let i = this.formData.expandFields.length - 1; i >= 0; i--) {
      const field = this.formData.expandFields[i];
      if (!newValues.includes(field.sourceFieldName)) {
        this.formData.expandFields.splice(i, 1);
      }
    }
    const addExpandFields: IDataModelManage.IMasterTableField[] = [];
    for (const sourceFieldName of newValues) {
      const originalField = this.nonPrimaryKeyFields.find(
        (field: IDataModelManage.IMasterTableField) => field.sourceFieldName === sourceFieldName
      );
      if (originalField) {
        // 已存在的字段
        const existField = this.formData.expandFields.find(
          (field: IDataModelManage.IMasterTableField) => field.sourceFieldName === sourceFieldName
        ) || {};
        addExpandFields.push(Object.assign(originalField, existField));
      }
    }
    this.formData.expandFields = addExpandFields;
  }

  /**
     * 控制字段删除
     * @param row
     * @param index
     */
  public handleRemoveField(row: IDataModelManage.IMasterTableField, index: number) {
    if (this.isReadonly(row)) {
      return;
    }
    this.selectedExpandFields.splice(index, 1);
  }

  /**
     * 保存设置信息
     */
  public async handleSave() {
    const validate = await this.$refs.dimensionForm.validate().then(
      validator => true,
      validator => false
    );
    if (validate === false) {
      return false;
    }
    const result = {
      joinFieldName: this.formData.masterFieldName,
      expandFields: [],
      relation: {
        modelId: this.activeModel.modelId,
        fieldName: this.formData.masterFieldName,
        relatedModelId: this.formData.dimensionModelId,
        relatedFieldName: this.formData.dimensionFieldName,
        joinFieldUId: this.joinField && this.joinField.uid,
      },
    };
    const sourceInfo = {
      sourceModelId: this.formData.dimensionModelId,
      joinFieldName: this.formData.masterFieldName,
    };
    for (const field of this.formData.expandFields) {
      if (field.id && field.fieldName && field.fieldAlias) {
        Object.assign(field, sourceInfo, { joinFieldUId: this.joinField && this.joinField.uid });
        field.isExtendedField = true;
        result.expandFields.push(field);
      }
    }
    this.handleEmitSave(result, this.isEdit);
  }

  /**
     * 展示确认框
     * @param e
     */
  public handleShowConfirm(e) {
    this.clearConfirmPop.handlePopShow(e, { placement: 'top-end', hideOnClick: false, maxWidth: '280px' });
  }

  /**
     * 隐藏确认框
     */
  public handleHideConfirm() {
    this.clearConfirmPop && this.clearConfirmPop.handlePopHidden();
  }

  /**
     * 确认删除
     */
  public handleConfirmDelete() {
    const result = {
      joinFieldName: this.formData.masterFieldName,
      expandFields: [],
      relation: {
        modelId: this.activeModel.modelId,
        fieldName: this.formData.masterFieldName,
        relatedModelId: this.formData.dimensionModelId,
        relatedFieldName: this.formData.dimensionFieldName,
        joinFieldUId: this.joinField && this.joinField.uid,
      },
    };
    this.handleEmitSave(result, this.isEdit, true);
    this.handleHideConfirm();
  }

  @Emit('create-model-table')
  public handleCreateNewModelTable() {
    return true;
  }
}
