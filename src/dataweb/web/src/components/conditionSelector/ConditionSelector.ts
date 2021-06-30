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
import popContainer from '@/components/popContainer';
import { Component, Emit, Prop, Ref, Vue, Watch } from 'vue-property-decorator';
import { VNode } from 'vue/types/umd';

@Component({
  components: {
    popContainer,
  },
})
export default class ConditionSelector extends Vue {
  // 条件列表
  get conditionList() {
    return this.constraintList.sort((next, prev) => next.groupType - prev.groupType);
  }

  get conditionMap() {
    const map: any = {};
    for (const condition of this.constraintList) {
      map[condition.constraintId] = condition;
      if (condition.children) {
        for (const child of condition.children) {
          child.parentId = condition.constraintId;
          map[child.constraintId] = child;
        }
      }
    }
    return map;
  }
  @Prop({ default: () => ({}) }) public constraintContent!: object;
  @Prop({ default: () => [] }) public constraintList!: any[];
  @Prop({ default: false }) public readonly!: boolean;
  @Prop({ default: 'click' }) public trigger!: string;

  @Ref() public readonly conditionSelectorForm!: VNode;
  @Ref() public readonly conditionSelectorInput!: VNode;

  public selectorBoundary: Element = document.body;

  public validatorMap = {
    number: {
      validator(val: string) {
        return !isNaN(Number(val));
      },
      message: this.$t('请输入数字'),
      trigger: 'blur',
    },
    regex: {
      validator(val: string) {
        return /^\/.+\/[gimsuy]*$/.test(val);
      },
      message: this.$t('请输入正确的正则'),
      trigger: 'blur',
    },
    required: {
      required: true,
      message: this.$t('不能为空'),
      trigger: 'blur',
    },
  };

  public conditionData = {};

  public sameGroupIds = [];

  @Emit('change')
  public handleContentChange(content: object | null) {
    return content;
  }

  @Watch('constraintContent')
  public handleChange() {
    this.handleShowCondition();
  }

  public deepClone(obj: any, cache: any[] = []) {
    if (obj == null || typeof obj !== 'object') {
      return obj;
    }
    const hit = cache.filter(c => c.original === obj)[0];
    if (hit) {
      return hit.copy;
    }
    const copy = Array.isArray(obj) ? [] : {};
    cache.push({
      original: obj,
      copy,
    });
    Object.keys(obj).forEach(key => {
      copy[key] = this.deepClone(obj[key], cache);
    });
    return copy;
  }

  public getDefaultGroupData() {
    return {
      uid: generateId('_group_'),
      isEmpty: true,
      op: 'AND',
      items: [this.getDefaultItemData()],
    };
  }

  public getDefaultItemData() {
    return {
      uid: generateId('_item_'),
      constraintId: [],
      constraintContent: '',
      config: {
        editable: true,
        placeholder: '',
        rules: [],
      },
    };
  }

  public formatData(content: object) {
    const data = this.deepClone(content);
    const groups = data.groups || [];
    for (let index = 0, len = groups.length; index < len; index++) {
      const group = groups[index];
      !group.uid && Object.assign(group, { uid: generateId('_group_') });
      const items = [];
      const conditions = group.items || [];
      for (let childIndex = 0, childLen = conditions.length; childIndex < childLen; childIndex++) {
        const item = conditions[childIndex];
        !item.uid && Object.assign(item, { uid: generateId('_item_') });
        const formatItem = {
          ...item,
          constraintId: [item.constraintId],
          config: {
            editable: true,
            placeholder: '',
            rules: [],
          },
        };
        const conditionItem = this.conditionMap[item.constraintId];
        if (formatItem && conditionItem) {
          conditionItem.parentId && formatItem.constraintId.unshift(conditionItem.parentId);
          const formatCondition = this.formatCondition(formatItem, conditionItem, index, childIndex, false);
          Object.assign(formatItem, formatCondition);
        }
        items.push(formatItem);
      }
      group.items = items;
    }
    return data;
  }

  public formatCondition(data: object, condition: object, index: number, childIndex: number, setValue = true) {
    data.config.placeholder = condition.constraintValue;
    data.config.editable = condition.editable;

    // 同组内条件是否相同判断
    data.config.rules = [
      {
        message: this.$t('条件重复'),
        validator: (value: string) => this.conditionRepeatedValidator(value, index, childIndex),
        trigger: 'blur',
      },
    ];

    if (!condition.editable && setValue) {
      data.constraintContent = condition.constraintValue;
    } else if (condition.editable) {
      data.config.rules.unshift(this.validatorMap.required);
      if (condition.validator.content && condition.validator.content.regex) {
        const regex = condition.validator.content.regex;
        data.config.rules.push({
          regex: new RegExp(regex),
          message: `${this.$t('输入内容不符合正则要求')}: /${regex}/`,
          trigger: 'blur',
        });
      } else if (condition.validator.type === 'number_validator') {
        data.config.rules.push(this.validatorMap.number);
      } else if (condition.validator.type === 'regex_validator') {
        // 目前根据前后斜杠校验是否为正则，python可不写前后斜杠，所以先不校验
        // data.config.rules.push(this.validatorMap['regex'])
      }
    }
    return data;
  }

  public conditionRepeatedValidator(value: string, index: number, childIndex: number) {
    const group = this.conditionData.groups[index];
    if (group?.items) {
      if (group.items.length <= 1) {
        return true;
      }
      const curItem = group.items[childIndex];
      for (let i = 0, length = group.items.length; i < length; i++) {
        if (i === childIndex) {
          continue;
        }
        const item = group.items[i];
        // 编辑状态不同则为不同条件
        if (item.config.editable !== curItem.config.editable) {
          continue;
        }
        if (
          curItem.constraintContent === item.constraintContent
                    && curItem.constraintId.toString() === item.constraintId.toString()
        ) {
          return false;
        }
      }
    }
    return true;
  }

  // 校验不同组条件是否完全相同
  public diffConditionGroupvalidator() {
    // 清空报错组
    this.sameGroupIds = [];

    const groups = this.deepClone(this.conditionData.groups);
    if (groups.length <= 1) {
      return true;
    }
    const sameGroupIds = [];
    for (let i = 0; i < groups.length; i++) {
      const curGroup = groups[i];
      const curItems = curGroup.items;
      const curConditionValue = curItems.map(item => `${item.constraintId.toString()}_${item.constraintContent}`);
      for (let j = groups.length - 1; j > i; j--) {
        const nextGroup = groups[j];
        const nextItems = nextGroup.items;
        // 组内条件数不同则为不同条件组
        if (curItems.length !== nextItems.length || j === i) {
          continue;
        }
        const nextValue = nextItems.map(item => `${item.constraintId.toString()}_${item.constraintContent}`);
        // 通过去重判断是否为相同条件组
        const valuesSet = new Set(curConditionValue.concat(nextValue));
        // 去重后的个数和原来个数相同则为相同条件组
        if (valuesSet.size === curConditionValue.length) {
          sameGroupIds.push(...[curGroup.uid, nextGroup.uid]);
          groups.splice(j, 1);
        }
      }
    }
    this.sameGroupIds = Array.from(new Set(sameGroupIds));
    return !this.sameGroupIds.length;
  }

  // 手动触发同组条件校验
  public sameGroupConditionValidator(index: number, childIndex: number) {
    const validateItemPrefix = 'validate_content_' + index;
    const curItemKey = `validate_content_${index}_${childIndex}`;
    const validateKeys = Object.keys(this.$refs).filter(key => key.indexOf(validateItemPrefix) === 0);
    for (const key of validateKeys) {
      // 避免选择后立即校验为空报错
      if (key === curItemKey) {
        continue;
      }
      const item = Array.isArray(this.$refs[key]) ? this.$refs[key][0] : this.$refs[key];
      if (item) {
        item.clearError();
        item.validate('blur');
      }
    }
  }

  public handleAddGroup() {
    this.conditionData.groups.push(this.getDefaultGroupData());
  }

  public handleDeleteGroup(index: number) {
    this.conditionData.groups.splice(index, 1);
    this.sameGroupIds = [];
  }

  public handleAddItem(index: number) {
    this.conditionData.groups[index].items.push(this.getDefaultItemData());
    this.$nextTick(() => {
      this.sameGroupConditionValidator(index);
    });
    this.sameGroupIds = [];
  }

  public handleDeleteItem(index: number, childIndex: number) {
    this.conditionData.groups[index].items.splice(childIndex, 1);
    this.$nextTick(() => {
      this.sameGroupConditionValidator(index);
    });
    this.sameGroupIds = [];
  }

  public handleItemBlur(index: number) {
    this.sameGroupIds = [];
    this.$nextTick(() => {
      this.sameGroupConditionValidator(index);
    });
  }

  public handleClearCurConditionError(index: number, childIndex: number) {
    const refIds = [`validate_id_${index}_${childIndex}`, `validate_content_${index}_${childIndex}`];
    for (const key of refIds) {
      const item = Array.isArray(this.$refs[key]) ? this.$refs[key][0] : this.$refs[key];
      item?.clearError();
    }
  }

  public handleConditionChange(
    newValue: any[], oldValue: any[], selectList: any[], index: number, childIndex: number) {
    const depth = newValue.length;
    const value = depth > 1 ? newValue[1] : newValue[0];
    const selectItem = depth > 1 ? selectList[1] : selectList[0];
    const item = this.conditionData.groups[index].items[childIndex];

    // 重置内容
    item.config.editable = true;
    item.constraintContent = '';
    item.config.placeholder = '请输入';
    item.config.rules = [];

    // 清空当前条件报错
    this.handleClearCurConditionError(index, childIndex);

    if (value && selectItem) {
      Object.assign(item, this.formatCondition(item, selectItem, index, childIndex));
    }
    this.$nextTick(() => {
      this.sameGroupConditionValidator(index, childIndex);
    });
    this.sameGroupIds = [];
  }

  public handleShowCondition() {
    // 初始化数据
    this.conditionData = this.constraintContent
      ? this.formatData(this.constraintContent)
      : {
        op: 'AND',
        groups: [this.getDefaultGroupData()],
      };
  }

  // public handleClear() {
  //     this.conditionData = {
  //         op: 'AND',
  //         groups: [this.getDefaultGroupData()]
  //     }
  //     this.handleContentChange(null)
  // }

  public handleChangeItemOp(index: number, op: string) {
    const item = this.conditionData.groups[index];
    if (item) {
      Object.assign(item, { op: op === 'AND' ? 'OR' : 'AND' });
    }
  }

  public handleChangeGroupOp(op: string) {
    this.$set(this.conditionData, 'op', op === 'AND' ? 'OR' : 'AND');
  }

  public handleSubmitCondition() {
    return this.conditionSelectorForm.validate().then(validate => {
      if (!this.diffConditionGroupvalidator()) {
        return Promise.reject(this.$t('条件组重复'));
      }
      const data = this.deepClone(this.conditionData);
      // 处理提交数据
      for (const group of data.groups) {
        delete group.isEmpty;
        delete group.uid;
        for (const item of group.items) {
          delete item.config;
          delete item.uid;
          item.constraintId = item.constraintId[item.constraintId.length - 1];
        }
      }
      this.handleContentChange(data);
      return data;
    });
  }

  public mounted() {
    this.conditionData = this.constraintContent
      ? this.formatData(this.constraintContent)
      : {
        op: 'AND',
        groups: [this.getDefaultGroupData()],
      };
  }
}
