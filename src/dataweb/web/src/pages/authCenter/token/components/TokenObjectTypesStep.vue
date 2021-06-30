

<!--
  - Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
  - Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
  - BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
  -
  - License for BK-BASE 蓝鲸基础平台:
  - -------------------------------------------------------------------
  -
  - Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
  - documentation files (the "Software"), to deal in the Software without restriction, including without limitation
  - the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
  - and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
  - The above copyright notice and this permission notice shall be included in all copies or substantial
  - portions of the Software.
  -
  - THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
  - LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
  - NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
  - WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
  - SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE
  -->

<template>
  <div class="token-objects-types">
    <!-- <BKDataAlert>{{ $t('请选择功能权限，并指明对象的操作功能') }}</BKDataAlert> -->
    <div class="bk-form">
      <bkdata-checkbox-group v-model="checkedActionID">
        <!-- @todo 这里有些特殊逻辑，把biz.access_raw_data 放到了 raw_data下展示，需讨论 -->
        <span v-for="(object, index) of calcObjectTypeList"
          :key="index">
          <template v-if="object.object_class !== 'biz'">
            <bkdata-checkbox
              v-for="(action, xIndex) of object.actions"
              :key="xIndex"
              :disabled="!action.can_be_applied || !object.scope_object_classes.length"
              :value="action.action_id">
              {{ action.action_name }}
            </bkdata-checkbox>
          </template>
        </span>
      </bkdata-checkbox-group>
    </div>
  </div>
</template>

<script>
export default {
  props: {
    selectedScopes: {
      type: Array,
      default: () => [],
    },
  },
  data() {
    return {
      objectTypeList: [],
      checkedActionID: [],
    };
  },
  computed: {
    biz_access_raw_data_can_be_applied() {
      let result = false;
      this.objectTypeList.map(object => {
        object.actions.map(action => {
          if (action.action_id === 'biz.access_raw_data') {
            result = action.can_be_applied;
          }
        });
      });
      return result;
    },
    calcObjectTypeList() {
      // eslint-disable-next-line vue/no-side-effects-in-computed-properties
      return this.objectTypeList
        .sort((a, b) => a.order - b.order)
        .map(obj => {
          obj.actions = obj.actions.sort((a1, b1) => a1.order - b1.order);
          return obj;
        });
    },
  },
  watch: {
    checkedActionID(val) {
      let scopes = [];
      for (let actionId of val) {
        for (let object of this.objectTypeList) {
          for (let action of object.actions) {
            if (actionId === action.action_id) {
              scopes.push({
                object_class: object.object_class,
                object_class_name: object.object_class_name,
                action_id: action.action_id,
                action_name: action.action_name,
                has_instance: action.has_instance,
                scope_object_classes: object['scope_object_classes'],
              });
            }
          }
        }
      }

      this.$emit('update:selectedScopes', scopes);
    },
  },
  async mounted() {
    this.objectTypeList = await this.$store.dispatch('auth/actionUpdateObjectTypeList');
    // 过滤掉不能申请和子对象数组长度为0的子项
    this.objectTypeList = this.objectTypeList.filter(
      obj => !obj.actions.every(child => !child.can_be_applied) && obj.scope_object_classes.length
    );
    this.objectTypeList.forEach(item => {
      item.actions = item.actions.filter(child => child.can_be_applied);
    });
  },
};
</script>

<style lang="scss" scoped>
.token-objects-types {
  margin-left: 48px;
  .tools {
    display: flex;
    flex-wrap: wrap;
    .bk-button {
      margin-right: 5px;
      margin-bottom: 5px;
    }
  }
  .bk-form {
    overflow: hidden;

    .bk-form-item {
      width: 100%;
      margin-top: 0;
      .bk-form-content {
        .bk-form-control {
          display: flex;
        }
      }
    }
  }
}
</style>
