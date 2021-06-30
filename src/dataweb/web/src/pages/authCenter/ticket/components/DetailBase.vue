

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
  <div class="ticket-base">
    <div v-for="(item, index) in items"
      :key="index"
      class="base-item">
      <!-- 申请主体 -->
      <div class="base-wrap">
        <div class="base-row">
          <div class="base-label">
            {{ item.subject_class_name }}
          </div>
          <div class="base-content">
            <div v-for="(sub, idx) in item.subjects"
              :key="idx"
              class="auth-object-wrap">
              {{ showAuthObject(item.subject_class, sub) }}
            </div>
          </div>
        </div>
      </div>
      <div class="base-splitter" />

      <!-- 申请内容 -->
      <div class="base-wrap">
        <div class="base-row">
          <div class="base-label">
            {{ $t('申请内容') }}
          </div>
          <div class="base-content">
            {{ item.object_class_name }} > {{ item.action_name }}
          </div>
        </div>
      </div>
      <div class="base-splitter" />

      <!-- 申请范围 -->
      <div class="base-wrap">
        <div class="base-row">
          <div class="base-label">
            {{ $t('申请范围') }}
          </div>
          <div class="base-content">
            <div v-for="(scope, scopeIndex) in item.scope_objects"
              :key="scopeIndex"
              class="auth-object-wrap">
              [{{ scope.scope_object_class_name }}] {{ scope.scope_name }}
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
export default {
  props: {
    ticketType: {
      type: String,
      required: true,
    },
    // 这里 Permission 的数据结构跟后台关系紧密，需要好好对齐接口协议
    permissions: {
      type: Array,
      required: true,
    },
  },
  data() {
    return {
      items: [],
    };
  },

  watch: {
    permissions(newVal) {
      this.toSelfDataFromPermissions(newVal);
    },
  },
  mounted() {
    this.toSelfDataFromPermissions(this.permissions);
  },
  methods: {
    showAuthObject(subjectClass, sub) {
      // 用户只显示id即可
      if (subjectClass === 'user') {
        return sub.subject_id;
      }
      return `[${sub.subject_id}] ${sub.subject_name}`;
    },
    toSelfDataFromPermissions(permissions) {
      let M_KEYS = {};

      // 根据主体、动作、客体，划分不同申请内容
      for (let perm of permissions) {
        let key = `${perm.subject_id}_${perm.subject_class}_${perm.action}_${perm.object_class}`;
        if (M_KEYS[key] === undefined) {
          M_KEYS[key] = {
            subject_class: perm.subject_class,
            subject_class_name: perm.subject_class_name,
            object_class: perm.object_class,
            object_class_name: perm.object_class_name,
            action: perm.action,
            action_name: perm.action_name,
            subjects: [
              {
                subject_id: perm.subject_id,
                subject_name: perm.subject_name,
                subject_info: perm.subject_info,
              },
            ],
            scope_objects: [perm.scope_object],
          };
        } else {
          // 判断 perm 的申请主体是否重复
          let isDup = false;
          for (let sub of M_KEYS[key].subjects) {
            if (perm.subject_id === sub.subject_id) {
              isDup = true;
              break;
            }
          }
          if (!isDup) {
            M_KEYS[key].subjects.push({
              subject_id: perm.subject_id,
              subject_name: perm.subject_name,
              subject_info: perm.subject_info,
            });
          }

          M_KEYS[key].scope_objects.push(perm.scope_object);
        }
      }

      this.items = [];
      for (let k in M_KEYS) {
        this.items.push(M_KEYS[k]);
      }
    },
  },
};
</script>

<style lang="scss">
@import '../../scss/base.scss';

.base-item {
  background-color: #efefef59;
  padding: 10px 20px;
  margin-bottom: 20px;
}

.base-wrap {
  .base-row {
    margin-top: 5px;
    margin-bottom: 5px;
    display: flex;
  }
  .base-label {
    float: left;
    min-width: 100px;
    line-height: 40px;
    border-right: 1px dashed #ccc;
  }
  .base-content {
    margin-left: 25px;
    line-height: 40px;
  }
  .auth-object-wrap {
    margin-left: 0;
  }
}
.base-splitter {
  border-bottom: 1px solid #ccc;
  border-bottom-style: dashed;
}
</style>
