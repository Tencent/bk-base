

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
  <div>
    <UpdataMember
      v-model="memberList"
      :isDialogLoading="isDialogLoading"
      :isLoading="isAllMemberLoading"
      :managerList="managerList"
      :objectClassName="$t('数据源')"
      :managerName="$t('数据管理员')"
      :bizId="details.bk_biz_id"
      :objectName="details.data_id"
      @close="close"
      @saveMember="saveMember" />
  </div>
</template>

<script>
import { updateRoleUsers } from '@/common/api/auth';
import UpdataMember from '@/components/global/updataMember';
import { showMsg } from '@/common/js/util.js';

export default {
  components: {
    UpdataMember,
  },
  props: {
    value: {
      type: Boolean,
      default: false,
    },
    details: {
      type: Object,
      default: () => ({}),
    },
  },
  data() {
    return {
      memberList: [],
      managerOriginList: [],
      isDialogLoading: false,
      isAllMemberLoading: false,
    };
  },
  computed: {
    managerList() {
      return (this.managerOriginList || []).map(user => {
        return {
          id: user,
          name: user,
        };
      });
    },
    roles() {
      return (
        (this.details.access_raw_data
          && this.details.access_raw_data.maintainer && [
          {
            members: this.details.access_raw_data.maintainer.split(','),
          },
        ])
        || []
      );
    },
  },
  watch: {
    roles: {
      immediate: true,
      handler(val) {
        if (val.length) {
          this.memberList = [];
          val[0].members.forEach(role => {
            this.memberList.push(role);
          });
        }
      },
    },
  },
  mounted() {
    this.initDataMember();
  },
  methods: {
    initDataMember() {
      this.isAllMemberLoading = true;
      this.axios
        .get('projects/list_all_user/')
        .then(res => {
          if (res.result) {
            this.managerOriginList = res.data;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.isAllMemberLoading = false;
        });
    },
    saveMember() {
      this.isDialogLoading = true;
      updateRoleUsers({
        role_users: [
          {
            role_id: 'raw_data.manager',
            role_name: '数据管理员',
            object_class: 'raw_data',
            object_class_name: '原始数据',
            object_name: `[${this.details.data_id}]${this.details.access_raw_data.raw_data_name}`,
            scope_id: this.details.data_id,
            user_ids: this.memberList,
            can_modify: true,
          },
        ],
      })
        .then(res => {
          if (res.result) {
            showMsg(window.$t('保存成功'), 'success', { delay: 2000 });
          } else {
            showMsg(res.message, 'error');
          }
        })
        ['finally'](() => {
          this.isDialogLoading = false;
          this.close();
        });
    },
    close() {
      this.$emit('input', false);
    },
  },
};
</script>
