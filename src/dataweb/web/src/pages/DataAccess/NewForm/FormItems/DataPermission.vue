

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
  <Container class="bk-access-Container">
    <Item>
      <div class="bk-form-content permission">
        <div v-bkloading="{ isLoading: memberLoading }"
          class="bk-user-permission">
          <bkdata-tag-input
            ref="userManager"
            v-model="sports"
            v-tooltip.notrigger="validate['maintainer']"
            :placeholder="inputPlaceholder"
            :hasDeleteIcon="true"
            :list="roleMembers"
            :tpl="tpl" />
        </div>
      </div>
    </Item>
  </Container>
</template>
<script>
import { validateComponentForm } from '../SubformConfig/validate.js';
import Container from './ItemContainer';
import Item from './Item';
export default {
  components: { Container, Item },
  props: {
    bizId: {
      type: Number,
      default: () => {
        return 0;
      },
    },
    isEdit: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      memberLoading: false,
      roleIds: ['raw_data.manager'],
      sports: [],
      inputPlaceholder: window.$t('请输入并按Enter结束'),
      isFirstValidate: true,
      roleLists: [],
      managers: [],
      params: {
        role: 'bk_biz_maintainer',
        maintainer: '',
      },
      validate: {
        maintainer: {
          regs: { required: true, error: window.$t('不能为空') },
          content: '',
          visible: false,
          class: 'error-red',
        },
      },
    };
  },
  computed: {
    scopeID() {
      return this.$route.params.did;
    },
    bkUser() {
      return this.$store.getters.getUserName;
    },
    roleMembers() {
      return (this.roleLists || []).map(user => {
        return {
          id: user,
          name: user,
        };
      });
    },
    maintainers() {
      return this.sports.join(',');
    },
  },
  watch: {
    bizId: {
      immediate: true,
      handler(val) {
        !this.isEdit && this.getDefaultDataManager();
      },
    },
    maintainers(val) {
      !this.isFirstValidate && validateComponentForm(this.validate, { maintainer: val });
      this.$forceUpdate();
    },
  },
  mounted() {
    this.initDataMember();
    !this.isEdit && this.getDefaultDataManager();
    this.scopeID && this.getManagerMember(); // 编辑时，通过auth接口获取管理员名单
  },
  methods: {
    tpl(node, ctx) {
      let parentClass = 'bkdata-selector-node';
      let textClass = 'text';
      let imgClass = 'avatar';
      return (
        <div class={parentClass}>
          <span class={textClass}>{node.name}</span>
        </div>
      );
    },
    /**
     * 获取默认数据管理员
     */
    getDefaultDataManager() {

      if (this.bizId) {
        this.sports = [this.bkUser];
      }
    },

    /**
     * 获取该任务下的管理员名单
     */
    getManagerMember() {
      this.memberLoading = true;
      this.bkRequest
        .httpRequest('authV1/queryUserRoles', {
          query: {
            scope_id: this.scopeID,
            object_class: 'raw_data',
            show_display: true,
          },
        })
        .then(res => {
          if (res.result) {
            const data = res.data[0].roles.filter(item => {
              return this.roleIds.includes(item.role_id);
            });
            this.sports = data[0].users;
          } else {
            this.getMethodWarning(res.message, 'error');
          }
        })
        ['finally'](() => {
          this.memberLoading = false;
        });
    },
    /*
                 获取所有成员名单,获取成员
             */
    initDataMember() {
      return new Promise((resolve, reject) => {
        this.axios.get('projects/list_all_user/').then(res => {
          if (res.result) {
            this.roleLists = res.data;
            resolve();
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        });
        !this.isEdit && this.sports.push(this.bkUser);
      });
    },

    formatFormData() {
      this.params.maintainer = this.maintainers;
      return {
        group: '',
        identifier: 'access_raw_data',
        data: this.params,
      };
    },
    renderData(data) {
      Object.keys(this.params).forEach(key => {
        data['access_raw_data'][key] && this.$set(this.params, key, data['access_raw_data'][key]);
      });

      // this.sports = this.params.maintainer.split(',')
    },
    validateForm(validateFunc) {
      this.isFirstValidate = false;
      const isvalidate = validateFunc(this.validate, { maintainer: this.maintainers });
      this.$forceUpdate();
      return isvalidate;
    },
  },
};
</script>
<style lang="scss" scoped>
.bk-access-Container {
  .bk-form-content {
    display: flex;
    width: 100%;
    .bk-user-permission .bk-tag-input {
      width: 600px;
      height: 200px;
    }

    &.permission {
      width: 100%;
    }
    .bk-user-permission {
      box-shadow: none;
      width: 789px;
      min-height: 80px;

      ::v-deep .bk-tag-input {
        min-height: 80px;
      }
    }
  }
}
</style>
