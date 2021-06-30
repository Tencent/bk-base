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

import permissionApply from '@/pages/authCenter/permissions/PermissionApplyWindow';
import newProject from '@/pages/DataGraph/Graph/Components/project/newProject.vue';
import { Component, Emit, Prop, PropSync, Vue } from 'vue-property-decorator';
import { ModelTab } from './Common/index.ts';
import MenuItemList from './MenuItemList';
@Component({
  components: {
    MenuItemList,
    ModelTab,
    permissionApply,
    newProject,
  },
})
export default class ModelLeftMenu extends Vue {
  @Prop({ default: true }) public isProject: boolean;

  @Prop({ default: () => [] }) public projectList: any[];

  /** 顶部Tab菜单数据 */
  @Prop({ default: () => [] }) public topTabList: any[];

  /** 左侧Tab菜单数据 */
  @Prop({ default: [] }) public leftTabList: any[];

  /** 顶部tab选中 */
  @PropSync('topTabName') public syncTopTabName: string;

  /** 顶部tab选中 */
  @PropSync('leftTabName') public syncLeftTabName: string;

  @PropSync('projectId') public syncProjectId: Number | string;

  get showProjectList() {
    return this.projectList
      .map(item => Object.assign({}, item, { displayName: `[${item.project_id}]${item.project_name}` })
      );
  }

  @Emit('projectChanged')
  public handleChange(value: string) {
    return value;
  }

  @Emit('activeTypeChanged')
  public handleTypeChanged(typeName: string) {
    return typeName;
  }

  @Emit('modelTypeChanged')
  public handleModelTypeChanged(modelType: string) {
    return modelType;
  }

  public handleNewProject() {
    this.$refs.newproject.open();
  }

  public handlePermissionApply() {
    this.$refs.permissionApply && this.$refs.permissionApply.openDialog();
  }

  public handleCreatedProject(params) {
    this.bkRequest.httpRequest('meta/creatProject', { params }).then(res => {
      if (res.result) {
        this.$bkMessage({
          message: this.$t('成功'),
          theme: 'success',
        });
        this.$refs.newproject.close();
        Object.assign(params, { project_id: res.data });
        this.projectList.unshift(params);
        this.syncProjectId = res.data;
        this.handleChange(res.data);
        // 补充添加人员
        this.bkRequest.httpRequest('auth/addProjectMember', {
          params: {
            project_id: this.projectId,
            role_users: [
              {
                role_id: 'project.manager',
                user_ids: params.admin,
              },
              {
                role_id: 'project.flow_member',
                user_ids: params.member,
              },
            ],
          },
        });
      } else {
        postMethodWarning(res.message, 'error');
        this.$refs.newproject.reset();
      }
    });
  }

  public defaultSelectedValue() {
    return {
      roleId: 'project.viewer',
      objectClass: 'project',
      scopeId: this.syncProjectId,
    };
  }
}
