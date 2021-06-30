

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
  <div class="no-project">
    <new-project ref="project"
      @createNewProject="handCreateNewProject" />
  </div>
</template>
<script type="text/javascript">
import { mapGetters } from 'vuex';
import { postMethodWarning } from '@/common/js/util.js';
import newProject from '@/pages/DataGraph/Graph/Components/project/newProject';
export default {
  components: {
    newProject,
  },
  data() {
    return {
      name: '',
      error: false,
      tip: '',
    };
  },
  computed: {
    ...mapGetters(['getNow']),
  },
  watch: {
    /*
     * 项目下拉变化，触发全局 PID 更新
     */
    defaultSelected(val, oldVal) {
      this.$store.dispatch('updatePid', val);
    },
  },
  mounted() {
    this.$refs.project.open();
  },
  methods: {
    handCreateNewProject(params) {
      this.axios.post('v3/meta/projects/', params).then(res => {
        if (res.result) {
          this.$bkMessage({
            message: this.$t('成功'),
            theme: 'success',
          });
          this.$router.push({ name: 'user_center' });
        } else {
          postMethodWarning(res.message, 'error');
        }
        this.$refs.project.reset();
        this.$refs.project.close();
      });
    },
    nameInput() {
      if (this.name.length === 0) {
        this.tip = this.validator.message.required;
        this.error = true;
      } else if (this.name.length > 15) {
        this.tip = this.validator.message.max15;
        this.error = true;
      } else {
        this.error = false;
      }
    },
    createItem() {
      if (this.name.length === 0 || this.name.length > 15) {
        return;
      } else {
        let params = {
          project_name: this.name,
          description: this.name,
        };
        this.axios
          .post('v3/meta/projects/', params)
          .then(res => {
            if (res.result) {
              this.$bkMessage({
                message: this.$t('成功'),
                theme: 'success',
              });
              this.$store.dispatch('updataTime', new Date());
              this.$router.push('/');
              this.error = false;
              this.name = '';
            } else {
              postMethodWarning(res.message, 'error');
            }
          })
          ['catch'](function (error) {
            this.$bkInfo({
              statusOpts: {
                title: this.$t('加载数据失败'),
                subtitle: false,
              },
              type: 'error',
            });
            console.log(error);
          });
      }
    },
  },
};
</script>
<style lang="scss" type="text/css">
.no-project {
  background: #f2f4f9;
  width: 100%;
  height: 93%;
  position: relative;
  .project {
    background: #fff;
    width: 576px;
    height: 278px;
    position: absolute;
    left: 50%;
    top: 50%;
    transform: translate(-50%, -50%);
  }
  .left {
    float: left;
    height: 100%;
    width: 180px;
    background-color: #3a84ff;
    border-radius: 2px 0 0 2px;
    i {
      color: #fff;
      font-size: 69px;
      position: relative;
      top: 103px;
      left: 60px;
    }
  }
  .right {
    box-sizing: border-box;
    width: 395px;
    float: right;
    padding: 50px 40px;
    background: #fff;
    height: 100%;
    .title {
      font-size: 18px;
      font-weight: 500;
      width: 260px;
      color: #444444;
    }
    .tip {
      padding-top: 20px;
      color: #737987;
      font-size: 14px;
      color: #aaa;
    }
    .bk-form {
      width: 320px;
    }
    .bk-form-content {
      margin-top: 15px;
      margin-left: 0px;
    }
    .bk-button {
      width: 120px;
      height: 42px;
      line-height: 42px;
    }
  }
  .input-error {
    border-color: red;
  }
  .error-tip {
    margin-top: 5px;
    color: red;
  }
}
</style>
