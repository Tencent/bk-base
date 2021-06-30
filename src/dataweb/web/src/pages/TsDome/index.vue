

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
    <bkdata-table style="margin-top: 15px"
      :data="noteList">
      <bkdata-table-column type="selection"
        width="60"
        align="center" />
      <bkdata-table-column type="index"
        label="序列"
        align="center"
        width="60" />
      <bkdata-table-column label="ID"
        prop="notebook_id" />
      <bkdata-table-column label="笔记名"
        prop="notebook_name" />
      <bkdata-table-column label="URL"
        prop="notebook_url" />
      <bkdata-table-column label="创建时间"
        prop="created_at" />
      <bkdata-table-column label="创建人"
        prop="created_by" />
      <bkdata-table-column label="修改时间"
        prop="updated_at" />
      <bkdata-table-column label="修改人"
        prop="updated_by" />
    </bkdata-table>
  </div>
</template>
<script>
import { note } from '../../controller/test.ts';
export default {
  data() {
    return {
      noteList: [],
    };
  },
  mounted() {
    this.noteList = note;
    console.log('123', note);
  },
};
</script>
