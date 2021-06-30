<!---
 Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available. 
 
 Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved. 
 
 BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
 
 License for BK-BASE 蓝鲸基础平台:
 --------------------------------------------------------------------
 Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 documentation files (the "Software"), to deal in the Software without restriction, including without limitation
 the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
 and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 
 The above copyright notice and this permission notice shall be included in all copies or substantial
 portions of the Software.
 
 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
 LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
-->

###1.3.1
- 优化：flink-streaming在关闭资源之前，先判断对象是否存在。
- 修复：确保checkpoint之前将内存中的数据发送出去。
- 功能：支持实时累加窗口计算
- 修复：实时输出到kafka的数据，以utf8编码转成byte
- 优化：flink中flink-streaming-java依赖范围更改为provided
- 修复：修复实时计算静态关联有时关联不到数据的问题
- 修复：kafka consumer指定auto.offset.reset值，修复当从头读取数据偶尔不生效的问题
- 优化：kafak producer设置max.request.size为3M