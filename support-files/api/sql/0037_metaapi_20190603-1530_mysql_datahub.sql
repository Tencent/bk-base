/*
 * Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
 *
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
 *
 * BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
 *
 * License for BK-BASE 蓝鲸基础平台:
 * --------------------------------------------------------------------
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
 * and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
 * LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 * NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

set NAMES utf8;

use bkdata_basic;

INSERT INTO content_language_config (content_value, language, content_key, description) VALUES ('bkdata','en','蓝鲸基础计算平台','');
INSERT INTO content_language_config (content_value, language, content_key, description) VALUES ('default group','en','默认集群组','');
INSERT INTO content_language_config (content_value, language, content_key, description) VALUES ('double','en','浮点型','');
INSERT INTO content_language_config (content_value, language, content_key, description) VALUES ('int','en','整型','');
INSERT INTO content_language_config (content_value, language, content_key, description) VALUES ('long','en','长整型','');
INSERT INTO content_language_config (content_value, language, content_key, description) VALUES ('string(512)','en','字符类型','');
INSERT INTO content_language_config (content_value, language, content_key, description) VALUES ('text','en','文本类型','');
INSERT INTO content_language_config (content_value, language, content_key, description) VALUES ('timestamp','en','时间戳类型','');
INSERT INTO content_language_config (content_value, language, content_key, description) VALUES ('decommissioned','en','已退服','');
INSERT INTO content_language_config (content_value, language, content_key, description) VALUES ('disabled','en','任务禁用','');
INSERT INTO content_language_config (content_value, language, content_key, description) VALUES ('failed','en','已失败','');
INSERT INTO content_language_config (content_value, language, content_key, description) VALUES ('partially succeeded','en','失败，但存在部分成功','');
INSERT INTO content_language_config (content_value, language, content_key, description) VALUES ('finished','en','运行完成','');
INSERT INTO content_language_config (content_value, language, content_key, description) VALUES ('killed','en','杀死','');
INSERT INTO content_language_config (content_value, language, content_key, description) VALUES ('none','en','无状态','');
INSERT INTO content_language_config (content_value, language, content_key, description) VALUES ('pending','en','等待中','');
INSERT INTO content_language_config (content_value, language, content_key, description) VALUES ('preparing','en','准备中','');
INSERT INTO content_language_config (content_value, language, content_key, description) VALUES ('queued','en','排队中','');
INSERT INTO content_language_config (content_value, language, content_key, description) VALUES ('ready','en','就绪','');
INSERT INTO content_language_config (content_value, language, content_key, description) VALUES ('running','en','运行中','');
INSERT INTO content_language_config (content_value, language, content_key, description) VALUES ('skipped','en','无效任务','');
INSERT INTO content_language_config (content_value, language, content_key, description) VALUES ('stopped','en','已停止','');
INSERT INTO content_language_config (content_value, language, content_key, description) VALUES ('stopping','en','正在停止','');
INSERT INTO content_language_config (content_value, language, content_key, description) VALUES ('succeeded','en','已成功','');
INSERT INTO content_language_config (content_value, language, content_key, description) VALUES ('clean','en','清洗','');
INSERT INTO content_language_config (content_value, language, content_key, description) VALUES ('batch','en','批处理','');
INSERT INTO content_language_config (content_value, language, content_key, description) VALUES ('stream','en','流处理','');
INSERT INTO content_language_config (content_value, language, content_key, description) VALUES ('model','en','模型','');
INSERT INTO content_language_config (content_value, language, content_key, description) VALUES ('transform','en','固化节点处理','');
INSERT INTO content_language_config (content_value, language, content_key, description) VALUES ('shipper','en','分发','');
INSERT INTO content_language_config (content_value, language, content_key, description) VALUES ('puller','en','拉取','');
INSERT INTO content_language_config (content_value, language, content_key, description) VALUES ('stream model','en','实时模型','');
INSERT INTO content_language_config (content_value, language, content_key, description) VALUES ('batch model','en','离线模型','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('用户状态','en','user status','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('用户运营','en','user operate','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('用户设备','en','user equipment','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('客户端信息','en','user client','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('用户终端','en','terminal','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('技术运营数据','en','techops','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('舆情','en','sentiment','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('安全','en','security','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('公共组件','en','public componet','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('操作系统','en','performance','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('营销支付','en','payments','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('付费相关','en','pay related information','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('运维操作','en','ops','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('操作数据','en','operation','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('在线相关','en','online related information','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('公告','en','notice','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('网络拓扑','en','net topology','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('网络状态','en','net status','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('网络设备','en','net equipment','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('网络','en','net','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('营销操作','en','marketing ops','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('登录登出','en','login&out','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('硬件/资源拓扑','en','hardware topology','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('硬件/资源','en','hardware resources','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('硬件/资源设备','en','hardware equipment','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('下载安装','en','download install','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('模块组件','en','component','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('通用安全','en','common security','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('业务安全','en','business security','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('业务模块','en','business module','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('业务用户数据','en','bissness','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('基础组件','en','base componet','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('告警/故障','en','alert','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('活跃相关','en','active related information','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('基础设施层','en','infrastructure','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('硬件资源','en','hw resources','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('网络','en','net resources','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('服务器','en','server','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('物理机','en','physical machine','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('虚拟机','en','virtual machine','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('容器','en','container','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('应用（组件）层','en','component','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('业务层','en','business','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('蓝鲸监控','en','BK Monitor','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('配置平台','en','CMDB','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('蓝鲸监控','en','BK Monitor','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('日志检索','en','Log-Search','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('数据平台','en','Data','');

