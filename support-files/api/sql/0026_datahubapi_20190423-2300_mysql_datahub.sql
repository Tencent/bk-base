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

use bkdata_basic;

set names utf8;

ALTER TABLE access_raw_data ADD permission varchar(30) DEFAULT "all" NULL COMMENT '数据限制，"all","read_only"';

ALTER TABLE access_scenario_config ADD attribute varchar(30) DEFAULT "" NULL COMMENT '属性："clouds"';

ALTER TABLE access_host_config ADD active int DEFAULT 1 NULL COMMENT '是否有效';

delete from access_scenario_config;

insert into access_scenario_config(id,data_scenario_name, data_scenario_alias,active,attribute,type,orders, description)
values (35,'log','日志文件',1,'clouds','common','1','log');
insert into access_scenario_config(id,data_scenario_name, data_scenario_alias,active,attribute,type,orders, description)
values (36,'db','数据库',1,'','common','2','db');
insert into access_scenario_config(id,data_scenario_name, data_scenario_alias,active,attribute,type,orders, description)
values (37,'queue','消息队列',0,'','common','3','queue');
insert into access_scenario_config(id,data_scenario_name, data_scenario_alias,active,attribute,type,orders, description)
values (38,'http','HTTP',1,'clouds','common','4','http');
insert into access_scenario_config(id,data_scenario_name, data_scenario_alias,active,attribute,type,orders, description)
values (39,'file','文件上传',1,'','common','5','file');
insert into access_scenario_config(id,data_scenario_name, data_scenario_alias,active,attribute,type,orders, description)
values (40,'script','脚本上报',1,'','common','6','script');
insert into access_scenario_config(id,data_scenario_name, data_scenario_alias,active,attribute,type,orders, description)
values (41,'sdk','SDK',0,'','common','7','sdk');

insert into access_scenario_config(id,data_scenario_name, data_scenario_alias,active,attribute,type,orders, description)
values (42,'beacon','灯塔',1,'','other','8','beacon');
insert into access_scenario_config(id,data_scenario_name, data_scenario_alias,active,attribute,type,orders, description)
values (43,'tglog','TGLOG',1,'clouds','other','9','tglog');
insert into access_scenario_config(id,data_scenario_name, data_scenario_alias,active,attribute,type,orders, description)
values (44,'tlog','TLOG',1,'','other','10','tlog');
insert into access_scenario_config(id,data_scenario_name, data_scenario_alias,active,attribute,type,orders, description)
values (45,'tqos','TQOS',1,'','other','11','tqos');
insert into access_scenario_config(id,data_scenario_name, data_scenario_alias,active,attribute,type,orders, description)
values (46,'tdw','TDW',1,'','other','12','tdw');
insert into access_scenario_config(id,data_scenario_name, data_scenario_alias,active,attribute,type,orders, description)
values (47,'custom','自定义',1,'clouds','other','13','custom');
insert into access_scenario_config(id,data_scenario_name, data_scenario_alias,active,attribute,type,orders, description)
values (48,'tube','TDBANK',1,'','other','14','tube');
insert into access_scenario_config(id,data_scenario_name, data_scenario_alias,active,attribute,type,orders, description)
values (49,'tdm','TDM',1,'','other','15','tdm');



insert into access_manager_config(names, type,  created_by, description) values ("","email","admin","");
insert into access_manager_config(names, type,  created_by, description) values ("admin","beacon","admin","");


