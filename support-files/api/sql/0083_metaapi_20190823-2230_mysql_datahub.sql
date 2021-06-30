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

BEGIN;
delete
from cluster_group_config
where cluster_group_id = 'default';

INSERT INTO `tag_target` (target_id, target_type, tag_code, active, description, created_by, updated_by,
                          source_tag_code, checked, scope, tag_type)
VALUES ('1', 'project', 'NA', 1, '', 'bkdata', 'bkdata', 'NA', 1, 'business', 'manage');
INSERT INTO `tag_target` (target_id, target_type, tag_code, active, description, created_by, updated_by,
                          source_tag_code, checked, scope, tag_type)
VALUES ('1', 'project', 'overseas', 1, '', 'bkdata', 'bkdata', 'NA', 1, 'business', 'manage');
INSERT INTO `tag_target` (target_id, target_type, tag_code, active, description, created_by, updated_by,
                          source_tag_code, checked, scope, tag_type)
VALUES ('1', 'project', 'geog_area', 1, '', 'bkdata', 'bkdata', 'NA', 1, 'business', 'manage');

INSERT INTO `tag_target` (target_id, target_type, tag_code, active, description, created_by, updated_by,
                          source_tag_code, checked, scope, tag_type)
VALUES ('2', 'project', 'NA', 1, '', 'bkdata', 'bkdata', 'NA', 1, 'business', 'manage');
INSERT INTO `tag_target` (target_id, target_type, tag_code, active, description, created_by, updated_by,
                          source_tag_code, checked, scope, tag_type)
VALUES ('2', 'project', 'overseas', 1, '', 'bkdata', 'bkdata', 'NA', 1, 'business', 'manage');
INSERT INTO `tag_target` (target_id, target_type, tag_code, active, description, created_by, updated_by,
                          source_tag_code, checked, scope, tag_type)
VALUES ('2', 'project', 'geog_area', 1, '', 'bkdata', 'bkdata', 'NA', 1, 'business', 'manage');

INSERT INTO `tag_target` (target_id, target_type, tag_code, active, description, created_by, updated_by,
                          source_tag_code, checked, scope, tag_type)
VALUES ('3', 'project', 'NA', 1, '', 'bkdata', 'bkdata', 'NA', 1, 'business', 'manage');
INSERT INTO `tag_target` (target_id, target_type, tag_code, active, description, created_by, updated_by,
                          source_tag_code, checked, scope, tag_type)
VALUES ('3', 'project', 'overseas', 1, '', 'bkdata', 'bkdata', 'NA', 1, 'business', 'manage');
INSERT INTO `tag_target` (target_id, target_type, tag_code, active, description, created_by, updated_by,
                          source_tag_code, checked, scope, tag_type)
VALUES ('3', 'project', 'geog_area', 1, '', 'bkdata', 'bkdata', 'NA', 1, 'business', 'manage');



COMMIT;

use bkdata_meta;
BEGIN;
delete
from cluster_group_config
where cluster_group_id = 'default';


INSERT INTO `tag_target` (target_id, target_type, tag_code, active, description, created_by, updated_by,
                          source_tag_code, checked, scope, tag_type)
VALUES ('1', 'project', 'NA', 1, '', 'bkdata', 'bkdata', 'NA', 1, 'business', 'manage');
INSERT INTO `tag_target` (target_id, target_type, tag_code, active, description, created_by, updated_by,
                          source_tag_code, checked, scope, tag_type)
VALUES ('1', 'project', 'overseas', 1, '', 'bkdata', 'bkdata', 'NA', 1, 'business', 'manage');
INSERT INTO `tag_target` (target_id, target_type, tag_code, active, description, created_by, updated_by,
                          source_tag_code, checked, scope, tag_type)
VALUES ('1', 'project', 'geog_area', 1, '', 'bkdata', 'bkdata', 'NA', 1, 'business', 'manage');

INSERT INTO `tag_target` (target_id, target_type, tag_code, active, description, created_by, updated_by,
                          source_tag_code, checked, scope, tag_type)
VALUES ('2', 'project', 'NA', 1, '', 'bkdata', 'bkdata', 'NA', 1, 'business', 'manage');
INSERT INTO `tag_target` (target_id, target_type, tag_code, active, description, created_by, updated_by,
                          source_tag_code, checked, scope, tag_type)
VALUES ('2', 'project', 'overseas', 1, '', 'bkdata', 'bkdata', 'NA', 1, 'business', 'manage');
INSERT INTO `tag_target` (target_id, target_type, tag_code, active, description, created_by, updated_by,
                          source_tag_code, checked, scope, tag_type)
VALUES ('2', 'project', 'geog_area', 1, '', 'bkdata', 'bkdata', 'NA', 1, 'business', 'manage');

INSERT INTO `tag_target` (target_id, target_type, tag_code, active, description, created_by, updated_by,
                          source_tag_code, checked, scope, tag_type)
VALUES ('3', 'project', 'NA', 1, '', 'bkdata', 'bkdata', 'NA', 1, 'business', 'manage');
INSERT INTO `tag_target` (target_id, target_type, tag_code, active, description, created_by, updated_by,
                          source_tag_code, checked, scope, tag_type)
VALUES ('3', 'project', 'overseas', 1, '', 'bkdata', 'bkdata', 'NA', 1, 'business', 'manage');
INSERT INTO `tag_target` (target_id, target_type, tag_code, active, description, created_by, updated_by,
                          source_tag_code, checked, scope, tag_type)
VALUES ('3', 'project', 'geog_area', 1, '', 'bkdata', 'bkdata', 'NA', 1, 'business', 'manage');



COMMIT;