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

BEGIN;
--
-- Create model Tag
--
CREATE TABLE IF NOT EXISTS `tag` (`id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY, `tag_attr_id` integer NOT NULL, `attr_value` varchar(128) NULL, `active` integer NOT NULL, `created_by` varchar(50) NULL, `created_at` datetime(6) NULL, `updated_by` varchar(50) NULL, `updated_at` datetime(6) NULL, `tag_target_id` integer NOT NULL);
--
-- Create model TagAttributeSchema
--
CREATE TABLE IF NOT EXISTS `tag_attribute_schema` (`id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY, `tag_code` varchar(128) NOT NULL, `attr_name` varchar(128) NOT NULL, `attr_alias` varchar(128) NOT NULL, `attr_type` varchar(128) NOT NULL, `constraint_value` longtext NULL, `attr_index` integer NOT NULL, `description` longtext NULL, `active` integer NOT NULL, `created_by` varchar(50) NULL, `created_at` datetime(6) NULL, `updated_by` varchar(50) NULL, `updated_at` datetime(6) NULL);
--
-- Create model TagSchema
--
CREATE TABLE IF NOT EXISTS `tag_schema` (`id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY, `code` varchar(128) NOT NULL UNIQUE, `alias` varchar(128) NULL, `parent_id` integer NOT NULL, `seq_index` integer NOT NULL, `sync` integer NOT NULL, `tag_type` varchar(32) NULL, `kpath` integer NOT NULL, `icon` longtext NULL, `active` integer NOT NULL, `created_by` varchar(50) NOT NULL, `created_at` datetime(6) NULL, `updated_by` varchar(50) NULL, `updated_at` datetime(6) NULL, `description` longtext NULL);
--
-- Create model TagTarget
--
CREATE TABLE IF NOT EXISTS `tag_target` (`id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY, `target_id` varchar(128) NOT NULL, `target_type` varchar(64) NOT NULL, `tag_code` varchar(128) NOT NULL, `probability` double precision NOT NULL, `checked` integer NOT NULL, `active` integer NOT NULL, `description` longtext NULL, `created_by` varchar(50) NOT NULL, `created_at` datetime(6) NULL, `updated_by` varchar(50) NULL, `updated_at` datetime(6) NULL, `tag_type` varchar(32) NULL, `bk_biz_id` integer NULL, `project_id` integer NULL);
--
-- Alter unique_together for tagtarget (1 constraint(s))
--
ALTER TABLE `tag_target` ADD CONSTRAINT `tag_target_tag_code_target_type_target_id_c8089653_uniq` UNIQUE (`tag_code`, `target_type`, `target_id`);
--
-- Alter unique_together for tagattributeschema (1 constraint(s))
--
ALTER TABLE `tag_attribute_schema` ADD CONSTRAINT `tag_attribute_schema_tag_code_attr_name_099c9c56_uniq` UNIQUE (`tag_code`, `attr_name`);
COMMIT;
BEGIN;
--
-- Add field scope to tagtarget
--
ALTER TABLE `tag_target` ADD COLUMN `scope` varchar(128) DEFAULT 'business' NOT NULL;
ALTER TABLE `tag_target` ALTER COLUMN `scope` DROP DEFAULT;
--
-- Alter field attr_alias on tagattributeschema
--
ALTER TABLE `tag_attribute_schema` MODIFY `attr_alias` varchar(128) NULL;
--
-- Alter field tag_type on tagschema
--
ALTER TABLE `tag_schema` MODIFY `tag_type` varchar(32) NOT NULL;
COMMIT;
BEGIN;
--
-- Alter field probability on tagtarget
--
ALTER TABLE `tag_target` MODIFY `probability` double precision NULL;
COMMIT;
BEGIN;
--
-- Rename model Tag to TagAttribute
--
--
-- Add field source_tag_code to tagtarget
--
ALTER TABLE `tag_target` ADD COLUMN `source_tag_code` varchar(128) DEFAULT '' NOT NULL;
ALTER TABLE `tag_target` ALTER COLUMN `source_tag_code` DROP DEFAULT;
--
-- Alter unique_together for tagtarget (1 constraint(s))
--
ALTER TABLE `tag_target` DROP INDEX `tag_target_tag_code_target_type_target_id_c8089653_uniq`;
ALTER TABLE `tag_target` ADD CONSTRAINT `tag_target_tag_code_target_type_tar_6ab827a6_uniq` UNIQUE (`tag_code`, `target_type`, `target_id`, `source_tag_code`);
--
-- Rename table for tagattribute to tag_attribute
--
RENAME TABLE `tag` TO `tag_attribute`;
COMMIT;
BEGIN;
--
-- Rename model TagSchema to Tag
--
--
-- Rename table for tag to tag
--
RENAME TABLE `tag_schema` TO `tag`;
COMMIT;
BEGIN ;
ALTER TABLE tag CONVERT TO CHARACTER SET utf8;
ALTER TABLE tag_target CONVERT TO CHARACTER SET utf8;
ALTER TABLE tag_attribute_schema CONVERT TO CHARACTER SET utf8;
ALTER TABLE tag_attribute CONVERT TO CHARACTER SET utf8;
COMMIT ;


use bkdata_meta;

BEGIN;
--
-- Create model Tag
--
CREATE TABLE IF NOT EXISTS `tag` (`id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY, `tag_attr_id` integer NOT NULL, `attr_value` varchar(128) NULL, `active` integer NOT NULL, `created_by` varchar(50) NULL, `created_at` datetime(6) NULL, `updated_by` varchar(50) NULL, `updated_at` datetime(6) NULL, `tag_target_id` integer NOT NULL);
--
-- Create model TagAttributeSchema
--
CREATE TABLE IF NOT EXISTS `tag_attribute_schema` (`id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY, `tag_code` varchar(128) NOT NULL, `attr_name` varchar(128) NOT NULL, `attr_alias` varchar(128) NOT NULL, `attr_type` varchar(128) NOT NULL, `constraint_value` longtext NULL, `attr_index` integer NOT NULL, `description` longtext NULL, `active` integer NOT NULL, `created_by` varchar(50) NULL, `created_at` datetime(6) NULL, `updated_by` varchar(50) NULL, `updated_at` datetime(6) NULL);
--
-- Create model TagSchema
--
CREATE TABLE IF NOT EXISTS `tag_schema` (`id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY, `code` varchar(128) NOT NULL UNIQUE, `alias` varchar(128) NULL, `parent_id` integer NOT NULL, `seq_index` integer NOT NULL, `sync` integer NOT NULL, `tag_type` varchar(32) NULL, `kpath` integer NOT NULL, `icon` longtext NULL, `active` integer NOT NULL, `created_by` varchar(50) NOT NULL, `created_at` datetime(6) NULL, `updated_by` varchar(50) NULL, `updated_at` datetime(6) NULL, `description` longtext NULL);
--
-- Create model TagTarget
--
CREATE TABLE IF NOT EXISTS `tag_target` (`id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY, `target_id` varchar(128) NOT NULL, `target_type` varchar(64) NOT NULL, `tag_code` varchar(128) NOT NULL, `probability` double precision NOT NULL, `checked` integer NOT NULL, `active` integer NOT NULL, `description` longtext NULL, `created_by` varchar(50) NOT NULL, `created_at` datetime(6) NULL, `updated_by` varchar(50) NULL, `updated_at` datetime(6) NULL, `tag_type` varchar(32) NULL, `bk_biz_id` integer NULL, `project_id` integer NULL);
--
-- Alter unique_together for tagtarget (1 constraint(s))
--
ALTER TABLE `tag_target` ADD CONSTRAINT `tag_target_tag_code_target_type_target_id_c8089653_uniq` UNIQUE (`tag_code`, `target_type`, `target_id`);
--
-- Alter unique_together for tagattributeschema (1 constraint(s))
--
ALTER TABLE `tag_attribute_schema` ADD CONSTRAINT `tag_attribute_schema_tag_code_attr_name_099c9c56_uniq` UNIQUE (`tag_code`, `attr_name`);
COMMIT;
BEGIN;
--
-- Add field scope to tagtarget
--
ALTER TABLE `tag_target` ADD COLUMN `scope` varchar(128) DEFAULT 'business' NOT NULL;
ALTER TABLE `tag_target` ALTER COLUMN `scope` DROP DEFAULT;
--
-- Alter field attr_alias on tagattributeschema
--
ALTER TABLE `tag_attribute_schema` MODIFY `attr_alias` varchar(128) NULL;
--
-- Alter field tag_type on tagschema
--
ALTER TABLE `tag_schema` MODIFY `tag_type` varchar(32) NOT NULL;
COMMIT;
BEGIN;
--
-- Alter field probability on tagtarget
--
ALTER TABLE `tag_target` MODIFY `probability` double precision NULL;
COMMIT;
BEGIN;
--
-- Rename model Tag to TagAttribute
--
--
-- Add field source_tag_code to tagtarget
--
ALTER TABLE `tag_target` ADD COLUMN `source_tag_code` varchar(128) DEFAULT '' NOT NULL;
ALTER TABLE `tag_target` ALTER COLUMN `source_tag_code` DROP DEFAULT;
--
-- Alter unique_together for tagtarget (1 constraint(s))
--
ALTER TABLE `tag_target` DROP INDEX `tag_target_tag_code_target_type_target_id_c8089653_uniq`;
ALTER TABLE `tag_target` ADD CONSTRAINT `tag_target_tag_code_target_type_tar_6ab827a6_uniq` UNIQUE (`tag_code`, `target_type`, `target_id`, `source_tag_code`);
--
-- Rename table for tagattribute to tag_attribute
--
RENAME TABLE `tag` TO `tag_attribute`;
COMMIT;
BEGIN;

--
-- Rename model TagSchema to Tag
--
--
-- Rename table for tag to tag
--
RENAME TABLE `tag_schema` TO `tag`;
COMMIT;
BEGIN ;
ALTER TABLE tag CONVERT TO CHARACTER SET utf8;
ALTER TABLE tag_target CONVERT TO CHARACTER SET utf8;
ALTER TABLE tag_attribute_schema CONVERT TO CHARACTER SET utf8;
ALTER TABLE tag_attribute CONVERT TO CHARACTER SET utf8;
COMMIT ;