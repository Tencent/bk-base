#!/bin/bash -l

BASE_PATH=$(cd `dirname $0`; pwd)

PROJECT_PATH=$(cd ${BASE_PATH}; cd ../..; pwd)

DATABASE_HOST='localhost'
DATABASE_PORT='3306'
DATABASE_USER='root'
DATABASE_PASS='123'
DATABASE_NAME='test_bkdata_auth'

# 清理DB
echo "Clear database ${DATABASE_NAME} ..."
mysql -u${DATABASE_USER} -h${DATABASE_HOST} -P${DATABASE_PORT} -p${DATABASE_PASS} -e "drop database ${DATABASE_NAME}"

# 创建测试DB
echo "Create database ${DATABASE_NAME} ..."
mysql -u${DATABASE_USER} -h${DATABASE_HOST} -P${DATABASE_PORT} -p${DATABASE_PASS} -e "create database ${DATABASE_NAME}"

# 前置初始化数据
echo "Init pre-init data ..."
mysql -u${DATABASE_USER} -h${DATABASE_HOST} -P${DATABASE_PORT} -p${DATABASE_PASS} ${DATABASE_NAME} < ${PROJECT_PATH}/contents/init_pre.sql

# 初始化authapi相关表结构
echo "Init main data ..."
mysql -u${DATABASE_USER} -h${DATABASE_HOST} -P${DATABASE_PORT} -p${DATABASE_PASS} ${DATABASE_NAME} < ${PROJECT_PATH}/contents/auth.sql

# 初始化单元测试需要的测试数据
echo "Init test data ..."
mysql -u${DATABASE_USER} -h${DATABASE_HOST} -P${DATABASE_PORT} -p${DATABASE_PASS} ${DATABASE_NAME} < ${PROJECT_PATH}/tests/bin/init.sql
