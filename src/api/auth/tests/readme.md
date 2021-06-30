# 单元测试说明
保证单元测试的 A-TRIP
自动性(Automatic)
完备性(Thorough)
可重复性(Repeatable)
独立性(Independent)
专业性(Professional)

## 需准备好测试数据库，并初始化测试数据库

执行SQL文件  auth/auth.sql 和 auth/tests/tools/init.sql

1. 创建测试数据库
    TEST_DB=test_bk_bkdata_api
    
    mysql -uroot
    
    create database $TEST_DB;

2. 执行前置初始化数据init_pre.sql（不依赖auth.sql)

    mysql -uroot -D$TEST_DB < auth/tests/tools/init_pre.sql

3. 执行auth.sql 初始化authapi相关的表结构

    mysql -uroot -D$TEST_DB < auth/auth.sql

4. 执行初始化auth单元测试用的数据init.sql（依赖auth.sql)

    mysql -uroot -D$TEST_DB < auth/tests/tools/init.sql


## 安装 mock Python 包
```
pip install mock
```


## 执行django单元测试
```
python manage.py test --keepdb auth.tests
```

## 清理DB
```sql
drop database $TEST_DB;
```

## 快捷脚本

- bin/reinit_test_db 重新初始化测试数据库（脚本内的配置信息，按照本地实际情况进行调整）
