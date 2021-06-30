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

## 缓存SDK用法

### 简介
本SDK是用于查询Ignite缓存中数据的jar包。

### 应用场景

目前能支持的业务应用场景有：
- 计算相关的IP库关联数据
- 计算相关的用户导入的关联数据
- 查询的一级缓存

### 使用方法
以IP库为例查询Ignite缓存（例如用户关联数据）。

1、生成配置文件databus.properties
```
# 配置ignite集群中的节点IP，逗号分隔，或者域名。
ignite.host=xx.xx.xx.xx,xx.xx.xx.xx,xx.xx.xx.xx
ignite.port=10800
# 单进程里ignite连接池里最多缓存的连接数量，按需配置
ignite.pool.size=3
# Ignite鉴权信息，如果Ignite集群启用了鉴权，这里需要输入账号和密码
#ignite.user=ignite
#ignite.pass=xxx

# 主缓存类型
cache.primary=ignite
```

2、代码示例
```java
// 构建缓存查询对象（这里false表示查询发生异常时，不抛出异常，仅返回空的数据对象。改为true表示异常会向上抛出，业务代码需要捕获异常。）
BkCache<String> cache = (new CacheFactory<String>()).buildCache(false);

// 需要查询的key集合
Set<String> keys = new HashSet<>();
keys.add("xx.xx.xx.xx");
keys.add("xx.xx.xx.x");

// 需要查询的字段列表
List<String> fields = new ArrayList<>(10);
fields.add("IP");
fields.add("Asid");
fields.add("City");
fields.add("Country");
fields.add("Province");
fields.add("Region");

// 查询数据，这里cacheName为需要查询的缓存名称
Map<String, Map> result = cache.getByKeys(cacheName, keys, fields);
```


#### 不使用databus.properties配置文件
如果不想将在项目中增加databus.properties这个配置文件，可以通过如下两种方式达到传递配置给SDK包。
1、将配置写入另外的配置文件，例如a.config，然后在启动JVM的参数里databus.properties.file属性的值为a.config
```
-Ddatabus.properties.file=a.config
```

2、在代码中逐项设置SDK所需配置项
```java
DatabusProps props = DatabusProps.getInstance();
props.setProperty(CacheConsts.CACHE_PRIMARY, primary);
props.setProperty(CacheConsts.IGNITE_HOST, "xx.xx.xx.xx,xx.xx.xx.xx,xx.xx.xx.xx");
props.setProperty(CacheConsts.IGNITE_PORT, "10800");
props.setProperty(CacheConsts.IGNITE_USER, "ignite");
props.setProperty(CacheConsts.IGNITE_PASS, "xxxx");
props.setProperty(CacheConsts.IGNITE_POOL_SIZE, 1);

BkCache<String> cache = (new CacheFactory<String>()).buildCache(false);
```
