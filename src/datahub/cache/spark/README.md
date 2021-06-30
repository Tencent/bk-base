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

## Spark使用缓存SDK用法

### 简介
本SDK是用于Spark查询Ignite中缓存数据的jar包，使用时通过指定ignite配置文件，或传入ignite集群的配置参数构建SparkIgCache对象，然后通过调用此对象的constructDataset方法来获取Dataset，进而通过spark来查询数据。

### 应用场景

离线计算读取缓存数据。

### 使用方法

以IP库为例，假定IP库对应的缓存名称为ipv4_591，包含的字段如下。
 - IP: string (nullable = true)
 - COMMENT: string (nullable = true)
 - ASID: string (nullable = true)
 - FRONT_ISP: string (nullable = true)
 - COUNTRY: string (nullable = true)
 - REGION: string (nullable = true)
 - CITY: string (nullable = true)
 - PROVINCE: string (nullable = true)
 - BACKBONE_ISP: string (nullable = true)

### 查询缓存代码样例

以下代码实例中，通过两种不同的方式构建SparkIgCache，然后对缓存数据进行查询。
分为如下步骤：
 - 构造SparkIgCache对象
 - 调用SparkIgCache#constructDataset方法，传入spark session、缓存名称、缓存的主键列表
 - 通过spark SQL或者spark dataset的操作接口进行数据查询

```java
  /**
   * 测试spark ignite集成
   * @param cacheName 缓存名称
   */
  private void sparkDataset(String cacheName) {
    //Creating spark session.
    SparkSession spark = SparkSession
        .builder()
        .appName("JavaIgniteDataFrameExample")
        .master("local")
        .config("spark.executor.instances", "2")
        .getOrCreate();

    SparkIgCache sic = new SparkIgCache(DatabusProps.getInstance().getOrDefault("ignite.conf.file", "ignite.xml"));
    Dataset<Row> df = sic.constructDataset(spark, cacheName, "ip");
    df.printSchema();
    df.show();

    df.select(col("IP"), col("asid"), col("country"), col("city")).show();

    df.createOrReplaceTempView("ip_lib");
    Dataset<Row> sqlDf = spark.sql("SELECT count(1) FROM ip_lib WHERE country='中国'");
    sqlDf.show();

    // 通过配置项获取ignite相关配置，并生成ignite集群配置文件
    LogUtils.info(log, "using auto created ignite config file to test");
    SparkSession spark2 = SparkSession
        .builder()
        .appName("JavaIgniteDataFrameExample")
        .master("local")
        .config("spark.executor.instances", "2")
        .getOrCreate();
    Map<String, Object> props = new HashMap<>(DatabusProps.getInstance().toMap());
    SparkIgCache sic2 = new SparkIgCache(props);
    Dataset<Row> df2 = sic2.constructDataset(spark2, cacheName, "ip");

    df2.filter(col("FRONT_ISP").eqNullSafe("google")).show();
    df2.filter(col("city").eqNullSafe("okayama")).show();
  }
```

### Ignite配置文件样例

生成xml格式的ignite配置文件，内容样例如下：

```xml
<?xml version="1.0" encoding="UTF-8"?>
<beans xmlns="http://www.springframework.org/schema/beans"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xsi:schemaLocation="
             http://www.springframework.org/schema/beans
             http://www.springframework.org/schema/beans/spring-beans.xsd">

    <bean id="grid.cfg" name="default" class="org.apache.ignite.configuration.IgniteConfiguration">

        <!--<property name="peerClassLoadingEnabled" value="true"/>-->
        <property name="clientMode" value="true" />
        <property name="metricsLogFrequency" value="0" />

        <property name="discoverySpi">
            <bean class="org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpi">
                <property name="zkConnectionString" value="xx.xx.xx:2181"/>
                <property name="zkRootPath" value="/test"/>
                <property name="sessionTimeout" value="30000"/>
                <property name="joinTimeout" value="10000"/>
            </bean>
        </property>
    </bean>

</beans>
```

通过如下方法构建SparkIgCache对象：

```java
SparkIgCache sic = new SparkIgCache(igniteConfFile);
```


### Ignite集群配置样例

在传入SparkIgCache构造函数的map参数中，需包含如下几个键值对：

| 键名称 | 默认值 | 备注 |
| :--: | :--: | --- |
| cluster.name | default | 集群名称 |
| zk.domain | localhost | ignite集群绑定的zk集群域名 |
| zk.port | 2181 | ignite集群绑定的zk集群端口 |
| zk.root.path | / | zk上根目录 |
| session.timeout | 30000 | zk的session过期时间 |
| join.timeout | 10000 | 加入集群的过期时间 |

通过如下方法构建SparkIgCache对象：

```java
public SparkIgCache(Map<String, Object> connInfo)
```
