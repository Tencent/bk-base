##在docker环境下运行测试


###一、安装docker
>上docker官网下载

###二、下载databusapi镜像
>从企业云盘上下载databusapi这个镜像，并加载到docker的仓库中
```
docker load -i databusapi.tar
docker images
```

###三、导入镜像
>找到刚才导入的databusapi这个image，设置其的名称和tag为databusapi:v4。下面命令中a68af077f5ca为镜像的id
```
docker tag a68af077f5ca databusapi:v4
```

###四、配置docker选项(references)
>a) 将代码目录(/your/code/path)增加到"File Sharing"中<br/>
>b) 根据机器所属网络，手动配置docker使用的代理地址

###五、启动依赖环境
>使用docker-compose启动测试所依赖的环境:
```
docker-compose -f /your/code/path/databus.yml up -d
```

###六、修改测试脚本
>修改pizza下的run_test_in_docker.sh脚本，将第一行的 cd src 改为 cd /your/code/path

###七、导入mysql数据库表结构
>a) 更新config_db项目代码，进入目录 表结构第二轮 <br/>
>b) 命令行下登录mysql客户端，密码为XXXXXX，source上一步目录里的 平台公共表.sql 和 数据总线.sql <br/>
>c) 修改pizza项目下pizza/settings.py文件，配置数据库连接
```
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.mysql',
        'NAME': 'bkdata_basic',
        'HOST': "mysql",
        'USER': "root",
        'PASSWORD': "XXXXX",
        'PORT': 3306
    }
}
```

###八、运行测试
>启动测试，命令如下(将/your/code/path替换为实际的代码目录):
```
docker run --rm -v /your/code/path:/your/code/path -it --link mysql:mysql --link zookeeper:zk --link kafka:kafka --network pizza_default databusapi:v4 /bin/sh /your/code/path/run_test_in_docker.sh
```

###九、docker环境配置发生变化时更新步骤：
>a) 停掉运行中的docker测试环境<br/>
```
docker-compose -f /your/code/path/databus.yml stop
```
>b) 找到刚才运行的docker容器的ID，删除这些容器（将CONTAINER_ID替换为实际容器的ID）<br/>
```
docker ps -a
docker rm CONTAINER_ID1 CONTAINER_ID2 CONTAINER_ID3
```
>c) 重复步骤五、步骤七将测试环境启动，并初始化数据库<br/>
