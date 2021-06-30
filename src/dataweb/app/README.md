# BK_DATAWEB

蓝鲸数据平台WEB端，项目采用前后端分离的方式开发，前端使用 VUE 框架，后端使用 DJANGO+DRF 提供 API

## 代码规范

* Python 遵照 PEP8 标准，使用 flake8 工具在 CI 中进行代码检查
* 类中的各类方法，请按照 classmethod、instancemethod、property、staticmethod 次序添加
* 获取单个元素信息方法，命名为 get_xxxx()，获取元素列表方法，命名为 list_xxx()

### API 文档

安装 apidoc 
```
npm install apidoc -g
```

在项目目录生成 APIDOC 静态页面，提供前端访问
```
apidoc -i . -o static/apidoc
```

### 安装过程

1. 使用 python3.6 环境
2. 安装 requirements.txt 中的包
3. 在项目工程目录下添加 .env 文件，添加环境变量

```
DATABASE_URL=mysql://root:123@localhost:3306/dataweb
DJANGO_SETTINGS_MODULE=settings
RUN_VER=openpaas
APP_ID=bk_dataweb
APP_TOKEN=__APP_TOKEN__
BKAPP_PAAS_HOST=__BKAPP_PAAS_HOST__
```

`__APP_TOKEN__` 为应用在对应的平台生成密钥
`__BKAPP_PAAS_HOST__` 为应用所在的 PaaS 平台根域名，形如 http://xxx.bkee.com

### 单元测试

1. 请安装前置依赖包

```
pip install requests_mock
pip install Faker
```

2. 测试文件统一放置在 tests 目录下
3. 测试指令 `python manage.py test --keepdb`
