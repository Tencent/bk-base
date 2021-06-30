# data

> dataweb

## Build Setup


### 配置本地git unchaned file

git远程仓库里有这些文件，比如配置，我们必须要在本地修改配置来适应当前运行环境，难说还会涉及到数据库连接密码等敏感信息，这种情况下我们不想每次提交的时候都去跟踪这些文件，也不想把本地的记录提交上去。

忽略自动生成的垃圾文件、中间文件、敏感信息文件。

为防止开发时不小心提交本地配置文件，造成敏感信息泄露，首先需要配置本地git的unchange file，具体命令如下： 

1. 忽略.gloabal.dev.config.json配置
```bash
cd src/dataweb/web/
git update-index --assume-unchanged .global.dev.config.json
```

2. 忽略extends下的所有文件夹
```bash
cd src/dataweb/web/src/extends
git update-index --assume-unchanged $(git ls-files | tr '\n' ' ')
```

3. 检查是否成功设置
```bash
cd /src/dataweb/web/
git ls-files -v | grep '^h '

# 正常设置后的输出为:

h .global.dev.config.json
h src/extends/.extend.config
h src/extends/index.ts
h src/extends/init.js
h src/extends/packages/index.ts
h src/extends/restore.js
```

### 配置本地运行环境
 - 找到`.global.dev.config.json`文件，`BKBASE_Global`

```code
siteUrl: 必须设置，用于访问API
loginUrl： 必须设置，用于登录
staticUrl：必须设置，用于访问静态文件
``` 
其他参数按照实际需求配置即可

### 配置Extends
 - 在 `src\extends`目录下面找到`.extend.config`，配置扩展项目路径(只支持绝对路径)
```
运行 npm run init-extend
还原配置 npm run restore-extend
```


``` bash
# install dependencies
npm install

# serve with hot reload at localhost:8080
npm run dev

# build for production with minification
npm run build

# build for production and view the bundle analyzer report
npm run build --report
```