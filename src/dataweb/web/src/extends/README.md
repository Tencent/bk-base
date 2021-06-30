## 模块说明

### 用途
- 通过关联其他项目，扩展当前功能

### 使用说明
- 项目关联通过`extend.config`配置，配置内容为扩展项目所在绝对路径
- 通过执行脚本`npm run init-extend`进行项目关联
- 通过执行脚本`npm run restore-extend`取消项目关联

### 注意事项
- 可以直接在当前目录下的`packages`目录进行开发
- 最终输出内容参照`packages/index.ts`内定义的导出标准
- 如果使用关联其他项目，`packages`目录将被重命名，新建一个 `packages`关联到指定的项目
- 关联项目内可以进行单独的Webpack配置
