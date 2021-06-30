

## .flake8

.flake8 为 flake8 工具依赖的 PEP8 规范规则文件，建议统一维护升级

## pyproject.toml

pyproject.toml 配置 black 自动格式化时依赖的规则，建议统一维护升级


## .pre-commit-config.yaml

`pre-commit` 插件可以防止不符合规范的代码提交到仓库，强烈推荐每位开发者代码提交前检查，其中 `.pre-commit-config.yaml` 为 `pre-commit` 的配置文件。

`pre-commit` 检查通过本地配置实现，因此每个开发者在开发之前都必须先配好本地的 Git Hooks。

推荐使用 [pre-conmmit](https://pre-commit.com/) 框架对 Git Hooks 进行配置及管理。**pre-commit** 是由 python 实现的，用于管理和维护多个 pre-commit hooks 的实用框架。它提供了插件式的管理机制，并拥有大量的官方与第三方插件（需要时可自行开发），能够快速实现常见的代码检查任务，如 `eslint` 检查（支持跨语言），`flake8` 检查，`isort` 代码美化等。

当项目中已有 .pre-commit-config.yaml，然后执行以下指令初始化

```shell
# py2
pip install zipp==0.5.1
pip install pre-commit
pre-commit install --allow-missing-config
pre-commit install --hook-type commit-msg --allow-missing-config

# py3
pip install pre-commit
pre-commit install --allow-missing-config
pre-commit install --hook-type commit-msg --allow-missing-config
```
