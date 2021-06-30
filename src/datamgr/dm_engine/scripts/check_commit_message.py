# -*- coding: utf-8 -*-
"""
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
"""

import sys

ALLOWED_COMMIT_MSG_PREFIX = [
    ("feature", "新特性"),
    ("bugfix", "线上功能bug"),
    ("minor", "不重要的修改（换行，拼写错误等）"),
    ("optimization", "功能优化"),
    ("sprintfix", "未上线代码修改 （功能模块未上线部分bug）"),
    ("refactor", "功能重构"),
    ("test", "增加测试代码"),
    ("docs", "编写文档"),
    ("merge", "分支合并及冲突解决"),
]


def get_commit_message():
    args = sys.argv
    if len(args) <= 1:
        print("Warning: The path of file `COMMIT_EDITMSG` not given, skipped!")
        return 0
    commit_message_filepath = args[1]
    with open(commit_message_filepath, "r") as fd:
        content = fd.read()
    return content.strip().lower()


def main():
    content = get_commit_message()
    for prefix in ALLOWED_COMMIT_MSG_PREFIX:
        if content.startswith(prefix[0]):
            return 0

    else:
        print("Commit Message 不符合规范！必须包含以下前缀之一：")
        [print("%-12s\t- %s" % prefix) for prefix in ALLOWED_COMMIT_MSG_PREFIX]

    return 1


if __name__ == "__main__":
    exit(main())
