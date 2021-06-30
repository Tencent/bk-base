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

import subprocess

from common.log import logger


def get_cmd_response(cmd):
    """
    通过调用命令行执行命令，将命令执行结果输出返回
    :param cmd: 命令行参数
    :return: 执行结果和输出，tuple结构
    """
    try:
        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
        logger.info("CMD: {}, OUTPUT: \n{}".format(cmd, output))
        return True, output
    except subprocess.CalledProcessError as e:
        logger.error("{}. OUTPUT: {}".format(e, e.output))
        return False, e.output
    except Exception as e:
        logger.error("failed to get cmd {} output. {}".format(cmd, e))
        return False, str(e)


def run_cmd_and_send_confirm(cmd, confirm_count=1):
    """
    执行指定的命令，并且在读取到命令的输出内容后，通过交互的方式输入字母 y 和 回车，等待命令继续执行完毕
    :param cmd: 命令行参数
    :return: True/False，执行是否成功
    """
    try:
        proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        s = ""
        for i in [1, confirm_count]:
            s += "y\n"
        std_output, err_output = proc.communicate(bytes(s))
        logger.info("RETURNCODE: {}, STDOUT: {}, STDERR: {}".format(proc.poll(), std_output, err_output))
        return proc.poll() == 0
    except Exception as e:
        logger.error("failed to execute {} and send confirm message. {}".format(cmd, e))
        return False
